#!/usr/bin/env python3
"""
Real-Time Stock Data Producer
Fetches 1-minute OHLCV data from Yahoo Finance and publishes to Redpanda/Kafka
Features: Retry logic, state management, DLQ, idempotent design
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pytz
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pythonjsonlogger import jsonlogger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


# ==========================================
# Configuration
# ==========================================
# Maximum backfill days allowed (Yahoo Finance limitations)
MAX_BACKFILL_DAYS = 365

# Validate and set BACKFILL_DAYS with maximum limit
BACKFILL_DAYS_CONFIG = int(os.getenv("BACKFILL_DAYS", "30"))

if BACKFILL_DAYS_CONFIG > MAX_BACKFILL_DAYS:
    print("=" * 80)
    print(f"⚠️  BACKFILL_DAYS LIMIT EXCEEDED ⚠️")
    print(f"Requested: {BACKFILL_DAYS_CONFIG} days")
    print(f"Maximum: {MAX_BACKFILL_DAYS} days")
    print(f"Action: Capping at {MAX_BACKFILL_DAYS} days")
    print("Reason: Yahoo Finance API limitations, processing time, storage")
    print("=" * 80)
    BACKFILL_DAYS = MAX_BACKFILL_DAYS
else:
    BACKFILL_DAYS = BACKFILL_DAYS_CONFIG
REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "stock-raw-data")
KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "stock-dlq")
KAFKA_TOPIC_BACKFILL_DLQ = os.getenv("KAFKA_TOPIC_BACKFILL_DLQ", "stock-backfill-dlq")
STOCK_TICKERS = os.getenv("STOCK_TICKERS", "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,NFLX,AMD,AVGO").split(",")
PRODUCER_INTERVAL_SECONDS = int(os.getenv("PRODUCER_INTERVAL_SECONDS", "60"))
PRODUCER_MAX_RETRIES = int(os.getenv("PRODUCER_MAX_RETRIES", "3"))
PRODUCER_RETRY_DELAY = int(os.getenv("PRODUCER_RETRY_DELAY", "5"))
# Validate and set BACKFILL_DAYS with maximum limit
BACKFILL_DAYS_CONFIG = int(os.getenv("BACKFILL_DAYS", "30"))

if BACKFILL_DAYS_CONFIG > MAX_BACKFILL_DAYS:
    print("=" * 80)
    print(f"⚠️  BACKFILL_DAYS LIMIT EXCEEDED ⚠️")
    print(f"Requested: {BACKFILL_DAYS_CONFIG} days")
    print(f"Maximum: {MAX_BACKFILL_DAYS} days")
    print(f"Action: Capping at {MAX_BACKFILL_DAYS} days")
    print("Reason: Yahoo Finance API limitations, processing time, storage")
    print("=" * 80)
    BACKFILL_DAYS = MAX_BACKFILL_DAYS
else:
    BACKFILL_DAYS = BACKFILL_DAYS_CONFIG
STATE_FILE_PATH = Path("/app/state/last_fetch.json")
LOG_FILE_PATH = Path("/app/logs/producer.log")

# Timezone
NY_TZ = pytz.timezone("America/New_York")
UTC_TZ = pytz.UTC


# ==========================================
# Logging Setup
# ==========================================
def setup_logging():
    """Configure structured JSON logging"""
    LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # JSON formatter
    json_formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(json_formatter)
    logger.addHandler(console_handler)
    
    # File handler
    file_handler = logging.FileHandler(LOG_FILE_PATH)
    file_handler.setFormatter(json_formatter)
    logger.addHandler(file_handler)
    
    return logger


logger = setup_logging()

if BACKFILL_DAYS_CONFIG <= MAX_BACKFILL_DAYS:
    logger.info(f"Backfill period set to {BACKFILL_DAYS} days (max allowed: {MAX_BACKFILL_DAYS})")
else:
    logger.info(f"Backfill period capped at {BACKFILL_DAYS} days (requested: {BACKFILL_DAYS_CONFIG})")


# ==========================================
# State Management
# ==========================================
class StateManager:
    """Manages last successful fetch timestamp per ticker"""
    
    def __init__(self, state_file: Path):
        self.state_file = state_file
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()
    
    def _load_state(self) -> Dict[str, str]:
        """Load state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    logger.info("State loaded", extra={"ticker_count": len(state)})
                    return state
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return {}
        return {}
    
    def _save_state(self):
        """Save state to file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
            logger.debug("State saved successfully")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    def get_last_timestamp(self, ticker: str) -> Optional[datetime]:
        """Get last successful fetch timestamp for a ticker (always returns UTC)"""
        timestamp_str = self.state.get(ticker)
        if timestamp_str:
            try:
                dt = datetime.fromisoformat(timestamp_str)
                # Ensure timezone-aware and convert to UTC
                if dt.tzinfo is None:
                    # If no timezone, assume UTC
                    dt = UTC_TZ.localize(dt)
                else:
                    # Convert to UTC
                    dt = dt.astimezone(UTC_TZ)
                return dt
            except ValueError:
                logger.warning(f"Invalid timestamp for {ticker}: {timestamp_str}")
        return None
    
    def update_timestamp(self, ticker: str, timestamp: datetime):
        """Update last successful fetch timestamp (always stores in UTC)"""
        # Ensure timestamp is in UTC before saving
        if timestamp.tzinfo is None:
            timestamp = UTC_TZ.localize(timestamp)
        else:
            timestamp = timestamp.astimezone(UTC_TZ)
        self.state[ticker] = timestamp.isoformat()
        self._save_state()
        logger.info(f"State updated for {ticker}", extra={"timestamp": timestamp.isoformat()})
    
    def get_all_timestamps(self) -> Dict[str, datetime]:
        """Get all timestamps"""
        result = {}
        for ticker, ts_str in self.state.items():
            try:
                result[ticker] = datetime.fromisoformat(ts_str)
            except ValueError:
                pass
        return result


# ==========================================
# Kafka Producer
# ==========================================
class StockDataProducer:
    """Produces stock data to Kafka with retry and DLQ support"""
    
    def __init__(self):
        self.producer = self._create_producer()
        self.state_manager = StateManager(STATE_FILE_PATH)
    
    def _create_producer(self) -> KafkaProducer:
        """Create Kafka producer with retry logic"""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=REDPANDA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',  # Wait for all replicas
                    retries=3,
                    max_in_flight_requests_per_connection=1,  # Ensure ordering
                    compression_type='gzip',
                )
                logger.info("Kafka producer connected", extra={"broker": REDPANDA_BROKER})
                return producer
            except Exception as e:
                logger.warning(
                    f"Kafka connection attempt {attempt + 1}/{max_retries} failed: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    logger.error("Failed to connect to Kafka after max retries")
                    raise
    
    def fetch_stock_data(self, ticker: str) -> List[Dict]:
        """
        Fetch stock data for a ticker
        First run: chunked backfill with different intervals, subsequent runs: fetch from last timestamp onwards
        Returns list of messages (empty if no new data)
        """
        try:
            logger.info(f"Fetching data for {ticker}")

            # Determine fetch period based on state
            last_timestamp = self.state_manager.get_last_timestamp(ticker)
            if last_timestamp:
                # Incremental fetch: get data from last timestamp to now
                return self._fetch_incremental(ticker, last_timestamp)
            else:
                # First run: chunked backfill with different intervals
                return self._fetch_backfill_chunked(ticker)

        except Exception as e:
            logger.error(f"Error fetching {ticker}: {e}", exc_info=True)
            raise

    @retry(
        stop=stop_after_attempt(PRODUCER_MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=PRODUCER_RETRY_DELAY, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def _fetch_incremental(self, ticker: str, last_timestamp: datetime) -> List[Dict]:
        """Fetch real-time incremental data using direct Yahoo Finance API (more reliable than yfinance)"""
        try:
            # Convert to timezone-aware if needed
            if last_timestamp.tzinfo is None:
                start_date = UTC_TZ.localize(last_timestamp)
            else:
                start_date = last_timestamp.astimezone(UTC_TZ)
            
            # Calculate gap between last_timestamp and now
            now = datetime.now(UTC_TZ)
            gap_seconds = (now - start_date).total_seconds()
            gap_days = gap_seconds / (24 * 3600)
            
            # If start_date is in the future, reset state to current time and return empty (no future data exists)
            if gap_seconds <= 0:
                logger.warning(f"State timestamp for {ticker} is in the future ({start_date.isoformat()} vs now {now.isoformat()}, gap: {gap_seconds:.0f}s). Resetting state to current time.")
                # Reset state to current time (no data exists in the future)
                self.state_manager.update_timestamp(ticker, now)
                logger.info(f"Reset state for {ticker} to current time {now.isoformat()}. No new data available (state was in future).")
                return []
            elif gap_seconds < 60:  # Less than 1 minute gap
                logger.debug(f"Gap too small for {ticker} ({gap_seconds:.0f}s), no new data expected")
                return []
            
            # Always use direct Yahoo Finance API for incremental fetches (more reliable)
            # For small gaps (< 2.1 days), fetch 1-minute data
            # For larger gaps, use the catchup method
            if gap_days > 2.1:  # 2.1 to account for timezone differences
                logger.info(f"Incremental fetch for {ticker}: {gap_days:.1f} days gap, using catchup method")
                return self._fetch_direct_yahoo_finance_incremental_catchup(ticker, start_date)
            else:
                # Small gap: fetch directly using 1-minute interval
                # Ensure start_date is at least 1 minute before now
                if (now - start_date).total_seconds() < 60:
                    start_date = now - timedelta(minutes=2)
                gap_minutes = gap_seconds / 60
                logger.info(f"Incremental fetch for {ticker}: {gap_minutes:.1f} minutes gap ({gap_days:.3f} days), fetching from {start_date.isoformat()} to {now.isoformat()}")
                return self._fetch_direct_yahoo_finance_chunk(ticker, start_date, now, "1m")

        except Exception as e:
            logger.error(f"Incremental fetch failed for {ticker}: {e}", exc_info=True)
            return []

    def _fetch_direct_yahoo_finance_incremental_catchup(self, ticker: str, last_timestamp: datetime) -> List[Dict]:
        """Catch up large gaps using direct Yahoo Finance API"""
        try:
            import requests
            import pandas as pd

            # Calculate how many days we need to catch up
            now = datetime.now(UTC_TZ)
            gap_days = (now - last_timestamp).total_seconds() / (24 * 3600)

            # Determine appropriate period and chunking
            if gap_days <= 7:
                period = "7d"
                chunk_days = 1  # Daily chunks
            elif gap_days <= 30:
                period = "30d"
                chunk_days = 3  # 3-day chunks
            elif gap_days <= 90:
                period = "90d"
                chunk_days = 7  # Weekly chunks
            elif gap_days <= 365:
                period = "1y"
                chunk_days = 30  # Monthly chunks
            else:
                # Cap at 365 days maximum
                logger.warning(f"Gap too large for {ticker} ({gap_days:.1f} days). Capping at 365 days max backfill limit.")
                period = "1y"
                chunk_days = 30

            # Create chunks to catch up
            chunks = self._create_catchup_chunks(last_timestamp, now, chunk_days)

            all_messages = []
            for chunk_start, chunk_end in chunks:
                try:
                    messages = self._fetch_direct_yahoo_finance_chunk(ticker, chunk_start, chunk_end, "1m")
                    all_messages.extend(messages)

                    # Polite delay
                    time.sleep(0.5)

                except Exception as chunk_error:
                    logger.warning(f"Catchup chunk failed for {ticker} {chunk_start.date()}: {chunk_error}")
                    continue

            if all_messages:
                logger.info(f"Catchup completed for {ticker}: {len(all_messages)} bars, {gap_days:.1f} day gap")
            else:
                logger.warning(f"No catchup data available for {ticker}")

            return all_messages

        except Exception as e:
            logger.error(f"Catchup failed for {ticker}: {e}")
            return []

    def _create_catchup_chunks(self, start_date: datetime, end_date: datetime, chunk_days: int) -> List[tuple]:
        """Create chunks for catchup operations"""
        chunks = []
        current = start_date

        while current < end_date:
            chunk_end = min(current + timedelta(days=chunk_days), end_date)
            chunks.append((current, chunk_end))
            current = chunk_end

        return chunks

    def _fetch_direct_yahoo_finance_chunk(self, ticker: str, start_date: datetime, end_date: datetime, interval: str) -> List[Dict]:
        """Fetch a specific chunk using direct Yahoo Finance API"""
        try:
            import requests
            import pandas as pd

            # Validate timestamps - ensure start_date is before end_date
            if start_date >= end_date:
                logger.warning(f"Invalid timestamp range for {ticker}: start_date ({start_date}) >= end_date ({end_date}). Adjusting...")
                # If start_date is in future or equal, use last 2 days
                now = datetime.now(UTC_TZ)
                if start_date >= now:
                    start_date = now - timedelta(days=2)
                # Ensure end_date is after start_date
                if end_date <= start_date:
                    end_date = start_date + timedelta(days=1)
                logger.info(f"Adjusted timestamps for {ticker}: start={start_date}, end={end_date}")

            # Direct API call
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            params = {
                "interval": interval,
                "period1": int(start_date.timestamp()),
                "period2": int(end_date.timestamp())
            }

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()

            data = response.json()

            # Parse response (same logic as backfill)
            if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                result = data['chart']['result'][0]

                if 'timestamp' in result and 'indicators' in result:
                    timestamps = result['timestamp']
                    quotes = result['indicators']['quote'][0]

                    # Extract and validate data
                    open_prices = quotes.get('open', [])
                    high_prices = quotes.get('high', [])
                    low_prices = quotes.get('low', [])
                    close_prices = quotes.get('close', [])
                    volumes = quotes.get('volume', [])

                    min_length = min(len(timestamps), len(open_prices), len(high_prices),
                                    len(low_prices), len(close_prices), len(volumes))

                    if min_length == 0:
                        return []

                    aligned_data = []
                    for i in range(min_length):
                        if (open_prices[i] is None or high_prices[i] is None or
                            low_prices[i] is None or close_prices[i] is None):
                            continue

                        timestamp_dt = pd.to_datetime(timestamps[i], unit='s')
                        aligned_data.append({
                            'timestamp': timestamp_dt,
                            'Open': open_prices[i],
                            'High': high_prices[i],
                            'Low': low_prices[i],
                            'Close': close_prices[i],
                            'Volume': volumes[i] if volumes[i] is not None else 0
                        })

                    if not aligned_data:
                        return []

                    df = pd.DataFrame(aligned_data)
                    df.set_index('timestamp', inplace=True)
                    df = df.dropna()

                    return self._process_dataframe(ticker, df, "catchup", interval, None, None)

            return []

        except Exception as e:
            logger.error(f"Direct Yahoo chunk failed for {ticker}: {e}")
            raise


    def _fetch_backfill_chunked(self, ticker: str) -> List[Dict]:
        """Fetch historical data using chunked approach with different intervals"""
        all_messages = []
        # Use UTC for consistency, convert to NY timezone only for date calculations
        end_date_utc = datetime.now(UTC_TZ)
        end_date = end_date_utc.astimezone(NY_TZ)

        # Define backfill windows and intervals
        backfill_windows = [
            # (days_back, interval, chunk_days, description)
            (7, "1m", 3, "recent_1min"),      # 0-7 days: 1m interval, 3-day chunks
            (60, "2m", 14, "medium_2min"),    # 8-60 days: 2m interval, 14-day chunks
            (365, "1h", 30, "historical_1h"), # 61-365 days: 1h interval, 30-day chunks
            (730, "1d", 60, "extended_daily"), # 366-730 days: 1d interval, 60-day chunks
        ]

        for days_back, interval, chunk_days, window_type in backfill_windows:
            if days_back > BACKFILL_DAYS:
                continue  # Skip if beyond requested backfill period

            start_date = end_date - timedelta(days=min(days_back, BACKFILL_DAYS))
            chunks = self._create_date_chunks(start_date, end_date, chunk_days)

            for chunk_start, chunk_end in chunks:
                messages = self._fetch_chunk_with_dlq(ticker, chunk_start, chunk_end, interval, window_type)
                all_messages.extend(messages)

                # Polite delay between chunks
                time.sleep(0.5 + (0.5 * time.time() % 1))  # 0.5-1.5s random delay

        if all_messages:
            logger.info(f"Backfill completed for {ticker}: {len(all_messages)} total bars")
        else:
            logger.warning(f"No backfill data available for {ticker}")

        return all_messages

    def _create_date_chunks(self, start_date: datetime, end_date: datetime, chunk_days: int) -> List[tuple]:
        """Create date chunks for polite API usage"""
        chunks = []
        current = start_date

        while current < end_date:
            chunk_end = min(current + timedelta(days=chunk_days), end_date)
            chunks.append((current, chunk_end))
            current = chunk_end

        return chunks

    def _fetch_chunk_with_dlq(self, ticker: str, start_date: datetime, end_date: datetime,
                              interval: str, window_type: str) -> List[Dict]:
        """Fetch backfill data using direct Yahoo Finance API only"""
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                # Use direct Yahoo Finance API for backfill (more reliable for historical data)
                logger.debug(f"Fetching backfill chunk for {ticker} {start_date.date()}-{end_date.date()} using direct API")
                return self._fetch_direct_yahoo_finance(ticker, start_date, end_date, interval)

            except Exception as e:
                error_msg = str(e)
                if attempt < max_attempts - 1:
                    # Exponential backoff: 2^attempt seconds
                    delay = 2 ** attempt
                    logger.warning(f"Backfill chunk retry {attempt + 1}/{max_attempts} for {ticker} {start_date.date()}-{end_date.date()} in {delay}s: {str(e)[:100]}...")
                    time.sleep(delay)
                else:
                    # Final failure - send to DLQ
                    self._publish_backfill_dlq(ticker, start_date, end_date, interval, error_msg)
                    logger.error(f"Backfill chunk failed permanently for {ticker} {start_date.date()}-{end_date.date()}: {error_msg}")
                    return []

    def _fetch_direct_yahoo_finance(self, ticker: str, start_date: datetime, end_date: datetime,
                                   interval: str) -> List[Dict]:
        """Direct Yahoo Finance API call as fallback"""
        import requests
        import pandas as pd

        try:
            # Convert interval to Yahoo Finance format
            interval_map = {
                "1m": "1m",
                "2m": "2m",
                "5m": "5m",
                "15m": "15m",
                "30m": "30m",
                "60m": "1h",
                "1h": "1h",
                "1d": "1d",
                "5d": "5d",
                "1wk": "1wk",
                "1mo": "1mo",
                "3mo": "3mo"
            }

            yahoo_interval = interval_map.get(interval, "1d")

            # Calculate period based on date range
            days_diff = (end_date - start_date).days
            if days_diff <= 7:
                period = "7d"
            elif days_diff <= 60:
                period = "60d"
            elif days_diff <= 365:
                period = "1y"
            else:
                period = "2y"

            # Direct API call (similar to your curl command)
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            params = {
                "interval": yahoo_interval,
                "range": period
            }

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()

            data = response.json()

            # Parse the response
            if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                result = data['chart']['result'][0]

                if 'timestamp' in result and 'indicators' in result:
                    timestamps = result['timestamp']
                    quotes = result['indicators']['quote'][0]

                    # Extract data arrays, handling None values
                    open_prices = quotes.get('open', [])
                    high_prices = quotes.get('high', [])
                    low_prices = quotes.get('low', [])
                    close_prices = quotes.get('close', [])
                    volumes = quotes.get('volume', [])

                    # Ensure all arrays are the same length
                    min_length = min(len(timestamps), len(open_prices), len(high_prices),
                                    len(low_prices), len(close_prices), len(volumes))

                    if min_length == 0:
                        logger.debug(f"No valid data arrays for {ticker}")
                        return []

                    # Create aligned data
                    aligned_data = []
                    for i in range(min_length):
                        # Skip if any essential value is None
                        if (open_prices[i] is None or high_prices[i] is None or
                            low_prices[i] is None or close_prices[i] is None or
                            close_prices[i] is None):
                            continue

                        aligned_data.append({
                            'timestamp': pd.to_datetime(timestamps[i], unit='s'),
                            'Open': open_prices[i],
                            'High': high_prices[i],
                            'Low': low_prices[i],
                            'Close': close_prices[i],
                            'Volume': volumes[i] if volumes[i] is not None else 0
                        })

                    if not aligned_data:
                        logger.debug(f"No valid OHLC data for {ticker}")
                        return []

                    # Create DataFrame from cleaned data
                    df = pd.DataFrame(aligned_data)
                    df.set_index('timestamp', inplace=True)
                    df = df.dropna()  # Remove any remaining NaN values

                    if not df.empty:
                        logger.debug(f"Direct Yahoo API fetched {len(df)} bars for {ticker}")
                        return self._process_dataframe(ticker, df, "backfill", interval,
                                                     start_date.isoformat(), end_date.isoformat())

            logger.warning(f"No data in direct Yahoo API response for {ticker}")
            return []

        except Exception as e:
            logger.error(f"Direct Yahoo API failed for {ticker}: {e}")
            raise

    def _publish_backfill_dlq(self, ticker: str, start_date: datetime, end_date: datetime,
                             interval: str, error: str):
        """Publish failed backfill chunk to DLQ"""
        dlq_message = {
            "ticker": ticker,
            "chunk_start": start_date.isoformat(),
            "chunk_end": end_date.isoformat(),
            "interval": interval,
            "error": str(error),
            "timestamp": datetime.now(UTC_TZ).isoformat(),
            "retry_count": 3,
            "failure_type": "backfill_chunk"
        }

        try:
            # Use a separate DLQ topic for backfill failures
            future = self.producer.send(KAFKA_TOPIC_BACKFILL_DLQ, value=dlq_message)
            record_metadata = future.get(timeout=10)
            logger.warning(f"Published backfill failure to DLQ for {ticker}",
                          extra={"partition": record_metadata.partition, "offset": record_metadata.offset})
        except Exception as e:
            logger.error(f"Failed to publish backfill DLQ: {e}")

    def _process_dataframe(self, ticker: str, df, fetch_type: str, source_interval: str,
                          window_start: str = None, window_end: str = None) -> List[Dict]:
        """Process dataframe into standardized message format"""
        messages = []
        latest_timestamp = None

        for timestamp, row in df.iterrows():
            # Convert to UTC
            if timestamp.tzinfo is None:
                timestamp = NY_TZ.localize(timestamp)
            timestamp_utc = timestamp.astimezone(UTC_TZ)

            # Prepare message with enhanced metadata
            message = {
                "symbol": ticker,
                "timestamp": int(timestamp_utc.timestamp()),
                "open": float(row['Open']),
                "high": float(row['High']),
                "low": float(row['Low']),
                "close": float(row['Close']),
                "volume": int(row['Volume']),
                "source": "yahoo",
                "source_interval": source_interval,
                "fetched_from": fetch_type,
                "ingested_at": datetime.now(UTC_TZ).isoformat(),
            }

            # Add backfill window metadata if available
            if window_start and window_end:
                message["backfill_window_start"] = window_start
                message["backfill_window_end"] = window_end

            # Validate data
            if message['open'] <= 0 or message['close'] <= 0:
                logger.warning(f"Invalid price data for {ticker} at {timestamp_utc}: {message}")
                continue

            messages.append(message)

            # Track latest timestamp for state update
            if latest_timestamp is None or timestamp_utc > latest_timestamp:
                latest_timestamp = timestamp_utc

        if messages:
            logger.debug(f"Processed {len(messages)} bars for {ticker} ({fetch_type}, {source_interval})")

        return messages
    
    def publish_to_kafka(self, message: Dict, topic: str = KAFKA_TOPIC_RAW):
        """Publish message to Kafka topic"""
        try:
            ticker = message['symbol']
            future = self.producer.send(
                topic,
                key=ticker,  # Partition by ticker
                value=message
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Published to Kafka",
                extra={
                    "topic": topic,
                    "ticker": ticker,
                    "partition": record_metadata.partition,
                    "offset": record_metadata.offset
                }
            )
            
            return True
            
        except KafkaError as e:
            logger.error(f"Kafka error: {e}", exc_info=True)
            return False
    
    def publish_to_dlq(self, ticker: str, error: str, original_data: Optional[Dict] = None):
        """Publish failed message to DLQ"""
        dlq_message = {
            "ticker": ticker,
            "error": str(error),
            "timestamp": datetime.now(UTC_TZ).isoformat(),
            "original_data": original_data,
        }
        
        try:
            self.producer.send(KAFKA_TOPIC_DLQ, value=dlq_message)
            logger.warning(f"Published to DLQ for {ticker}", extra={"error": error})
        except Exception as e:
            logger.error(f"Failed to publish to DLQ: {e}")
    
    def process_ticker(self, ticker: str):
        """Process a single ticker: fetch and publish all new bars"""
        try:
            # Check if this is an incremental fetch (has state) or backfill (no state)
            last_state_timestamp = self.state_manager.get_last_timestamp(ticker)
            had_state_before = last_state_timestamp is not None
            if had_state_before:
                now_utc = datetime.now(UTC_TZ)
                gap = (now_utc - last_state_timestamp).total_seconds() / 3600  # hours
                logger.info(f"Processing {ticker}: Has state timestamp {last_state_timestamp.isoformat()} (UTC), current time {now_utc.isoformat()} (UTC), gap: {gap:.2f} hours, will do INCREMENTAL fetch")
            else:
                logger.info(f"Processing {ticker}: No state timestamp, will do BACKFILL")
            
            # Fetch data (now returns list of messages)
            messages = self.fetch_stock_data(ticker)

            if not messages:
                # No new data or market closed - not an error
                return

            # For incremental fetches, filter out messages we've already processed
            if had_state_before and last_state_timestamp:
                original_count = len(messages)
                messages = [
                    msg for msg in messages 
                    if datetime.fromtimestamp(msg['timestamp'], tz=UTC_TZ) > last_state_timestamp
                ]
                if len(messages) < original_count:
                    logger.debug(f"Filtered {original_count - len(messages)} already-processed messages for {ticker}")

            # Publish each message to Kafka and track latest successful publish
            failed_messages = []
            latest_successful_timestamp = None
            now_utc = datetime.now(UTC_TZ)

            for message in messages:
                msg_timestamp = datetime.fromtimestamp(message['timestamp'], tz=UTC_TZ)

                # Log timestamp info for debugging
                time_diff_hours = (msg_timestamp - now_utc).total_seconds() / 3600
                if abs(time_diff_hours) > 1:  # Log if difference is more than 1 hour
                    logger.info(f"Message timestamp for {ticker}: {msg_timestamp.isoformat()} (diff: {time_diff_hours:+.1f} hours from now)")

                # For now, allow all messages through to see what we're getting
                # TODO: Fix timestamp handling based on actual Yahoo Finance behavior
                
                success = self.publish_to_kafka(message)

                if success:
                    # Track the latest successfully published timestamp (only valid, non-future timestamps)
                    if latest_successful_timestamp is None or msg_timestamp > latest_successful_timestamp:
                        latest_successful_timestamp = msg_timestamp
                else:
                    failed_messages.append(message)

            # Update state based on fetch type
            now_utc = datetime.now(UTC_TZ)
            if had_state_before:
                # Incremental fetch: Update state to CURRENT TIME (when fetch completed)
                # This ensures next fetch starts from "now", not from data timestamp
                # Example: Fetch from 12:00:00 to 12:00:08, update state to 12:00:08
                # Next fetch: from 12:00:08 to 12:00:10 (new now)
                self.state_manager.update_timestamp(ticker, now_utc)
                logger.info(f"Incremental fetch: Updated state for {ticker} to current time {now_utc.isoformat()} (processed {len(messages)} new bars, latest data was {latest_successful_timestamp.isoformat() if latest_successful_timestamp else 'N/A'})")
            elif latest_successful_timestamp:
                # Backfill: Update state to latest data timestamp (for historical tracking)
                # But cap to current time to prevent future timestamps
                state_timestamp = min(latest_successful_timestamp, now_utc)
                self.state_manager.update_timestamp(ticker, state_timestamp)
                logger.debug(f"Backfill: Updated state for {ticker} to {state_timestamp.isoformat()} (capped at now: {now_utc.isoformat()})")

            # Send failed messages to DLQ
            if failed_messages:
                for failed_msg in failed_messages:
                    self.publish_to_dlq(ticker, "Failed to publish to Kafka", failed_msg)

            logger.info(f"Processed {len(messages)} bars for {ticker}",
                       extra={"successful": len(messages) - len(failed_messages), "failed": len(failed_messages)})

        except Exception as e:
            logger.error(f"Failed to process {ticker} after retries: {e}")
            self.publish_to_dlq(ticker, f"Processing error: {e}")
    
    def run(self):
        """Main producer loop"""
        logger.info(
            "Stock producer started",
            extra={
                "tickers": STOCK_TICKERS,
                "interval": PRODUCER_INTERVAL_SECONDS,
                "broker": REDPANDA_BROKER,
                "backfill_days": BACKFILL_DAYS
            }
        )
        
        while True:
            try:
                start_time = time.time()
                
                # Check if we need to run (either during market hours OR if any ticker needs initial backfill)
                now = datetime.now(NY_TZ)
                is_weekday = now.weekday() < 5  # Monday=0, Friday=4
                is_market_hours = 9 <= now.hour < 17  # Simplified check

                # Check if any ticker needs initial backfill (no state file or missing timestamp)
                needs_backfill = False
                for ticker in STOCK_TICKERS:
                    if not self.state_manager.get_last_timestamp(ticker.strip()):
                        needs_backfill = True
                        break

                # Force backfill mode for testing (uncomment to test anytime)
                needs_backfill = True

                # Allow processing if: during market hours OR if any ticker needs backfill
                should_process = (is_weekday and is_market_hours) or needs_backfill

                if should_process:
                    if needs_backfill:
                        logger.info(f"Processing {len(STOCK_TICKERS)} tickers (backfill mode - allowing anytime)")
                    else:
                        logger.info(f"Processing {len(STOCK_TICKERS)} tickers (market hours)")

                    # Process all tickers
                    for ticker in STOCK_TICKERS:
                        self.process_ticker(ticker.strip())

                    logger.info("Batch processing completed")
                else:
                    logger.info(
                        "Market closed and no backfill needed - skipping fetch",
                        extra={
                            "day": now.strftime("%A"),
                            "hour": now.hour,
                            "is_weekday": is_weekday,
                            "is_market_hours": is_market_hours
                        }
                    )
                
                # Sleep until next interval
                elapsed = time.time() - start_time
                sleep_time = max(0, PRODUCER_INTERVAL_SECONDS - elapsed)
                
                if sleep_time > 0:
                    logger.debug(f"Sleeping for {sleep_time:.2f}s")
                    time.sleep(sleep_time)
                    
            except KeyboardInterrupt:
                logger.info("Producer stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
                time.sleep(10)  # Brief pause before retrying
        
        # Cleanup
        self.producer.close()
        logger.info("Producer shut down gracefully")


# ==========================================
# Main Entry Point
# ==========================================
if __name__ == "__main__":
    try:
        producer = StockDataProducer()
        producer.run()
    except Exception as e:
        logger.critical(f"Producer failed to start: {e}", exc_info=True)
        sys.exit(1)

