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
import yfinance as yf
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
REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "stock-raw-data")
KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "stock-dlq")
STOCK_TICKERS = os.getenv("STOCK_TICKERS", "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,NFLX,AMD,AVGO").split(",")
PRODUCER_INTERVAL_SECONDS = int(os.getenv("PRODUCER_INTERVAL_SECONDS", "60"))
PRODUCER_MAX_RETRIES = int(os.getenv("PRODUCER_MAX_RETRIES", "3"))
PRODUCER_RETRY_DELAY = int(os.getenv("PRODUCER_RETRY_DELAY", "5"))
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
        """Get last successful fetch timestamp for a ticker"""
        timestamp_str = self.state.get(ticker)
        if timestamp_str:
            try:
                return datetime.fromisoformat(timestamp_str)
            except ValueError:
                logger.warning(f"Invalid timestamp for {ticker}: {timestamp_str}")
        return None
    
    def update_timestamp(self, ticker: str, timestamp: datetime):
        """Update last successful fetch timestamp"""
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
    
    @retry(
        stop=stop_after_attempt(PRODUCER_MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=PRODUCER_RETRY_DELAY, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def fetch_stock_data(self, ticker: str) -> Optional[Dict]:
        """
        Fetch latest 1-minute data for a ticker with retry
        Returns None if market is closed or data unavailable
        """
        try:
            logger.info(f"Fetching data for {ticker}")
            
            # Fetch 1-day of 1-minute data (to get latest bar)
            stock = yf.Ticker(ticker)
            df = stock.history(period="1d", interval="1m")
            
            if df.empty:
                logger.warning(f"No data available for {ticker} (market may be closed)")
                return None
            
            # Get the most recent bar
            latest = df.iloc[-1]
            timestamp = df.index[-1]
            
            # Convert to UTC
            if timestamp.tzinfo is None:
                timestamp = NY_TZ.localize(timestamp)
            timestamp_utc = timestamp.astimezone(UTC_TZ)
            
            # Check if this is new data
            last_timestamp = self.state_manager.get_last_timestamp(ticker)
            if last_timestamp and timestamp_utc <= last_timestamp:
                logger.debug(
                    f"No new data for {ticker}",
                    extra={
                        "current": timestamp_utc.isoformat(),
                        "last": last_timestamp.isoformat()
                    }
                )
                return None
            
            # Prepare message
            message = {
                "symbol": ticker,
                "timestamp": int(timestamp_utc.timestamp()),
                "open": float(latest['Open']),
                "high": float(latest['High']),
                "low": float(latest['Low']),
                "close": float(latest['Close']),
                "volume": int(latest['Volume']),
                "ingested_at": datetime.now(UTC_TZ).isoformat(),
            }
            
            # Validate data
            if message['open'] <= 0 or message['close'] <= 0:
                logger.warning(f"Invalid price data for {ticker}: {message}")
                return None
            
            logger.info(
                f"Fetched data for {ticker}",
                extra={
                    "timestamp": timestamp_utc.isoformat(),
                    "close": message['close'],
                    "volume": message['volume']
                }
            )
            
            return message
            
        except Exception as e:
            logger.error(f"Error fetching {ticker}: {e}", exc_info=True)
            raise
    
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
            
            # Update state only after successful publish
            if topic == KAFKA_TOPIC_RAW:
                timestamp = datetime.fromtimestamp(message['timestamp'], tz=UTC_TZ)
                self.state_manager.update_timestamp(ticker, timestamp)
            
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
        """Process a single ticker: fetch and publish"""
        try:
            # Fetch data
            data = self.fetch_stock_data(ticker)
            
            if data is None:
                # No new data or market closed - not an error
                return
            
            # Publish to Kafka
            success = self.publish_to_kafka(data)
            
            if not success:
                self.publish_to_dlq(ticker, "Failed to publish to Kafka", data)
                
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
                "broker": REDPANDA_BROKER
            }
        )
        
        while True:
            try:
                start_time = time.time()
                
                # Check if market is open (simple check: weekday during EST hours)
                now = datetime.now(NY_TZ)
                is_weekday = now.weekday() < 5  # Monday=0, Friday=4
                is_market_hours = 9 <= now.hour < 17  # Simplified check
                
                if is_weekday and is_market_hours:
                    logger.info(f"Processing {len(STOCK_TICKERS)} tickers")
                    
                    # Process all tickers
                    for ticker in STOCK_TICKERS:
                        self.process_ticker(ticker.strip())
                    
                    logger.info("Batch processing completed")
                else:
                    logger.info(
                        "Market closed - skipping fetch",
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

