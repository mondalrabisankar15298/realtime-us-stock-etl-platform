#!/usr/bin/env python3
"""
Real-Time Stock Data Producer (fixed & production-ready)
- Fetches 1-minute OHLCV data from Yahoo Finance (direct API calls)
- Publishes to Redpanda/Kafka with retries and DLQ
- Robust state handling (per-ticker UTC last-success timestamp)
- Signal-safe shutdown, small polite sleeps, and backfill control via env
"""

import json
import logging
import os
import sys
import time
import signal
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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

# Local imports for HTTP & data processing are used inside functions to keep cold start small (requests, pandas)

# ==========================================
# Configuration
# ==========================================
MAX_BACKFILL_DAYS = 365
BACKFILL_DAYS_CONFIG = int(os.getenv("BACKFILL_DAYS", "30"))
BACKFILL_DAYS = min(BACKFILL_DAYS_CONFIG, MAX_BACKFILL_DAYS)

REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "stock-raw-data")
KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "stock-dlq")
KAFKA_TOPIC_BACKFILL_DLQ = os.getenv("KAFKA_TOPIC_BACKFILL_DLQ", "stock-backfill-dlq")

STOCK_TICKERS = os.getenv(
    "STOCK_TICKERS", "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,NFLX,AMD,AVGO"
).split(",")

PRODUCER_INTERVAL_SECONDS = int(os.getenv("PRODUCER_INTERVAL_SECONDS", "60"))
PRODUCER_MAX_RETRIES = int(os.getenv("PRODUCER_MAX_RETRIES", "3"))
PRODUCER_RETRY_DELAY = int(os.getenv("PRODUCER_RETRY_DELAY", "5"))

POLITE_SLEEP_BETWEEN_TICKERS = float(os.getenv("POLITE_SLEEP_BETWEEN_TICKERS", "0.25"))

STATE_FILE_PATH = Path(os.getenv("STATE_FILE_PATH", "/app/state/last_fetch.json"))
LOG_FILE_PATH = Path(os.getenv("LOG_FILE_PATH", "/app/logs/producer.log"))

FORCE_BACKFILL = os.getenv("FORCE_BACKFILL", "false").lower() in ("1", "true", "yes")
RUN_ONCE = os.getenv("RUN_ONCE", "false").lower() in ("1", "true", "yes")

# Timezones
UTC_TZ = pytz.UTC
NY_TZ = pytz.timezone("America/New_York")

# ==========================================
# Logging Setup
# ==========================================
def setup_logging():
    LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("stock_producer")
    logger.setLevel(logging.INFO)

    json_formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(json_formatter)
    logger.addHandler(ch)

    fh = logging.FileHandler(LOG_FILE_PATH)
    fh.setFormatter(json_formatter)
    logger.addHandler(fh)

    return logger


logger = setup_logging()
logger.info("Producer configuration", extra={
    "tickers": STOCK_TICKERS,
    "interval_seconds": PRODUCER_INTERVAL_SECONDS,
    "backfill_days": BACKFILL_DAYS,
    "force_backfill": FORCE_BACKFILL,
    "run_once": RUN_ONCE
})

# ==========================================
# Graceful Shutdown
# ==========================================
SHUTDOWN = False


def _signal_handler(signum, frame):
    global SHUTDOWN
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    SHUTDOWN = True


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ==========================================
# State Management
# ==========================================
class StateManager:
    def __init__(self, state_file: Path):
        self.state_file = state_file
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state: Dict[str, str] = self._load_state()

    def _load_state(self) -> Dict[str, str]:
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    s = json.load(f)
                    logger.info("Loaded state", extra={"ticker_count": len(s)})
                    return s
            except Exception as e:
                logger.warning(f"Failed to load state file: {e}")
                return {}
        return {}

    def _save_state(self):
        try:
            with open(self.state_file, "w") as f:
                json.dump(self.state, f, indent=2)
            logger.debug("State saved")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def get_last_timestamp(self, ticker: str) -> Optional[datetime]:
        ts_str = self.state.get(ticker)
        if not ts_str:
            return None
        try:
            dt = datetime.fromisoformat(ts_str)
            # ensure tz-aware
            if dt.tzinfo is None:
                dt = UTC_TZ.localize(dt)
            else:
                dt = dt.astimezone(UTC_TZ)
            return dt
        except Exception:
            logger.warning(f"Invalid timestamp in state for {ticker}: {ts_str}")
            return None

    def update_timestamp(self, ticker: str, timestamp: datetime):
        # normalize to UTC and save ISO
        if timestamp.tzinfo is None:
            timestamp = UTC_TZ.localize(timestamp)
        else:
            timestamp = timestamp.astimezone(UTC_TZ)
        self.state[ticker] = timestamp.isoformat()
        self._save_state()
        logger.info("State updated", extra={"ticker": ticker, "timestamp": timestamp.isoformat()})

    def get_all(self) -> Dict[str, str]:
        return self.state.copy()


# ==========================================
# Stock Producer
# ==========================================
class StockDataProducer:
    def __init__(self):
        self.state_manager = StateManager(STATE_FILE_PATH)
        self.producer = self._create_producer()

    def _create_producer(self) -> KafkaProducer:
        attempts = 10
        for i in range(attempts):
            try:
                p = KafkaProducer(
                    bootstrap_servers=REDPANDA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                    compression_type="gzip",
                )
                logger.info("Connected to Kafka/Redpanda", extra={"broker": REDPANDA_BROKER})
                return p
            except Exception as e:
                logger.warning(f"Kafka connection attempt {i+1}/{attempts} failed: {e}")
                time.sleep(3)
        logger.critical("Unable to connect to Kafka after multiple attempts")
        raise RuntimeError("Kafka connection failed")

    # -----------------------------
    # Public fetch / publish helpers
    # -----------------------------
    def fetch_stock_data(self, ticker: str) -> List[Dict]:
        """
        Decide incremental vs backfill.
        Returns list of message dicts (timestamp int, open, high, low, close, volume...)
        """
        last_ts = self.state_manager.get_last_timestamp(ticker)
        if last_ts:
            return self._fetch_incremental(ticker, last_ts)
        else:
            return self._fetch_backfill_chunked(ticker)

    @retry(
        stop=stop_after_attempt(PRODUCER_MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=PRODUCER_RETRY_DELAY, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def _fetch_incremental(self, ticker: str, last_timestamp: datetime) -> List[Dict]:
        """
        Fetch newer data since last_timestamp.
        Uses direct Yahoo API for precise windows.
        """
        import requests
        import pandas as pd

        now = datetime.now(UTC_TZ)
        # Missing or future states handled
        if last_timestamp >= now:
            logger.warning("Last state >= now; resetting state to now and returning empty", extra={"ticker": ticker})
            self.state_manager.update_timestamp(ticker, now)
            return []

        gap_seconds = (now - last_timestamp).total_seconds()
        # If gap less than 60s, nothing to fetch
        if gap_seconds < 60:
            logger.debug(f"No new data for {ticker}, gap {gap_seconds:.1f}s")
            return []

        # If the gap is large, perform catchup chunks (method covers that)
        if gap_seconds > (2.1 * 24 * 3600):
            return self._fetch_direct_yahoo_finance_incremental_catchup(ticker, last_timestamp)

        # Small gap: fetch 1m data from last_timestamp -> now
        start = last_timestamp - timedelta(seconds=10)  # safety margin
        end = now
        return self._fetch_direct_yahoo_finance_chunk(ticker, start, end, "1m")

    # -----------------------------
    # Yahoo direct helpers
    # -----------------------------
    def _fetch_direct_yahoo_finance_chunk(self, ticker: str, start_date: datetime, end_date: datetime, interval: str) -> List[Dict]:
        """
        Precise chunk fetch using period1/period2 (Unix seconds). Returns processed messages.
        """
        import requests
        import pandas as pd

        # Ensure timezone-aware UTC
        if start_date.tzinfo is None:
            start_date = UTC_TZ.localize(start_date)
        else:
            start_date = start_date.astimezone(UTC_TZ)
        if end_date.tzinfo is None:
            end_date = UTC_TZ.localize(end_date)
        else:
            end_date = end_date.astimezone(UTC_TZ)

        if start_date >= end_date:
            logger.warning("Start >= end in chunk fetch; adjusting", extra={"ticker": ticker, "start": start_date.isoformat(), "end": end_date.isoformat()})
            end_date = start_date + timedelta(days=1)

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        params = {
            "interval": interval,
            "period1": int(start_date.timestamp()),
            "period2": int(end_date.timestamp()),
        }
        headers = {"User-Agent": "Mozilla/5.0"}

        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if "chart" in data and data["chart"].get("result"):
            result = data["chart"]["result"][0]
            timestamps = result.get("timestamp", [])
            indicators = result.get("indicators", {})
            quote = indicators.get("quote", [{}])[0]

            open_prices = quote.get("open", [])
            high_prices = quote.get("high", [])
            low_prices = quote.get("low", [])
            close_prices = quote.get("close", [])
            volumes = quote.get("volume", [])

            min_length = min(len(timestamps), len(open_prices), len(high_prices), len(low_prices), len(close_prices), len(volumes))
            if min_length == 0:
                return []

            aligned = []
            for i in range(min_length):
                o, h, l, c, v = open_prices[i], high_prices[i], low_prices[i], close_prices[i], volumes[i]
                if o is None or h is None or l is None or c is None:
                    continue
                # Yahoo timestamps are epoch seconds in UTC
                ts_utc = pd.to_datetime(timestamps[i], unit="s", utc=True)
                aligned.append({
                    "timestamp": ts_utc,
                    "Open": float(o),
                    "High": float(h),
                    "Low": float(l),
                    "Close": float(c),
                    "Volume": int(v) if v is not None else 0,
                })

            if not aligned:
                return []

            df = pd.DataFrame(aligned).set_index("timestamp").sort_index()
            df = df.dropna()

            return self._process_dataframe(ticker, df, "chunk", interval, start_date.isoformat(), end_date.isoformat())

        return []

    def _fetch_direct_yahoo_finance_incremental_catchup(self, ticker: str, last_timestamp: datetime) -> List[Dict]:
        """
        Break a large gap into smaller chunks (polite) and fetch with _fetch_direct_yahoo_finance_chunk
        """
        now = datetime.now(UTC_TZ)
        gap_days = (now - last_timestamp).total_seconds() / (24 * 3600)
        if gap_days <= 7:
            chunk_days = 1
        elif gap_days <= 30:
            chunk_days = 3
        elif gap_days <= 90:
            chunk_days = 7
        elif gap_days <= BACKFILL_DAYS:
            chunk_days = 30
        else:
            chunk_days = 30

        chunks = self._create_date_chunks(last_timestamp, now, chunk_days)
        messages = []
        for s, e in chunks:
            try:
                messages.extend(self._fetch_direct_yahoo_finance_chunk(ticker, s, e, "1m"))
                time.sleep(0.5)
            except Exception as exc:
                logger.warning("Catchup chunk failed", extra={"ticker": ticker, "start": s.isoformat(), "end": e.isoformat(), "error": str(exc)})
                continue

        return messages

    # -----------------------------
    # Backfill strategy (first-run)
    # -----------------------------
    def _fetch_backfill_chunked(self, ticker: str) -> List[Dict]:
        """
        Chunked historical backfill with multiple windows & intervals.
        Honours BACKFILL_DAYS cap.
        """
        all_messages = []
        now_utc = datetime.now(UTC_TZ)
        end_date = now_utc

        # windows (days_back, interval, chunk_days)
        windows = [
            (7, "1m", 3),
            (60, "2m", 14),
            (365, "1h", 30),
        ]

        for days_back, interval, chunk_days in windows:
            if days_back > BACKFILL_DAYS:
                continue
            start_date = end_date - timedelta(days=min(days_back, BACKFILL_DAYS))
            chunks = self._create_date_chunks(start_date, end_date, chunk_days)
            for s, e in chunks:
                msgs = self._fetch_chunk_with_dlq(ticker, s, e, interval)
                all_messages.extend(msgs)
                time.sleep(0.5 + (0.5 * (time.time() % 1)))

        logger.info("Backfill finished", extra={"ticker": ticker, "bars": len(all_messages)})
        return all_messages

    def _create_date_chunks(self, start: datetime, end: datetime, chunk_days: int) -> List[Tuple[datetime, datetime]]:
        chunks = []
        cur = start
        while cur < end:
            nxt = min(cur + timedelta(days=chunk_days), end)
            chunks.append((cur, nxt))
            cur = nxt
        return chunks

    def _fetch_chunk_with_dlq(self, ticker: str, start_date: datetime, end_date: datetime, interval: str) -> List[Dict]:
        attempts = 3
        for attempt in range(attempts):
            try:
                return self._fetch_direct_yahoo_finance_chunk(ticker, start_date, end_date, interval)
            except Exception as e:
                if attempt < attempts - 1:
                    delay = 2 ** attempt
                    logger.warning("Backfill chunk retry", extra={"ticker": ticker, "attempt": attempt + 1, "delay": delay})
                    time.sleep(delay)
                else:
                    self._publish_backfill_dlq(ticker, start_date, end_date, interval, str(e))
                    logger.error("Backfill chunk permanently failed", extra={"ticker": ticker, "start": start_date.isoformat(), "end": end_date.isoformat(), "error": str(e)})
                    return []

    def _publish_backfill_dlq(self, ticker: str, s: datetime, e: datetime, interval: str, error: str):
        msg = {
            "ticker": ticker,
            "chunk_start": s.isoformat(),
            "chunk_end": e.isoformat(),
            "interval": interval,
            "error": error,
            "timestamp": datetime.now(UTC_TZ).isoformat(),
        }
        try:
            f = self.producer.send(KAFKA_TOPIC_BACKFILL_DLQ, value=msg)
            md = f.get(timeout=10)
            logger.warning("Published backfill DLQ", extra={"ticker": ticker, "partition": md.partition, "offset": md.offset})
        except Exception as exc:
            logger.error("Failed to publish backfill DLQ", extra={"error": str(exc)})

    # -----------------------------
    # Data processing & publishing
    # -----------------------------
    def _process_dataframe(self, ticker: str, df, fetch_type: str, source_interval: str, window_start=None, window_end=None) -> List[Dict]:
        """
        Converts a pandas DataFrame indexed by timezone-aware UTC timestamps to message dicts.
        """
        messages = []
        latest_ts = None

        for timestamp, row in df.iterrows():
            # timestamp is timezone-aware UTC (as we used utc=True)
            ts_utc = timestamp.tz_convert(UTC_TZ) if timestamp.tzinfo else UTC_TZ.localize(timestamp)

            # basic validation
            o, h, l, c, v = row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]
            if o <= 0 or c <= 0:
                logger.warning("Skipping non-positive price", extra={"ticker": ticker, "timestamp": ts_utc.isoformat()})
                continue

            message = {
                "symbol": ticker,
                "timestamp": int(ts_utc.timestamp()),
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "volume": int(v),
                "source": "yahoo",
                "source_interval": source_interval,
                "fetched_from": fetch_type,
                "ingested_at": datetime.now(UTC_TZ).isoformat(),
            }
            if window_start and window_end:
                message["backfill_window_start"] = window_start
                message["backfill_window_end"] = window_end

            messages.append(message)
            if latest_ts is None or ts_utc > latest_ts:
                latest_ts = ts_utc

        logger.debug("Processed dataframe", extra={"ticker": ticker, "count": len(messages)})
        return messages

    def publish_to_kafka(self, message: Dict, topic: str = KAFKA_TOPIC_RAW) -> bool:
        try:
            future = self.producer.send(topic, key=message["symbol"], value=message)
            md = future.get(timeout=10)
            logger.info("Published message", extra={"topic": topic, "symbol": message["symbol"], "partition": md.partition, "offset": md.offset})
            return True
        except KafkaError as e:
            logger.error("Kafka publish error", extra={"error": str(e)})
            return False
        except Exception as e:
            logger.error("Publish unexpected error", extra={"error": str(e)})
            return False

    def publish_to_dlq(self, ticker: str, error: str, original: Optional[Dict] = None):
        msg = {"ticker": ticker, "error": error, "timestamp": datetime.now(UTC_TZ).isoformat(), "original": original}
        try:
            self.producer.send(KAFKA_TOPIC_DLQ, value=msg)
            logger.warning("Published to DLQ", extra={"ticker": ticker, "error": error})
        except Exception as e:
            logger.error("Failed to publish to DLQ", extra={"error": str(e)})

    # -----------------------------
    # Worker per ticker
    # -----------------------------
    def process_ticker(self, ticker: str):
        """
        Fetch & publish pipeline for a single ticker.
        Ensures state advances only to latest successful published message.
        """
        try:
            ticker = ticker.strip()
            has_state = self.state_manager.get_last_timestamp(ticker) is not None
            if has_state:
                logger.info("Incremental mode for ticker", extra={"ticker": ticker})
            else:
                logger.info("Backfill mode for ticker", extra={"ticker": ticker})

            messages = self.fetch_stock_data(ticker)
            if not messages:
                logger.info("No messages fetched", extra={"ticker": ticker})
                return

            # Filter messages > last_state (if incremental)
            last_state_ts = self.state_manager.get_last_timestamp(ticker)
            if last_state_ts:
                filtered = [m for m in messages if datetime.fromtimestamp(m["timestamp"], tz=UTC_TZ) > last_state_ts]
                if len(filtered) < len(messages):
                    logger.debug("Filtered already-processed messages", extra={"ticker": ticker, "filtered": len(messages) - len(filtered)})
                messages = filtered

            failed = []
            latest_success_ts = None

            for msg in messages:
                if SHUTDOWN:
                    logger.info("Shutdown requested during publishing; stopping ticker publish", extra={"ticker": ticker})
                    break

                ok = self.publish_to_kafka(msg)
                if ok:
                    ts = datetime.fromtimestamp(msg["timestamp"], tz=UTC_TZ)
                    if (latest_success_ts is None) or (ts > latest_success_ts):
                        latest_success_ts = ts
                else:
                    failed.append(msg)

            # Update state: prefer latest_success_ts; if none and we were backfilling, do not advance state
            if latest_success_ts:
                self.state_manager.update_timestamp(ticker, latest_success_ts)
            else:
                logger.warning("No successful publishes for ticker; state not advanced", extra={"ticker": ticker})

            # Send failed messages to DLQ
            for fm in failed:
                self.publish_to_dlq(ticker, "publish_failed", fm)

            logger.info("Ticker processing complete", extra={"ticker": ticker, "fetched": len(messages), "failed": len(failed)})
        except Exception as e:
            logger.exception("Failed to process ticker", extra={"ticker": ticker, "error": str(e)})
            self.publish_to_dlq(ticker, f"processing_exception: {e}")

    # -----------------------------
    # Main loop
    # -----------------------------
    def run(self):
        logger.info("Producer started main loop")
        while not SHUTDOWN:
            start = time.time()

            # Determine whether to run: market hours OR initial backfill OR forced backfill flag
            now_ny = datetime.now(NY_TZ)
            is_weekday = now_ny.weekday() < 5
            is_market_hours = 9 <= now_ny.hour < 17

            needs_backfill = any(self.state_manager.get_last_timestamp(t.strip()) is None for t in STOCK_TICKERS)

            should_run = (is_weekday and is_market_hours) or needs_backfill or FORCE_BACKFILL

            if should_run:
                mode = "backfill" if (needs_backfill or FORCE_BACKFILL) else "realtime"
                logger.info("Starting processing round", extra={"mode": mode, "tickers": len(STOCK_TICKERS)})
                for t in STOCK_TICKERS:
                    if SHUTDOWN:
                        break
                    self.process_ticker(t)
                    # polite sleep between tickers
                    time.sleep(POLITE_SLEEP_BETWEEN_TICKERS)
                logger.info("Processing round complete")
            else:
                logger.info("Skipping fetch (market closed and no backfill needed)", extra={"weekday": is_weekday, "market_hours": is_market_hours})

            # Sleep until next interval unless RUN_ONCE or SHUTDOWN
            elapsed = time.time() - start
            sleep_for = max(0, PRODUCER_INTERVAL_SECONDS - elapsed)
            if RUN_ONCE:
                logger.info("RUN_ONCE set - exiting after single round")
                break
            if SHUTDOWN:
                break
            if sleep_for > 0:
                time.sleep(sleep_for)

        # Cleanup
        try:
            self.producer.flush()
            self.producer.close()
        except Exception:
            pass
        logger.info("Producer shut down gracefully")


# ==========================================
# Main
# ==========================================
def main():
    try:
        p = StockDataProducer()
        p.run()
    except Exception as e:
        logger.critical("Producer failed to start", extra={"error": str(e)})
        sys.exit(1)


if __name__ == "__main__":
    main()
