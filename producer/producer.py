#!/usr/bin/env python3
"""
Real-Time Stock Data Producer (fixed & production-ready)
- Fetches 1-minute OHLCV data from Yahoo Finance (direct API calls)
- Publishes to Redpanda/Kafka with retries and DLQ
- Robust per-ticker state handling (last-success timestamp + backfill_done flag)
- Signal-safe shutdown, polite sleeps, and backfill control via env
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
# State Management (per-ticker dict with last_timestamp & backfill_done)
# Example state structure:
# {
#   "AAPL": {"last_timestamp": "2025-11-30T14:59:00+00:00", "backfill_done": true},
#   "MSFT": {"last_timestamp": "2025-11-30T14:59:00+00:00", "backfill_done": false}
# }
# ==========================================
class StateManager:
    def __init__(self, state_file: Path):
        self.state_file = state_file
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state: Dict[str, Dict] = self._load_state()

    def _load_state(self) -> Dict[str, Dict]:
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    s = json.load(f)
                    # Backwards compatibility: if value is string -> convert to dict
                    normalized = {}
                    for k, v in s.items():
                        if isinstance(v, str):
                            normalized[k] = {"last_timestamp": v, "backfill_done": False}
                        elif isinstance(v, dict):
                            # ensure keys exist
                            normalized[k] = {
                                "last_timestamp": v.get("last_timestamp"),
                                "backfill_done": bool(v.get("backfill_done", False)),
                            }
                        else:
                            normalized[k] = {"last_timestamp": None, "backfill_done": False}
                    logger.info("Loaded state", extra={"ticker_count": len(normalized)})
                    return normalized
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
        entry = self.state.get(ticker)
        if not entry:
            return None
        ts_str = entry.get("last_timestamp")
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
        entry = self.state.get(ticker, {"last_timestamp": None, "backfill_done": False})
        entry["last_timestamp"] = timestamp.isoformat()
        self.state[ticker] = entry
        self._save_state()
        logger.info("State updated", extra={"ticker": ticker, "timestamp": timestamp.isoformat()})

    def mark_backfill_done(self, ticker: str, done: bool = True):
        entry = self.state.get(ticker, {"last_timestamp": None, "backfill_done": False})
        entry["backfill_done"] = bool(done)
        self.state[ticker] = entry
        self._save_state()
        logger.info("Backfill flag updated", extra={"ticker": ticker, "backfill_done": entry["backfill_done"]})

    def backfill_done(self, ticker: str) -> bool:
        entry = self.state.get(ticker)
        return bool(entry and entry.get("backfill_done", False))

    def get_all(self) -> Dict[str, Dict]:
        return {k: v.copy() for k, v in self.state.items()}


# ==========================================
# Stock Producer
# ==========================================
class StockDataProducer:
    def __init__(self):
        self.state_manager = StateManager(STATE_FILE_PATH)
        self.producer = self._create_producer()
        # Keep a mutable one-shot override so we can control if necessary within instance
        self._force_backfill_env = FORCE_BACKFILL

        # --- ADDED: session guard so each ticker backfill is attempted only once per running process
        self._session_backfilled = set()

        # --- ADDED: if FORCE_BACKFILL true at startup, reset persisted backfill_done=False for all tickers
        # so the process will attempt backfill for each ticker once.
        if self._force_backfill_env:
            try:
                logger.info("FORCE_BACKFILL true at startup - resetting persisted backfill_done for all tickers", extra={"tickers": STOCK_TICKERS})
                for t in STOCK_TICKERS:
                    self.state_manager.mark_backfill_done(t.strip(), False)
            except Exception as e:
                logger.warning(f"Failed to reset persisted backfill flags at startup: {e}")

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
    # Interval chooser (moved into producer so self._choose_interval_for_range exists)
    # -----------------------------
    def _choose_interval_for_range(self, days: float) -> str:
        """
        Pick a Yahoo-friendly interval for a requested range length in days.
        The mapping is empirical — adjusts which interval to try first.
        Returns interval string like "1m", "2m", "5m", "15m", "1h", "1d".
        """
        # Conservative mapping based on Yahoo tolerance (empirical)
        if days <= 7:
            return "1m"
        if days <= 30:
            # 2m sometimes flaky for large-ish ranges; prefer 5m for stability for ~30 days
            return "5m"
        if days <= 90:
            # 5m or 15m can work; choose 15m for ~month+ windows to reduce size
            return "15m"
        if days <= 365:
            return "1h"
        return "1d"

    # -----------------------------
    # Public fetch / publish helpers
    # -----------------------------
    def fetch_stock_data(self, ticker: str) -> Tuple[List[Dict], bool]:
        """
        Decide incremental vs backfill for this ticker.
        Returns (messages, performed_backfill_bool)
        performed_backfill_bool indicates whether this call executed backfill for the ticker.
        """
        last_ts = self.state_manager.get_last_timestamp(ticker)
        backfill_done_flag = self.state_manager.backfill_done(ticker)

        # --- ADDED: if we've already attempted backfill for this ticker in this session, skip re-attempt
        if ticker in self._session_backfilled:
            logger.debug("Skipping backfill in this session (already attempted)", extra={"ticker": ticker})
            # if we have a last timestamp, proceed with incremental; otherwise return empty (avoid repeated backfill)
            if last_ts:
                return self._fetch_incremental(ticker, last_ts), False
            else:
                return [], False

        # CASES per-ticker:
        # 1) FORCE_BACKFILL (env True) and backfill_done is False -> perform backfill (regardless of last_ts)
        # 2) FORCE_BACKFILL False and last_ts is None -> perform backfill
        # 3) else -> incremental
        if self._force_backfill_env and not backfill_done_flag:
            logger.info("FORCE_BACKFILL active for ticker (will backfill once)", extra={"ticker": ticker})
            # mark attempted for this session so we don't repeat within same process
            self._session_backfilled.add(ticker)
            msgs = self._fetch_backfill_chunked(ticker)
            return msgs, True
        if not self._force_backfill_env and last_ts is None:
            logger.info("No state found for ticker (will backfill)", extra={"ticker": ticker})
            # mark attempted for this session so we don't repeat within same process
            self._session_backfilled.add(ticker)
            msgs = self._fetch_backfill_chunked(ticker)
            return msgs, True
        # Otherwise use incremental fetch based on last_ts
        return self._fetch_incremental(ticker, last_ts), False

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
        if last_timestamp is None:
            # defensive - treat as tiny backfill (shouldn't get here)
            logger.warning("Incremental called with no last_timestamp; performing short backfill", extra={"ticker": ticker})
            start = now - timedelta(minutes=10)
            return self._fetch_direct_yahoo_finance_chunk(ticker, start, now, "1m")

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
    # Yahoo direct helpers (unchanged)
    # -----------------------------
    def _fetch_direct_yahoo_finance_chunk(self, ticker: str, start_date: datetime, end_date: datetime, interval: str) -> List[Dict]:
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

        resp = requests.get(url, params=params, headers=headers, timeout=15)
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
    # Backfill strategy (first-run / per-ticker)
    # -----------------------------
    def _fetch_backfill_chunked(self, ticker: str) -> List[Dict]:
        all_messages = []
        now_utc = datetime.now(UTC_TZ)
        end_date = now_utc

        # windows (days_back, interval, chunk_days)
        windows = [
            (7, "1m", 3),
            (30, "2m", 7),
            (BACKFILL_DAYS, "5m", 30),
            # (BACKFILL_DAYS, "1m", 1),
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
            # for s, e in chunks:
            #     logger.info("Backfill chunk start", extra={
            #         "ticker": ticker,
            #         "start": s.isoformat(),
            #         "end": e.isoformat(),
            #         "interval": interval,
            #         "range_days": (e - s).total_seconds() / (24*3600)
            #     })
            #     msgs = self._fetch_chunk_with_dlq(ticker, s, e, interval)
            #     logger.info("Backfill chunk done", extra={
            #         "ticker": ticker,
            #         "start": s.isoformat(),
            #         "end": e.isoformat(),
            #         "interval": interval,
            #         "bars": len(msgs)
            #     })
            #     all_messages.extend(msgs)
            #     time.sleep(0.5 + (0.5 * (time.time() % 1)))

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
        """
        Try fetching the chunk; choose a more likely-to-succeed interval up-front based on range.
        Keeps fallback and DLQ behaviour.
        """
        import requests

        # compute range in days
        range_days = (end_date - start_date).total_seconds() / (24 * 3600)

        # choose a better-first interval based on requested range
        chosen_interval = self._choose_interval_for_range(range_days)

        # If the caller explicitly requested a very fine interval and range_days is small,
        # keep the caller interval; otherwise prefer chosen_interval.
        # (This preserves backward compatibility: caller can still override by passing interval.)
        try_first_interval = interval if (interval in ("1m","2m","5m","15m","30m","1h","1d") and
                                          ( (interval == "1m" and range_days <= 7) or
                                            (interval == "2m" and range_days <= 60) or
                                            (interval == "5m" and range_days <= 60) or
                                            (interval == "15m" and range_days <= 180) or
                                            (interval == "1h" and range_days <= 365) or
                                            (interval == "1d")
                                          )
                                         ) else chosen_interval

        # Now try fetch with that first-good-interval, then fallbacks (preserve original logic)
        attempts = 3
        try:
            for attempt in range(attempts):
                try:
                    return self._fetch_direct_yahoo_finance_chunk(ticker, start_date, end_date, try_first_interval)
                except requests.exceptions.HTTPError as e:
                    status = getattr(e.response, "status_code", None)
                    if status == 422:
                        # break to fallback logic below
                        break
                    if attempt < attempts - 1:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        self._publish_backfill_dlq(ticker, start_date, end_date, try_first_interval, str(e))
                        return []
                except Exception as e:
                    if attempt < attempts - 1:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        self._publish_backfill_dlq(ticker, start_date, end_date, try_first_interval, str(e))
                        return []
        except Exception:
            # defensive
            pass

        # fallback intervals (coarser) to try after first attempt fails
        fallback_intervals = ["2m", "5m", "15m", "1h", "1d"]
        # ensure chosen interval isn't tried twice
        if try_first_interval in fallback_intervals:
            fallback_order = [iv for iv in fallback_intervals if iv != try_first_interval]
        else:
            fallback_order = [iv for iv in fallback_intervals]

        # Try fallbacks
        for iv in fallback_order:
            try:
                return self._fetch_direct_yahoo_finance_chunk(ticker, start_date, end_date, iv)
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 422:
                    continue
                else:
                    continue
            except Exception:
                continue

        # As last resort, split into 1-day sub-chunks and try again (coarser intervals)
        sub_messages = []
        cur = start_date
        sub_chunk_days = 1
        while cur < end_date:
            nxt = min(cur + timedelta(days=sub_chunk_days), end_date)
            got = []
            tried_ok = False
            # try original-chosen and fallbacks for tiny chunk
            for iv in [try_first_interval] + fallback_order:
                try:
                    got = self._fetch_direct_yahoo_finance_chunk(ticker, cur, nxt, iv)
                    if got:
                        sub_messages.extend(got)
                    tried_ok = True
                    break
                except Exception:
                    continue
            if not tried_ok:
                # unable to fetch this tiny sub-chunk -> DLQ and continue
                self._publish_backfill_dlq(ticker, cur, nxt, try_first_interval, "failed all fallbacks for tiny chunk")
            cur = nxt

        return sub_messages

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
        messages = []
        latest_ts = None

        for timestamp, row in df.iterrows():
            ts_utc = timestamp.tz_convert(UTC_TZ) if timestamp.tzinfo else UTC_TZ.localize(timestamp)

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
            # Decide per-ticker backfill/incremental and whether we performed backfill
            messages, performed_backfill = self.fetch_stock_data(ticker)

            if not messages:
                logger.info("No messages fetched", extra={"ticker": ticker, "performed_backfill": performed_backfill})
                # If we performed backfill but got nothing, don't mark backfill_done; will retry next round
                return

            # Only filter messages > last_state when this is an incremental run.
            # If we are performing backfill (performed_backfill == True) we intentionally ignore persisted last_state,
            # because the user requested FORCE_BACKFILL (or initial missing-state backfill) to fetch historical data
            # even if last_timestamp exists.
            last_state_ts = self.state_manager.get_last_timestamp(ticker)
            if not performed_backfill and last_state_ts:
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
                # If we performed backfill (either forced or initial), mark this ticker as done
                if performed_backfill:
                    self.state_manager.mark_backfill_done(ticker, True)
            else:
                logger.warning("No successful publishes for ticker; state not advanced", extra={"ticker": ticker})

            # Send failed messages to DLQ
            for fm in failed:
                self.publish_to_dlq(ticker, "publish_failed", fm)

            logger.info("Ticker processing complete", extra={"ticker": ticker, "fetched": len(messages), "failed": len(failed), "performed_backfill": performed_backfill})
        except Exception as e:
            logger.exception("Failed to process ticker", extra={"ticker": ticker, "error": str(e)})
            self.publish_to_dlq(ticker, f"processing_exception: {e}")


    # -----------------------------
    # Main loop
    # -----------------------------
    def run(self):
        logger.info("Producer started main loop")
        first_round = True
        while not SHUTDOWN:
            start = time.time()

            now_ny = datetime.now(NY_TZ)
            is_weekday = now_ny.weekday() < 5
            is_market_hours = 9 <= now_ny.hour < 17

            # Determine if any ticker lacks state/backfill_done (per-ticker)
            needs_backfill = any(
                (not self.state_manager.get_last_timestamp(t.strip())) for t in STOCK_TICKERS
            )

            # We run if market is open OR there are tickers needing backfill OR the env force flag is set
            should_run = (is_weekday and is_market_hours) or needs_backfill or self._force_backfill_env

            # Mode is for logging only; actual per-ticker behavior is per-state & performed_backfill
            mode = "backfill" if (needs_backfill or self._force_backfill_env) else "realtime"
            if should_run:
                logger.info("Starting processing round", extra={"mode": mode, "tickers": len(STOCK_TICKERS)})
                for t in STOCK_TICKERS:
                    if SHUTDOWN:
                        break
                    self.process_ticker(t)
                    # polite sleep between tickers
                    time.sleep(POLITE_SLEEP_BETWEEN_TICKERS)
                logger.info("Processing round complete")
                # If FORCE_BACKFILL env was set and we want it to apply only once per-instance, disable it after first round.
                # User asked for behavior: if FORCE_BACKFILL true, do backfill once then incremental.
                # We respect that by turning off the instance flag after the first full round where tickers had
                # an opportunity to perform backfill. This prevents repeated backfill across rounds within the same container.
                if self._force_backfill_env and first_round:
                    logger.info("Disabling instance FORCE_BACKFILL after first round (backfills performed per-ticker)", extra={})
                    self._force_backfill_env = False
            else:
                logger.info("Skipping fetch (market closed and no backfill needed)", extra={"weekday": is_weekday, "market_hours": is_market_hours})

            elapsed = time.time() - start
            sleep_for = max(0, PRODUCER_INTERVAL_SECONDS - elapsed)
            if RUN_ONCE:
                logger.info("RUN_ONCE set - exiting after single round")
                break
            if SHUTDOWN:
                break
            if sleep_for > 0:
                time.sleep(sleep_for)

            first_round = False

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
