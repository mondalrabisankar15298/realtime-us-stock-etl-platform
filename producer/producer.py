#!/usr/bin/env python3
"""
Real-Time Stock Data Producer (no Kafka reset)
- Fetches OHLCV from Yahoo Finance
- Publishes to Redpanda/Kafka with retries and DLQ
- Per-ticker state: last_timestamp + backfill_done
- Watermarks persisted to a separate file (watermarks.json)
- If FORCE_BACKFILL true at startup: reset persisted backfill flags and delete watermark file.
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
WATERMARK_FILE_PATH = Path(os.getenv("WATERMARK_FILE_PATH", "/app/state/watermarks.json"))
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
    "run_once": RUN_ONCE,
    "state_file": str(STATE_FILE_PATH),
    "watermark_file": str(WATERMARK_FILE_PATH),
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
                    normalized = {}
                    for k, v in s.items():
                        if isinstance(v, str):
                            normalized[k] = {"last_timestamp": v, "backfill_done": False}
                        elif isinstance(v, dict):
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
            if dt.tzinfo is None:
                dt = UTC_TZ.localize(dt)
            else:
                dt = dt.astimezone(UTC_TZ)
            return dt
        except Exception:
            logger.warning(f"Invalid timestamp in state for {ticker}: {ts_str}")
            return None

    def update_timestamp(self, ticker: str, timestamp: datetime):
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
# Watermark Manager (separate persisted file)
# ==========================================
class WatermarkManager:
    def __init__(self, watermark_file: Path):
        self.watermark_file = watermark_file
        self.watermark_file.parent.mkdir(parents=True, exist_ok=True)
        self.watermarks = self._load()

    def _load(self) -> Dict[str, Optional[str]]:
        if self.watermark_file.exists():
            try:
                with open(self.watermark_file, "r") as f:
                    data = json.load(f)
                    logger.info("Loaded watermark file", extra={"count": len(data)})
                    return data
            except Exception as e:
                logger.warning(f"Failed to load watermark file: {e}")
                return {}
        return {}

    def _save(self):
        try:
            with open(self.watermark_file, "w") as f:
                json.dump(self.watermarks, f, indent=2)
            logger.debug("Watermark file saved")
        except Exception as e:
            logger.error(f"Failed to save watermark file: {e}")

    def update(self, ticker: str, ts: datetime):
        if ts.tzinfo is None:
            ts = UTC_TZ.localize(ts)
        else:
            ts = ts.astimezone(UTC_TZ)
        self.watermarks[ticker] = ts.isoformat()
        self._save()
        logger.info("Watermark updated", extra={"ticker": ticker, "watermark": ts.isoformat()})

    def delete_file(self):
        try:
            if self.watermark_file.exists():
                self.watermark_file.unlink()
                self.watermarks = {}
                logger.info("Watermark file deleted due to FORCE_BACKFILL")
        except Exception as e:
            logger.warning(f"Failed to delete watermark file: {e}")


# ==========================================
# Stock Producer
# ==========================================
class StockDataProducer:
    def __init__(self):
        self.state_manager = StateManager(STATE_FILE_PATH)
        self.watermark_manager = WatermarkManager(WATERMARK_FILE_PATH)
        self.producer = self._create_producer()
        self._force_backfill_env = FORCE_BACKFILL
        self._session_backfilled = set()

        # If FORCE_BACKFILL true at startup: reset persisted backfill_done flags (so backfill runs per-ticker)
        # Note: Comprehensive cleanup (Kafka, Delta tables, checkpoints, TimescaleDB, Airflow) is now handled
        # by the cleanup-init container before producer starts.
        if self._force_backfill_env:
            try:
                logger.info("FORCE_BACKFILL true at startup - resetting backfill flags")
                for t in STOCK_TICKERS:
                    self.state_manager.mark_backfill_done(t.strip(), False)
            except Exception as e:
                logger.warning(f"Failed to reset backfill flags: {e}")

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

    def _choose_interval_for_range(self, days: float) -> str:
        if days <= 7:
            return "1m"
        if days <= 30:
            return "5m"
        if days <= 90:
            return "15m"
        if days <= 365:
            return "1h"
        return "1d"

    def fetch_stock_data(self, ticker: str) -> Tuple[List[Dict], bool]:
        last_ts = self.state_manager.get_last_timestamp(ticker)
        backfill_done_flag = self.state_manager.backfill_done(ticker)

        if ticker in self._session_backfilled:
            logger.debug("Skipping backfill in this session (already attempted)", extra={"ticker": ticker})
            if last_ts:
                return self._fetch_incremental(ticker, last_ts), False
            else:
                return [], False

        if self._force_backfill_env and not backfill_done_flag:
            logger.info("FORCE_BACKFILL active for ticker (will backfill once)", extra={"ticker": ticker})
            self._session_backfilled.add(ticker)
            msgs = self._fetch_backfill_chunked(ticker)
            return msgs, True
        if not self._force_backfill_env and last_ts is None:
            logger.info("No state found for ticker (will backfill)", extra={"ticker": ticker})
            self._session_backfilled.add(ticker)
            msgs = self._fetch_backfill_chunked(ticker)
            return msgs, True
        return self._fetch_incremental(ticker, last_ts), False

    @retry(
        stop=stop_after_attempt(PRODUCER_MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=PRODUCER_RETRY_DELAY, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def _fetch_incremental(self, ticker: str, last_timestamp: datetime) -> List[Dict]:
        import requests
        import pandas as pd

        now = datetime.now(UTC_TZ)
        if last_timestamp is None:
            logger.warning("Incremental called with no last_timestamp; performing short backfill", extra={"ticker": ticker})
            start = now - timedelta(minutes=10)
            return self._fetch_direct_yahoo_finance_chunk(ticker, start, now, "1m")

        if last_timestamp >= now:
            logger.warning("Last state >= now; resetting state to now and returning empty", extra={"ticker": ticker})
            self.state_manager.update_timestamp(ticker, now)
            return []

        gap_seconds = (now - last_timestamp).total_seconds()
        if gap_seconds < 60:
            logger.debug(f"No new data for {ticker}, gap {gap_seconds:.1f}s")
            return []

        if gap_seconds > (2.1 * 24 * 3600):
            return self._fetch_direct_yahoo_finance_incremental_catchup(ticker, last_timestamp)

        # Fetch a larger window (e.g. 15 mins) to ensure we get valid volume data
        # Yahoo sometimes returns partial/zero data if the requested window is too narrow.
        start = now - timedelta(minutes=15)
        end = now
        return self._fetch_direct_yahoo_finance_chunk(ticker, start, end, "1m")

    def _fetch_daily_adjusted_closes(self, ticker: str, start_date: datetime, end_date: datetime) -> Dict[str, float]:
        """
        Fetches 1d data to get the definitive ANSWER KEY for what the price should be.
        Returns a dict mapping 'YYYY-MM-DD' -> Adjusted Close Price (float).
        """
        import requests
        
        # Buffer the date range slightly
        adj_start = start_date - timedelta(days=5)
        adj_end = end_date + timedelta(days=5)
        
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        params = {
            "interval": "1d",
            "period1": int(adj_start.timestamp()),
            "period2": int(adj_end.timestamp()),
            "includeAdjustedClose": "true",
        }
        headers = {"User-Agent": "Mozilla/5.0"}
        
        daily_prices = {}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            resp.raise_for_status()
            
            data = resp.json()
            if "chart" not in data or not data["chart"].get("result"):
                if "chart" in data and data["chart"].get("error"):
                     error_code = data["chart"]["error"].get("code")
                     logger.warning(f"Yahoo API returned error for daily prices: {error_code}")
                     raise ValueError(f"Yahoo API error: {error_code}")
                return {}
                
            result = data["chart"]["result"][0]
            timestamps = result.get("timestamp", [])
            indicators = result.get("indicators", {})
            adj_close_obj = indicators.get("adjclose", [{}])[0]
            adj_closes = adj_close_obj.get("adjclose", [])
            
            for i, ts in enumerate(timestamps):
                if i >= len(adj_closes): 
                    break
                
                ac = adj_closes[i]
                if ac is None:
                    continue
                    
                dt_str = datetime.fromtimestamp(ts, UTC_TZ).date().isoformat()
                daily_prices[dt_str] = float(ac)
                    
        except Exception as e:
            logger.error(f"CRITICAL: Failed to fetch daily reference prices for {ticker}: {e}")
            raise 
            
        return daily_prices

    def _align_timestamp(self, ts: int, interval: str) -> int:
        """
        Aligns a unix timestamp to the start of the interval bucket.
        Useful because Yahoo sometimes returns the 'last trade time' for the most recent bar
        instead of the bucket start time, causing misalignment and flat candles in the chart.
        """
        if not interval:
            return ts
        
        # Parse interval (e.g. "1m", "5m", "1h", "1d")
        # Only align minute-based intervals to avoid messing up daily/weekly timings
        try:
            if interval.endswith("m"):
                minutes = int(interval[:-1])
                seconds = minutes * 60
                return ts - (ts % seconds)
            elif interval.endswith("h"):
                hours = int(interval[:-1])
                seconds = hours * 3600
                return ts - (ts % seconds)
        except Exception:
            pass
            
        return ts

    def _fetch_direct_yahoo_finance_chunk(self, ticker: str, start_date: datetime, end_date: datetime, interval: str) -> List[Dict]:
        import requests
        import pandas as pd

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

        # 1. Fetch Daily Reference Prices (The "Answer Key")
        is_intraday = interval.endswith("m") or interval.endswith("h")
        daily_targets = {}
        if is_intraday:
            daily_targets = self._fetch_daily_adjusted_closes(ticker, start_date, end_date)

        # 2. Fetch the actual chunk
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        params = {
            "interval": interval,
            "period1": int(start_date.timestamp()),
            "period2": int(end_date.timestamp()),
            "includeAdjustedClose": "true",
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
                ts = timestamps[i]
                
                # Align timestamp to interval (fix for live ticks)
                ts = self._align_timestamp(ts, interval)
                
                o = open_prices[i]
                h = high_prices[i]
                l = low_prices[i]
                c = close_prices[i]
                v = volumes[i]
                
                if o is None or h is None or l is None or c is None:
                    continue

                ts_utc = datetime.fromtimestamp(ts, UTC_TZ)
                
                # NEW ADJUSTMENT LOGIC: Compare Intraday Close to Daily Target
                ratio = 1.0
                if is_intraday:
                    dt_str = datetime.fromtimestamp(ts, UTC_TZ).date().isoformat()
                    if dt_str in daily_targets:
                        target_price = daily_targets[dt_str]
                        # Calculate ratio: target / current
                        if c != 0:
                            implied_ratio = target_price / c
                            
                            # Only apply if diff > 10% (avoid micro-adjustment of intraday vs daily close noise)
                            if abs(implied_ratio - 1.0) > 0.1:
                                ratio = implied_ratio

                if abs(ratio - 1.0) > 0.0001:
                    o = o * ratio
                    h = h * ratio
                    l = l * ratio
                    c = c * ratio # Use calculated ratio on Close as well
                    if v is not None:
                        v = int(float(v) / ratio)

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

    def _fetch_backfill_chunked(self, ticker: str) -> List[Dict]:
        all_messages = []
        now_utc = datetime.now(UTC_TZ)
        end_date = now_utc

        windows = [
            (7, "1m", 3),
            (30, "2m", 7),
            (BACKFILL_DAYS, "5m", 30),
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
        import requests

        range_days = (end_date - start_date).total_seconds() / (24 * 3600)
        chosen_interval = self._choose_interval_for_range(range_days)

        try_first_interval = interval if (interval in ("1m","2m","5m","15m","30m","1h","1d") and
                                          ( (interval == "1m" and range_days <= 7) or
                                            (interval == "2m" and range_days <= 60) or
                                            (interval == "5m" and range_days <= 60) or
                                            (interval == "15m" and range_days <= 180) or
                                            (interval == "1h" and range_days <= 365) or
                                            (interval == "1d")
                                          )
                                         ) else chosen_interval

        attempts = 3
        try:
            for attempt in range(attempts):
                try:
                    res = self._fetch_direct_yahoo_finance_chunk(ticker, start_date, end_date, try_first_interval)
                    if res:
                        return res
                    # If empty result, break retry loop to try fallbacks
                    # (Unless it's a 422 which is handled below, but empty list usually means valid request but no data for this interval)
                    break
                except requests.exceptions.HTTPError as e:
                    status = getattr(e.response, "status_code", None)
                    if status == 422:
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
            pass

        fallback_intervals = ["2m", "5m", "15m", "1h", "1d"]
        if try_first_interval in fallback_intervals:
            fallback_order = [iv for iv in fallback_intervals if iv != try_first_interval]
        else:
            fallback_order = [iv for iv in fallback_intervals]

        for iv in fallback_order:
            try:
                res = self._fetch_direct_yahoo_finance_chunk(ticker, start_date, end_date, iv)
                if res:
                    return res
                # If empty, continue to next fallback
                continue
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 422:
                    continue
                else:
                    continue
            except Exception:
                continue

        sub_messages = []
        cur = start_date
        sub_chunk_days = 1
        while cur < end_date:
            nxt = min(cur + timedelta(days=sub_chunk_days), end_date)
            got = []
            tried_ok = False
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

    def process_ticker(self, ticker: str):
        try:
            ticker = ticker.strip()
            messages, performed_backfill = self.fetch_stock_data(ticker)

            if not messages:
                logger.info("No messages fetched", extra={"ticker": ticker, "performed_backfill": performed_backfill})
                return

            last_state_ts = self.state_manager.get_last_timestamp(ticker)
            if not performed_backfill and last_state_ts:
                # Relax filter to allow re-emitting recent data (e.g. last 5 mins)
                # This ensures that if Volume arrives late (after price), we capture the update.
                # Downstream Spark Bronze/Silver jobs handle deduplication/merging.
                cutoff = last_state_ts - timedelta(minutes=5)
                filtered = [m for m in messages if datetime.fromtimestamp(m["timestamp"], tz=UTC_TZ) >= cutoff]
                
                # Check if we are really filtering anything to avoid debug spam
                if len(filtered) < len(messages):
                     pass 
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

            if latest_success_ts:
                self.state_manager.update_timestamp(ticker, latest_success_ts)
                if performed_backfill:
                    self.state_manager.mark_backfill_done(ticker, True)
                # update watermark file as well
                self.watermark_manager.update(ticker, latest_success_ts)
            else:
                logger.warning("No successful publishes for ticker; state not advanced", extra={"ticker": ticker})

            for fm in failed:
                self.publish_to_dlq(ticker, "publish_failed", fm)

            logger.info("Ticker processing complete", extra={"ticker": ticker, "fetched": len(messages), "failed": len(failed), "performed_backfill": performed_backfill})
        except Exception as e:
            logger.exception("Failed to process ticker", extra={"ticker": ticker, "error": str(e)})
            self.publish_to_dlq(ticker, f"processing_exception: {e}")

    def run(self):
        logger.info("Producer started main loop")
        first_round = True
        while not SHUTDOWN:
            start = time.time()

            now_ny = datetime.now(NY_TZ)
            is_weekday = now_ny.weekday() < 5
            is_market_hours = 9 <= now_ny.hour < 17

            needs_backfill = any(
                (not self.state_manager.get_last_timestamp(t.strip())) for t in STOCK_TICKERS
            )

            should_run = (is_weekday and is_market_hours) or needs_backfill or self._force_backfill_env
            mode = "backfill" if (needs_backfill or self._force_backfill_env) else "realtime"
            if should_run:
                logger.info("Starting processing round", extra={"mode": mode, "tickers": len(STOCK_TICKERS)})
                for t in STOCK_TICKERS:
                    if SHUTDOWN:
                        break
                    self.process_ticker(t)
                    time.sleep(POLITE_SLEEP_BETWEEN_TICKERS)
                logger.info("Processing round complete")
                if self._force_backfill_env and first_round:
                    logger.info("Disabling instance FORCE_BACKFILL after first round (backfills performed per-ticker)")
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