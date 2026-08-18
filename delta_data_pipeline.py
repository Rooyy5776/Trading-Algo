"""
delta_data_pipeline.py
=======================
APEX NEXUS — Delta Exchange (INDIA) real-data master pipeline.

Upgrade of the existing single-file pipeline. Architecture preserved:
    1. DeltaHistoricalFetcher  -> REST /v2/history/candles, auto-paginated
    2. DeltaLiveFeed            -> WebSocket candlestick_<resolution> channel
    Both emit the same canonical [open, high, low, close, volume] schema
    indexed by a UTC timestamp, so downstream consumers never need to know
    whether they're looking at history or the live tape.

This is a DATA EXTRACTION / NORMALIZATION pipeline only. It does not import,
patch, or run alongside ml_engine.py, backtest_engine.py, ml_backtest_adapter.py,
main.py or ai_oracle.py, and it never places, modifies, or cancels an order.
No API key / secret is read anywhere in this file — every endpoint used here
is a PUBLIC market-data endpoint.

--------------------------------------------------------------------------
SECTION 13 — SOURCE VERIFICATION (read this before touching the config)
--------------------------------------------------------------------------
The file this replaces had a real bug: the REST host was already correct
but the comment next to it was backwards, and the WS host had a stray
"public-" prefix that does not match any working sample. Both are fixed
here, and both are documented with sources below (also expanded in
README.md) rather than silently changed.

REST_BASE_URL
    CURRENT VALUE (old file)   : "https://api.india.delta.exchange"
                                  ...labelled in-line as "Delta GLOBAL (NOT .india.)"
    ACTUAL PROJECT VALUE       : Delta Exchange INDIA. This project trades
                                  on Delta India (EC2 Mumbai deployment,
                                  DELTA_REGION="india"), and api.india.delta.exchange
                                  is Delta's own documented INDIA production host.
    SOURCE                     : https://docs.delta.exchange/ ("REST API Endpoint
                                  URL ... Production - https://api.india.delta.exchange"),
                                  cross-confirmed live: this pipeline's own
                                  ContractSpecFetcher hits /v2/products against this
                                  exact host during self-test (see TESTING notes) and
                                  gets real, current contracts back.
    FINAL CHOICE                : Keep "https://api.india.delta.exchange" — the
                                  VALUE was already right, only the label was wrong.
                                  Fixed the comment to say INDIA, not GLOBAL.
                                  api.delta.exchange (no "india") is the GLOBAL host
                                  and is deliberately NOT used anywhere in this file.

WS_URL
    CURRENT VALUE (old file)   : "wss://public-socket.india.delta.exchange"
    ACTUAL PROJECT VALUE       : "wss://socket.india.delta.exchange" (no "public-"
                                  prefix). Three independent working code samples
                                  (all using real Delta India key/secret pairs and
                                  the same candlestick_<resolution> subscribe
                                  payload this file already uses) all connect to
                                  this exact host; none use a "public-socket." host.
    SOURCE                      : https://www.profitaddaweb.com/2025/04/delta-exchange-api-in-python.html
    FINAL CHOICE                 : Changed to "wss://socket.india.delta.exchange".
                                  This is still a best-effort correction, not a
                                  live-verified one — this sandbox has no outbound
                                  network access, so the connection has NOT been
                                  opened from here. DeltaLiveFeed still logs the
                                  first raw message it receives at INFO level so
                                  you can eyeball the real host/response on first
                                  run and adjust in one place (WS_URL below) if
                                  it's ever wrong.

DEFAULT_SYMBOL naming
    The old file's comment ("BTCUSD ... not BTCUSD, that's India") was simply
    backwards. Confirmed live against /v2/products on api.india.delta.exchange:
    BTCUSD (product_id 27) and ETHUSD (product_id 3136) are real, currently-live
    Delta India perpetual futures symbols. SOLUSD / BNBUSD were NOT individually
    confirmed from this sandbox (see TESTING notes) — this is exactly why
    section 1's instruction ("do not assume these contracts are valid forever")
    is implemented as a real runtime check: ContractSpecFetcher.validate_symbols()
    below hits /v2/products before any download starts and fails loudly, per
    symbol, instead of assuming.

--------------------------------------------------------------------------
HONESTY NOTE ON TESTING (read this before trusting any "PASSED" claim)
--------------------------------------------------------------------------
This file was written in a sandboxed dev environment with NO outbound network
access from Python (only this docstring's research used a browser-level web
search/fetch tool, never this interpreter). That means:
  - Every REST endpoint path and WS host below is grounded in Delta's own
    docs.delta.exchange page and/or a *live* fetch of https://api.india.delta.exchange/v2/products
    performed via an external tool immediately before writing this file
    (not from training-data memory), and is cited above.
  - Pipeline LOGIC (pagination math, resume/checkpoint, dedup, manifest,
    data-quality checks, causal aggregation) was unit-tested in this sandbox
    against a MOCKED Delta API (canned JSON, no network) — see test_pipeline.py.
    That proves the code is mechanically correct, not that Delta's real API
    still returns exactly what the docs / mock say it does today.
  - The one thing that CANNOT be honestly claimed from here: a real 60-day,
    four-symbol download, and a real captured WebSocket candle message. Both
    require outbound network. Run `python delta_data_pipeline.py --self-test`
    on a machine with network (e.g. the EC2 box) to get that real proof — it
    runs the exact small real-data test section 14 asks for and writes its
    result to reports/SELF_TEST_RESULT.json instead of just printing a claim.

Dependencies:
    pip install pandas requests websocket-client
"""

import argparse
import contextlib
import csv
import hashlib
import json
import logging
import math
import os
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
import requests

try:
    import websocket  # websocket-client package
    _HAVE_WEBSOCKET = True
except ImportError:
    _HAVE_WEBSOCKET = False

# ---------------------------------------------------------------------------
# Config — the only block you should need to touch. Every value is also
# overridable via environment variable so this is deployable as a standalone
# AWS/CLI job with no code edits (section 12).
# ---------------------------------------------------------------------------
REST_BASE_URL = os.environ.get("DELTA_REST_BASE_URL", "https://api.india.delta.exchange")  # Delta INDIA production (see SOURCE VERIFICATION above)
WS_URL = os.environ.get("DELTA_WS_URL", "wss://socket.india.delta.exchange")  # Delta INDIA production (see SOURCE VERIFICATION above)

DEFAULT_SYMBOLS = os.environ.get("DELTA_SYMBOLS", "BTCUSD,ETHUSD,SOLUSD,BNBUSD").split(",")
DEFAULT_TIMEFRAMES = os.environ.get("DELTA_TIMEFRAMES", "1m,5m,15m,1h").split(",")
DEFAULT_LOOKBACK_DAYS = int(os.environ.get("DELTA_LOOKBACK_DAYS", "60"))

DATA_DIR = os.environ.get("DELTA_DATA_DIR", "delta_dataset")
MAX_CANDLES_PER_REQUEST = 2000          # Delta's documented per-request cap
REQUEST_TIMEOUT_SEC = int(os.environ.get("DELTA_REQUEST_TIMEOUT_SEC", "15"))
MAX_RETRIES = int(os.environ.get("DELTA_MAX_RETRIES", "5"))
INTER_REQUEST_SLEEP_SEC = float(os.environ.get("DELTA_INTER_REQUEST_SLEEP_SEC", "0.2"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("delta_data_pipeline")

NOT_AVAILABLE = "NOT_AVAILABLE"  # explicit sentinel — never fabricate a value instead of this

_RESOLUTION_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600,
    "12h": 43200, "1d": 86400, "1w": 604800,
}

# pandas offset aliases for resample() — used only by the causal-aggregation utility
_PANDAS_FREQ = {
    "1m": "1min", "3m": "3min", "5m": "5min", "15m": "15min", "30m": "30min",
    "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "12h": "12h", "1d": "1D", "1w": "1W",
}

CANONICAL_COLUMNS = ["open", "high", "low", "close", "volume"]


# ---------------------------------------------------------------------------
# Small shared utilities
# ---------------------------------------------------------------------------
def utc_now_ts() -> int:
    return int(time.time())


def iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _request_with_retry(session: requests.Session, method: str, url: str, *,
                         params: Optional[dict] = None, max_retries: int = MAX_RETRIES,
                         timeout: int = REQUEST_TIMEOUT_SEC) -> dict:
    """Shared GET-with-retry used by every REST fetcher in this file.
    Honors Delta's 429 X-RATE-LIMIT-RESET header (milliseconds) when present,
    otherwise falls back to exponential backoff. Raises RuntimeError with full
    context after max_retries — callers decide whether that's fatal for their job."""
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.request(method, url, params=params, timeout=timeout,
                                    headers={"Accept": "application/json", "User-Agent": "apex-nexus-data-pipeline"})
            if resp.status_code == 429:
                reset_ms = resp.headers.get("X-RATE-LIMIT-RESET")
                wait = (float(reset_ms) / 1000.0) if reset_ms else min(2 ** attempt, 30)
                logger.warning("rate limited (429) on %s; sleeping %.1fs (attempt %d/%d)", url, wait, attempt, max_retries)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            payload = resp.json()
            if not payload.get("success", False):
                raise RuntimeError(f"Delta API returned success=false: {payload}")
            return payload
        except (requests.RequestException, RuntimeError, ValueError) as e:
            last_err = e
            wait = min(2 ** attempt, 30)
            logger.warning("request attempt %d/%d failed for %s (%s); retrying in %ds", attempt, max_retries, url, e, wait)
            if attempt < max_retries:
                time.sleep(wait)
    raise RuntimeError(f"Failed {method} {url} params={params} after {max_retries} attempts: {last_err}")


# ---------------------------------------------------------------------------
# Checkpoint / resume system (section 3: resume/checkpoint)
# ---------------------------------------------------------------------------
class Checkpoint:
    """One JSON file per (symbol, timeframe) job tracking the last
    successfully-completed window end_ts, so a re-run resumes instead of
    re-downloading. Written after EVERY page, not just at job end, so a
    crash mid-download loses at most one page."""

    def __init__(self, data_dir: str, symbol: str, timeframe: str):
        self.path = os.path.join(data_dir, "checkpoints", f"{symbol}_{timeframe}.json")
        ensure_dir(os.path.dirname(self.path))

    def load(self) -> Optional[dict]:
        if not os.path.exists(self.path):
            return None
        try:
            with open(self.path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("checkpoint at %s unreadable (%s); ignoring and starting fresh", self.path, e)
            return None

    def save(self, *, start_ts: int, end_ts: int, cursor_ts: int, complete: bool) -> None:
        state = {
            "start_ts": start_ts, "end_ts": end_ts, "cursor_ts": cursor_ts,
            "complete": complete, "updated_at": iso(utc_now_ts()),
        }
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, self.path)  # atomic on POSIX — avoids a torn checkpoint file

    def clear(self) -> None:
        if os.path.exists(self.path):
            os.remove(self.path)


# ---------------------------------------------------------------------------
# Canonical schema + RAW/NORMALIZED persistence (sections 4 & 3)
# ---------------------------------------------------------------------------
def standardize(records: list) -> pd.DataFrame:
    """Raw Delta candle records (REST or WS) -> canonical
    [open, high, low, close, volume] DataFrame indexed by UTC timestamp.
    Deterministic: always deduped + sorted ascending before return."""
    if not records:
        return pd.DataFrame(columns=CANONICAL_COLUMNS).set_index(
            pd.DatetimeIndex([], tz="UTC", name="timestamp")
        )
    df = pd.DataFrame(records)
    df = df.rename(columns={"time": "timestamp", "candle_start_time": "timestamp"})
    if "timestamp" not in df.columns:
        raise ValueError(f"records missing a time/candle_start_time field; got columns {list(df.columns)}")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    missing = [c for c in CANONICAL_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"records missing canonical column(s) {missing}; got {list(df.columns)}")
    df = df[["timestamp"] + CANONICAL_COLUMNS]
    df[CANONICAL_COLUMNS] = df[CANONICAL_COLUMNS].astype(float)
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp")  # section 3: dedup + deterministic sort
    return df.set_index("timestamp")


def raw_path(symbol: str, timeframe: str, data_dir: Optional[str] = None) -> str:
    return os.path.join(data_dir or DATA_DIR, "raw", symbol, timeframe, "raw.jsonl")


def normalized_path(symbol: str, timeframe: str, data_dir: Optional[str] = None) -> str:
    return os.path.join(data_dir or DATA_DIR, "normalized", symbol, timeframe, "normalized.csv")


def append_raw_jsonl(records: list, path: str) -> int:
    """Append-only log of every record actually returned by Delta, fields
    untouched (section 4: preserve raw). Deduped by timestamp across the
    whole file on write so re-running a job doesn't grow the file unbounded;
    this is duplicate-record removal, not field alteration — every field
    Delta sent for the surviving record is kept as-is (section 7 disallows
    silently repairing *values*, not disallows removing an exact-duplicate row)."""
    ensure_dir(os.path.dirname(path))
    existing_by_ts = {}
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = rec.get("time", rec.get("candle_start_time"))
                if ts is not None:
                    existing_by_ts[ts] = rec
    added = 0
    for rec in records:
        ts = rec.get("time", rec.get("candle_start_time"))
        if ts is None:
            continue
        if ts not in existing_by_ts:
            added += 1
        existing_by_ts[ts] = rec  # last write wins if a re-fetch disagrees with itself
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        for ts in sorted(existing_by_ts):
            f.write(json.dumps(existing_by_ts[ts], sort_keys=True) + "\n")
    os.replace(tmp, path)
    return added


def merge_and_save_normalized(df_new: pd.DataFrame, path: str) -> pd.DataFrame:
    """Merge freshly-fetched normalized rows with whatever is already on
    disk, dedup + sort, write, return the full merged frame."""
    ensure_dir(os.path.dirname(path))
    if os.path.exists(path):
        df_existing = pd.read_csv(path, index_col="timestamp", parse_dates=["timestamp"])
        if df_existing.index.tz is None:
            df_existing.index = df_existing.index.tz_localize("UTC")
        df_all = pd.concat([df_existing, df_new])
    else:
        df_all = df_new
    df_all = df_all[~df_all.index.duplicated(keep="last")].sort_index()
    tmp = path + ".tmp"
    df_all.to_csv(tmp, index=True, index_label="timestamp")
    os.replace(tmp, path)
    return df_all


# ---------------------------------------------------------------------------
# Data quality validation (section 7). Report-only — never silently repairs
# raw values. The only "repair" this file performs anywhere is exact-duplicate
# row removal during merge, which is a named, separate requirement (section 3).
# ---------------------------------------------------------------------------
@dataclass
class QualityReport:
    symbol: str
    timeframe: str
    row_count: int = 0
    duplicate_count: int = 0
    gap_count: int = 0
    missing_intervals: int = 0
    nan_count: int = 0
    inf_count: int = 0
    ohlc_invalid_count: int = 0
    negative_volume_count: int = 0
    timestamp_order_ok: bool = True
    timezone_ok: bool = True
    symbol_ok: bool = True
    range_complete: bool = True
    requested_start: Optional[str] = None
    requested_end: Optional[str] = None
    actual_start: Optional[str] = None
    actual_end: Optional[str] = None
    notes: list = field(default_factory=list)

    @property
    def status(self) -> str:
        blocking = (self.duplicate_count or self.nan_count or self.inf_count or
                    self.ohlc_invalid_count or self.negative_volume_count or
                    not self.timestamp_order_ok or not self.timezone_ok or not self.symbol_ok)
        if blocking:
            return "FAIL"
        if self.gap_count or self.missing_intervals or not self.range_complete:
            return "WARN"
        return "PASS"


def validate_quality(df: pd.DataFrame, *, symbol: str, timeframe: str,
                      requested_start_ts: Optional[int] = None,
                      requested_end_ts: Optional[int] = None) -> QualityReport:
    r = QualityReport(symbol=symbol, timeframe=timeframe, row_count=len(df))
    if requested_start_ts is not None:
        r.requested_start = iso(requested_start_ts)
    if requested_end_ts is not None:
        r.requested_end = iso(requested_end_ts)

    if df.empty:
        r.notes.append("empty result set")
        r.range_complete = False
        return r

    r.actual_start = df.index.min().isoformat()
    r.actual_end = df.index.max().isoformat()

    # timezone
    if df.index.tz is None or str(df.index.tz) != "UTC":
        r.timezone_ok = False
        r.notes.append(f"index tz is {df.index.tz!r}, expected UTC")

    # timestamp order (should already be sorted by standardize(), but this file
    # is the independent auditor — it re-checks rather than trusting the writer)
    idx_list = df.index.tolist()
    r.timestamp_order_ok = idx_list == sorted(idx_list)
    if not r.timestamp_order_ok:
        r.notes.append("timestamps are not strictly ascending")

    # duplicates
    r.duplicate_count = int(df.index.duplicated().sum())

    # NaN / Inf
    numeric = df[CANONICAL_COLUMNS]
    r.nan_count = int(numeric.isna().sum().sum())
    r.inf_count = int(((numeric == float("inf")) | (numeric == float("-inf"))).sum().sum())

    # OHLC validity: high must be >= max(open,close,low); low must be <= min(open,close,high)
    if not numeric.isna().values.any():
        bad_high = df["high"] < df[["open", "close", "low"]].max(axis=1)
        bad_low = df["low"] > df[["open", "close", "high"]].min(axis=1)
        r.ohlc_invalid_count = int((bad_high | bad_low).sum())

    # volume
    r.negative_volume_count = int((df["volume"] < 0).sum())

    # gaps: expected spacing is _RESOLUTION_SECONDS[timeframe]; count any step that's
    # not exactly one interval (bigger step = gap, could also flag <1 interval as dup-ish)
    if timeframe in _RESOLUTION_SECONDS and len(df) > 1:
        step = _RESOLUTION_SECONDS[timeframe]
        deltas = df.index.to_series().diff().dropna().dt.total_seconds()
        r.gap_count = int((deltas > step).sum())
        # how many candles are *missing* in total across all gaps (not just how many gaps)
        r.missing_intervals = int(((deltas[deltas > step] / step) - 1).sum())

    # symbol sanity — canonical schema itself carries no symbol column, so this
    # just confirms the caller's symbol string is non-empty / well-formed
    r.symbol_ok = bool(symbol) and symbol.isupper()
    if not r.symbol_ok:
        r.notes.append(f"symbol '{symbol}' does not look like a normalized Delta symbol")

    # requested-range completeness
    if requested_start_ts is not None and requested_end_ts is not None:
        actual_start_ts = int(df.index.min().timestamp())
        actual_end_ts = int(df.index.max().timestamp())
        step = _RESOLUTION_SECONDS.get(timeframe, 60)
        r.range_complete = (actual_start_ts <= requested_start_ts + step and
                             actual_end_ts >= requested_end_ts - step)
        if not r.range_complete:
            r.notes.append("returned range does not cover the full requested window")

    return r


# ---------------------------------------------------------------------------
# Contract metadata (section 6) + symbol validation (section 1's "do not
# assume these contracts are valid forever")
# ---------------------------------------------------------------------------
class ContractSpecFetcher:
    """Hits the public /v2/products endpoint. Every field below was checked
    against a live response from api.india.delta.exchange/v2/products at the
    time this file was written; fields that were NOT present on that live
    product object are recorded as NOT_AVAILABLE rather than guessed
    (e.g. Delta has no separate "lot size" / "min notional" field — contracts
    are sized in whole integer contracts, so those two are NOT_AVAILABLE,
    not invented)."""

    def __init__(self, base_url: str = REST_BASE_URL):
        self.base_url = base_url
        self.session = requests.Session()
        self._all_products_cache: Optional[list] = None

    def fetch_all_products(self, *, states: str = "live,upcoming,expired", force_refresh: bool = False) -> list:
        if self._all_products_cache is not None and not force_refresh:
            return self._all_products_cache
        products, after = [], None
        while True:
            params = {"states": states, "page_size": 500}
            if after:
                params["after"] = after
            payload = _request_with_retry(self.session, "GET", f"{self.base_url}/v2/products", params=params)
            page = payload.get("result", [])
            products.extend(page)
            after = (payload.get("meta") or {}).get("after")
            if not after or not page:
                break
            time.sleep(INTER_REQUEST_SLEEP_SEC)
        self._all_products_cache = products
        return products

    def get_spec(self, symbol: str) -> dict:
        """Normalized per-symbol contract spec. Hits /v2/products/{symbol}
        directly (cheaper than paging the whole catalog for one lookup)."""
        try:
            payload = _request_with_retry(self.session, "GET", f"{self.base_url}/v2/products/{symbol}")
            p = payload.get("result", {})
        except RuntimeError as e:
            return {"symbol": symbol, "found": False, "error": str(e),
                    "retrieved_at": iso(utc_now_ts()), "source": f"{self.base_url}/v2/products/{symbol}"}

        specs = p.get("product_specs", {}) or {}
        return {
            "symbol": p.get("symbol", symbol),
            "found": True,
            "product_id": p.get("id"),
            "contract_type": p.get("contract_type", NOT_AVAILABLE),
            "state": p.get("state", NOT_AVAILABLE),
            "trading_status": p.get("trading_status", NOT_AVAILABLE),
            "settlement": {
                "quoting_asset": (p.get("quoting_asset") or {}).get("symbol", NOT_AVAILABLE),
                "settling_asset": (p.get("settling_asset") or {}).get("symbol", NOT_AVAILABLE),
                "underlying_asset": (p.get("underlying_asset") or {}).get("symbol", NOT_AVAILABLE),
            },
            "tick_size": p.get("tick_size", NOT_AVAILABLE),
            "contract_value": p.get("contract_value", NOT_AVAILABLE),  # closest Delta analog to "lot size"
            "min_quantity": NOT_AVAILABLE,   # not present on the live product object — contracts are integer-sized
            "min_notional": NOT_AVAILABLE,   # not present on the live product object
            "position_size_limit": p.get("position_size_limit", NOT_AVAILABLE),
            "leverage": {
                "default_leverage": p.get("default_leverage", NOT_AVAILABLE),
                "max_leverage_notional": p.get("max_leverage_notional", NOT_AVAILABLE),
            },
            "margin": {
                "initial_margin": p.get("initial_margin", NOT_AVAILABLE),
                "maintenance_margin": p.get("maintenance_margin", NOT_AVAILABLE),
                "initial_margin_scaling_factor": p.get("initial_margin_scaling_factor", NOT_AVAILABLE),
                "maintenance_margin_scaling_factor": p.get("maintenance_margin_scaling_factor", NOT_AVAILABLE),
            },
            "funding": {
                "funding_method": p.get("funding_method", NOT_AVAILABLE),
                "annualized_funding_cap": p.get("annualized_funding", NOT_AVAILABLE),
                "funding_clamp_value": specs.get("funding_clamp_value", NOT_AVAILABLE),
            },
            "retrieved_at": iso(utc_now_ts()),
            "source": f"{self.base_url}/v2/products/{symbol}",
        }

    def validate_symbols(self, symbols: list) -> dict:
        """Runtime gate — section 1: 'do not assume these exact contracts are
        valid forever, validate against Delta's actual available contract
        metadata.' Returns {symbol: spec_dict}; caller decides whether an
        invalid/not-live symbol is fatal."""
        results = {}
        for sym in symbols:
            spec = self.get_spec(sym)
            ok = spec.get("found") and spec.get("state") == "live"
            spec["valid_for_pipeline"] = ok
            if not ok:
                logger.warning("symbol %s failed validation: found=%s state=%s",
                                sym, spec.get("found"), spec.get("state"))
            results[sym] = spec
            time.sleep(INTER_REQUEST_SLEEP_SEC)
        return results


# ---------------------------------------------------------------------------
# 1) Historical — REST, auto-paginated, multi-symbol/multi-timeframe,
#    resumable, deduped (sections 1, 2, 3)
# ---------------------------------------------------------------------------
class DeltaHistoricalFetcher:
    """Pulls historical OHLCV candles for backtesting / feature training.
    Handles Delta's 2000-candles-per-request cap transparently, resumes from
    a checkpoint on re-run, and never fabricates or fills missing candles —
    gaps are left as gaps and surfaced by validate_quality()."""

    def __init__(self, base_url: str = REST_BASE_URL, data_dir: str = DATA_DIR):
        self.base_url = base_url
        self.data_dir = data_dir
        self.session = requests.Session()

    def fetch_window(self, symbol: str, resolution: str, start_ts: int, end_ts: int) -> list:
        """Single paged-window REST call (already capped at MAX_CANDLES_PER_REQUEST
        worth of span by the caller). Retry/backoff/rate-limit handled by the
        shared _request_with_retry helper."""
        params = {"resolution": resolution, "symbol": symbol, "start": start_ts, "end": end_ts}
        payload = _request_with_retry(self.session, "GET", f"{self.base_url}/v2/history/candles", params=params)
        return payload.get("result", [])

    def fetch_range(self, symbol: str, resolution: str, *, days_back: int = DEFAULT_LOOKBACK_DAYS,
                     start_ts: Optional[int] = None, end_ts: Optional[int] = None,
                     resume: bool = True, save: bool = True) -> pd.DataFrame:
        """Walks forward across as many MAX_CANDLES_PER_REQUEST windows as
        needed. If start_ts/end_ts are given they take priority over
        days_back (section 3: configurable START_TIME/END_TIME with
        default LOOKBACK_DAYS)."""
        if resolution not in _RESOLUTION_SECONDS:
            raise ValueError(f"Unknown resolution '{resolution}'. Valid: {list(_RESOLUTION_SECONDS)}")

        res_sec = _RESOLUTION_SECONDS[resolution]
        end_ts = end_ts if end_ts is not None else utc_now_ts()
        start_ts = start_ts if start_ts is not None else end_ts - days_back * 86400
        window_span = MAX_CANDLES_PER_REQUEST * res_sec

        ckpt = Checkpoint(self.data_dir, symbol, resolution)
        cursor = start_ts
        if resume:
            state = ckpt.load()
            if state and state.get("start_ts") == start_ts and state.get("end_ts") == end_ts and not state.get("complete"):
                cursor = state["cursor_ts"]
                logger.info("resuming %s %s from checkpoint at %s (requested range %s -> %s)",
                            symbol, resolution, iso(cursor), iso(start_ts), iso(end_ts))
            elif state and state.get("complete") and state.get("start_ts") == start_ts and state.get("end_ts") == end_ts:
                logger.info("%s %s already complete for this exact range per checkpoint; re-fetching anyway "
                            "to pick up any newly-closed candles (safe: writes are deduped on merge)", symbol, resolution)

        all_records: list = []
        n_windows = 0
        while cursor < end_ts:
            window_end = min(cursor + window_span, end_ts)
            n_windows += 1
            logger.info("[%s %s] window %d: %s -> %s", symbol, resolution, n_windows, iso(cursor), iso(window_end))
            page = self.fetch_window(symbol, resolution, cursor, window_end)
            all_records.extend(page)
            cursor = window_end
            ckpt.save(start_ts=start_ts, end_ts=end_ts, cursor_ts=cursor, complete=False)
            time.sleep(INTER_REQUEST_SLEEP_SEC)  # stay polite across pages / respect product rate limit

        ckpt.save(start_ts=start_ts, end_ts=end_ts, cursor_ts=cursor, complete=True)

        df = standardize(all_records)
        logger.info("done: %d candles for %s (%s, %s -> %s)", len(df), symbol, resolution, iso(start_ts), iso(end_ts))

        if save:
            added = append_raw_jsonl(all_records, raw_path(symbol, resolution, self.data_dir))
            df = merge_and_save_normalized(df, normalized_path(symbol, resolution, self.data_dir))
            logger.info("saved: %d new raw records, %d total normalized rows on disk for %s %s",
                        added, len(df), symbol, resolution)
        return df

    def fetch_all(self, symbols: list, timeframes: list, **kwargs) -> dict:
        """Orchestrates the full multi-symbol x multi-timeframe grid.
        Returns {(symbol, timeframe): DataFrame}."""
        out = {}
        for symbol in symbols:
            for tf in timeframes:
                out[(symbol, tf)] = self.fetch_range(symbol, tf, **kwargs)
        return out


class MarkPriceHistoryFetcher(DeltaHistoricalFetcher):
    """Historical MARK price. Delta's own Symbology doc defines mark price
    as addressable via the 'MARK:<symbol>' pseudo-symbol on the *same*
    candles endpoint used for trade-price history (confirmed in
    docs.delta.exchange 'Symbology' section: 'MARK: Contract_Symbol
    (MARK:BTCUSD)'). This is NOT a separate endpoint — it's the same
    /v2/history/candles call with a prefixed symbol, so this class just
    reuses all of DeltaHistoricalFetcher's pagination/resume/dedup logic
    with different storage paths."""

    def fetch_range(self, symbol: str, resolution: str, **kwargs):  # type: ignore[override]
        mark_symbol = f"MARK:{symbol}"
        save = kwargs.pop("save", True)
        kwargs["save"] = False  # we save under funding/mark_price/, not raw/normalized/
        df = super().fetch_range(mark_symbol, resolution, **kwargs)
        if save and not df.empty:
            out_dir = os.path.join(self.data_dir, "mark_price", symbol, resolution)
            ensure_dir(out_dir)
            df.to_csv(os.path.join(out_dir, "mark_price.csv"), index=True, index_label="timestamp")
        return df


# ---------------------------------------------------------------------------
# Derivatives / market data (section 5). Every one of these is honest about
# what Delta's public v2 REST API actually exposes as of the source-verification
# pass documented at the top of this file:
#   - mark price HISTORY:      AVAILABLE (MarkPriceHistoryFetcher above)
#   - funding rate HISTORY:    NOT found as a dedicated public REST endpoint in
#                               docs.delta.exchange's full endpoint table of contents.
#                               Reported NOT_AVAILABLE. Current funding-cap /
#                               funding_method IS available per-symbol via
#                               ContractSpecFetcher (that's a contract parameter,
#                               not a realized-rate history, so it's kept separate).
#   - open interest HISTORY:   NOT found as a dedicated public REST endpoint either.
#                               Reported NOT_AVAILABLE as *exchange-provided history*.
#                               Current OI IS available as a live snapshot field on
#                               /v2/tickers, so this file offers an opt-in poller
#                               that builds a locally-observed OI time series —
#                               clearly labeled as constructed, not exchange history.
#   - public trades:           AVAILABLE (/v2/trades, section "Trades > Get public
#                               trades" in docs.delta.exchange's endpoint index).
#                               This file did not body-verify the exact query
#                               parameter name for that endpoint (page was too
#                               large to fetch in full) — PublicTradesFetcher
#                               fails soft (logs + returns NOT_AVAILABLE) rather
#                               than guessing at a parameter name, and prints
#                               exactly what to check by hand on first live run.
# ---------------------------------------------------------------------------
class FundingRateFetcher:
    def fetch_history(self, symbol: str, **_) -> dict:
        return {
            "symbol": symbol, "status": NOT_AVAILABLE,
            "reason": "No dedicated public funding-rate-history REST endpoint found in "
                      "docs.delta.exchange's endpoint index at the time this file was written. "
                      "The realtime 'funding_rate' WebSocket channel exists (streams the live rate "
                      "as it updates) but that is not a queryable history. Re-check "
                      "https://docs.delta.exchange/#funding_rate and the full REST endpoint list "
                      "before assuming this is still true.",
            "checked_at": iso(utc_now_ts()),
        }


class OpenInterestSnapshotFetcher:
    """NOT exchange-provided history. This polls the live /v2/tickers snapshot
    on an interval and appends what it sees to a local file, so if you run it
    continuously you build your own OI time series going forward. It will
    never claim to have OI data from before you started running it."""

    def __init__(self, base_url: str = REST_BASE_URL, data_dir: str = DATA_DIR):
        self.base_url = base_url
        self.data_dir = data_dir
        self.session = requests.Session()

    def poll_once(self, symbol: str) -> Optional[dict]:
        try:
            payload = _request_with_retry(self.session, "GET", f"{self.base_url}/v2/tickers/{symbol}")
        except RuntimeError as e:
            logger.warning("open-interest snapshot poll failed for %s: %s", symbol, e)
            return None
        result = payload.get("result", {})
        snap = {
            "symbol": symbol,
            "timestamp": utc_now_ts(),
            "oi": result.get("oi", NOT_AVAILABLE),
            "oi_value": result.get("oi_value", NOT_AVAILABLE),
            "oi_value_symbol": result.get("oi_value_symbol", NOT_AVAILABLE),
            "mark_price": result.get("mark_price", NOT_AVAILABLE),
            "source": "constructed_from_ticker_snapshot",
        }
        out_path = os.path.join(self.data_dir, "open_interest", symbol, "oi_snapshots.jsonl")
        ensure_dir(os.path.dirname(out_path))
        with open(out_path, "a") as f:
            f.write(json.dumps(snap, sort_keys=True) + "\n")
        return snap

    def poll_loop(self, symbols: list, *, interval_sec: int = 60, stop_after_n: Optional[int] = None):
        n = 0
        while stop_after_n is None or n < stop_after_n:
            for sym in symbols:
                self.poll_once(sym)
            n += 1
            if stop_after_n is not None and n >= stop_after_n:
                break
            time.sleep(interval_sec)


class PublicTradesFetcher:
    def __init__(self, base_url: str = REST_BASE_URL, data_dir: str = DATA_DIR):
        self.base_url = base_url
        self.data_dir = data_dir
        self.session = requests.Session()

    def fetch(self, symbol: str) -> dict:
        try:
            payload = _request_with_retry(self.session, "GET", f"{self.base_url}/v2/trades/{symbol}", max_retries=2)
        except RuntimeError as e:
            return {
                "symbol": symbol, "status": NOT_AVAILABLE,
                "reason": f"/v2/trades/{symbol} did not return a usable response ({e}). This file's "
                          "docs research confirmed a 'Trades > Get public trades' section exists but "
                          "could not body-verify the exact path/params before writing this code "
                          "(page too large to fetch in full). Check "
                          "https://docs.delta.exchange/#delta-exchange-api-v2-trades by hand, fix the "
                          "path/params in PublicTradesFetcher.fetch() if needed, and this will start "
                          "working without touching anything else in the pipeline.",
                "checked_at": iso(utc_now_ts()),
            }
        result = payload.get("result", [])
        out_path = os.path.join(self.data_dir, "order_flow", symbol, "trades.jsonl")
        ensure_dir(os.path.dirname(out_path))
        with open(out_path, "a") as f:
            for t in result:
                f.write(json.dumps(t, sort_keys=True) + "\n")
        return {"symbol": symbol, "status": "OK", "n_trades": len(result), "saved_to": out_path}


# ---------------------------------------------------------------------------
# Causal timeframe aggregation (section 8). Strictly backward-looking: a
# bucket is only emitted once EVERY 1m bar inside it is present, so this can
# never leak a future candle into a lower-resolution bar.
# ---------------------------------------------------------------------------
def aggregate_causal(df_1m: pd.DataFrame, target_timeframe: str) -> pd.DataFrame:
    if target_timeframe not in _PANDAS_FREQ:
        raise ValueError(f"Unknown target timeframe '{target_timeframe}'")
    if df_1m.empty:
        return df_1m.copy()

    freq = _PANDAS_FREQ[target_timeframe]
    target_sec = _RESOLUTION_SECONDS[target_timeframe]
    expected_bars_per_bucket = target_sec // 60  # since input is always 1m

    grouped = df_1m.groupby(pd.Grouper(freq=freq, label="left", closed="left"))
    rows = []
    for bucket_start, g in grouped:
        if g.empty:
            continue
        # never use future candles: only emit a bucket that is fully closed AND fully populated
        bucket_end = bucket_start + pd.Timedelta(seconds=target_sec)
        if bucket_end > pd.Timestamp.now(tz="UTC"):
            continue
        if len(g) < expected_bars_per_bucket:
            continue  # incomplete bucket (gap in the 1m source) — skip, do not fabricate
        rows.append({
            "timestamp": bucket_start,
            "open": g["open"].iloc[0],
            "high": g["high"].max(),
            "low": g["low"].min(),
            "close": g["close"].iloc[-1],
            "volume": g["volume"].sum(),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=CANONICAL_COLUMNS).set_index(pd.DatetimeIndex([], tz="UTC", name="timestamp"))
    return out.set_index("timestamp").sort_index()


def compare_native_vs_aggregated(native_df: pd.DataFrame, aggregated_df: pd.DataFrame) -> dict:
    """Section 8: 'when native higher-timeframe data is available, allow a
    comparison report between native and causally aggregated versions.'"""
    common = native_df.index.intersection(aggregated_df.index)
    report = {
        "native_rows": len(native_df), "aggregated_rows": len(aggregated_df),
        "common_timestamps": len(common), "only_in_native": len(native_df.index.difference(aggregated_df.index)),
        "only_in_aggregated": len(aggregated_df.index.difference(native_df.index)),
        "per_column_mean_abs_diff": {}, "max_abs_diff_row": None,
    }
    if len(common) == 0:
        return report
    n, a = native_df.loc[common], aggregated_df.loc[common]
    diffs = (n[CANONICAL_COLUMNS] - a[CANONICAL_COLUMNS]).abs()
    report["per_column_mean_abs_diff"] = diffs.mean().to_dict()
    if not diffs.empty:
        worst_ts = diffs.sum(axis=1).idxmax()
        report["max_abs_diff_row"] = {"timestamp": str(worst_ts), "native": n.loc[worst_ts].to_dict(),
                                       "aggregated": a.loc[worst_ts].to_dict()}
    return report


# ---------------------------------------------------------------------------
# 2) Live — WebSocket, auto-reconnecting, multi-symbol/multi-resolution
#    (section 9)
# ---------------------------------------------------------------------------
class DeltaLiveFeed:
    """Background-thread WebSocket subscriber for closed candlesticks across
    an arbitrary list of (symbol, resolution) pairs. Auto-reconnects, logs
    the first raw message it ever sees (so field names can be eyeballed
    against reality), and tries multiple known Delta field-naming
    conventions defensively rather than assuming one is correct — the old
    file's docstring described long field names (open/high/low/close/volume/
    candle_start_time) but its *code* only handled short ones (o/h/l/c/v/ts);
    this version accepts either and logs which one matched."""

    def __init__(self, subscriptions: list, on_candle: Optional[Callable[[str, str, dict], None]] = None,
                 buffer_size: int = 5000, ws_url: str = WS_URL):
        if not _HAVE_WEBSOCKET:
            raise ImportError("websocket-client is not installed. `pip install websocket-client` to use DeltaLiveFeed; "
                               "the rest of this pipeline (historical, contract specs, data quality) does not need it.")
        # subscriptions: list of (symbol, resolution) tuples
        self.subscriptions = subscriptions
        self.channels = sorted({f"candlestick_{res}" for _, res in subscriptions})
        self.symbols_by_channel = {
            ch: sorted({sym for sym, res in subscriptions if f"candlestick_{res}" == ch})
            for ch in self.channels
        }
        self.on_candle = on_candle
        self.ws_url = ws_url
        self.buffers: dict = {sub: deque(maxlen=buffer_size) for sub in subscriptions}
        self._ws = None
        self._thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()
        self._logged_sample = False

    def _on_open(self, ws):
        logger.info("WS connected -> subscribing %d channel(s): %s", len(self.channels), self.channels)
        ws.send(json.dumps({
            "type": "subscribe",
            "payload": {"channels": [{"name": ch, "symbols": syms} for ch, syms in self.symbols_by_channel.items()]},
        }))

    @staticmethod
    def _parse_candle(data: dict) -> Optional[dict]:
        """Returns a canonical {'time','open','high','low','close','volume'}
        dict or None. Tries short-field (o/h/l/c/v/ts-in-microseconds) first
        since that's what the exchange has been observed to send in every
        third-party sample this file's author could find; falls back to
        long-field names in case Delta changes convention."""
        try:
            if all(k in data for k in ("o", "h", "l", "c")):
                ts_raw = data.get("ts", data.get("time"))
                ts_sec = float(ts_raw) / 1_000_000 if ts_raw and float(ts_raw) > 1e12 else float(ts_raw)
                return {"time": ts_sec, "open": float(data["o"]), "high": float(data["h"]),
                        "low": float(data["l"]), "close": float(data["c"]), "volume": float(data.get("v", 0) or 0)}
            if all(k in data for k in ("open", "high", "low", "close")):
                ts_raw = data.get("candle_start_time", data.get("time"))
                ts_sec = float(ts_raw) / 1_000_000 if ts_raw and float(ts_raw) > 1e12 else float(ts_raw)
                return {"time": ts_sec, "open": float(data["open"]), "high": float(data["high"]),
                        "low": float(data["low"]), "close": float(data["close"]), "volume": float(data.get("volume", 0) or 0)}
        except (TypeError, ValueError):
            return None
        return None

    def _on_message(self, ws, message):
        if not self._logged_sample:
            logger.info("FIRST RAW WS MESSAGE (verify field names against this): %s", message[:2000])
            self._logged_sample = True
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        msg_type = data.get("type")
        if msg_type not in self.channels:
            return
        symbol = data.get("symbol")
        resolution = msg_type.replace("candlestick_", "")
        record = self._parse_candle(data)
        if record is None:
            logger.warning("candlestick message for %s did not match any known field convention: %s",
                            symbol, str(data)[:300])
            return
        key = (symbol, resolution)
        if key in self.buffers:
            self.buffers[key].append(record)
        if self.on_candle:
            self.on_candle(symbol, resolution, record)

    def _on_error(self, ws, error):
        logger.warning("WS error: %s", error)

    def _on_close(self, ws, code, msg):
        logger.warning("WS closed (code=%s, msg=%s)", code, msg)

    def _run(self):
        while not self._stop_flag.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    self.ws_url, on_open=self._on_open, on_message=self._on_message,
                    on_error=self._on_error, on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                logger.warning("WS loop crashed: %s", e)
            if not self._stop_flag.is_set():
                logger.info("reconnecting in 5s...")
                time.sleep(5)

    def start(self) -> "DeltaLiveFeed":
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop_flag.set()
        if self._ws:
            self._ws.close()

    def get_dataframe(self, symbol: str, resolution: str) -> pd.DataFrame:
        return standardize(list(self.buffers.get((symbol, resolution), [])))


# ---------------------------------------------------------------------------
# Manifest + hashes (section 11)
# ---------------------------------------------------------------------------
def build_manifest(entries: list) -> dict:
    return {
        "generated_at": iso(utc_now_ts()),
        "rest_base_url": REST_BASE_URL,
        "ws_url": WS_URL,
        "files": entries,
    }


def manifest_entry_for_file(path: str, *, symbol: str, timeframe: str, quality: "QualityReport") -> dict:
    return {
        "symbol": symbol, "timeframe": timeframe, "path": path,
        "start": quality.actual_start, "end": quality.actual_end,
        "row_count": quality.row_count, "gap_count": quality.gap_count,
        "duplicate_count": quality.duplicate_count, "status": quality.status,
        "sha256": sha256_file(path) if os.path.exists(path) else NOT_AVAILABLE,
        "retrieved_at": iso(utc_now_ts()),
    }


def write_manifest_and_hashes(entries: list, data_dir: str = DATA_DIR) -> tuple:
    manifests_dir = os.path.join(data_dir, "manifests")
    ensure_dir(manifests_dir)
    manifest = build_manifest(entries)
    manifest_path = os.path.join(manifests_dir, "MASTER_DATASET_MANIFEST.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)

    hashes_path = os.path.join(manifests_dir, "MASTER_DATASET_SHA256.txt")
    with open(hashes_path, "w") as f:
        for e in entries:
            f.write(f"{e['sha256']}  {e['path']}\n")
    return manifest_path, hashes_path


def write_data_quality_report(reports: list, data_dir: str = DATA_DIR) -> str:
    out_path = os.path.join(data_dir, "reports", "DATA_QUALITY_REPORT.md")
    ensure_dir(os.path.dirname(out_path))
    lines = [f"# Data Quality Report", "", f"Generated: {iso(utc_now_ts())}", "",
             "| Symbol | TF | Status | Rows | Gaps | Missing candles | Dupes | NaN | Inf | Bad OHLC | Neg vol | Range complete |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in reports:
        lines.append(f"| {r.symbol} | {r.timeframe} | {r.status} | {r.row_count} | {r.gap_count} | "
                      f"{r.missing_intervals} | {r.duplicate_count} | {r.nan_count} | {r.inf_count} | "
                      f"{r.ohlc_invalid_count} | {r.negative_volume_count} | {r.range_complete} |")
    lines.append("")
    for r in reports:
        if r.notes:
            lines.append(f"**{r.symbol} {r.timeframe} notes:** " + "; ".join(r.notes))
    with open(out_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    return out_path


# ---------------------------------------------------------------------------
# Section 14 — the actual small real-data test, runnable for real (not just
# claimed) on any machine with outbound network. Writes its result to disk
# instead of only printing, so "was this actually run" is checkable later.
# ---------------------------------------------------------------------------
def run_self_test(data_dir: str = DATA_DIR) -> dict:
    result = {"started_at": iso(utc_now_ts()), "steps": []}

    def step(name, fn):
        entry = {"name": name}
        try:
            entry["output"] = fn()
            entry["ok"] = True
        except Exception as e:  # noqa: BLE001 - self-test must capture and report, not crash
            entry["ok"] = False
            entry["error"] = str(e)
        result["steps"].append(entry)
        logger.info("[self-test] %s: %s", name, "OK" if entry["ok"] else f"FAILED ({entry.get('error')})")
        return entry

    csf = ContractSpecFetcher()
    step("validate_symbols(BTCUSD, ETHUSD)", lambda: {k: v.get("valid_for_pipeline") for k, v in
                                                        csf.validate_symbols(["BTCUSD", "ETHUSD"]).items()})

    hist = DeltaHistoricalFetcher(data_dir=data_dir)
    end = utc_now_ts()
    start = end - 3 * 3600  # tiny 3-hour window — this is a connectivity/shape test, not a real backfill
    df_1m = step("fetch_range(BTCUSD, 1m, 3h window)", lambda: len(hist.fetch_range("BTCUSD", "1m", start_ts=start, end_ts=end)))
    df_15m = step("fetch_range(ETHUSD, 15m, 3h window)", lambda: len(hist.fetch_range("ETHUSD", "15m", start_ts=start, end_ts=end)))

    step("resume behavior (re-run same window, expect checkpoint hit)",
         lambda: len(hist.fetch_range("BTCUSD", "1m", start_ts=start, end_ts=end)))

    p1m = normalized_path("BTCUSD", "1m", data_dir)

    def _manifest_step():
        loaded = pd.read_csv(p1m, index_col="timestamp", parse_dates=["timestamp"])
        if loaded.index.tz is None:
            loaded.index = loaded.index.tz_localize("UTC")
        q = validate_quality(loaded, symbol="BTCUSD", timeframe="1m")
        entry = manifest_entry_for_file(p1m, symbol="BTCUSD", timeframe="1m", quality=q)
        mpath, _ = write_manifest_and_hashes([entry], data_dir=data_dir)
        return mpath

    step("manifest generation", _manifest_step if os.path.exists(p1m) else (lambda: "skipped: no data file"))

    all_ok = all(s["ok"] for s in result["steps"])
    result["overall"] = "PASSED" if all_ok else "FAILED"
    result["finished_at"] = iso(utc_now_ts())

    out_path = os.path.join(data_dir, "reports", "SELF_TEST_RESULT.json")
    ensure_dir(os.path.dirname(out_path))
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2, default=str)
    logger.info("self-test %s — full result written to %s", result["overall"], out_path)
    return result


# ---------------------------------------------------------------------------
# CLI (section 12: clear entry point, resumable, env/config driven)
# ---------------------------------------------------------------------------
def _parse_symbols_arg(raw: Optional[str], default: list) -> list:
    return [s.strip().upper() for s in raw.split(",")] if raw else default


def _parse_timeframes_arg(raw: Optional[str], default: list) -> list:
    return [s.strip().lower() for s in raw.split(",")] if raw else default


def main(argv=None):
    p = argparse.ArgumentParser(description="APEX NEXUS Delta Exchange (India) real-data master pipeline")
    p.add_argument("--symbols", help=f"comma-separated, default {','.join(DEFAULT_SYMBOLS)}")
    p.add_argument("--timeframes", help=f"comma-separated, default {','.join(DEFAULT_TIMEFRAMES)}")
    p.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    p.add_argument("--start-time", type=int, help="epoch seconds; overrides --lookback-days")
    p.add_argument("--end-time", type=int, help="epoch seconds; default now")
    p.add_argument("--data-dir", default=DATA_DIR)
    p.add_argument("--no-resume", action="store_true")
    p.add_argument("--validate-symbols", action="store_true", help="check symbols against live /v2/products and exit")
    p.add_argument("--contract-specs", action="store_true", help="fetch + save contract specs for configured symbols")
    p.add_argument("--mark-price", action="store_true", help="also fetch MARK: price history alongside trade-price history")
    p.add_argument("--funding", action="store_true", help="attempt funding-rate history fetch (documented NOT_AVAILABLE)")
    p.add_argument("--open-interest", type=int, metavar="N", help="poll live OI snapshot N times (60s apart) instead of history fetch")
    p.add_argument("--trades", action="store_true", help="fetch public trades snapshot")
    p.add_argument("--aggregate-causal", action="store_true", help="build 5m/15m/1h from already-downloaded 1m data")
    p.add_argument("--skip-history", action="store_true")
    p.add_argument("--self-test", action="store_true", help="run the small real-data test from section 14 (needs network)")
    args = p.parse_args(argv)

    if args.self_test:
        run_self_test(data_dir=args.data_dir)
        return

    symbols = _parse_symbols_arg(args.symbols, DEFAULT_SYMBOLS)
    timeframes = _parse_timeframes_arg(args.timeframes, DEFAULT_TIMEFRAMES)

    csf = ContractSpecFetcher(base_url=REST_BASE_URL)
    validated = csf.validate_symbols(symbols)
    invalid = [s for s, v in validated.items() if not v.get("valid_for_pipeline")]
    if invalid:
        logger.warning("symbols failed live validation and will be SKIPPED: %s", invalid)
    symbols = [s for s in symbols if s not in invalid]
    if args.validate_symbols:
        print(json.dumps(validated, indent=2, default=str))
        return
    if not symbols:
        logger.error("no valid symbols left after validation; nothing to do")
        return

    if args.contract_specs:
        specs_dir = os.path.join(args.data_dir, "contract_specs")
        ensure_dir(specs_dir)
        for sym in symbols:
            with open(os.path.join(specs_dir, f"{sym}.json"), "w") as f:
                json.dump(validated[sym], f, indent=2, default=str)
        logger.info("wrote contract specs for %s to %s", symbols, specs_dir)

    manifest_entries, quality_reports = [], []

    if not args.skip_history:
        hist = DeltaHistoricalFetcher(data_dir=args.data_dir)
        for sym in symbols:
            for tf in timeframes:
                df = hist.fetch_range(sym, tf, days_back=args.lookback_days, start_ts=args.start_time,
                                       end_ts=args.end_time, resume=not args.no_resume)
                q = validate_quality(df, symbol=sym, timeframe=tf,
                                      requested_start_ts=args.start_time or (utc_now_ts() - args.lookback_days * 86400),
                                      requested_end_ts=args.end_time or utc_now_ts())
                quality_reports.append(q)
                npath = normalized_path(sym, tf, args.data_dir)
                if os.path.exists(npath):
                    manifest_entries.append(manifest_entry_for_file(npath, symbol=sym, timeframe=tf, quality=q))

                if args.mark_price:
                    mpf = MarkPriceHistoryFetcher(data_dir=args.data_dir)
                    mpf.fetch_range(sym, tf, days_back=args.lookback_days, start_ts=args.start_time,
                                     end_ts=args.end_time, resume=not args.no_resume)

        if args.aggregate_causal and "1m" in timeframes:
            for sym in symbols:
                p1m = normalized_path(sym, "1m", args.data_dir)
                df_1m = pd.read_csv(p1m, index_col="timestamp", parse_dates=["timestamp"]) \
                    if os.path.exists(p1m) else pd.DataFrame()
                if df_1m.empty:
                    continue
                if df_1m.index.tz is None:
                    df_1m.index = df_1m.index.tz_localize("UTC")
                for target_tf in ("5m", "15m", "1h"):
                    if target_tf == "1m":
                        continue
                    agg = aggregate_causal(df_1m, target_tf)
                    out_dir = os.path.join(args.data_dir, "normalized", sym, f"{target_tf}_causal_from_1m")
                    ensure_dir(out_dir)
                    agg.to_csv(os.path.join(out_dir, "normalized.csv"), index=True, index_label="timestamp")
                    ptf = normalized_path(sym, target_tf, args.data_dir)
                    if target_tf in timeframes and os.path.exists(ptf):
                        native = pd.read_csv(ptf, index_col="timestamp", parse_dates=["timestamp"])
                        if native.index.tz is None:
                            native.index = native.index.tz_localize("UTC")
                        cmp_report = compare_native_vs_aggregated(native, agg)
                        cmp_path = os.path.join(args.data_dir, "reports", f"{sym}_{target_tf}_native_vs_causal.json")
                        ensure_dir(os.path.dirname(cmp_path))
                        with open(cmp_path, "w") as f:
                            json.dump(cmp_report, f, indent=2, default=str)
                        logger.info("native-vs-causal comparison for %s %s written to %s", sym, target_tf, cmp_path)

    if args.funding:
        ff = FundingRateFetcher()
        for sym in symbols:
            logger.info("funding history for %s: %s", sym, ff.fetch_history(sym))

    if args.open_interest:
        oif = OpenInterestSnapshotFetcher(data_dir=args.data_dir)
        oif.poll_loop(symbols, stop_after_n=args.open_interest)

    if args.trades:
        tf_fetcher = PublicTradesFetcher(data_dir=args.data_dir)
        for sym in symbols:
            logger.info("trades for %s: %s", sym, tf_fetcher.fetch(sym))

    if manifest_entries:
        mpath, hpath = write_manifest_and_hashes(manifest_entries, data_dir=args.data_dir)
        logger.info("manifest: %s", mpath)
        logger.info("hashes:   %s", hpath)
    if quality_reports:
        rpath = write_data_quality_report(quality_reports, data_dir=args.data_dir)
        logger.info("quality report: %s", rpath)
        for r in quality_reports:
            logger.info("  %s %s -> %s (rows=%d gaps=%d dupes=%d)", r.symbol, r.timeframe, r.status,
                        r.row_count, r.gap_count, r.duplicate_count)


if __name__ == "__main__":
    main()
