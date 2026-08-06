"""
delta_data_pipeline.py
=======================
Standalone Delta Exchange (GLOBAL) OHLCV data pipeline — built for feeding
the new institutional-grade ML engine (quant_feature_core.py) with clean,
consistently-shaped candle data.

Two independent data paths, ONE identical output schema:

    1. DeltaHistoricalFetcher  -> REST /v2/history/candles, auto-paginated
                                   (Delta caps each request at 2000 candles)
                                   Use for backtesting / training features
                                   like Hurst (R/S), permutation entropy,
                                   Kalman state-space fitting, frac-diff, etc.

    2. DeltaLiveFeed            -> WebSocket candlestick_<resolution> channel,
                                   auto-reconnecting background thread.
                                   Use for the engine's live decisioning.

Both return a pandas DataFrame indexed by UTC timestamp with columns
[open, high, low, close, volume] — so quant_feature_core.py never needs
to know or care whether it's looking at history or the live tape.

IMPORTANT — this module is intentionally isolated from your production stack:
  * No API key / secret is read anywhere in this file.
  * It only touches PUBLIC market-data endpoints (candles, candlestick ws
    channel) — nothing here can place, modify, or cancel an order.
  * It does not import, patch, or run alongside ml_engine.py / the live bot.
This is a "Phase 1"-style standalone module, same spirit as quant_feature_core.py.

Dependencies:
    pip install pandas requests websocket-client

One thing flagged honestly rather than guessed: I don't have a captured
sample of a live candlestick_1m websocket message to verify field names
against (my sandbox has no network access to hit Delta's API directly).
The parsing below follows Delta's documented v2 pattern (type / symbol /
candle_start_time / open / high / low / close / volume), and the feed
logs the FIRST raw message it receives at INFO level so you can eyeball
it in ~2 seconds and adjust `_on_message` below if any field name differs.
"""

import json
import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Callable, Optional

import pandas as pd
import requests
import websocket  # websocket-client package

# ---------------------------------------------------------------------------
# Config — the only block you should need to touch
# ---------------------------------------------------------------------------
REST_BASE_URL = "https://api.india.delta.exchange"   # Delta GLOBAL (NOT .india.)
WS_URL = "wss://public-socket.india.delta.exchange"         # Delta GLOBAL — cross-check this
                                                # against the constant your
                                                # existing delta_order_flow.py
                                                # already connects with; if that
                                                # file uses a different host,
                                                # use that one here instead.

DEFAULT_SYMBOL = "BTCUSD"     # Delta GLOBAL naming (not BTCUSD, that's India)
DEFAULT_RESOLUTION = "1m"
MAX_CANDLES_PER_REQUEST = 2000
REQUEST_TIMEOUT_SEC = 10
MAX_RETRIES = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("delta_data_pipeline")


DATA_DIR = "delta_dataset"


def save_dataframe(df: pd.DataFrame, path: str) -> str:
    """Persist a normalized market-data DataFrame as CSV for ML research."""
    import os
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    out = df.copy()
    out.to_csv(path, index=True, index_label="timestamp")
    logger.info("saved %s rows -> %s", len(out), path)
    return path



_RESOLUTION_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600,
    "12h": 43200, "1d": 86400, "1w": 604800,
}


def standardize(records: list) -> pd.DataFrame:
    """Turn Delta's raw candle records (REST or WS) into the one true schema
    every downstream consumer (quant_feature_core.py) should rely on."""
    cols = ["open", "high", "low", "close", "volume"]
    if not records:
        return pd.DataFrame(columns=cols).set_index(
            pd.DatetimeIndex([], tz="UTC", name="timestamp")
        )
    df = pd.DataFrame(records)
    df = df.rename(columns={"time": "timestamp", "candle_start_time": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df = df[["timestamp"] + cols]
    df[cols] = df[cols].astype(float)
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp")
    return df.set_index("timestamp")


# ---------------------------------------------------------------------------
# 1) Historical — REST, auto-paginated
# ---------------------------------------------------------------------------
class DeltaHistoricalFetcher:
    """Pulls historical OHLCV candles for backtesting / feature training.
    Handles Delta's 2000-candles-per-request cap transparently."""

    def __init__(self, base_url: str = REST_BASE_URL):
        self.base_url = base_url
        self.session = requests.Session()

    def fetch_window(self, symbol: str, resolution: str, start_ts: int, end_ts: int) -> list:
        """Single REST call, retried with backoff. Returns raw record list."""
        params = {"resolution": resolution, "symbol": symbol, "start": start_ts, "end": end_ts}
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(
                    f"{self.base_url}/v2/history/candles",
                    params=params,
                    timeout=REQUEST_TIMEOUT_SEC,
                )
                resp.raise_for_status()
                payload = resp.json()
                if not payload.get("success", False):
                    raise RuntimeError(f"Delta API returned success=false: {payload}")
                return payload.get("result", [])
            except (requests.RequestException, RuntimeError, ValueError) as e:
                last_err = e
                wait = min(2 ** attempt, 30)
                logger.warning(
                    f"fetch_window attempt {attempt}/{MAX_RETRIES} failed ({e}); retrying in {wait}s"
                )
                time.sleep(wait)
        raise RuntimeError(
            f"Failed fetching {symbol} {resolution} [{start_ts}:{end_ts}] "
            f"after {MAX_RETRIES} attempts: {last_err}"
        )

    def fetch_range(
        self,
        symbol: str = DEFAULT_SYMBOL,
        resolution: str = DEFAULT_RESOLUTION,
        days_back: int = 90,
        end_ts: Optional[int] = None,
    ) -> pd.DataFrame:
        """Walks backward from `end_ts` (default: now) across as many
        2000-candle windows as needed and returns one clean DataFrame."""
        if resolution not in _RESOLUTION_SECONDS:
            raise ValueError(f"Unknown resolution '{resolution}'. Valid: {list(_RESOLUTION_SECONDS)}")

        res_sec = _RESOLUTION_SECONDS[resolution]
        end_ts = end_ts or int(time.time())
        start_ts = end_ts - days_back * 86400
        window_span = MAX_CANDLES_PER_REQUEST * res_sec

        all_records: list = []
        cursor = start_ts
        n_windows = 0
        while cursor < end_ts:
            window_end = min(cursor + window_span, end_ts)
            n_windows += 1
            logger.info(
                f"[{n_windows}] fetching {symbol} {resolution} candles "
                f"{datetime.fromtimestamp(cursor, tz=timezone.utc).isoformat()} -> "
                f"{datetime.fromtimestamp(window_end, tz=timezone.utc).isoformat()}"
            )
            page = self.fetch_window(symbol, resolution, cursor, window_end)
            all_records.extend(page)
            cursor = window_end
            time.sleep(0.2)  # stay polite to the API across pages

        df = standardize(all_records)
        logger.info(f"done: {len(df)} candles for {symbol} ({resolution}, {days_back}d back)")
        return df


# ---------------------------------------------------------------------------
# 2) Live — WebSocket, auto-reconnecting
# ---------------------------------------------------------------------------
class DeltaLiveFeed:
    """Background-thread WebSocket subscriber for closed candlesticks.
    Buffers the most recent `buffer_size` candles and optionally fires
    `on_candle(record_dict)` as each new one arrives."""

    def __init__(
        self,
        symbol: str = DEFAULT_SYMBOL,
        resolution: str = DEFAULT_RESOLUTION,
        on_candle: Optional[Callable[[dict], None]] = None,
        buffer_size: int = 5000,
        ws_url: str = WS_URL,
    ):
        self.symbol = symbol
        self.resolution = resolution
        self.channel = f"candlestick_{resolution}"
        self.on_candle = on_candle
        self.ws_url = ws_url
        self.buffer: deque = deque(maxlen=buffer_size)
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()
        self._logged_sample = False

    # -- websocket callbacks -------------------------------------------------
    def _on_open(self, ws):
        logger.info(f"WS connected -> subscribing {self.channel} for {self.symbol}")
        ws.send(json.dumps({
            "type": "subscribe",
            "payload": {"channels": [{"name": self.channel, "symbols": [self.symbol]}]},
        }))

    def _on_message(self, ws, message):
        # Current Delta India public candlestick schema:
        # c/h/l/o = close/high/low/open, v = volume, ts = microseconds.
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        if data.get("type") != self.channel:
            return

        ts_us = data.get("ts")
        if ts_us is None:
            return

        try:
            record = {
                "time": int(ts_us) / 1_000_000,
                "open": float(data["o"]),
                "high": float(data["h"]),
                "low": float(data["l"]),
                "close": float(data["c"]),
                "volume": float(data.get("v", 0) or 0),
            }
        except (KeyError, TypeError, ValueError):
            logger.warning("Malformed candlestick message ignored: %s", message[:400])
            return

        self.buffer.append(record)
        if self.on_candle:
            self.on_candle(record)

    def _on_error(self, ws, error):
        logger.warning(f"WS error: {error}")

    def _on_close(self, ws, code, msg):
        logger.warning(f"WS closed (code={code}, msg={msg})")

    def _run(self):
        while not self._stop_flag.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                logger.warning(f"WS loop crashed: {e}")
            if not self._stop_flag.is_set():
                logger.info("reconnecting in 5s...")
                time.sleep(5)

    # -- public interface ------------------------------------------------
    def start(self) -> "DeltaLiveFeed":
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop_flag.set()
        if self._ws:
            self._ws.close()

    def get_dataframe(self) -> pd.DataFrame:
        """Snapshot of everything buffered so far, same schema as historical."""
        return standardize(list(self.buffer))


# ---------------------------------------------------------------------------
# Demo / sanity check — run directly: python delta_data_pipeline.py
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # ---------------------------------------------------------------
    # DATA EXPORT MODE
    # This does NOT touch the existing live trading bot or API keys.
    # It only downloads public Delta India market data and saves files.
    # ---------------------------------------------------------------
    import os

    os.makedirs(DATA_DIR, exist_ok=True)

    DAYS_BACK = 30  # first test; change to 180 after successful verification

    print(f"\n=== 1) Historical backfill ({DAYS_BACK}d) ===")
    hist = DeltaHistoricalFetcher()
    df_hist = hist.fetch_range(
        symbol=DEFAULT_SYMBOL,
        resolution=DEFAULT_RESOLUTION,
        days_back=DAYS_BACK,
    )

    historical_path = (
        f"{DATA_DIR}/{DEFAULT_SYMBOL}_{DEFAULT_RESOLUTION}_{DAYS_BACK}d.csv"
    )
    save_dataframe(df_hist, historical_path)

    print(df_hist.tail())
    print(f"rows: {len(df_hist)}")
    print(f"SAVED: {historical_path}")

    print("\n=== 2) Live feed — watching for 90s (Ctrl+C to stop early) ===")
    feed = DeltaLiveFeed(
        symbol=DEFAULT_SYMBOL,
        resolution=DEFAULT_RESOLUTION
    ).start()

    try:
        time.sleep(90)
    except KeyboardInterrupt:
        pass

    feed.stop()
    df_live = feed.get_dataframe()

    live_path = f"{DATA_DIR}/{DEFAULT_SYMBOL}_{DEFAULT_RESOLUTION}_live_snapshot.csv"
    save_dataframe(df_live, live_path)

    print(df_live.tail())
    print(f"rows buffered: {len(df_live)}")
    print(f"SAVED: {live_path}")

    print("\n=== DATA EXPORT COMPLETE ===")
    print(f"Historical: {historical_path}")
    print(f"Live snapshot: {live_path}")
