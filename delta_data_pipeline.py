"""
APEX NEXUS — Delta Market Data -> ML Engine Integration
=========================================================
Purpose:
  Connect the existing Delta market-data pipeline to the new ML engine
  WITHOUT replacing or modifying the existing live execution/API-key layer.

Architecture:
  Delta public market data
        -> delta_data_pipeline.py
        -> normalized OHLCV DataFrame
        -> ML feature/decision engine

This module DOES NOT:
  - read API keys/secrets
  - place/cancel/modify orders
  - replace the existing live bot
  - depend on TradingView signals

This module DOES:
  - fetch historical candles for research/backtesting
  - maintain a live candle feed
  - expose the same normalized schema to the ML engine
  - provide a safe adapter boundary for the ML engine
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

import pandas as pd

from delta_data_pipeline import DeltaHistoricalFetcher, DeltaLiveFeed

logger = logging.getLogger("apex_delta_ml_integration")


@dataclass(frozen=True)
class MarketDataConfig:
    symbol: str = "BTCUSD"
    resolution: str = "1m"
    history_days: int = 180
    live_buffer: int = 5000


class DeltaMLDataAdapter:
    """
    Single data boundary for the new ML engine.

    The ML engine should consume:
        pd.DataFrame indexed by UTC timestamp
        columns: open, high, low, close, volume
    """

    def __init__(self, config: MarketDataConfig = MarketDataConfig()):
        self.config = config
        self.historical = DeltaHistoricalFetcher()
        self.live: Optional[DeltaLiveFeed] = None

    def load_history(self) -> pd.DataFrame:
        df = self.historical.fetch_range(
            symbol=self.config.symbol,
            resolution=self.config.resolution,
            days_back=self.config.history_days,
        )
        return self._validate(df, "historical")

    def start_live(self, on_candle: Optional[Callable[[dict], None]] = None):
        self.live = DeltaLiveFeed(
            symbol=self.config.symbol,
            resolution=self.config.resolution,
            on_candle=on_candle,
            buffer_size=self.config.live_buffer,
        ).start()
        return self.live

    def live_snapshot(self) -> pd.DataFrame:
        if self.live is None:
            raise RuntimeError("Live feed has not been started.")
        return self._validate(self.live.get_dataframe(), "live")

    @staticmethod
    def _validate(df: pd.DataFrame, source: str) -> pd.DataFrame:
        required = ["open", "high", "low", "close", "volume"]

        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"{source}: expected pandas DataFrame")

        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"{source}: missing columns: {missing}")

        if df.index.tz is None:
            raise ValueError(f"{source}: timestamp index must be timezone-aware UTC")

        out = df.copy()
        out = out.sort_index()
        out = out[~out.index.duplicated(keep="last")]

        if out[required].isnull().any().any():
            logger.warning("%s: null values found in OHLCV data", source)

        # Basic OHLC sanity checks; do not invent/fill market data.
        bad = (
            (out["high"] < out[["open", "close", "low"]].max(axis=1))
            | (out["low"] > out[["open", "close", "high"]].min(axis=1))
            | (out["volume"] < 0)
        )

        if bool(bad.any()):
            raise ValueError(
                f"{source}: {int(bad.sum())} invalid OHLCV rows detected"
            )

        return out


def example_ml_hook(candle: dict[str, Any]) -> None:
    """
    Replace this callback with the ML engine's EXISTING public entry point.

    IMPORTANT:
      This function is intentionally not connected to order execution.
      A model decision must pass through the project's existing risk/execution
      controls before any real order is submitted.
    """
    logger.info(
        "New market candle received: %s",
        candle,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    adapter = DeltaMLDataAdapter(
        MarketDataConfig(
            symbol="BTCUSD",
            resolution="1m",
            history_days=30,
        )
    )

    history = adapter.load_history()
    print("Historical rows:", len(history))
    print(history.tail())

    adapter.start_live(on_candle=example_ml_hook)

    print("Live feed started. Press Ctrl+C to stop.")
    try:
        import time
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        if adapter.live:
            adapter.live.stop()
        print("Stopped.")
