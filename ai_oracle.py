#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
════════════════════════════════════════════════════════════════════════════════
AI ORACLE — Standalone Footprint + Gemini Consensus Service (Phase 1)
════════════════════════════════════════════════════════════════════════════════
Runs as a completely separate process from APEX_NEXUS_SELF_CHECK.py. It never
imports from, or gets imported by, the main bot — the only thing the two share
is the SQLite file `apex_state.db` (specifically the `control_flags` table),
so this can be deployed, restarted, or crash independently without touching
live trading.

WHAT IT DOES, every ORACLE_INTERVAL_S seconds, for every symbol in
ORACLE_SYMBOLS:
  1. Pulls Delta Exchange's public L2 orderbook + recent trades for the
     symbol (no API key required — same public endpoints the main bot's
     fetch_live_orderbook_imbalance() already uses).
  2. Aggregates that into a compact "footprint" JSON: bid/ask depth
     imbalance, recent taker buy/sell volume split, and trade count.
  3. Sends that footprint to the Gemini API with a strict system prompt and
     asks for exactly one of: BULLISH / BEARISH / NEUTRAL.
  4. Writes the result into control_flags under key `ai_consensus_<SYMBOL>`
     (and the most-recently-processed symbol's rating also mirrors to the
     bare `ai_consensus` key, for a single-symbol Auto-Pilot to read without
     needing to know which symbol is "the" one).

DEPLOY:
  pip install aiohttp
  Required env vars: GEMINI_API_KEY
  Optional env vars: ORACLE_SYMBOLS (default "BTCUSD"), ORACLE_INTERVAL_S
    (default 60), DELTA_REGION ("global"|"india", default "global"),
    DELTA_USER_AGENT, ORACLE_DB_PATH (default "apex_state.db"),
    ORACLE_ORDERBOOK_DEPTH (default 10), ORACLE_TRADE_LOOKBACK (default 100)
  Run:
    python ai_oracle.py
════════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import json
import time
import asyncio
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp

# ════════════════════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════════════════════
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

REGION = os.environ.get("DELTA_REGION", "global").strip().lower()
BASE_URLS = {
    "global": "https://api.delta.exchange",
    "india": "https://api.india.delta.exchange",
}
if REGION not in BASE_URLS:
    REGION = "global"
BASE_URL = BASE_URLS[REGION]

DELTA_USER_AGENT = os.environ.get("DELTA_USER_AGENT", "APEX-NEXUS-AIOracle/1.0")

ORACLE_SYMBOLS = [s.strip().upper() for s in
                  os.environ.get("ORACLE_SYMBOLS", "BTCUSD").split(",") if s.strip()]
ORACLE_INTERVAL_S = int(os.environ.get("ORACLE_INTERVAL_S", "60"))
ORDERBOOK_DEPTH = int(os.environ.get("ORACLE_ORDERBOOK_DEPTH", "10"))
TRADE_LOOKBACK = int(os.environ.get("ORACLE_TRADE_LOOKBACK", "100"))

DB_PATH = os.environ.get("ORACLE_DB_PATH", "apex_state.db")

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")
GEMINI_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent"
)

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=8)
MAX_HTTP_RETRIES = 3
RETRY_BACKOFF_BASE_S = 1.5

VALID_CONSENSUS = {"BULLISH", "BEARISH", "NEUTRAL"}

# ════════════════════════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ai_oracle] %(levelname)s %(message)s",
)
log = logging.getLogger("ai_oracle")


# ════════════════════════════════════════════════════════════════════════════════
# DATABASE — shares apex_state.db with the main bot via control_flags only.
# WAL mode + busy_timeout mirror the main bot's db() helper exactly, so both
# processes can read/write the same file concurrently without lock errors.
# ════════════════════════════════════════════════════════════════════════════════
def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _ensure_control_flags_table(conn: sqlite3.Connection):
    # Defensive only: the main bot's init_db() normally creates this table
    # first. If ai_oracle.py is ever started before the main bot's very first
    # boot, this guarantees it doesn't crash on a missing table.
    conn.execute("""CREATE TABLE IF NOT EXISTS control_flags (
        key TEXT PRIMARY KEY, value TEXT)""")
    conn.commit()


def write_control_flag_sync(key: str, value: str, retries: int = 5) -> bool:
    """Synchronous, retrying, WAL-safe write. Called via asyncio.to_thread()
    so it never blocks the async event loop. Returns True on success, False
    if every retry was exhausted (caller must treat that as non-fatal — a
    missed oracle tick is fine, a crashed process is not)."""
    last_err = None
    for attempt in range(1, retries + 1):
        conn = None
        try:
            conn = _get_conn()
            _ensure_control_flags_table(conn)
            conn.execute(
                "INSERT OR REPLACE INTO control_flags (key, value) VALUES (?,?)",
                (key, value),
            )
            conn.commit()
            return True
        except sqlite3.OperationalError as e:
            # "database is locked" — back off and retry rather than failing
            # outright, since the main bot may be mid-write on the same file.
            last_err = e
            time.sleep(0.3 * attempt)
        except Exception as e:
            last_err = e
            break
        finally:
            if conn is not None:
                conn.close()
    log.error(f"write_control_flag_sync failed for key={key!r} after {retries} attempts: {last_err}")
    return False


async def write_control_flag(key: str, value: str) -> bool:
    return await asyncio.to_thread(write_control_flag_sync, key, value)


# ════════════════════════════════════════════════════════════════════════════════
# DELTA EXCHANGE — DEEP DATA FETCHER
# Public endpoints only, no API key needed (matches the main bot's own
# fetch_live_orderbook_imbalance() at GET /v2/l2orderbook/{symbol}).
# ════════════════════════════════════════════════════════════════════════════════
async def _get_json_with_retries(session: aiohttp.ClientSession, url: str,
                                  params: Optional[Dict] = None) -> Optional[Dict]:
    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            async with session.get(url, params=params, timeout=HTTP_TIMEOUT) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.warning(f"GET {url} -> HTTP {resp.status}: {body[:200]}")
                    if attempt == MAX_HTTP_RETRIES:
                        return None
                    await asyncio.sleep(RETRY_BACKOFF_BASE_S * attempt)
                    continue
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning(f"GET {url} attempt {attempt}/{MAX_HTTP_RETRIES} failed: {e}")
            if attempt == MAX_HTTP_RETRIES:
                return None
            await asyncio.sleep(RETRY_BACKOFF_BASE_S * attempt)
    return None


async def fetch_orderbook(session: aiohttp.ClientSession, symbol: str) -> Optional[Dict]:
    data = await _get_json_with_retries(session, f"{BASE_URL}/v2/l2orderbook/{symbol}")
    if not data:
        return None
    result = data.get("result", {}) or {}
    buy_levels = result.get("buy", [])[:ORDERBOOK_DEPTH]
    sell_levels = result.get("sell", [])[:ORDERBOOK_DEPTH]

    def _sum_size(levels):
        total = 0.0
        for lvl in levels:
            try:
                total += float(lvl.get("size", 0))
            except (TypeError, ValueError):
                continue
        return total

    bid_qty = _sum_size(buy_levels)
    ask_qty = _sum_size(sell_levels)
    total_qty = bid_qty + ask_qty
    imbalance = 0.0 if total_qty <= 0 else max(-1.0, min(1.0, (bid_qty - ask_qty) / total_qty))

    best_bid = float(buy_levels[0]["price"]) if buy_levels and buy_levels[0].get("price") else None
    best_ask = float(sell_levels[0]["price"]) if sell_levels and sell_levels[0].get("price") else None
    spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

    return {
        "bid_depth_qty": round(bid_qty, 6),
        "ask_depth_qty": round(ask_qty, 6),
        "depth_imbalance": round(imbalance, 4),  # -1 = all asks, +1 = all bids
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "levels_used": {"buy": len(buy_levels), "sell": len(sell_levels)},
    }


async def fetch_recent_trades(session: aiohttp.ClientSession, symbol: str) -> Optional[Dict]:
    data = await _get_json_with_retries(
        session, f"{BASE_URL}/v2/trades/{symbol}", params={"page_size": TRADE_LOOKBACK}
    )
    if not data:
        return None
    trades = data.get("result", []) or []

    buy_vol = 0.0
    sell_vol = 0.0
    for t in trades:
        try:
            size = float(t.get("size", 0))
        except (TypeError, ValueError):
            continue
        side = (t.get("buyer_role") or t.get("side") or "").lower()
        # Delta's public trade feed marks taker side; treat unknown as
        # neither-side so it never silently skews the split.
        if side == "taker" and t.get("side"):
            side = t.get("side", "").lower()
        if side == "buy":
            buy_vol += size
        elif side == "sell":
            sell_vol += size

    total_vol = buy_vol + sell_vol
    taker_delta = 0.0 if total_vol <= 0 else max(-1.0, min(1.0, (buy_vol - sell_vol) / total_vol))

    return {
        "trade_count": len(trades),
        "taker_buy_volume": round(buy_vol, 6),
        "taker_sell_volume": round(sell_vol, 6),
        "taker_delta": round(taker_delta, 4),  # -1 = all selling, +1 = all buying
    }


async def fetch_delta_footprint(session: aiohttp.ClientSession, symbol: str) -> Dict:
    """Aggregates orderbook depth + recent trade flow into one clean footprint
    JSON for a symbol. Individual sub-fetch failures degrade gracefully
    (missing sections rather than raising) so one bad endpoint never blocks
    the whole tick."""
    orderbook, trades = await asyncio.gather(
        fetch_orderbook(session, symbol),
        fetch_recent_trades(session, symbol),
    )
    footprint = {
        "symbol": symbol,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "orderbook": orderbook,   # None if the fetch failed
        "order_flow": trades,     # None if the fetch failed
    }
    return footprint


# ════════════════════════════════════════════════════════════════════════════════
# GEMINI CONSENSUS AGENT
# ════════════════════════════════════════════════════════════════════════════════
GEMINI_SYSTEM_PROMPT = (
    "You are a strict market micro-structure classifier for a crypto perpetual "
    "futures trading bot. You will be given a JSON 'footprint' snapshot for one "
    "symbol: order book depth imbalance (bid vs ask resting size) and recent "
    "taker order-flow (buy vs sell volume split from actual executed trades). "
    "Classify the immediate directional pressure implied by this data as "
    "exactly one of these three words, and nothing else: BULLISH, BEARISH, "
    "NEUTRAL. Respond with ONLY that single word — no punctuation, no "
    "explanation, no markdown. If the data is missing, thin, or contradictory, "
    "respond NEUTRAL."
)


async def get_gemini_consensus(session: aiohttp.ClientSession, footprint: Dict) -> str:
    """Returns one of BULLISH/BEARISH/NEUTRAL. Any failure (missing key,
    network error, malformed response, off-list answer) safely falls back to
    NEUTRAL rather than ever raising — a stalled AI opinion should never be
    treated as a directional signal."""
    if not GEMINI_API_KEY:
        log.error("GEMINI_API_KEY is not set — defaulting consensus to NEUTRAL.")
        return "NEUTRAL"

    body = {
        "system_instruction": {"parts": [{"text": GEMINI_SYSTEM_PROMPT}]},
        "contents": [
            {"role": "user", "parts": [{"text": json.dumps(footprint, default=str)}]}
        ],
        "generationConfig": {
            "temperature": 0.0,
            "maxOutputTokens": 8,
        },
    }

    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            async with session.post(
                GEMINI_URL,
                params={"key": GEMINI_API_KEY},
                json=body,
                timeout=HTTP_TIMEOUT,
            ) as resp:
                if resp.status != 200:
                    err_text = await resp.text()
                    log.warning(f"Gemini HTTP {resp.status}: {err_text[:200]}")
                    if attempt == MAX_HTTP_RETRIES:
                        return "NEUTRAL"
                    await asyncio.sleep(RETRY_BACKOFF_BASE_S * attempt)
                    continue

                data = await resp.json()
                candidates = data.get("candidates", [])
                if not candidates:
                    log.warning(f"Gemini returned no candidates: {data}")
                    return "NEUTRAL"

                parts = candidates[0].get("content", {}).get("parts", [])
                raw_text = "".join(p.get("text", "") for p in parts).strip().upper()
                # Extract the first valid label found, in case the model
                # wraps it in stray punctuation/whitespace despite instructions.
                for label in VALID_CONSENSUS:
                    if label in raw_text:
                        return label
                log.warning(f"Gemini returned an off-list answer, defaulting to NEUTRAL: {raw_text!r}")
                return "NEUTRAL"

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning(f"Gemini call attempt {attempt}/{MAX_HTTP_RETRIES} failed: {e}")
            if attempt == MAX_HTTP_RETRIES:
                return "NEUTRAL"
            await asyncio.sleep(RETRY_BACKOFF_BASE_S * attempt)
        except Exception as e:
            log.error(f"Unexpected Gemini error, defaulting to NEUTRAL: {e}")
            return "NEUTRAL"

    return "NEUTRAL"


# ════════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ════════════════════════════════════════════════════════════════════════════════
async def process_symbol(session: aiohttp.ClientSession, symbol: str, is_last: bool):
    try:
        footprint = await fetch_delta_footprint(session, symbol)
        consensus = await get_gemini_consensus(session, footprint)

        ok = await write_control_flag(f"ai_consensus_{symbol}", consensus)
        await write_control_flag(f"ai_consensus_{symbol}_updated_at",
                                  datetime.now(timezone.utc).isoformat())

        # Mirror the LAST symbol processed each tick onto the bare
        # `ai_consensus` key too, so a single-symbol Auto-Pilot mode (Phase 2)
        # has one obvious key to read without needing to know the symbol list.
        if is_last:
            await write_control_flag("ai_consensus", consensus)
            await write_control_flag("ai_consensus_symbol", symbol)
            await write_control_flag("ai_consensus_updated_at",
                                      datetime.now(timezone.utc).isoformat())

        log.info(f"{symbol}: consensus={consensus} "
                 f"depth_imbalance={(footprint['orderbook'] or {}).get('depth_imbalance')} "
                 f"taker_delta={(footprint['order_flow'] or {}).get('taker_delta')} "
                 f"db_write_ok={ok}")

    except Exception as e:
        # A single symbol's failure must never take down the whole loop.
        log.error(f"process_symbol({symbol}) failed: {e}", exc_info=True)


async def oracle_loop():
    if not GEMINI_API_KEY:
        log.error("❌ GEMINI_API_KEY is not set. The oracle will run but every "
                  "consensus will default to NEUTRAL until it's configured.")
    if not ORACLE_SYMBOLS:
        log.error("❌ No symbols configured (ORACLE_SYMBOLS). Exiting.")
        sys.exit(1)

    log.info(f"🚀 AI Oracle starting — region={REGION} base_url={BASE_URL} "
              f"symbols={ORACLE_SYMBOLS} interval={ORACLE_INTERVAL_S}s "
              f"db={DB_PATH} model={GEMINI_MODEL}")

    headers = {"User-Agent": DELTA_USER_AGENT}
    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            tick_start = time.monotonic()
            for i, symbol in enumerate(ORACLE_SYMBOLS):
                await process_symbol(session, symbol, is_last=(i == len(ORACLE_SYMBOLS) - 1))

            elapsed = time.monotonic() - tick_start
            sleep_for = max(1.0, ORACLE_INTERVAL_S - elapsed)
            await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    try:
        asyncio.run(oracle_loop())
    except KeyboardInterrupt:
        log.info("AI Oracle stopped by user.")
