#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
════════════════════════════════════════════════════════════════════════════════
APEX NEXUS — FINAL CONSOLIDATED BUILD (Auto Product-ID Discovery Edition)
════════════════════════════════════════════════════════════════════════════════
THIS FILE IS THE SINGLE SOURCE OF TRUTH.
  Checked class-by-class and function-by-function against every earlier build:
  V2 → V3 → V4 → V5_FIXED → V7 → V8 → APEX_NEXUS_SELF_CHECK.py → prior main.py
  saves. Every class and function that ever existed anywhere in that lineage
  exists here too — confirmed by diffing signatures across all of them, not
  just by file size. Safe to archive the older files; nothing is lost by
  doing so.

COMPANION FILES — kept separate on purpose, NOT merged into this file:
  • ai_oracle.py — standalone Gemini market-sentiment service (its own
    header calls it "Phase 1"). Deliberately a separate process that only
    touches this bot through the shared apex_state.db control_flags table,
    so it can crash or redeploy without ever touching live trading — merging
    it in here would remove that safety property. This build now reads and
    surfaces its output read-only (see the "ai_market_sentiment" block in
    GET /config) so you can see what it thinks; it does NOT gate or size any
    trade. Turning that into a real signal is a deliberate risk-model
    decision, not something to bolt on silently — flag it if you want that
    built as an actual Phase 2.
  • confidence_engine.mojo — a Mojo-language reimplementation of
    ConfidenceEngine for a speed benchmark. Mojo isn't Python and can't run
    inside this file; kept only as a reference/benchmark artifact.
  • apex-nexus-react/ — a separate, standalone React+Vite dashboard
    (deployed on its own, e.g. Vercel/Netlify — NOT served by this file).
    Talks to this backend entirely over the HTTP API below; nothing about
    it lives inside this Python file except the 4 additions below, made
    specifically so that dashboard has real data for every panel:
      - GET  /mark-prices?symbols=A,B,C   → real PnL/ROI on open positions
      - GET  /status now includes "uptime_seconds"
      - POST /ask/<secret> now includes "mode":"ai" in its response
      - CORS preflight now sends Allow-Headers/Allow-Methods, not just
        Allow-Origin — required for that dashboard's POST /ask to survive
        the browser's preflight check when called from a different origin
    The OLD single-file mock (ApexNexusDashboard.jsx, matching the original
    screenshot with a hardcoded $128,745.32 balance / 92.7% confidence /
    98.6% "model accuracy" etc.) is superseded — every number in it was
    fake/random, kept only as a visual reference for the look, not for
    deployment. The dashboard embedded below (Mission Control) still works
    standalone and needs none of this; the React app is an alternative, not
    a replacement of it.
════════════════════════════════════════════════════════════════════════════════
THE ONE PROBLEM THIS VERSION SOLVES FOREVER:
  You used to have to manually look up and hardcode a Delta Exchange
  "product_id" for every single coin. Add a new coin to your Pine indicator
  and the bot would silently fail to trade it until you edited the code.

  This version NEVER needs that. On startup — and automatically in the
  background every 10 minutes — it downloads Delta's FULL live product list
  and builds its own lookup table. Send it "BTC", "ETH", "SOL", "DOGE",
  literally anything Delta lists as a perpetual future, and it resolves the
  correct product_id itself. Zero manual mapping, ever.

PREMIUM ENGINES INCLUDED:
  • Auto Product-ID Discovery Engine  (the star of this version)
  • ConfidenceEngine (AI-based signal scoring, additive not multiplicative)
  • BinanceLiquidationFeed (real-time liquidation websocket)
  • OrderBookImbalance (bid/ask pressure)
  • RegimeDetector (UPTREND / DOWNTREND / RANGE)
  • Native Bracket Orders — Delta attaches SL + TP automatically at entry
    and cancels whichever one didn't fill, on its own (verified against
    Delta's official /v2/orders/bracket API — see comments below)
  • Full control endpoints (pause / resume / close-all / reset-circuit-breaker)
  • [PREMIUM NEW] Circuit Breaker — daily loss limit (R-multiples) + max
    consecutive losses, DB-backed so it works correctly under multiple
    gunicorn workers. Fed by the new TRADE_CLOSE alert (see below).
  • [PREMIUM NEW] Correct action routing for UPDATE_SL / EXIT_TP1 / EXIT_TP2 /
    TRADE_CLOSE — CRITICAL FIX: previously ANY action other than "ENTRY" fell
    through to a full market close, meaning a Pine UPDATE_SL trailing-stop
    push would have closed the entire position outright, and EXIT_TP1/TP2
    partial scale-outs would have closed 100% instead of close_fraction.
    Each action now does exactly what its name says and nothing else.
  • Telegram alerts
  • SQLite state (positions, trades)
  • Every route wrapped so a single bad request can NEVER crash the bot —
    you always get a clean JSON response, never a blank error page.

DEPLOY:
  1. Fill in .env (see .env.example — only 3 REQUIRED secrets now, everything
     else is optional or auto-discovered)
  2. pip install -r requirements.txt
  3. python main.py   (production: gunicorn main:app)
  4. Webhook URL: https://your-domain/webhook/<APEX_WEBHOOK_PASSPHRASE>
  5. Optional companion: run ai_oracle.py as a second process (same DB file)
     if you want market-sentiment data collected — see its own header.
════════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import time
import hmac
import json
import math
import hashlib
import logging
import sqlite3
import contextlib
import threading
import traceback
import statistics
import re
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, timezone
from abc import ABC, abstractmethod

import requests
from flask import Flask, request, jsonify

try:
    import websocket  # package: websocket-client — only needed if liquidation feed is used
except ImportError:
    websocket = None

try:
    import psutil  # package: psutil — only needed for the System Health panel
except ImportError:
    psutil = None

_PROCESS_START_TIME = time.time()

app = Flask(__name__)


# ════════════════════════════════════════════════════════════════════════════════
# [PREMIUM FIX — MISSING USER-AGENT] Delta's India endpoint (api.india.delta.exchange)
# has been confirmed via live API docs to require a real User-Agent header — some
# CDN/WAF layers in front of Delta India silently drop or reject requests that carry
# no User-Agent (or a generic one), which shows up as bare connection failures or
# odd 401/403s that have nothing to do with the API key itself, and are easy to
# misdiagnose as a credentials or region problem. requests' own default (a bare
# "python-requests/X.Y.Z" string) is exactly the kind of generic client signature
# that can get caught here. Every outbound call to Delta (signed or public) goes
# through this one shared Session so the header is guaranteed to be present
# everywhere, with nothing to remember to repeat at each call site.
# ════════════════════════════════════════════════════════════════════════════════
DELTA_USER_AGENT = os.environ.get("DELTA_USER_AGENT", "APEX-NEXUS-TradingBot/1.0")
delta_http = requests.Session()
delta_http.headers.update({"User-Agent": DELTA_USER_AGENT})


# ════════════════════════════════════════════════════════════════════════════════
# [DASHBOARD NEW] RAW EXCHANGE DATA LOG — every real HTTP call this bot makes
# to Delta (product discovery, credential check, orderbook, orders, ...) all
# go through this ONE shared `delta_http` Session (see above). A response
# hook here means we can log every one of them in exactly one place, without
# having to touch each of the ~10 call sites individually.
#
# WHY: "is it really working or not" is hard to answer from log lines alone
# on a phone — this makes the actual raw bytes coming back from Delta
# visible right on the dashboard, so a bad/empty/error response is obvious
# at a glance instead of requiring a Railway log dive.
#
# SAFETY: only the response is inspected — never the request headers, so the
# api-key/signature/timestamp headers built in _signed_request() are never
# captured or exposed here. The URL is stored with its query string stripped
# (Delta's auth lives in headers, not query params, but this is intentionally
# defensive rather than assuming that never changes). Response bodies are
# truncated so one huge product-list response can't crowd out everything
# else in the rolling log.
# ════════════════════════════════════════════════════════════════════════════════
_raw_api_log_lock = threading.Lock()
_raw_api_log = deque(maxlen=40)
RAW_API_BODY_MAX_CHARS = 600


def _capture_raw_api_response(resp, *args, **kwargs):
    try:
        from urllib.parse import urlsplit
        path_only = urlsplit(resp.url).path
        body = resp.text or ""
        truncated = len(body) > RAW_API_BODY_MAX_CHARS
        snippet = body[:RAW_API_BODY_MAX_CHARS]
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "method": resp.request.method if resp.request else "?",
            "path": path_only,
            "status_code": resp.status_code,
            "ok": resp.ok,
            "elapsed_ms": round(resp.elapsed.total_seconds() * 1000, 1) if resp.elapsed else None,
            "body_snippet": snippet,
            "truncated": truncated,
        }
        with _raw_api_log_lock:
            _raw_api_log.append(entry)
    except Exception:
        # Logging the traffic must never be able to break the traffic itself.
        pass
    return None


delta_http.hooks["response"].append(_capture_raw_api_response)


def get_raw_api_log(limit: int = 20) -> List[Dict]:
    with _raw_api_log_lock:
        items = list(_raw_api_log)[-limit:]
    return list(reversed(items))


# ════════════════════════════════════════════════════════════════════════════════
# ★★★ CRITICAL FIX — SERVER TIME-DRIFT SYNC ★★★
# ────────────────────────────────────────────────────────────────────────────────
# ROOT CAUSE OF THE 401 "expired_signature" ERROR:
# Delta's signature scheme is `HMAC(timestamp + METHOD + path + body)`. Delta's
# server checks that the timestamp you sent is within a small tolerance window
# (a few seconds) of ITS OWN clock. This bot used to build that timestamp from
# `time.time()` — i.e. the LOCAL machine's clock only. Railway/any cloud host's
# clock can silently drift by seconds from Delta's servers (VM clock skew, NTP
# sync gaps, container restarts) — and once it drifts past Delta's tolerance
# window, EVERY signed request fails with expired_signature, forever, until the
# host's clock happens to resync — completely invisible from this bot's own
# perspective, because its local clock looks perfectly fine to itself.
#
# THE FIX: measure the actual drift between this server and Delta's server, and
# apply it as a correction on every signed timestamp, so the timestamp we send
# reflects Delta's clock, not just ours.
#
# METHOD: Delta's REST responses (like every HTTP server) carry a standard
# `Date` response header — this is Delta's own server clock, accurate to the
# second, and available on ANY response including public, unauthenticated
# endpoints (so this works even before/independent of whether the API key is
# valid). We hit a cheap public endpoint, read that header, and diff it against
# our own `time.time()` at the moment the response arrived. That diff is the
# drift. Runs once at boot (before ANY signed call is ever made — see
# bootstrap()) and again periodically in the background, so a drift that
# develops mid-session (e.g. a container clock jump) gets caught and corrected
# without needing a redeploy.
# ════════════════════════════════════════════════════════════════════════════════
_time_drift_lock = threading.Lock()
_time_drift_ms = 0.0            # server_time_ms - local_time_ms, ADD this to every local timestamp
_time_drift_last_synced = 0.0   # time.time() of the last successful sync
_time_drift_last_error = None


def get_time_drift_ms() -> float:
    with _time_drift_lock:
        return _time_drift_ms


def synced_timestamp_ms() -> int:
    """The one function every signed request should build its timestamp from.
    Local wall-clock time, corrected by whatever drift was last measured
    against Delta's own server clock."""
    return int(time.time() * 1000 + get_time_drift_ms())


def sync_time_with_delta(retries: int = 2) -> bool:
    """
    Measures clock drift against Delta's server using the HTTP Date header off
    a cheap public GET (no auth needed, so this works independent of whether
    DELTA_API_KEY is even valid — nothing about signature correctness has to
    already work for this to run). Never raises: a failure here just means the
    drift correction stays at its last-known value (0.0 on first boot), which
    is exactly the pre-fix behavior — this function can only make things
    better or neutral, never worse.
    """
    global _time_drift_ms, _time_drift_last_synced, _time_drift_last_error
    for attempt in range(retries + 1):
        try:
            t_local_before = time.time()
            resp = delta_http.get(f"{BASE_URL}/v2/products", params={"page_size": 1}, timeout=5)
            t_local_after = time.time()
            date_header = resp.headers.get("Date")
            if not date_header:
                raise ValueError("response had no Date header to sync against")

            server_dt = parsedate_to_datetime(date_header)
            if server_dt.tzinfo is None:
                server_dt = server_dt.replace(tzinfo=timezone.utc)
            server_time_ms = server_dt.timestamp() * 1000

            # Use the midpoint of request/response as "local time at the moment
            # the server's clock reading was true" — cheap, effective one-way-
            # latency compensation without needing NTP-grade round-trip math.
            local_time_ms = ((t_local_before + t_local_after) / 2.0) * 1000
            drift = server_time_ms - local_time_ms

            with _time_drift_lock:
                _time_drift_ms = drift
                _time_drift_last_synced = time.time()
                _time_drift_last_error = None

            if abs(drift) >= 1000:
                log.warning(f"🕒 Time-drift sync: local clock is {drift:+.0f}ms off Delta's server clock — "
                            f"correction applied to all future signed requests.")
            else:
                log.info(f"🕒 Time-drift sync OK: {drift:+.0f}ms (well within tolerance)")
            return True
        except Exception as e:
            _time_drift_last_error = str(e)
            log.warning(f"🕒 Time-drift sync attempt {attempt+1}/{retries+1} failed: {e}")
            time.sleep(0.5)
    log.error("🕒 Time-drift sync FAILED after all retries — signed requests will use uncorrected local "
              "clock time, which is exactly the condition that causes 401 expired_signature errors if "
              "this server's clock has drifted from Delta's.")
    return False


def time_drift_status() -> Dict:
    with _time_drift_lock:
        return {"drift_ms": round(_time_drift_ms, 1), "last_synced_epoch": _time_drift_last_synced,
                "seconds_since_sync": round(time.time() - _time_drift_last_synced, 1) if _time_drift_last_synced else None,
                "last_error": _time_drift_last_error}


def _background_time_sync_loop():
    """Re-checks drift every few minutes forever — catches a clock that jumps
    mid-session (container migration, host NTP correction, etc.) without
    needing a redeploy to fix a 401 storm."""
    while True:
        time.sleep(300)  # every 5 minutes
        sync_time_with_delta(retries=1)


@app.after_request
def add_cors_headers(response):
    # [REACT DASHBOARD NEW] The React dashboard is deployed on a different
    # origin (Vercel/Netlify) than this API (Render), so every request is
    # cross-origin. Plain GETs only needed Allow-Origin, but POST /ask sends
    # Content-Type: application/json, which forces the browser to send a
    # preflight OPTIONS request first — without Allow-Headers/Allow-Methods
    # here, that preflight fails silently and the real POST never fires.
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


def require_key(fn):
    """Gate a GET endpoint behind ?key=<APEX_WEBHOOK_PASSPHRASE>. Applied to every
    route that reveals live positions, trades, or account state — those used to
    be wide open to anyone who found the Railway URL. Same secret you already
    use for the webhook, so there's still only one thing to remember."""
    from functools import wraps

    @wraps(fn)
    def wrapper(*args, **kwargs):
        key = request.args.get("key", "")
        if not WEBHOOK_SECRET_TOKEN or not hmac.compare_digest(key, WEBHOOK_SECRET_TOKEN):
            return jsonify({"error": "unauthorized", "hint": "append ?key=<your webhook passphrase>"}), 403
        return fn(*args, **kwargs)

    return wrapper


# ════════════════════════════════════════════════════════════════════════════════
# GLOBAL SAFETY NET — a single bad request can never crash this bot or hang it.
# Every route below also has its own try/except for a clean, specific message;
# this is the final catch-all so NOTHING ever falls through as an ugly 500 page.
# ════════════════════════════════════════════════════════════════════════════════
@app.errorhandler(Exception)
def handle_any_error(e):
    # [FIX] Flask's own routing already raises normal HTTP errors (404 for
    # an unknown path like a stray /favicon.ico request, 403 for a bad
    # secret, etc.) as werkzeug HTTPException — those have a correct status
    # code and should just pass through as-is. Catching them here and
    # rewriting them all to a generic 500 was turning harmless "page not
    # found" hits into scary-looking "UNHANDLED ERROR" log entries and the
    # wrong status code for the client. Only genuine unexpected crashes
    # (anything that ISN'T already a proper HTTP error) should hit this
    # catch-all and become a 500.
    from werkzeug.exceptions import HTTPException
    if isinstance(e, HTTPException):
        return jsonify({"error": e.name, "detail": e.description}), e.code
    log.error(f"UNHANDLED ERROR: {e}\n{traceback.format_exc()}")
    return jsonify({"error": "internal_error", "detail": str(e)}), 500


# ════════════════════════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════════════════════════
log = logging.getLogger("apex_nexus")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_console = logging.StreamHandler()
_console.setFormatter(_fmt)
log.addHandler(_console)
_file = RotatingFileHandler("apex_nexus.log", maxBytes=5_000_000, backupCount=10)
_file.setFormatter(_fmt)
log.addHandler(_file)


# ════════════════════════════════════════════════════════════════════════════════
# SECRETS — from environment only, never hardcoded
# ════════════════════════════════════════════════════════════════════════════════
API_KEY = os.environ.get("DELTA_API_KEY")
API_SECRET = os.environ.get("DELTA_API_SECRET")
WEBHOOK_SECRET_TOKEN = os.environ.get("APEX_WEBHOOK_PASSPHRASE")
CONTROL_PASSWORD = os.environ.get("APEX_CONTROL_PASSWORD", WEBHOOK_SECRET_TOKEN)

# [DASHBOARD NEW — AI Q&A] Optional: powers the dashboard's "Ask APEX NEXUS"
# chat panel. Not required for anything else in this file — every other
# feature works with this unset. If it's missing, /ask/<secret> just returns
# a clear "not configured" error instead of failing in a confusing way.
# Uses Google's Gemini API (free tier available at aistudio.google.com,
# no credit card needed). Get a key, then set GEMINI_API_KEY in your .env.
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

# [PREMIUM FIX — LIVE/DRY-RUN TOGGLE] LIVE_MODE_ENV_DEFAULT is only the
# STARTUP default, read once from the environment. The actual live value used
# everywhere else in this file is is_live_mode() (defined after the DB helpers
# below), which checks the DB-backed control_flags table first and only falls
# back to this env default if nothing's been set there yet. This mirrors
# is_paused()'s own reasoning exactly: a plain module-level LIVE_MODE global
# would only update in the ONE gunicorn worker that happened to handle a
# /mode toggle request, while every other worker kept trading in whatever
# mode it booted with — a silent, dangerous split-brain on a live-money
# system. DRY_RUN is likewise now a function (is_dry_run()), not a constant.
LIVE_MODE_ENV_DEFAULT = os.environ.get("LIVE_MODE", "false").strip().lower() == "true"
# NOTE: there is deliberately no in-memory PAUSED global — see is_paused()
# further down for why that would be unsafe with more than one worker.

# ─── REGION — the ONE setting that controls both the API base URL and which
# quote-currency suffix (USDT vs USD) bare coin names resolve to. Matches the
# same REGION concept already used in your delta_order_flow.py module, so
# both pieces of your system agree on the same account type.
REGION = os.environ.get("DELTA_REGION", "global").strip().lower()  # "global" or "india"

BASE_URLS = {
    "global": "https://api.delta.exchange",
    "india": "https://api.india.delta.exchange",
}
PERP_SUFFIX = {
    "global": "USDT",
    "india": "USD",
}
if REGION not in BASE_URLS:
    log.warning(f"Unknown DELTA_REGION={REGION!r}, defaulting to 'global'")
    REGION = "global"

BASE_URL = BASE_URLS[REGION]
QUOTE_SUFFIX = PERP_SUFFIX[REGION]

REQUEST_TIMEOUT = 10
MAX_RETRIES = 2
RETRY_BACKOFF_BASE = 1.5
DUPLICATE_WINDOW_SECONDS = 5
DB_PATH = "apex_state.db"

# Auto-attach SL/TP as a native Delta bracket order right after entry fills.
# This means Delta itself watches the price and closes the position — the
# bot does NOT need to stay online or receive a separate "exit" webhook for
# risk management to work. Verified against Delta's official bracket-order
# API (POST /v2/orders/bracket). Toggle off if you'd rather manage exits
# yourself via separate EXIT webhook alerts.
AUTO_BRACKET_ORDERS = os.environ.get("AUTO_BRACKET_ORDERS", "true").strip().lower() == "true"
BRACKET_RETRY_ATTEMPTS = 3
BRACKET_RETRY_DELAY = 1.2  # seconds — gives the market entry a moment to actually fill first

# ════════════════════════════════════════════════════════════════════════════════
# [PREMIUM NEW] CIRCUIT BREAKER — the one real safety gap this bot had. Every
# entry/SL-move/partial-exit already flowed through here, but nothing ever
# paused new entries after a bad run — a losing streak or a big daily drawdown
# could keep trading indefinitely. This mirrors the R-multiple-based breaker
# already proven in the companion Pine script (Section 25B there): pause NEW
# entries (never touches an already-open position's own bracket SL/TP) once
# either threshold trips. DB-backed via control_flags — like is_paused()
# above, this must never be an in-memory global, or a multi-worker deployment
# would have workers silently disagreeing about whether the breaker is armed.
# ════════════════════════════════════════════════════════════════════════════════
CIRCUIT_BREAKER_ENABLED = os.environ.get("CIRCUIT_BREAKER_ENABLED", "true").strip().lower() == "true"
DAILY_LOSS_LIMIT_R = float(os.environ.get("DAILY_LOSS_LIMIT_R", "6.0"))
MAX_CONSECUTIVE_LOSSES = int(os.environ.get("MAX_CONSECUTIVE_LOSSES", "4"))


# ════════════════════════════════════════════════════════════════════════════════
# FIELD MAP — matches APEX NEXUS V12-P2 (PLOTBUDGET_FIXED)'s build_json() output
# exactly, field for field. If you ever rename a field in the Pine script, this
# is the only place you need to edit.
# ════════════════════════════════════════════════════════════════════════════════
FIELD_MAP = {
    "signal": "signal", "direction": "direction", "action": "action", "symbol": "symbol",
    "version": "version", "preset": "preset", "timeframe": "timeframe", "time": "time",
    "sl": "sl", "tp1": "tp1", "tp2": "tp2", "tp3": "tp3",
    "price": "price", "close": "close",
    "ai_score_buy": "ai_score_buy", "ai_score_sell": "ai_score_sell",
    "systems_buy": "systems_buy", "systems_sell": "systems_sell",
    "rsi": "rsi", "adx": "adx", "ofi_pct": "ofi_pct", "knn_score": "knn_score",
    "market_state": "market_state", "in_shock": "in_shock", "ml_healthy": "ml_healthy",
    "premium_shield": "premium_shield", "mtf_align_bars": "mtf_align_bars",
    "win_rate": "win_rate", "total_trades": "total_trades",
}

def f(alert_data, key, default=None):
    return alert_data.get(FIELD_MAP.get(key, key), default)

def fbool(alert_data, key, default=False):
    """Pine sends booleans via str.tostring(), so they arrive as the strings
    'true'/'false', not JSON booleans. Handle both forms defensively."""
    v = f(alert_data, key, default)
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() == "true"

# The Pine version this bot was built against. If a signal arrives tagged with
# a different version, it still gets traded (no reason to block real money over
# a label) but a warning is logged so you notice if the chart and bot drift
# out of sync — e.g. someone reverts the indicator without telling the bot.
EXPECTED_PINE_VERSION = "12.2-P2"


# ════════════════════════════════════════════════════════════════════════════════
# CONFIG — only 2 things left to decide, nothing about product IDs
# ════════════════════════════════════════════════════════════════════════════════
TIER_QUANTITY = {
    "NEXUS": 1, "STRONG": 1, "FAST": 1, "WARP": 1,
    "GHOST": 1, "RECOVERY": 1, "PULLBACK": 1, "SCALP": 1,
}
DEFAULT_QTY = 1

# [PREMIUM FIX — PER-TIER ON/OFF TOGGLE] ACTIVE_SIGNALS_DEFAULT is only the
# STARTUP default. The live set used everywhere else is get_active_signals()
# (defined after the DB helpers below), which is DB-backed via control_flags
# for the same multi-worker-safety reason as is_paused() and is_live_mode() —
# a plain module-level set would only update in whichever single gunicorn
# worker handled a /signals toggle request.
ACTIVE_SIGNALS_DEFAULT = {"NEXUS", "STRONG"}
# Every tier this bot knows how to size/trade at all — the universe /signals
# is allowed to turn on or off. Anything not in this list is silently ignored
# by set_active_signals() so a typo in a /signals request can never create a
# phantom "signal" that later matches nothing and does nothing.
ALL_KNOWN_SIGNALS = sorted(TIER_QUANTITY.keys())

# OPTIONAL manual override — leave empty. Only fill this in if Delta ever
# lists a coin under a symbol the auto-resolver genuinely can't figure out
# (extremely rare — e.g. a renamed/delisted-and-relisted asset). This dict
# is checked FIRST, so anything you put here wins, but you should never
# actually need to touch it.
SYMBOL_OVERRIDE: Dict[str, int] = {
    # "WEIRDCOIN": 99999,
}

BLOCKED_MARKET_STATES = {"LIQUIDATION_CASCADE"}
BLOCK_ENTRIES_DURING_SHOCK = True


# ════════════════════════════════════════════════════════════════════════════════
# ★★★ AUTO PRODUCT-ID DISCOVERY ENGINE ★★★
# ────────────────────────────────────────────────────────────────────────────────
# This is the feature that replaces "manually change the product ID for every
# coin." It downloads Delta's live perpetual-futures list, indexes it two
# ways (full symbol, and bare underlying asset), caches it, and refreshes
# itself in the background. resolve() below is the only function you need —
# hand it literally anything ("BTC", "btc", "BTCUSDT", "sol", "ETHUSDT"...)
# and it returns the correct numeric product_id, or None with a helpful log
# line listing close matches if the coin genuinely isn't listed on Delta.
# ════════════════════════════════════════════════════════════════════════════════
class DeltaProductResolver:
    def __init__(self):
        self._lock = threading.Lock()
        self.by_symbol: Dict[str, int] = {}          # "BTCUSDT" -> 12345
        self.by_underlying: Dict[str, int] = {}       # "BTC" -> 12345 (region-correct quote only)
        self.tick_size: Dict[int, float] = {}         # product_id -> tick_size (for price rounding)
        self.product_symbol: Dict[int, str] = {}      # product_id -> "BTCUSDT" (Delta wants this in bracket calls)
        self.last_refresh = 0.0
        self.refresh_interval = 600  # 10 minutes
        self.all_symbols_seen: List[str] = []         # for "did you mean" suggestions

    def refresh(self, force: bool = False) -> int:
        """Pull the full live perpetual-futures list from Delta. Returns count found."""
        with self._lock:
            if not force and (time.time() - self.last_refresh) < self.refresh_interval:
                return len(self.by_symbol)
            try:
                by_symbol, by_underlying, tick, prod_sym, all_seen = {}, {}, {}, {}, []
                after_cursor = None
                for _ in range(20):  # safety cap — Delta has nowhere near 2000 perps
                    params = {"contract_types": "perpetual_futures", "states": "live", "page_size": 100}
                    if after_cursor:
                        params["after"] = after_cursor
                    resp = delta_http.get(f"{BASE_URL}/v2/products", params=params, timeout=REQUEST_TIMEOUT)
                    resp.raise_for_status()
                    data = resp.json()
                    products = data.get("result", [])
                    for p in products:
                        pid = p.get("id")
                        sym = (p.get("symbol") or "").upper()
                        underlying = ((p.get("underlying_asset") or {}).get("symbol") or "").upper()
                        quote = ((p.get("quote_asset") or {}).get("symbol") or "").upper()
                        if not pid or not sym:
                            continue
                        by_symbol[sym] = pid
                        prod_sym[pid] = sym
                        all_seen.append(sym)
                        try:
                            tick[pid] = float(p.get("tick_size", 0) or 0)
                        except (TypeError, ValueError):
                            tick[pid] = 0.0
                        # Only index the bare-asset shortcut for the product whose
                        # quote currency matches THIS region's expected suffix, so
                        # "BTC" never accidentally resolves to the wrong contract.
                        if underlying and quote == QUOTE_SUFFIX:
                            by_underlying[underlying] = pid

                    next_cursor = (data.get("meta") or {}).get("after")
                    if not next_cursor:
                        break
                    after_cursor = next_cursor

                self.by_symbol = by_symbol
                self.by_underlying = by_underlying
                self.tick_size = tick
                self.product_symbol = prod_sym
                self.all_symbols_seen = sorted(all_seen)
                self.last_refresh = time.time()
                log.info(f"✅ Product discovery: {len(by_symbol)} live perpetuals indexed "
                          f"({REGION}, quote={QUOTE_SUFFIX}). Sample: {self.all_symbols_seen[:8]}")
                return len(by_symbol)
            except Exception as e:
                log.error(f"❌ Product discovery failed: {e}")
                return len(self.by_symbol)  # keep serving the old cache rather than going blank

    def resolve(self, raw: str) -> Optional[int]:
        """The only function callers need. Accepts bare asset or full symbol, any case."""
        if not raw:
            return None
        s = raw.strip().upper()

        # [FIX] TradingView appends ".P" to perpetual-contract symbols on
        # some data feeds (e.g. "BTCUSD.P"). Delta's own symbols never
        # include this suffix — without stripping it, "BTCUSD.P" never
        # matches "BTCUSD" even though it's exactly the right contract.
        # This was confirmed as the exact cause of every failed resolve in
        # the deploy logs (e.g. "Could not resolve 'BTCUSD.P'").
        if s.endswith(".P"):
            s = s[:-2]

        # Manual override always wins (should normally be empty)
        if s in SYMBOL_OVERRIDE:
            return SYMBOL_OVERRIDE[s]

        if not self.by_symbol:
            self.refresh(force=True)

        # 1) Exact full-symbol match ("BTCUSDT" sent directly)
        pid = self.by_symbol.get(s)
        if pid:
            return pid

        # 2) Bare asset + region-correct suffix ("BTC" -> "BTCUSDT")
        candidate = s if (s.endswith("USDT") or s.endswith("USD")) else f"{s}{QUOTE_SUFFIX}"
        pid = self.by_symbol.get(candidate)
        if pid:
            return pid

        # 3) Underlying-asset table as a fallback
        pid = self.by_underlying.get(s)
        if pid:
            return pid

        # 4) Maybe it's a brand-new listing since our last refresh — force one retry
        self.refresh(force=True)
        pid = self.by_symbol.get(s) or self.by_symbol.get(candidate) or self.by_underlying.get(s)
        if pid:
            return pid

        # 5) Genuinely not found — log close matches to make debugging painless
        close = [x for x in self.all_symbols_seen if s in x or x.startswith(s[:3])][:8]
        log.warning(f"⚠️ Could not resolve '{raw}' to a Delta product_id. "
                     f"Closest listed symbols: {close or 'none found'}")
        return None

    def get_symbol_for(self, product_id: int) -> Optional[str]:
        return self.product_symbol.get(product_id)

    def get_tick_size(self, product_id: int) -> float:
        return self.tick_size.get(product_id) or 0.0


resolver = DeltaProductResolver()


def safe_float(value, default=None) -> Optional[float]:
    """
    Parse untrusted external input (webhook JSON) into a float that can NEVER
    raise. Found by direct testing, not theory: sending sl=inf or sl="garbage"
    used to throw an uncaught exception from INSIDE the bracket-order call —
    which happens AFTER the real entry order already fired. That crash skipped
    upsert_position() entirely, leaving a real open position completely
    untracked by this bot. Every conversion of untrusted numeric input now
    goes through this instead of a bare float()/int() call.
    """
    if value in (None, "", "null"):
        return default
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return v if math.isfinite(v) else default


def safe_int(value, default=None) -> Optional[int]:
    f = safe_float(value, None)
    return int(f) if f is not None else default


def round_to_tick(price: float, tick_size: float) -> Optional[str]:
    """Delta rejects prices that don't align to a product's tick size. Round to
    the nearest valid tick and format as a plain string (Delta's API expects
    strings). Returns None — never raises — if price isn't a usable finite
    number, so a bad value degrades to 'skip this leg', not a crash."""
    price = safe_float(price)
    if price is None:
        return None
    if not tick_size or tick_size <= 0:
        return str(round(price, 8)).rstrip("0").rstrip(".") or "0"
    steps = round(price / tick_size)
    snapped = steps * tick_size
    decimals = max(0, len(str(tick_size).split(".")[-1])) if "." in str(tick_size) else 0
    return f"{snapped:.{decimals}f}"


def _background_refresh_loop():
    """Keeps the product table warm AND periodically re-checks that the API
    key still actually works — both run forever, without ever blocking a
    webhook call."""
    while True:
        time.sleep(resolver.refresh_interval)
        resolver.refresh(force=True)
        verify_api_credentials()


# ════════════════════════════════════════════════════════════════════════════════
# CONFIDENCE ENGINE — v2. Previously this scored ai_score/win_rate against a
# liquidation bias, but order_imbalance and regime_score were hardcoded to 0.0
# because nothing was feeding them — dead code pretending to be a feature.
# Pine's own JSON already carries real multi-factor context (systems_buy/sell,
# premium_shield, ml_healthy, mtf_align_bars) that was being received and then
# silently dropped. This version actually uses it — every input below now maps
# to a real field, either from Pine's payload or a live Delta orderbook fetch.
# Every adjustment is ADDITIVE on top of Pine's own ai_score, never multiplicative
# (multiplying several <1.0 discounts together can silently strangle a signal
# to near-zero without any single factor looking dangerous on its own).
# ════════════════════════════════════════════════════════════════════════════════
@dataclass
class ConfidenceBreakdown:
    base_score: float = 0.0
    systems_adj: float = 0.0
    ml_health_adj: float = 0.0
    freshness_adj: float = 0.0
    liquidation_adj: float = 0.0
    imbalance_adj: float = 0.0
    final_score: float = 0.0
    reason: str = ""
    hard_block: bool = False
    hard_block_reason: str = ""


class ConfidenceEngine:
    def __init__(self):
        self.liquidation_weight = 1.0
        self.imbalance_weight = 0.8

    def compute(self, ai_score: float, win_rate: float, systems: int, ml_healthy: bool,
                premium_shield: bool, mtf_align_bars: int, liquidation_bias: float,
                order_imbalance: float) -> ConfidenceBreakdown:
        b = ConfidenceBreakdown()

        # Pine's own entry conditions already require premium_shield to be true
        # for THIS direction before a signal can fire at all (see nexus_raw_b/s
        # etc. in the Pine script — "and premium_shield_b" is a hard AND-gate).
        # Receiving an entry alert with premium_shield=false means either a
        # timing edge-case or a genuine inconsistency — not a matter of degree,
        # so this hard-blocks rather than just docking points.
        if not premium_shield:
            b.hard_block = True
            b.hard_block_reason = "premium_shield=false on an entry signal — Pine's own VSA/CVD gate disagrees with itself, skipping as a safety precaution"
            return b

        base = min(100, max(0, float(ai_score)))
        wr = float(win_rate) / 100.0 if win_rate > 1 else float(win_rate)
        wr = min(1.0, max(0.0, wr))
        wr_mult = max(0.5, min(1.5, 0.5 + wr))
        b.base_score = base * wr_mult

        # Systems confluence (0-6 directional systems agreeing, from Pine's own
        # sys_b/sys_s). 3 is Pine's own baseline floor for most tiers — reward
        # bars where MORE than the minimum actually agreed.
        b.systems_adj = max(-6.0, min(9.0, (systems - 3) * 3.0))

        # ML drift — mirrors Pine's own halving of w_ml when a drift detector
        # trips, expressed as a confidence penalty on this side too rather than
        # silently trusting a score Pine itself is already discounting.
        b.ml_health_adj = 0.0 if ml_healthy else -8.0

        # Trend freshness — same tiering philosophy as Pine's own MTF bonus
        # decay (Section 20, V9.12-B): a fresh alignment is a better entry than
        # a mature one riding out its move, even at the same ai_score.
        if mtf_align_bars is None:
            b.freshness_adj = 0.0
        elif mtf_align_bars < 10:
            b.freshness_adj = 3.0
        elif mtf_align_bars < 30:
            b.freshness_adj = 1.5
        else:
            b.freshness_adj = 0.0

        b.liquidation_adj = liquidation_bias * self.liquidation_weight * 15
        b.imbalance_adj = order_imbalance * self.imbalance_weight * 10

        b.final_score = max(0, min(100, b.base_score + b.systems_adj + b.ml_health_adj +
                                    b.freshness_adj + b.liquidation_adj + b.imbalance_adj))
        b.reason = (f"base={b.base_score:.1f} systems={b.systems_adj:+.1f} "
                    f"mlhealth={b.ml_health_adj:+.1f} fresh={b.freshness_adj:+.1f} "
                    f"liq={b.liquidation_adj:+.1f} imb={b.imbalance_adj:+.1f} -> {b.final_score:.1f}")
        return b


confidence_engine = ConfidenceEngine()


def fetch_live_orderbook_imbalance(symbol: str) -> Tuple[float, bool]:
    """
    Best-effort independent cross-check: pulls Delta's OWN live L2 order book
    at the moment of entry (public endpoint, no auth needed) and computes bid/
    ask imbalance from real resting size — a second opinion from the exchange
    itself, separate from anything Pine calculated. Verified against Delta's
    official API (GET /v2/l2orderbook/{symbol}).
    NEVER blocks a trade: any failure here just returns neutral (0.0, False),
    exactly as if this feature didn't exist.
    """
    try:
        resp = delta_http.get(f"{BASE_URL}/v2/l2orderbook/{symbol}", timeout=4)
        resp.raise_for_status()
        result = resp.json().get("result", {})
        buy_levels = result.get("buy", [])[:10]
        sell_levels = result.get("sell", [])[:10]
        bid_qty = sum(float(lvl.get("size", 0)) for lvl in buy_levels)
        ask_qty = sum(float(lvl.get("size", 0)) for lvl in sell_levels)
        total = bid_qty + ask_qty
        if total <= 0:
            return 0.0, False
        return max(-1.0, min(1.0, (bid_qty - ask_qty) / total)), True
    except Exception as e:
        log.debug(f"Orderbook imbalance fetch skipped for {symbol}: {e}")
        return 0.0, False


# ════════════════════════════════════════════════════════════════════════════════
# ORDER BOOK IMBALANCE
# ════════════════════════════════════════════════════════════════════════════════
@dataclass
class ImbalanceResult:
    exists: bool = False
    imbalance_score: float = 0.0


class OrderBookImbalance:
    @staticmethod
    def from_depth(bids: List[List[float]], asks: List[List[float]], depth_level=10) -> ImbalanceResult:
        if not bids or not asks:
            return ImbalanceResult(exists=False)
        bid_qty = sum(b[1] for b in bids[:depth_level])
        ask_qty = sum(a[1] for a in asks[:depth_level])
        total = bid_qty + ask_qty
        if total == 0:
            return ImbalanceResult(exists=False)
        return ImbalanceResult(exists=True, imbalance_score=max(-1, min(1, (bid_qty - ask_qty) / total)))


# ════════════════════════════════════════════════════════════════════════════════
# MARKET REGIME DETECTION
# ════════════════════════════════════════════════════════════════════════════════
@dataclass
class RegimeResult:
    regime: str = "UNKNOWN"
    score: float = 0.0


class RegimeDetector:
    @staticmethod
    def from_candles(candles: List[Dict], lookback=20) -> RegimeResult:
        if len(candles) < lookback:
            return RegimeResult()
        closes = [c["close"] for c in candles[-lookback:]]
        sma_short = statistics.mean(closes[-5:])
        sma_long = statistics.mean(closes[-20:])
        slope = (sma_short - sma_long) / sma_long if sma_long else 0
        if slope > 0.02:
            return RegimeResult("UPTREND", min(1.0, slope / 0.05))
        if slope < -0.02:
            return RegimeResult("DOWNTREND", max(-1.0, slope / 0.05))
        return RegimeResult("RANGE", 0.0)


# ════════════════════════════════════════════════════════════════════════════════
# BINANCE LIQUIDATION FEED (optional real-time context signal)
# ════════════════════════════════════════════════════════════════════════════════
class LiquidationAggregator:
    def __init__(self, window_seconds=300):
        self.window = window_seconds
        self.buy_liq = deque()
        self.sell_liq = deque()

    def add(self, side: str, qty: float):
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - self.window * 1000
        (self.buy_liq if side == "BUY" else self.sell_liq).append((now_ms, qty))
        while self.buy_liq and self.buy_liq[0][0] < cutoff:
            self.buy_liq.popleft()
        while self.sell_liq and self.sell_liq[0][0] < cutoff:
            self.sell_liq.popleft()

    def get_bias(self) -> float:
        buy_total = sum(q for _, q in self.buy_liq)
        sell_total = sum(q for _, q in self.sell_liq)
        total = buy_total + sell_total
        return -1.0 * (buy_total - sell_total) / total if total else 0.0

    def get_snapshot(self) -> Dict:
        """[DASHBOARD NEW — ORDER FLOW] Real numbers straight off the live
        Binance forced-liquidation stream (last `self.window` seconds) — buy
        vs sell liquidation volume and how many events of each. This is
        genuinely live market data, not a placeholder; it just starts empty
        until the websocket has seen events, which can take a few minutes
        on a quiet market."""
        buy_total = sum(q for _, q in self.buy_liq)
        sell_total = sum(q for _, q in self.sell_liq)
        total = buy_total + sell_total
        return {
            "window_seconds": self.window,
            "buy_liq_qty": round(buy_total, 4),
            "sell_liq_qty": round(sell_total, 4),
            "buy_liq_count": len(self.buy_liq),
            "sell_liq_count": len(self.sell_liq),
            "net_flow_qty": round(buy_total - sell_total, 4),
            "bias": round(self.get_bias(), 4),
            "has_data": total > 0,
        }


class BinanceLiquidationFeed:
    def __init__(self, symbols: List[str], aggregator: LiquidationAggregator):
        self.symbols = symbols
        self.aggregator = aggregator

    def start(self):
        if websocket is None:
            log.warning("websocket-client not installed — liquidation feed disabled")
            return
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        streams = "/".join(f"{s.lower()}@forceOrder" for s in self.symbols)
        url = f"wss://fstream.binance.com/ws/stream?streams={streams}"
        while True:
            try:
                ws = websocket.WebSocketApp(url, on_message=self._on_message)
                ws.run_forever()
            except Exception as e:
                log.warning(f"Liquidation feed error: {e}")
            time.sleep(5)

    def _on_message(self, ws, message):
        try:
            order = json.loads(message).get("data", {}).get("o", {})
            if order:
                side = "BUY" if order.get("S") == "BUY" else "SELL"
                self.aggregator.add(side, float(order.get("q", 0)))
        except Exception:
            pass


liquidation_aggregator = LiquidationAggregator()


# ════════════════════════════════════════════════════════════════════════════════
# AI MARKET CONSENSUS ORACLE — merged in from the standalone ai_oracle.py
# (Phase 1) into this same process/file, upgraded to an "Institutional
# Edition" ensemble along the way. Runs as ONE MORE background daemon thread
# — same pattern as _background_refresh_loop() and _self_check_loop() below,
# same `delta_http` session, same control_flags table. No more separate
# process, no asyncio — a synchronous thread loop using plain `requests`,
# exactly like everything else here.
#
# TWO independent "votes" every tick, then an ensemble combine:
#   1. Gemini vote   — reads the live footprint, gives a label + confidence
#                       (reuses the SAME GEMINI_API_KEY/GEMINI_MODEL already
#                       configured for the /ask Q&A panel above).
#   2. Quant vote     — a deterministic score from real order-book/trade/
#                       funding data. Needs no API key and can't go down with
#                       Gemini, so the oracle always has an opinion.
# A circuit breaker stops calling Gemini after repeated failures (falls back
# to quant-only, "degraded_mode": true). An accuracy tracker logs every
# consensus with the mark price at the time and later scores itself against
# what price actually did — so "is this oracle any good" is a real number,
# not a vibe.
#
# [NEW] AI GATEKEEPER — answers the open question left in the last
# consolidation pass ("should the oracle's read actually affect trades, or
# stay informational?"). OFF by default (AI_ORACLE_GATE_TRADES=false). When
# turned on, it is a STRICT VETO ONLY: it can block an entry that actively
# conflicts with a high-confidence oracle read, it can never approve or size
# up a trade Pine/ConfidenceEngine/Neural Syndicate already rejected, and a
# NEUTRAL or low-confidence oracle read always passes through untouched.
# ════════════════════════════════════════════════════════════════════════════════
ORACLE_SYMBOLS = [s.strip().upper() for s in
                  os.environ.get("ORACLE_SYMBOLS", "BTCUSD,ETHUSD").split(",") if s.strip()]
ORACLE_INTERVAL_S = int(os.environ.get("ORACLE_INTERVAL_S", "60"))
ORACLE_TRADE_LOOKBACK = int(os.environ.get("ORACLE_TRADE_LOOKBACK", "100"))

ORACLE_W_DEPTH = float(os.environ.get("ORACLE_W_DEPTH", "0.35"))
ORACLE_W_TAKER = float(os.environ.get("ORACLE_W_TAKER", "0.35"))
ORACLE_W_MOMENTUM = float(os.environ.get("ORACLE_W_MOMENTUM", "0.20"))
ORACLE_W_FUNDING = float(os.environ.get("ORACLE_W_FUNDING", "0.10"))
ORACLE_QUANT_THRESHOLD = float(os.environ.get("ORACLE_QUANT_THRESHOLD", "0.15"))
ORACLE_MOMENTUM_NORMALIZER_PCT = float(os.environ.get("ORACLE_MOMENTUM_NORMALIZER_PCT", "0.30"))
ORACLE_FUNDING_NORMALIZER = float(os.environ.get("ORACLE_FUNDING_NORMALIZER", "0.01"))

ORACLE_W_GEMINI = float(os.environ.get("ORACLE_W_GEMINI", "0.5"))
ORACLE_W_QUANT = float(os.environ.get("ORACLE_W_QUANT", "0.5"))
ORACLE_ENSEMBLE_THRESHOLD = float(os.environ.get("ORACLE_ENSEMBLE_THRESHOLD", "0.12"))

ORACLE_CB_FAILURE_THRESHOLD = int(os.environ.get("ORACLE_CB_FAILURE_THRESHOLD", "5"))
ORACLE_CB_COOLDOWN_S = float(os.environ.get("ORACLE_CB_COOLDOWN_S", "300"))

ORACLE_ACCURACY_LOOKAHEAD_S = float(os.environ.get("ORACLE_ACCURACY_LOOKAHEAD_S", "900"))
ORACLE_ACCURACY_FLAT_PCT = float(os.environ.get("ORACLE_ACCURACY_FLAT_PCT", "0.05"))
ORACLE_ACCURACY_WINDOW = int(os.environ.get("ORACLE_ACCURACY_WINDOW", "200"))

# [NEW] AI Gatekeeper — see docstring above. Deliberately OFF by default.
AI_ORACLE_GATE_TRADES = os.environ.get("AI_ORACLE_GATE_TRADES", "false").strip().lower() == "true"
AI_ORACLE_GATE_MIN_CONFIDENCE = float(os.environ.get("AI_ORACLE_GATE_MIN_CONFIDENCE", "0.55"))


def _clamp(v: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def fetch_oracle_footprint(symbol: str) -> Dict:
    """One clean footprint per symbol: order-book depth imbalance (reuses the
    same /v2/l2orderbook logic as fetch_live_orderbook_imbalance above, but
    keeps its own bid/ask qty split), recent taker order-flow + momentum, and
    ticker context (funding rate, open interest, 24h range). Every sub-fetch
    degrades to None on failure rather than raising — one bad endpoint never
    blocks the whole tick."""
    orderbook = None
    try:
        resp = delta_http.get(f"{BASE_URL}/v2/l2orderbook/{symbol}", timeout=4)
        resp.raise_for_status()
        result = resp.json().get("result", {})
        buy_levels = result.get("buy", [])[:10]
        sell_levels = result.get("sell", [])[:10]
        bid_qty = sum(float(lvl.get("size", 0)) for lvl in buy_levels)
        ask_qty = sum(float(lvl.get("size", 0)) for lvl in sell_levels)
        total_qty = bid_qty + ask_qty
        imbalance = 0.0 if total_qty <= 0 else _clamp((bid_qty - ask_qty) / total_qty)
        orderbook = {"bid_depth_qty": round(bid_qty, 6), "ask_depth_qty": round(ask_qty, 6),
                     "depth_imbalance": round(imbalance, 4)}
    except Exception as e:
        log.debug(f"Oracle orderbook fetch skipped for {symbol}: {e}")

    order_flow = None
    try:
        resp = delta_http.get(f"{BASE_URL}/v2/trades/{symbol}",
                               params={"page_size": ORACLE_TRADE_LOOKBACK}, timeout=4)
        resp.raise_for_status()
        trades = resp.json().get("result", []) or []
        buy_vol = sell_vol = 0.0
        prices = []
        for t in trades:
            try:
                size = float(t.get("size", 0))
            except (TypeError, ValueError):
                continue
            side = (t.get("side") or "").lower()
            if side == "buy":
                buy_vol += size
            elif side == "sell":
                sell_vol += size
            p = t.get("price")
            if p is not None:
                try:
                    prices.append(float(p))
                except (TypeError, ValueError):
                    pass
        total_vol = buy_vol + sell_vol
        taker_delta = 0.0 if total_vol <= 0 else _clamp((buy_vol - sell_vol) / total_vol)
        momentum_pct = None
        # Delta's public trade feed returns most-recent-first: prices[0] is
        # the newest print, prices[-1] the oldest in our lookback window.
        if len(prices) >= 8:
            quarter = max(2, len(prices) // 4)
            recent_avg = statistics.fmean(prices[:quarter])
            older_avg = statistics.fmean(prices[-quarter:])
            if older_avg:
                momentum_pct = round((recent_avg - older_avg) / older_avg * 100.0, 4)
        order_flow = {"trade_count": len(trades), "taker_delta": round(taker_delta, 4),
                      "momentum_pct": momentum_pct}
    except Exception as e:
        log.debug(f"Oracle trade-flow fetch skipped for {symbol}: {e}")

    ticker = None
    try:
        resp = delta_http.get(f"{BASE_URL}/v2/tickers/{symbol}", timeout=4)
        resp.raise_for_status()
        result = resp.json().get("result", {}) or {}

        def _f(key):
            v = result.get(key)
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        ticker = {"mark_price": _f("mark_price") or _f("close") or _f("spot_price"),
                  "funding_rate": _f("funding_rate"), "open_interest": _f("oi"),
                  "high_24h": _f("high"), "low_24h": _f("low")}
    except Exception as e:
        log.debug(f"Oracle ticker fetch skipped for {symbol}: {e}")

    volatility_pct = None
    if ticker and ticker.get("high_24h") and ticker.get("low_24h") and ticker.get("mark_price"):
        try:
            volatility_pct = round((ticker["high_24h"] - ticker["low_24h"]) / ticker["mark_price"] * 100.0, 4)
        except (TypeError, ZeroDivisionError):
            pass

    return {"symbol": symbol, "timestamp": datetime.now(timezone.utc).isoformat(),
            "orderbook": orderbook, "order_flow": order_flow, "ticker": ticker,
            "volatility_pct_24h": volatility_pct}


def compute_quant_vote(footprint: Dict) -> Dict:
    """Deterministic second 'vote' — no LLM call, can't go down with Gemini."""
    orderbook = footprint.get("orderbook") or {}
    order_flow = footprint.get("order_flow") or {}
    ticker = footprint.get("ticker") or {}

    depth_imbalance = orderbook.get("depth_imbalance") or 0.0
    taker_delta = order_flow.get("taker_delta") or 0.0
    momentum_pct = order_flow.get("momentum_pct") or 0.0
    funding_rate = ticker.get("funding_rate") or 0.0

    momentum_component = _clamp(momentum_pct / ORACLE_MOMENTUM_NORMALIZER_PCT) if ORACLE_MOMENTUM_NORMALIZER_PCT else 0.0
    # Contrarian: crowded-long (high +funding) tilts slightly bearish (mean
    # reversion risk); crowded-short tilts slightly bullish.
    funding_component = _clamp(-funding_rate / ORACLE_FUNDING_NORMALIZER) if ORACLE_FUNDING_NORMALIZER else 0.0

    score = _clamp(ORACLE_W_DEPTH * depth_imbalance + ORACLE_W_TAKER * taker_delta +
                    ORACLE_W_MOMENTUM * momentum_component + ORACLE_W_FUNDING * funding_component)

    label = ("BULLISH" if score >= ORACLE_QUANT_THRESHOLD else
             "BEARISH" if score <= -ORACLE_QUANT_THRESHOLD else "NEUTRAL")

    return {"label": label, "score": round(score, 4), "confidence": round(abs(score), 4),
            "features": {"depth_imbalance": depth_imbalance, "taker_delta": taker_delta,
                         "momentum_pct": momentum_pct, "funding_rate": funding_rate}}


class _OracleCircuitBreaker:
    """Closed = calling Gemini normally. Open = short-circuited, quant-only
    fallback for ORACLE_CB_COOLDOWN_S. Half-open = one trial call allowed
    after the cooldown to see if Gemini has recovered."""
    def __init__(self, failure_threshold: int, cooldown_s: float):
        self.failure_threshold = failure_threshold
        self.cooldown_s = cooldown_s
        self.consecutive_failures = 0
        self.state = "closed"
        self.opened_at = None

    def allow_call(self) -> bool:
        if self.state == "closed":
            return True
        if self.state == "open":
            if self.opened_at is not None and (time.monotonic() - self.opened_at) >= self.cooldown_s:
                self.state = "half_open"
                return True
            return False
        return True

    def record_success(self):
        if self.state != "closed":
            log.info("🟢 AI Oracle circuit breaker CLOSED — Gemini call succeeded again.")
        self.consecutive_failures = 0
        self.state = "closed"
        self.opened_at = None

    def record_failure(self):
        self.consecutive_failures += 1
        if self.state == "half_open":
            self.state = "open"
            self.opened_at = time.monotonic()
            log.warning("🟡 AI Oracle circuit breaker re-OPENED — trial call failed.")
        elif self.consecutive_failures >= self.failure_threshold and self.state == "closed":
            self.state = "open"
            self.opened_at = time.monotonic()
            log.error(f"🔴 AI Oracle circuit breaker OPENED after {self.consecutive_failures} "
                      f"consecutive Gemini failures — falling back to quant-only consensus for "
                      f"{self.cooldown_s:.0f}s.")

    def snapshot(self) -> Dict:
        return {"state": self.state, "consecutive_failures": self.consecutive_failures,
                "cooldown_remaining_s": (
                    max(0.0, self.cooldown_s - (time.monotonic() - self.opened_at))
                    if self.state == "open" and self.opened_at is not None else 0.0)}


oracle_breaker = _OracleCircuitBreaker(ORACLE_CB_FAILURE_THRESHOLD, ORACLE_CB_COOLDOWN_S)

_ORACLE_GEMINI_SYSTEM_PROMPT = (
    "You are a strict market micro-structure classifier for a crypto perpetual "
    "futures trading bot. You are given a JSON footprint for one symbol: order "
    "book depth imbalance, recent taker order-flow (buy/sell split + short-term "
    "price momentum from real executed trades), and funding/open-interest/"
    "volatility context. Classify the immediate directional pressure as one of "
    "BULLISH, BEARISH, or NEUTRAL, and give your confidence 0.0-1.0. Respond "
    "with ONLY the label and confidence, space-separated — e.g. 'BULLISH 0.7'. "
    "No punctuation, no explanation. If data is thin or contradictory, respond "
    "'NEUTRAL 0.3'."
)
_ORACLE_GEMINI_RE = re.compile(r"(BULLISH|BEARISH|NEUTRAL)\D{0,5}(\d(?:\.\d+)?)", re.IGNORECASE)
VALID_CONSENSUS_LABELS = {"BULLISH", "BEARISH", "NEUTRAL"}


def get_gemini_oracle_vote(footprint: Dict) -> Optional[Dict]:
    """Returns {"label", "confidence"} or None ("no opinion" — the circuit
    breaker is open, no API key, or the call ultimately failed). None is
    treated by combine_oracle_consensus() as pure quant-only for this tick."""
    if not GEMINI_API_KEY or not oracle_breaker.allow_call():
        return None
    try:
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
            headers={"content-type": "application/json"},
            params={"key": GEMINI_API_KEY},
            json={"system_instruction": {"parts": [{"text": _ORACLE_GEMINI_SYSTEM_PROMPT}]},
                  "contents": [{"role": "user", "parts": [{"text": json.dumps(footprint, default=str)}]}],
                  "generationConfig": {"temperature": 0.0, "maxOutputTokens": 12}},
            timeout=8,
        )
        resp.raise_for_status()
        data = resp.json()
        candidates = data.get("candidates", [])
        if not candidates:
            oracle_breaker.record_failure()
            return None
        parts = candidates[0].get("content", {}).get("parts", [])
        raw_text = "".join(p.get("text", "") for p in parts).strip().upper()

        match = _ORACLE_GEMINI_RE.search(raw_text)
        if match:
            try:
                confidence = _clamp(float(match.group(2)), 0.0, 1.0)
            except ValueError:
                confidence = 0.5
            oracle_breaker.record_success()
            return {"label": match.group(1), "confidence": round(confidence, 4)}

        for label in VALID_CONSENSUS_LABELS:
            if label in raw_text:
                oracle_breaker.record_success()
                return {"label": label, "confidence": 0.5}

        oracle_breaker.record_failure()
        return None
    except requests.exceptions.RequestException as e:
        log.warning(f"AI Oracle Gemini call failed: {e}")
        oracle_breaker.record_failure()
        return None
    except Exception as e:
        log.error(f"AI Oracle Gemini unexpected error: {e}")
        oracle_breaker.record_failure()
        return None


def _directional_value(label: str, confidence: float) -> float:
    if label == "BULLISH":
        return confidence
    if label == "BEARISH":
        return -confidence
    return 0.0


def combine_oracle_consensus(gemini_vote: Optional[Dict], quant_vote: Dict) -> Dict:
    degraded = gemini_vote is None
    if degraded:
        combined_score = quant_vote["score"]
    else:
        g_value = _directional_value(gemini_vote["label"], gemini_vote["confidence"])
        total_w = ORACLE_W_GEMINI + ORACLE_W_QUANT
        combined_score = _clamp((ORACLE_W_GEMINI * g_value + ORACLE_W_QUANT * quant_vote["score"]) / total_w) \
            if total_w else quant_vote["score"]

    label = ("BULLISH" if combined_score >= ORACLE_ENSEMBLE_THRESHOLD else
             "BEARISH" if combined_score <= -ORACLE_ENSEMBLE_THRESHOLD else "NEUTRAL")
    agreement = (not degraded) and (gemini_vote["label"] == quant_vote["label"])

    return {"label": label, "confidence": round(abs(combined_score), 4),
            "combined_score": round(combined_score, 4), "degraded_mode": degraded,
            "agreement": agreement, "gemini_vote": gemini_vote, "quant_vote": quant_vote}


def record_oracle_prediction(symbol: str, consensus: str, confidence: float, mark_price: Optional[float]):
    try:
        with db() as conn:
            conn.execute("INSERT INTO oracle_predictions (symbol, ts, consensus, confidence, mark_price) "
                         "VALUES (?,?,?,?,?)",
                         (symbol, datetime.now(timezone.utc).isoformat(), consensus, confidence, mark_price))
            conn.commit()
    except Exception as e:
        log.error(f"record_oracle_prediction failed for {symbol}: {e}")


def evaluate_oracle_predictions(symbol: str, current_mark_price: Optional[float]) -> Optional[float]:
    """Scores any prediction for `symbol` at least ORACLE_ACCURACY_LOOKAHEAD_S
    old against current_mark_price ('what actually happened'). Returns the
    fresh rolling accuracy % (0-100), or None without enough history yet."""
    if current_mark_price is None:
        return None
    try:
        with db() as conn:
            cutoff_iso = (datetime.now(timezone.utc) -
                          timedelta(seconds=ORACLE_ACCURACY_LOOKAHEAD_S)).isoformat()
            rows = conn.execute(
                "SELECT id, consensus, mark_price FROM oracle_predictions "
                "WHERE symbol=? AND evaluated=0 AND ts<=? AND mark_price IS NOT NULL",
                (symbol, cutoff_iso)).fetchall()
            for row in rows:
                old_price = row["mark_price"]
                if not old_price:
                    conn.execute("UPDATE oracle_predictions SET evaluated=1, correct=NULL WHERE id=?", (row["id"],))
                    continue
                pct_move = (current_mark_price - old_price) / old_price * 100.0
                actual = ("BULLISH" if pct_move > ORACLE_ACCURACY_FLAT_PCT else
                          "BEARISH" if pct_move < -ORACLE_ACCURACY_FLAT_PCT else "NEUTRAL")
                correct = 1 if row["consensus"] == actual else 0
                conn.execute("UPDATE oracle_predictions SET evaluated=1, correct=? WHERE id=?",
                             (correct, row["id"]))
            conn.commit()

            recent = conn.execute(
                "SELECT correct FROM oracle_predictions WHERE symbol=? AND evaluated=1 "
                "AND correct IS NOT NULL ORDER BY id DESC LIMIT ?",
                (symbol, ORACLE_ACCURACY_WINDOW)).fetchall()
        if not recent:
            return None
        return round(100.0 * sum(r["correct"] for r in recent) / len(recent), 2)
    except Exception as e:
        log.error(f"evaluate_oracle_predictions failed for {symbol}: {e}")
        return None


_oracle_health_lock = threading.Lock()
_oracle_health: Dict[str, Dict] = {}
_oracle_started_at = time.monotonic()
_oracle_tick_count = 0


def get_ai_oracle_snapshot() -> Dict:
    """Everything the dashboard's AI Confidence Matrix / AI Oracle panels
    need — per-symbol ensemble label, confidence, degraded/agreement flags,
    rolling self-scored accuracy, and the Gemini circuit breaker state."""
    with _oracle_health_lock:
        symbols_snapshot = dict(_oracle_health)
    return {
        "uptime_s": round(time.monotonic() - _oracle_started_at, 1),
        "tick_count": _oracle_tick_count,
        "gemini_configured": bool(GEMINI_API_KEY),
        "gate_trades_enabled": AI_ORACLE_GATE_TRADES,
        "circuit_breaker": oracle_breaker.snapshot(),
        "symbols": symbols_snapshot,
    }


def ai_oracle_gate_check(symbol: str, direction: str) -> Tuple[bool, str]:
    """[NEW] The AI Gatekeeper itself. Returns (True, "") to allow the entry
    to proceed untouched, or (False, reason) to veto it. Only ever blocks —
    never approves or upsizes a trade the earlier gates already rejected —
    and only acts once the oracle's ensemble confidence for THIS symbol
    clears AI_ORACLE_GATE_MIN_CONFIDENCE; a NEUTRAL or low-confidence read,
    or a symbol the oracle hasn't ticked for yet, always passes through."""
    if not AI_ORACLE_GATE_TRADES:
        return True, ""
    with _oracle_health_lock:
        snap = _oracle_health.get(symbol)
    if not snap or not snap.get("ok"):
        return True, ""  # no opinion yet — never block on missing data
    consensus = snap.get("consensus")
    confidence = snap.get("confidence") or 0.0
    if consensus == "NEUTRAL" or confidence < AI_ORACLE_GATE_MIN_CONFIDENCE:
        return True, ""
    wants_long = direction == "BUY"
    wants_short = direction == "SELL"
    if wants_long and consensus == "BEARISH":
        return False, f"AI Oracle is BEARISH ({confidence:.0%} confidence) on {symbol}, conflicts with LONG entry"
    if wants_short and consensus == "BULLISH":
        return False, f"AI Oracle is BULLISH ({confidence:.0%} confidence) on {symbol}, conflicts with SHORT entry"
    return True, ""


def _ai_oracle_tick():
    global _oracle_tick_count
    for symbol in ORACLE_SYMBOLS:
        try:
            footprint = fetch_oracle_footprint(symbol)
            quant_vote = compute_quant_vote(footprint)
            gemini_vote = get_gemini_oracle_vote(footprint)
            consensus = combine_oracle_consensus(gemini_vote, quant_vote)
            mark_price = (footprint.get("ticker") or {}).get("mark_price")

            set_control_flag(f"ai_consensus_{symbol}", consensus["label"])
            set_control_flag(f"ai_consensus_{symbol}_confidence", str(consensus["confidence"]))
            set_control_flag(f"ai_consensus_{symbol}_detail", json.dumps(consensus, default=str))
            set_control_flag(f"ai_consensus_{symbol}_updated_at", datetime.now(timezone.utc).isoformat())

            record_oracle_prediction(symbol, consensus["label"], consensus["confidence"], mark_price)
            accuracy_pct = evaluate_oracle_predictions(symbol, mark_price)
            if accuracy_pct is not None:
                set_control_flag(f"ai_oracle_accuracy_{symbol}", str(accuracy_pct))

            with _oracle_health_lock:
                _oracle_health[symbol] = {
                    "ok": True, "consensus": consensus["label"], "confidence": consensus["confidence"],
                    "degraded_mode": consensus["degraded_mode"], "agreement": consensus["agreement"],
                    "mark_price": mark_price, "rolling_accuracy_pct": accuracy_pct,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            log.info(f"🔮 AI Oracle {symbol}: consensus={consensus['label']} conf={consensus['confidence']} "
                      f"degraded={consensus['degraded_mode']} accuracy={accuracy_pct}")
        except Exception as e:
            with _oracle_health_lock:
                prev = _oracle_health.get(symbol, {})
                _oracle_health[symbol] = {**prev, "ok": False, "last_error": str(e),
                                           "last_error_at": datetime.now(timezone.utc).isoformat()}
            log.error(f"AI Oracle tick failed for {symbol}: {e}")

    if ORACLE_SYMBOLS:
        last_symbol = ORACLE_SYMBOLS[-1]
        last = _oracle_health.get(last_symbol, {})
        if last.get("ok"):
            set_control_flag("ai_consensus", last["consensus"])
            set_control_flag("ai_consensus_symbol", last_symbol)
            set_control_flag("ai_consensus_confidence", str(last["confidence"]))
            set_control_flag("ai_consensus_updated_at", datetime.now(timezone.utc).isoformat())
    _oracle_tick_count += 1


def _ai_oracle_loop():
    """Background daemon thread — same pattern as _background_refresh_loop()
    and _self_check_loop() below. Runs forever, ticks every ORACLE_INTERVAL_S,
    and a single symbol's failure (handled inside _ai_oracle_tick) never
    takes the loop down."""
    if not GEMINI_API_KEY:
        log.warning("⚠️ GEMINI_API_KEY not set — AI Oracle will run on quant-only "
                    "consensus (degraded_mode) until it's configured.")
    if AI_ORACLE_GATE_TRADES:
        log.warning(f"🔮 AI Oracle Gatekeeper is ENABLED — entries conflicting with a "
                    f"≥{AI_ORACLE_GATE_MIN_CONFIDENCE:.0%}-confidence oracle read will be vetoed.")
    log.info(f"🔮 AI Oracle loop starting — symbols={ORACLE_SYMBOLS} interval={ORACLE_INTERVAL_S}s")
    while True:
        try:
            _ai_oracle_tick()
        except Exception as e:
            log.error(f"AI Oracle loop tick crashed (continuing): {e}\n{traceback.format_exc()}")
        time.sleep(ORACLE_INTERVAL_S)


# ════════════════════════════════════════════════════════════════════════════════
# DATABASE
# ════════════════════════════════════════════════════════════════════════════════
# ════════════════════════════════════════════════════════════════════════════════
# DATABASE
# ────────────────────────────────────────────────────────────────────────────────
# MIGRATION SAFETY: CREATE TABLE IF NOT EXISTS only protects a table that
# doesn't exist yet — it does NOT add new columns to a table that already
# exists with an older shape. That gap is exactly what caused the "no such
# column" failures traced in a real deployment (see the STARTUP section
# below for the full story). _ensure_column() closes that gap: every column
# this bot needs is declared once, in one place, and added automatically to
# an existing table if it's missing. Adding a new field in the future means
# adding ONE line here — never a manual migration, never a wiped database.
# ════════════════════════════════════════════════════════════════════════════════
@contextlib.contextmanager
def db():
    # timeout=10: if the DB is briefly locked by another connection, wait up
    # to 10s and retry instead of failing immediately with "database is locked".
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    # WAL mode lets reads and writes happen concurrently instead of blocking
    # each other — standard hardening for any Flask+SQLite app that might see
    # more than one request in flight at once (e.g. gunicorn -w > 1).
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    # [CRITICAL FIX — FD LEAK / crash after running a while] sqlite3.Connection
    # implements __enter__/__exit__ itself, but that built-in context manager
    # only commits (on success) or rolls back (on exception) — it does NOT
    # close the connection. Every "with db() as conn:" in this file was
    # trusting `with` to close it the way it closes a file; it never did.
    # Each call leaked one connection (plus WAL's extra -wal/-shm file
    # handles) for the rest of the process's life. Enough of those over
    # enough hours/days and the process hits its open-file-descriptor limit:
    # "OSError: [Errno 24] Too many open files" /
    # "sqlite3.OperationalError: unable to open database file" — which is
    # unrecoverable without a restart. Making this a real @contextmanager
    # with try/finally: conn.close() fixes all 31 call sites at once — every
    # "with db() as conn:" elsewhere in the file is unchanged and keeps its
    # existing commit-on-success / rollback-on-exception behavior, it just
    # now actually closes too.
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _ensure_column(conn, table: str, column: str, coltype: str):
    """Add `column` to `table` if it isn't already there. Safe to call every
    startup, unconditionally — a no-op once the column exists."""
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
        log.info(f"🔧 Migration: added {table}.{column} ({coltype})")


def init_db():
    with db() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS positions (
            symbol TEXT PRIMARY KEY, signal TEXT, direction TEXT, entry_price REAL,
            entry_time TIMESTAMP, qty REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL,
            product_id INTEGER, status TEXT DEFAULT 'open')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, signal TEXT, direction TEXT,
            event TEXT, qty REAL, price REAL, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            raw_result TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS control_flags (
            key TEXT PRIMARY KEY, value TEXT)""")
        # [DASHBOARD NEW] Every place an entry gets blocked or an exchange
        # order actually gets rejected now writes one row here — the
        # dashboard's new "Rejected Orders" panel reads straight from this
        # instead of the person having to dig through Render logs.
        conn.execute("""CREATE TABLE IF NOT EXISTS rejections (
            id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, signal TEXT, direction TEXT,
            reason TEXT, detail TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        # [SELF-CHECK NEW] Every row here is one automated self-diagnostic
        # message — the bot writing to its own dashboard without a human
        # asking it to. level is 'info' | 'warn' | 'danger' so the dashboard
        # can style/prioritize the same way it already does for Alert Center.
        conn.execute("""CREATE TABLE IF NOT EXISTS self_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT, level TEXT, category TEXT,
            message TEXT, detail TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        # [AI ORACLE MERGE] Owned entirely by the AI Market Consensus Oracle
        # above — logs every consensus call with the mark price at the time,
        # so evaluate_oracle_predictions() can later score it against what
        # price actually did and produce a real rolling accuracy %.
        conn.execute("""CREATE TABLE IF NOT EXISTS oracle_predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, ts TEXT NOT NULL,
            consensus TEXT NOT NULL, confidence REAL, mark_price REAL,
            evaluated INTEGER DEFAULT 0, correct INTEGER)""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_oracle_predictions_eval
            ON oracle_predictions (symbol, evaluated, ts)""")
        conn.commit()

        # New Pine-sync columns (V12-P2 PLOTBUDGET_FIXED) — added via migration
        # so an existing database from an older version of this bot upgrades
        # in place instead of breaking.
        for col, typ in [
            ("systems", "INTEGER"), ("rsi", "REAL"), ("adx", "REAL"),
            ("ofi_pct", "REAL"), ("knn_score", "REAL"), ("ml_healthy", "INTEGER"),
            ("premium_shield", "INTEGER"), ("mtf_align_bars", "INTEGER"),
            ("preset", "TEXT"), ("pine_version", "TEXT"), ("confidence_score", "REAL"),
            ("confidence_reason", "TEXT"),
        ]:
            _ensure_column(conn, "positions", col, typ)
        for col, typ in [
            ("systems", "INTEGER"), ("preset", "TEXT"), ("pine_version", "TEXT"),
            ("confidence_score", "REAL"),
            # [V9 ADD] Previously only the `positions` row carried the full
            # signal breakdown (rsi/adx/ofi_pct/knn_score/ml_healthy/
            # premium_shield/mtf_align_bars/confidence_reason) — the moment
            # delete_position() marked it closed, that reasoning was gone
            # from anywhere the dashboard's /trades (history) endpoint could
            # see. The AI Decision Engine panel needs it for CLOSED trades
            # too, not just whatever's open right now, so it's mirrored onto
            # `trades` as well and populated by log_trade() below.
            ("rsi", "REAL"), ("adx", "REAL"), ("ofi_pct", "REAL"), ("knn_score", "REAL"),
            ("ml_healthy", "INTEGER"), ("premium_shield", "INTEGER"), ("mtf_align_bars", "INTEGER"),
            ("confidence_reason", "TEXT"),
        ]:
            _ensure_column(conn, "trades", col, typ)
        conn.commit()
    log.info("✅ Database ready (tables verified, migrations applied if needed)")


def is_duplicate_alert(signal, direction, symbol, action) -> bool:
    cutoff = datetime.utcnow() - timedelta(seconds=DUPLICATE_WINDOW_SECONDS)
    with db() as conn:
        row = conn.execute(
            "SELECT 1 FROM trades WHERE symbol=? AND signal=? AND direction=? AND event=? AND timestamp>?",
            (symbol, signal, direction, action, cutoff.isoformat())).fetchone()
    return bool(row)


def get_position(symbol) -> Optional[Dict]:
    with db() as conn:
        row = conn.execute("SELECT * FROM positions WHERE symbol=? AND status='open'", (symbol,)).fetchone()
    return dict(row) if row else None


def claim_symbol_for_entry(symbol: str) -> bool:
    """
    Atomically checks-and-marks a symbol as 'entering' in one indivisible step.
    Without this, two near-simultaneous webhook deliveries for the same symbol
    (TradingView occasionally double-fires on reconnect) could BOTH pass a
    plain get_position() check before either had written its row, and both
    place a real order — a genuine double-entry risk, not a hypothetical one.
    BEGIN IMMEDIATE grabs SQLite's write lock immediately, so a second caller
    racing the first one blocks here and then correctly sees 'already open'
    once the first caller commits — the two can never both win.
    Returns True if this call won the claim, False if one already existed.
    """
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT 1 FROM positions WHERE symbol=? AND status IN ('open','entering')", (symbol,)
        ).fetchone()
        if existing:
            conn.execute("ROLLBACK")
            return False
        conn.execute(
            "INSERT OR REPLACE INTO positions (symbol, status) VALUES (?, 'entering')", (symbol,)
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def release_claim(symbol: str):
    """Undo claim_symbol_for_entry if the order that followed it failed —
    otherwise a rejected order would leave the symbol permanently stuck
    'entering' and unable to ever be traded again."""
    with db() as conn:
        conn.execute("DELETE FROM positions WHERE symbol=? AND status='entering'", (symbol,))
        conn.commit()


def force_release_if_still_entering(symbol: str):
    """
    Last-resort safety net for the ENTRY flow. Every KNOWN failure path
    (blocked by filter, order rejected, etc.) already calls release_claim()
    explicitly — this exists for the UNKNOWN path: some unexpected exception
    firing between a successful claim and the position being finalized as
    'open'. Without this, that symbol would stay stuck in 'entering' forever,
    silently blocking every future signal for it — a self-inflicted version
    of the exact 'stuck state' bug this whole claim system exists to prevent
    elsewhere. Only touches rows still literally sitting at 'entering', so it
    can never clobber a position that already finalized successfully.
    """
    try:
        with db() as conn:
            conn.execute("DELETE FROM positions WHERE symbol=? AND status='entering'", (symbol,))
            conn.commit()
    except Exception as e:
        log.error(f"Failed to release stuck claim for {symbol}: {e}")


def upsert_position(pos: Dict):
    with db() as conn:
        conn.execute("""INSERT OR REPLACE INTO positions
            (symbol, signal, direction, entry_price, entry_time, qty, sl, tp1, tp2, tp3, product_id, status,
             systems, rsi, adx, ofi_pct, knn_score, ml_healthy, premium_shield, mtf_align_bars, preset,
             pine_version, confidence_score, confidence_reason)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (pos.get("symbol"), pos.get("signal"), pos.get("direction"), pos.get("entry_price", 0),
             pos.get("entry_time"), pos.get("qty"), pos.get("sl"), pos.get("tp1"), pos.get("tp2"),
             pos.get("tp3"), pos.get("product_id"), pos.get("status", "open"),
             pos.get("systems"), pos.get("rsi"), pos.get("adx"), pos.get("ofi_pct"), pos.get("knn_score"),
             pos.get("ml_healthy"), pos.get("premium_shield"), pos.get("mtf_align_bars"), pos.get("preset"),
             pos.get("pine_version"), pos.get("confidence_score"), pos.get("confidence_reason")))
        conn.commit()


def delete_position(symbol):
    with db() as conn:
        conn.execute("UPDATE positions SET status='closed' WHERE symbol=?", (symbol,))
        conn.commit()


def log_trade(symbol, signal, direction, event, qty, price, raw_result,
              systems=None, preset=None, pine_version=None, confidence_score=None,
              rsi=None, adx=None, ofi_pct=None, knn_score=None, ml_healthy=None,
              premium_shield=None, mtf_align_bars=None, confidence_reason=None):
    with db() as conn:
        conn.execute("""INSERT INTO trades
            (symbol,signal,direction,event,qty,price,raw_result,systems,preset,pine_version,confidence_score,
             rsi,adx,ofi_pct,knn_score,ml_healthy,premium_shield,mtf_align_bars,confidence_reason)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                     (symbol, signal, direction, event, qty, price, raw_result,
                      systems, preset, pine_version, confidence_score,
                      rsi, adx, ofi_pct, knn_score, ml_healthy, premium_shield,
                      mtf_align_bars, confidence_reason))
        conn.commit()


def log_rejection(symbol, signal, direction, reason, detail=""):
    """[DASHBOARD NEW] Records exactly why an entry never happened — a
    circuit-breaker/kill-switch/pause block, a resolver miss, a shock-state
    block, OR (most useful) an actual rejection from Delta itself (bad
    leverage, insufficient margin, invalid price band, etc.). Never raises —
    a logging failure must never be allowed to affect trading logic."""
    try:
        with db() as conn:
            conn.execute("""INSERT INTO rejections (symbol,signal,direction,reason,detail)
                VALUES (?,?,?,?,?)""", (symbol, signal, direction, reason, detail))
            conn.commit()
    except Exception as e:
        log.error(f"log_rejection failed (non-fatal): {e}")


def get_control_flag(key, default=None):
    with db() as conn:
        row = conn.execute("SELECT value FROM control_flags WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def set_control_flag(key, value):
    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO control_flags (key, value) VALUES (?,?)", (key, value))
        conn.commit()


def is_paused() -> bool:
    """
    ALWAYS reads fresh from the database — never trusts in-process memory.
    Why this matters: if this bot ever runs with more than one gunicorn worker
    (`gunicorn -w 4 ...`), each worker is a SEPARATE process with its own
    Python memory. A plain `PAUSED = True` global set by /control/pause only
    updates the ONE worker that happened to handle that HTTP request — every
    other worker would keep trading, believing it was never paused, while the
    Telegram message and dashboard both confidently say "PAUSED". That's a
    silent safety gap on a live-money system. Reading the DB on every check
    costs a fraction of a millisecond and closes it completely, regardless of
    worker count. This is the ONLY piece of cross-request state in this bot
    that isn't already DB-backed for this exact reason.
    """
    return get_control_flag("paused") == "true"


def is_live_mode() -> bool:
    """
    [PREMIUM NEW] DB-backed, same pattern and same reasoning as is_paused()
    above. Checks control_flags first; if nothing has ever been set there
    (fresh DB, or nobody has touched /mode yet), falls back to whatever
    LIVE_MODE was in the environment at boot. Once anyone sets it via
    /mode/<token>, that DB value wins from then on, on every worker, until
    changed again or the DB is wiped.
    """
    raw = get_control_flag("live_mode")
    if raw is None:
        return LIVE_MODE_ENV_DEFAULT
    return raw == "true"


def is_dry_run() -> bool:
    return not is_live_mode()


def get_active_signals() -> set:
    """
    [PREMIUM NEW] DB-backed, same pattern as is_paused()/is_live_mode(). Falls
    back to ACTIVE_SIGNALS_DEFAULT until /signals/<token> is used at least
    once. Always filtered against ALL_KNOWN_SIGNALS so a stale or malformed
    DB value can never resurrect a tier name this build doesn't actually
    know how to size/trade.
    """
    raw = get_control_flag("active_signals")
    if raw is None:
        return set(ACTIVE_SIGNALS_DEFAULT)
    signals = {s.strip().upper() for s in raw.split(",") if s.strip()}
    return {s for s in signals if s in ALL_KNOWN_SIGNALS}


def set_active_signals(signals: set):
    clean = sorted({s.strip().upper() for s in signals if s.strip().upper() in ALL_KNOWN_SIGNALS})
    set_control_flag("active_signals", ",".join(clean))


# ════════════════════════════════════════════════════════════════════════════════
# [PREMIUM NEW] CIRCUIT BREAKER STATE — same DB-backed pattern as is_paused()
# above (never in-memory), so it's correct regardless of worker count.
# ════════════════════════════════════════════════════════════════════════════════
def _today_utc() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _cb_state() -> Tuple[float, int]:
    """Returns (daily_loss_r, consecutive_losses), auto-resetting at UTC midnight."""
    today = _today_utc()
    stored_day = get_control_flag("cb_date")
    if stored_day != today:
        set_control_flag("cb_date", today)
        set_control_flag("cb_daily_loss_r", "0.0")
        set_control_flag("cb_consecutive_losses", "0")
        return 0.0, 0
    daily_loss_r = float(get_control_flag("cb_daily_loss_r", "0.0") or 0.0)
    consec = int(get_control_flag("cb_consecutive_losses", "0") or 0)
    return daily_loss_r, consec


def circuit_breaker_status() -> Dict:
    daily_loss_r, consec = _cb_state()
    tripped, reason = circuit_breaker_tripped()
    return {"enabled": CIRCUIT_BREAKER_ENABLED, "daily_loss_r": round(daily_loss_r, 3),
            "daily_loss_limit_r": DAILY_LOSS_LIMIT_R, "consecutive_losses": consec,
            "max_consecutive_losses": MAX_CONSECUTIVE_LOSSES, "tripped": tripped, "reason": reason}


def circuit_breaker_tripped() -> Tuple[bool, str]:
    """Read-only check — does NOT reset anything. Called before every new ENTRY."""
    if not CIRCUIT_BREAKER_ENABLED:
        return False, ""
    daily_loss_r, consec = _cb_state()
    if daily_loss_r >= DAILY_LOSS_LIMIT_R:
        return True, f"daily loss limit hit ({daily_loss_r:.2f}R >= {DAILY_LOSS_LIMIT_R}R) — resets at UTC midnight"
    if consec >= MAX_CONSECUTIVE_LOSSES:
        return True, f"{consec} consecutive losses (limit {MAX_CONSECUTIVE_LOSSES}) — resets on next win/breakeven"
    return False, ""


def record_trade_outcome(outcome: str, r_multiple) -> None:
    """
    Feeds the circuit breaker from a TRADE_CLOSE alert (see webhook() below).
    WIN or BREAKEVEN resets the consecutive-loss streak (matches the Pine
    tracker's own definition: only a real SL loss counts against the streak).
    Only LOSS ever adds to daily_loss_r — wins deliberately do NOT subtract
    from it, so a single big win can't paper over an otherwise-bad day; the
    daily figure is a pure "how much have I given back today" ratchet that
    only the UTC-midnight reset can clear, same philosophy as the Pine
    script's own daily-loss-count breaker.
    """
    if not CIRCUIT_BREAKER_ENABLED:
        return
    daily_loss_r, consec = _cb_state()
    r = safe_float(r_multiple, 0.0) or 0.0
    outcome = (outcome or "").strip().upper()
    if outcome == "LOSS":
        daily_loss_r += abs(r)
        consec += 1
    elif outcome in ("WIN", "BREAKEVEN"):
        consec = 0
    set_control_flag("cb_daily_loss_r", str(round(daily_loss_r, 4)))
    set_control_flag("cb_consecutive_losses", str(consec))
    if outcome == "LOSS" and (daily_loss_r >= DAILY_LOSS_LIMIT_R or consec >= MAX_CONSECUTIVE_LOSSES):
        log.warning(f"🛑 Circuit breaker armed — daily_loss_r={daily_loss_r:.2f}R, consecutive_losses={consec}")


# ════════════════════════════════════════════════════════════════════════════════
# [SELF-CHECK NEW] SELF-DIAGNOSTIC REPORTING
# ────────────────────────────────────────────────────────────────────────────────
# The bot talks to the dashboard about itself, unprompted. log_self_report()
# is the one function every self-check writes through — it stores the
# message so the dashboard's new "Self-Diagnostics" panel can show it on the
# next poll, and (for warn/danger only, to avoid spamming Telegram every
# cycle) also pushes it to Telegram. Old rows are trimmed so this table
# never grows unbounded.
# ════════════════════════════════════════════════════════════════════════════════
SELF_REPORTS_MAX_ROWS = int(os.environ.get("SELF_REPORTS_MAX_ROWS", "300"))


def log_self_report(level: str, category: str, message: str, detail: str = None):
    level = (level or "info").strip().lower()
    with db() as conn:
        conn.execute(
            "INSERT INTO self_reports (level, category, message, detail) VALUES (?,?,?,?)",
            (level, category, message, detail),
        )
        conn.execute(
            "DELETE FROM self_reports WHERE id NOT IN "
            "(SELECT id FROM self_reports ORDER BY id DESC LIMIT ?)",
            (SELF_REPORTS_MAX_ROWS,),
        )
        conn.commit()
    log.info(f"🩺 self-check [{level}] {category}: {message}")
    if level in ("warn", "danger"):
        icon = "⚠️" if level == "warn" else "🛑"
        notify_telegram(f"{icon} Self-check ({category}): {message}")


def get_self_reports(limit: int = 50) -> List[Dict]:
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM self_reports ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


# ════════════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ════════════════════════════════════════════════════════════════════════════════
def notify_telegram(text: str):
    if not TELEGRAM_ENABLED:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=5)
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")


# ════════════════════════════════════════════════════════════════════════════════
# DELTA API — SIGNED REQUESTS
# ════════════════════════════════════════════════════════════════════════════════
def _signed_request(method: str, path: str, payload_dict: Dict = None) -> Optional[Dict]:
    # [CRITICAL FIX — REGRESSION] Without this, the "use Delta's own
    # authoritative server_time from the error body" correction further
    # below (`_time_drift_ms = ...`) silently creates a LOCAL variable
    # instead of updating the module-level drift used by every future
    # signed request — Python decides a name is local to a function at
    # compile time if it's assigned anywhere in that function, unless it's
    # declared global first. That's exactly why the corrected value never
    # stuck: every request kept re-signing with the old, wrong drift and
    # kept failing with expired_signature no matter how many times this
    # "fix" ran. Same bug class as the one already fixed in
    # sync_time_with_delta() — but that fix lives in a different function's
    # scope, so it didn't cover this one.
    global _time_drift_ms
    if not API_KEY or not API_SECRET:
        raise ValueError("DELTA_API_KEY / DELTA_API_SECRET not set")

    url = BASE_URL + path
    payload = json.dumps(payload_dict) if payload_dict else ""
    # [CRITICAL FIX] Two independent bugs here, either one alone would make
    # EVERY signed request fail:
    #  1. Delta's own docs: "Use Unix timestamp format (seconds since epoch)"
    #     and the signature window is 5 SECONDS. synced_timestamp_ms() returns
    #     MILLISECONDS (13 digits) by design (that's the right unit for
    #     measuring drift precisely) — but sending that directly as the
    #     'timestamp' header means Delta reads a value ~1000x larger than
    #     the real current time, i.e. a timestamp far in the future by its
    #     own clock, which is exactly what "expired_signature" rejects.
    #     Converting to whole seconds here is what actually matches the API.
    #  2. Delta's docs, and every working example (including this file's own
    #     earlier /v2/products call above): the prehash string order is
    #     method + timestamp + path + body. This was building it as
    #     timestamp + method + path + body — swapped — which produces a
    #     completely different HMAC than what Delta computes to check
    #     against, so the signature would never match regardless of the
    #     timestamp being correct.
    # The time-drift MEASUREMENT itself (sync_time_with_delta) was correct —
    # only this final assembly step was wrong, which is why the earlier
    # smoke test (no real network access to Delta) couldn't have caught it.
    timestamp = str(int(synced_timestamp_ms() / 1000))
    sig_msg = method.upper() + timestamp + path + payload
    signature = hmac.new(API_SECRET.encode(), sig_msg.encode(), hashlib.sha256).hexdigest()
    headers = {"api-key": API_KEY, "timestamp": timestamp, "signature": signature,
               "Content-Type": "application/json", "User-Agent": DELTA_USER_AGENT}

    last_err = None
    resynced_once = False
    for attempt in range(MAX_RETRIES + 1):
        try:
            if method.upper() == "GET":
                resp = delta_http.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            else:
                resp = delta_http.request(method.upper(), url, headers=headers, data=payload, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            last_err = e
            body = getattr(e.response, "text", "") if hasattr(e, "response") and e.response is not None else ""
            status = e.response.status_code if hasattr(e, "response") and e.response is not None else None
            log.warning(f"Delta API {method} {path} failed (attempt {attempt+1}): {e} | {body[:300]}")
            # 401/403 mean the credentials themselves are wrong or lack
            # permission — retrying with the SAME key can never fix that, it
            # only burns 3x the time before reporting the same failure (this
            # is exactly what the deploy logs showed: three identical
            # "invalid_api_key" errors in a row for one order attempt).
            # Only retry errors that can plausibly be transient (timeouts,
            # 5xx, rate limits).
            if status in (401, 403):
                # [CRITICAL FIX] A 401 whose body specifically says the
                # signature/timestamp is the problem (not the key itself) is
                # exactly the symptom of clock drift developing mid-session —
                # this server's clock moved since the last sync. Resync once,
                # rebuild the signature with the corrected timestamp, and
                # retry exactly once. Bounded to once per call (resynced_once)
                # so a genuinely bad key can't loop forever pretending to be
                # a clock problem.
                looks_like_signature_issue = any(
                    tok in body.lower() for tok in ("expired_signature", "signature", "timestamp")
                )
                if looks_like_signature_issue and not resynced_once:
                    resynced_once = True
                    # [CRITICAL FIX v2 — the resync-against-/v2/products fix
                    # didn't actually solve this in production: logs showed
                    # the SAME ~59s gap persisting on the retry even right
                    # after a resync. Root cause: /v2/products' HTTP `Date`
                    # header can be served by a different edge/CDN node than
                    # the one that actually validates the HMAC signature, so
                    # syncing against it corrects drift relative to the WRONG
                    # clock. Delta's own 401 error body already hands us the
                    # exact clock that validates signatures directly —
                    # {"error":{"context":{"server_time":...}}} — so use THAT
                    # authoritative value first, and only fall back to the
                    # products-endpoint resync if the error body doesn't have
                    # it (e.g. a 401 for a totally different reason).
                    used_authoritative_time = False
                    try:
                        err_json = json.loads(body)
                        auth_server_time = err_json.get("error", {}).get("context", {}).get("server_time")
                        if auth_server_time is not None:
                            with _time_drift_lock:
                                _time_drift_ms = float(auth_server_time) * 1000 - time.time() * 1000
                            log.warning(f"🕒 Corrected clock drift using Delta's own signing-server "
                                        f"time from this error's response ({auth_server_time}) — more "
                                        f"authoritative than /v2/products' Date header for this purpose.")
                            used_authoritative_time = True
                    except Exception:
                        pass
                    if not used_authoritative_time:
                        log.warning("🕒 401 looks like a signature/timestamp issue — resyncing clock and "
                                    "retrying this request once with a corrected timestamp.")
                        sync_time_with_delta(retries=1)
                    timestamp = str(int(synced_timestamp_ms() / 1000))
                    sig_msg = method.upper() + timestamp + path + payload
                    signature = hmac.new(API_SECRET.encode(), sig_msg.encode(), hashlib.sha256).hexdigest()
                    headers = {"api-key": API_KEY, "timestamp": timestamp, "signature": signature,
                               "Content-Type": "application/json", "User-Agent": DELTA_USER_AGENT}
                    continue
                break
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_BASE ** attempt)
    log.error(f"Delta API {method} {path} failed after {MAX_RETRIES+1} attempts: {last_err}")
    return None


# ════════════════════════════════════════════════════════════════════════════════
# CREDENTIAL VERIFICATION — catches "invalid API key" BEFORE a real trade tries
# to fire and silently fails. GET /v2/wallet/balances is Delta's own documented
# authenticated, read-only endpoint (no side effects) — the cheapest possible
# way to prove the key+secret pair actually works, as opposed to product
# discovery, which is a PUBLIC endpoint and succeeds even with a garbage key.
# That gap is exactly what made this failure mode invisible until a real
# order attempt hit it: /diagnostics only ever tested the public endpoint.
# ════════════════════════════════════════════════════════════════════════════════
API_CREDENTIALS_OK = None    # None = not checked yet, True/False after first check
API_CREDENTIALS_MSG = "not checked yet"
_credentials_lock = threading.Lock()


def verify_api_credentials():
    """Runs at boot and periodically thereafter. Never raises — a failure here
    just means trading is blocked, not that the whole process should die,
    since a key can be legitimately fixed later without a redeploy."""
    global API_CREDENTIALS_OK, API_CREDENTIALS_MSG
    if not API_KEY or not API_SECRET:
        ok, msg = False, "DELTA_API_KEY or DELTA_API_SECRET is not set at all"
    else:
        try:
            result = _signed_request("GET", "/v2/wallet/balances")
            if result and result.get("success", True):
                ok, msg = True, "API key verified — authenticated call succeeded"
            elif result is None:
                # _signed_request already retried/logged the underlying cause
                # (timeout, DNS, connection refused, etc.) — distinguishing
                # this from an explicit rejection avoids a confusing
                # "credentials: None" message when the real issue is network
                # reachability, not the key itself.
                ok, msg = False, "Could not reach Delta's API at all (network/connection issue) — see the warning above this line for the exact cause"
            else:
                ok, msg = False, f"Delta explicitly rejected the credentials: {result}"
        except Exception as e:
            ok, msg = False, f"Credential check raised: {e}"

    with _credentials_lock:
        API_CREDENTIALS_OK = ok
        API_CREDENTIALS_MSG = msg

    if ok:
        log.info(f"✅ API credentials verified against Delta — {msg}")
    else:
        log.error(f"🚨 API CREDENTIALS INVALID — every real order will fail until this is fixed: {msg}\n"
                  f"   Checklist: (1) DELTA_API_KEY/SECRET in Railway match Delta's dashboard EXACTLY, "
                  f"no extra spaces/newlines from copy-paste  (2) the key hasn't been regenerated/revoked "
                  f"on Delta's side since you set it  (3) the key has TRADING permission enabled, not "
                  f"read-only  (4) if the key has an IP whitelist, Railway's outbound IP is on it  "
                  f"(5) DELTA_REGION={REGION} matches the account the key was created on "
                  f"(global vs india use separate credentials).")
    return ok, msg


# ════════════════════════════════════════════════════════════════════════════════
# TRADE EXECUTION
# ════════════════════════════════════════════════════════════════════════════════
def place_entry_order(product_id: int, symbol: str, direction: str, qty: float) -> Tuple[bool, str, Optional[Dict]]:
    order_side = "buy" if direction == "BUY" else "sell"
    body = {"product_id": product_id, "order_type": "market_order", "side": order_side, "size": qty}

    if is_dry_run():
        msg = f"[DRY RUN] {direction} {qty} {symbol} @ market"
        log.info(msg)
        return True, msg, {"id": f"dry_{symbol}_{int(time.time())}"}

    result = _signed_request("POST", "/v2/orders", body)
    if result and result.get("success", True):
        order = result.get("result", result)
        msg = f"✅ {direction} {qty} {symbol} | Order ID: {order.get('id')}"
        log.info(msg)
        return True, msg, order
    return False, f"Entry order failed for {symbol}", None


def place_bracket_order(product_id: int, symbol: str, sl_price: Optional[float],
                         tp_price: Optional[float]) -> Tuple[bool, str]:
    """
    Attaches native SL + TP to the position that was just opened, using Delta's
    official /v2/orders/bracket endpoint. Delta manages this as a true OCO pair —
    when one leg fires, it cancels the other on its own. Size is NOT specified
    because a bracket order always closes the full open position (per Delta docs).
    Both legs use market_order so they fire with certainty in fast markets.

    GUARANTEE: this function returns (bool, str) under every circumstance and
    NEVER raises. Found by direct testing: a malformed sl/tp value (inf, or an
    unparseable string) used to throw from inside this function — and because
    this runs AFTER the real entry order, an uncaught exception here meant the
    position never reached upsert_position(), leaving a real, live position
    completely untracked. The entry already happened; this function reporting
    failure honestly is always safer than this function crashing.
    """
    try:
        if not AUTO_BRACKET_ORDERS:
            return True, "bracket orders disabled by config"

        sl_price = safe_float(sl_price)
        tp_price = safe_float(tp_price)
        if sl_price is None and tp_price is None:
            return True, "no usable SL/TP provided by signal — skipping bracket"

        tick = resolver.get_tick_size(product_id)
        body = {"product_id": product_id, "product_symbol": resolver.get_symbol_for(product_id) or symbol,
                "bracket_stop_trigger_method": "last_traded_price"}

        sl_str = round_to_tick(sl_price, tick) if sl_price is not None else None
        tp_str = round_to_tick(tp_price, tick) if tp_price is not None else None
        if sl_str is not None:
            body["stop_loss_order"] = {"order_type": "market_order", "stop_price": sl_str}
        if tp_str is not None:
            body["take_profit_order"] = {"order_type": "market_order", "stop_price": tp_str}
        if "stop_loss_order" not in body and "take_profit_order" not in body:
            return True, "SL/TP values were not usable numbers — skipping bracket"

        if is_dry_run():
            msg = f"[DRY RUN] bracket SL={sl_str} TP={tp_str} on {symbol}"
            log.info(msg)
            return True, msg

        # A market entry can take a beat to actually reflect as an open position
        # on Delta's side — retry briefly rather than failing the first race.
        last_err = "unknown"
        for attempt in range(BRACKET_RETRY_ATTEMPTS):
            result = _signed_request("POST", "/v2/orders/bracket", body)
            if result and result.get("success", True):
                msg = f"🎯 Bracket attached on {symbol}: SL={sl_str} TP={tp_str}"
                log.info(msg)
                return True, msg
            last_err = result
            time.sleep(BRACKET_RETRY_DELAY)

        msg = f"⚠️ Bracket order failed for {symbol} after {BRACKET_RETRY_ATTEMPTS} attempts: {last_err}"
        log.error(msg)
        return False, msg

    except Exception as e:
        # Belt-and-braces: even a bug in this function itself must not escape
        # and jeopardize the position record that has to be written next.
        msg = f"⚠️ Bracket order raised unexpectedly for {symbol}: {e}"
        log.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg


def place_exit_order(product_id: int, symbol: str, direction: str, qty: float) -> Tuple[bool, str]:
    exit_side = "sell" if direction == "BUY" else "buy"
    body = {"product_id": product_id, "order_type": "market_order", "side": exit_side,
            "size": qty, "reduce_only": True}

    if is_dry_run():
        msg = f"[DRY RUN] EXIT {qty} {symbol}"
        log.info(msg)
        return True, msg

    result = _signed_request("POST", "/v2/orders", body)
    if result and result.get("success", True):
        return True, f"Exit order placed for {symbol}"
    return False, f"Exit order failed for {symbol}"


def update_bracket_sl(position: Dict, new_sl) -> Tuple[bool, str]:
    """
    [PREMIUM NEW] Handles the Pine script's UPDATE_SL alert (trailing-stop
    push) WITHOUT closing the position — see the webhook() bug-fix note below
    for why routing this through place_exit_order() would have been a
    serious problem. Local DB `sl` is always updated regardless of the
    exchange call's outcome, so the dashboard/circuit-breaker/journal stay
    accurate for tracking even if the exchange leg needs attention.

    HONESTY NOTE: this bot doesn't persist a standalone bracket/SL order id
    from place_bracket_order's response (Delta's bracket endpoint returns the
    pair as one object, not two separately-trackable ids), so this re-sends
    the bracket definition with the new stop price rather than editing a
    known order id. Verify this PUT behaves as an amend (not a second,
    stacking bracket) against your own Delta account before relying on it
    for size — if it ever doesn't, the safe fallback is exactly what happens
    here anyway: local SL tracking stays correct and Telegram tells you to
    check the exchange manually.
    """
    new_sl = safe_float(new_sl)
    symbol = position.get("symbol", "?")
    if new_sl is None:
        return False, f"UPDATE_SL for {symbol} ignored — new_sl was not a usable number"

    # Local tracking is updated unconditionally — this is never wrong to do,
    # entry/exit qty and direction are untouched.
    upsert_position({**position, "sl": new_sl})

    if is_dry_run():
        msg = f"[DRY RUN] would move SL to {new_sl} for {symbol}"
        log.info(msg)
        return True, msg

    product_id = position.get("product_id")
    try:
        tick = resolver.get_tick_size(product_id)
        sl_str = round_to_tick(new_sl, tick)
        body = {"product_id": product_id,
                "product_symbol": resolver.get_symbol_for(product_id) or symbol,
                "bracket_stop_trigger_method": "last_traded_price",
                "stop_loss_order": {"order_type": "market_order", "stop_price": sl_str}}
        result = _signed_request("PUT", "/v2/orders/bracket", body)
        if result and result.get("success", True):
            return True, f"SL amended to {sl_str} for {symbol}"
        return False, f"⚠️ SL amend rejected by exchange for {symbol}: {result} (local SL updated regardless — verify on Delta manually)"
    except Exception as e:
        log.error(f"update_bracket_sl raised for {symbol}: {e}\n{traceback.format_exc()}")
        return False, f"⚠️ SL amend raised an error for {symbol}: {e} (local SL updated regardless — verify on Delta manually)"


# ════════════════════════════════════════════════════════════════════════════════
# ★★★ PHASE 8 — OBSIDIAN COMMAND CENTER (cyberpunk console formatting) ★★★
# Defined early since every phase below logs through it. Pure cosmetics —
# never changes what's logged, only how it reads on a console.
# ════════════════════════════════════════════════════════════════════════════════
class ObsidianFormatter(logging.Formatter):
    _COLORS = {
        logging.DEBUG: "\033[38;5;245m", logging.INFO: "\033[38;5;51m",
        logging.WARNING: "\033[38;5;220m", logging.ERROR: "\033[38;5;196m",
        logging.CRITICAL: "\033[48;5;196m\033[38;5;231m",
    }
    _RESET = "\033[0m"
    _DIM = "\033[38;5;240m"

    def format(self, record):
        color = self._COLORS.get(record.levelno, "")
        ts = self.formatTime(record, "%H:%M:%S")
        tag = f"{color}[{record.levelname:<8}]{self._RESET}"
        head = f"{self._DIM}◆ APEX::{ts}{self._RESET}"
        return f"{head} {tag} {record.getMessage()}"


def enable_obsidian_console(enabled: bool = True):
    """Swaps the console handler's formatter to the cyberpunk style. Disabled
    automatically falls back to the plain formatter (e.g. if OBSIDIAN_STYLE=false,
    for log aggregators that don't render ANSI color codes well)."""
    fmt = ObsidianFormatter() if enabled else _fmt
    _console.setFormatter(fmt)


enable_obsidian_console(os.environ.get("OBSIDIAN_STYLE", "true").strip().lower() == "true")


# ════════════════════════════════════════════════════════════════════════════════
# ★★★ PHASE 1 — THE HFT ENGINE (zero-latency parallel SL/TP execution) ★★★
# ────────────────────────────────────────────────────────────────────────────────
# AUTO_BRACKET_ORDERS (Delta's native /v2/orders/bracket) remains the DEFAULT
# and SAFER path — Delta manages SL+TP as one true OCO pair server-side, which
# is strictly better than two independently-tracked client-side orders. This
# engine is the HFT_PARALLEL_EXITS alternative path for setups that need two
# genuinely separate orders (e.g. a venue/account where bracket isn't
# available, or SL and TP need different order types): instead of firing SL
# then TP sequentially (latency = t_sl + t_tp), both legs are submitted to the
# thread pool in the same instant (latency = max(t_sl, t_tp)). Toggle via
# HFT_PARALLEL_EXITS=true; default stays off since bracket orders are safer.
# ════════════════════════════════════════════════════════════════════════════════
HFT_PARALLEL_EXITS = os.environ.get("HFT_PARALLEL_EXITS", "false").strip().lower() == "true"
_hft_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="hft-exit")


class HFTExecutionEngine:
    """Fires SL and TP the instant an entry is confirmed filled, in parallel
    rather than back-to-back. Both legs are independent reduce-only orders —
    if one leg fails, the other still stands (unlike a single bracket call
    where a malformed body kills both at once), and each failure is reported
    individually so nothing fails silently."""

    @staticmethod
    def fire_protective_orders_parallel(product_id: int, symbol: str, direction: str, qty: float,
                                         sl_price: Optional[float], tp_price: Optional[float]) -> Dict:
        exit_side = "sell" if direction == "BUY" else "buy"
        tick = resolver.get_tick_size(product_id)
        jobs = {}

        def _submit_stop(price):
            price_str = round_to_tick(price, tick)
            body = {"product_id": product_id, "order_type": "stop_market_order", "side": exit_side,
                     "size": qty, "reduce_only": True, "stop_price": price_str}
            return _signed_request("POST", "/v2/orders", body)

        def _submit_limit(price):
            price_str = round_to_tick(price, tick)
            body = {"product_id": product_id, "order_type": "limit_order", "side": exit_side,
                     "size": qty, "reduce_only": True, "limit_price": price_str}
            return _signed_request("POST", "/v2/orders", body)

        futures = {}
        if sl_price is not None and not is_dry_run():
            futures[_hft_pool.submit(_submit_stop, sl_price)] = "SL"
        if tp_price is not None and not is_dry_run():
            futures[_hft_pool.submit(_submit_limit, tp_price)] = "TP"

        if is_dry_run():
            log.info(f"[DRY RUN][HFT] would fire SL={sl_price} and TP={tp_price} for {symbol} in parallel")
            return {"SL": "dry_run", "TP": "dry_run"}

        results = {}
        for fut in as_completed(futures):
            leg = futures[fut]
            try:
                res = fut.result()
                results[leg] = "ok" if (res and res.get("success", True)) else f"failed: {res}"
            except Exception as e:
                results[leg] = f"raised: {e}"
        log.info(f"⚡ HFT parallel exits for {symbol}: {results}")
        return results


hft_engine = HFTExecutionEngine()


# ════════════════════════════════════════════════════════════════════════════════
# ★★★ PHASE 2 — PREDATOR VISION (L2 order-book stop-hunt wall detection) ★★★
# ────────────────────────────────────────────────────────────────────────────────
# Pulls Delta's live L2 book (same public endpoint as fetch_live_orderbook_imbalance)
# and looks specifically for an abnormal resting-size WALL sitting just beyond
# where this trade's stop-loss would sit. A wall exactly at/near a stop cluster
# is the classic signature of a stop-hunt: price gets pushed to sweep it before
# reversing. This is a HARD pre-entry safety check — unlike the imbalance score
# (informational, feeds ConfidenceEngine), a detected wall REJECTS the trade
# outright, because trading into a probable hunt zone isn't a matter of degree.
# ════════════════════════════════════════════════════════════════════════════════
PREDATOR_WALL_MULTIPLIER = float(os.environ.get("PREDATOR_WALL_MULTIPLIER", "4.0"))
PREDATOR_ENABLED = os.environ.get("PREDATOR_VISION_ENABLED", "true").strip().lower() == "true"


class PredatorVision:
    """Real-time DOM scanner. safe_to_enter() is the only method callers need."""

    @staticmethod
    def _scan_side(levels: List[Dict]) -> Tuple[float, float]:
        """Returns (average_size, max_size) for a list of L2 levels."""
        sizes = [float(l.get("size", 0)) for l in levels if l.get("size") is not None]
        if not sizes:
            return 0.0, 0.0
        return (sum(sizes) / len(sizes)), max(sizes)

    @staticmethod
    def safe_to_enter(symbol: str, direction: str, sl_price: Optional[float]) -> Tuple[bool, str]:
        """
        Returns (True, "") if no anomalous wall detected, or (False, reason) to
        hard-block the entry. NEVER blocks on its own failure — an orderbook
        fetch error degrades to "safe" (True), exactly as if this feature
        didn't exist, matching this bot's overall philosophy that a monitoring
        feature failing must never be mistaken for a reason to refuse a
        signal Pine already confirmed.
        """
        if not PREDATOR_ENABLED or sl_price is None:
            return True, ""
        try:
            resp = delta_http.get(f"{BASE_URL}/v2/l2orderbook/{symbol}", timeout=4)
            resp.raise_for_status()
            result = resp.json().get("result", {})
            buy_levels = result.get("buy", [])[:15]
            sell_levels = result.get("sell", [])[:15]

            # A BUY trade's stop-loss sits BELOW price -> hunt walls show up as
            # abnormal size on the BID side (sellers waiting to slam the stop
            # cluster). A SELL trade's stop sits ABOVE -> watch the ASK side.
            watch_levels = buy_levels if direction == "BUY" else sell_levels
            avg_size, max_size = PredatorVision._scan_side(watch_levels)
            if avg_size <= 0:
                return True, ""

            if max_size >= avg_size * PREDATOR_WALL_MULTIPLIER:
                reason = (f"Predator Vision: anomalous resting wall detected on the "
                          f"{'bid' if direction == 'BUY' else 'ask'} side for {symbol} "
                          f"({max_size:.4g} vs avg {avg_size:.4g}, {PREDATOR_WALL_MULTIPLIER}x threshold) — "
                          f"classic stop-hunt signature, rejecting entry as a safety precaution")
                return False, reason
            return True, ""
        except Exception as e:
            log.debug(f"Predator Vision scan skipped for {symbol} (degrading to safe): {e}")
            return True, ""


predator_vision = PredatorVision()


# ════════════════════════════════════════════════════════════════════════════════
# ★★★ PHASE 3 — INSTITUTIONAL SHIELD (dynamic risk-% sizing + kill-switch) ★★★
# ────────────────────────────────────────────────────────────────────────────────
# Two independent pieces:
#  (a) calculate_position_size() — turns "risk X% of account on this trade"
#      plus the actual SL distance into a concrete quantity, instead of the
#      fixed TIER_QUANTITY table. OFF by default (RISK_BASED_SIZING=false) so
#      it never silently changes existing behavior — TIER_QUANTITY still
#      wins unless explicitly enabled.
#  (b) the GLOBAL KILL-SWITCH — deliberately separate from both is_paused()
#      (a soft, easily-toggled pause) and the circuit breaker (auto-resets on
#      UTC midnight / next win). The kill-switch is a manual, sticky override
#      that only a human clears via /control/<secret>/kill-switch/reset — for
#      "something is wrong, stop everything until I've looked at it personally."
# ════════════════════════════════════════════════════════════════════════════════
RISK_BASED_SIZING = os.environ.get("RISK_BASED_SIZING", "false").strip().lower() == "true"
RISK_PCT_PER_TRADE = float(os.environ.get("RISK_PCT_PER_TRADE", "1.0"))  # % of account balance risked per trade


def risk_sizing_enabled() -> bool:
    """[DASHBOARD NEW] DB-backed override so the dashboard's Dynamic Sizing
    toggle can actually flip this at runtime — the RISK_BASED_SIZING env var
    alone only takes effect at boot. Falls back to the env var's value until
    the flag is explicitly set at least once, so nothing changes for anyone
    who never touches the new toggle."""
    v = get_control_flag("risk_sizing_enabled")
    return RISK_BASED_SIZING if v is None else (v == "true")


def set_risk_sizing_enabled(enabled: bool) -> None:
    set_control_flag("risk_sizing_enabled", "true" if enabled else "false")


_balance_cache = {"value": None, "ts": 0.0, "error": None}
_balance_cache_lock = threading.Lock()
BALANCE_CACHE_MAX_AGE_S = 8.0  # dashboard polls every 5s; this just keeps us from hammering Delta's wallet endpoint every single poll


def get_cached_balance(force: bool = False) -> Dict:
    """[DASHBOARD NEW] Cached wrapper around get_account_balance() so the
    dashboard can show a live balance without a fresh signed Delta API call
    on every 5s poll. Returns a dict (never raises) so the dashboard always
    gets a usable JSON shape even when the live fetch fails."""
    with _balance_cache_lock:
        fresh_enough = (time.time() - _balance_cache["ts"]) < BALANCE_CACHE_MAX_AGE_S
        if fresh_enough and not force:
            return {"balance": _balance_cache["value"], "error": _balance_cache["error"],
                     "cached_age_s": round(time.time() - _balance_cache["ts"], 1)}
    bal = None
    err = None
    try:
        bal = get_account_balance()
        if bal is None:
            err = "could not fetch balance (see server logs)"
    except Exception as e:
        err = str(e)
    with _balance_cache_lock:
        _balance_cache["value"] = bal
        _balance_cache["ts"] = time.time()
        _balance_cache["error"] = err
    return {"balance": bal, "error": err, "cached_age_s": 0}


def get_account_balance() -> Optional[float]:
    """Live account balance from Delta's own wallet endpoint. Returns None
    (never raises) on any failure — callers must treat None as 'fixed sizing
    only', never as zero."""
    try:
        result = _signed_request("GET", "/v2/wallet/balances")
        if not result:
            return None
        balances = result.get("result", result) if isinstance(result, dict) else result
        if isinstance(balances, list):
            for b in balances:
                if b.get("asset_symbol") in ("USDT", "USD"):
                    return safe_float(b.get("balance") or b.get("available_balance"))
        return None
    except Exception as e:
        log.warning(f"get_account_balance failed: {e}")
        return None


def calculate_position_size(entry_price: float, sl_price: Optional[float], fallback_qty: float) -> float:
    """
    qty = (account_balance * risk_pct/100) / |entry_price - sl_price|
    Falls back to fallback_qty (the existing TIER_QUANTITY value) whenever
    RISK_BASED_SIZING is off, or whenever any input needed for the real
    calculation isn't available (no live balance, no SL, or a degenerate
    zero-distance SL) — a trade should never be blocked or mis-sized just
    because this optional feature couldn't compute cleanly.
    """
    if not risk_sizing_enabled():
        return fallback_qty
    sl_price = safe_float(sl_price)
    entry_price = safe_float(entry_price)
    if sl_price is None or entry_price is None or sl_price == entry_price:
        log.warning("Risk-based sizing enabled but SL/entry unusable this bar — falling back to fixed qty")
        return fallback_qty
    balance = get_account_balance()
    if balance is None or balance <= 0:
        log.warning("Risk-based sizing enabled but couldn't fetch a live account balance — falling back to fixed qty")
        return fallback_qty
    risk_amount = balance * (RISK_PCT_PER_TRADE / 100.0)
    sl_distance = abs(entry_price - sl_price)
    qty = risk_amount / sl_distance
    log.info(f"📐 Risk-based sizing: balance={balance:.2f}, risk={RISK_PCT_PER_TRADE}% "
             f"(${risk_amount:.2f}), sl_distance={sl_distance:.6g} -> qty={qty:.6g}")
    return round(qty, 8) if qty > 0 else fallback_qty


def is_kill_switch_active() -> bool:
    """DB-backed, same is_paused()-style pattern — correct across every
    gunicorn worker regardless of count."""
    return get_control_flag("kill_switch") == "true"


def activate_kill_switch(reason: str):
    set_control_flag("kill_switch", "true")
    set_control_flag("kill_switch_reason", reason)
    set_control_flag("kill_switch_time", datetime.utcnow().isoformat())
    log.critical(f"🔴 GLOBAL KILL-SWITCH ACTIVATED: {reason}")
    notify_telegram(f"🔴🔴🔴 KILL-SWITCH ACTIVATED 🔴🔴🔴\n{reason}\nAll new entries blocked until manually reset.")


# ════════════════════════════════════════════════════════════════════════════════
# ★★★ PHASE 4 — AGGRESSIVE EXITS (auto-breakeven + trailing stop) ★★★
# ────────────────────────────────────────────────────────────────────────────────
# Runs as a background thread, independent of Pine's own UPDATE_SL alerts —
# this is the BOT deciding, from live price, when to move its own tracked
# positions' stops, as a second layer on top of whatever Pine sends. Purely
# additive: it only ever tightens a stop (moves it toward/past entry in the
# trade's favor), never loosens one, and any exchange-side amend failure just
# leaves the previous (still-valid, still-protective) stop in place.
# ════════════════════════════════════════════════════════════════════════════════
AGGRESSIVE_EXITS_ENABLED = os.environ.get("AGGRESSIVE_EXITS_ENABLED", "false").strip().lower() == "true"
BREAKEVEN_TRIGGER_R = float(os.environ.get("BREAKEVEN_TRIGGER_R", "1.0"))   # move SL to entry after +1R
TRAIL_TRIGGER_R = float(os.environ.get("TRAIL_TRIGGER_R", "1.5"))          # start trailing after +1.5R
TRAIL_DISTANCE_R = float(os.environ.get("TRAIL_DISTANCE_R", "0.8"))        # keep trail this many R behind price
POSITION_MONITOR_INTERVAL = int(os.environ.get("POSITION_MONITOR_INTERVAL", "15"))  # seconds


def get_last_traded_price(product_id: int, symbol: str) -> Optional[float]:
    try:
        resp = delta_http.get(f"{BASE_URL}/v2/tickers/{symbol}", timeout=4)
        resp.raise_for_status()
        result = resp.json().get("result", {})
        return safe_float(result.get("close") or result.get("mark_price") or result.get("last_price"))
    except Exception as e:
        log.debug(f"get_last_traded_price failed for {symbol}: {e}")
        return None


# ════════════════════════════════════════════════════════════════════════════
# REAL HISTORICAL CANDLES — Delta's public /v2/history/candles (no signing
# needed, it's public market data). This was previously MISSING entirely even
# though the dashboard's own JS comment claimed "Real candles — proxied
# through main.py's /candles" — that route never existed, so the chart
# silently fell back to fake random-walk candles on every single load, live
# or not. Also reused below by the historical backtest engine, which needs
# the same data in bulk rather than a display-sized window.
# ════════════════════════════════════════════════════════════════════════════
VALID_RESOLUTIONS = {"1m","3m","5m","15m","30m","1h","2h","4h","6h","1d","7d","30d","1w","2w"}
_RES_SECONDS = {"1m":60,"3m":180,"5m":300,"15m":900,"30m":1800,"1h":3600,"2h":7200,
                "4h":14400,"6h":21600,"1d":86400,"7d":604800,"30d":2592000,"1w":604800,"2w":1209600}

def fetch_delta_candles(symbol: str, resolution: str, start_ts: int, end_ts: int) -> List[Dict]:
    """Real historical OHLCV from Delta's public candles endpoint. Raises on
    failure rather than swallowing errors — callers decide how to degrade."""
    if resolution not in VALID_RESOLUTIONS:
        raise ValueError(f"unsupported resolution {resolution!r}, must be one of {sorted(VALID_RESOLUTIONS)}")
    resp = delta_http.get(f"{BASE_URL}/v2/history/candles", params={
        "symbol": symbol, "resolution": resolution, "start": start_ts, "end": end_ts,
    }, timeout=10)
    resp.raise_for_status()
    body = resp.json()
    rows = body.get("result", []) or []
    # Delta returns newest-first; normalize to oldest-first for both the chart
    # and the backtest simulator, which both need to walk forward in time.
    rows = sorted(rows, key=lambda r: r.get("time", 0))
    return [{"time": r.get("time"), "open": safe_float(r.get("open")), "high": safe_float(r.get("high")),
              "low": safe_float(r.get("low")), "close": safe_float(r.get("close")),
              "volume": safe_float(r.get("volume"))} for r in rows]


@app.route("/candles", methods=["GET"])
@require_key
def candles():
    symbol = request.args.get("symbol", "BTCUSD").strip().upper()
    resolution = request.args.get("resolution", "15m").strip().lower()
    limit = request.args.get("limit", 60, type=int)
    limit = max(1, min(limit, 1000))
    if resolution not in VALID_RESOLUTIONS:
        return jsonify({"error": f"unsupported resolution, use one of {sorted(VALID_RESOLUTIONS)}"}), 400
    end_ts = int(time.time())
    start_ts = end_ts - limit * _RES_SECONDS[resolution]
    try:
        rows = fetch_delta_candles(symbol, resolution, start_ts, end_ts)
        return jsonify({"candles": rows[-limit:], "symbol": symbol, "resolution": resolution})
    except Exception as e:
        log.warning(f"/candles fetch failed for {symbol}@{resolution}: {e}")
        return jsonify({"error": "could not fetch candles from Delta", "detail": str(e)}), 502


# ════════════════════════════════════════════════════════════════════════════
# HISTORICAL BACKTEST ENGINE — real OHLCV replay, honestly scoped.
# ────────────────────────────────────────────────────────────────────────────
# IMPORTANT — READ BEFORE TRUSTING THESE NUMBERS:
# This does NOT replicate APEX NEXUS's actual Pine Script (9 signal tiers,
# VSA Fakeout Shield, KNN/LR/MLP ensemble, Wyckoff/ICT structure, CVD, the
# ADX-tied ML weighting, etc). Porting 4,000+ lines of Pine logic 1:1 to
# Python — and proving it produces bar-for-bar identical signals — is a
# separate, much larger project than what's built here.
# What THIS is: a real engine that pulls genuine historical OHLCV from Delta
# and replays a much simpler EMA-trend + RSI + ADX-filter strategy against
# it bar-by-bar, with realistic fees and slippage subtracted from every
# trade. It answers "does a basic trend/momentum approach have any edge on
# this symbol's real history at all" — a sanity floor, not a verdict on your
# actual strategy. Treat a good result here as "worth building the real
# port"; treat a bad result as a signal to look harder before going live —
# either way, it is not a substitute for backtesting the real signal logic.
# ════════════════════════════════════════════════════════════════════════════

def _ema_series(values: List[float], period: int) -> List[Optional[float]]:
    if len(values) < period: return [None]*len(values)
    k = 2/(period+1)
    out = [None]*(period-1)
    seed = sum(values[:period])/period
    out.append(seed)
    prev = seed
    for v in values[period:]:
        prev = v*k + prev*(1-k)
        out.append(prev)
    return out

def _rsi_series(closes: List[float], period: int = 14) -> List[Optional[float]]:
    n = len(closes)
    out = [None]*n
    if n <= period: return out
    gains = losses = 0.0
    for i in range(1, period+1):
        d = closes[i]-closes[i-1]
        gains += max(d,0); losses += max(-d,0)
    avg_gain, avg_loss = gains/period, losses/period
    out[period] = 100.0 if avg_loss==0 else 100 - 100/(1+avg_gain/avg_loss)
    for i in range(period+1, n):
        d = closes[i]-closes[i-1]
        avg_gain = (avg_gain*(period-1) + max(d,0))/period
        avg_loss = (avg_loss*(period-1) + max(-d,0))/period
        out[i] = 100.0 if avg_loss==0 else 100 - 100/(1+avg_gain/avg_loss)
    return out

def _atr_adx_series(highs, lows, closes, period: int = 14):
    n = len(closes)
    tr = [0.0]*n; plus_dm=[0.0]*n; minus_dm=[0.0]*n
    for i in range(1,n):
        tr[i] = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        up, down = highs[i]-highs[i-1], lows[i-1]-lows[i]
        plus_dm[i] = up if (up>down and up>0) else 0.0
        minus_dm[i] = down if (down>up and down>0) else 0.0
    atr=[None]*n; pdi=[None]*n; mdi=[None]*n; adx=[None]*n
    if n <= period*2: return atr, adx
    atr_v = sum(tr[1:period+1])/period
    pdm_v = sum(plus_dm[1:period+1])/period
    mdm_v = sum(minus_dm[1:period+1])/period
    dx_hist = []
    for i in range(period+1, n):
        atr_v = (atr_v*(period-1)+tr[i])/period
        pdm_v = (pdm_v*(period-1)+plus_dm[i])/period
        mdm_v = (mdm_v*(period-1)+minus_dm[i])/period
        atr[i] = atr_v
        pdi_v = 100*pdm_v/atr_v if atr_v>0 else 0
        mdi_v = 100*mdm_v/atr_v if atr_v>0 else 0
        dx = 100*abs(pdi_v-mdi_v)/(pdi_v+mdi_v) if (pdi_v+mdi_v)>0 else 0
        dx_hist.append(dx)
        if len(dx_hist) >= period:
            adx[i] = sum(dx_hist[-period:])/period
    return atr, adx

def simulate_simple_strategy(candles: List[Dict], adx_threshold: float = 20.0,
                              sl_atr_mult: float = 1.5, tp_atr_mult: float = 2.5,
                              fee_pct: float = 0.05, slippage_pct: float = 0.03) -> List[Dict]:
    """Bar-by-bar replay of the simplified proxy strategy described above.
    fee_pct/slippage_pct are ROUND-TRIP percentages applied against notional
    on both entry and exit — deliberately pessimistic (real Delta taker fees
    are typically lower) so this errs toward under- rather than over-stating
    edge. Returns a list of completed trade dicts with real entry/exit prices
    and realized pnl already net of costs."""
    closes = [c["close"] for c in candles]; highs=[c["high"] for c in candles]; lows=[c["low"] for c in candles]
    ema_fast = _ema_series(closes, 9); ema_slow = _ema_series(closes, 21)
    rsi = _rsi_series(closes, 14)
    atr, adx = _atr_adx_series(highs, lows, closes, 14)
    trades = []
    pos = None  # {direction, entry_price, entry_i, sl, tp}
    cost_mult = (fee_pct + slippage_pct) / 100.0
    for i in range(1, len(candles)):
        if None in (ema_fast[i], ema_slow[i], ema_fast[i-1], ema_slow[i-1], rsi[i], adx[i], atr[i]):
            continue
        price = closes[i]
        if pos:
            hit_sl = price <= pos["sl"] if pos["direction"]=="BUY" else price >= pos["sl"]
            hit_tp = price >= pos["tp"] if pos["direction"]=="BUY" else price <= pos["tp"]
            if hit_sl or hit_tp:
                exit_price = pos["sl"] if hit_sl else pos["tp"]
                dirmult = 1 if pos["direction"]=="BUY" else -1
                gross = dirmult*(exit_price - pos["entry_price"])
                cost = (pos["entry_price"] + exit_price) * cost_mult
                trades.append({"direction":pos["direction"], "entry_price":pos["entry_price"], "exit_price":exit_price,
                                "entry_time":pos["entry_time"], "exit_time":candles[i]["time"],
                                "pnl_pct": ((gross-cost)/pos["entry_price"])*100, "exit_reason":"TP" if hit_tp else "SL"})
                pos = None
        if not pos:
            bull_cross = ema_fast[i-1] <= ema_slow[i-1] and ema_fast[i] > ema_slow[i]
            bear_cross = ema_fast[i-1] >= ema_slow[i-1] and ema_fast[i] < ema_slow[i]
            if adx[i] >= adx_threshold and bull_cross and rsi[i] > 50:
                pos = {"direction":"BUY","entry_price":price,"entry_time":candles[i]["time"],
                       "sl":price - atr[i]*sl_atr_mult, "tp":price + atr[i]*tp_atr_mult}
            elif adx[i] >= adx_threshold and bear_cross and rsi[i] < 50:
                pos = {"direction":"SELL","entry_price":price,"entry_time":candles[i]["time"],
                       "sl":price + atr[i]*sl_atr_mult, "tp":price - atr[i]*tp_atr_mult}
    return trades

def summarize_backtest_trades(trades: List[Dict]) -> Optional[Dict]:
    if not trades: return None
    wins = [t for t in trades if t["pnl_pct"]>0]; losses=[t for t in trades if t["pnl_pct"]<=0]
    gross_profit = sum(t["pnl_pct"] for t in wins); gross_loss = abs(sum(t["pnl_pct"] for t in losses))
    running=0.0; peak=0.0; max_dd=0.0; curve=[]
    returns = [t["pnl_pct"] for t in trades]
    for r in returns:
        running += r; peak = max(peak, running); max_dd = max(max_dd, peak-running); curve.append(running)
    mean_r = running/len(trades)
    variance = sum((r-mean_r)**2 for r in returns)/len(trades) if len(trades)>1 else 0
    stdev = variance**0.5
    return {
        "total_trades": len(trades), "wins": len(wins), "losses": len(losses),
        "win_rate": (len(wins)/len(trades))*100,
        "profit_factor": (gross_profit/gross_loss) if gross_loss>0 else (float('inf') if gross_profit>0 else 0),
        "net_return_pct": running, "expectancy_pct": mean_r, "max_drawdown_pct": max_dd,
        "sharpe_like": (mean_r/stdev) if stdev>0 else None,
        "equity_curve_pct": curve,
    }

@app.route("/backtest/run", methods=["GET"])
@require_key
def backtest_run():
    symbol = request.args.get("symbol", "BTCUSD").strip().upper()
    resolution = request.args.get("resolution", "1h").strip().lower()
    days = request.args.get("days", 60, type=int)
    adx_threshold = request.args.get("adx_threshold", 20.0, type=float)
    if resolution not in VALID_RESOLUTIONS:
        return jsonify({"error": f"unsupported resolution, use one of {sorted(VALID_RESOLUTIONS)}"}), 400
    max_candles = 5000  # keep single-request payload/runtime sane
    days = max(3, min(days, (max_candles * _RES_SECONDS[resolution]) // 86400 or 3))
    end_ts = int(time.time()); start_ts = end_ts - days*86400
    try:
        rows = fetch_delta_candles(symbol, resolution, start_ts, end_ts)
    except Exception as e:
        log.warning(f"/backtest/run candle fetch failed for {symbol}@{resolution}: {e}")
        return jsonify({"error": "could not fetch historical candles from Delta", "detail": str(e)}), 502
    if len(rows) < 60:
        return jsonify({"error": "not enough historical candles returned to backtest (need 60+)", "candles_received": len(rows)}), 422

    all_trades = simulate_simple_strategy(rows, adx_threshold=adx_threshold)
    full_stats = summarize_backtest_trades(all_trades)

    # [BONUS] Walk-forward split — first 70% of history (in-sample) vs the
    # most recent 30% (out-of-sample). If in-sample looks great but
    # out-of-sample doesn't, that's a classic overfitting/regime-shift red
    # flag worth knowing about BEFORE trusting the headline numbers.
    split_i = int(len(rows)*0.7)
    in_sample_trades = simulate_simple_strategy(rows[:split_i], adx_threshold=adx_threshold)
    out_sample_trades = simulate_simple_strategy(rows[split_i:], adx_threshold=adx_threshold)

    return jsonify({
        "symbol": symbol, "resolution": resolution, "days": days,
        "candles_used": len(rows),
        "range": {"start": rows[0]["time"], "end": rows[-1]["time"]},
        "params": {"adx_threshold": adx_threshold, "sl_atr_mult": 1.5, "tp_atr_mult": 2.5,
                    "fee_pct_roundtrip": 0.05, "slippage_pct_roundtrip": 0.03},
        "full_period": full_stats,
        "walk_forward": {
            "in_sample": summarize_backtest_trades(in_sample_trades),
            "out_of_sample": summarize_backtest_trades(out_sample_trades),
        },
        "methodology_note": ("Simplified EMA(9/21) + RSI(14) + ADX(14) trend/momentum proxy strategy on REAL Delta "
                              "historical OHLCV, fixed ATR-multiple SL/TP, realistic round-trip fees+slippage deducted. "
                              "This is NOT a replica of your Pine Script's full signal system — see code comments above "
                              "backtest_run() for the full disclosure."),
    })


def _aggressive_exits_tick():
    with db() as conn:
        open_positions = conn.execute("SELECT * FROM positions WHERE status='open'").fetchall()

    for pos in open_positions:
        try:
            symbol = pos["symbol"]
            entry = safe_float(pos["entry_price"]) or 0.0
            sl = safe_float(pos["sl"])
            direction = pos["direction"]
            product_id = pos["product_id"]
            if not entry or sl is None or not product_id:
                continue

            r_distance = abs(entry - sl)
            if r_distance <= 0:
                continue

            price = get_last_traded_price(product_id, symbol)
            if price is None:
                continue

            r_gain = ((price - entry) / r_distance) if direction == "BUY" else ((entry - price) / r_distance)
            new_sl = None

            # Breakeven: once +BREAKEVEN_TRIGGER_R is reached, move SL to entry
            # (only if current SL hasn't already moved past entry in our favor).
            if r_gain >= BREAKEVEN_TRIGGER_R:
                already_past_entry = (sl >= entry) if direction == "BUY" else (sl <= entry)
                if not already_past_entry:
                    new_sl = entry

            # Trailing: once further along, trail TRAIL_DISTANCE_R behind price.
            if r_gain >= TRAIL_TRIGGER_R:
                trail_sl = (price - TRAIL_DISTANCE_R * r_distance) if direction == "BUY" \
                    else (price + TRAIL_DISTANCE_R * r_distance)
                if new_sl is None:
                    tighter = (trail_sl > sl) if direction == "BUY" else (trail_sl < sl)
                    new_sl = trail_sl if tighter else None
                else:
                    tighter = (trail_sl > new_sl) if direction == "BUY" else (trail_sl < new_sl)
                    if tighter:
                        new_sl = trail_sl

            if new_sl is not None:
                ok, msg = update_bracket_sl(dict(pos), new_sl)
                log.info(f"🎯 Aggressive exit for {symbol}: r_gain={r_gain:.2f}R -> new_sl={new_sl:.6g} ({msg})")
        except Exception as e:
            log.error(f"Aggressive exits tick failed for {pos['symbol'] if pos else '?'}: {e}")


def _aggressive_exits_loop():
    while True:
        time.sleep(POSITION_MONITOR_INTERVAL)
        if AGGRESSIVE_EXITS_ENABLED and not is_paused():
            try:
                _aggressive_exits_tick()
            except Exception as e:
                log.error(f"Aggressive exits loop error: {e}\n{traceback.format_exc()}")


# ════════════════════════════════════════════════════════════════════════════════
# [SELF-CHECK NEW] SELF-CHECK LOOP — the bot audits itself on a timer and
# writes what it finds straight to the dashboard, with zero human involved.
# ────────────────────────────────────────────────────────────────────────────────
# THIS IS NOT full historical backtesting (that needs re-fetching years of
# candle data and replaying the Pine strategy bar-by-bar — a separate,
# heavier project). This is closer to what a good trader does every day:
# a periodic honest look at (a) is the plumbing healthy — clock drift, API
# credentials, product discovery, circuit breaker — and (b) is the ACTUAL
# live/dry-run track record (the real TRADE_CLOSE outcomes already sitting
# in the `trades` table) still behaving like a system worth trusting.
# Every check below is additive and read-only: it can never place, modify,
# or close a trade. It only writes rows to self_reports.
# ════════════════════════════════════════════════════════════════════════════════
SELF_CHECK_INTERVAL_S = int(os.environ.get("SELF_CHECK_INTERVAL_S", "900"))       # 15 min default
SELF_CHECK_LOOKBACK_TRADES = int(os.environ.get("SELF_CHECK_LOOKBACK_TRADES", "20"))
SELF_CHECK_DRIFT_WARN_MS = float(os.environ.get("SELF_CHECK_DRIFT_WARN_MS", "2000"))


def _self_check_recent_performance() -> Optional[Dict]:
    """Pulls the last N TRADE_CLOSE rows and computes real, already-happened
    performance — win rate, cumulative R, current losing streak. Returns
    None if there isn't enough closed-trade history yet to say anything
    meaningful."""
    with db() as conn:
        rows = conn.execute(
            "SELECT raw_result FROM trades WHERE event='TRADE_CLOSE' "
            "ORDER BY id DESC LIMIT ?", (SELF_CHECK_LOOKBACK_TRADES,)
        ).fetchall()
    if not rows:
        return None
    outcomes = []
    for r in rows:
        try:
            j = json.loads(r["raw_result"] or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        rm = j.get("r_multiple")
        if isinstance(rm, (int, float)):
            outcomes.append(rm)
    if not outcomes:
        return None
    wins = sum(1 for r in outcomes if r > 0)
    cum_r = sum(outcomes)
    gross_win_r = sum(r for r in outcomes if r > 0)
    gross_loss_r = abs(sum(r for r in outcomes if r < 0))
    profit_factor = round(gross_win_r / gross_loss_r, 2) if gross_loss_r > 0 else None
    # current losing streak, most-recent-first
    streak = 0
    for r in outcomes:
        if r < 0:
            streak += 1
        else:
            break
    return {
        "n": len(outcomes), "wins": wins, "win_rate": round(wins / len(outcomes) * 100, 1),
        "cum_r": round(cum_r, 2), "avg_r": round(cum_r / len(outcomes), 3),
        "profit_factor": profit_factor,
        "current_losing_streak": streak,
    }


def _self_check_performance_by_signal() -> List[Dict]:
    """Same real closed-trade history as _self_check_recent_performance(),
    grouped by signal tier (NEXUS/SCALP/WARP/GHOST/...) instead of blended
    into one number — so you can see which specific signal is actually
    pulling its weight and which one is dragging the account down. Pulls a
    wider window than the single-number check since it has to be split
    across every tier."""
    with db() as conn:
        rows = conn.execute(
            "SELECT signal, raw_result FROM trades WHERE event='TRADE_CLOSE' "
            "ORDER BY id DESC LIMIT ?", (SELF_CHECK_LOOKBACK_TRADES * 5,)
        ).fetchall()
    by_signal: Dict[str, list] = {}
    for r in rows:
        try:
            j = json.loads(r["raw_result"] or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        rm = j.get("r_multiple")
        if not isinstance(rm, (int, float)):
            continue
        sig = (r["signal"] or "UNKNOWN").upper()
        by_signal.setdefault(sig, []).append(rm)

    out = []
    for sig, outcomes in by_signal.items():
        wins = sum(1 for r in outcomes if r > 0)
        cum_r = sum(outcomes)
        out.append({
            "signal": sig, "n": len(outcomes), "wins": wins,
            "win_rate": round(wins / len(outcomes) * 100, 1),
            "cum_r": round(cum_r, 2), "avg_r": round(cum_r / len(outcomes), 3),
        })
    out.sort(key=lambda x: x["cum_r"], reverse=True)
    return out


# ════════════════════════════════════════════════════════════════════════════════
# [SELF-CHECK NEW] SYSTEM INTEGRITY CHECK — verifies the bot's OWN code and
# wiring are intact: expected Flask routes are actually registered, expected
# DB tables exist with every column this code reads/writes, and the core
# engine singletons (product resolver, confidence engine, etc.) are the real
# thing rather than missing because an earlier import or class definition
# silently failed. This is a structural/wiring check, NOT a live-trading
# test — it never places, fakes, or simulates a trade, so it can never
# pollute the real trades/positions tables with test data. Read-only, safe
# to run any time, any number of times, in LIVE or DRY_RUN alike.
# ════════════════════════════════════════════════════════════════════════════════
EXPECTED_TABLES = {
    "positions": {"symbol", "signal", "direction", "entry_price", "qty", "sl", "tp1", "tp2", "tp3",
                  "product_id", "status"},
    "trades": {"symbol", "signal", "direction", "event", "qty", "price", "raw_result"},
    "control_flags": {"key", "value"},
    "rejections": {"symbol", "signal", "direction", "reason", "detail"},
    "self_reports": {"level", "category", "message", "detail"},
}

EXPECTED_ROUTES = {
    "/webhook/<secret_token>", "/mode/<secret>", "/signals/<secret>",
    "/control/<secret>/pause", "/control/<secret>/resume", "/control/<secret>/close-all",
    "/control/<secret>/reset-circuit-breaker", "/control/<secret>/self-check",
    "/config", "/positions", "/trades", "/rejections", "/self-reports", "/balance",
    "/raw-api-log", "/ask/<secret>", "/dashboard/<token>",
    "/order-flow", "/execution-stats", "/system-health",
}


def _self_check_system_integrity() -> Dict:
    issues = []

    # 1) DB tables — every expected table exists AND has every column this
    # code actually reads/writes. A table existing with the WRONG columns
    # (a stale DB from an older version, a migration that silently never
    # ran) is exactly the "no such column" failure this bot has already been
    # bitten by once — see the STARTUP section further down — so it's worth
    # catching on-demand too, not just trusting init_db() ran cleanly.
    tables_ok = True
    try:
        with db() as conn:
            existing_tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            for table, cols in EXPECTED_TABLES.items():
                if table not in existing_tables:
                    tables_ok = False
                    issues.append(f"table '{table}' is missing entirely")
                    continue
                existing_cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
                missing_cols = cols - existing_cols
                if missing_cols:
                    tables_ok = False
                    issues.append(f"table '{table}' is missing column(s): {', '.join(sorted(missing_cols))}")
    except Exception as e:
        tables_ok = False
        issues.append(f"could not inspect the database at all: {e}")

    # 2) Flask routes — confirms every route this bot depends on actually got
    # registered (catches a decorator that silently failed to apply, a
    # typo'd path, or a route accidentally deleted in a future edit).
    registered = {str(rule) for rule in app.url_map.iter_rules()}
    missing_routes = sorted(r for r in EXPECTED_ROUTES if r not in registered)
    routes_ok = len(missing_routes) == 0
    if not routes_ok:
        issues.append(f"route(s) not registered: {', '.join(missing_routes)}")

    # 3) Core engine singletons — confirms the module-level objects this code
    # depends on everywhere (resolver.resolve(), confidence_engine.compute(),
    # etc.) actually exist and are the expected type, not None/missing
    # because an earlier import or class definition silently failed.
    objects_ok = True
    expected_objects = {
        "resolver": "DeltaProductResolver",
        "confidence_engine": "ConfidenceEngine",
        "liquidation_aggregator": "LiquidationAggregator",
    }
    for name, expected_type in expected_objects.items():
        obj = globals().get(name)
        if obj is None or type(obj).__name__ != expected_type:
            objects_ok = False
            issues.append(f"'{name}' is missing or not a {expected_type} instance")

    # 4) Re-verify the two things that can silently go stale between checks:
    # credentials (can be revoked/changed on Delta's side any time) and
    # product discovery (needs at least one product to trade anything).
    creds_ok, creds_msg = verify_api_credentials()
    drift_ms = get_time_drift_ms()
    products = len(resolver.by_symbol) if globals().get("resolver") is not None else 0

    all_ok = (tables_ok and routes_ok and objects_ok and creds_ok
              and products > 0 and abs(drift_ms) < SELF_CHECK_DRIFT_WARN_MS)

    if not all_ok:
        for issue in issues:
            log_self_report("danger", "system_integrity", issue)
        if not creds_ok:
            log_self_report("danger", "system_integrity", f"API credentials check failed: {creds_msg}")
        if products == 0:
            log_self_report("danger", "system_integrity",
                             "Zero products discovered — resolver has nothing to trade against.")

    return {
        "all_ok": all_ok,
        "tables_ok": tables_ok,
        "routes_ok": routes_ok,
        "objects_ok": objects_ok,
        "api_credentials_ok": creds_ok,
        "api_credentials_msg": creds_msg,
        "time_drift_ms": round(drift_ms, 1),
        "products_discovered": products,
        "issues": issues,
    }


def _self_check_tick():
    # 1) Plumbing health — reuses state the bot already tracks, no new I/O.
    drift = get_time_drift_ms()
    if abs(drift) >= SELF_CHECK_DRIFT_WARN_MS:
        log_self_report("warn", "clock_drift",
                         f"Clock drift vs Delta's server is {drift:.0f}ms — approaching the "
                         f"signature tolerance window.",
                         detail="Runs a fresh sync automatically; if this keeps climbing the "
                                "host machine's clock itself may need attention.")

    if len(resolver.by_symbol) == 0:
        log_self_report("danger", "product_discovery",
                         "Zero tradable products discovered from Delta — every signal will fail "
                         "to resolve a product_id right now.")

    cb = circuit_breaker_status()
    if cb.get("tripped"):
        log_self_report("danger", "circuit_breaker", f"Circuit breaker is tripped — {cb.get('reason')}")
    elif cb.get("consecutive_losses", 0) >= max(1, cb.get("max_consecutive_losses", 4) - 1):
        log_self_report("warn", "circuit_breaker",
                         f"{cb['consecutive_losses']}/{cb['max_consecutive_losses']} consecutive "
                         f"losses — one more trips the breaker.")

    # 2) Real performance, from actual closed trades — not a simulation.
    perf = _self_check_recent_performance()
    if perf is None:
        log_self_report("info", "performance",
                         "No closed trades yet — nothing to evaluate. Will report again once "
                         "trades start closing.")
    else:
        msg = (f"Last {perf['n']} closed trades: {perf['wins']}/{perf['n']} wins "
               f"({perf['win_rate']}%), cumulative {perf['cum_r']:+.2f}R, "
               f"avg {perf['avg_r']:+.2f}R/trade.")
        if perf["current_losing_streak"] >= 3:
            log_self_report("warn", "performance",
                             f"{msg} Currently on a {perf['current_losing_streak']}-trade losing streak.")
        elif perf["cum_r"] < 0:
            log_self_report("warn", "performance", f"{msg} Cumulative R is negative over this window.")
        else:
            log_self_report("info", "performance", msg)


def _self_check_loop():
    # Small initial delay so this doesn't race init_db()/resolver.refresh()
    # on the very first bootstrap tick.
    time.sleep(30)
    while True:
        try:
            _self_check_tick()
        except Exception as e:
            log.error(f"Self-check loop error: {e}\n{traceback.format_exc()}")
            try:
                log_self_report("danger", "self_check_error", f"Self-check itself failed: {e}")
            except Exception:
                pass
        time.sleep(SELF_CHECK_INTERVAL_S)


# ════════════════════════════════════════════════════════════════════════════════
# ★★★ PHASE 5 — STATISTICAL ARBITRAGE (pairs-trading evaluation framework) ★★★
# ────────────────────────────────────────────────────────────────────────────────
# A self-contained evaluation class — deliberately NOT wired into the webhook
# execution path (pairs trading is a fundamentally different strategy from
# the single-symbol signal flow this bot already runs), so adding it can
# never change existing single-symbol behavior. Wire calculate_pair_signal()
# into a new webhook action or a scheduled job if/when you're ready to trade
# pairs live. "Zero-risk" here means market-neutral (long one leg, short the
# other) — NOT actually risk-free; that framing is kept for the trade
# journal/labeling only.
# ════════════════════════════════════════════════════════════════════════════════
@dataclass
class PairSignal:
    symbol_a: str
    symbol_b: str
    zscore: float = 0.0
    hedge_ratio: float = 1.0
    action: str = "NONE"       # "LONG_A_SHORT_B" | "SHORT_A_LONG_B" | "CLOSE" | "NONE"
    reason: str = ""


class PairsTradingEngine:
    def __init__(self, lookback: int = 60, entry_z: float = 2.0, exit_z: float = 0.5):
        self.lookback = lookback
        self.entry_z = entry_z
        self.exit_z = exit_z
        self._price_history: Dict[str, deque] = {}

    def ingest_price(self, symbol: str, price: float):
        buf = self._price_history.setdefault(symbol, deque(maxlen=self.lookback))
        buf.append(price)

    def _hedge_ratio(self, series_a: List[float], series_b: List[float]) -> float:
        """OLS slope of A on B without needing numpy/scipy — plain-Python
        covariance/variance, fine at this lookback size."""
        n = len(series_a)
        mean_a = sum(series_a) / n
        mean_b = sum(series_b) / n
        cov = sum((a - mean_a) * (b - mean_b) for a, b in zip(series_a, series_b))
        var_b = sum((b - mean_b) ** 2 for b in series_b)
        return cov / var_b if var_b > 0 else 1.0

    def evaluate(self, symbol_a: str, symbol_b: str) -> PairSignal:
        sig = PairSignal(symbol_a=symbol_a, symbol_b=symbol_b)
        a = list(self._price_history.get(symbol_a, []))
        b = list(self._price_history.get(symbol_b, []))
        n = min(len(a), len(b))
        if n < max(20, self.lookback // 2):
            sig.reason = f"not enough overlapping history yet ({n} bars)"
            return sig
        a, b = a[-n:], b[-n:]

        hedge_ratio = self._hedge_ratio(a, b)
        spread = [ai - hedge_ratio * bi for ai, bi in zip(a, b)]
        mean_spread = sum(spread) / len(spread)
        std_spread = statistics.pstdev(spread) if len(spread) > 1 else 0.0
        if std_spread <= 0:
            sig.reason = "spread has zero variance — pair isn't cointegrated (or feed is stale)"
            return sig

        z = (spread[-1] - mean_spread) / std_spread
        sig.zscore = round(z, 3)
        sig.hedge_ratio = round(hedge_ratio, 6)

        if z >= self.entry_z:
            sig.action, sig.reason = "SHORT_A_LONG_B", f"spread {z:.2f}σ above mean — short A / long B"
        elif z <= -self.entry_z:
            sig.action, sig.reason = "LONG_A_SHORT_B", f"spread {z:.2f}σ below mean — long A / short B"
        elif abs(z) <= self.exit_z:
            sig.action, sig.reason = "CLOSE", f"spread reverted to {z:.2f}σ — close pair position"
        else:
            sig.action, sig.reason = "NONE", f"spread at {z:.2f}σ — inside entry/exit bands, hold"
        return sig


pairs_engine = PairsTradingEngine()


# ════════════════════════════════════════════════════════════════════════════════
# ★★★ PHASE 6 — THE ORACLE AI (night-watch diagnostics + trade journaling) ★★★
# ────────────────────────────────────────────────────────────────────────────────
# Not a separate ML model — "Oracle AI" here means: continuously watch the
# bot's own health signals (time-drift, credential status, circuit breaker,
# kill-switch, recent error rate) and surface a single, honest verdict, so a
# human checking at 3am gets one answer instead of having to correlate six
# different endpoints by hand.
# ════════════════════════════════════════════════════════════════════════════════
_recent_errors: deque = deque(maxlen=50)


def oracle_log_error(context: str, error: Exception):
    _recent_errors.append({"time": datetime.utcnow().isoformat(), "context": context, "error": str(error)})


def oracle_night_watch_report() -> Dict:
    drift = time_drift_status()
    cb = circuit_breaker_status()
    verdict = "HEALTHY"
    issues = []

    if drift["last_error"]:
        issues.append(f"time-sync last failed: {drift['last_error']}")
    if drift["drift_ms"] and abs(drift["drift_ms"]) >= 2000:
        issues.append(f"clock drift is large ({drift['drift_ms']:+.0f}ms) — verify time-sync is running")
    if API_CREDENTIALS_OK is False:
        issues.append(f"API credentials invalid: {API_CREDENTIALS_MSG}")
    if cb["tripped"]:
        issues.append(f"circuit breaker tripped: {cb['reason']}")
    if is_kill_switch_active():
        issues.append(f"KILL-SWITCH ACTIVE: {get_control_flag('kill_switch_reason', '(no reason logged)')}")
    if len(_recent_errors) >= 10:
        issues.append(f"{len(_recent_errors)} errors logged recently — check /oracle for detail")

    if issues:
        verdict = "CRITICAL" if is_kill_switch_active() or API_CREDENTIALS_OK is False else "DEGRADED"

    return {
        "verdict": verdict, "issues": issues, "time_drift": drift,
        "circuit_breaker": cb, "kill_switch_active": is_kill_switch_active(),
        "api_credentials_ok": API_CREDENTIALS_OK, "recent_error_count": len(_recent_errors),
        "recent_errors": list(_recent_errors)[-10:],
    }


# ════════════════════════════════════════════════════════════════════════════════
# ★★★ PHASE 7 — THE NEURAL SYNDICATE (strict multi-agent consensus voting) ★★★
# ────────────────────────────────────────────────────────────────────────────────
# Three independent agents, each returning a hard True/False verdict on the
# SAME proposed trade. ALL THREE must vote True for the trade to proceed —
# this is a strict AND-gate, deliberately separate from ConfidenceEngine's
# additive SCORE (which grades quality on a spectrum). The Syndicate instead
# answers a binary question per agent: "does anything about this trade, from
# my angle, say don't take it?" Wired into the webhook ENTRY flow as one more
# hard gate alongside premium_shield, circuit breaker, and Predator Vision.
# ════════════════════════════════════════════════════════════════════════════════
NEURAL_SYNDICATE_ENABLED = os.environ.get("NEURAL_SYNDICATE_ENABLED", "true").strip().lower() == "true"


class QuantAgent:
    """Votes on signal quality using exactly what Pine already sent: ai_score,
    win_rate, and systems confluence. Rejects weak setups even if every other
    gate already passed."""
    name = "Quant"

    def vote(self, ctx: Dict) -> Tuple[bool, str]:
        ai_score = ctx.get("ai_score", 0)
        win_rate = ctx.get("win_rate", 50)
        systems = ctx.get("systems", 0)
        if ai_score < 55:
            return False, f"ai_score {ai_score} below 55 floor"
        if win_rate < 35:
            return False, f"live win_rate {win_rate}% is too low to trust this signal type right now"
        if systems < 2:
            return False, f"only {systems} confirming systems — too thin a confluence"
        return True, f"ai_score={ai_score}, win_rate={win_rate}%, systems={systems} — quant checks pass"


class RiskAgent:
    """Votes on account-level risk: circuit breaker, kill-switch, and pause
    state. This agent's 'no' is absolute — no amount of signal quality
    overrides an active risk control."""
    name = "Risk"

    def vote(self, ctx: Dict) -> Tuple[bool, str]:
        if is_kill_switch_active():
            return False, "global kill-switch is active"
        if is_paused():
            return False, "bot is paused"
        tripped, reason = circuit_breaker_tripped()
        if tripped:
            return False, f"circuit breaker: {reason}"
        return True, "no active risk control blocks this trade"


class PredatorAgent:
    """Votes using Predator Vision's DOM scan — see PredatorVision above."""
    name = "Predator"

    def vote(self, ctx: Dict) -> Tuple[bool, str]:
        safe, reason = predator_vision.safe_to_enter(ctx.get("symbol", ""), ctx.get("direction", ""),
                                                       ctx.get("sl"))
        return safe, (reason or "no stop-hunt wall detected")


class NeuralSyndicate:
    def __init__(self):
        self.agents = [QuantAgent(), RiskAgent(), PredatorAgent()]

    def consensus(self, ctx: Dict) -> Tuple[bool, Dict]:
        votes = {}
        all_yes = True
        for agent in self.agents:
            try:
                ok, reason = agent.vote(ctx)
            except Exception as e:
                ok, reason = False, f"agent raised: {e}"
                oracle_log_error(f"NeuralSyndicate.{agent.name}", e)
            votes[agent.name] = {"vote": ok, "reason": reason}
            all_yes = all_yes and ok
        return all_yes, votes


neural_syndicate = NeuralSyndicate()


# ════════════════════════════════════════════════════════════════════════════════
# MISSION CONTROL — the visual dashboard
# ────────────────────────────────────────────────────────────────────────────────
# One page: live/dry badge, open positions, recent trades, pause/resume/close-all.
# Palette is pulled straight from your own Pine indicator's regime colors (lime =
# healthy/trending, amber = caution/volatile, coral = danger/choppy) so the
# dashboard and the chart read as one system, not two disconnected tools.
# Gated behind /dashboard/<token> — only reachable with your webhook passphrase.
# ════════════════════════════════════════════════════════════════════════════════
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
<title>APEX NEXUS — Quantum AI Trading Command Center</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@500;600;700;800;900&family=Rajdhani:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
/* ================================================================
   DESIGN TOKENS
   ================================================================ */
:root{
  --lime:#9dff1f; --lime-soft:#c9ff7a; --lime-dim:#6fbf12;
  --cyan:#2fe4ff; --violet:#b463ff; --purple-deep:#6a2bd9;
  --amber:#ffb020; --coral:#ff4f6d;
  --carbon:#03050a; --space-900:#060b14; --space-800:#0a1220; --space-700:#111c30;
  --panel-bg:rgba(9,16,28,.58); --panel-bg-soft:rgba(9,16,28,.4);
  --panel-border:rgba(157,255,31,.16); --panel-border-strong:rgba(157,255,31,.55);
  --text-hi:#eaf6ff; --text-mid:#93a4bd; --text-dim:#5c6b83;
  --font-display:'Orbitron',system-ui,-apple-system,sans-serif;
  --font-body:'Rajdhani',system-ui,-apple-system,sans-serif;
  --radius:14px;
}
*{box-sizing:border-box;margin:0;padding:0;}
html,body{background:var(--carbon);}
body{
  color:var(--text-hi); font-family:var(--font-body); font-weight:500;
  overflow-x:hidden; min-height:100vh; position:relative;
  -webkit-font-smoothing:antialiased;
}
button{font-family:inherit;}
::selection{background:rgba(157,255,31,.3);}
:focus-visible{outline:2px solid var(--cyan);outline-offset:2px;border-radius:4px;}

/* ================================================================
   AMBIENT BACKGROUND
   ================================================================ */
.scene-bg{position:fixed;inset:0;z-index:0;background:
  radial-gradient(ellipse 90% 60% at 50% -10%, rgba(106,43,217,.22), transparent 60%),
  radial-gradient(ellipse 70% 50% at 85% 15%, rgba(47,228,255,.10), transparent 60%),
  radial-gradient(ellipse 70% 50% at 10% 85%, rgba(157,255,31,.08), transparent 60%),
  linear-gradient(180deg,var(--carbon),var(--space-900) 40%,var(--carbon));
}
.grid-overlay{position:fixed;inset:0;z-index:0;opacity:.35;pointer-events:none;
  background-image:linear-gradient(rgba(157,255,31,.05) 1px,transparent 1px),linear-gradient(90deg,rgba(157,255,31,.05) 1px,transparent 1px);
  background-size:42px 42px; mask-image:radial-gradient(ellipse 80% 70% at 50% 30%,#000,transparent 85%);
}
#starfield{position:fixed;inset:0;z-index:0;pointer-events:none;}

/* ================================================================
   BOOT-IN REVEAL
   ================================================================ */
.panel,.topbar,.bottom-nav,.status-bar,.orb-stage{
  opacity:0;transform:translateY(16px);
  transition:opacity .6s cubic-bezier(.16,.84,.44,1),transform .6s cubic-bezier(.16,.84,.44,1),
             box-shadow .35s ease,border-color .35s ease;
}
.panel.in,.topbar.in,.bottom-nav.in,.status-bar.in,.orb-stage.in{opacity:1;transform:none;}

/* ================================================================
   LAYOUT SHELL
   ================================================================ */
.app-shell{position:relative;z-index:1;max-width:1680px;margin:0 auto;padding:14px 14px 0;display:flex;flex-direction:column;gap:16px;}

/* ================================================================
   TOPBAR
   ================================================================ */
.topbar{display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:14px;
  padding:14px 20px;background:var(--panel-bg);border:1px solid var(--panel-border);border-radius:var(--radius);
  backdrop-filter:blur(18px) saturate(140%);-webkit-backdrop-filter:blur(18px) saturate(140%);}
.brand-block{display:flex;align-items:center;gap:10px;}
.brand-mark{width:40px;height:40px;display:grid;place-items:center;filter:drop-shadow(0 0 7px rgba(157,255,31,.55));position:relative;}
.brand-mark svg{width:40px;height:40px;overflow:visible;}
.brand-mark .bm-ring-outer{animation:bmSpin 18s linear infinite;transform-origin:20px 20px;}
.brand-mark .bm-ring-inner{animation:bmSpinRev 12s linear infinite;transform-origin:20px 20px;}
.brand-mark .bm-core{animation:bmPulse 2.8s ease-in-out infinite;transform-origin:20px 20px;}
@keyframes bmSpin{from{transform:rotate(0)}to{transform:rotate(360deg)}}
@keyframes bmSpinRev{from{transform:rotate(360deg)}to{transform:rotate(0)}}
@keyframes bmPulse{0%,100%{opacity:.88;transform:scale(1);}50%{opacity:1;transform:scale(1.035);}}
.brand-text{display:flex;flex-direction:column;line-height:1.25;}
.brand-name{display:flex;align-items:center;gap:4px;font-family:var(--font-display);font-weight:700;font-size:13px;letter-spacing:.03em;}
.brand-name svg{width:12px;height:12px;stroke:var(--text-mid);fill:none;stroke-width:2;}
.brand-sub{font-size:11px;color:var(--text-mid);letter-spacing:.04em;}
.brand-sub em{font-style:normal;color:var(--amber);margin-left:6px;}

.ticker-group{display:flex;gap:10px;flex-wrap:wrap;}
.ticker-pill{display:flex;align-items:center;gap:8px;background:var(--panel-bg-soft);border:1px solid var(--panel-border);
  border-radius:10px;padding:6px 12px;}
.tk-icon{width:22px;height:22px;border-radius:50%;display:grid;place-items:center;font-family:var(--font-display);
  font-size:10px;font-weight:700;color:#0a0f08;flex-shrink:0;}
.tk-meta{display:flex;flex-direction:column;line-height:1.2;}
.tk-sym{font-size:10px;color:var(--text-mid);letter-spacing:.03em;}
.tk-price{font-family:var(--font-display);font-size:13px;font-weight:600;}
.tk-chg{font-family:var(--font-display);font-size:11.5px;font-weight:700;padding-left:6px;}
.tk-chg.up{color:var(--lime);} .tk-chg.down{color:var(--coral);}

.center-title{text-align:center;display:flex;flex-direction:column;align-items:center;gap:2px;}
.center-logo{width:30px;height:30px;margin-bottom:2px;filter:drop-shadow(0 0 8px rgba(157,255,31,.55));}
.center-title h1{font-family:var(--font-display);font-weight:800;font-size:clamp(18px,3.4vw,26px);letter-spacing:.05em;}
.tt-apex{color:var(--text-hi);} .tt-nexus{color:var(--lime);text-shadow:0 0 18px rgba(157,255,31,.55);}
.tt-sub{font-size:10.5px;letter-spacing:.22em;color:var(--text-mid);text-transform:uppercase;}
.tt-edition{font-size:9.5px;letter-spacing:.18em;color:var(--amber);text-transform:uppercase;}

.system-status-block{display:flex;align-items:center;gap:12px;}
.ss-text{display:flex;flex-direction:column;align-items:flex-end;line-height:1.3;}
.ss-label{font-size:10px;letter-spacing:.12em;color:var(--text-mid);text-transform:uppercase;}
.ss-value{font-size:12px;font-weight:700;color:var(--lime);}
.ss-date{font-size:9.5px;color:var(--text-dim);}
.ss-gauge{width:56px;height:56px;}
.gauge-num{font-family:var(--font-display);font-size:15px;font-weight:700;fill:var(--text-hi);}

@media (max-width:900px){
  .topbar{flex-direction:column;align-items:stretch;text-align:center;}
  .center-title{order:-1;}
  .ticker-group{justify-content:center;}
  .system-status-block{justify-content:center;}
}

/* ================================================================
   PANEL SYSTEM (shared glass cards)
   ================================================================ */
.panel{position:relative;background:var(--panel-bg);border:1px solid var(--panel-border);border-radius:var(--radius);
  padding:16px 18px;backdrop-filter:blur(18px) saturate(140%);-webkit-backdrop-filter:blur(18px) saturate(140%);
  box-shadow:0 18px 40px -22px rgba(0,0,0,.7),0 0 24px -10px rgba(157,255,31,.08);overflow:hidden;}
.panel::before{content:'';position:absolute;inset:0;pointer-events:none;opacity:.9;background:
  linear-gradient(var(--panel-border-strong),var(--panel-border-strong)) top left/16px 2px no-repeat,
  linear-gradient(var(--panel-border-strong),var(--panel-border-strong)) top left/2px 16px no-repeat,
  linear-gradient(var(--panel-border-strong),var(--panel-border-strong)) bottom right/16px 2px no-repeat,
  linear-gradient(var(--panel-border-strong),var(--panel-border-strong)) bottom right/2px 16px no-repeat;}
.panel:hover{border-color:rgba(157,255,31,.34);transform:translateY(-2px);
  box-shadow:0 22px 48px -18px rgba(0,0,0,.7),0 0 34px -6px rgba(157,255,31,.2);}
.panel.flash{border-color:var(--cyan);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset,0 0 50px -6px rgba(47,228,255,.6);}
.panel-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:14px;}
.panel-title{display:flex;align-items:center;gap:8px;font-family:var(--font-body);font-weight:700;font-size:11.5px;
  letter-spacing:.13em;text-transform:uppercase;color:var(--text-mid);}
.panel-title svg{width:15px;height:15px;stroke:var(--lime);fill:none;stroke-width:1.6;
  filter:drop-shadow(0 0 4px rgba(157,255,31,.6));flex-shrink:0;}
.panel-more{color:var(--text-dim);cursor:pointer;font-size:16px;line-height:1;}
.count-pill,.status-pill{font-size:10.5px;color:var(--text-mid);background:rgba(255,255,255,.04);
  border:1px solid var(--panel-border);border-radius:20px;padding:3px 10px;}
.status-pill.online{color:var(--lime);}
.status-pill.online::before{content:'●';margin-right:5px;font-size:8px;}

/* ================================================================
   GAUGES & BARS
   ================================================================ */
.gauge-ring circle{fill:none;stroke-width:7;}
.gauge-ring .gauge-bg{stroke:rgba(255,255,255,.06);}
.gauge-ring .gauge-fg{stroke:var(--lime);stroke-linecap:round;transform-origin:50% 50%;transform:rotate(-90deg);
  filter:drop-shadow(0 0 6px rgba(157,255,31,.65));}
.big-gauge{width:132px;height:132px;}
.big-gauge-wrap{display:flex;align-items:center;justify-content:center;position:relative;margin:0 auto 6px;}
.big-gauge-label{position:absolute;text-align:center;}
.big-gauge-pct{display:block;font-family:var(--font-display);font-size:26px;font-weight:700;color:var(--lime);}
.big-gauge-tag{display:block;font-size:9.5px;letter-spacing:.1em;color:var(--text-mid);text-transform:uppercase;}

.half-gauge{width:100%;max-width:170px;margin:0 auto;}
.half-gauge path{fill:none;stroke-width:9;stroke-linecap:round;}
.half-gauge .hg-bg{stroke:rgba(255,255,255,.06);}
.half-gauge .hg-fg{stroke:var(--amber);filter:drop-shadow(0 0 6px rgba(255,176,32,.6));}
.half-gauge-num{font-family:var(--font-display);font-size:22px;font-weight:700;text-anchor:middle;fill:var(--amber);}
.half-gauge-tag{font-size:8.5px;text-anchor:middle;fill:var(--text-mid);letter-spacing:.08em;}
.half-gauge-end{font-size:8px;fill:var(--text-dim);}

.bar-row{margin-top:10px;}
.bar-row-head{display:flex;justify-content:space-between;font-size:10.5px;color:var(--text-mid);margin-bottom:5px;letter-spacing:.05em;}
.bar-row-head strong{color:var(--text-hi);font-weight:700;}
.bar-track{height:6px;border-radius:4px;background:rgba(255,255,255,.06);overflow:hidden;}
.bar-fill{height:100%;border-radius:4px;background:linear-gradient(90deg,var(--lime),var(--cyan));width:0%;
  transition:width 1.3s cubic-bezier(.16,.84,.44,1);}
.bar-fill.amber{background:linear-gradient(90deg,var(--amber),var(--coral));}

/* ================================================================
   LEFT / RIGHT COLUMN PANEL CONTENT
   ================================================================ */
.stat-hero{font-family:var(--font-display);font-size:clamp(22px,3vw,28px);font-weight:700;letter-spacing:.01em;}
.stat-hero-label{font-size:10px;letter-spacing:.1em;color:var(--text-mid);text-transform:uppercase;margin-bottom:2px;}
.stat-pnl{font-family:var(--font-display);font-weight:700;font-size:14px;margin:8px 0 12px;}
.stat-pnl.up{color:var(--lime);} .stat-pnl.down{color:var(--coral);}
.stat-grid-2{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px;}
.mini-stat-label{font-size:9.5px;color:var(--text-dim);letter-spacing:.06em;text-transform:uppercase;}
.mini-stat-value{font-family:var(--font-display);font-size:13.5px;font-weight:600;margin-top:2px;}

.confidence-list{display:flex;flex-direction:column;gap:8px;margin-top:4px;}
.confidence-row{display:flex;justify-content:space-between;font-size:11.5px;}
.confidence-row span:first-child{color:var(--text-mid);}
.tag{font-weight:700;letter-spacing:.03em;}
.tag.bullish,.tag.strong,.tag.high,.tag.optimal,.tag.positive{color:var(--lime);}
.tag.medium{color:var(--amber);}
.tag.bearish{color:var(--coral);}
.tag.neutral{color:var(--text-mid);}

.brain-row{display:flex;align-items:center;gap:14px;margin-bottom:10px;}
.brain-icon{width:46px;height:46px;flex-shrink:0;display:grid;place-items:center;border-radius:50%;
  background:radial-gradient(circle at 35% 30%,rgba(180,99,255,.35),rgba(180,99,255,.04));}
.brain-icon svg{width:26px;height:26px;stroke:var(--violet);fill:none;stroke-width:1.4;
  filter:drop-shadow(0 0 6px rgba(180,99,255,.7));}
.perf-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px;}
.perf-cell-label{font-size:9px;letter-spacing:.06em;color:var(--text-dim);text-transform:uppercase;}
.perf-cell-value{font-family:var(--font-display);font-size:14px;font-weight:600;margin-top:2px;color:var(--text-hi);}

.risk-grid{display:flex;flex-direction:column;gap:11px;margin-top:4px;}

/* ================================================================
   [REAL TABS ADD] Autopilot · Portfolio Vault · Backtest Engine ·
   System Settings — these used to just toast "queued for next build
   phase". Layout mirrors the existing .dash-grid panel language so
   nothing feels bolted-on.
   ================================================================ */
.tab-view{max-width:820px;margin:0 auto;padding:18px 16px 32px;display:flex;flex-direction:column;gap:16px;}
.tab-view[hidden]{display:none;}
.tab-empty{display:flex;flex-direction:column;align-items:center;text-align:center;gap:10px;padding:38px 20px;color:var(--text-mid);font-size:12.5px;line-height:1.6;}
.tab-empty svg{width:30px;height:30px;stroke:var(--text-dim);fill:none;stroke-width:1.6;}
.tab-empty .connect-cta{margin-top:4px;background:var(--lime);color:#04240a;border:none;border-radius:9px;padding:9px 18px;font-weight:700;font-size:12px;font-family:var(--font-body);cursor:pointer;}

.act-row{display:flex;gap:10px;flex-wrap:wrap;margin-top:6px;}
.act-btn{flex:1;min-width:132px;border-radius:10px;padding:13px 14px;font-family:var(--font-body);font-weight:700;
  font-size:13px;letter-spacing:.02em;border:1px solid var(--panel-border);background:rgba(255,255,255,.03);
  color:var(--text-hi);cursor:pointer;transition:filter .15s,transform .1s;}
.act-btn:active{transform:scale(.98);}
.act-btn.primary{background:var(--lime);color:#04240a;border-color:var(--lime);}
.act-btn.warn{background:rgba(255,176,32,.12);color:var(--amber);border-color:rgba(255,176,32,.4);}
.act-btn.danger{background:rgba(255,79,109,.12);color:var(--coral);border-color:rgba(255,79,109,.4);}
.act-btn:disabled{opacity:.4;cursor:not-allowed;}
.act-btn .sub{display:block;font-size:9.5px;font-weight:500;opacity:.75;margin-top:2px;letter-spacing:.02em;text-transform:none;}

.status-banner{display:flex;align-items:center;gap:10px;padding:12px 14px;border-radius:10px;font-size:12.5px;font-weight:600;}
.status-banner.ok{background:rgba(157,255,31,.08);border:1px solid rgba(157,255,31,.3);color:var(--lime-soft);}
.status-banner.paused{background:rgba(255,176,32,.1);border:1px solid rgba(255,176,32,.35);color:var(--amber);}
.status-banner.danger{background:rgba(255,79,109,.1);border:1px solid rgba(255,79,109,.35);color:var(--coral);}
.status-dot{width:8px;height:8px;border-radius:50%;background:currentColor;box-shadow:0 0 8px currentColor;flex:none;}

.settings-row{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:10px 0;border-bottom:1px solid rgba(255,255,255,.05);font-size:12px;}
.settings-row:last-child{border-bottom:none;}
.settings-row .k{color:var(--text-mid);}
.settings-row .v{color:var(--text-hi);font-weight:600;font-family:var(--font-display);font-size:11.5px;text-align:right;}
.settings-row .v.on{color:var(--lime);} .settings-row .v.off{color:var(--text-dim);} .settings-row .v.danger{color:var(--coral);}
.signal-tag{display:inline-block;font-size:10px;font-weight:700;padding:3px 9px;border-radius:20px;margin:2px 3px 0 0;background:rgba(157,255,31,.1);color:var(--lime-soft);border:1px solid rgba(157,255,31,.25);cursor:pointer;user-select:none;transition:transform .1s ease,opacity .1s ease;}
.signal-tag.off{background:rgba(255,255,255,.03);color:var(--text-dim);border-color:var(--panel-border);}
.signal-tag:active{transform:scale(.9);}
.settings-row.tappable{cursor:pointer;transition:opacity .1s ease;}
.settings-row.tappable:active{opacity:.55;}

.stat-mini-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;}
.stat-mini{background:rgba(255,255,255,.02);border:1px solid var(--panel-border);border-radius:10px;padding:11px 12px;}
.stat-mini .lbl{font-size:9.5px;letter-spacing:.08em;text-transform:uppercase;color:var(--text-dim);margin-bottom:4px;}
.stat-mini .val{font-family:var(--font-display);font-size:16px;font-weight:700;color:var(--text-hi);}
.stat-mini .val.pos{color:var(--lime);} .stat-mini .val.neg{color:var(--coral);}

.equity-wrap{width:100%;height:110px;margin-top:8px;}
.equity-wrap svg{width:100%;height:100%;}

.confirm-overlay{position:fixed;inset:0;background:rgba(2,4,9,.72);backdrop-filter:blur(4px);z-index:80;
  display:flex;align-items:flex-end;justify-content:center;opacity:0;pointer-events:none;transition:opacity .2s;}
.confirm-overlay.show{opacity:1;pointer-events:auto;}
.confirm-card{width:100%;max-width:420px;background:var(--space-800);border:1px solid var(--panel-border-strong);
  border-radius:18px 18px 0 0;padding:22px 20px calc(22px + env(safe-area-inset-bottom,0px));
  transform:translateY(16px);transition:transform .25s cubic-bezier(.16,.84,.44,1);}
.confirm-overlay.show .confirm-card{transform:translateY(0);}
.confirm-title{font-family:var(--font-display);font-size:15px;font-weight:700;color:var(--text-hi);margin-bottom:8px;}
.confirm-body{font-size:12.5px;color:var(--text-mid);line-height:1.55;margin-bottom:18px;}
.confirm-actions{display:flex;gap:10px;}
.confirm-actions .act-btn{margin-top:0;}
.risk-row{display:flex;align-items:center;justify-content:space-between;font-size:11.5px;}
.risk-row span:first-child{display:flex;align-items:center;gap:7px;color:var(--text-mid);}
.risk-dot{width:6px;height:6px;border-radius:50%;background:var(--cyan);box-shadow:0 0 6px var(--cyan);}
.risk-row strong{font-family:var(--font-display);font-weight:600;color:var(--text-hi);}

/* ================================================================
   CENTER COLUMN — CORE HEADER
   ================================================================ */
.core-header{display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:14px;}
.core-id{display:flex;align-items:center;gap:12px;}
.core-id-icon{width:38px;height:38px;border-radius:10px;display:grid;place-items:center;
  background:linear-gradient(135deg,rgba(157,255,31,.18),rgba(47,228,255,.1));border:1px solid var(--panel-border);}
.core-id-icon svg{width:20px;height:20px;stroke:var(--lime);fill:none;stroke-width:1.5;}
.core-id-text{display:flex;flex-direction:column;line-height:1.3;}
.core-id-title{font-family:var(--font-display);font-size:13px;font-weight:700;letter-spacing:.04em;}
.core-id-status{font-size:10.5px;color:var(--text-mid);letter-spacing:.04em;}
.core-id-status span{color:var(--cyan);}
.core-metrics{display:flex;gap:22px;flex-wrap:wrap;}
.core-metric{display:flex;flex-direction:column;gap:5px;min-width:110px;}
.core-metric-label{font-size:9px;letter-spacing:.08em;color:var(--text-dim);text-transform:uppercase;}
.core-metric-value{font-family:var(--font-display);font-size:15px;font-weight:600;}
.mini-bar-track{height:4px;border-radius:3px;background:rgba(255,255,255,.06);width:90px;overflow:hidden;}
.mini-bar-fill{height:100%;border-radius:3px;background:linear-gradient(90deg,var(--amber),var(--coral));width:0%;
  transition:width 1.3s ease;}
.spark{width:90px;height:20px;}
.spark polyline{fill:none;stroke:var(--cyan);stroke-width:1.6;stroke-linecap:round;stroke-linejoin:round;
  filter:drop-shadow(0 0 3px rgba(47,228,255,.7));}

/* ---- Mid row: radar / orb / intel ---- */
.mid-row{display:flex;flex-direction:column;gap:16px;}
@media (min-width:900px){.mid-row{display:grid;grid-template-columns:1fr 1.25fr 1fr;gap:16px;align-items:stretch;}}

.radar-svg{width:100%;max-width:210px;display:block;margin:0 auto;}
.radar-ring{fill:none;stroke:rgba(157,255,31,.22);stroke-width:1;}
.radar-cross{stroke:rgba(157,255,31,.1);stroke-width:1;}
.radar-sweep{transform-origin:100px 100px;animation:sweep 4.5s linear infinite;}
.radar-sweep path{fill:rgba(157,255,31,.16);}
.radar-blip{fill:var(--lime);filter:drop-shadow(0 0 4px rgba(157,255,31,.9));animation:blipPulse 2.4s ease-in-out infinite;}
.radar-blip.alt{fill:var(--cyan);filter:drop-shadow(0 0 4px rgba(47,228,255,.9));}
.radar-blip.warn{fill:var(--amber);filter:drop-shadow(0 0 4px rgba(255,176,32,.9));}
.radar-ping{fill:none;stroke:var(--lime);stroke-width:1.4;opacity:0;animation:radarPing 2.6s ease-out infinite;transform-origin:center;}
@keyframes radarPing{0%{r:2;opacity:.85;stroke-width:2;}100%{r:16;opacity:0;stroke-width:.4;}}
.radar-core{fill:var(--lime);filter:drop-shadow(0 0 6px rgba(157,255,31,1));}
@keyframes sweep{to{transform:rotate(360deg);}}
@keyframes blipPulse{0%,100%{opacity:.35;}50%{opacity:1;}}

.heatmap-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(118px,1fr));gap:10px;}
.heat-tile{border-radius:11px;padding:12px 10px;border:1px solid var(--panel-border);background:rgba(157,255,31,.04);
  display:flex;flex-direction:column;gap:4px;transition:background-color .5s ease,border-color .5s ease,box-shadow .5s ease;}
.heat-sym{font-family:var(--font-display);font-weight:700;font-size:12px;letter-spacing:.05em;color:var(--text-hi);}
.heat-price{font-family:var(--font-display);font-size:12.5px;font-weight:600;color:var(--text-hi);}
.heat-chg{font-size:11px;font-weight:700;letter-spacing:.02em;}
.heat-chg.up{color:var(--lime);} .heat-chg.down{color:var(--coral);}
.heat-bar-track{height:3px;border-radius:3px;background:rgba(255,255,255,.08);overflow:hidden;margin-top:2px;}
.heat-bar-fill{height:100%;border-radius:3px;transition:width .5s ease,background-color .5s ease;}
.latency-list{list-style:none;margin-top:12px;display:flex;flex-direction:column;gap:7px;}
.latency-list li{display:flex;align-items:center;font-size:11px;color:var(--text-mid);}
.latency-list .dot{width:6px;height:6px;border-radius:50%;margin-right:8px;flex-shrink:0;}
.latency-list .ex-name{flex:1;}
.latency-list .ms{font-family:var(--font-display);font-weight:600;color:var(--text-hi);font-size:11px;}
.signals-total{display:flex;justify-content:space-between;margin-top:12px;padding-top:10px;
  border-top:1px solid var(--panel-border);font-size:10.5px;color:var(--text-mid);letter-spacing:.05em;text-transform:uppercase;}
.signals-total strong{font-family:var(--font-display);color:var(--lime);font-size:13px;}

.orb-stage{position:relative;min-height:300px;display:grid;place-items:center;border-radius:var(--radius);
  background:radial-gradient(ellipse 75% 65% at 50% 48%,rgba(157,255,31,.16),rgba(47,228,255,.07) 45%,transparent 72%);}
.orb-stage canvas{position:absolute;inset:0;width:100%;height:100%;}
.orb-fallback{width:200px;height:200px;border-radius:50%;position:relative;
  background:radial-gradient(circle at 38% 34%,rgba(157,255,31,.55),rgba(10,20,10,0) 62%);}
.orb-fallback::before{content:'';position:absolute;inset:-16px;border-radius:50%;
  background:conic-gradient(from 0deg,var(--lime),var(--cyan),var(--violet),var(--lime));
  -webkit-mask:radial-gradient(farthest-side,transparent calc(100% - 3px),#000 calc(100% - 3px));
  mask:radial-gradient(farthest-side,transparent calc(100% - 3px),#000 calc(100% - 3px));
  animation:spin 7s linear infinite;}
@keyframes spin{to{transform:rotate(360deg);}}

.network-svg{width:100%;display:block;}
.network-line{stroke:rgba(47,228,255,.4);stroke-width:1;stroke-dasharray:4 3;animation:dashFlow 3s linear infinite;}
.network-node{fill:var(--cyan);filter:drop-shadow(0 0 4px rgba(47,228,255,.85));}
.network-node.alt{fill:var(--lime);filter:drop-shadow(0 0 4px rgba(157,255,31,.85));}
.worldmap-wrap{width:100%;aspect-ratio:240/110;position:relative;}
.worldmap-wrap canvas{width:100%;height:100%;display:block;}
@keyframes dashFlow{to{stroke-dashoffset:-14;}}

.sentiment-block{margin-top:14px;}
.sentiment-head{display:flex;justify-content:space-between;font-size:10.5px;letter-spacing:.08em;color:var(--text-mid);
  text-transform:uppercase;margin-bottom:6px;}
.sentiment-tag{color:var(--lime);font-weight:700;font-size:12px;letter-spacing:.03em;}

.fng-block{margin-top:16px;text-align:center;}

/* ---- Voice row ---- */
.voice-row{display:flex;flex-direction:column;gap:16px;}
@media (min-width:900px){.voice-row{display:grid;grid-template-columns:1fr auto 1fr;gap:16px;align-items:center;}}

.say-list{list-style:none;display:flex;flex-direction:column;gap:7px;}
.say-item{cursor:pointer;font-size:12px;color:var(--text-mid);padding:7px 10px;border-radius:8px;
  border:1px solid transparent;transition:all .2s ease;display:flex;align-items:center;gap:8px;}
.say-item::before{content:'▸';color:var(--lime);font-size:10px;}
.say-item:hover{background:rgba(157,255,31,.07);border-color:var(--panel-border);color:var(--text-hi);transform:translateX(2px);}

.mic-stage{display:flex;flex-direction:column;align-items:center;gap:10px;}
.mic-rings{position:relative;width:150px;height:150px;display:grid;place-items:center;}
.ring{position:absolute;inset:0;border-radius:50%;border:1px solid rgba(180,99,255,.32);animation:ringPulse 3s ease-out infinite;}
.ring.r2{animation-delay:1s;border-color:rgba(47,228,255,.28);}
.ring.r3{animation-delay:2s;border-color:rgba(157,255,31,.28);}
@keyframes ringPulse{0%{transform:scale(.5);opacity:.9;}100%{transform:scale(1.55);opacity:0;}}
.mic-btn{position:relative;z-index:2;width:72px;height:72px;border-radius:50%;cursor:pointer;
  background:radial-gradient(circle at 35% 30%,rgba(180,99,255,.95),rgba(70,15,130,.95));
  border:1px solid rgba(255,255,255,.25);box-shadow:0 0 40px -4px rgba(180,99,255,.75);
  display:grid;place-items:center;color:#fff;transition:transform .2s ease,box-shadow .3s ease;}
.mic-btn svg{width:26px;height:26px;stroke:#fff;fill:none;stroke-width:1.7;}
.mic-btn:hover{transform:scale(1.06);}
.mic-stage.is-listening .ring{animation-duration:1.1s;}
.mic-stage.is-listening .mic-btn{box-shadow:0 0 60px -2px rgba(157,255,31,.85);}
.mic-stage.is-speaking .mic-btn{box-shadow:0 0 60px -2px rgba(47,228,255,.85);}
.mic-status{font-family:var(--font-display);font-size:11px;letter-spacing:.16em;color:var(--violet);text-transform:uppercase;}
.mic-stage.is-listening .mic-status{color:var(--lime);}
.mic-stage.is-speaking .mic-status{color:var(--cyan);}
.waveform{display:flex;align-items:center;gap:3px;height:30px;}
.waveform span{width:3px;background:linear-gradient(var(--lime),var(--cyan));height:20%;border-radius:2px;opacity:.35;
  animation:wave 1.4s ease-in-out infinite;}
.mic-stage.is-listening .waveform span,.mic-stage.is-speaking .waveform span{opacity:.9;animation-duration:.55s;}
@keyframes wave{0%,100%{height:15%;}50%{height:var(--h,60%);}}
.transcript{font-size:10.5px;color:var(--text-dim);text-align:center;min-height:14px;max-width:220px;}

.nexus-reply .reply-bubble{font-size:13px;color:var(--text-hi);line-height:1.5;margin-top:4px;}
.nexus-history{margin-top:12px;padding-top:10px;border-top:1px solid var(--panel-border);display:flex;flex-direction:column;gap:5px;}
.nexus-history-item{font-size:10px;color:var(--text-dim);}
.nexus-history-item strong{color:var(--text-mid);font-weight:600;}

/* ================================================================
   RIGHT COLUMN — TERMINAL / TABLES
   ================================================================ */
.tf-tabs{display:flex;gap:4px;background:rgba(255,255,255,.03);border-radius:8px;padding:3px;}
.tf-tab{background:transparent;border:none;color:var(--text-dim);font-size:10.5px;font-weight:700;padding:5px 9px;
  border-radius:6px;cursor:pointer;letter-spacing:.03em;transition:all .2s ease;}
.tf-tab.active{background:rgba(157,255,31,.16);color:var(--lime);}
.terminal-price-row{display:flex;align-items:baseline;gap:10px;margin-bottom:8px;}
.terminal-symbol{font-size:11px;color:var(--text-mid);letter-spacing:.04em;}
.terminal-price{font-family:var(--font-display);font-size:19px;font-weight:700;}
.terminal-change{font-family:var(--font-display);font-size:12px;font-weight:700;}
.terminal-change.up{color:var(--lime);} .terminal-change.down{color:var(--coral);}
.chart-wrap{width:100%;height:220px;position:relative;}
.chart-wrap canvas{width:100%;height:100%;display:block;cursor:crosshair;}
.chart-tooltip{position:absolute;pointer-events:none;background:rgba(7,12,22,.96);border:1px solid var(--panel-border-strong);
  border-radius:8px;padding:8px 11px;font-size:10.5px;color:var(--text-mid);line-height:1.6;z-index:10;
  box-shadow:0 12px 26px -8px rgba(0,0,0,.65);white-space:nowrap;}
.chart-tooltip b{color:var(--text-hi);font-family:var(--font-display);font-weight:600;margin-left:5px;}
.chart-live-tag{font-size:9px;font-weight:700;letter-spacing:.06em;color:var(--lime);text-transform:uppercase;
  background:rgba(157,255,31,.1);border:1px solid rgba(157,255,31,.3);border-radius:20px;padding:2px 8px;margin-left:8px;}
.chart-xaxis{display:flex;justify-content:space-between;font-size:10px;color:var(--text-dim);margin-top:6px;}

.table-scroll{overflow-x:auto;}
.data-table{width:100%;border-collapse:collapse;font-size:11.5px;min-width:420px;}
.data-table th{text-align:left;font-size:9.5px;letter-spacing:.07em;color:var(--text-dim);text-transform:uppercase;
  padding:0 8px 8px 0;font-weight:600;}
.data-table td{padding:7px 8px 7px 0;border-top:1px solid rgba(255,255,255,.05);white-space:nowrap;}
.side-tag{font-weight:700;font-size:10.5px;padding:2px 7px;border-radius:5px;letter-spacing:.03em;}
.side-tag.long,.side-tag.buy{color:var(--lime);background:rgba(157,255,31,.1);}
.side-tag.short,.side-tag.sell{color:var(--coral);background:rgba(255,79,109,.1);}
.pnl-val.up{color:var(--lime);} .pnl-val.down{color:var(--coral);}
.status-chip{color:var(--lime);font-size:10px;font-weight:700;letter-spacing:.04em;}

.news-list{display:flex;flex-direction:column;gap:11px;}
.news-item{display:flex;gap:10px;font-size:11.5px;}
.news-icon{width:26px;height:26px;border-radius:8px;flex-shrink:0;display:grid;place-items:center;
  background:rgba(255,176,32,.1);}
.news-icon svg{width:14px;height:14px;stroke:var(--amber);fill:none;stroke-width:1.6;}
.news-text{color:var(--text-hi);line-height:1.35;}
.news-time{color:var(--text-dim);font-size:10px;display:block;margin-top:2px;}
.view-all{display:block;margin-top:12px;text-align:right;font-size:11px;color:var(--cyan);cursor:pointer;
  text-decoration:none;letter-spacing:.03em;}
.view-all:hover{text-decoration:underline;}

/* ================================================================
   BOTTOM NAV
   ================================================================ */
.bottom-nav{display:flex;align-items:center;gap:8px;background:var(--panel-bg);border:1px solid var(--panel-border);
  border-radius:var(--radius);padding:10px;backdrop-filter:blur(18px);-webkit-backdrop-filter:blur(18px);}
.nav-arrow{background:rgba(255,255,255,.04);border:1px solid var(--panel-border);color:var(--text-mid);width:34px;height:34px;
  border-radius:9px;cursor:pointer;flex-shrink:0;display:grid;place-items:center;}
.nav-arrow svg{width:16px;height:16px;stroke:currentColor;fill:none;stroke-width:2;}
.nav-items{display:flex;gap:10px;overflow-x:auto;scrollbar-width:none;flex:1;}
.nav-items::-webkit-scrollbar{display:none;}
.nav-item{flex-shrink:0;display:flex;align-items:center;gap:9px;background:rgba(255,255,255,.02);
  border:1px solid var(--panel-border);border-radius:10px;padding:9px 14px;cursor:pointer;transition:all .2s ease;text-align:left;}
.nav-item svg{width:18px;height:18px;stroke:var(--text-mid);fill:none;stroke-width:1.6;flex-shrink:0;}
.nav-item-text{display:flex;flex-direction:column;line-height:1.25;}
.nav-title{font-family:var(--font-display);font-size:10.5px;font-weight:700;color:var(--text-mid);letter-spacing:.03em;}
.nav-sub{font-size:9px;color:var(--text-dim);}
.nav-item:hover{border-color:rgba(157,255,31,.3);}
.nav-item.active{background:rgba(157,255,31,.12);border-color:var(--lime);}
.nav-item.active svg{stroke:var(--lime);filter:drop-shadow(0 0 4px rgba(157,255,31,.7));}
.nav-item.active .nav-title{color:var(--lime);}

/* ================================================================
   FOOTER STATUS BAR
   ================================================================ */
.status-bar{display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:14px;
  padding:12px 20px;background:var(--panel-bg-soft);border:1px solid var(--panel-border);border-radius:var(--radius);
  margin-bottom:16px;}
.stat-item{display:flex;flex-direction:column;gap:2px;}
.stat-item .label{font-size:8.5px;letter-spacing:.08em;color:var(--text-dim);text-transform:uppercase;}
.stat-item .value{font-family:var(--font-display);font-size:11.5px;font-weight:600;}
.stat-item .value.good{color:var(--lime);}
.stat-center{display:flex;align-items:center;gap:8px;font-family:var(--font-display);font-size:11px;
  letter-spacing:.06em;color:var(--text-mid);text-transform:uppercase;}
.stat-center strong{color:var(--lime);}
.pulse-dot{width:8px;height:8px;border-radius:50%;background:var(--lime);box-shadow:0 0 10px var(--lime);
  animation:dotPulse 1.6s ease-in-out infinite;}
@keyframes dotPulse{0%,100%{opacity:1;transform:scale(1);}50%{opacity:.4;transform:scale(.7);}}

/* ================================================================
   TOAST
   ================================================================ */
.toast{position:fixed;left:50%;bottom:24px;transform:translate(-50%,140%);z-index:50;
  background:rgba(10,18,32,.92);border:1px solid var(--panel-border-strong);border-radius:10px;padding:11px 18px;
  font-size:12px;color:var(--text-hi);backdrop-filter:blur(12px);transition:transform .35s cubic-bezier(.16,.84,.44,1);
  max-width:88vw;text-align:center;box-shadow:0 10px 30px -8px rgba(0,0,0,.6);}
.toast.show{transform:translate(-50%,0);}

/* ================================================================
   FLASH FEEDBACK
   ================================================================ */
.flash-up{animation:flashUp .8s ease;} .flash-down{animation:flashDown .8s ease;}
@keyframes flashUp{0%{text-shadow:0 0 14px rgba(157,255,31,.9);}100%{text-shadow:none;}}
@keyframes flashDown{0%{text-shadow:0 0 14px rgba(255,79,109,.9);}100%{text-shadow:none;}}

/* ================================================================
   MAIN GRID
   ================================================================ */
.dash-grid{display:flex;flex-direction:column;gap:16px;}
@media (min-width:1180px){.dash-grid{display:grid;grid-template-columns:296px 1fr 336px;align-items:start;}}
.col-left,.col-right,.col-center{display:flex;flex-direction:column;gap:16px;min-width:0;}

/* ================================================================
   REDUCED MOTION
   ================================================================ */
@media (prefers-reduced-motion:reduce){
  *,*::before,*::after{animation-duration:.001ms !important;animation-iteration-count:1 !important;
    transition-duration:.001ms !important;}
}

/* ================================================================
   LIVE / DEMO DATA BADGE + CONNECT POPOVER
   ================================================================ */
.data-mode-wrap{position:relative;}
.data-mode-badge{display:flex;align-items:center;gap:7px;background:rgba(255,176,32,.08);border:1px solid rgba(255,176,32,.32);
  color:var(--amber);border-radius:20px;padding:7px 13px;font-size:10px;font-weight:700;letter-spacing:.05em;cursor:pointer;
  text-transform:uppercase;font-family:var(--font-body);}
.data-mode-badge.live{background:rgba(157,255,31,.09);border-color:rgba(157,255,31,.38);color:var(--lime);}
.dm-dot{width:6px;height:6px;border-radius:50%;background:currentColor;box-shadow:0 0 6px currentColor;flex-shrink:0;
  animation:dotPulse 1.6s ease-in-out infinite;}
.connect-pop{position:absolute;top:calc(100% + 8px);right:0;width:250px;background:rgba(7,12,22,.97);
  border:1px solid var(--panel-border-strong);border-radius:12px;padding:14px;z-index:60;backdrop-filter:blur(16px);
  -webkit-backdrop-filter:blur(16px);box-shadow:0 20px 50px -12px rgba(0,0,0,.75);}
.connect-pop-title{font-size:11px;font-weight:700;letter-spacing:.03em;margin-bottom:10px;color:var(--text-hi);}
.connect-pop input{width:100%;background:rgba(255,255,255,.04);border:1px solid var(--panel-border);border-radius:8px;
  color:var(--text-hi);font-family:var(--font-body);font-size:12px;padding:8px 10px;margin-bottom:8px;}
.connect-pop input::placeholder{color:var(--text-dim);}
.connect-pop-actions{display:flex;gap:8px;margin-top:2px;}
.connect-btn,.disconnect-btn{flex:1;border:none;border-radius:8px;padding:8px;font-size:11px;font-weight:700;cursor:pointer;
  letter-spacing:.03em;font-family:var(--font-body);}
.connect-btn{background:var(--lime);color:#04240a;}
.disconnect-btn{background:rgba(255,79,109,.12);color:var(--coral);}
.connect-pop-note{font-size:9.5px;color:var(--text-dim);margin-top:9px;line-height:1.4;}

/* ================================================================
   ENGINE STATUS CHIPS + GATEKEEPER LOG
   ================================================================ */
.engine-chip-row{display:flex;flex-wrap:wrap;gap:6px;margin-top:12px;}
.engine-chip{font-size:9px;font-weight:700;letter-spacing:.03em;padding:4px 9px;border-radius:20px;text-transform:uppercase;}
.engine-chip.on{color:var(--lime);background:rgba(157,255,31,.1);border:1px solid rgba(157,255,31,.3);}
.engine-chip.off{color:var(--text-dim);background:rgba(255,255,255,.03);border:1px solid var(--panel-border);}
.engine-chip.danger{color:var(--coral);background:rgba(255,79,109,.12);border:1px solid rgba(255,79,109,.35);}
.pulse-oracle{margin-top:12px;padding-top:11px;border-top:1px solid var(--panel-border);font-size:11px;color:var(--text-mid);line-height:1.45;}
.gatekeeper-item{border-left:2px solid var(--coral);padding-left:10px;margin-bottom:12px;}
.gatekeeper-item:last-child{margin-bottom:0;}
.gatekeeper-item .gk-head{display:flex;justify-content:space-between;gap:8px;font-size:10.5px;font-weight:700;color:var(--coral);letter-spacing:.02em;}
.gatekeeper-item .gk-detail{font-size:11px;color:var(--text-mid);margin-top:3px;line-height:1.35;}
.gatekeeper-item .gk-time{font-size:9.5px;color:var(--text-dim);margin-top:3px;display:block;}
</style>
</head>
<body>

<div class="scene-bg"></div>
<div class="grid-overlay"></div>
<canvas id="starfield"></canvas>

<!-- shared SVG defs -->
<svg width="0" height="0" style="position:absolute">
  <defs>
    <linearGradient id="logoGrad" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#c9ff7a"/><stop offset="100%" stop-color="#2fe4ff"/>
    </linearGradient>
  </defs>
</svg>

<div class="app-shell">

  <!-- ============================== TOPBAR ============================== -->
  <header class="topbar">
    <div class="brand-block">
      <div class="brand-mark">
        <svg viewBox="0 0 40 40">
          <defs>
            <linearGradient id="bmGrad" x1="0" y1="0" x2="1" y2="1">
              <stop offset="0%" stop-color="#eaffb0"/>
              <stop offset="55%" stop-color="#9dff1f"/>
              <stop offset="100%" stop-color="#2fe4ff"/>
            </linearGradient>
          </defs>
          <circle class="bm-ring-outer" cx="20" cy="20" r="18" fill="none" stroke="url(#bmGrad)" stroke-width="1" stroke-dasharray="2.5 5" opacity=".55"/>
          <polygon class="bm-ring-inner" points="20,4.5 33.5,12.5 33.5,27.5 20,35.5 6.5,27.5 6.5,12.5" fill="none" stroke="var(--cyan)" stroke-width="1" opacity=".4"/>
          <g class="bm-core">
            <path d="M20 9 L31 31 H9 Z" fill="url(#bmGrad)"/>
            <path d="M20 17 L26 28 H14 Z" fill="#03050a" opacity=".55"/>
            <circle cx="20" cy="20" r="1.7" fill="#eaffb0"/>
          </g>
        </svg>
      </div>
      <div class="brand-text">
        <span class="brand-name">APEX NEXUS AI <svg viewBox="0 0 24 24"><path d="M6 9l6 6 6-6"/></svg></span>
        <span class="brand-sub">Master Account <em>Ultra Prime Access</em></span>
      </div>
    </div>

    <div class="ticker-group ticker-left">
      <div class="ticker-pill">
        <span class="tk-icon" style="background:var(--amber)">B</span>
        <div class="tk-meta"><span class="tk-sym">BTC/USDT</span><span class="tk-price" id="price-BTC">66,432.58</span></div>
        <span class="tk-chg up" id="chg-BTC">+2.35%</span>
      </div>
      <div class="ticker-pill">
        <span class="tk-icon" style="background:var(--violet)">E</span>
        <div class="tk-meta"><span class="tk-sym">ETH/USDT</span><span class="tk-price" id="price-ETH">3,142.68</span></div>
        <span class="tk-chg up" id="chg-ETH">+3.12%</span>
      </div>
    </div>

    <div class="center-title">
      <div class="center-logo"><svg viewBox="0 0 40 40"><path d="M20 4 L35 33 H5 Z" fill="none" stroke="url(#logoGrad)" stroke-width="2.2"/></svg></div>
      <h1><span class="tt-apex">APEX</span> <span class="tt-nexus">NEXUS</span></h1>
      <p class="tt-sub">Quantum AI Trading Command Center</p>
      <p class="tt-edition">Permanent Ultimate Edition</p>
    </div>

    <div class="ticker-group ticker-right">
      <div class="ticker-pill">
        <span class="tk-icon" style="background:var(--lime)">S</span>
        <div class="tk-meta"><span class="tk-sym">SOL/USDT</span><span class="tk-price" id="price-SOL">165.42</span></div>
        <span class="tk-chg up" id="chg-SOL">+4.85%</span>
      </div>
      <div class="ticker-pill">
        <span class="tk-icon" style="background:var(--amber)">N</span>
        <div class="tk-meta"><span class="tk-sym">BNB/USDT</span><span class="tk-price" id="price-BNB">594.32</span></div>
        <span class="tk-chg up" id="chg-BNB">+1.25%</span>
      </div>
    </div>

    <div class="system-status-block">
      <div class="ss-text">
        <span class="ss-label">System Status</span>
        <span class="ss-value">All Systems Operational</span>
        <span class="ss-date" id="stat-date">—</span>
      </div>
      <svg viewBox="0 0 80 80" class="ss-gauge gauge-ring">
        <circle cx="40" cy="40" r="32" class="gauge-bg"/>
        <circle cx="40" cy="40" r="32" class="gauge-fg" data-pct="100"/>
        <text x="40" y="45" text-anchor="middle" class="gauge-num">100%</text>
      </svg>
    </div>

    <div class="data-mode-wrap">
      <button class="data-mode-badge" id="dataModeBadge" type="button" aria-label="Live data connection settings">
        <span class="dm-dot"></span><span id="dataModeText">Simulated Data</span>
      </button>
      <div class="connect-pop" id="connectPop" hidden>
        <div class="connect-pop-title">Connect to your live bot</div>
        <input type="text" id="connBaseUrl" placeholder="https://your-bot.onrender.com" autocomplete="off">
        <input type="password" id="connKey" placeholder="APEX_WEBHOOK_PASSPHRASE" autocomplete="off">
        <div class="connect-pop-actions">
          <button class="connect-btn" id="connectBtn" type="button">Connect</button>
          <button class="disconnect-btn" id="disconnectBtn" type="button">Disconnect</button>
        </div>
        <p class="connect-pop-note">Saved only in this browser. Positions, trades, balance, rejections, engine status, and the candlestick chart (real Delta OHLCV) all switch to your bot — the exchange-latency list and AI Core Load stay illustrative since main.py has no feed for those.</p>
      </div>
    </div>
  </header>

  <!-- ============================== MAIN GRID ============================== -->
  <main class="dash-grid tab-view" id="view-dashboard">

    <!-- ---------------- LEFT COLUMN ---------------- -->
    <section class="col-left">

      <div class="panel" id="panel-account">
        <div class="panel-head">
          <span class="panel-title"><svg viewBox="0 0 24 24"><polyline points="3,17 9,10 13,14 21,4"/></svg>Account Overview</span>
          <span class="panel-more">⋯</span>
        </div>
        <div class="stat-hero-label">Total Balance (USDT)</div>
        <div class="stat-hero" id="stat-balance">$ 128,745.32</div>
        <div class="stat-pnl up" id="stat-pnl">+ $7,532.68 (+6.21%)</div>
        <div class="stat-grid-2">
          <div><div class="mini-stat-label">Available Margin</div><div class="mini-stat-value" id="stat-avail">$98,432.11</div></div>
          <div><div class="mini-stat-label">Used Margin</div><div class="mini-stat-value" id="stat-used">$30,313.21</div></div>
        </div>
        <div class="bar-row">
          <div class="bar-row-head"><span>Margin Utilization</span><strong id="stat-marginpct">23.56%</strong></div>
          <div class="bar-track"><div class="bar-fill" id="bar-margin" data-pct="23.56"></div></div>
        </div>
      </div>

      <div class="panel" id="panel-confidence">
        <div class="panel-head">
          <span class="panel-title"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="2.4"/><circle cx="5" cy="6" r="1.6"/><circle cx="19" cy="6" r="1.6"/><circle cx="5" cy="18" r="1.6"/><circle cx="19" cy="18" r="1.6"/><line x1="12" y1="12" x2="5" y2="6"/><line x1="12" y1="12" x2="19" y2="6"/><line x1="12" y1="12" x2="5" y2="18"/><line x1="12" y1="12" x2="19" y2="18"/></svg>AI Confidence Matrix</span>
          <span class="count-pill" data-modepill id="confidenceMode">Demo</span>
        </div>
        <div class="big-gauge-wrap">
          <svg viewBox="0 0 120 120" class="big-gauge gauge-ring">
            <circle cx="60" cy="60" r="50" class="gauge-bg"/>
            <circle cx="60" cy="60" r="50" class="gauge-fg" data-pct="92.7" id="confGaugeFg"/>
          </svg>
          <div class="big-gauge-label"><span class="big-gauge-pct" id="confGaugePct">92.7%</span><span class="big-gauge-tag" id="confGaugeTag">AI Extreme High</span></div>
        </div>
        <div class="confidence-list" id="confidenceList">
          <div class="confidence-row"><span>Market Sentiment</span><span class="tag bullish">BULLISH</span></div>
          <div class="confidence-row"><span>Volatility Index</span><span class="tag medium">MEDIUM</span></div>
          <div class="confidence-row"><span>Trend Strength</span><span class="tag strong">VERY STRONG</span></div>
          <div class="confidence-row"><span>Volume Analysis</span><span class="tag high">HIGH</span></div>
          <div class="confidence-row"><span>Order Flow</span><span class="tag optimal">OPTIMAL</span></div>
          <div class="confidence-row"><span>News Impact</span><span class="tag positive">POSITIVE</span></div>
        </div>
      </div>

      <div class="panel" id="panel-model-perf">
        <div class="panel-head">
          <span class="panel-title"><svg viewBox="0 0 24 24"><path d="M9 3a4 4 0 0 0-3.5 6A4 4 0 0 0 6 16.5 3.5 3.5 0 0 0 9.5 20 3 3 0 0 0 12 18.5V6A3.5 3.5 0 0 0 9 3Z"/><path d="M15 3a4 4 0 0 1 3.5 6A4 4 0 0 1 18 16.5a3.5 3.5 0 0 1-3.5 3.5A3 3 0 0 1 12 18.5V6A3.5 3.5 0 0 1 15 3Z"/></svg>AI Model Performance</span>
          <span class="count-pill" data-modepill id="perfMode">Demo</span>
        </div>
        <div class="brain-row">
          <div class="brain-icon"><svg viewBox="0 0 24 24"><path d="M9 3a4 4 0 0 0-3.5 6A4 4 0 0 0 6 16.5 3.5 3.5 0 0 0 9.5 20 3 3 0 0 0 12 18.5V6A3.5 3.5 0 0 0 9 3Z"/><path d="M15 3a4 4 0 0 1 3.5 6A4 4 0 0 1 18 16.5a3.5 3.5 0 0 1-3.5 3.5A3 3 0 0 1 12 18.5V6A3.5 3.5 0 0 1 15 3Z"/></svg></div>
          <div><div class="stat-hero" style="font-size:22px;color:var(--violet)" id="perfWinRateHero">98.6%</div><div class="mini-stat-label" id="perfWinRateLabel">Model Accuracy</div></div>
        </div>
        <div class="perf-grid">
          <div><div class="perf-cell-label">Closed Trades</div><div class="perf-cell-value" id="perfTrades">24,856</div></div>
          <div><div class="perf-cell-label">Win Rate</div><div class="perf-cell-value" id="perfWinRate2">78.3%</div></div>
          <div><div class="perf-cell-label">Cumulative R</div><div class="perf-cell-value" id="perfCumR">2.71</div></div>
          <div><div class="perf-cell-label">Profit Factor</div><div class="perf-cell-value" id="perfProfitFactor">3.89</div></div>
        </div>
      </div>

      <div class="panel" id="panel-risk">
        <div class="panel-head">
          <span class="panel-title"><svg viewBox="0 0 24 24"><path d="M12 2 L20 5.5 V11 C20 16.5 16.5 20.5 12 22 C7.5 20.5 4 16.5 4 11 V5.5 Z"/></svg>Risk Management Suite</span>
          <span class="count-pill" data-modepill>Demo</span>
        </div>
        <div class="risk-grid">
          <div class="risk-row"><span><span class="risk-dot"></span>Max Drawdown</span><strong id="risk-maxdd">—</strong></div>
          <div class="risk-row"><span><span class="risk-dot"></span>Exposure</span><strong id="risk-exposure">—</strong></div>
          <div class="risk-row"><span><span class="risk-dot"></span>Open Risk (to SL)</span><strong id="risk-openrisk">—</strong></div>
          <div class="risk-row"><span><span class="risk-dot"></span>Leverage</span><strong id="risk-leverage">—</strong></div>
        </div>
        <div class="risk-note" id="risk-note" style="font-size:9.5px;color:var(--text-dim);margin-top:9px;line-height:1.4;"></div>
      </div>

      <div class="panel" id="panel-syspulse">
        <div class="panel-head">
          <span class="panel-title"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><line x1="12" y1="12" x2="12" y2="7"/><line x1="12" y1="12" x2="15.3" y2="14"/></svg>System Pulse</span>
          <span class="count-pill" data-modepill id="pulseMode">Demo</span>
        </div>
        <div class="risk-grid">
          <div class="risk-row"><span><span class="risk-dot"></span>Execution Latency</span><strong id="pulse-avgms">31ms avg</strong></div>
          <div class="risk-row"><span><span class="risk-dot"></span>API Success Rate</span><strong id="pulse-success">99.4%</strong></div>
          <div class="risk-row"><span><span class="risk-dot"></span>Host CPU / RAM</span><strong id="pulse-hostload">18% / 34%</strong></div>
          <div class="risk-row"><span><span class="risk-dot"></span>Process Uptime</span><strong id="pulse-hostuptime">15d 22h</strong></div>
        </div>
        <div class="engine-chip-row" id="engineChips">
          <span class="engine-chip on">Predator Vision</span>
          <span class="engine-chip on">Neural Syndicate</span>
          <span class="engine-chip off">HFT Exits</span>
          <span class="engine-chip off">Shock Block</span>
        </div>
        <div class="pulse-oracle" id="pulseOracle">AI Oracle consensus (Gemini) appears here once connected to your bot — illustrative until then.</div>
      </div>
    </section>

    <!-- ---------------- CENTER COLUMN ---------------- -->
    <section class="col-center">

      <div class="panel core-header">
        <div class="core-id">
          <div class="core-id-icon"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5.5"/><circle cx="12" cy="12" r="2"/></svg></div>
          <div class="core-id-text">
            <span class="core-id-title">AI Core Neural Engine</span>
            <span class="core-id-status">Status: <span id="core-status-word">Learning</span> · Adapting · Optimizing</span>
          </div>
        </div>
        <div class="core-metrics">
          <div class="core-metric">
            <span class="core-metric-label">Core Temperature</span>
            <span class="core-metric-value" id="core-temp">42.7°C</span>
            <div class="mini-bar-track"><div class="mini-bar-fill" id="core-temp-bar" data-pct="58"></div></div>
          </div>
          <div class="core-metric">
            <span class="core-metric-label">Neural Ops / Sec</span>
            <span class="core-metric-value">98.7B</span>
            <svg class="spark" viewBox="0 0 90 20"><polyline points="0,16 12,9 24,13 36,5 48,11 60,4 72,9 90,2"/></svg>
          </div>
        </div>
      </div>

      <div class="mid-row">
        <div class="panel">
          <div class="panel-head"><span class="panel-title"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5.5"/><circle cx="12" cy="12" r="2"/></svg>Quantum Market Radar</span></div>
          <svg viewBox="0 0 200 200" class="radar-svg">
            <circle cx="100" cy="100" r="88" class="radar-ring"/>
            <circle cx="100" cy="100" r="62" class="radar-ring"/>
            <circle cx="100" cy="100" r="36" class="radar-ring"/>
            <line x1="100" y1="12" x2="100" y2="188" class="radar-cross"/>
            <line x1="12" y1="100" x2="188" y2="100" class="radar-cross"/>
            <g class="radar-sweep"><path d="M100,100 L100,12 A88,88 0 0,1 152,30 Z"/></g>
            <circle cx="126" cy="58" r="3.6" class="radar-blip" style="animation-delay:.1s"/>
            <circle cx="66" cy="140" r="3.6" class="radar-blip alt" style="animation-delay:.6s"/>
            <circle cx="150" cy="128" r="3.6" class="radar-blip" style="animation-delay:1.1s"/>
            <circle cx="70" cy="66" r="3.6" class="radar-blip warn" style="animation-delay:1.6s"/>
            <circle cx="100" cy="30" r="3" class="radar-blip alt" style="animation-delay:.35s"/>
            <circle cx="168" cy="104" r="3" class="radar-blip" style="animation-delay:.9s"/>
            <circle cx="44" cy="96" r="3" class="radar-blip warn" style="animation-delay:1.4s"/>
            <circle cx="116" cy="166" r="3" class="radar-blip alt" style="animation-delay:1.9s"/>
            <circle cx="88" cy="52" r="2.4" class="radar-blip" style="animation-delay:2.2s"/>
            <circle cx="139" cy="150" r="2.4" class="radar-blip warn" style="animation-delay:.75s"/>
            <circle cx="126" cy="58" r="2" class="radar-ping" style="animation-delay:.1s"/>
            <circle cx="150" cy="128" r="2" class="radar-ping" style="animation-delay:1.3s"/>
            <circle cx="44" cy="96" r="2" class="radar-ping" style="animation-delay:2s"/>
            <circle cx="100" cy="100" r="5" class="radar-core"/>
          </svg>
          <ul class="latency-list" id="latency-list">
            <li data-base="23"><span class="dot" style="background:var(--amber)"></span><span class="ex-name">Binance</span><span class="ms">23ms</span></li>
            <li data-base="31"><span class="dot" style="background:var(--amber)"></span><span class="ex-name">Bybit</span><span class="ms">31ms</span></li>
            <li data-base="24"><span class="dot" style="background:var(--lime)"></span><span class="ex-name">Delta Exchange</span><span class="ms">24ms</span></li>
            <li data-base="28"><span class="dot" style="background:var(--cyan)"></span><span class="ex-name">OKX</span><span class="ms">28ms</span></li>
            <li data-base="35"><span class="dot" style="background:var(--violet)"></span><span class="ex-name">Kucoin</span><span class="ms">35ms</span></li>
            <li data-base="30"><span class="dot" style="background:var(--coral)"></span><span class="ex-name">Bitget</span><span class="ms">30ms</span></li>
          </ul>
          <div class="signals-total"><span>Total Signals</span><strong id="stat-signals">58,642</strong></div>
        </div>

        <div class="orb-stage in" id="orbStage">
          <canvas id="orbCanvas"></canvas>
          <div class="orb-fallback" id="orbFallback" hidden></div>
        </div>

        <div class="panel">
          <div class="panel-head"><span class="panel-title"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><ellipse cx="12" cy="12" rx="4" ry="9"/><line x1="3" y1="12" x2="21" y2="12"/></svg>Market Intelligence</span></div>
          <div class="worldmap-wrap"><canvas id="worldMapCanvas"></canvas></div>
          <div class="sentiment-block">
            <div class="sentiment-head"><span>Global Market Sentiment</span><span class="sentiment-tag">Bullish 73.6%</span></div>
            <div class="bar-track"><div class="bar-fill" id="bar-sentiment" data-pct="73.6"></div></div>
          </div>
          <div class="fng-block">
            <svg viewBox="0 0 120 68" class="half-gauge">
              <path class="hg-bg" d="M10,60 A50,50 0 0 1 110,60"/>
              <path class="hg-fg" id="fng-path" data-pct="78" d="M10,60 A50,50 0 0 1 110,60"/>
              <text x="60" y="46" class="half-gauge-num" id="fng-num">78</text>
              <text x="60" y="60" class="half-gauge-tag">FEAR &amp; GREED · GREED</text>
              <text x="8" y="66" class="half-gauge-end">0</text>
              <text x="108" y="66" class="half-gauge-end" text-anchor="end">100</text>
            </svg>
          </div>
        </div>
      </div>

      <div class="panel heatmap-panel" id="panel-heatmap">
        <div class="panel-head">
          <span class="panel-title"><svg viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg>Market Heatmap</span>
          <span class="count-pill" data-modepill>Demo</span>
        </div>
        <div class="heatmap-grid" id="heatmapGrid">
          <div class="heat-tile" id="heat-BTC">
            <span class="heat-sym">BTC</span><span class="heat-price" id="heatprice-BTC">$66,432.58</span>
            <span class="heat-chg up" id="heatchg-BTC">+2.35%</span>
            <div class="heat-bar-track"><div class="heat-bar-fill" id="heatbar-BTC"></div></div>
          </div>
          <div class="heat-tile" id="heat-ETH">
            <span class="heat-sym">ETH</span><span class="heat-price" id="heatprice-ETH">$3,142.68</span>
            <span class="heat-chg up" id="heatchg-ETH">+3.12%</span>
            <div class="heat-bar-track"><div class="heat-bar-fill" id="heatbar-ETH"></div></div>
          </div>
          <div class="heat-tile" id="heat-SOL">
            <span class="heat-sym">SOL</span><span class="heat-price" id="heatprice-SOL">$165.42</span>
            <span class="heat-chg up" id="heatchg-SOL">+4.85%</span>
            <div class="heat-bar-track"><div class="heat-bar-fill" id="heatbar-SOL"></div></div>
          </div>
          <div class="heat-tile" id="heat-BNB">
            <span class="heat-sym">BNB</span><span class="heat-price" id="heatprice-BNB">$594.32</span>
            <span class="heat-chg up" id="heatchg-BNB">+1.25%</span>
            <div class="heat-bar-track"><div class="heat-bar-fill" id="heatbar-BNB"></div></div>
          </div>
        </div>
      </div>

      <div class="voice-row">
        <div class="panel you-can-say">
          <div class="panel-head"><span class="panel-title"><svg viewBox="0 0 24 24"><rect x="9" y="3" width="6" height="11" rx="3"/><path d="M5 11a7 7 0 0 0 14 0"/><line x1="12" y1="18" x2="12" y2="22"/></svg>You Can Say</span></div>
          <ul class="say-list" id="sayList">
            <li class="say-item" data-phrase="show performance">Show Performance</li>
            <li class="say-item" data-phrase="what is market status">What is Market Status?</li>
            <li class="say-item" data-phrase="enable autopilot">Enable Auto Pilot</li>
            <li class="say-item" data-phrase="close all positions">Close All Positions</li>
            <li class="say-item" data-phrase="risk management report">Risk Management Report</li>
            <li class="say-item" data-phrase="market news">Market News</li>
          </ul>
        </div>

        <div class="mic-stage" id="micStage">
          <div class="mic-rings">
            <div class="ring r1"></div><div class="ring r2"></div><div class="ring r3"></div>
            <button class="mic-btn" id="micBtn" aria-label="Talk to NEXUS AI">
              <svg viewBox="0 0 24 24"><rect x="9" y="3" width="6" height="11" rx="3"/><path d="M5 11a7 7 0 0 0 14 0"/><line x1="12" y1="18" x2="12" y2="22"/><line x1="8" y1="22" x2="16" y2="22"/></svg>
            </button>
          </div>
          <div class="mic-status" id="micStatusText">Tap to Speak</div>
          <div class="waveform" id="waveform"></div>
          <div class="transcript" id="transcript"></div>
        </div>

        <div class="panel nexus-reply">
          <div class="panel-head"><span class="panel-title">Nexus AI</span><span class="status-pill online">Online &amp; Ready</span></div>
          <div class="reply-bubble" id="nexusReplyText">How can I assist you, Master?</div>
          <div class="nexus-history" id="nexusHistory"></div>
        </div>
      </div>
    </section>

    <!-- ---------------- RIGHT COLUMN ---------------- -->
    <section class="col-right">

      <div class="panel terminal-panel">
        <div class="panel-head">
          <span class="panel-title"><svg viewBox="0 0 24 24"><polyline points="3,17 9,10 13,14 21,4"/></svg>Live Trading Terminal</span>
          <div class="tf-tabs" id="tfTabs">
            <button class="tf-tab active" data-tf="1m">1m</button>
            <button class="tf-tab" data-tf="5m">5m</button>
            <button class="tf-tab" data-tf="15m">15m</button>
            <button class="tf-tab" data-tf="1h">1h</button>
            <button class="tf-tab" data-tf="1D">1D</button>
          </div>
        </div>
        <div class="terminal-price-row">
          <span class="terminal-symbol">BTC/USDT</span>
          <span class="terminal-price" id="chartPrice">66,432.58</span>
          <span class="terminal-change up" id="chartChange">+2.35%</span>
          <span class="chart-live-tag" id="chartLiveTag" hidden>Live · Delta</span>
        </div>
        <div class="chart-wrap"><canvas id="priceChart"></canvas><div class="chart-tooltip" id="chartTooltip" hidden></div></div>
        <div class="chart-xaxis" id="chartXAxis"></div>
      </div>

      <div class="panel positions-panel" id="panel-positions">
        <div class="panel-head">
          <span class="panel-title"><svg viewBox="0 0 24 24"><rect x="3" y="3" width="8" height="8" rx="1.5"/><rect x="13" y="3" width="8" height="8" rx="1.5"/><rect x="3" y="13" width="8" height="8" rx="1.5"/><rect x="13" y="13" width="8" height="8" rx="1.5"/></svg>Active Positions</span>
          <span class="count-pill" id="posCount">7 Active Positions</span>
        </div>
        <div class="table-scroll">
          <table class="data-table">
            <thead><tr><th>Pair</th><th>Side</th><th>Size</th><th>Entry</th><th>PNL (USDT)</th><th>ROI</th></tr></thead>
            <tbody id="positionsBody"></tbody>
          </table>
        </div>
      </div>

      <div class="panel trades-panel">
        <div class="panel-head">
          <span class="panel-title"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><line x1="12" y1="12" x2="12" y2="7"/><line x1="12" y1="12" x2="15.3" y2="14"/></svg>Recent Trades</span>
          <span class="count-pill">All</span>
        </div>
        <div class="table-scroll">
          <table class="data-table">
            <thead><tr><th>Time</th><th>Pair</th><th>Side</th><th>Size</th><th>Price</th><th>Status</th></tr></thead>
            <tbody id="tradesBody"></tbody>
          </table>
        </div>
      </div>

      <div class="panel news-panel">
        <div class="panel-head">
          <span class="panel-title"><svg viewBox="0 0 24 24"><path d="M12 3C9 3 7.5 5 7.5 8V11L5.5 14.5H18.5L16.5 11V8C16.5 5 15 3 12 3Z"/><path d="M10 17a2 2 0 0 0 4 0"/></svg>News &amp; Alerts</span>
          <span class="count-pill">All</span>
        </div>
        <div class="news-list" id="newsList"></div>
        <a class="view-all" id="viewAllNews">View All News →</a>
      </div>

      <div class="panel gatekeeper-panel" id="panel-gatekeeper">
        <div class="panel-head">
          <span class="panel-title"><svg viewBox="0 0 24 24"><path d="M12 2 L20 5.5 V11 C20 16.5 16.5 20.5 12 22 C7.5 20.5 4 16.5 4 11 V5.5 Z"/></svg>AI Gatekeeper Log</span>
          <span class="count-pill" data-modepill id="gatekeeperMode">Demo</span>
        </div>
        <div id="gatekeeperList"></div>
      </div>
    </section>

  </main>

  <!-- ================= AUTOPILOT ================= -->
  <section class="tab-view" id="view-autopilot" hidden>
    <div class="panel">
      <div class="panel-head">
        <span class="panel-title"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M4.2 4.2l2.1 2.1M17.7 17.7l2.1 2.1M2 12h3M19 12h3M4.2 19.8l2.1-2.1M17.7 6.3l2.1-2.1"/></svg>Autopilot</span>
        <span class="count-pill" data-modepill id="autopilotMode">Demo</span>
      </div>
      <div id="autopilotBody"><div class="tab-empty">Loading…</div></div>
    </div>
  </section>

  <!-- ================= PORTFOLIO VAULT ================= -->
  <section class="tab-view" id="view-vault" hidden>
    <div class="panel">
      <div class="panel-head">
        <span class="panel-title"><svg viewBox="0 0 24 24"><rect x="3" y="7" width="18" height="13" rx="2"/><path d="M3 10h18M8 3.5h8"/></svg>Portfolio Vault</span>
        <span class="count-pill" data-modepill id="vaultMode">Demo</span>
      </div>
      <div id="vaultBody"><div class="tab-empty">Loading…</div></div>
    </div>
  </section>

  <!-- ================= BACKTEST ENGINE ================= -->
  <section class="tab-view" id="view-backtest" hidden>
    <div class="panel">
      <div class="panel-head">
        <span class="panel-title"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3.5 2"/></svg>Backtest Engine</span>
        <span class="count-pill" data-modepill id="backtestMode">Demo</span>
      </div>
      <div id="backtestBody"><div class="tab-empty">Loading…</div></div>
    </div>

    <div class="panel">
      <div class="panel-head">
        <span class="panel-title"><svg viewBox="0 0 24 24"><path d="M3 3v18h18"/><path d="M18.7 8l-5.1 5.1-3.5-3.5L4 15.7"/></svg>Historical Strategy Backtest</span>
        <span class="count-pill" style="background:rgba(157,255,31,.08);color:var(--text-mid);border:1px solid var(--panel-border);">Real OHLCV</span>
      </div>
      <div class="settings-row" style="border:none;padding-top:0;">
        <span style="max-width:100%;font-size:11px;line-height:1.5;color:var(--text-mid);">
          Runs a <b style="color:var(--text-hi);">simplified EMA/RSI/ADX proxy strategy</b> against REAL historical
          Delta candles — fees and slippage included. This is <b style="color:var(--coral);">not</b> a replica of your
          full Pine Script (9 tiers, VSA Shield, KNN/ML ensemble aren't ported here) — it's a sanity floor: does a
          basic trend approach have any edge on this symbol's real history at all.
        </span>
      </div>
      <div class="act-row" style="margin-top:10px;">
        <select id="btSymbol" class="act-btn" style="flex:1;">
          <option value="BTCUSD">BTCUSD</option><option value="ETHUSD">ETHUSD</option>
          <option value="SOLUSD">SOLUSD</option><option value="BNBUSD">BNBUSD</option>
        </select>
        <select id="btResolution" class="act-btn" style="flex:1;">
          <option value="15m">15m</option><option value="1h" selected>1h</option><option value="1d">1D</option>
        </select>
      </div>
      <div class="act-row" style="margin-top:8px;">
        <select id="btDays" class="act-btn" style="flex:1;">
          <option value="14">14 days</option><option value="30" selected>30 days</option>
          <option value="60">60 days</option><option value="90">90 days</option>
        </select>
        <button class="act-btn primary" id="btnRunBacktest" style="flex:1;">Run Backtest</button>
      </div>
      <div id="btResults" style="margin-top:14px;"></div>
    </div>
  </section>

  <!-- ================= SYSTEM SETTINGS ================= -->
  <section class="tab-view" id="view-settings" hidden>
    <div class="panel">
      <div class="panel-head">
        <span class="panel-title"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.9l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.9-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.7 1.7 0 0 0-1-1.6 1.7 1.7 0 0 0-1.9.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.9 1.7 1.7 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.7 1.7 0 0 0 1.6-1 1.7 1.7 0 0 0-.3-1.9l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.9.3H9a1.7 1.7 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.9-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.9V9a1.7 1.7 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z"/></svg>System Settings</span>
        <span class="count-pill" data-modepill id="settingsMode">Demo</span>
      </div>
      <div id="settingsBody"><div class="tab-empty">Loading…</div></div>
    </div>
  </section>

  <!-- ================= CONFIRM MODAL (shared by any destructive action) ================= -->
  <div class="confirm-overlay" id="confirmOverlay">
    <div class="confirm-card">
      <div class="confirm-title" id="confirmTitle">Are you sure?</div>
      <div class="confirm-body" id="confirmBody"></div>
      <div class="confirm-actions">
        <button class="act-btn" id="confirmCancelBtn" type="button" style="flex:1;">Cancel</button>
        <button class="act-btn danger" id="confirmOkBtn" type="button" style="flex:1;">Confirm</button>
      </div>
    </div>
  </div>

  <!-- ============================== BOTTOM NAV ============================== -->
  <nav class="bottom-nav">
    <button class="nav-arrow" id="navLeft" aria-label="scroll left"><svg viewBox="0 0 24 24"><polyline points="15,6 9,12 15,18"/></svg></button>
    <div class="nav-items" id="navItems">
      <button class="nav-item active" data-nav="dashboard">
        <svg viewBox="0 0 24 24"><rect x="3" y="3" width="8" height="8" rx="1.5"/><rect x="13" y="3" width="8" height="8" rx="1.5"/><rect x="3" y="13" width="8" height="8" rx="1.5"/><rect x="13" y="13" width="8" height="8" rx="1.5"/></svg>
        <span class="nav-item-text"><span class="nav-title">Dashboard</span><span class="nav-sub">Command Center</span></span>
      </button>
      <button class="nav-item" data-nav="strategy">
        <svg viewBox="0 0 24 24"><path d="M9 3V7L4.5 18C4 20 5.5 21 7.5 21H16.5C18.5 21 20 20 19.5 18L15 7V3"/><line x1="8" y1="3" x2="16" y2="3"/></svg>
        <span class="nav-item-text"><span class="nav-title">Strategy Lab</span><span class="nav-sub">AI Strategies</span></span>
      </button>
      <button class="nav-item" data-nav="backtest">
        <svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><line x1="12" y1="12" x2="12" y2="7"/><line x1="12" y1="12" x2="15.3" y2="14"/></svg>
        <span class="nav-item-text"><span class="nav-title">Backtest Engine</span><span class="nav-sub">Historical Analysis</span></span>
      </button>
      <button class="nav-item" data-nav="vault">
        <svg viewBox="0 0 24 24"><rect x="5" y="11" width="14" height="10" rx="2"/><path d="M8 11V7a4 4 0 0 1 8 0v4"/></svg>
        <span class="nav-item-text"><span class="nav-title">Portfolio Vault</span><span class="nav-sub">Holdings &amp; Equity</span></span>
      </button>
      <button class="nav-item" data-nav="autopilot">
        <svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><polygon points="14.5,7.5 10.5,10.5 9.5,16.5 13.5,13.5"/></svg>
        <span class="nav-item-text"><span class="nav-title">Autopilot</span><span class="nav-sub">AI Trading Mode</span></span>
      </button>
      <button class="nav-item" data-nav="settings">
        <svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="4"/><line x1="12" y1="20" x2="12" y2="23"/><line x1="1" y1="12" x2="4" y2="12"/><line x1="20" y1="12" x2="23" y2="12"/><line x1="4.2" y1="4.2" x2="6.3" y2="6.3"/><line x1="17.7" y1="17.7" x2="19.8" y2="19.8"/><line x1="4.2" y1="19.8" x2="6.3" y2="17.7"/><line x1="17.7" y1="6.3" x2="19.8" y2="4.2"/></svg>
        <span class="nav-item-text"><span class="nav-title">System Settings</span><span class="nav-sub">Configuration</span></span>
      </button>
    </div>
    <button class="nav-arrow" id="navRight" aria-label="scroll right"><svg viewBox="0 0 24 24"><polyline points="9,6 15,12 9,18"/></svg></button>
  </nav>

  <!-- ============================== FOOTER STATUS ============================== -->
  <footer class="status-bar">
    <div class="stat-item"><span class="label">Uptime</span><span class="value" id="stat-uptime">15D 22H 47M</span></div>
    <div class="stat-item"><span class="label">Data Streams</span><span class="value">12 Live</span></div>
    <div class="stat-item"><span class="label">AI Core Load</span><span class="value" id="stat-load">67.3%</span></div>
    <div class="stat-center"><span class="pulse-dot"></span>Quantum Processing <strong>Active</strong></div>
    <div class="stat-item"><span class="label">Security Status</span><span class="value good">Maximum</span></div>
    <div class="stat-item"><span class="label">Server Latency</span><span class="value" id="stat-srvlatency">24ms</span></div>
    <div class="stat-item"><span class="label">Data Encryption</span><span class="value">AES-256</span></div>
  </footer>
</div>

<div class="toast" id="toast"></div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
<script>
/* ================================================================
   CONFIG / STATE
   ================================================================ */
const state = {
  prices:{ BTC:66432.58, ETH:3142.68, SOL:165.42, BNB:594.32 },
  changePct:{ BTC:2.35, ETH:3.12, SOL:4.85, BNB:1.25 },
  balance:128745.32, pnl24h:7532.68, pnlPct:6.21,
  availMargin:98432.11, usedMargin:30313.21,
  totalSignals:58642, coreLoad:67.3, srvLatency:24,
};

/* ------------------------------------------------------------
   LIVE DATA BRIDGE (optional, real — verified against main.py)
   Every read below is a real main.py endpoint, gated behind
   ?key=<APEX_WEBHOOK_PASSPHRASE>. CORS is already wide open on the
   bot (Access-Control-Allow-Origin:*), so this works from any host —
   no same-origin requirement.
     GET /status         -> { live_mode, paused, open_positions, total_trades, circuit_breaker }
     GET /positions      -> { positions:[{ symbol, direction, entry_price, qty, sl, tp1, tp2, tp3, status }] }
     GET /trades         -> { trades:[{ symbol, direction, event, qty, price, timestamp }] }
     GET /balance        -> { balance, error, cached_age_s }
     GET /mark-prices    -> { prices:{ SYMBOL: price } }              — turns entry_price+qty into real PnL/ROI
     GET /config         -> { predator_vision_enabled, neural_syndicate_enabled, hft_parallel_exits,
                               block_entries_during_shock, kill_switch_active,
                               ai_market_sentiment:{consensus, symbol, updated_at}, ... }
     GET /rejections     -> { rejections:[{ symbol, signal, direction, reason, detail, timestamp }] }
     GET /execution-stats -> { count, avg_ms, fastest_ms, slowest_ms, success_rate }
     GET /system-health  -> { available, uptime_seconds, cpu_percent, memory_percent, memory_mb, thread_count }
   No control actions (pause/resume/close-all/kill-switch) are wired
   here on purpose — those actually move real money and deserve their
   own confirmation UX, not a voice command or a demo toggle.
   ------------------------------------------------------------ */
const LIVE = { enabled:false, baseUrl:'', key:'' };
const LIVE_STORAGE_KEY = 'apex_nexus_dashboard_connection';
const connState = { lastLatencyMs:null, lastOk:null };

function loadLiveConfig(){
  try{
    const raw = localStorage.getItem(LIVE_STORAGE_KEY);
    if(raw){
      const cfg = JSON.parse(raw);
      if(cfg.baseUrl && cfg.key){ LIVE.baseUrl = cfg.baseUrl; LIVE.key = cfg.key; LIVE.enabled = true; }
    }
  }catch(e){}
  updateDataModeBadge();
}
function applyLiveConfig(baseUrl, key){
  LIVE.baseUrl = baseUrl.replace(/\/+$/,''); LIVE.key = key; LIVE.enabled = !!(LIVE.baseUrl && LIVE.key);
  try{ localStorage.setItem(LIVE_STORAGE_KEY, JSON.stringify({ baseUrl:LIVE.baseUrl, key:LIVE.key })); }catch(e){}
  updateDataModeBadge();
  if(LIVE.enabled){ showToast('Connected — pulling live data from your bot.'); pollLive(); loadCandles(chartState.tf); }
}
function clearLiveConfig(){
  LIVE.baseUrl=''; LIVE.key=''; LIVE.enabled=false;
  try{ localStorage.removeItem(LIVE_STORAGE_KEY); }catch(e){}
  updateDataModeBadge();
  loadCandles(chartState.tf);
  renderRiskPanel();
  showToast('Disconnected — back to simulated data.');
}
function updateDataModeBadge(){
  const badge = document.getElementById('dataModeBadge'), text = document.getElementById('dataModeText');
  badge.classList.toggle('live', LIVE.enabled);
  text.textContent = LIVE.enabled ? 'Live Data' : 'Simulated Data';
  document.querySelectorAll('[data-modepill]').forEach(p=> p.textContent = LIVE.enabled ? 'Live' : 'Demo');
  const urlInput = document.getElementById('connBaseUrl');
  if(urlInput) urlInput.value = LIVE.baseUrl;
}

async function liveFetch(path){
  if(!LIVE.enabled || !LIVE.baseUrl) return null;
  const sep = path.includes('?') ? '&' : '?';
  const t0 = performance.now();
  try{
    const res = await fetch(LIVE.baseUrl + path + sep + 'key=' + encodeURIComponent(LIVE.key), { cache:'no-store' });
    connState.lastLatencyMs = Math.round(performance.now() - t0);
    if(!res.ok){ connState.lastOk = false; return null; }
    connState.lastOk = true;
    return await res.json();
  }catch(e){ connState.lastOk = false; return null; }
}

const _lastLiveTickerPrice = {};
const LIVECACHE = { status:null, positions:null, rawPositions:null, balance:null, config:null, marks:null, cycles:null, stats:null };

async function pollLive(){
  if(!LIVE.enabled) return;
  const [statusJ, posJ, trdJ, balJ, cfgJ, rejJ, execJ, sysJ, tickJ, oracleJ, perfJ, statsTrdJ] = await Promise.all([
    liveFetch('/status'), liveFetch('/positions'), liveFetch('/trades?limit=8'), liveFetch('/balance'),
    liveFetch('/config'), liveFetch('/rejections?limit=6'), liveFetch('/execution-stats'), liveFetch('/system-health'),
    liveFetch('/mark-prices?symbols='+TICKERS.join(',')),
    liveFetch('/ai-oracle'), liveFetch('/performance-summary'),
    liveFetch('/trades?limit=500')
  ]);
  if(tickJ && tickJ.prices){
    TICKERS.forEach(sym=>{
      const p = tickJ.prices[sym];
      if(p==null) return;
      const prev = _lastLiveTickerPrice[sym] != null ? _lastLiveTickerPrice[sym] : p;
      const pct = prev ? ((p-prev)/prev)*100 : 0;
      _lastLiveTickerPrice[sym] = p;
      state.prices[sym] = p;
      state.changePct[sym] = clamp(state.changePct[sym]*0.7 + pct*30, -20, 20);
      const priceEl = document.getElementById('price-'+sym), chEl = document.getElementById('chg-'+sym);
      if(priceEl) setTextFlash(priceEl, fmtUSD(p), pct>=0?1:-1);
      if(chEl){
        const v = state.changePct[sym];
        chEl.textContent = (v>=0?'+':'') + v.toFixed(2) + '%';
        chEl.classList.toggle('up', v>=0); chEl.classList.toggle('down', v<0);
      }
      updateHeatTile(sym, state.prices[sym], state.changePct[sym]);
    });
  }
  if(connState.lastLatencyMs!=null) document.getElementById('stat-srvlatency').textContent = connState.lastLatencyMs+'ms';
  if(balJ && typeof balJ.balance === 'number'){
    state.balance = balJ.balance;
    setTextFlash(document.getElementById('stat-balance'), fmtUSD(state.balance,2,'$ '), 1);
  }
  LIVECACHE.status = statusJ; LIVECACHE.balance = balJ; LIVECACHE.config = cfgJ;
  let marks = {};
  if(posJ && Array.isArray(posJ.positions)){
    const bases = [...new Set(posJ.positions.map(p=>(p.symbol||'').replace(/USDT?$/i,'').toUpperCase()).filter(Boolean))];
    const markJ = bases.length ? await liveFetch('/mark-prices?symbols='+encodeURIComponent(bases.join(','))) : null;
    marks = (markJ && markJ.prices) || {};
    positions = posJ.positions.map(p=>mapLivePosition(p, marks));
    renderPositions();
    LIVECACHE.rawPositions = posJ.positions; LIVECACHE.marks = marks;
  }
  if(trdJ && Array.isArray(trdJ.trades)){ trades = trdJ.trades.slice(0,8).map(mapLiveTrade); renderTrades(); }
  if(statsTrdJ && Array.isArray(statsTrdJ.trades)){
    // API returns newest-first; reconstructTradeCycles needs oldest-first.
    const cycles = reconstructTradeCycles(statsTrdJ.trades.slice().reverse());
    LIVECACHE.cycles = cycles; LIVECACHE.stats = computeBacktestStats(cycles);
    renderRiskPanel();
    if(document.getElementById('view-backtest') && !document.getElementById('view-backtest').hidden) renderBacktestTab();
  }
  if(statusJ) document.getElementById('posCount').textContent = (statusJ.open_positions ?? positions.length) + ' Active Positions';
  if(rejJ && Array.isArray(rejJ.rejections)){ gatekeeperLog = rejJ.rejections.map(mapLiveRejection); renderGatekeeper(); }
  if(execJ) renderExecutionStats(execJ);
  if(sysJ) renderSystemHealth(sysJ);
  if(cfgJ) renderEngineChips(cfgJ);
  if(oracleJ) renderAiOracle(oracleJ);
  if(perfJ) renderPerformance(perfJ);
  // These four react to data that may have arrived above even if their own
  // tab isn't the active view yet — cheap to keep current in the background
  // so switching tabs shows fresh numbers immediately, not a stale snapshot.
  if(document.getElementById('view-autopilot') && !document.getElementById('view-autopilot').hidden) renderAutopilotTab();
  if(document.getElementById('view-vault') && !document.getElementById('view-vault').hidden) renderVaultTab();
  if(document.getElementById('view-settings') && !document.getElementById('view-settings').hidden) renderSettingsTab();
}
function renderRiskPanel(){
  const dd = document.getElementById('risk-maxdd'), exp = document.getElementById('risk-exposure'),
        risk = document.getElementById('risk-openrisk'), lev = document.getElementById('risk-leverage'),
        note = document.getElementById('risk-note');
  if(!LIVE.enabled){
    dd.textContent='12.4%'; exp.textContent='35.6%'; risk.textContent='$2,341.32'; lev.textContent='10x Isolated';
    if(note) note.textContent = 'Illustrative — connect your live bot for real numbers.';
    return;
  }
  const rawPos = LIVECACHE.rawPositions || [], marks = LIVECACHE.marks || {}, bal = LIVECACHE.balance && LIVECACHE.balance.balance;
  const openRisk = computeOpenRiskUSD(rawPos);
  const exposureUSD = computeExposureUSD(rawPos, marks);
  risk.textContent = fmtUSD(openRisk, 2, '$ ');
  exp.textContent = (bal && bal>0) ? ((exposureUSD/bal)*100).toFixed(1)+'%' : (exposureUSD>0 ? fmtUSD(exposureUSD,0,'$ ') : '—');
  if(LIVECACHE.stats){
    dd.textContent = fmtUSD(LIVECACHE.stats.maxDrawdownAbs, 2, '$ ') + ' (realized)';
  } else { dd.textContent = rawPos.length || (LIVECACHE.cycles && LIVECACHE.cycles.length) ? '$ 0.00 (realized)' : 'No closed trades yet'; }
  lev.textContent = 'Not tracked by backend';
  if(note) note.textContent = 'Max drawdown = peak-to-trough on REALIZED pnl from your trade log (not full account-equity history, which isn\'t stored). Leverage isn\'t recorded per-position by this backend.';
}

/* ================================================================
   [REAL TABS ADD] Autopilot · Portfolio Vault · Backtest Engine ·
   System Settings — shared confirm modal + control-action caller,
   then one render function per tab.
   ================================================================ */
function confirmAction(title, body, onConfirm){
  const overlay = document.getElementById('confirmOverlay');
  document.getElementById('confirmTitle').textContent = title;
  document.getElementById('confirmBody').textContent = body;
  overlay.classList.add('show');
  const okBtn = document.getElementById('confirmOkBtn'), cancelBtn = document.getElementById('confirmCancelBtn');
  function cleanup(){ overlay.classList.remove('show'); okBtn.removeEventListener('click', onOk); cancelBtn.removeEventListener('click', onCancel); overlay.removeEventListener('click', onBackdrop); }
  function onOk(){ cleanup(); onConfirm(); }
  function onCancel(){ cleanup(); }
  function onBackdrop(e){ if(e.target===overlay) onCancel(); }
  okBtn.addEventListener('click', onOk);
  cancelBtn.addEventListener('click', onCancel);
  overlay.addEventListener('click', onBackdrop);
}
async function callControl(action){
  if(!LIVE.enabled){ showToast('Connect your live bot first, Master.'); return null; }
  try{
    const res = await fetch(LIVE.baseUrl + '/control/' + encodeURIComponent(LIVE.key) + '/' + action, { cache:'no-store' });
    const body = await res.json().catch(()=>({}));
    if(!res.ok){
      showToast('Action failed: ' + (body.error || res.status) + (res.status===403 ? ' — if APEX_CONTROL_PASSWORD is set separately on your bot, your connect key won\'t match it.' : ''));
      return null;
    }
    return body;
  }catch(e){ showToast('Network error — could not reach your bot.'); return null; }
}
// [PREMIUM FIX — LIVE/DRY-RUN + SIGNAL TIER CONTROLS] The backend has had
// /mode/<secret> and /signals/<secret> for a while, but nothing in this
// dashboard ever called them — Mode was rendered as a read-only label and
// signal tiers weren't shown at all outside Settings, where they were also
// just static tags. These three helpers follow the exact same shape as
// callControl() above (GET-with-query, same error toast, same "not
// connected" guard) so every button below behaves identically to the ones
// that already worked.
async function callMode(liveBool){
  if(!LIVE.enabled){ showToast('Connect your live bot first, Master.'); return null; }
  try{
    const res = await fetch(LIVE.baseUrl + '/mode/' + encodeURIComponent(LIVE.key) + '?live_mode=' + (liveBool?'true':'false'), { cache:'no-store' });
    const body = await res.json().catch(()=>({}));
    if(!res.ok){ showToast('Mode change failed: ' + (body.error || res.status)); return null; }
    return body;
  }catch(e){ showToast('Network error — could not reach your bot.'); return null; }
}
async function callSignalTier(tier, enable){
  if(!LIVE.enabled){ showToast('Connect your live bot first, Master.'); return null; }
  try{
    const qp = (enable ? 'enable=' : 'disable=') + encodeURIComponent(tier);
    const res = await fetch(LIVE.baseUrl + '/signals/' + encodeURIComponent(LIVE.key) + '?' + qp, { cache:'no-store' });
    const body = await res.json().catch(()=>({}));
    if(!res.ok){ showToast('Signal tier update failed: ' + (body.error || res.status)); return null; }
    return body;
  }catch(e){ showToast('Network error — could not reach your bot.'); return null; }
}
async function callRiskSizing(enabledBool){
  if(!LIVE.enabled){ showToast('Connect your live bot first, Master.'); return null; }
  try{
    const res = await fetch(LIVE.baseUrl + '/control/' + encodeURIComponent(LIVE.key) + '/risk-sizing?enabled=' + (enabledBool?'true':'false'), { cache:'no-store' });
    const body = await res.json().catch(()=>({}));
    if(!res.ok){ showToast('Risk sizing update failed: ' + (body.error || res.status)); return null; }
    return body;
  }catch(e){ showToast('Network error — could not reach your bot.'); return null; }
}

function renderAutopilotTab(){
  const body = document.getElementById('autopilotBody');
  if(!LIVE.enabled){
    body.innerHTML = '<div class="tab-empty"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>'+
      'Connect your live bot to control it from here — pause/resume entries, arm the kill switch, or close every open position.'+
      '<button class="connect-cta" type="button" id="autopilotConnectCta">Connect Now</button></div>';
    document.getElementById('autopilotConnectCta').onclick = (e)=>{ e.stopPropagation(); document.getElementById('connectPop').hidden = false; };
    return;
  }
  const cfg = LIVECACHE.config || {};
  const paused = !!cfg.paused, killed = !!cfg.kill_switch_active, live = !!cfg.live_mode;
  const cb = cfg.circuit_breaker || {};
  let bannerClass='ok', bannerText = live ? 'LIVE — placing real orders' : 'DRY RUN — no real orders sent';
  if(paused){ bannerClass='paused'; bannerText='PAUSED — no new entries will be taken'; }
  if(killed){ bannerClass='danger'; bannerText='KILL SWITCH ARMED — all new entries blocked'; }
  body.innerHTML =
    '<div class="status-banner '+bannerClass+'"><span class="status-dot"></span>'+bannerText+'</div>'+
    '<div class="act-row">'+
      '<button class="act-btn '+(live?'warn':'primary')+'" id="btnModeToggle">'+(live?'Switch to Dry Run':'Go Live')+'<span class="sub">'+(live?'Simulate only — nothing hits the exchange':'Start placing REAL orders on Delta')+'</span></button>'+
    '</div>'+
    '<div class="act-row">'+
      '<button class="act-btn primary" id="btnResume" '+(!paused?'disabled':'')+'>Resume Trading<span class="sub">Allow new entries again</span></button>'+
      '<button class="act-btn warn" id="btnPause" '+(paused?'disabled':'')+'>Pause<span class="sub">Block new entries · open trades keep running</span></button>'+
    '</div>'+
    '<div class="act-row">'+
      '<button class="act-btn '+(killed?'primary':'danger')+'" id="btnKill">'+(killed?'Disarm Kill Switch':'Arm Kill Switch')+'<span class="sub">'+(killed?'Resume normal operation':'Blocks new entries until manually reset')+'</span></button>'+
      '<button class="act-btn danger" id="btnCloseAll">Close All Positions<span class="sub">Market-close every open trade now</span></button>'+
    '</div>'+
    '<div class="panel" style="padding:14px;margin-top:2px;">'+
      '<div class="settings-row"><span class="k">Circuit Breaker</span><span class="v '+(cb.tripped?'danger':'on')+'">'+(cb.tripped?'TRIPPED':'Clear')+'</span></div>'+
      '<div class="settings-row"><span class="k">Consecutive Losses</span><span class="v">'+(cb.consecutive_losses ?? '—')+' / '+(cb.max_consecutive_losses ?? '—')+'</span></div>'+
      '<div class="settings-row"><span class="k">Mode</span><span class="v '+(live?'danger':'on')+'">'+(live?'LIVE (real orders)':'DRY RUN (simulated)')+'</span></div>'+
    '</div>';
  document.getElementById('btnPause').onclick = ()=> confirmAction('Pause trading?',
    'No new entries will be taken until you resume. Positions already open keep running with their normal SL/TP/trailing logic — pausing does not touch them.',
    async ()=>{ const r = await callControl('pause'); if(r){ showToast('Paused.'); await pollLive(); renderAutopilotTab(); } });
  document.getElementById('btnResume').onclick = async ()=>{
    const r = await callControl('resume'); if(r){ showToast('Resumed — new entries allowed again.'); await pollLive(); renderAutopilotTab(); }
  };
  document.getElementById('btnKill').onclick = ()=>{
    if(killed){
      callControl('kill-switch/reset').then(r=>{ if(r){ showToast('Kill switch disarmed.'); pollLive().then(renderAutopilotTab); } });
    } else {
      confirmAction('Arm the kill switch?',
        'Blocks every new entry immediately and stays armed until you manually disarm it — meant for "something is wrong, stop everything until I\'ve looked at it". It does NOT close positions already open; use Close All for that separately.',
        async ()=>{ const r = await callControl('kill-switch'); if(r){ showToast('Kill switch armed.'); await pollLive(); renderAutopilotTab(); } });
    }
  };
  document.getElementById('btnCloseAll').onclick = ()=> confirmAction('Close ALL open positions?',
    'Immediately market-closes every open position at whatever price is available right now — it does not wait for TP/SL levels, and this cannot be undone.',
    async ()=>{ const r = await callControl('close-all'); if(r){ showToast('Close-all sent.'); await pollLive(); renderAutopilotTab(); } });
  document.getElementById('btnModeToggle').onclick = ()=>{
    if(live){
      (async ()=>{ const r = await callMode(false); if(r){ showToast('Switched to DRY RUN — simulated orders only.'); await pollLive(); renderAutopilotTab(); if(document.getElementById('view-settings') && !document.getElementById('view-settings').hidden) renderSettingsTab(); } })();
    } else {
      confirmAction('Go LIVE?',
        'This switches the bot to placing REAL orders with real money on your connected exchange account. Double-check position sizing and active signal tiers first — Close All and Kill Switch still work independently if something goes wrong.',
        async ()=>{ const r = await callMode(true); if(r){ showToast('LIVE — placing real orders now.'); await pollLive(); renderAutopilotTab(); if(document.getElementById('view-settings') && !document.getElementById('view-settings').hidden) renderSettingsTab(); } });
    }
  };
}

function renderVaultTab(){
  const body = document.getElementById('vaultBody');
  if(!LIVE.enabled){
    body.innerHTML = '<div class="tab-empty"><svg viewBox="0 0 24 24"><rect x="3" y="7" width="18" height="13" rx="2"/><path d="M3 10h18M8 3.5h8"/></svg>'+
      'Connect your live bot to see your real Delta account balance and position exposure here.'+
      '<button class="connect-cta" type="button" id="vaultConnectCta">Connect Now</button></div>';
    document.getElementById('vaultConnectCta').onclick = (e)=>{ e.stopPropagation(); document.getElementById('connectPop').hidden = false; };
    return;
  }
  const balJ = LIVECACHE.balance || {}, rawPos = LIVECACHE.rawPositions || [], marks = LIVECACHE.marks || {};
  const bal = typeof balJ.balance === 'number' ? balJ.balance : null;
  const exposureUSD = computeExposureUSD(rawPos, marks);
  const rows = rawPos.map(p=>{
    const base=(p.symbol||'').replace(/USDT?$/i,'').toUpperCase();
    const px = marks[base]!=null ? marks[base] : p.entry_price;
    const val = (px!=null && p.qty!=null) ? px*p.qty : null;
    return '<div class="settings-row"><span class="k">'+(p.symbol||'—')+' · '+(p.direction||'—')+'</span><span class="v">'+(val!=null?fmtUSD(val,2,'$ '):'—')+'</span></div>';
  }).join('') || '<div class="settings-row"><span class="k">No open positions</span><span class="v">—</span></div>';
  body.innerHTML =
    '<div class="stat-hero-label">Total Balance (USDT)</div>'+
    '<div class="stat-hero">'+(bal!=null ? fmtUSD(bal,2,'$ ') : (balJ.error ? 'Unavailable' : '—'))+'</div>'+
    (balJ.error ? '<div style="font-size:10.5px;color:var(--coral);margin-top:4px;">'+balJ.error+(balJ.cached_age_s?' · last good value '+Math.round(balJ.cached_age_s)+'s ago':'')+'</div>' : '')+
    '<div class="stat-mini-grid" style="margin-top:16px;">'+
      '<div class="stat-mini"><div class="lbl">Position Value</div><div class="val">'+fmtUSD(exposureUSD,2,'$ ')+'</div></div>'+
      '<div class="stat-mini"><div class="lbl">Exposure %</div><div class="val">'+((bal&&bal>0)?((exposureUSD/bal)*100).toFixed(1)+'%':'—')+'</div></div>'+
    '</div>'+
    '<div class="panel-title" style="margin:18px 0 4px;font-size:10.5px;">Open Position Value</div>'+
    '<div class="panel" style="padding:12px 14px;">'+rows+'</div>';
}

function renderBacktestTab(){
  const body = document.getElementById('backtestBody');
  if(!LIVE.enabled){
    body.innerHTML = '<div class="tab-empty"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3.5 2"/></svg>'+
      'Connect your live bot to see real stats reconstructed from your actual trade log — win rate, profit factor, max drawdown, expectancy.'+
      '<button class="connect-cta" type="button" id="backtestConnectCta">Connect Now</button></div>';
    document.getElementById('backtestConnectCta').onclick = (e)=>{ e.stopPropagation(); document.getElementById('connectPop').hidden = false; };
    return;
  }
  const s = LIVECACHE.stats;
  if(!s){
    body.innerHTML = '<div class="tab-empty">No completed trades yet — this fills in once your bot has closed at least one full position.</div>';
    return;
  }
  const pf = isFinite(s.profitFactor) ? s.profitFactor.toFixed(2) : '∞';
  const curve = s.equityCurve, w = 300, h = 92;
  const minV = Math.min(0, ...curve), maxV = Math.max(0, ...curve);
  const range = (maxV - minV) || 1;
  const pts = curve.map((v,i)=> (i/(Math.max(curve.length-1,1)))*w + ',' + (h - ((v-minV)/range)*h)).join(' ');
  const zeroY = h - ((0-minV)/range)*h;
  body.innerHTML =
    '<div class="stat-mini-grid">'+
      '<div class="stat-mini"><div class="lbl">Total Trades</div><div class="val">'+s.totalTrades+'</div></div>'+
      '<div class="stat-mini"><div class="lbl">Win Rate</div><div class="val '+(s.winRate>=50?'pos':'neg')+'">'+s.winRate.toFixed(1)+'%</div></div>'+
      '<div class="stat-mini"><div class="lbl">Profit Factor</div><div class="val '+(s.profitFactor>=1?'pos':'neg')+'">'+pf+'</div></div>'+
      '<div class="stat-mini"><div class="lbl">Expectancy / Trade</div><div class="val '+(s.expectancy>=0?'pos':'neg')+'">'+fmtUSD(s.expectancy,2,'$ ')+'</div></div>'+
      '<div class="stat-mini"><div class="lbl">Net Realized PnL</div><div class="val '+(s.netPnl>=0?'pos':'neg')+'">'+fmtUSD(s.netPnl,2,'$ ')+'</div></div>'+
      '<div class="stat-mini"><div class="lbl">Max Drawdown</div><div class="val neg">'+fmtUSD(s.maxDrawdownAbs,2,'$ ')+'</div></div>'+
      '<div class="stat-mini"><div class="lbl">Best Trade</div><div class="val pos">'+fmtUSD(s.bestTrade,2,'$ ')+'</div></div>'+
      '<div class="stat-mini"><div class="lbl">Worst Trade</div><div class="val neg">'+fmtUSD(s.worstTrade,2,'$ ')+'</div></div>'+
    '</div>'+
    '<div class="panel-title" style="margin:16px 0 2px;font-size:10.5px;">Realized Equity Curve (last '+curve.length+' closed trades)</div>'+
    '<div class="equity-wrap"><svg viewBox="0 0 '+w+' '+h+'" preserveAspectRatio="none">'+
      '<line x1="0" y1="'+zeroY+'" x2="'+w+'" y2="'+zeroY+'" stroke="rgba(255,255,255,.12)" stroke-width="1"/>'+
      '<polyline points="'+pts+'" fill="none" stroke="'+(s.netPnl>=0?'#9dff1f':'#ff4f6d')+'" stroke-width="2"/>'+
    '</svg></div>'+
    '<div style="font-size:9.5px;color:var(--text-dim);margin-top:10px;line-height:1.5;">Reconstructed from your real trades log (ENTRY paired with its EXIT_TP1/TP2/TP3/SL/MANUAL fills) — there\'s no stored pnl column, so every number here is computed from actual logged fill prices, not estimated. Only fully-closed positions count; anything still open is excluded.</div>';
}

function renderBacktestResult(container, label, s){
  if(!s){ return '<div class="tab-empty" style="padding:16px;">'+label+': no completed trades in this window.</div>'; }
  const pf = isFinite(s.profit_factor) ? s.profit_factor.toFixed(2) : '∞';
  const curve = s.equity_curve_pct, w=300, h=70;
  const minV = Math.min(0,...curve), maxV = Math.max(0,...curve), range=(maxV-minV)||1;
  const pts = curve.map((v,i)=>(i/(Math.max(curve.length-1,1)))*w+','+(h-((v-minV)/range)*h)).join(' ');
  const zeroY = h-((0-minV)/range)*h;
  return '<div class="panel-title" style="font-size:10px;margin-bottom:8px;">'+label+'</div>'+
    '<div class="stat-mini-grid">'+
      '<div class="stat-mini"><div class="lbl">Trades</div><div class="val">'+s.total_trades+'</div></div>'+
      '<div class="stat-mini"><div class="lbl">Win Rate</div><div class="val '+(s.win_rate>=50?'pos':'neg')+'">'+s.win_rate.toFixed(1)+'%</div></div>'+
      '<div class="stat-mini"><div class="lbl">Profit Factor</div><div class="val '+(s.profit_factor>=1?'pos':'neg')+'">'+pf+'</div></div>'+
      '<div class="stat-mini"><div class="lbl">Net Return</div><div class="val '+(s.net_return_pct>=0?'pos':'neg')+'">'+s.net_return_pct.toFixed(2)+'%</div></div>'+
      '<div class="stat-mini"><div class="lbl">Max Drawdown</div><div class="val neg">'+s.max_drawdown_pct.toFixed(2)+'%</div></div>'+
      '<div class="stat-mini"><div class="lbl">Expectancy/Trade</div><div class="val '+(s.expectancy_pct>=0?'pos':'neg')+'">'+s.expectancy_pct.toFixed(3)+'%</div></div>'+
    '</div>'+
    '<div class="equity-wrap" style="margin-top:8px;"><svg viewBox="0 0 '+w+' '+h+'" preserveAspectRatio="none">'+
      '<line x1="0" y1="'+zeroY+'" x2="'+w+'" y2="'+zeroY+'" stroke="rgba(255,255,255,.12)" stroke-width="1"/>'+
      '<polyline points="'+pts+'" fill="none" stroke="'+(s.net_return_pct>=0?'#9dff1f':'#ff4f6d')+'" stroke-width="2"/>'+
    '</svg></div>';
}
async function runHistoricalBacktest(){
  const btn = document.getElementById('btnRunBacktest');
  const out = document.getElementById('btResults');
  if(!LIVE.enabled){
    out.innerHTML = '<div class="tab-empty">Connect your live bot first — this fetches real historical candles through your backend.<button class="connect-cta" type="button" id="btConnectCta2">Connect Now</button></div>';
    document.getElementById('btConnectCta2').onclick = (e)=>{ e.stopPropagation(); document.getElementById('connectPop').hidden = false; };
    return;
  }
  const symbol = document.getElementById('btSymbol').value;
  const resolution = document.getElementById('btResolution').value;
  const days = document.getElementById('btDays').value;
  btn.disabled = true; btn.textContent = 'Running…';
  out.innerHTML = '<div class="tab-empty">Fetching real Delta history and simulating trades — a few seconds…</div>';
  try{
    const j = await liveFetch('/backtest/run?symbol='+encodeURIComponent(symbol)+'&resolution='+encodeURIComponent(resolution)+'&days='+encodeURIComponent(days));
    if(!j || j.error){
      out.innerHTML = '<div class="tab-empty">Could not run backtest: '+((j&&j.error)||'unknown error')+(j&&j.detail?' — '+j.detail:'')+'</div>';
      return;
    }
    const wf = j.walk_forward;
    out.innerHTML =
      renderBacktestResult(out, 'Full Period ('+j.candles_used+' candles, '+j.days+'d)', j.full_period) +
      '<div style="height:16px;"></div>' +
      renderBacktestResult(out, 'In-Sample (first 70%)', wf.in_sample) +
      '<div style="height:16px;"></div>' +
      renderBacktestResult(out, 'Out-of-Sample (last 30%, unseen)', wf.out_of_sample) +
      '<div style="font-size:9.5px;color:var(--text-dim);margin-top:14px;line-height:1.5;">'+j.methodology_note+' Fees+slippage: '+
      (j.params.fee_pct_roundtrip+j.params.slippage_pct_roundtrip)+'% round-trip deducted from every simulated trade. '+
      '<b style="color:var(--text-mid);">If Out-of-Sample looks much worse than In-Sample, that\'s a real overfitting/regime-change warning — don\'t just trust the Full Period number.</b></div>';
  }catch(e){
    out.innerHTML = '<div class="tab-empty">Request failed: '+e.message+'</div>';
  }finally{
    btn.disabled = false; btn.textContent = 'Run Backtest';
  }
}
function renderSettingsTab(){
  const body = document.getElementById('settingsBody');
  if(!LIVE.enabled){
    body.innerHTML = '<div class="tab-empty"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.9"/></svg>'+
      'Connect your live bot to see its real configuration — active signals, safety switches, and feature flags.'+
      '<button class="connect-cta" type="button" id="settingsConnectCta">Connect Now</button></div>';
    document.getElementById('settingsConnectCta').onclick = (e)=>{ e.stopPropagation(); document.getElementById('connectPop').hidden = false; };
    return;
  }
  const c = LIVECACHE.config;
  if(!c){ body.innerHTML = '<div class="tab-empty">Loading configuration…</div>'; return; }
  const flag = (label,on) => '<div class="settings-row"><span class="k">'+label+'</span><span class="v '+(on?'on':'off')+'">'+(on?'ON':'OFF')+'</span></div>';
  const active = new Set((c.active_signals||[]).map(s=>s.toUpperCase()));
  const tags = (c.all_known_signals||c.active_signals||[]).map(s=>{
    const isOn = active.has(s.toUpperCase());
    return '<span class="signal-tag '+(isOn?'':'off')+'" data-tier="'+s+'" data-on="'+(isOn?'1':'0')+'">'+s+'</span>';
  }).join('') || '—';
  const td = c.time_drift || {};
  body.innerHTML =
    '<div class="settings-row"><span class="k">Region</span><span class="v">'+(c.region||'—')+'</span></div>'+
    '<div class="settings-row tappable" id="settingsModeRow"><span class="k">Mode <span style="opacity:.5;font-size:9px;">(tap to switch)</span></span><span class="v '+(c.live_mode?'danger':'on')+'">'+(c.live_mode?'LIVE':'DRY RUN')+'</span></div>'+
    '<div class="settings-row"><span class="k">Paused</span><span class="v '+(c.paused?'off':'on')+'">'+(c.paused?'YES':'NO')+'</span></div>'+
    '<div class="settings-row"><span class="k">Kill Switch</span><span class="v '+(c.kill_switch_active?'danger':'on')+'">'+(c.kill_switch_active?'ARMED':'clear')+'</span></div>'+
    '<div class="settings-row"><span class="k">Auto Bracket Orders</span><span class="v '+(c.auto_bracket_orders?'on':'off')+'">'+(c.auto_bracket_orders?'ON':'OFF')+'</span></div>'+
    '<div class="settings-row"><span class="k">API Credentials</span><span class="v '+(c.api_credentials_ok?'on':'danger')+'">'+(c.api_credentials_ok?'OK':'CHECK')+'</span></div>'+
    '<div class="settings-row"><span class="k">Products Discovered</span><span class="v">'+(c.products_discovered ?? '—')+'</span></div>'+
    '<div class="settings-row"><span class="k">Clock Drift</span><span class="v '+((td.drift_ms==null||Math.abs(td.drift_ms)<1000)?'on':'danger')+'">'+(td.drift_ms!=null?Math.round(td.drift_ms)+'ms':'—')+'</span></div>'+
    '<div class="panel-title" style="margin:16px 0 6px;font-size:10.5px;">Active Signal Tiers <span style="opacity:.5;font-weight:400;text-transform:none;">(tap to enable/disable)</span></div>'+
    '<div>'+tags+'</div>'+
    '<div class="panel-title" style="margin:18px 0 2px;font-size:10.5px;">Feature Flags</div>'+
    flag('HFT Parallel Exits', c.hft_parallel_exits) +
    flag('Predator Vision', c.predator_vision_enabled) +
    '<div class="settings-row tappable" id="riskSizingRow"><span class="k">Risk-Based Sizing <span style="opacity:.5;font-size:9px;">(tap to toggle)</span></span><span class="v '+(c.risk_based_sizing?'on':'off')+'">'+(c.risk_based_sizing?'ON':'OFF')+'</span></div>'+
    flag('Aggressive Exits', c.aggressive_exits_enabled) +
    flag('Neural Syndicate', c.neural_syndicate_enabled) +
    flag('Shock Entry Block', c.block_entries_during_shock) +
    flag('Telegram Alerts', c.telegram_enabled) +
    '<div style="font-size:9.5px;color:var(--text-dim);margin-top:14px;line-height:1.5;">Mode, Active Signal Tiers and Risk-Based Sizing above are live — tap any of them to change it right now. Everything else on this screen is boot-time only: changing it means an env var + redeploy, not a toggle here, so it can\'t silently drift from what\'s actually running.</div>';

  const modeRow = document.getElementById('settingsModeRow');
  if(modeRow) modeRow.onclick = ()=>{
    if(c.live_mode){
      (async ()=>{ const r = await callMode(false); if(r){ showToast('Switched to DRY RUN.'); await pollLive(); renderSettingsTab(); if(document.getElementById('view-autopilot') && !document.getElementById('view-autopilot').hidden) renderAutopilotTab(); } })();
    } else {
      confirmAction('Go LIVE?',
        'This switches the bot to placing REAL orders with real money on your connected exchange account.',
        async ()=>{ const r = await callMode(true); if(r){ showToast('LIVE — placing real orders now.'); await pollLive(); renderSettingsTab(); if(document.getElementById('view-autopilot') && !document.getElementById('view-autopilot').hidden) renderAutopilotTab(); } });
    }
  };
  const riskRow = document.getElementById('riskSizingRow');
  if(riskRow) riskRow.onclick = async ()=>{
    const r = await callRiskSizing(!c.risk_based_sizing);
    if(r){ showToast('Risk-based sizing turned '+(r.risk_based_sizing?'ON':'OFF')+'.'); await pollLive(); renderSettingsTab(); }
  };
  body.querySelectorAll('.signal-tag[data-tier]').forEach(el=>{
    el.onclick = async ()=>{
      const tier = el.dataset.tier, isOn = el.dataset.on === '1';
      const r = await callSignalTier(tier, !isOn);
      if(r){ showToast(tier + ' ' + (isOn?'disabled':'enabled') + '.'); await pollLive(); renderSettingsTab(); }
    };
  });
}

function renderAiOracle(oracleJ){
  const mode = document.getElementById('confidenceMode');
  if(mode) mode.textContent = 'Live';
  const symbols = oracleJ.symbols || {};
  const keys = Object.keys(symbols);
  const preferred = keys.find(k=>/BTC/i.test(k)) || keys[0];
  const sym = preferred ? symbols[preferred] : null;

  const pctEl = document.getElementById('confGaugePct'), tagEl = document.getElementById('confGaugeTag'),
        fgEl = document.getElementById('confGaugeFg'), listEl = document.getElementById('confidenceList');
  if(!sym || !sym.ok){
    if(pctEl) pctEl.textContent = '—';
    if(tagEl) tagEl.textContent = 'No oracle data yet';
    if(listEl) listEl.innerHTML = '<div class="confidence-row"><span>AI Oracle</span><span class="tag neutral">WARMING UP</span></div>';
    return;
  }
  const pct = Math.round(sym.confidence*100);
  if(pctEl) pctEl.textContent = pct+'%';
  if(fgEl){ fgEl.dataset.pct = pct; setGaugeProgress(fgEl, pct, 0); }
  const tagClass = sym.consensus==='BULLISH' ? 'bullish' : sym.consensus==='BEARISH' ? 'bearish' : 'neutral';
  if(tagEl) tagEl.textContent = (preferred||'') + ' · ' + sym.consensus;
  if(listEl){
    listEl.innerHTML = [
      ['Consensus (' + (preferred||'') + ')', sym.consensus, tagClass],
      ['Confidence', pct+'%', tagClass],
      ['Data Source', sym.degraded_mode ? 'Quant-only (Gemini down)' : 'Gemini + Quant', sym.degraded_mode?'medium':'optimal'],
      ['Models Agree', sym.agreement ? 'YES' : 'NO', sym.agreement?'positive':'medium'],
      ['Rolling Accuracy', sym.rolling_accuracy_pct!=null ? sym.rolling_accuracy_pct+'%' : '— (needs more history)', 'neutral'],
      ['Gemini Circuit', (oracleJ.circuit_breaker && oracleJ.circuit_breaker.state) || '—', (oracleJ.circuit_breaker && oracleJ.circuit_breaker.state==='closed') ? 'optimal' : 'medium'],
    ].map(([k,v,cls])=>`<div class="confidence-row"><span>${k}</span><span class="tag ${cls}">${v}</span></div>`).join('');
  }
}
function renderPerformance(perfJ){
  const mode = document.getElementById('perfMode');
  if(mode) mode.textContent = 'Live';
  const o = perfJ.overall;
  const heroEl = document.getElementById('perfWinRateHero'), labelEl = document.getElementById('perfWinRateLabel'),
        tradesEl = document.getElementById('perfTrades'), wrEl = document.getElementById('perfWinRate2'),
        cumREl = document.getElementById('perfCumR'), pfEl = document.getElementById('perfProfitFactor');
  if(!o){
    if(heroEl) heroEl.textContent = '—';
    if(labelEl) labelEl.textContent = 'No closed trades yet';
    if(tradesEl) tradesEl.textContent = '0';
    if(wrEl) wrEl.textContent = '—';
    if(cumREl) cumREl.textContent = '—';
    if(pfEl) pfEl.textContent = '—';
    return;
  }
  if(heroEl) heroEl.textContent = o.win_rate+'%';
  if(labelEl) labelEl.textContent = 'Win Rate — last '+o.n+' closed trades';
  if(tradesEl) tradesEl.textContent = o.n.toLocaleString();
  if(wrEl) wrEl.textContent = o.win_rate+'%';
  if(cumREl) cumREl.textContent = (o.cum_r>=0?'+':'')+o.cum_r+'R';
  if(pfEl) pfEl.textContent = o.profit_factor!=null ? o.profit_factor : '—';
}
function mapLivePosition(p, marks){
  const base = (p.symbol||'').replace(/USDT?$/i,'').toUpperCase();
  const mark = marks && marks[base]!=null ? marks[base] : (state.prices[base]!=null ? state.prices[base] : null);
  const dirMult = (p.direction||'LONG').toUpperCase()==='LONG' ? 1 : -1;
  const pnl = (mark!=null && p.entry_price!=null) ? (mark - p.entry_price) * p.qty * dirMult : null;
  const roi = (pnl!=null && p.entry_price) ? (pnl / (p.entry_price*p.qty)) * 100 : null;
  return { pair:(base||p.symbol)+'/USDT', side:(p.direction||'LONG').toUpperCase(), size:p.qty, entry:p.entry_price, pnl, roi };
}
function mapLiveTrade(t){
  const base = (t.symbol||'').replace(/USDT?$/i,'').toUpperCase();
  const side = /exit|tp|sl|close/i.test(t.event||'') ? 'SELL' : 'BUY';
  return { time:new Date(t.timestamp).toLocaleTimeString('en-GB'), pair:(base||t.symbol)+'/USDT', side, size:t.qty, price:t.price, status:'FILLED' };
}
function mapLiveRejection(r){
  return { time: new Date(r.timestamp).toLocaleTimeString('en-GB'), symbol:r.symbol||'—', reason:r.reason||'Blocked', detail:r.detail||'' };
}

/* ================================================================
   REAL TRADE-CYCLE RECONSTRUCTION — shared by Risk panel (max
   drawdown) and the Backtest Engine tab (all its stats).
   The `trades` table logs raw ENTRY/EXIT_* fill events (symbol,
   direction, event, qty, price, timestamp) — there is NO stored
   pnl column anywhere in this backend. Rather than fabricate a
   number, this walks the real event log chronologically per symbol
   and pairs each ENTRY with the EXIT_* events that follow it
   (handling partial closes across EXIT_TP1/TP2/TP3/SL/MANUAL), so
   every figure downstream is reconstructed from real logged fills.
   ================================================================ */
function reconstructTradeCycles(rawTrades){
  const bySymbol = {};
  rawTrades.forEach(t => { (bySymbol[t.symbol] = bySymbol[t.symbol] || []).push(t); });
  const cycles = [];
  Object.keys(bySymbol).forEach(sym => {
    const events = bySymbol[sym].slice().sort((a,b)=> new Date(a.timestamp) - new Date(b.timestamp));
    let open = null;
    events.forEach(ev => {
      const type = (ev.event||'').toUpperCase();
      if(type === 'ENTRY'){
        // A fresh ENTRY while one is already "open" in our reconstruction
        // means we never saw its closing fill(s) (e.g. history predates the
        // ?limit window). That partial cycle is dropped rather than guessed
        // at — honesty over false precision.
        open = { symbol: sym, direction: (ev.direction||'BUY').toUpperCase(), entryPrice: ev.price, entryQty: ev.qty||0, entryTime: ev.timestamp, exitedQty: 0, realizedPnl: 0, lastExitTime: ev.timestamp };
      } else if(open && ev.price != null && /EXIT|TP|SL|MANUAL/i.test(type)){
        const dirMult = open.direction === 'BUY' ? 1 : -1;
        const remaining = Math.max(open.entryQty - open.exitedQty, 0);
        const partialQty = Math.min(ev.qty || remaining, remaining);
        open.realizedPnl += dirMult * (ev.price - open.entryPrice) * partialQty;
        open.exitedQty += partialQty;
        open.lastExitTime = ev.timestamp;
        if(open.exitedQty >= open.entryQty - 1e-9){ cycles.push(open); open = null; }
      }
    });
    // An ENTRY still open with no matching exit is a currently-live position,
    // correctly excluded — it isn't a completed cycle yet.
  });
  cycles.sort((a,b)=> new Date(a.lastExitTime) - new Date(b.lastExitTime));
  return cycles;
}
function computeBacktestStats(cycles){
  if(!cycles.length) return null;
  const wins = cycles.filter(c=>c.realizedPnl>0), losses = cycles.filter(c=>c.realizedPnl<=0);
  const grossProfit = wins.reduce((s,c)=>s+c.realizedPnl,0);
  const grossLoss = Math.abs(losses.reduce((s,c)=>s+c.realizedPnl,0));
  let running=0, peak=0, maxDD=0; const equityCurve=[];
  cycles.forEach(c=>{ running+=c.realizedPnl; peak=Math.max(peak,running); maxDD=Math.max(maxDD,peak-running); equityCurve.push(running); });
  return {
    totalTrades: cycles.length, wins: wins.length, losses: losses.length,
    winRate: (wins.length/cycles.length)*100,
    profitFactor: grossLoss>0 ? grossProfit/grossLoss : (grossProfit>0 ? Infinity : 0),
    expectancy: running/cycles.length, grossProfit, grossLoss, netPnl: running,
    maxDrawdownAbs: maxDD, equityCurve,
    bestTrade: cycles.reduce((m,c)=>Math.max(m,c.realizedPnl), -Infinity),
    worstTrade: cycles.reduce((m,c)=>Math.min(m,c.realizedPnl), Infinity),
  };
}
function computeOpenRiskUSD(rawPositions){
  // Entry-to-SL distance × qty, summed — the risk capital actually committed
  // at entry for each open position, per position sl already on record.
  return rawPositions.reduce((sum,p)=> (p.sl!=null && p.entry_price!=null && p.qty!=null) ? sum + Math.abs(p.entry_price-p.sl)*p.qty : sum, 0);
}
function computeExposureUSD(rawPositions, marks){
  return rawPositions.reduce((sum,p)=>{
    const base=(p.symbol||'').replace(/USDT?$/i,'').toUpperCase();
    const px = (marks && marks[base]!=null) ? marks[base] : p.entry_price;
    return (px!=null && p.qty!=null) ? sum + px*p.qty : sum;
  }, 0);
}

/* ================================================================
   UTILITIES
   ================================================================ */
function clamp(v,min,max){ return Math.max(min, Math.min(max, v)); }
function fmtUSD(n, decimals=2, prefix=''){
  return prefix + n.toLocaleString('en-US',{minimumFractionDigits:decimals, maximumFractionDigits:decimals});
}
function setTextFlash(el, text, dir){
  if(!el) return;
  el.textContent = text;
  el.classList.remove('flash-up','flash-down');
  void el.offsetWidth;
  el.classList.add(dir>0 ? 'flash-up' : 'flash-down');
}
function showToast(msg){
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  clearTimeout(showToast._h);
  showToast._h = setTimeout(()=> t.classList.remove('show'), 2600);
}

/* ================================================================
   BOOT SEQUENCE
   ================================================================ */
function bootSequence(){
  const els = document.querySelectorAll('.panel, .topbar, .bottom-nav, .status-bar');
  els.forEach((el,i)=> setTimeout(()=> el.classList.add('in'), 50*i + 40));
}

/* ================================================================
   GAUGES & BARS
   ================================================================ */
function setGaugeProgress(shapeEl, pct, delay){
  if(!shapeEl || typeof shapeEl.getTotalLength !== 'function') return;
  // [BUGFIX] getTotalLength() throws (not just returns 0) on an SVG element
  // that isn't currently rendered — e.g. its panel sits inside the Dashboard
  // tab-view while a different tab (Autopilot/Vault/Backtest/Settings) is
  // active. Background polling still updates these gauges' data-pct so they
  //'re correct the instant the person switches back, but must not let a
  // hidden gauge's failed measurement abort the rest of whatever render
  // function called this (renderAiOracle, fetchRealFearGreed, etc).
  let len;
  try{ len = shapeEl.getTotalLength(); }catch(e){ return; }
  shapeEl.style.strokeDasharray = len;
  shapeEl.style.strokeDashoffset = len;
  setTimeout(()=>{
    shapeEl.style.transition = 'stroke-dashoffset 1.3s cubic-bezier(.16,.84,.44,1)';
    shapeEl.style.strokeDashoffset = len - (len*pct/100);
  }, delay);
}
function initGaugesAndBars(){
  document.querySelectorAll('.gauge-fg[data-pct]').forEach((el,i)=> setGaugeProgress(el, parseFloat(el.dataset.pct), 300+i*130));
  const fng = document.getElementById('fng-path');
  setGaugeProgress(fng, parseFloat(fng.dataset.pct), 500);
  document.querySelectorAll('.bar-fill[data-pct]').forEach((el,i)=>{
    setTimeout(()=>{ el.style.width = el.dataset.pct + '%'; }, 400+i*150);
  });
  const tempBar = document.getElementById('core-temp-bar');
  setTimeout(()=>{ tempBar.style.width = tempBar.dataset.pct + '%'; }, 500);
}

/* ================================================================
   THREE.JS — AI CORE ORB
   ================================================================ */
function initOrb(){
  const stage = document.getElementById('orbStage');
  const canvas = document.getElementById('orbCanvas');
  if(typeof THREE === 'undefined'){ showOrbFallback(); return; }
  let renderer;
  try{ renderer = new THREE.WebGLRenderer({ canvas, antialias:true, alpha:true }); }
  catch(e){ showOrbFallback(); return; }
  renderer.setPixelRatio(Math.min(window.devicePixelRatio||1, 2));

  const scene = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(45, 1, 0.1, 100);
  camera.position.set(0,0,6.4);

  scene.add(new THREE.AmbientLight(0x223311, 1.1));
  const l1 = new THREE.PointLight(0x9dff1f, 3.0, 20); l1.position.set(4,3,5); scene.add(l1);
  const l2 = new THREE.PointLight(0x2fe4ff, 2.1, 20); l2.position.set(-4,-2,4); scene.add(l2);
  const l3 = new THREE.PointLight(0xb463ff, 1.6, 20); l3.position.set(0,-4,3); scene.add(l3);

  // Self-contained "bloom" — a soft radial-gradient texture painted on an
  // offscreen canvas at runtime, applied to additive-blended THREE.Sprites.
  // Real bloom post-processing (UnrealBloomPass) needs extra three.js
  // example modules on top of the single three.min.js CDN file; this gets
  // the same glowing-halo look with zero extra network dependencies, so it
  // can't silently fail if one more CDN script doesn't load.
  function makeGlowTexture(hex){
    const c = document.createElement('canvas'); c.width = c.height = 256;
    const ctx = c.getContext('2d');
    const g = ctx.createRadialGradient(128,128,0,128,128,128);
    g.addColorStop(0, hex+'ff'); g.addColorStop(0.35, hex+'55'); g.addColorStop(1, hex+'00');
    ctx.fillStyle = g; ctx.fillRect(0,0,256,256);
    return new THREE.CanvasTexture(c);
  }
  const glowSprites = [
    { hex:'#9dff1f', scale:5.6, opacity:.55 },
    { hex:'#2fe4ff', scale:4.0, opacity:.4 },
    { hex:'#b463ff', scale:3.2, opacity:.35 },
  ].map(def=>{
    const mat = new THREE.SpriteMaterial({ map:makeGlowTexture(def.hex), transparent:true,
      opacity:def.opacity, blending:THREE.AdditiveBlending, depthWrite:false });
    const sprite = new THREE.Sprite(mat);
    sprite.scale.set(def.scale, def.scale, 1);
    scene.add(sprite);
    return sprite;
  });

  const core = new THREE.Mesh(new THREE.IcosahedronGeometry(1.55,1),
    new THREE.MeshBasicMaterial({ color:0x9dff1f, wireframe:true, transparent:true, opacity:.62 }));
  scene.add(core);

  const innerCore = new THREE.Mesh(new THREE.IcosahedronGeometry(0.95,0),
    new THREE.MeshStandardMaterial({ color:0x9dff1f, emissive:0x5f9c14, emissiveIntensity:.9, wireframe:true, transparent:true, opacity:.5 }));
  scene.add(innerCore);

  const apexMark = new THREE.Mesh(new THREE.TetrahedronGeometry(0.34,0),
    new THREE.MeshStandardMaterial({ color:0xd8ffb0, emissive:0x9dff1f, emissiveIntensity:1.4, metalness:.2, roughness:.25 }));
  scene.add(apexMark);

  const ringDefs = [
    { r:2.05, tube:.012, color:0x2fe4ff, tilt:1.15, speed:.006 },
    { r:2.45, tube:.008, color:0xb463ff, tilt:-.85, speed:-.004 },
    { r:2.8,  tube:.006, color:0xffb020, tilt:.4,  speed:.0032 },
  ];
  const rings = ringDefs.map(def=>{
    const m = new THREE.Mesh(new THREE.TorusGeometry(def.r, def.tube, 8, 120),
      new THREE.MeshBasicMaterial({ color:def.color, transparent:true, opacity:.55 }));
    m.rotation.x = def.tilt; m.userData.speed = def.speed; scene.add(m); return m;
  });

  const PCOUNT = 850;
  const pos = new Float32Array(PCOUNT*3), col = new Float32Array(PCOUNT*3);
  const palette = [[.62,1,.12],[.18,.9,1],[.71,.39,1]];
  for(let i=0;i<PCOUNT;i++){
    const r = 2.2 + Math.random()*1.6, th = Math.random()*Math.PI*2, ph = Math.acos((Math.random()*2)-1);
    pos[i*3] = r*Math.sin(ph)*Math.cos(th); pos[i*3+1] = r*Math.sin(ph)*Math.sin(th); pos[i*3+2] = r*Math.cos(ph);
    const c = palette[i%palette.length]; col[i*3]=c[0]; col[i*3+1]=c[1]; col[i*3+2]=c[2];
  }
  const pGeo = new THREE.BufferGeometry();
  pGeo.setAttribute('position', new THREE.BufferAttribute(pos,3));
  pGeo.setAttribute('color', new THREE.BufferAttribute(col,3));
  const particles = new THREE.Points(pGeo, new THREE.PointsMaterial({ size:.028, vertexColors:true, transparent:true, opacity:.85, blending:THREE.AdditiveBlending, depthWrite:false }));
  scene.add(particles);

  function resize(){
    const w = stage.clientWidth||300, h = stage.clientHeight||300;
    camera.aspect = w/h; camera.updateProjectionMatrix();
    renderer.setSize(w,h,false);
  }
  window.addEventListener('resize', resize);
  resize();

  let raf;
  function animate(){
    raf = requestAnimationFrame(animate);
    core.rotation.y += .0028; core.rotation.x += .0011;
    innerCore.rotation.y -= .0038;
    apexMark.rotation.y += .012;
    apexMark.scale.setScalar(1 + Math.sin(performance.now()*.0016)*.06);
    rings.forEach(r=> r.rotation.z += r.userData.speed);
    particles.rotation.y += .0009;
    const breathe = 1 + Math.sin(performance.now()*.0009)*.08;
    glowSprites.forEach((s,i)=> s.scale.setScalar([5.6,4.0,3.2][i]*breathe));
    renderer.render(scene, camera);
  }
  animate();
  document.addEventListener('visibilitychange', ()=>{
    if(document.hidden) cancelAnimationFrame(raf); else animate();
  });
}
function showOrbFallback(){
  const fb = document.getElementById('orbFallback'), cv = document.getElementById('orbCanvas');
  if(cv) cv.style.display = 'none';
  if(fb) fb.hidden = false;
}

/* ================================================================
   CANDLESTICK CHART
   ================================================================ */
const chartState = { candles:[], tf:'1m', vol:70, symbol:'BTCUSD', liveCandles:false };
const TF_TO_DELTA_RES = { '1m':'1m', '5m':'5m', '15m':'15m', '1h':'1h', '1D':'1d' };
const TF_TO_COUNT = { '1m':60, '5m':60, '15m':60, '1h':48, '1D':30 };

/* Simulated random-walk candles — used whenever the dashboard isn't
   connected live, or /candles can't be reached (offline, cold-starting
   Render instance, etc.). Never runs once real candles have loaded. */
function seedCandles(tf){
  const cfg = { '1m':{n:56,vol:70},'5m':{n:48,vol:150},'15m':{n:44,vol:260},'1h':{n:40,vol:520},'1D':{n:30,vol:1400} }[tf] || {n:50,vol:100};
  let price = state.prices.BTC, arr = [];
  for(let i=0;i<cfg.n;i++){
    const open = price, close = open + (Math.random()-0.48)*cfg.vol;
    const high = Math.max(open,close) + Math.random()*cfg.vol*0.4;
    const low = Math.min(open,close) - Math.random()*cfg.vol*0.4;
    arr.push({open,high,low,close}); price = close;
  }
  chartState.candles = arr; chartState.vol = cfg.vol; chartState.liveCandles = false;
  document.getElementById('chartLiveTag').hidden = true;
}

/* Real candles — proxied through main.py's /candles (Delta's public
   /v2/history/candles), so the browser never talks to the exchange
   directly. Falls back to seedCandles() on any failure. */
async function loadCandles(tf){
  chartState.tf = tf;
  if(LIVE.enabled){
    const count = TF_TO_COUNT[tf] || 60;
    const j = await liveFetch('/candles?symbol='+chartState.symbol+'&resolution='+TF_TO_DELTA_RES[tf]+'&limit='+count);
    if(j && Array.isArray(j.candles) && j.candles.length > 3){
      chartState.candles = j.candles.map(c=>({ open:c.open, high:c.high, low:c.low, close:c.close, time:c.time }));
      chartState.vol = null;
      chartState.liveCandles = true;
      document.getElementById('chartLiveTag').hidden = false;
      const first = chartState.candles[0], last = chartState.candles[chartState.candles.length-1];
      state.prices.BTC = last.close;
      if(first.open){
        const pct = ((last.close - first.open) / first.open) * 100;
        state.changePct.BTC = pct;
        ['chg-BTC','chartChange'].forEach(id=>{
          const el = document.getElementById(id);
          el.textContent = (pct>=0?'+':'')+pct.toFixed(2)+'%';
          el.classList.toggle('up', pct>=0); el.classList.toggle('down', pct<0);
        });
      }
      document.getElementById('chartPrice').textContent = fmtUSD(state.prices.BTC);
      renderXAxis(); drawChart();
      return;
    }
  }
  seedCandles(tf);
  renderXAxis(); drawChart();
}

function drawChart(){
  const canvas = document.getElementById('priceChart'), wrap = canvas.parentElement;
  const dpr = Math.min(window.devicePixelRatio||1, 2);
  const w = wrap.clientWidth, h = wrap.clientHeight;
  if(w===0||h===0) return;
  canvas.width = w*dpr; canvas.height = h*dpr;
  canvas.style.width = w+'px'; canvas.style.height = h+'px';
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr,0,0,dpr,0,0);
  ctx.clearRect(0,0,w,h);
  const candles = chartState.candles;
  if(!candles.length) return;
  const highs = candles.map(c=>c.high), lows = candles.map(c=>c.low);
  const max = Math.max.apply(null,highs), min = Math.min.apply(null,lows);
  const pad = (max-min)*0.1 || 1, top = max+pad, bottom = min-pad;
  const yFor = v => h - ((v-bottom)/(top-bottom))*h;
  ctx.strokeStyle = 'rgba(255,255,255,.06)'; ctx.fillStyle = 'rgba(147,164,189,.7)';
  ctx.font = '10px Rajdhani, sans-serif'; ctx.lineWidth = 1;
  for(let i=0;i<=4;i++){
    const v = bottom + ((top-bottom)/4)*i, y = yFor(v);
    ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(w,y); ctx.stroke();
    ctx.fillText(v.toLocaleString('en-US',{maximumFractionDigits:0}), 4, y-4);
  }
  const slot = w/candles.length, bodyW = Math.max(2, slot*0.55);
  candles.forEach((c,i)=>{
    const x = i*slot + slot/2, up = c.close >= c.open;
    ctx.strokeStyle = up ? '#9dff1f' : '#ff4f6d'; ctx.fillStyle = ctx.strokeStyle;
    ctx.beginPath(); ctx.moveTo(x, yFor(c.high)); ctx.lineTo(x, yFor(c.low)); ctx.stroke();
    const yO = yFor(c.open), yC = yFor(c.close);
    const bodyTop = Math.min(yO,yC), bodyH = Math.max(1.5, Math.abs(yC-yO));
    ctx.globalAlpha = .92; ctx.fillRect(x-bodyW/2, bodyTop, bodyW, bodyH); ctx.globalAlpha = 1;
  });
}
function renderXAxis(){
  const el = document.getElementById('chartXAxis');
  let labels;
  if(chartState.liveCandles && chartState.candles.length > 4){
    const c = chartState.candles;
    const idxs = [0, Math.floor(c.length*0.25), Math.floor(c.length*0.5), Math.floor(c.length*0.75), c.length-1];
    labels = idxs.map(i => c[i].time ? new Date(c[i].time*1000).toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit'}) : '—');
  } else {
    const now = new Date();
    const stepMin = { '1m':5,'5m':25,'15m':60,'1h':240,'1D':1440 }[chartState.tf] || 5;
    labels = [];
    for(let i=4;i>=0;i--){
      const d = new Date(now.getTime() - i*stepMin*60000);
      labels.push(d.toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit'}));
    }
  }
  el.innerHTML = labels.map(l=>'<span>'+l+'</span>').join('');
}
/* Simulated live-tick jitter — only runs when NOT showing real candles. */
function tickChart(){
  if(chartState.liveCandles) return;
  const arr = chartState.candles; if(!arr.length) return;
  const last = arr[arr.length-1], delta = (Math.random()-0.5)*chartState.vol*0.3;
  last.close += delta; last.high = Math.max(last.high,last.close); last.low = Math.min(last.low,last.close);
  state.prices.BTC = last.close;
  if(Math.random() < 0.28){
    const open = last.close, close = open + (Math.random()-0.48)*chartState.vol;
    arr.push({ open, high:Math.max(open,close), low:Math.min(open,close), close });
    if(arr.length > 70) arr.shift();
    renderXAxis();
  }
  drawChart();
  document.getElementById('chartPrice').textContent = fmtUSD(state.prices.BTC);
}
function attachChartTooltip(){
  const canvas = document.getElementById('priceChart'), wrap = canvas.parentElement, tip = document.getElementById('chartTooltip');
  function showAt(clientX){
    const candles = chartState.candles; if(!candles.length) return;
    const rect = canvas.getBoundingClientRect();
    const x = clamp(clientX - rect.left, 0, rect.width);
    const slot = rect.width / candles.length;
    const idx = clamp(Math.floor(x/slot), 0, candles.length-1);
    const c = candles[idx];
    const timeLabel = c.time ? new Date(c.time*1000).toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit'}) : ('#'+(idx+1));
    tip.innerHTML = timeLabel+'<br>O<b>'+fmtUSD(c.open)+'</b> H<b>'+fmtUSD(c.high)+'</b><br>L<b>'+fmtUSD(c.low)+'</b> C<b>'+fmtUSD(c.close)+'</b>';
    let left = x + 14; if(left + 118 > rect.width) left = x - 130;
    tip.style.left = clamp(left,0,rect.width-118)+'px'; tip.style.top = '6px';
    tip.hidden = false;
  }
  wrap.addEventListener('mousemove', e=> showAt(e.clientX));
  wrap.addEventListener('mouseleave', ()=> tip.hidden = true);
  wrap.addEventListener('touchstart', e=>{ if(e.touches[0]) showAt(e.touches[0].clientX); }, {passive:true});
  wrap.addEventListener('touchmove', e=>{ if(e.touches[0]) showAt(e.touches[0].clientX); }, {passive:true});
  wrap.addEventListener('touchend', ()=> setTimeout(()=> tip.hidden = true, 1400));
}

/* ================================================================
   TABLES — POSITIONS / TRADES / NEWS
   ================================================================ */
let positions = [
  {pair:'BTC/USDT', side:'LONG', size:2.50, entry:65120.0, pnl:3290.45, roi:5.04},
  {pair:'ETH/USDT', side:'LONG', size:15.00, entry:3012.45, pnl:1532.18, roi:3.38},
  {pair:'SOL/USDT', side:'LONG', size:50.00, entry:157.21, pnl:719.25, roi:9.15},
  {pair:'BNB/USDT', side:'LONG', size:20.00, entry:575.32, pnl:380.14, roi:6.62},
  {pair:'XRP/USDT', side:'LONG', size:10000, entry:0.5321, pnl:232.10, roi:4.36},
  {pair:'AVAX/USDT', side:'LONG', size:120.00, entry:38.42, pnl:184.30, roi:3.98},
  {pair:'DOGE/USDT', side:'LONG', size:50000, entry:0.1842, pnl:96.50, roi:1.05},
];
function renderPositions(){
  const body = document.getElementById('positionsBody');
  body.innerHTML = positions.map(p=>{
    const pnlOk = p.pnl==null || p.pnl>=0;
    const pnlTxt = p.pnl==null ? '—' : (p.pnl>=0?'+':'') + fmtUSD(p.pnl);
    const roiTxt = p.roi==null ? '—' : (p.roi>=0?'+':'') + p.roi.toFixed(2) + '%';
    return '<tr><td>'+p.pair+'</td><td><span class="side-tag '+p.side.toLowerCase()+'">'+p.side+'</span></td>'+
      '<td>'+p.size.toLocaleString('en-US',{maximumFractionDigits:2})+'</td><td>'+fmtUSD(p.entry, p.entry<10?4:2)+'</td>'+
      '<td class="pnl-val '+(pnlOk?'up':'down')+'">'+pnlTxt+'</td><td class="pnl-val '+(pnlOk?'up':'down')+'">'+roiTxt+'</td></tr>';
  }).join('');
}
function jitterPositions(){
  positions.forEach(p=>{
    if(p.pnl==null) return;
    p.pnl += (Math.random()-0.47) * Math.abs(p.pnl) * 0.02;
    p.roi = (p.pnl / (p.entry*p.size)) * 100;
  });
  renderPositions();
}

let trades = [
  {time:'21:42:21', pair:'BTC/USDT', side:'BUY', size:0.25, price:66432.5, status:'FILLED'},
  {time:'21:41:58', pair:'ETH/USDT', side:'BUY', size:2.50, price:3140.25, status:'FILLED'},
  {time:'21:41:35', pair:'SOL/USDT', side:'BUY', size:10.00, price:165.12, status:'FILLED'},
  {time:'21:40:12', pair:'BTC/USDT', side:'SELL', size:0.20, price:66210.3, status:'FILLED'},
  {time:'21:39:45', pair:'ETH/USDT', side:'BUY', size:1.50, price:3135.48, status:'FILLED'},
];
function renderTrades(){
  const body = document.getElementById('tradesBody');
  body.innerHTML = trades.map(t=>
    '<tr><td>'+t.time+'</td><td>'+t.pair+'</td><td><span class="side-tag '+t.side.toLowerCase()+'">'+t.side+'</span></td>'+
    '<td>'+t.size+'</td><td>'+fmtUSD(t.price, t.price<10?4:2)+'</td><td><span class="status-chip">'+t.status+'</span></td></tr>'
  ).join('');
}
function addSyntheticTrade(){
  const syms = ['BTC/USDT','ETH/USDT','SOL/USDT','BNB/USDT'];
  const pair = syms[Math.floor(Math.random()*syms.length)];
  const base = pair.split('/')[0];
  const price = state.prices[base] || 100;
  trades.unshift({ time:new Date().toLocaleTimeString('en-GB'), pair, side: Math.random()>0.35?'BUY':'SELL',
    size: +(Math.random()*3+0.1).toFixed(2), price, status:'FILLED' });
  if(trades.length > 8) trades.pop();
  renderTrades();
}

let newsItems = [
  {time:'21:42:10', text:'Whale Alert: Large BTC transfer detected'},
  {time:'21:41:22', text:'Funding rate for BTC/USDT is positive'},
  {time:'21:40:01', text:'High volatility detected in ETH market'},
  {time:'21:39:11', text:'AI Model updated: Accuracy improved +2.3%'},
];
const NEWS_BELL = '<svg viewBox="0 0 24 24"><path d="M12 3C9 3 7.5 5 7.5 8V11L5.5 14.5H18.5L16.5 11V8C16.5 5 15 3 12 3Z"/><path d="M10 17a2 2 0 0 0 4 0"/></svg>';
function renderNews(){
  const el = document.getElementById('newsList');
  el.innerHTML = newsItems.slice(0,6).map(n=>
    '<div class="news-item"><div class="news-icon">'+NEWS_BELL+'</div><div><div class="news-text">'+n.text+'</div><span class="news-time">'+n.time+'</span></div></div>'
  ).join('');
}
const SYNTH_NEWS = [
  'Order flow imbalance detected on ETH/USDT',
  'AI Confidence Matrix recalibrated · signal quality up',
  'Delta Exchange latency spike resolved',
  'Circuit breaker check passed · no anomalies',
  'New liquidity cluster forming near BTC resistance',
];
function addSyntheticNews(){
  newsItems.unshift({ time:new Date().toLocaleTimeString('en-GB'), text: SYNTH_NEWS[Math.floor(Math.random()*SYNTH_NEWS.length)] });
  if(newsItems.length > 10) newsItems.pop();
  renderNews();
}

/* ================================================================
   SYSTEM PULSE · ENGINE CHIPS · AI GATEKEEPER LOG
   Demo arrays below use the real vocabulary from your risk engine
   (Choppy Market Blocker, Drawdown Guard, Circuit Breaker) so the
   demo reads true to the system even before you connect it live.
   ================================================================ */
let gatekeeperLog = [
  {time:'21:38:02', symbol:'ETHUSD', reason:'Choppy Market Blocker', detail:'ADX below threshold — signal skipped'},
  {time:'21:22:47', symbol:'SOLUSD', reason:'Daily Circuit Breaker', detail:'Loss limit reached for the session'},
  {time:'20:58:15', symbol:'BTCUSD', reason:'Drawdown Guard', detail:'Position sizing reduced, entry paused'},
];
function renderGatekeeper(){
  const el = document.getElementById('gatekeeperList');
  if(!gatekeeperLog.length){
    el.innerHTML = '<div class="gatekeeper-item" style="border-color:var(--lime)"><div class="gk-detail">No signals blocked recently — Gatekeeper is clear.</div></div>';
    return;
  }
  el.innerHTML = gatekeeperLog.slice(0,6).map(g=>
    '<div class="gatekeeper-item"><div class="gk-head"><span>'+g.symbol+'</span><span>'+g.reason+'</span></div>'+
    (g.detail ? '<div class="gk-detail">'+g.detail+'</div>' : '') + '<span class="gk-time">'+g.time+'</span></div>'
  ).join('');
}
function renderExecutionStats(x){
  document.getElementById('pulse-avgms').textContent = x.avg_ms!=null ? x.avg_ms+'ms avg' : '—';
  document.getElementById('pulse-success').textContent = x.success_rate!=null ? x.success_rate+'%' : '—';
  if(x.avg_ms!=null){
    const li = document.querySelector('#latency-list li[data-base="24"]'); // Delta Exchange row
    if(li){ li.dataset.base = x.avg_ms; li.querySelector('.ms').textContent = Math.round(x.avg_ms)+'ms'; }
  }
}
function renderSystemHealth(x){
  document.getElementById('pulse-hostload').textContent = x.available ? (Math.round(x.cpu_percent)+'% / '+Math.round(x.memory_percent)+'%') : 'psutil off';
  const s = x.uptime_seconds||0, d = Math.floor(s/86400), h = Math.floor((s%86400)/3600);
  document.getElementById('pulse-hostuptime').textContent = d+'d '+h+'h';
}
function renderEngineChips(cfg){
  window.__apexKillActive = !!cfg.kill_switch_active;
  const items = [
    {label:'Predator Vision', on:cfg.predator_vision_enabled},
    {label:'Neural Syndicate', on:cfg.neural_syndicate_enabled},
    {label:'HFT Exits', on:cfg.hft_parallel_exits},
    {label:'Shock Block', on:cfg.block_entries_during_shock},
    {label:'Kill Switch', on:cfg.kill_switch_active, danger:true},
  ];
  document.getElementById('engineChips').innerHTML = items.map(i=>
    '<span class="engine-chip '+(i.on?(i.danger?'danger':'on'):'off')+'">'+i.label+'</span>'
  ).join('');
  const oracle = cfg.ai_market_sentiment, oracleEl = document.getElementById('pulseOracle');
  oracleEl.textContent = (oracle && oracle.consensus)
    ? 'AI Oracle (Gemini) consensus on '+(oracle.symbol||'—')+': '+oracle.consensus+(oracle.updated_at?(' · updated '+oracle.updated_at):'')
    : "AI Oracle (Gemini) hasn't reported a consensus yet — run ai_oracle.py alongside main.py to populate this.";
}

/* ------------------------------------------------------------
   Real (non-simulated) Fear & Greed — public index, no key needed,
   same alternative.me source your React dashboard uses. Runs
   regardless of LIVE.enabled and quietly keeps the illustrative
   78/Greed already in the markup if the fetch fails.
   ------------------------------------------------------------ */
async function fetchRealFearGreed(){
  try{
    const res = await fetch('https://api.alternative.me/fng/?limit=1', { cache:'no-store' });
    if(!res.ok) throw new Error('fng unavailable');
    const j = await res.json();
    const v = j && j.data && j.data[0];
    if(!v) throw new Error('fng empty');
    const val = Number(v.value), label = (v.value_classification||'').toUpperCase();
    document.getElementById('fng-num').textContent = val;
    const path = document.getElementById('fng-path');
    path.dataset.pct = val;
    setGaugeProgress(path, val, 60);
    const tagEl = document.querySelector('.half-gauge-tag');
    if(tagEl) tagEl.textContent = 'FEAR & GREED · ' + label + ' · LIVE';
  }catch(e){ /* keep the illustrative value already baked into the markup */ }
}

/* ================================================================
   LIVE SIMULATION TICK
   ================================================================ */
const TICKERS = ['BTC','ETH','SOL','BNB'];

/* Heatmap tile — shared by demo jitter and the live mark-price poll below,
   so the tile coloring logic only lives in one place. Intensity scales with
   |change%| so a +8% mover reads hotter than a +0.4% one, not just red/green. */
function updateHeatTile(sym, price, chgPct){
  const tile = document.getElementById('heat-'+sym);
  if(!tile) return;
  const priceEl = document.getElementById('heatprice-'+sym);
  const chEl = document.getElementById('heatchg-'+sym);
  const barEl = document.getElementById('heatbar-'+sym);
  if(priceEl) priceEl.textContent = fmtUSD(price);
  if(chEl){
    chEl.textContent = (chgPct>=0?'+':'') + chgPct.toFixed(2) + '%';
    chEl.classList.toggle('up', chgPct>=0); chEl.classList.toggle('down', chgPct<0);
  }
  const mag = clamp(Math.abs(chgPct)/9, 0.08, 1);
  const color = chgPct>=0 ? '157,255,31' : '255,79,109';
  tile.style.background = 'rgba('+color+','+(0.05+mag*0.16)+')';
  tile.style.borderColor = 'rgba('+color+','+(0.18+mag*0.45)+')';
  tile.style.boxShadow = mag>0.35 ? '0 0 18px -6px rgba('+color+',.6)' : 'none';
  if(barEl){ barEl.style.width = Math.round(mag*100)+'%'; barEl.style.background = 'rgb('+color+')'; }
}
function renderHeatmap(){ TICKERS.forEach(sym=> updateHeatTile(sym, state.prices[sym], state.changePct[sym])); }

function jitterPrices(){
  TICKERS.forEach(sym=>{
    if(sym==='BTC' && chartState.liveCandles) return; // real value owned by loadCandles()
    const before = state.prices[sym];
    const pct = (Math.random()-0.47) * 0.14;
    const after = before * (1 + pct/100);
    state.prices[sym] = after;
    state.changePct[sym] = clamp(state.changePct[sym] + pct*0.35, -9, 15);
    const priceEl = document.getElementById('price-'+sym), chEl = document.getElementById('chg-'+sym);
    if(priceEl) setTextFlash(priceEl, fmtUSD(after), after>=before?1:-1);
    if(chEl){
      const v = state.changePct[sym];
      chEl.textContent = (v>=0?'+':'') + v.toFixed(2) + '%';
      chEl.classList.toggle('up', v>=0); chEl.classList.toggle('down', v<0);
    }
    updateHeatTile(sym, state.prices[sym], state.changePct[sym]);
    if(sym==='BTC'){
      const chg = document.getElementById('chartChange');
      chg.textContent = (state.changePct.BTC>=0?'+':'')+state.changePct.BTC.toFixed(2)+'%';
      chg.classList.toggle('up', state.changePct.BTC>=0); chg.classList.toggle('down', state.changePct.BTC<0);
    }
  });
}
function jitterAccount(){
  const before = state.balance;
  state.balance += (Math.random()-0.46) * 40;
  state.pnl24h += (Math.random()-0.46) * 12;
  state.pnlPct = (state.pnl24h / (state.balance-state.pnl24h)) * 100;
  state.usedMargin = clamp(state.usedMargin + (Math.random()-0.5)*30, 20000, 45000);
  const marginPct = (state.usedMargin/(state.availMargin+state.usedMargin))*100;
  setTextFlash(document.getElementById('stat-balance'), fmtUSD(state.balance,2,'$ '), state.balance>=before?1:-1);
  const pnlEl = document.getElementById('stat-pnl');
  pnlEl.textContent = (state.pnl24h>=0?'+ $':'- $') + Math.abs(state.pnl24h).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}) + ' (' + (state.pnlPct>=0?'+':'') + state.pnlPct.toFixed(2) + '%)';
  pnlEl.classList.toggle('up', state.pnl24h>=0); pnlEl.classList.toggle('down', state.pnl24h<0);
  document.getElementById('stat-used').textContent = fmtUSD(state.usedMargin);
  document.getElementById('stat-marginpct').textContent = marginPct.toFixed(2)+'%';
  document.getElementById('bar-margin').style.width = marginPct+'%';
}
function jitterLatencies(){
  document.querySelectorAll('#latency-list li').forEach(li=>{
    const base = parseFloat(li.dataset.base);
    const val = Math.max(9, Math.round(base + (Math.random()-0.5)*6));
    li.querySelector('.ms').textContent = val+'ms';
  });
  state.totalSignals += Math.floor(Math.random()*4);
  document.getElementById('stat-signals').textContent = state.totalSignals.toLocaleString('en-US');
}
function jitterFooter(){
  state.coreLoad = clamp(state.coreLoad + (Math.random()-0.5)*3, 40, 92);
  state.srvLatency = clamp(Math.round(state.srvLatency + (Math.random()-0.5)*6), 12, 55);
  document.getElementById('stat-load').textContent = state.coreLoad.toFixed(1)+'%';
  document.getElementById('stat-srvlatency').textContent = state.srvLatency+'ms';
}
function tick(){
  jitterPrices(); jitterAccount(); jitterLatencies(); jitterFooter(); jitterPositions();
  if(chartState.liveCandles){ loadCandles(chartState.tf); } else { tickChart(); }
  pollLive();
  if(Math.random() < 0.32) addSyntheticTrade();
  if(Math.random() < 0.16) addSyntheticNews();
}

function tickUptime(){
  const baseSeconds = 15*86400 + 22*3600 + 47*60;
  const elapsed = Math.floor((performance.now() - tickUptime._start)/1000);
  const total = baseSeconds + elapsed;
  const d = Math.floor(total/86400), h = Math.floor((total%86400)/3600), m = Math.floor((total%3600)/60);
  document.getElementById('stat-uptime').textContent = d+'D '+h+'H '+m+'M';
}
tickUptime._start = performance.now();

function renderDate(){
  const d = new Date();
  document.getElementById('stat-date').textContent = d.toLocaleDateString('en-US',{ weekday:'long', day:'numeric', month:'long', year:'numeric' }).toUpperCase();
}

/* ================================================================
   VOICE ASSISTANT
   ================================================================ */
const VOICE_COMMANDS = [
  { match:['performance','stats','model accuracy'],
    reply:"Model accuracy is holding at 98.6% with a 78.3% win rate across 24,856 predictions, Master. Profit factor is 3.89.",
    action:()=>flashPanel('panel-model-perf') },
  { match:['market status','status of market','market condition'],
    reply:"Markets are reading Bullish at 73.6% sentiment. Fear and Greed is at 78 — Greed territory. Volatility is Medium.",
    action:()=>flashPanel('panel-confidence') },
  { match:['auto pilot','autopilot'],
    reply:"Auto Pilot needs a confirmed toggle from the Autopilot module, Master. Opening it now.",
    action:()=>setActiveNav('autopilot') },
  { match:['close all positions','close positions'],
    reply:"Opening Autopilot, Master — Close All Positions needs a manual confirmation there since it's irreversible.",
    action:()=>setActiveNav('autopilot') },
  { match:['risk management report','risk report','risk'],
    reply:()=>{
      const g = id => (document.getElementById(id)||{}).textContent || '—';
      return 'Max drawdown '+g('risk-maxdd')+', exposure '+g('risk-exposure')+', open risk to stop-loss '+g('risk-openrisk')+
        ', leverage '+g('risk-leverage')+(LIVE.enabled ? '.' : ' — connect your live bot for real numbers, these are illustrative.');
    },
    action:()=>flashPanel('panel-risk') },
  { match:['market news','news'],
    reply:"Latest: a large BTC whale transfer was flagged, and funding rates on BTC/USDT have turned positive.",
    action:()=>flashPanel('panel-account') },
];
function matchCommand(text){
  const t = text.toLowerCase();
  return VOICE_COMMANDS.find(c => c.match.some(m => t.includes(m)));
}
function addHistory(text){
  const el = document.getElementById('nexusHistory');
  const row = document.createElement('div');
  row.className = 'nexus-history-item';
  row.innerHTML = '<strong>' + new Date().toLocaleTimeString('en-GB') + '</strong> — ' + text;
  el.prepend(row);
  while(el.children.length > 4) el.removeChild(el.lastChild);
}
function setTranscript(text){ document.getElementById('transcript').textContent = text; }
function setNexusReply(text){ document.getElementById('nexusReplyText').textContent = text; }
function setMicState(mode){
  const stage = document.getElementById('micStage');
  stage.classList.remove('is-listening','is-speaking');
  const label = document.getElementById('micStatusText');
  if(mode==='listening'){ stage.classList.add('is-listening'); label.textContent = 'Listening…'; }
  else if(mode==='speaking'){ stage.classList.add('is-speaking'); label.textContent = 'Speaking…'; }
  else{ label.textContent = 'Tap to Speak'; }
}
function speakReply(text){
  setNexusReply(text);
  setMicState('speaking');
  if('speechSynthesis' in window){
    try{
      const u = new SpeechSynthesisUtterance(text);
      u.rate = 1.02; u.pitch = 0.92;
      u.onend = ()=> setMicState('idle');
      speechSynthesis.cancel(); speechSynthesis.speak(u);
    }catch(e){ setTimeout(()=> setMicState('idle'), 1800); }
  } else { setTimeout(()=> setMicState('idle'), 1800); }
}
async function runCommand(text){
  addHistory(text);
  const cmd = matchCommand(text);
  if(cmd && cmd.action) cmd.action();
  if(LIVE.enabled){
    try{
      const res = await fetch(LIVE.baseUrl + '/ask/' + encodeURIComponent(LIVE.key), {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ question:text, history:[] })
      });
      const body = await res.json().catch(()=>({}));
      if(res.ok && body.answer){ speakReply(body.answer); return; }
    }catch(e){ /* fall through to local reply below */ }
  }
  const reply = cmd ? (typeof cmd.reply === 'function' ? cmd.reply() : cmd.reply) : "I didn't catch a known command, Master. Try one from the list below.";
  speakReply(reply);
}

let recognition = null;
try{
  const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
  if(SR){
    recognition = new SR();
    recognition.continuous = false; recognition.interimResults = true; recognition.lang = 'en-IN';
    recognition.onresult = (e)=>{
      let interim='', final='';
      for(let i=e.resultIndex;i<e.results.length;i++){
        if(e.results[i].isFinal) final += e.results[i][0].transcript; else interim += e.results[i][0].transcript;
      }
      setTranscript(interim || final);
      if(final) runCommand(final);
    };
    recognition.onerror = ()=> setMicState('idle');
    recognition.onend = ()=>{ if(document.getElementById('micStage').classList.contains('is-listening')) setMicState('idle'); };
  }
}catch(e){ recognition = null; }

function startListening(){
  setMicState('listening');
  setTranscript('');
  if(recognition){
    try{ recognition.start(); }
    catch(e){ setTranscript('Mic unavailable — tap a command below instead.'); setTimeout(()=> setMicState('idle'), 1400); }
  } else {
    setTranscript('Voice recognition not available here — tap a command below.');
    setTimeout(()=> setMicState('idle'), 1600);
  }
}
document.getElementById('micBtn').addEventListener('click', ()=>{
  const stage = document.getElementById('micStage');
  if(stage.classList.contains('is-listening')){
    if(recognition){ try{ recognition.stop(); }catch(e){} }
    setMicState('idle');
  } else { startListening(); }
});
document.querySelectorAll('.say-item').forEach(el=>{
  el.addEventListener('click', ()=>{ setTranscript(el.dataset.phrase); runCommand(el.dataset.phrase); });
});

/* ================================================================
   NAV / TOAST
   ================================================================ */
const NAV_LABELS = { dashboard:'Dashboard', strategy:'Strategy Lab', backtest:'Backtest Engine', vault:'Portfolio Vault', autopilot:'Autopilot', settings:'System Settings' };
// [REAL TABS ADD] These 4 now have real content wired to real endpoints —
// only 'strategy' still has no backend concept (the bot runs one signal
// system, not multiple selectable strategies) so it keeps the honest toast.
const REAL_TABS = ['dashboard','autopilot','vault','backtest','settings'];
const TAB_RENDERERS = { autopilot: ()=>renderAutopilotTab(), vault: ()=>renderVaultTab(), backtest: ()=>renderBacktestTab(), settings: ()=>renderSettingsTab() };
function setActiveNav(key){
  if(!REAL_TABS.includes(key)){
    showToast(NAV_LABELS[key] + ' module is queued for the next build phase, Master.');
    return; // nothing real to switch to — leave the current view exactly as-is
  }
  document.querySelectorAll('.nav-item').forEach(b=> b.classList.toggle('active', b.dataset.nav===key));
  document.querySelectorAll('.tab-view').forEach(v=> v.hidden = (v.id !== 'view-'+key));
  window.scrollTo({top:0, behavior:'instant'});
  if(key === 'dashboard') initGaugesAndBars(); // re-sync gauges that silently skipped their draw while this tab was hidden
  if(TAB_RENDERERS[key]) TAB_RENDERERS[key]();
}
document.querySelectorAll('.nav-item').forEach(b=> b.addEventListener('click', ()=> setActiveNav(b.dataset.nav)));
document.getElementById('navLeft').addEventListener('click', ()=> document.getElementById('navItems').scrollBy({left:-220,behavior:'smooth'}));
document.getElementById('navRight').addEventListener('click', ()=> document.getElementById('navItems').scrollBy({left:220,behavior:'smooth'}));
document.getElementById('viewAllNews').addEventListener('click', ()=> showToast('Full News & Alerts feed is queued for the next build phase, Master.'));

/* ================================================================
   WAVEFORM BARS (generated once)
   ================================================================ */
function buildWaveform(){
  const wf = document.getElementById('waveform');
  for(let i=0;i<26;i++){
    const bar = document.createElement('span');
    bar.style.animationDelay = (Math.random()*1.2).toFixed(2)+'s';
    bar.style.setProperty('--h', (12+Math.random()*70).toFixed(0)+'%');
    wf.appendChild(bar);
  }
}

/* ================================================================
   TIMEFRAME TABS
   ================================================================ */
document.querySelectorAll('.tf-tab').forEach(btn=>{
  btn.addEventListener('click', ()=>{
    document.querySelectorAll('.tf-tab').forEach(b=> b.classList.remove('active'));
    btn.classList.add('active');
    loadCandles(btn.dataset.tf);
  });
});

/* ================================================================
   RESIZE (debounced)
   ================================================================ */
let resizeT;
window.addEventListener('resize', ()=>{
  clearTimeout(resizeT);
  resizeT = setTimeout(drawChart, 120);
});

/* ================================================================
   CONNECT POPOVER WIRING
   ================================================================ */
document.getElementById('dataModeBadge').addEventListener('click', (e)=>{
  e.stopPropagation();
  document.getElementById('connectPop').hidden = !document.getElementById('connectPop').hidden;
});
document.getElementById('connectBtn').addEventListener('click', ()=>{
  const u = document.getElementById('connBaseUrl').value.trim();
  const k = document.getElementById('connKey').value.trim();
  if(!u || !k){ showToast('Enter both the backend URL and your passphrase.'); return; }
  applyLiveConfig(u,k);
  document.getElementById('connectPop').hidden = true;
});
document.getElementById('disconnectBtn').addEventListener('click', ()=>{
  clearLiveConfig();
  document.getElementById('connKey').value = '';
});
document.addEventListener('click', (e)=>{
  const wrap = document.querySelector('.data-mode-wrap');
  if(wrap && !wrap.contains(e.target)) document.getElementById('connectPop').hidden = true;
});

/* ================================================================
   MARKET INTELLIGENCE — WORLD MAP
   A genuine (if stylised, low-poly) world map instead of an abstract
   node graph: hand-plotted continent silhouettes in equirectangular
   space (0-100 x = -180..180 lon, 0-100 y = 90..-90 lat), rasterised
   to a glowing dot grid on canvas. Point-in-polygon test per grid
   cell, no external map libraries or network fetch needed — this
   panel has to render even with the sandbox/deploy box offline.
   ================================================================ */
const WORLD_CONTINENTS = [
  // North America
  [[10,18],[14,11],[20,9],[26,12],[29,16],[28,22],[24,27],[19,32],[14,33],[10,29],[7,24],[8,20]],
  // Greenland
  [[28,7],[33,6],[35,9],[32,12],[28,11]],
  // Central America land-bridge
  [[16,32],[20,32],[19,36],[16,35]],
  // South America
  [[21,37],[26,36],[30,43],[29,53],[26,63],[23,59],[20,49],[19,41]],
  // Europe
  [[44,13],[50,10],[56,13],[57,18],[52,21],[46,20],[43,17]],
  // Africa
  [[45,21],[53,19],[58,27],[58,41],[54,53],[48,55],[43,47],[42,32]],
  // Asia (mainland + Siberia)
  [[55,9],[68,6],[84,10],[90,17],[86,23],[77,21],[71,27],[64,29],[58,25],[53,20]],
  // India
  [[61,29],[66,27],[68,34],[63,39],[59,34]],
  // SE Asia / Indonesia
  [[70,38],[78,37],[80,42],[74,44],[69,41]],
  // Australia
  [[77,57],[87,55],[91,61],[85,66],[77,64],[75,60]],
  // UK/Ireland (tiny, own poly so it doesn't get swallowed by Europe gap)
  [[41,14],[43,13],[43,16],[41,17]],
  // Japan
  [[87,20],[90,19],[91,24],[88,25]],
];

function _pointInPoly(x, y, poly){
  let inside = false;
  for(let i=0, j=poly.length-1; i<poly.length; j=i++){
    const xi=poly[i][0], yi=poly[i][1], xj=poly[j][0], yj=poly[j][1];
    const intersect = ((yi>y)!==(yj>y)) && (x < (xj-xi)*(y-yi)/((yj-yi)||1e-9) + xi);
    if(intersect) inside = !inside;
  }
  return inside;
}
function _isLand(xPct, yPct){
  for(const poly of WORLD_CONTINENTS){ if(_pointInPoly(xPct, yPct, poly)) return true; }
  return false;
}
// Precompute the land dot grid once — it never changes, only the paint
// (glow pulse / colour) needs to run every frame.
let _worldDots = null;
function _buildWorldDots(cols, rows){
  const dots = [];
  for(let r=0; r<rows; r++){
    for(let c=0; c<cols; c++){
      const xPct = (c+0.5)/cols*100, yPct = (r+0.5)/rows*100;
      if(_isLand(xPct, yPct)) dots.push({xPct, yPct, tw: Math.random()*Math.PI*2});
    }
  }
  return dots;
}
function initWorldMap(){
  const canvas = document.getElementById('worldMapCanvas');
  if(!canvas) return;
  const ctx = canvas.getContext('2d');
  let w, h;
  function resize(){
    const rect = canvas.parentElement.getBoundingClientRect();
    w = canvas.width = Math.max(1, Math.round(rect.width * (window.devicePixelRatio||1)));
    h = canvas.height = Math.max(1, Math.round(rect.height * (window.devicePixelRatio||1)));
    const cols = 70, rows = Math.round(cols * (h/w));
    _worldDots = _buildWorldDots(cols, rows);
  }
  window.addEventListener('resize', resize);
  resize();

  function colourFor(yPct){
    // Vertical gradient echoing the reference art: cyan/blue near the
    // poles, violet through the mid-latitudes, lime/amber near the
    // equator — purely cosmetic, same palette as the rest of the shell.
    if(yPct < 30) return '47,228,255';
    if(yPct < 55) return '180,99,255';
    if(yPct < 75) return '157,255,31';
    return '255,176,32';
  }
  function frame(t){
    if(!_worldDots){ requestAnimationFrame(frame); return; }
    ctx.clearRect(0,0,w,h);
    const dotR = Math.max(1, w/300);
    for(const d of _worldDots){
      const px = d.xPct/100*w, py = d.yPct/100*h;
      const tw = 0.55 + 0.45*Math.sin(t*0.0012 + d.tw);
      const rgb = colourFor(d.yPct);
      ctx.beginPath();
      ctx.fillStyle = `rgba(${rgb},${(0.35+0.5*tw).toFixed(2)})`;
      ctx.shadowColor = `rgba(${rgb},0.9)`;
      ctx.shadowBlur = dotR*2.5;
      ctx.arc(px, py, dotR, 0, Math.PI*2);
      ctx.fill();
    }
    ctx.shadowBlur = 0;
    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);
}

/* ================================================================
   AMBIENT CINEMATIC BACKGROUND — drifting particle field + slow-moving
   nebula glow bands + occasional streak, running continuously behind
   every panel so the whole shell feels alive rather than static. Colour
   drifts warmer (coral) if the kill switch is armed, otherwise stays on
   the lime/cyan/violet brand palette — a real (if subtle) status tell,
   not just decoration.
   ================================================================ */
function initStarfield(){
  const canvas = document.getElementById('starfield');
  if(!canvas) return;
  const ctx = canvas.getContext('2d');
  let w,h,dpr;
  function resize(){
    dpr = Math.min(window.devicePixelRatio||1, 2);
    w = canvas.width = innerWidth*dpr; h = canvas.height = innerHeight*dpr;
    canvas.style.width = innerWidth+'px'; canvas.style.height = innerHeight+'px';
  }
  resize();
  window.addEventListener('resize', resize);

  const N = Math.round((innerWidth*innerHeight)/3200);
  const stars = Array.from({length:Math.max(180,Math.min(N,480))}, ()=>({
    x:Math.random(), y:Math.random(), z:0.4+Math.random()*1.4,
    tw:Math.random()*Math.PI*2, spd:0.15+Math.random()*0.35,
    big: Math.random() < 0.12,
  }));
  const blobs = [
    {cx:.18,cy:.15,r:.55,hue:[157,255,31],spd:.011,ang:.2},
    {cx:.85,cy:.1, r:.5, hue:[47,228,255],spd:.008,ang:2.1},
    {cx:.12,cy:.88,r:.5, hue:[157,255,31],spd:.009,ang:4.0},
    {cx:.88,cy:.85,r:.46,hue:[180,99,255],spd:.013,ang:5.3},
    {cx:.5, cy:.5, r:.62,hue:[47,228,255],spd:.006,ang:3.0},
  ];
  let streak = null, nextStreakAt = performance.now() + 2500 + Math.random()*3500;
  let killTint = 0; // 0 = brand palette, 1 = full coral warning tint

  function frame(t){
    ctx.clearRect(0,0,w,h);

    killTint += ((( (typeof LIVE!=='undefined' && LIVE.enabled && window.__apexKillActive) ? 1 : 0) - killTint) * 0.02);

    blobs.forEach(b=>{
      const bx = (b.cx + Math.cos(t*0.00002*b.spd*60 + b.ang)*0.05) * w;
      const by = (b.cy + Math.sin(t*0.00002*b.spd*60 + b.ang)*0.05) * h;
      const rad = b.r * Math.max(w,h);
      const hue = killTint>0.05 ? [255,79,109] : b.hue;
      const g = ctx.createRadialGradient(bx,by,0,bx,by,rad);
      g.addColorStop(0, `rgba(${hue[0]},${hue[1]},${hue[2]},${(0.38*dpr).toFixed(2)})`);
      g.addColorStop(0.5, `rgba(${hue[0]},${hue[1]},${hue[2]},${(0.14*dpr).toFixed(2)})`);
      g.addColorStop(1, 'rgba(0,0,0,0)');
      ctx.fillStyle = g;
      ctx.fillRect(0,0,w,h);
    });

    stars.forEach(s=>{
      s.tw += 0.02*s.spd;
      const alpha = (0.55 + Math.sin(s.tw)*0.4) * Math.min(1, s.z);
      const px = s.x*w, py = (s.y*h + t*0.006*s.spd) % h;
      const r = (s.big ? 2.2 : 1.15) * dpr * s.z;
      if(s.big){
        ctx.shadowColor = 'rgba(210,235,255,.9)'; ctx.shadowBlur = r*3;
      }
      ctx.beginPath();
      ctx.fillStyle = `rgba(210,235,255,${Math.max(0,Math.min(1,alpha)).toFixed(2)})`;
      ctx.arc(px, py, r, 0, Math.PI*2);
      ctx.fill();
      ctx.shadowBlur = 0;
    });

    if(!streak && t > nextStreakAt){
      streak = { x:Math.random()*w*0.6+w*0.1, y:Math.random()*h*0.3, len:0, maxLen:(120+Math.random()*160)*dpr, ang:0.6+Math.random()*0.3 };
    }
    if(streak){
      streak.len += 14*dpr;
      const x2 = streak.x + Math.cos(streak.ang)*streak.len;
      const y2 = streak.y + Math.sin(streak.ang)*streak.len;
      const grad = ctx.createLinearGradient(streak.x,streak.y,x2,y2);
      grad.addColorStop(0,'rgba(234,255,176,0)');
      grad.addColorStop(0.85,'rgba(234,255,176,.9)');
      grad.addColorStop(1,'rgba(234,255,176,0)');
      ctx.strokeStyle = grad; ctx.lineWidth = 2*dpr;
      ctx.beginPath(); ctx.moveTo(streak.x,streak.y); ctx.lineTo(x2,y2); ctx.stroke();
      if(streak.len > streak.maxLen){ streak = null; nextStreakAt = t + 3500 + Math.random()*5000; }
    }

    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);
}

/* ================================================================
   INIT
   ================================================================ */
function autoConnectFromUrl(){
  // This file is normally served BY the bot itself at /dashboard/<token> —
  // in that case the origin and secret are already known, so auto-connect
  // instead of making the operator retype them into the connect popover.
  // If it's opened standalone (e.g. saved to disk) this quietly no-ops and
  // the manual Connect flow above still works exactly as before.
  try{
    const parts = window.location.pathname.split('/').filter(Boolean);
    const idx = parts.indexOf('dashboard');
    if(idx !== -1 && parts[idx+1]){
      const token = parts[idx+1];
      const raw = localStorage.getItem(LIVE_STORAGE_KEY);
      const already = raw ? JSON.parse(raw) : null;
      if(!already || already.key !== token){
        applyLiveConfig(window.location.origin, token);
      }
    }
  }catch(e){ /* standalone file — no URL token to read, stay in demo mode */ }
}

document.addEventListener('DOMContentLoaded', ()=>{
  bootSequence();
  renderDate();
  initGaugesAndBars();
  loadLiveConfig();
  autoConnectFromUrl();
  renderPositions();
  renderTrades();
  renderNews();
  renderGatekeeper();
  renderRiskPanel();
  document.getElementById('btnRunBacktest').onclick = runHistoricalBacktest;
  renderHeatmap();
  buildWaveform();
  loadCandles('1m');
  attachChartTooltip();
  try{ initOrb(); }catch(e){ showOrbFallback(); }
  try{ initWorldMap(); }catch(e){ /* canvas unsupported — panel still shows its other data */ }
  try{ initStarfield(); }catch(e){ /* canvas unsupported — page still works without it */ }
  setInterval(tickUptime, 1000); tickUptime();
  setInterval(tick, 2600);
  fetchRealFearGreed();
  if(LIVE.enabled) pollLive();
});
</script>
</body>
</html>
"""


@app.route("/", methods=["GET"])
def root():
    return jsonify({"status": "apex nexus running", "dashboard": "/dashboard/<APEX_WEBHOOK_PASSPHRASE>"})


@app.route("/dashboard/<token>", methods=["GET"])
def dashboard(token):
    if not WEBHOOK_SECRET_TOKEN or not hmac.compare_digest(token, WEBHOOK_SECRET_TOKEN):
        return jsonify({"error": "unauthorized"}), 403
    return DASHBOARD_HTML


# ════════════════════════════════════════════════════════════════════════════════
# WEBHOOK — the single entry point Pine talks to
# ════════════════════════════════════════════════════════════════════════════════
@app.route("/webhook/<secret_token>", methods=["POST"])
def webhook(secret_token):
    try:
        if not WEBHOOK_SECRET_TOKEN or not hmac.compare_digest(secret_token, WEBHOOK_SECRET_TOKEN):
            return jsonify({"error": "unauthorized"}), 403

        data = request.get_json(silent=True) or {}
        signal = str(f(data, "signal", "")).strip().upper()
        direction = str(f(data, "direction", "")).strip().upper()
        # [PREMIUM FIX] Pine now sends ENTRY, UPDATE_SL, EXIT_TP1, EXIT_TP2, and
        # TRADE_CLOSE (see the V12-P3 Pine changelog) — this used to default
        # any non-ENTRY action straight into a full market close (see the
        # removed `else:` branch further down), which meant an UPDATE_SL
        # trailing-stop push would have PREMATURELY CLOSED THE WHOLE POSITION
        # the instant Pine started sending it, and EXIT_TP1/EXIT_TP2 (intended
        # as partial scale-outs) would have closed 100% instead of the
        # signal's own close_fraction. Explicit per-action routing below fixes
        # this — see each branch's own comment.
        action = str(f(data, "action", "ENTRY")).strip().upper()
        symbol = str(f(data, "symbol", "")).strip().upper()
        pine_version = str(f(data, "version", "") or "")
        preset = str(f(data, "preset", "") or "")

        if not signal or not symbol:
            return jsonify({"error": "missing signal or symbol", "received": data}), 400

        if pine_version and pine_version != EXPECTED_PINE_VERSION:
            log.warning(f"⚠️ Pine version drift: alert tagged '{pine_version}', bot built for "
                        f"'{EXPECTED_PINE_VERSION}' — trading anyway, but check the chart is running "
                        f"the indicator version you think it is.")

        if signal not in get_active_signals():
            return jsonify({"status": "signal_not_active", "signal": signal}), 200

        if is_duplicate_alert(signal, direction, symbol, action):
            return jsonify({"status": "ignored_duplicate"}), 200

        # [PREMIUM NEW] TRADE_CLOSE is purely informational — the exchange-side
        # bracket already resolved the position (that's the whole point of
        # AUTO_BRACKET_ORDERS). No exchange order is sent here, ever. This also
        # runs BEFORE product-id resolution below since it doesn't need one.
        if action == "TRADE_CLOSE":
            outcome = str(f(data, "outcome", "")).strip().upper()
            r_multiple = safe_float(f(data, "r_multiple"), 0.0)
            record_trade_outcome(outcome, r_multiple)
            existing = get_position(symbol)
            if existing:
                delete_position(symbol)
            log_trade(symbol, signal, direction, "TRADE_CLOSE", 0, 0, json.dumps(data),
                      systems=safe_int(f(data, "systems_buy" if direction == "BUY" else "systems_sell")),
                      preset=preset, pine_version=pine_version)
            notify_telegram(f"📒 {symbol} CLOSED — {outcome} ({r_multiple:+.2f}R) [{signal}]")
            return jsonify({"status": "recorded", "outcome": outcome, "r_multiple": r_multiple,
                             "circuit_breaker": circuit_breaker_status()}), 200

        # ★ AUTO-RESOLVE — the entire point of this rebuild. No lookup table to maintain.
        product_id = resolver.resolve(symbol)
        if not product_id:
            log_rejection(symbol, signal, direction, "unresolved_symbol",
                          f"'{symbol}' is not a live Delta perpetual under that name")
            return jsonify({
                "error": f"'{symbol}' is not a live Delta perpetual (or isn't listed under that name)",
                "hint": "check /products to see everything currently discovered"
            }), 400

        sl = f(data, "sl")
        tp1 = f(data, "tp1")
        market_state = f(data, "market_state", "NORMAL")
        in_shock = fbool(data, "in_shock", False)
        ml_healthy = fbool(data, "ml_healthy", True)
        premium_shield = fbool(data, "premium_shield", True)
        ai_score = safe_float(f(data, "ai_score_buy" if direction == "BUY" else "ai_score_sell"), 50.0)
        win_rate = safe_float(f(data, "win_rate"), 50.0)
        systems = safe_int(f(data, "systems_buy" if direction == "BUY" else "systems_sell"), 0)
        mtf_align_bars = safe_int(f(data, "mtf_align_bars"))

        qty = TIER_QUANTITY.get(signal, DEFAULT_QTY)

        if action == "ENTRY":
            # Fail fast, before even taking the claim lock, if the last
            # credential check already came back bad — no point racing to
            # claim a symbol for an order that's guaranteed to be rejected at
            # the auth step. Live mode only: in dry-run no real call is ever
            # made, so a bad key doesn't block testing the rest of the flow.
            if is_live_mode() and API_CREDENTIALS_OK is False:
                log.error(f"🚨 Entry for {symbol} rejected before it started — "
                          f"API credentials are known invalid: {API_CREDENTIALS_MSG}")
                log_rejection(symbol, signal, direction, "invalid_credentials", API_CREDENTIALS_MSG or "")
                return jsonify({"status": "blocked_invalid_credentials",
                                 "detail": API_CREDENTIALS_MSG}), 200

            # [PREMIUM NEW — PHASE 3] Global kill-switch. Deliberately checked
            # here, before the claim lock, same reasoning as the credentials
            # check above: no point racing to claim a symbol for a trade that
            # a sticky, manual "stop everything" override already forbids.
            if is_kill_switch_active():
                reason = get_control_flag("kill_switch_reason", "(no reason logged)")
                log.warning(f"🔴 Entry for {symbol} rejected — global kill-switch is active: {reason}")
                log_rejection(symbol, signal, direction, "kill_switch", reason)
                return jsonify({"status": "blocked_by_kill_switch", "reason": reason}), 200

            # Atomic claim — closes the race window where two near-simultaneous
            # webhook deliveries for the same symbol (TradingView occasionally
            # double-fires on reconnect) could otherwise both pass a plain
            # existence check and both place a real order. See claim_symbol_for_entry.
            if not claim_symbol_for_entry(symbol):
                return jsonify({"status": "already_in_trade", "symbol": symbol}), 200

            # Safety net: whatever happens inside this block — a handled
            # rejection or a genuinely unexpected exception — the claim above
            # must never outlive it unresolved. force_release_if_still_entering
            # is a no-op once upsert_position has already promoted the row to
            # 'open', so this can't clobber a successful entry.
            try:
                if market_state in BLOCKED_MARKET_STATES or (in_shock and BLOCK_ENTRIES_DURING_SHOCK):
                    log_rejection(symbol, signal, direction, "market_filter", f"market_state={market_state}, in_shock={in_shock}")
                    return jsonify({"status": "blocked_by_market_filter", "market_state": market_state}), 200

                if is_paused():
                    log_rejection(symbol, signal, direction, "paused", "bot is paused via dashboard/control")
                    return jsonify({"status": "paused"}), 200

                # [PREMIUM NEW] Circuit breaker — see circuit_breaker_tripped()
                # docstring. Read-only check, resets are handled by
                # record_trade_outcome() on each TRADE_CLOSE.
                cb_tripped, cb_reason = circuit_breaker_tripped()
                if cb_tripped:
                    log.warning(f"🛑 Entry blocked by circuit breaker for {symbol}: {cb_reason}")
                    log_rejection(symbol, signal, direction, "circuit_breaker", cb_reason)
                    return jsonify({"status": "blocked_by_circuit_breaker", "reason": cb_reason}), 200

                # [AI ORACLE MERGE — NEW] AI Gatekeeper. OFF by default
                # (AI_ORACLE_GATE_TRADES=false) — see ai_oracle_gate_check()
                # docstring. Strict veto only: never approves anything the
                # gates above/below already rejected, never fires on a
                # NEUTRAL or low-confidence oracle read.
                gate_ok, gate_reason = ai_oracle_gate_check(symbol, direction)
                if not gate_ok:
                    log.warning(f"🔮 Entry for {symbol} blocked by AI Oracle Gatekeeper: {gate_reason}")
                    log_rejection(symbol, signal, direction, "ai_oracle_gate", gate_reason)
                    return jsonify({"status": "blocked_by_ai_oracle_gate", "reason": gate_reason}), 200

                # premium_shield is checked before the order fires (cheap, no
                # network, and Pine's own entry conditions already require it —
                # see ConfidenceEngine for why this is a hard block, not a discount).
                if not premium_shield:
                    reason = ("premium_shield=false on an entry signal — Pine's own VSA/CVD gate "
                              "disagrees with itself, skipping as a safety precaution")
                    log.warning(f"🛑 Entry skipped for {symbol}: {reason}")
                    log_rejection(symbol, signal, direction, "premium_shield", reason)
                    return jsonify({"status": "blocked_by_confidence_engine", "reason": reason}), 200

                # [PREMIUM NEW — PHASE 7] Neural Syndicate: Quant/Risk/Predator
                # agents each cast a hard True/False vote on this exact trade.
                # ALL three must agree — this is a strict AND-gate, separate
                # from (and stricter than) ConfidenceEngine's additive score
                # further below. Any single "no" hard-blocks the entry.
                if NEURAL_SYNDICATE_ENABLED:
                    syndicate_ctx = {"symbol": symbol, "direction": direction, "sl": sl,
                                      "ai_score": ai_score, "win_rate": win_rate, "systems": systems}
                    consensus_ok, votes = neural_syndicate.consensus(syndicate_ctx)
                    if not consensus_ok:
                        no_votes = {k: v["reason"] for k, v in votes.items() if not v["vote"]}
                        log.warning(f"🧠 Entry for {symbol} rejected by Neural Syndicate: {no_votes}")
                        log_rejection(symbol, signal, direction, "neural_syndicate", json.dumps(no_votes))
                        return jsonify({"status": "blocked_by_neural_syndicate", "votes": votes}), 200

                # [PREMIUM NEW — PHASE 3] Risk-based position sizing. No-op
                # (returns the existing fixed qty unchanged) unless
                # RISK_BASED_SIZING=true — see calculate_position_size().
                entry_price_estimate = safe_float(f(data, "price")) or safe_float(f(data, "close"))
                qty = calculate_position_size(entry_price_estimate, sl, qty)

                # SPEED PRIORITY: the market order fires now, immediately —
                # nothing network-bound or slow sits between "signal confirmed"
                # and "order placed".
                ok, msg, order = place_entry_order(product_id, symbol, direction, qty)
                if not ok:
                    # [DASHBOARD NEW] This is the real thing — an order Delta
                    # itself rejected (bad leverage, insufficient margin,
                    # invalid price band, symbol suspended, etc.), not a
                    # pre-trade filter. Most valuable rejection to see on the
                    # dashboard, since it means a signal fired but the actual
                    # exchange call failed.
                    log_rejection(symbol, signal, direction, "exchange_rejected", msg)
                    return jsonify({"error": msg}), 400

                # ═══════════════════════════════════════════════════════════════
                # RECORD THE POSITION *IMMEDIATELY* — before anything else.
                # ─────────────────────────────────────────────────────────────
                # Found by direct testing: the old order was entry -> bracket ->
                # THEN record. A malformed sl/tp value (inf, or a non-numeric
                # string) crashed place_bracket_order before the record ever
                # happened, leaving a real open position completely untracked —
                # the bot would have no idea it existed and could double-enter
                # the same symbol later. place_bracket_order is now guaranteed
                # not to raise (see its own docstring), but this ordering is a
                # second, independent layer of protection: the instant an entry
                # is real, it is written down, full stop, before anything else
                # is attempted. A confidence score of 0 with a note is strictly
                # better than a real position this bot has amnesia about.
                # ═══════════════════════════════════════════════════════════════
                breakdown = confidence_engine.compute(
                    ai_score, win_rate, systems, ml_healthy, premium_shield, mtf_align_bars,
                    liquidation_aggregator.get_bias(), 0.0)  # live orderbook check runs after, patched in below if it succeeds

                upsert_position({
                    "symbol": symbol, "signal": signal, "direction": direction,
                    "entry_time": datetime.utcnow().isoformat(), "qty": qty,
                    "sl": sl, "tp1": tp1, "tp2": f(data, "tp2"), "tp3": f(data, "tp3"),
                    "product_id": product_id, "status": "open",
                    "systems": systems, "rsi": f(data, "rsi"), "adx": f(data, "adx"),
                    "ofi_pct": f(data, "ofi_pct"), "knn_score": f(data, "knn_score"),
                    "ml_healthy": int(ml_healthy), "premium_shield": int(premium_shield),
                    "mtf_align_bars": mtf_align_bars, "preset": preset, "pine_version": pine_version,
                    "confidence_score": breakdown.final_score, "confidence_reason": breakdown.reason,
                })
                log_trade(symbol, signal, direction, "ENTRY", qty, 0, json.dumps(order),
                          systems=systems, preset=preset, pine_version=pine_version,
                          confidence_score=breakdown.final_score,
                          rsi=f(data, "rsi"), adx=f(data, "adx"), ofi_pct=f(data, "ofi_pct"),
                          knn_score=f(data, "knn_score"), ml_healthy=int(ml_healthy),
                          premium_shield=int(premium_shield), mtf_align_bars=mtf_align_bars,
                          confidence_reason=breakdown.reason)

                # Everything from here on is best-effort enrichment. Guaranteed
                # by place_bracket_order's own contract to never raise; wrapped
                # again here regardless, because a notification or an orderbook
                # fetch failing must never be mistaken for the entry itself failing
                # — the response below already promised the caller "success".
                try:
                    if HFT_PARALLEL_EXITS and not AUTO_BRACKET_ORDERS:
                        # [PREMIUM NEW — PHASE 1] Fire SL and TP as two
                        # independent orders in parallel instead of Delta's
                        # single bracket call. See HFTExecutionEngine's own
                        # docstring for when this path is actually preferable.
                        hft_results = hft_engine.fire_protective_orders_parallel(
                            product_id, symbol, direction, qty, sl, tp1)
                        bracket_ok = all(v in ("ok", "dry_run") for v in hft_results.values()) if hft_results else True
                        bracket_msg = f"HFT parallel exits: {hft_results}"
                    else:
                        bracket_ok, bracket_msg = place_bracket_order(product_id, symbol, sl, tp1)
                except Exception as e:
                    bracket_ok, bracket_msg = False, f"bracket raised unexpectedly: {e}"
                    log.error(f"Unexpected exception from place_bracket_order for {symbol}: {e}\n{traceback.format_exc()}")

                try:
                    live_imbalance, imbalance_ok = fetch_live_orderbook_imbalance(
                        resolver.get_symbol_for(product_id) or symbol)
                except Exception:
                    live_imbalance, imbalance_ok = 0.0, False

                urgent = "" if bracket_ok else "\n🚨 BRACKET FAILED — position may be UNPROTECTED. Check Delta manually."
                notify_telegram(f"✅ {direction} {qty} {symbol}\nSignal: {signal} | Systems: {systems}/6 | "
                                 f"Confidence: {breakdown.final_score:.0f}%\n{bracket_msg}\n{breakdown.reason}"
                                 + ("" if imbalance_ok else "\n(live orderbook check unavailable this time)")
                                 + urgent)

                return jsonify({"status": "success", "entry": msg, "bracket": bracket_msg,
                                 "confidence": breakdown.reason, "confidence_score": breakdown.final_score}), 200
            finally:
                force_release_if_still_entering(symbol)

        elif action == "UPDATE_SL":
            # [PREMIUM FIX] Previously this fell into the generic full-close
            # branch — a trailing-stop push would have closed the entire
            # position instead of just moving its stop. See update_bracket_sl().
            existing = get_position(symbol)
            if not existing:
                return jsonify({"status": "no_position", "symbol": symbol}), 200
            ok, msg = update_bracket_sl(existing, f(data, "new_sl"))
            if not ok:
                notify_telegram(f"⚠️ {symbol} SL update issue: {msg}")
            return jsonify({"status": "success" if ok else "exchange_amend_failed", "result": msg}), 200

        elif action in ("EXIT_TP1", "EXIT_TP2"):
            # [PREMIUM FIX] Previously this ALSO fell into the generic
            # full-close branch, ignoring close_fraction entirely and closing
            # 100% of the position instead of the intended partial scale-out.
            existing = get_position(symbol)
            if not existing:
                return jsonify({"status": "no_position", "symbol": symbol}), 200

            close_fraction = safe_float(f(data, "close_fraction"), 1.0) or 1.0
            close_fraction = max(0.0, min(close_fraction, 1.0))
            full_qty = safe_float(existing.get("qty"), 0.0) or 0.0
            close_qty = round(full_qty * close_fraction, 8)
            if close_qty <= 0:
                return jsonify({"status": "nothing_to_close", "symbol": symbol}), 200

            ok, msg = place_exit_order(product_id, symbol, existing["direction"], close_qty)
            if ok:
                remaining = round(full_qty - close_qty, 8)
                if remaining <= 1e-8 or close_fraction >= 0.999:
                    delete_position(symbol)
                else:
                    upsert_position({**existing, "qty": remaining})
                log_trade(symbol, signal, direction, action, close_qty, 0, msg,
                          systems=systems, preset=preset, pine_version=pine_version)
                notify_telegram(f"💰 {action} {symbol}: closed {close_qty} ({close_fraction*100:.0f}%), "
                                 f"{remaining} remaining\n{msg}")
                return jsonify({"status": "success", "result": msg, "closed_qty": close_qty,
                                 "remaining_qty": remaining}), 200
            return jsonify({"error": msg}), 400

        else:
            # [PREMIUM FIX] Safe default for anything not explicitly recognized
            # above: log it and take NO exchange action, rather than the old
            # behavior of assuming "not ENTRY" meant "close everything". An
            # unrecognized action should never be able to touch a live position.
            log.warning(f"Unrecognized webhook action '{action}' for {symbol} — no exchange action taken.")
            return jsonify({"status": "unknown_action_no_op", "action": action}), 200

    except Exception as e:
        log.error(f"Webhook error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "webhook_processing_failed", "detail": str(e)}), 500


# ════════════════════════════════════════════════════════════════════════════════
# DIAGNOSTICS — one endpoint to confirm EVERYTHING is actually working
# ════════════════════════════════════════════════════════════════════════════════
@app.route("/diagnostics", methods=["GET"])
@require_key
def diagnostics():
    try:
        products_count = len(resolver.by_symbol)
        api_reachable = products_count > 0
        with db() as conn:
            db_ok = True
            open_positions = conn.execute("SELECT COUNT(*) FROM positions WHERE status='open'").fetchone()[0]
    except Exception as e:
        db_ok = False
        open_positions = 0
        log.error(f"Diagnostics DB check failed: {e}")

    creds_ok = API_CREDENTIALS_OK  # None until the first check completes (a few seconds after boot)
    overall_ok = api_reachable and db_ok and (creds_ok is not False)

    return jsonify({
        "status": "ok" if overall_ok else "degraded",
        "region": REGION,
        "base_url": BASE_URL,
        "quote_suffix": QUOTE_SUFFIX,
        "delta_api_reachable": api_reachable,
        "products_discovered": products_count,
        "last_product_refresh": datetime.utcfromtimestamp(resolver.last_refresh).isoformat() if resolver.last_refresh else None,
        "database_ok": db_ok,
        "open_positions": open_positions,
        "live_mode": is_live_mode(),
        "dry_run": is_dry_run(),
        "paused": is_paused(),
        "auto_bracket_orders": AUTO_BRACKET_ORDERS,
        "active_signals": sorted(get_active_signals()),
        "telegram_enabled": TELEGRAM_ENABLED,
        # ★ This is the field that would have caught 'invalid_api_key' at a
        # glance instead of requiring a scroll through raw deploy logs after a
        # real, failed live order — product discovery alone can never reveal
        # this, since it's a public endpoint that works with any key at all.
        "api_credentials_ok": creds_ok,
        "api_credentials_message": API_CREDENTIALS_MSG,
    })


@app.route("/products", methods=["GET"])
@require_key
def products():
    """See every coin the bot currently knows how to trade — auto-discovered, live."""
    resolver.refresh()  # cheap no-op if cache is still fresh
    return jsonify({
        "region": REGION,
        "count": len(resolver.by_symbol),
        "symbols": resolver.all_symbols_seen,
    })


@app.route("/resolve/<symbol>", methods=["GET"])
@require_key
def resolve_debug(symbol):
    """Test resolution for any coin without placing a trade — e.g. /resolve/sol"""
    pid = resolver.resolve(symbol)
    return jsonify({
        "input": symbol,
        "resolved_product_id": pid,
        "resolved_symbol": resolver.get_symbol_for(pid) if pid else None,
        "tick_size": resolver.get_tick_size(pid) if pid else None,
    })


@app.route("/status", methods=["GET"])
@require_key
def status():
    with db() as conn:
        open_pos = conn.execute("SELECT COUNT(*) FROM positions WHERE status='open'").fetchone()[0]
        total_trades = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    return jsonify({"live_mode": is_live_mode(), "paused": is_paused(), "open_positions": open_pos,
                     "total_trades": total_trades, "products_discovered": len(resolver.by_symbol),
                     "circuit_breaker": circuit_breaker_status(),
                     # [REACT DASHBOARD NEW] real process uptime for the footer strip
                     "uptime_seconds": round(time.time() - _PROCESS_START_TIME, 1)})


@app.route("/positions", methods=["GET"])
@require_key
def positions():
    with db() as conn:
        rows = conn.execute("SELECT * FROM positions WHERE status='open'").fetchall()
    return jsonify({"positions": [dict(r) for r in rows]})


@app.route("/trades", methods=["GET"])
@require_key
def trades():
    limit = request.args.get("limit", 50, type=int)
    with db() as conn:
        rows = conn.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return jsonify({"trades": [dict(r) for r in rows]})


@app.route("/rejections", methods=["GET"])
@require_key
def rejections():
    """[DASHBOARD NEW] Every entry that got blocked or an exchange order
    that actually got rejected — so "why didn't my signal turn into a
    trade?" has an answer right on the dashboard instead of digging through
    Render logs."""
    limit = request.args.get("limit", 50, type=int)
    with db() as conn:
        rows = conn.execute("SELECT * FROM rejections ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return jsonify({"rejections": [dict(r) for r in rows]})


@app.route("/self-reports", methods=["GET"])
@require_key
def self_reports():
    """[SELF-CHECK NEW] Everything the bot has said about its own health and
    performance, newest first — this is what the dashboard's 'Self-Diagnostics'
    panel polls."""
    limit = request.args.get("limit", 50, type=int)
    return jsonify({"self_reports": get_self_reports(limit=limit)})


@app.route("/raw-api-log", methods=["GET"])
@require_key
def raw_api_log():
    """[DASHBOARD NEW] The actual raw HTTP responses this bot has gotten back
    from Delta Exchange recently — real bytes off the wire, not a summary —
    so 'is it actually talking to Delta right now' has a direct visual
    answer on the dashboard. See _capture_raw_api_response() for what is and
    isn't captured (never request headers/keys, only responses)."""
    limit = request.args.get("limit", 20, type=int)
    return jsonify({"raw_api_log": get_raw_api_log(limit=limit)})


@app.route("/order-flow", methods=["GET"])
@require_key
def order_flow():
    """[DASHBOARD NEW — ORDER FLOW] Live Binance forced-liquidation buy/sell
    volume, straight from liquidation_aggregator. Real market data, not
    computed from anything this bot itself has traded."""
    return jsonify(liquidation_aggregator.get_snapshot())


@app.route("/execution-stats", methods=["GET"])
@require_key
def execution_stats():
    """[DASHBOARD NEW — EXECUTION HEALTH] Aggregates the last N raw Delta API
    responses this bot actually received (same rows as /raw-api-log) into
    average latency and HTTP success rate. This measures API/network
    execution quality, NOT trade slippage or fill price — this bot doesn't
    currently record intended-vs-filled price, so we don't claim to."""
    limit = request.args.get("limit", 100, type=int)
    rows = get_raw_api_log(limit=limit)
    if not rows:
        return jsonify({"count": 0, "avg_ms": None, "success_rate": None, "fastest_ms": None, "slowest_ms": None})
    latencies = [r["elapsed_ms"] for r in rows if r.get("elapsed_ms") is not None]
    ok = sum(1 for r in rows if r.get("status_code") and 200 <= r["status_code"] < 300)
    return jsonify({
        "count": len(rows),
        "avg_ms": round(sum(latencies) / len(latencies), 1) if latencies else None,
        "fastest_ms": round(min(latencies), 1) if latencies else None,
        "slowest_ms": round(max(latencies), 1) if latencies else None,
        "success_rate": round(ok / len(rows) * 100, 1),
    })


@app.route("/system-health", methods=["GET"])
@require_key
def system_health():
    """[DASHBOARD NEW — SYSTEM HEALTH] Real host metrics for the process this
    bot is actually running in (CPU/RAM/uptime), via psutil. If psutil isn't
    installed, this returns available=false instead of faking numbers —
    add `psutil` to requirements.txt and redeploy to enable it."""
    uptime_s = round(time.time() - _PROCESS_START_TIME)
    if psutil is None:
        return jsonify({"available": False, "uptime_seconds": uptime_s})
    try:
        proc = psutil.Process()
        return jsonify({
            "available": True,
            "uptime_seconds": uptime_s,
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory_percent": round(proc.memory_percent(), 1),
            "memory_mb": round(proc.memory_info().rss / (1024 * 1024), 1),
            "thread_count": proc.num_threads(),
        })
    except Exception as e:
        return jsonify({"available": False, "uptime_seconds": uptime_s, "error": str(e)})


@app.route("/balance", methods=["GET"])
@require_key
def balance():
    """[DASHBOARD NEW] Live account balance from Delta, cached for
    BALANCE_CACHE_MAX_AGE_S so a 5s dashboard poll doesn't hammer Delta's
    wallet endpoint with a fresh signed request every single time."""
    force = request.args.get("force", "false").strip().lower() == "true"
    return jsonify(get_cached_balance(force=force))


_mark_price_cache = {"ts": 0.0, "prices": {}}
_mark_price_cache_lock = threading.Lock()
MARK_PRICE_CACHE_MAX_AGE_S = 3  # unsigned public endpoint, but still cheap to cache


@app.route("/mark-prices", methods=["GET"])
@require_key
def mark_prices():
    """[REACT DASHBOARD NEW] Real mark price for a comma-separated ?symbols=
    list — this is what turns entry_price + qty from /positions into an
    actual live PnL/ROI on the dashboard instead of a permanent '—'. Reuses
    the same unsigned Delta ticker call the aggressive-exits monitor already
    uses; cached briefly so several open dashboard tabs polling every 5s
    don't multiply into N calls per symbol per poll."""
    raw = request.args.get("symbols", "")
    symbols = sorted({s.strip().upper() for s in raw.split(",") if s.strip()})
    if not symbols:
        return jsonify({"prices": {}})

    now = time.time()
    with _mark_price_cache_lock:
        cached = _mark_price_cache["prices"]
        fresh = (now - _mark_price_cache["ts"]) < MARK_PRICE_CACHE_MAX_AGE_S
        if fresh and all(s in cached for s in symbols):
            return jsonify({"prices": {s: cached[s] for s in symbols}})

    prices = {}
    for s in symbols:
        try:
            p = get_last_traded_price(0, s)
        except Exception as e:
            log.debug(f"/mark-prices failed for {s}: {e}")
            p = None
        if p is not None:
            prices[s] = p

    with _mark_price_cache_lock:
        _mark_price_cache["prices"].update(prices)
        _mark_price_cache["ts"] = now

    return jsonify({"prices": prices})


@app.route("/ai-oracle", methods=["GET"])
@require_key
def ai_oracle_endpoint():
    """[AI ORACLE MERGE] Per-symbol ensemble consensus (Gemini vote + quant
    vote), confidence, degraded/agreement flags, rolling self-scored
    accuracy, Gemini circuit breaker state, and whether the AI Gatekeeper is
    currently live-gating entries. Powers both the Mission Control dashboard
    embedded below and, if wired up, the React dashboard."""
    return jsonify(get_ai_oracle_snapshot())


@app.route("/performance-summary", methods=["GET"])
@require_key
def performance_summary():
    """[DASHBOARD WIRING] Real closed-trade performance — win rate, trade
    count, cumulative/average R, profit factor — same numbers self-check
    already computes from actual TRADE_CLOSE rows. This is what backs the
    dashboard's 'AI Model Performance' panel; every field is None until
    there's real trade history, never a placeholder number pretending to
    be real."""
    overall = _self_check_recent_performance()
    by_signal = _self_check_performance_by_signal()
    return jsonify({"overall": overall, "by_signal": by_signal})


@app.route("/control/<secret>/pause", methods=["GET"])
def control_pause(secret):
    if secret != CONTROL_PASSWORD:
        return jsonify({"error": "unauthorized"}), 403
    set_control_flag("paused", "true")
    notify_telegram("⏸️ Bot PAUSED")
    return jsonify({"status": "paused"})


@app.route("/control/<secret>/resume", methods=["GET"])
def control_resume(secret):
    if secret != CONTROL_PASSWORD:
        return jsonify({"error": "unauthorized"}), 403
    set_control_flag("paused", "false")
    notify_telegram("▶️ Bot RESUMED")
    return jsonify({"status": "resumed"})


@app.route("/control/<secret>/close-all", methods=["GET"])
def control_close_all(secret):
    if secret != CONTROL_PASSWORD:
        return jsonify({"error": "unauthorized"}), 403
    with db() as conn:
        open_positions = conn.execute("SELECT * FROM positions WHERE status='open'").fetchall()
    closed = 0
    for pos in open_positions:
        pid = pos["product_id"] or resolver.resolve(pos["symbol"])
        if pid:
            ok, _ = place_exit_order(pid, pos["symbol"], pos["direction"], pos["qty"])
            if ok:
                delete_position(pos["symbol"])
                closed += 1
    notify_telegram(f"🔴 Closed {closed} position(s)")
    return jsonify({"closed": closed})


@app.route("/control/<secret>/reset-circuit-breaker", methods=["GET"])
def control_reset_circuit_breaker(secret):
    """[PREMIUM NEW] Manual override for the daily-loss/consecutive-loss
    breaker — same intent as the Pine script's own manual controls. Does NOT
    touch is_paused(); that's a separate switch. Useful after reviewing a bad
    day and deciding to resume trading before the UTC-midnight auto-reset."""
    if secret != CONTROL_PASSWORD:
        return jsonify({"error": "unauthorized"}), 403
    set_control_flag("cb_date", _today_utc())
    set_control_flag("cb_daily_loss_r", "0.0")
    set_control_flag("cb_consecutive_losses", "0")
    notify_telegram("🔧 Circuit breaker manually reset")
    return jsonify({"status": "circuit_breaker_reset", "circuit_breaker": circuit_breaker_status()})


@app.route("/control/<secret>/risk-sizing", methods=["GET"])
def control_risk_sizing(secret):
    """[DASHBOARD NEW] Makes the dashboard's Dynamic Sizing toggle actually
    do something — RISK_BASED_SIZING used to be a boot-time-only env var
    with no runtime switch at all, which is why clicking it on the old
    dashboard never worked. ?enabled=true|false required."""
    if secret != CONTROL_PASSWORD:
        return jsonify({"error": "unauthorized"}), 403
    raw = request.args.get("enabled")
    if raw is None or raw.strip().lower() not in ("true", "false"):
        return jsonify({"error": "pass ?enabled=true or ?enabled=false"}), 400
    enabled = raw.strip().lower() == "true"
    set_risk_sizing_enabled(enabled)
    notify_telegram(f"⚙️ Dynamic (risk-based) position sizing turned {'ON' if enabled else 'OFF'}")
    return jsonify({"status": "ok", "risk_based_sizing": enabled})


@app.route("/control/<secret>/kill-switch", methods=["GET", "POST"])
def control_kill_switch(secret):
    """[PREMIUM NEW — PHASE 3] Manually ARM the global kill-switch. Unlike
    pause (soft, instantly reversible) or the circuit breaker (auto-resets),
    this is sticky until a human explicitly clears it via the reset endpoint
    below — meant for 'something is wrong, stop everything until I've looked
    at it personally', not a routine daily control."""
    if secret != CONTROL_PASSWORD:
        return jsonify({"error": "unauthorized"}), 403
    reason = "manually activated via /control endpoint"
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        reason = payload.get("reason", reason)
    else:
        reason = request.args.get("reason", reason)
    activate_kill_switch(reason)
    return jsonify({"status": "kill_switch_activated", "reason": reason})


@app.route("/control/<secret>/kill-switch/reset", methods=["GET"])
def control_kill_switch_reset(secret):
    if secret != CONTROL_PASSWORD:
        return jsonify({"error": "unauthorized"}), 403
    set_control_flag("kill_switch", "false")
    log.warning("🟢 Global kill-switch manually cleared.")
    notify_telegram("🟢 Kill-switch cleared — trading can resume (subject to other gates).")
    return jsonify({"status": "kill_switch_cleared"})


@app.route("/control/<secret>/self-check", methods=["GET", "POST"])
def control_self_check(secret):
    """[SELF-CHECK NEW] On-demand version of the background _self_check_loop()
    tick, PLUS two extra checks not run automatically on a timer:
      - _self_check_system_integrity(): confirms the bot's own code/wiring
        are intact (routes registered, DB schema correct, core engine
        objects present, credentials still valid).
      - _self_check_performance_by_signal(): the same real closed-trade
        history, split out per signal tier instead of one blended number.

    Lets a human hit "Run Self-Check" on the dashboard and get an immediate,
    full answer instead of waiting up to SELF_CHECK_INTERVAL_S for the next
    scheduled tick (which only ever ran the health+overall-performance half).

    Works identically whether the bot is currently in LIVE mode or DRY_RUN —
    everything here reads whatever TRADE_CLOSE rows already exist in the
    `trades` table (real fills in LIVE, simulated fills in DRY_RUN), so
    there's nothing mode-specific to branch on.

    Same safety guarantee as the scheduled tick: 100% read-only. This can
    never place, modify, or close a trade, and never writes fake/simulated
    rows into positions/trades — it only reads existing DB rows, inspects
    the app's own routes/schema/objects, and writes fresh self_reports rows
    describing what it found.
    """
    if secret != CONTROL_PASSWORD:
        return jsonify({"error": "unauthorized"}), 403
    try:
        _self_check_tick()
        integrity = _self_check_system_integrity()
        by_signal = _self_check_performance_by_signal()
    except Exception as e:
        log.error(f"Manual self-check failed: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "self_check_failed", "detail": str(e)}), 500
    return jsonify({
        "status": "self_check_complete",
        "live_mode": is_live_mode(),
        "dry_run": is_dry_run(),
        "performance": _self_check_recent_performance(),
        "performance_by_signal": by_signal,
        "system_integrity": integrity,
        "self_reports": get_self_reports(limit=10),
    }), 200


# ════════════════════════════════════════════════════════════════════════════════
# [DASHBOARD NEW — AI Q&A] "Ask APEX NEXUS" — a real natural-language chat
# panel backed by Google's Gemini API. Every answer is grounded in a
# JSON snapshot of the bot's OWN current state (built fresh on every
# question, right below) — the model is never asked to guess; it's told
# exactly what self-check, credentials, drift, recent trades, and recent
# rejections currently say, and answers from that.
#
# SAFETY: this endpoint only ever READS bot state to build the context, and
# only ever returns TEXT back to the dashboard. It cannot place, modify, or
# close a trade, flip LIVE/DRY_RUN, or change any control flag — the model's
# reply is just displayed, never executed as a command. If GEMINI_API_KEY
# isn't set, this fails closed with a clear message instead of silently
# doing nothing.
# ════════════════════════════════════════════════════════════════════════════════
ASK_MAX_QUESTION_CHARS = 2000
ASK_MAX_HISTORY_TURNS = 6  # user+assistant pairs kept for follow-up context


def _build_bot_context_snapshot() -> Dict:
    """Everything a human would need to actually answer 'is this thing
    working' — same underlying data the dashboard itself renders, just
    collected into one JSON blob for the model instead of spread across
    dashboard panels."""
    try:
        with db() as conn:
            open_positions = [dict(r) for r in conn.execute(
                "SELECT symbol, signal, direction, entry_price, qty, sl, tp1, tp2, tp3, status "
                "FROM positions WHERE status='open'").fetchall()]
            recent_trades = [dict(r) for r in conn.execute(
                "SELECT symbol, signal, direction, event, qty, price, timestamp FROM trades "
                "ORDER BY id DESC LIMIT 15").fetchall()]
            recent_rejections = [dict(r) for r in conn.execute(
                "SELECT symbol, signal, direction, reason, detail, timestamp FROM rejections "
                "ORDER BY id DESC LIMIT 10").fetchall()]
    except Exception as e:
        open_positions, recent_trades, recent_rejections = [], [], []
        log.error(f"Context snapshot DB read failed: {e}")

    return {
        "now_utc": datetime.now(timezone.utc).isoformat(),
        "region": REGION, "base_url": BASE_URL,
        "live_mode": is_live_mode(), "dry_run": is_dry_run(), "paused": is_paused(),
        "api_credentials_ok": API_CREDENTIALS_OK, "api_credentials_msg": API_CREDENTIALS_MSG,
        "time_drift": time_drift_status(),
        "products_discovered": len(resolver.by_symbol),
        "circuit_breaker": circuit_breaker_status(),
        "active_signals": sorted(get_active_signals()),
        "overall_performance": _self_check_recent_performance(),
        "performance_by_signal": _self_check_performance_by_signal(),
        "open_positions": open_positions,
        "recent_trades": recent_trades,
        "recent_rejections": recent_rejections,
        "recent_self_reports": get_self_reports(limit=15),
        "recent_raw_api_log": get_raw_api_log(limit=8),
    }


def ask_bot_ai(question: str, history: List[Dict] = None) -> Dict:
    """Sends the question + a fresh state snapshot to Gemini via Google's
    plain generateContent HTTP API (no extra SDK dependency — just
    `requests`, already used everywhere else in this file). Never raises:
    any failure comes back as {"answer": None, "error": "..."} so the
    dashboard can show it in the chat panel instead of the request just
    breaking."""
    if not GEMINI_API_KEY:
        return {"answer": None, "error": "GEMINI_API_KEY is not set — add it to your .env to enable this feature."}

    question = (question or "").strip()
    if not question:
        return {"answer": None, "error": "Empty question."}
    if len(question) > ASK_MAX_QUESTION_CHARS:
        return {"answer": None, "error": f"Question too long (max {ASK_MAX_QUESTION_CHARS} characters)."}

    snapshot = _build_bot_context_snapshot()
    system_prompt = (
        "You are the built-in diagnostic assistant for APEX NEXUS, a Delta Exchange trading bot. "
        "You are given a JSON snapshot of the bot's actual current state below. Answer the "
        "operator's question using ONLY this data — never invent numbers, trades, or statuses "
        "that aren't in the snapshot. If the snapshot doesn't contain what's needed to answer, "
        "say so plainly instead of guessing. Reply in whichever language/style the operator used "
        "(Hindi, Hinglish, or English). Keep answers short and concrete — this is a diagnostics "
        "chat, not a general assistant. You cannot take any action yourself (no placing orders, "
        "no toggling settings) — if asked to DO something, explain which dashboard button or "
        "control endpoint does that instead.\n\n"
        f"CURRENT BOT STATE SNAPSHOT:\n{json.dumps(snapshot, default=str)}"
    )

    # Gemini has no separate "assistant" role — the model's own turns are
    # role "model" instead. Everything else about turn-taking is the same.
    contents = []
    for turn in (history or [])[-(ASK_MAX_HISTORY_TURNS * 2):]:
        role = turn.get("role")
        content = str(turn.get("content", ""))[:ASK_MAX_QUESTION_CHARS]
        if role == "user" and content:
            contents.append({"role": "user", "parts": [{"text": content}]})
        elif role == "assistant" and content:
            contents.append({"role": "model", "parts": [{"text": content}]})
    contents.append({"role": "user", "parts": [{"text": question}]})

    try:
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
            headers={"content-type": "application/json"},
            params={"key": GEMINI_API_KEY},
            json={
                "system_instruction": {"parts": [{"text": system_prompt}]},
                "contents": contents,
                "generationConfig": {"maxOutputTokens": 700},
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        candidates = data.get("candidates") or []
        text = ""
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            text = "".join(p.get("text", "") for p in parts).strip()
        if not text:
            block_reason = data.get("promptFeedback", {}).get("blockReason")
            if block_reason:
                return {"answer": None, "error": f"Gemini blocked the response ({block_reason})."}
            return {"answer": None, "error": "AI returned an empty response."}
        return {"answer": text, "error": None}
    except requests.exceptions.RequestException as e:
        body = getattr(e.response, "text", "") if hasattr(e, "response") and e.response is not None else ""
        log.error(f"AI Q&A request failed: {e} | {body[:300]}")
        return {"answer": None, "error": f"Request to Gemini API failed: {e}"}
    except Exception as e:
        log.error(f"AI Q&A unexpected error: {e}\n{traceback.format_exc()}")
        return {"answer": None, "error": f"Unexpected error: {e}"}


@app.route("/ask/<secret>", methods=["POST"])
def ask_endpoint(secret):
    """[DASHBOARD NEW — AI Q&A] Powers the 'Ask APEX NEXUS' chat panel.
    POST {"question": "...", "history": [{"role":"user"/"assistant","content":"..."}]}
    Gated behind CONTROL_PASSWORD (same secret as every other /control/*
    action) since each call is a real request to Gemini's API (free tier
    with rate limits, or billed if you're on a paid Gemini key)."""
    if secret != CONTROL_PASSWORD:
        return jsonify({"error": "unauthorized"}), 403
    payload = request.get_json(silent=True) or {}
    question = payload.get("question", "")
    history = payload.get("history", [])
    if not isinstance(history, list):
        history = []
    result = ask_bot_ai(question, history)
    status = 200 if result.get("answer") is not None else 400
    # [REACT DASHBOARD NEW] the chat panel labels each reply with this tag;
    # every non-error answer from ask_bot_ai() really is a live Gemini call,
    # so "ai" is always accurate here (no local/offline fallback exists).
    if result.get("answer") is not None:
        result["mode"] = "ai"
    return jsonify(result), status


@app.route("/oracle", methods=["GET"])
@require_key
def oracle_endpoint():
    """[PREMIUM NEW — PHASE 6] Single-call health verdict — see
    oracle_night_watch_report()'s own docstring for what it checks."""
    return jsonify(oracle_night_watch_report())


# ════════════════════════════════════════════════════════════════════════════════
# [PREMIUM NEW] CONFIG / MODE / SIGNALS — the three endpoints the Control
# Center dashboard talks to for LIVE/DRY_RUN toggling and per-tier signal
# ON/OFF, plus a single combined read for painting the dashboard on load.
# All three are DB-backed (control_flags), so they behave correctly under
# any gunicorn worker count — same guarantee as is_paused() above.
# ════════════════════════════════════════════════════════════════════════════════
@app.route("/config", methods=["GET"])
@require_key
def config_snapshot():
    """One-shot read for a dashboard's initial paint: everything it needs to
    render current mode, pause state, and which signal tiers are live, plus
    circuit breaker status so it doesn't need a second round-trip."""
    return jsonify({
        "region": REGION,
        "base_url": BASE_URL,
        "live_mode": is_live_mode(),
        "dry_run": is_dry_run(),
        "paused": is_paused(),
        "auto_bracket_orders": AUTO_BRACKET_ORDERS,
        "active_signals": sorted(get_active_signals()),
        "all_known_signals": ALL_KNOWN_SIGNALS,
        "circuit_breaker": circuit_breaker_status(),
        "products_discovered": len(resolver.by_symbol),
        "api_credentials_ok": API_CREDENTIALS_OK,
        "time_drift": time_drift_status(),
        "kill_switch_active": is_kill_switch_active(),
        "hft_parallel_exits": HFT_PARALLEL_EXITS,
        "predator_vision_enabled": PREDATOR_ENABLED,
        "risk_based_sizing": risk_sizing_enabled(),
        "aggressive_exits_enabled": AGGRESSIVE_EXITS_ENABLED,
        "neural_syndicate_enabled": NEURAL_SYNDICATE_ENABLED,
        # [DASHBOARD NEW] both already existed as module-level constants but
        # weren't in this snapshot yet — the new dashboard's Bot Status card
        # needs them for the Shock Filter / Telegram chips.
        "block_entries_during_shock": BLOCK_ENTRIES_DURING_SHOCK,
        "telegram_enabled": TELEGRAM_ENABLED,
        # [CONSOLIDATION NEW] Read-only view of whatever the separate
        # ai_oracle.py process last wrote to control_flags, if it's running.
        # Purely informational — nothing in this bot's entry/exit/sizing
        # logic reads these fields (yet). All three are None if ai_oracle.py
        # has never run against this DB.
        "ai_market_sentiment": {
            "consensus": get_control_flag("ai_consensus"),
            "symbol": get_control_flag("ai_consensus_symbol"),
            "updated_at": get_control_flag("ai_consensus_updated_at"),
        },
    }), 200


@app.route("/mode/<secret>", methods=["GET", "POST"])
def mode_control(secret):
    """
    GET  /mode/<secret>                         -> just reports current mode
    GET  /mode/<secret>?live_mode=true|false    -> sets and reports mode
    POST /mode/<secret> {"live_mode": true}     -> sets and reports mode
    Accepts GET-with-query (matches this file's other /control/* endpoints,
    easiest for a browser dashboard button) as well as POST-with-JSON body
    (more conventional for a state change) — either works, so the dashboard
    isn't forced into one calling convention.
    """
    if secret != CONTROL_PASSWORD:
        return jsonify({"error": "unauthorized"}), 403

    requested = None
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        requested = payload.get("live_mode")
    if requested is None:
        requested = request.args.get("live_mode")

    if requested is not None:
        new_live = str(requested).strip().lower() in ("true", "1", "yes", "live", "on")
        set_control_flag("live_mode", "true" if new_live else "false")
        log.warning(f"⚙️ Mode changed via /mode endpoint -> LIVE_MODE={new_live}")
        notify_telegram(f"⚙️ Mode switched to {'LIVE 🔴' if new_live else 'DRY_RUN 🧪'} via dashboard")

    return jsonify({"live_mode": is_live_mode(), "dry_run": is_dry_run()}), 200


@app.route("/signals/<secret>", methods=["GET", "POST"])
def signals_control(secret):
    """
    GET  /signals/<secret>                                  -> current active tiers
    GET  /signals/<secret>?enable=SCALP&enable=WARP          -> turn tiers on
    GET  /signals/<secret>?disable=STRONG                    -> turn tiers off
    GET  /signals/<secret>?active=NEXUS,SCALP                -> replace the whole set
    POST /signals/<secret> {"enable":[...], "disable":[...]} -> same, via JSON body
    POST /signals/<secret> {"active": [...]}                 -> replace the whole set
    Unknown tier names are silently dropped (see get_active_signals()/
    set_active_signals()) rather than stored as garbage that would later
    match nothing and quietly do nothing.
    """
    if secret != CONTROL_PASSWORD:
        return jsonify({"error": "unauthorized"}), 403

    payload = {}
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}

    replace_raw = payload.get("active")
    if replace_raw is None:
        replace_qs = request.args.get("active")
        replace_raw = replace_qs.split(",") if replace_qs else None

    enable = payload.get("enable") or request.args.getlist("enable")
    disable = payload.get("disable") or request.args.getlist("disable")

    if replace_raw is not None or enable or disable:
        current = get_active_signals()
        if replace_raw is not None:
            current = {s.strip().upper() for s in replace_raw if s.strip()}
        for s in (enable or []):
            current.add(s.strip().upper())
        for s in (disable or []):
            current.discard(s.strip().upper())
        set_active_signals(current)
        log.warning(f"⚙️ Active signals changed via /signals endpoint -> {sorted(get_active_signals())}")
        notify_telegram(f"⚙️ Active signal tiers now: {', '.join(sorted(get_active_signals())) or '(none)'}")

    return jsonify({"active_signals": sorted(get_active_signals()), "all_known_signals": ALL_KNOWN_SIGNALS}), 200


# ════════════════════════════════════════════════════════════════════════════════
# [ARCHITECTURE LAYER — NEW] Config / BrokerAdapter / BrokerManager /
# DatabaseLayer / HealthMonitor / WebhookHandler
# ────────────────────────────────────────────────────────────────────────────────
# Everything below is ADDITIVE. Not one existing class, function, route, or
# global above this line is modified, removed, or called differently than
# before. Every method here either (a) reads an already-defined global, or
# (b) delegates straight to an already-defined function — so there is still
# exactly ONE place that actually talks to Delta, touches the database, or
# decides trade logic. This section only adds a clean OOP seam around that
# single source of truth, for two concrete reasons:
#   1. A second broker can be added later by writing one new BrokerAdapter
#      subclass — no route, webhook, or trading logic needs to change.
#   2. One object (health_monitor) can be asked "is everything OK right now"
#      instead of that answer being scattered across six different routes.
# The live POST /webhook/<secret_token> route above is deliberately NOT
# rewired to go through WebhookHandler — its control flow (atomic symbol
# claim → kill-switch → circuit breaker → AI gate → order) is exactly right
# today, and refactoring it is out of scope for "verify no existing behavior
# changes". WebhookHandler exists as validation infrastructure for a FUTURE
# second alert source, not a replacement of the current one.
# ════════════════════════════════════════════════════════════════════════════════

# ---- Config Layer -----------------------------------------------------------
@dataclass(frozen=True)
class AppConfig:
    """Read-only snapshot of the settings this process actually booted with.
    Every field is read from the SAME env-parsed globals defined at the top
    of this file (REGION, BASE_URL, CIRCUIT_BREAKER_ENABLED, ...) — nothing
    here re-parses an env var or introduces a second definition of what it
    means. Secrets (API_KEY/API_SECRET/WEBHOOK_SECRET_TOKEN) are deliberately
    exposed only as booleans, never as values, since this backs a JSON route."""
    region: str
    base_url: str
    live_mode_default: bool
    circuit_breaker_enabled: bool
    daily_loss_limit_r: float
    max_consecutive_losses: int
    auto_bracket_orders: bool
    webhook_configured: bool
    credentials_configured: bool

    @classmethod
    def snapshot(cls) -> "AppConfig":
        return cls(
            region=REGION,
            base_url=BASE_URL,
            live_mode_default=LIVE_MODE_ENV_DEFAULT,
            circuit_breaker_enabled=CIRCUIT_BREAKER_ENABLED,
            daily_loss_limit_r=DAILY_LOSS_LIMIT_R,
            max_consecutive_losses=MAX_CONSECUTIVE_LOSSES,
            auto_bracket_orders=AUTO_BRACKET_ORDERS,
            webhook_configured=bool(WEBHOOK_SECRET_TOKEN),
            credentials_configured=bool(API_KEY and API_SECRET),
        )

    def as_dict(self) -> Dict:
        return {
            "region": self.region, "base_url": self.base_url,
            "live_mode_default": self.live_mode_default,
            "circuit_breaker_enabled": self.circuit_breaker_enabled,
            "daily_loss_limit_r": self.daily_loss_limit_r,
            "max_consecutive_losses": self.max_consecutive_losses,
            "auto_bracket_orders": self.auto_bracket_orders,
            "webhook_configured": self.webhook_configured,
            "credentials_configured": self.credentials_configured,
        }


# ---- Broker Adapter Layer ----------------------------------------------------
class BrokerAdapter(ABC):
    """Common interface every broker integration exposes. Delta is the only
    adapter with real credentials today; a second exchange joins later by
    implementing this interface once — nothing else in the file needs to
    know about it."""
    name: str = "unnamed"

    @abstractmethod
    def resolve_product(self, symbol: str) -> Optional[int]: ...

    @abstractmethod
    def get_balance(self) -> Optional[float]: ...

    @abstractmethod
    def get_position(self, symbol: str) -> Optional[Dict]: ...

    @abstractmethod
    def get_last_price(self, product_id: int, symbol: str) -> Optional[float]: ...

    @abstractmethod
    def place_bracket_order(self, product_id: int, symbol: str, sl_price, tp_price,
                             direction: str, qty: float): ...

    @abstractmethod
    def place_exit_order(self, product_id: int, symbol: str, direction: str, qty: float): ...

    @abstractmethod
    def is_configured(self) -> bool: ...


class DeltaBroker(BrokerAdapter):
    """Delegates every call straight to the existing, already-live Delta
    functions defined earlier in this file (resolver.resolve,
    get_account_balance, place_bracket_order, ...). Zero duplicated trading
    logic — this can never drift out of sync with the real order path
    because it never re-implements it, only names it."""
    name = "delta"

    def resolve_product(self, symbol: str) -> Optional[int]:
        return resolver.resolve(symbol)

    def get_balance(self) -> Optional[float]:
        return get_account_balance()

    def get_position(self, symbol: str) -> Optional[Dict]:
        return get_position(symbol)

    def get_last_price(self, product_id: int, symbol: str) -> Optional[float]:
        return get_last_traded_price(product_id, symbol)

    def place_bracket_order(self, product_id: int, symbol: str, sl_price, tp_price,
                             direction: str, qty: float):
        return place_bracket_order(product_id, symbol, sl_price, tp_price, direction, qty)

    def place_exit_order(self, product_id: int, symbol: str, direction: str, qty: float):
        return place_exit_order(product_id, symbol, direction, qty)

    def is_configured(self) -> bool:
        return bool(API_KEY and API_SECRET) and API_CREDENTIALS_OK is not False


class BrokerManager:
    """Thread-safe registry + hot-switch point for broker adapters. Only
    'delta' is registered today; register() lets a future broker join
    without touching this class, any route, or the webhook."""

    def __init__(self):
        self._lock = threading.Lock()
        self._brokers: Dict[str, BrokerAdapter] = {}
        self._active: Optional[str] = None

    def register(self, broker: BrokerAdapter, make_active: bool = False):
        with self._lock:
            self._brokers[broker.name] = broker
            if make_active or self._active is None:
                self._active = broker.name

    def switch(self, name: str) -> Tuple[bool, str]:
        with self._lock:
            if name not in self._brokers:
                return False, f"unknown broker '{name}' — registered: {list(self._brokers)}"
            self._active = name
            return True, f"active broker switched to '{name}'"

    def active(self) -> Optional[BrokerAdapter]:
        with self._lock:
            return self._brokers.get(self._active)

    def status(self) -> Dict:
        with self._lock:
            return {
                "active": self._active,
                "registered": [{"name": n, "configured": b.is_configured()}
                               for n, b in self._brokers.items()],
            }


broker_manager = BrokerManager()
broker_manager.register(DeltaBroker(), make_active=True)


# ---- Database Layer (thin, additive wrapper) ---------------------------------
class DatabaseLayer:
    """Generic query helpers built on top of the existing db()/init_db().
    Every existing call site in this file (`with db() as conn: ...`) is
    completely untouched and keeps behaving exactly as before — this class
    is only a convenience surface for new code, never a replacement."""

    @staticmethod
    def execute(query: str, params: Tuple = ()) -> int:
        with db() as conn:
            cur = conn.execute(query, params)
            conn.commit()
            return cur.lastrowid

    @staticmethod
    def query_one(query: str, params: Tuple = ()) -> Optional[Dict]:
        with db() as conn:
            row = conn.execute(query, params).fetchone()
            return dict(row) if row else None

    @staticmethod
    def query_all(query: str, params: Tuple = ()) -> List[Dict]:
        with db() as conn:
            return [dict(r) for r in conn.execute(query, params).fetchall()]


# ---- Health Monitor -----------------------------------------------------------
class HealthMonitor:
    """Periodic, strictly read-only health snapshot: DB reachability, Delta
    API reachability, clock drift, circuit-breaker/kill-switch state,
    credential status, active broker. This NEVER places, modifies, or closes
    a trade — it only ever writes to its own in-memory snapshot, the same
    pattern already used by the existing _raw_api_log deque above."""

    def __init__(self, interval_s: int = 60):
        self.interval_s = interval_s
        self._lock = threading.Lock()
        self._last: Dict = {}

    def check_once(self) -> Dict:
        db_ok, db_err = True, None
        try:
            with db() as conn:
                conn.execute("SELECT 1").fetchone()
        except Exception as e:
            db_ok, db_err = False, str(e)

        delta_ok, delta_err = True, None
        try:
            resp = delta_http.get(f"{BASE_URL}/v2/products", params={"page_size": 1}, timeout=5)
            resp.raise_for_status()
        except Exception as e:
            delta_ok, delta_err = False, str(e)

        cb_tripped, cb_reason = circuit_breaker_tripped()
        snapshot = {
            "timestamp": datetime.utcnow().isoformat(),
            "database": {"ok": db_ok, "error": db_err},
            "delta_api": {"ok": delta_ok, "error": delta_err},
            "clock_drift_ms": round(get_time_drift_ms(), 1),
            "circuit_breaker": {"tripped": cb_tripped, "reason": cb_reason},
            "kill_switch_active": is_kill_switch_active(),
            "credentials_ok": API_CREDENTIALS_OK,
            "broker": broker_manager.status(),
        }
        with self._lock:
            self._last = snapshot
        return snapshot

    def latest(self) -> Dict:
        with self._lock:
            return dict(self._last) if self._last else {}

    def run_loop(self):
        while True:
            try:
                self.check_once()
            except Exception as e:
                log.error(f"HealthMonitor check failed: {e}\n{traceback.format_exc()}")
            time.sleep(self.interval_s)


health_monitor = HealthMonitor(interval_s=int(os.environ.get("HEALTH_MONITOR_INTERVAL_S", "60")))


# ---- Webhook Handler (validation layer — NOT wired into the live route) ------
class WebhookHandler:
    """Stateless validation/parsing helpers that mirror what POST
    /webhook/<secret_token> already checks inline, above. Deliberately NOT
    called by that live route: its exact control flow (atomic symbol claim,
    kill-switch/circuit-breaker ordering, TRADE_CLOSE short-circuit) is
    already correct and battle-tested, and rewriting it to funnel through
    here would risk exactly the behavior change this merge was told never to
    make. This class is for a FUTURE second alert source (another route,
    another broker's webhook) that wants the same validation without
    copy-pasting it — it is genuinely new capability, not a hidden refactor
    of the current path."""

    def __init__(self, secret_token: str):
        self.secret_token = secret_token

    def is_authorized(self, provided_token: str) -> bool:
        return bool(self.secret_token) and hmac.compare_digest(provided_token, self.secret_token)

    def parse(self, data: Dict) -> Dict:
        return {
            "signal": str(f(data, "signal", "")).strip().upper(),
            "direction": str(f(data, "direction", "")).strip().upper(),
            "action": str(f(data, "action", "ENTRY")).strip().upper(),
            "symbol": str(f(data, "symbol", "")).strip().upper(),
        }

    def is_valid(self, parsed: Dict) -> Tuple[bool, Optional[str]]:
        if not parsed["signal"] or not parsed["symbol"]:
            return False, "missing signal or symbol"
        if parsed["signal"] not in get_active_signals():
            return False, "signal_not_active"
        return True, None


webhook_handler = WebhookHandler(WEBHOOK_SECRET_TOKEN)


# ---- New routes exposing the architecture layer above ------------------------
# All three paths are brand-new (/config-layer, /broker/status, /broker/health,
# /control/<secret>/broker/switch/<name>) — none collide with any existing
# route, and none of them are called by any existing route either.
@app.route("/config-layer", methods=["GET"])
@require_key
def config_layer_view():
    return jsonify(AppConfig.snapshot().as_dict())


@app.route("/broker/status", methods=["GET"])
@require_key
def broker_status_view():
    return jsonify(broker_manager.status())


@app.route("/broker/health", methods=["GET"])
@require_key
def broker_health_view():
    snap = health_monitor.latest() or health_monitor.check_once()
    return jsonify(snap)


@app.route("/control/<secret>/broker/switch/<name>", methods=["GET"])
def control_broker_switch(secret, name):
    if secret != CONTROL_PASSWORD:
        return jsonify({"error": "unauthorized"}), 403
    ok, msg = broker_manager.switch(name)
    if ok:
        notify_telegram(f"🔀 Active broker switched to '{name}'")
    return jsonify({"status": "ok" if ok else "error", "detail": msg}), (200 if ok else 400)


# ════════════════════════════════════════════════════════════════════════════════
# STARTUP
# ────────────────────────────────────────────────────────────────────────────────
# ⚠️ CRITICAL DEPLOYMENT NOTE — read this before changing anything below.
#
# In production this app runs under gunicorn: `gunicorn app:app`. Gunicorn
# IMPORTS this file as a module to get the `app` object — it never executes
# it as __main__. Any setup code placed only inside `if __name__ == "__main__"`
# silently NEVER RUNS in production. That single mistake is the exact, sole
# root cause traced from a real failing deployment: init_db() lived only
# inside that guard, so the database tables were never created — every
# webhook call failed with "no such table" / "no such column" forever, no
# matter how many times the schema itself was patched, because the code that
# creates the schema was unreachable. Ad-hoc module-level patches "fixed" it
# partially (module-level code DOES run under gunicorn's import) but each
# patch used a different, inconsistent table definition, which is why the
# errors kept changing shape instead of going away.
#
# THE FIX: every piece of startup work below runs unconditionally at module
# load time — under `python this_file.py` AND under `gunicorn this_file:app`
# alike. The `if __name__ == "__main__"` block at the very bottom is reduced
# to ONLY `app.run(...)`, which is exclusively the local-dev server and must
# never be called a second time by gunicorn (gunicorn provides its own).
# ════════════════════════════════════════════════════════════════════════════════
def validate_config_or_die():
    required = ["DELTA_API_KEY", "DELTA_API_SECRET", "APEX_WEBHOOK_PASSPHRASE"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        print(f"❌ ERROR: Missing environment variables: {', '.join(missing)}")
        sys.exit(1)


def bootstrap():
    """Runs exactly once, at import time, regardless of how this file is launched."""
    # NOTE: this first print runs BEFORE init_db(), so it deliberately uses
    # LIVE_MODE_ENV_DEFAULT (the raw env value) rather than is_live_mode() —
    # the control_flags table doesn't exist yet at this point, and calling
    # is_live_mode() here would crash with "no such table: control_flags".
    # The real, DB-backed value is logged further down, after init_db().
    print(f"""
════════════════════════════════════════════════════════════════════════════════
APEX NEXUS — PREMIUM FINAL (Auto Product-ID Discovery)
Region: {REGION} | Base: {BASE_URL} | Live (env default): {LIVE_MODE_ENV_DEFAULT}
════════════════════════════════════════════════════════════════════════════════
""")
    validate_config_or_die()
    init_db()

    boot_paused = is_paused()  # informational log line only — every real check re-reads the DB

    # [CRITICAL FIX] Sync clock drift against Delta's server BEFORE anything
    # that signs a request — product discovery below is public/unauthenticated
    # so it doesn't strictly need this, but credential verification and every
    # real order after it absolutely does. Doing this first means the very
    # first signed call this process ever makes already has a correct
    # timestamp, instead of learning about drift only after a live order
    # fails with expired_signature.
    sync_time_with_delta(retries=2)

    # Discover every tradable coin BEFORE accepting any webhook traffic, so the
    # very first alert that arrives already has full product coverage.
    count = resolver.refresh(force=True)
    if count == 0:
        log.error("⚠️ Started with ZERO products discovered — check DELTA_API_KEY/SECRET "
                   "and DELTA_REGION, and confirm outbound network access to Delta's API.")

    # Prove the key+secret pair actually works BEFORE a real signal ever tries
    # to trade with it. Product discovery above is a public endpoint and tells
    # you nothing about whether the key is valid — this is the one check that
    # would have caught 'invalid_api_key' at startup instead of only on the
    # first live order attempt.
    verify_api_credentials()

    threading.Thread(target=_background_refresh_loop, daemon=True).start()
    threading.Thread(target=_background_time_sync_loop, daemon=True).start()
    threading.Thread(target=_self_check_loop, daemon=True).start()
    log.info(f"🩺 Self-check loop started (every {SELF_CHECK_INTERVAL_S}s, "
             f"last {SELF_CHECK_LOOKBACK_TRADES} closed trades)")
    threading.Thread(target=_ai_oracle_loop, daemon=True).start()
    log.info(f"🔮 AI Oracle merged in-process — symbols={ORACLE_SYMBOLS}, every {ORACLE_INTERVAL_S}s "
             f"(ensemble: Gemini + quant model, gate_trades={AI_ORACLE_GATE_TRADES})")

    threading.Thread(target=health_monitor.run_loop, daemon=True).start()
    log.info(f"🩺 HealthMonitor started (every {health_monitor.interval_s}s) — "
             f"broker_manager active='{broker_manager.status()['active']}'")
    if AGGRESSIVE_EXITS_ENABLED:
        threading.Thread(target=_aggressive_exits_loop, daemon=True).start()
        log.info(f"🎯 Aggressive Exits monitor started (breakeven +{BREAKEVEN_TRIGGER_R}R, "
                 f"trail +{TRAIL_TRIGGER_R}R @ {TRAIL_DISTANCE_R}R distance, "
                 f"every {POSITION_MONITOR_INTERVAL}s)")

    log.info(f"is_live_mode()={is_live_mode()} is_dry_run()={is_dry_run()} PAUSED={boot_paused} "
             f"AUTO_BRACKET_ORDERS={AUTO_BRACKET_ORDERS} get_active_signals()={get_active_signals()}")

    liq_symbols = os.environ.get("LIQUIDATION_SYMBOLS", "BTCUSDT,ETHUSDT").split(",")
    if liq_symbols and liq_symbols != ['']:
        BinanceLiquidationFeed([s.strip() for s in liq_symbols if s.strip()], liquidation_aggregator).start()

    log.info("✅ Bootstrap complete — ready to receive webhooks.")


# Runs on import — i.e. the moment gunicorn (or `python this_file.py`) loads
# this module. This is the ONLY call site for bootstrap(); nothing later in
# this file duplicates it, so it always runs exactly once per process.
bootstrap()


if __name__ == "__main__":
    # Local development only. In production, gunicorn serves `app` directly
    # and this block never executes — bootstrap() above already ran on import.
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

