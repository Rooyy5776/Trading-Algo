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
import threading
import traceback
import statistics
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, timezone

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
    return conn


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
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>APEX NEXUS · Mission Control</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700;800&family=Orbitron:wght@600;700;800;900&display=swap" rel="stylesheet">
<style>
  :root{
    --void:#050709; --bg:#0B0E14; --surface:#141922; --surface-hi:#1B212C; --border:#232A38;
    --text:#E6E9EF; --muted:#8A94A6; --dim:#5B6472;
    --lime:#8FD14F; --amber:#E8A33D; --coral:#E85D4E; --info:#6B8CAE; --violet:#A98FE8;
    --cyan:#3FE0D0;
    --mono:'JetBrains Mono', ui-monospace, monospace;
    --display:'Orbitron', var(--mono);
  }
  *{box-sizing:border-box; margin:0; padding:0;}
  body{
    background:
      radial-gradient(ellipse 900px 500px at 15% -10%, rgba(63,224,208,.05), transparent 60%),
      radial-gradient(ellipse 900px 500px at 85% 0%, rgba(169,143,232,.05), transparent 60%),
      var(--void);
    color:var(--text); font-family:var(--mono);
    -webkit-font-smoothing:antialiased; padding-bottom:88px; min-height:100vh;
  }
  a{color:inherit}
  .wrap{max-width:1100px; margin:0 auto; padding:0 16px;}

  header{position:sticky; top:0; z-index:20; background:rgba(5,7,9,0.88); backdrop-filter:blur(10px); border-bottom:1px solid var(--border);
    box-shadow:0 1px 0 rgba(143,209,79,.06);}
  .headrow{display:flex; align-items:center; justify-content:space-between; padding:12px 16px; gap:12px;}
  .brand-group{display:flex; align-items:center; gap:10px;}
  #reactor{width:38px; height:38px; flex:none; filter:drop-shadow(0 0 6px rgba(143,209,79,.35));}
  .brand{display:flex; align-items:baseline; gap:0; font-family:var(--display); font-weight:800; font-size:15px;
    letter-spacing:0.06em; text-shadow:0 0 14px rgba(143,209,79,.35);}
  .brand .mark{display:none;}
  .brand small{color:var(--dim); font-weight:600; font-size:9.5px; letter-spacing:0.14em; font-family:var(--mono); text-shadow:none;}
  .hb{display:flex; align-items:center; gap:8px; font-size:11px; color:var(--muted);}
  .hb .dot{width:6px; height:6px; border-radius:50%; background:var(--dim); transition:background .2s;}
  .hb .dot.ok{background:var(--lime);} .hb .dot.bad{background:var(--coral);}
  @media (prefers-reduced-motion:no-preference){
    .hb .dot.pulse{animation:pulse .6s ease-out;}
    @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(143,209,79,.55);}100%{box-shadow:0 0 0 8px rgba(143,209,79,0);}}
  }
  .modebadge{font-size:11px; font-weight:700; letter-spacing:.06em; padding:4px 9px; border-radius:5px; border:1px solid; white-space:nowrap; cursor:pointer;}
  .modebadge.live{color:var(--coral); border-color:var(--coral); background:rgba(232,93,78,.08);}
  .modebadge.dry{color:var(--info); border-color:var(--info); background:rgba(107,140,174,.08);}

  .strip{display:flex; gap:8px; overflow-x:auto; padding:12px 16px; border-bottom:1px solid var(--border);}
  .chip{flex:none; background:var(--surface); border:1px solid var(--border); border-radius:8px; padding:8px 12px; min-width:100px;}
  .chip .k{font-size:9px; color:var(--dim); letter-spacing:.09em; text-transform:uppercase; margin-bottom:3px;}
  .chip .v{font-size:14px; font-weight:700;}
  .chip .v.warn{color:var(--amber);} .chip .v.danger{color:var(--coral);} .chip .v.ok{color:var(--lime);}

  .banner{display:none; margin:14px 16px 0; padding:11px 14px; border-radius:8px; font-size:12.5px;
           border:1px solid var(--coral); background:rgba(232,93,78,.08); color:#F3B4AC; line-height:1.5;}
  .banner.show{display:block;}
  .banner.kill{border-color:var(--coral); background:rgba(232,93,78,.14); color:#fff;}

  main{padding:18px 0 0;}
  .grid{display:grid; grid-template-columns:1fr; gap:16px; padding:0 16px;}
  @media (min-width:860px){ .grid{grid-template-columns:1fr 1fr;} }
  .grid.g4{grid-template-columns:1fr;}
  @media (min-width:860px){ .grid.g4{grid-template-columns:1fr 1fr;} }

  .panel{background:var(--surface); border:1px solid var(--border); border-radius:10px; overflow:hidden;}
  .panel h2{font-size:11px; letter-spacing:.1em; text-transform:uppercase; color:var(--muted);
    padding:12px 14px; border-bottom:1px solid var(--border); display:flex; justify-content:space-between; align-items:center;}
  .panel h2 span{color:var(--dim);}
  .panel-body{padding:16px 14px;}
  .panel-body.collapsed{display:none;}

  /* [DASHBOARD NEW — HIDE/SHOW PANELS] A small eye toggle in every panel
     header. Purely a client-side visibility switch — hiding a panel does
     NOT stop the bot or its data; it only stops that one section from
     re-rendering every 5s poll, which is what actually helps if the phone
     starts feeling laggy from too much on-screen at once. State is saved
     in localStorage so your hidden panels stay hidden after a refresh. */
  .panel-toggle{background:none; border:1px solid var(--border); border-radius:6px; color:var(--dim);
    font-size:12px; line-height:1; padding:4px 7px; cursor:pointer; flex-shrink:0; margin-left:8px;}
  .panel-toggle:active{opacity:.6;}
  .panel-header-left{display:flex; align-items:center; gap:6px; overflow:hidden;}

  /* ══ [V9 ADD] Ambient particle backdrop — pure decoration, sits behind
     everything at z-index:-1, never intercepts clicks (pointer-events:none).
     Cheap: ~40 dots on a fixed canvas, no libraries. ══ */
  #particleBg{position:fixed; inset:0; z-index:-1; pointer-events:none; opacity:.55;}

  /* ══ [V9 ADD] Glassmorphism upgrade for panels — layered on top of the
     existing .panel rule above rather than replacing it, so nothing that
     already worked (borders, radii) changes, only adds blur + subtle glow. ══ */
  .panel{background:linear-gradient(180deg, rgba(27,33,44,.75), rgba(20,25,34,.85)); backdrop-filter:blur(10px);
    box-shadow:0 1px 0 rgba(255,255,255,.03) inset, 0 8px 24px -12px rgba(0,0,0,.5);}
  .panel.glow-ok{box-shadow:0 0 0 1px rgba(143,209,79,.18), 0 8px 24px -12px rgba(0,0,0,.5);}
  .panel.glow-bad{box-shadow:0 0 0 1px rgba(232,93,78,.25), 0 8px 24px -12px rgba(0,0,0,.5);}

  /* ══ [PREMIUM UI ADD] Cyberpunk finish layer — top hairline accent per
     panel + a slow ambient border glow, plus a subtle lift on hover/focus
     for touch/mouse alike. Pure CSS, GPU-cheap (transform+opacity only),
     respects prefers-reduced-motion. ══ */
  .panel{position:relative; transition:box-shadow .25s ease, transform .25s ease;}
  .panel::before{content:''; position:absolute; top:0; left:14px; right:14px; height:1px;
    background:linear-gradient(90deg, transparent, rgba(63,224,208,.5), rgba(143,209,79,.5), transparent);
    opacity:.55;}
  .panel:hover{box-shadow:0 0 0 1px rgba(63,224,208,.22), 0 14px 32px -14px rgba(0,0,0,.6); transform:translateY(-1px);}
  .panel h2{font-family:var(--display); font-weight:700; letter-spacing:.13em; font-size:10px;}
  @media (prefers-reduced-motion:reduce){ .panel{transition:none;} .panel:hover{transform:none;} }

  /* ══ [V9 ADD] AI Decision Engine — reasoning readout for the most recent
     ENTRY, built entirely from real columns already logged per trade
     (rsi/adx/ofi_pct/knn_score/systems/premium_shield/ml_healthy/
     confidence_reason) — nothing here is a placeholder number. ══ */
  .ai-head{display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:8px; margin-bottom:12px;}
  .ai-verdict{font-size:15px; font-weight:800; letter-spacing:.03em; padding:5px 12px; border-radius:6px; border:1px solid;}
  .ai-verdict.buy{color:var(--lime); border-color:var(--lime); background:rgba(143,209,79,.08);}
  .ai-verdict.sell{color:var(--coral); border-color:var(--coral); background:rgba(232,93,78,.08);}
  .ai-conf{font-size:20px; font-weight:800; font-variant-numeric:tabular-nums;}
  .ai-checks{display:grid; grid-template-columns:repeat(auto-fill,minmax(110px,1fr)); gap:8px; margin:12px 0;}
  .ai-chk{background:var(--surface-hi); border:1px solid var(--border); border-radius:7px; padding:8px 10px;}
  .ai-chk .k{font-size:9px; color:var(--dim); letter-spacing:.08em; text-transform:uppercase; margin-bottom:3px;}
  .ai-chk .v{font-size:13px; font-weight:700;}
  .ai-chk .v.pass{color:var(--lime);} .ai-chk .v.fail{color:var(--coral);}
  .ai-reason{font-size:12px; color:var(--muted); line-height:1.6; border-top:1px solid var(--border); padding-top:10px; margin-top:4px;}
  .ai-bars{display:flex; flex-direction:column; gap:9px; margin-top:14px;}
  .ai-bar-row{display:flex; align-items:center; gap:10px; font-size:11px;}
  .ai-bar-row .lbl{width:108px; flex:none; color:var(--muted);}
  .ai-bar-track{flex:1; height:7px; background:var(--surface-hi); border-radius:99px; overflow:hidden; border:1px solid var(--border);}
  .ai-bar-fill{height:100%; border-radius:99px; background:linear-gradient(90deg, var(--info), var(--lime)); transition:width .6s ease;}
  .ai-bar-row .pct{width:34px; flex:none; text-align:right; font-variant-numeric:tabular-nums; color:var(--text);}

  /* ══ [V9 ADD] Equity curve — sparkline canvas drawn from real R-multiples
     parsed off TRADE_CLOSE events (same source refreshTrades() already
     used for Cumulative R), not synthetic data. ══ */
  #equityCanvas{width:100%; height:120px; display:block;}
  .equity-foot{display:flex; justify-content:space-between; font-size:10px; color:var(--dim); margin-top:6px;}

  /* ══ [V9 ADD] Alert Center — every line here is derived from data the
     dashboard already polls (config/status/balance), not invented. ══ */
  .alert-row{display:flex; align-items:flex-start; gap:9px; padding:9px 0; border-bottom:1px solid var(--border); font-size:12.5px; line-height:1.5;}
  .alert-row:last-child{border-bottom:none;}
  .alert-row .ic{flex:none; font-size:14px;}
  .alert-row.warn{color:#F0C98A;} .alert-row.danger{color:#F3B4AC;} .alert-row.info{color:var(--muted);}
  .alert-clear{padding:22px 0; text-align:center; color:var(--dim); font-size:12.5px;}
  .panel-body.scroll{max-height:420px; overflow-y:auto; padding:0;}
  .empty{padding:30px 16px; text-align:center; color:var(--dim); font-size:12.5px; line-height:1.7;}

  /* System status */
  .bigstat{font-size:28px; font-weight:800; letter-spacing:.02em;}
  .bigstat.ok{color:var(--lime);} .bigstat.bad{color:var(--coral);}

  /* Footprint "scanning" indicator — purely decorative, shows the bot is
     actively watching the market. Four footprints light up one after
     another in a loop, like steps walking forward, then repeat. */
  .footprint-track{display:flex; gap:2px; align-items:center; opacity:.85;}
  .footprint-track .foot{
    font-size:13px; filter:grayscale(1) brightness(1.6); opacity:.25;
    animation:footStep 1.6s ease-in-out infinite;
    display:inline-block; transform:translateY(2px);
  }
  .footprint-track .foot:nth-child(1){animation-delay:0s;}
  .footprint-track .foot:nth-child(2){animation-delay:.4s;}
  .footprint-track .foot:nth-child(3){animation-delay:.8s;}
  .footprint-track .foot:nth-child(4){animation-delay:1.2s;}
  @keyframes footStep{
    0%{opacity:.2; filter:grayscale(1) brightness(1.6); transform:translateY(2px) scale(.85);}
    15%{opacity:1; filter:grayscale(0) brightness(1); transform:translateY(0) scale(1);}
    40%{opacity:.5; filter:grayscale(.6) brightness(1.3); transform:translateY(1px) scale(.95);}
    100%{opacity:.2; filter:grayscale(1) brightness(1.6); transform:translateY(2px) scale(.85);}
  }
  @media (prefers-reduced-motion:reduce){
    .footprint-track .foot{animation:none; opacity:.6;}
  }
  .substats{display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-top:14px;}
  .substats div{font-size:11px; color:var(--dim);}
  .substats b{display:block; font-size:13px; color:var(--text); font-weight:600; margin-top:2px;}

  /* Confidence gauge */
  .gaugewrap{display:flex; flex-direction:column; align-items:center; padding-top:4px;}
  .gaugenum{font-size:34px; font-weight:800; margin-top:-46px;}
  .gaugelabel{font-size:11px; letter-spacing:.1em; text-transform:uppercase; color:var(--dim); margin-top:2px;}

  /* Bot status */
  .kv{font-size:12px; color:var(--muted); display:flex; justify-content:space-between; padding:6px 0; border-bottom:1px solid var(--border);}
  .kv:last-child{border-bottom:none;}
  .kv b{color:var(--text); font-weight:600;}
  .pill{display:inline-block; font-size:10px; font-weight:700; letter-spacing:.05em; padding:3px 9px; border-radius:20px; border:1px solid;}
  .pill.on{color:var(--lime); border-color:var(--lime); background:rgba(143,209,79,.08);}
  .pill.off{color:var(--dim); border-color:var(--border); background:transparent;}

  /* Signal switchboard */
  .switchgrid{display:grid; grid-template-columns:repeat(2,1fr); gap:8px;}
  @media (min-width:600px){ .switchgrid{grid-template-columns:repeat(4,1fr);} }
  .sw{border:1px solid var(--border); border-radius:8px; padding:9px 8px; text-align:center; cursor:pointer;
      background:var(--surface-hi); transition:.15s; user-select:none;}
  .sw.on{border-color:var(--lime); background:rgba(143,209,79,.08);}
  .sw .name{font-size:11.5px; font-weight:700; letter-spacing:.03em;}
  .sw.on .name{color:var(--lime);}
  .sw.off .name{color:var(--dim);}
  .sw .st{font-size:9px; margin-top:3px; letter-spacing:.08em;}
  .sw.on .st{color:var(--lime);} .sw.off .st{color:var(--dim);}
  .sw:active{transform:scale(.96);}

  /* Performance */
  .perfrow{display:flex; justify-content:space-between; align-items:baseline; padding:9px 0; border-bottom:1px solid var(--border); font-size:12.5px; color:var(--muted);}
  .perfrow:last-child{border-bottom:none;}
  .perfrow .v{font-size:16px; font-weight:700; color:var(--text);}
  .perfrow .v.pos{color:var(--lime);} .perfrow .v.neg{color:var(--coral);}

  /* Control center */
  .ctrlgrid{display:grid; grid-template-columns:1fr 1fr; gap:8px;}
  @media (min-width:600px){ .ctrlgrid{grid-template-columns:repeat(4,1fr);} }

  /* [DASHBOARD NEW — SWIPE TO ARM] A deliberate drag gesture for the one
     control that blocks all new trading — arming should take a conscious
     action, unlike Clear (a plain button, since undoing a block should be
     easy). Pointer Events cover mouse + touch in one code path. */
  .kill-swipe-wrap{margin-top:14px;}
  .kill-swipe-track{position:relative; height:52px; border-radius:26px; background:var(--surface-hi);
    border:1px solid rgba(232,93,78,.4); overflow:hidden; touch-action:none; user-select:none;}
  .kill-swipe-fill{position:absolute; inset:0; width:0; background:linear-gradient(90deg, rgba(232,93,78,.15), rgba(232,93,78,.35));
    transition:width .05s linear;}
  .kill-swipe-label{position:absolute; inset:0; display:flex; align-items:center; justify-content:center;
    font-size:11.5px; letter-spacing:.1em; color:var(--coral); font-weight:700; pointer-events:none;}
  .kill-swipe-knob{position:absolute; top:2px; left:2px; width:46px; height:46px; border-radius:50%;
    background:radial-gradient(circle at 35% 30%, #3a2320, #1a0f0e); border:1px solid var(--coral);
    box-shadow:0 0 14px rgba(232,93,78,.55); display:flex; align-items:center; justify-content:center;
    font-size:20px; cursor:grab; transition:left .05s linear;}
  .kill-swipe-track.armed .kill-swipe-fill{width:100% !important; background:rgba(232,93,78,.5);}
  .kill-swipe-track.armed .kill-swipe-label{color:#fff;}
  .kill-swipe-sub{margin-top:8px; font-size:10.5px; color:var(--dim); text-align:center;}

  .cbtn{font-family:var(--mono); font-weight:700; font-size:11px; letter-spacing:.03em; padding:11px 8px;
        border-radius:8px; border:1px solid; background:transparent; cursor:pointer; text-transform:uppercase;}
  .cbtn:active{transform:scale(.96);}
  .cbtn.amber{color:var(--amber); border-color:var(--amber);}
  .cbtn.lime{color:var(--lime); border-color:var(--lime);}
  .cbtn.coral{color:var(--coral); border-color:var(--coral);}
  .cbtn.violet{color:var(--violet); border-color:var(--violet);}
  .cbtn.info{color:var(--info); border-color:var(--info);}
  .cbtn.solid{background:var(--coral); color:#fff; border-color:var(--coral);}
  .modetoggle{display:flex; align-items:center; justify-content:space-between; padding:10px 0;}
  .modetoggle .lbl{font-size:12px; color:var(--muted);}
  .switch{position:relative; width:46px; height:24px; border-radius:20px; background:var(--surface-hi); border:1px solid var(--border); cursor:pointer;}
  .switch .knob{position:absolute; top:2px; left:2px; width:18px; height:18px; border-radius:50%; background:var(--dim); transition:.15s;}
  .switch.on{background:rgba(232,93,78,.15); border-color:var(--coral);}
  .switch.on .knob{left:24px; background:var(--coral);}
  .switch.lime.on{background:rgba(143,209,79,.15); border-color:var(--lime);}
  .switch.lime.on::after{content:''; position:absolute; top:2px; left:24px; width:18px; height:18px; border-radius:50%; background:var(--lime);}
  .switch.lime::after{content:''; position:absolute; top:2px; left:2px; width:18px; height:18px; border-radius:50%; background:var(--dim); transition:.15s;}

  /* [DASHBOARD NEW — SETTINGS] Gear button in the header + a centered
     modal listing every panel with an ON/OFF switch, so hiding several
     sections at once (to lighten the page) doesn't mean hunting for each
     panel's own 👁 button individually. */
  .icon-btn{background:var(--surface); border:1px solid var(--border); border-radius:8px; color:var(--muted);
    font-size:15px; padding:6px 9px; cursor:pointer; line-height:1;}
  .icon-btn:active{opacity:.6;}
  .modal-overlay{position:fixed; inset:0; z-index:50; background:rgba(6,8,12,.72); backdrop-filter:blur(2px);
    display:none; align-items:flex-end; justify-content:center;}
  .modal-overlay.show{display:flex;}
  @media (min-width:640px){ .modal-overlay{align-items:center;} }
  .modal-box{width:100%; max-width:520px; max-height:80vh; overflow-y:auto; background:var(--surface);
    border:1px solid var(--border); border-radius:14px 14px 0 0; padding:18px 16px calc(18px + env(safe-area-inset-bottom));}
  @media (min-width:640px){ .modal-box{border-radius:14px; max-height:70vh;} }
  .modal-head{display:flex; align-items:center; justify-content:space-between; margin-bottom:6px;}
  .modal-head h3{font-size:14px; letter-spacing:.06em; text-transform:uppercase; color:var(--text);}
  .modal-close{background:none; border:1px solid var(--border); border-radius:7px; color:var(--muted);
    font-size:13px; padding:4px 9px; cursor:pointer;}
  .modal-sub{font-size:11.5px; color:var(--dim); line-height:1.5; margin-bottom:14px;}
  .settings-list{display:flex; flex-direction:column; gap:2px; margin-bottom:14px;}
  .settings-row{display:flex; align-items:center; justify-content:space-between; gap:10px;
    padding:11px 4px; border-bottom:1px solid var(--border);}
  .settings-row:last-child{border-bottom:none;}
  .settings-row-label{font-size:12.5px; color:var(--text); text-transform:uppercase; letter-spacing:.03em;}
  .modal-actions{display:flex; gap:8px;}
  .modal-actions .cbtn{flex:1;}

  /* Position / trade rows (unchanged pattern) */
  .pos{padding:12px 14px; border-bottom:1px solid var(--border);}
  .pos:last-child{border-bottom:none;}
  .pos-top{display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;}
  .pos-sym{font-weight:700; font-size:14px;}
  .dir{font-size:11px; font-weight:700; padding:2px 7px; border-radius:5px;}
  .dir.buy{color:var(--lime); background:rgba(143,209,79,.1);}
  .dir.sell{color:var(--coral); background:rgba(232,93,78,.1);}
  .pos-meta{font-size:10.5px; color:var(--dim);}
  .pos-grid{display:grid; grid-template-columns:repeat(4,1fr); gap:6px; margin-top:8px;}
  .pos-grid div{background:var(--surface-hi); border-radius:6px; padding:6px 4px; text-align:center;}
  .pos-grid .k{font-size:8.5px; color:var(--dim); text-transform:uppercase; letter-spacing:.06em;}
  .pos-grid .v{font-size:11.5px; font-weight:600; margin-top:2px;}
  .pos-grid .v.ok{color:var(--lime);} .pos-grid .v.warn{color:var(--amber);} .pos-grid .v.danger{color:var(--coral);}

  .trade{display:flex; align-items:center; gap:10px; padding:9px 14px; border-bottom:1px solid var(--border);
         border-left:3px solid var(--dim); font-size:11.5px;}
  .trade:last-child{border-bottom:none;}
  .trade.entry{border-left-color:var(--info);}
  .trade.win{border-left-color:var(--lime);}
  .trade.loss{border-left-color:var(--coral);}
  .trade .t-time{color:var(--dim); min-width:64px; font-size:10px;}
  .trade .t-sym{font-weight:700; min-width:64px;}
  .trade .t-ev{color:var(--muted); flex:1;}
  .trade .t-qty{color:var(--muted); text-align:right;}

  .rej{padding:9px 14px; border-bottom:1px solid var(--border); border-left:3px solid var(--coral); font-size:11.5px;}
  .rej:last-child{border-bottom:none;}
  .rej-top{display:flex; justify-content:space-between; align-items:center;}
  .rej-sym{font-weight:700;}
  .rej-reason{font-size:10px; font-weight:700; letter-spacing:.04em; text-transform:uppercase; color:var(--coral);}
  .rej-detail{color:var(--dim); font-size:10.5px; margin-top:3px;}
  .rej-time{color:var(--dim); font-size:10px;}

  .ctrlbar{position:fixed; bottom:0; left:0; right:0; z-index:30; background:rgba(11,14,20,.96);
    backdrop-filter:blur(8px); border-top:1px solid var(--border); padding:10px 16px calc(10px + env(safe-area-inset-bottom));}
  .ctrl-row{max-width:1100px; margin:0 auto; display:grid; grid-template-columns:1fr 1fr 1.2fr; gap:8px;}
  .btn{font-family:var(--mono); font-weight:700; font-size:12px; letter-spacing:.03em; padding:12px 8px;
       border-radius:8px; border:1px solid; background:transparent; cursor:pointer; text-transform:uppercase;}
  .btn:active{transform:scale(.97);}
  .btn.pause{color:var(--amber); border-color:var(--amber);}
  .btn.resume{color:var(--lime); border-color:var(--lime);}
  .btn.danger{color:#fff; border-color:var(--coral); background:var(--coral);}
  .toast{position:fixed; bottom:96px; left:16px; right:16px; z-index:40; max-width:1100px; margin:0 auto;
    background:var(--surface-hi); border:1px solid var(--border); border-radius:8px; padding:11px 14px;
    font-size:12px; opacity:0; transform:translateY(6px); transition:.2s; pointer-events:none;}
  .toast.show{opacity:1; transform:translateY(0);}

  /* [DASHBOARD NEW — APEX CORE] A glowing 3D-style orbiting logo, in the
     same spirit as those flashy AI-trading-bot hero screens: a pulsing
     core sphere with the brand letter, wrapped in tilted orbit rings that
     sweep around it. Everything here is pure CSS (transform + gradient
     animation only) — no canvas, no per-frame JS — so it costs the GPU
     almost nothing and won't contribute to any dashboard lag, and it
     collapses via the same 👁 Hide button as every other panel if you'd
     rather reclaim the space. Colors reuse the dashboard's own palette
     (--lime / --info / --violet) so it matches the rest of the UI instead
     of looking like a bolted-on graphic.  */
  .core-panel .panel-body{display:flex; flex-direction:column; align-items:center; gap:16px; padding:28px 14px 26px;}
  .apex-orb-wrap{position:relative; width:210px; height:210px; display:flex; align-items:center; justify-content:center;}
  .apex-orb-glow{position:absolute; width:190px; height:190px; border-radius:50%;
    background:radial-gradient(circle, rgba(143,209,79,.38), rgba(107,140,174,.12) 55%, transparent 72%);
    filter:blur(14px); animation:apexPulse 3.4s ease-in-out infinite;}
  .apex-orb-ring{position:absolute; border-radius:50%; border:1.5px solid transparent;
    -webkit-mask:radial-gradient(farthest-side, transparent calc(100% - 2px), #000 calc(100% - 2px));
            mask:radial-gradient(farthest-side, transparent calc(100% - 2px), #000 calc(100% - 2px));}
  .apex-orb-ring.ring1{width:210px; height:210px; transform:rotateX(72deg);
    background:conic-gradient(from 0deg, transparent 0%, var(--lime) 18%, transparent 38%);
    animation:apexSpin 5s linear infinite;}
  .apex-orb-ring.ring2{width:170px; height:170px; transform:rotateX(70deg) rotate(55deg);
    background:conic-gradient(from 0deg, transparent 0%, var(--info) 22%, transparent 44%);
    animation:apexSpin 7.5s linear infinite reverse;}
  .apex-orb-ring.ring3{width:150px; height:150px; transform:rotateX(68deg) rotate(-40deg);
    background:conic-gradient(from 0deg, transparent 0%, var(--violet) 16%, transparent 34%);
    animation:apexSpin 10s linear infinite;}
  /* [DASHBOARD NEW — APEX LOGO] A faceted, beveled hex badge instead of a
     plain circle — same visual family as a chiseled metal emblem (frame in
     a light-to-dark diagonal "chrome" gradient, dark inset face, glowing
     embossed lettering), built with layered clip-path hexagons + gradients
     so it stays pure CSS (cheap, no image asset to host/load). */
  .apex-logo-hex{position:relative; width:132px; height:112px; padding:4px;
    clip-path:polygon(25% 3%, 75% 3%, 100% 50%, 75% 97%, 25% 97%, 0% 50%);
    background:linear-gradient(135deg, #e9edf2 0%, #a7b0bc 12%, #4a525c 32%, #1c2128 50%, #4a525c 68%, #a7b0bc 88%, #e9edf2 100%);
    box-shadow:0 0 26px rgba(143,209,79,.35), 0 6px 18px rgba(0,0,0,.5);
    animation:apexHexTilt 6.5s ease-in-out infinite;}
  .apex-logo-hex-face{width:100%; height:100%;
    clip-path:polygon(25% 3%, 75% 3%, 100% 50%, 75% 97%, 25% 97%, 0% 50%);
    background:radial-gradient(circle at 38% 28%, #17231a 0%, #0a0d12 72%);
    display:flex; align-items:center; justify-content:center;
    box-shadow:inset 0 0 24px rgba(143,209,79,.28), inset 0 2px 4px rgba(255,255,255,.08);}
  .apex-logo-text{font-size:25px; font-weight:800; letter-spacing:.05em;
    background:linear-gradient(180deg, #eef7e2 0%, var(--lime) 55%, #4f7a2a 100%);
    -webkit-background-clip:text; background-clip:text; color:transparent;
    filter:drop-shadow(0 0 8px rgba(143,209,79,.75));}
  .apex-orb-spark{position:absolute; width:4px; height:4px; border-radius:50%; background:var(--lime);
    box-shadow:0 0 8px 2px rgba(143,209,79,.8); opacity:.85;}
  .apex-orb-spark.s1{animation:apexOrbit1 5s linear infinite;}
  .apex-orb-spark.s2{background:var(--info); box-shadow:0 0 8px 2px rgba(107,140,174,.8); animation:apexOrbit2 7.5s linear infinite reverse;}
  .apex-orb-spark.s3{background:var(--violet); box-shadow:0 0 8px 2px rgba(169,143,232,.8); animation:apexOrbit3 10s linear infinite;}
  .apex-orb-caption{font-size:11px; letter-spacing:.14em; text-transform:uppercase; color:var(--muted);
    display:flex; align-items:center; gap:8px;}
  .apex-orb-livedot{width:7px; height:7px; border-radius:50%; background:var(--lime);
    box-shadow:0 0 8px 2px rgba(143,209,79,.7); animation:apexDotPulse 1.8s ease-in-out infinite;}

  @keyframes apexSpin{ from{ transform:rotateX(70deg) rotate(0deg); } to{ transform:rotateX(70deg) rotate(360deg); } }
  @keyframes apexPulse{ 0%,100%{ opacity:.7; transform:scale(1); } 50%{ opacity:1; transform:scale(1.06); } }
  @keyframes apexHexTilt{ 0%,100%{ transform:perspective(700px) rotateY(-10deg) rotateX(2deg); }
    50%{ transform:perspective(700px) rotateY(10deg) rotateX(-2deg); } }
  @keyframes apexDotPulse{ 0%,100%{ opacity:.5; } 50%{ opacity:1; } }
  @keyframes apexOrbit1{ from{ transform:rotate(0deg) translateX(96px) rotate(0deg); }
    to{ transform:rotate(360deg) translateX(96px) rotate(-360deg); } }
  @keyframes apexOrbit2{ from{ transform:rotate(90deg) translateX(76px) rotate(-90deg); }
    to{ transform:rotate(450deg) translateX(76px) rotate(-450deg); } }
  @keyframes apexOrbit3{ from{ transform:rotate(200deg) translateX(64px) rotate(-200deg); }
    to{ transform:rotate(560deg) translateX(64px) rotate(-560deg); } }

  /* Signal Radar — real rotating sweep (like a proper radar/sonar display),
     with blips placed for actual recent signals: green = order taken,
     red = blocked/rejected. This is the thing that replaces the small
     footprint strip — the footprints stay too (in System Status) as a tiny
     "still scanning" heartbeat, but THIS is the real, informative radar. */
  .radar-panel .panel-body{display:flex; flex-direction:column; align-items:center; gap:14px; padding-top:20px;}
  .radar{position:relative; width:236px; height:236px; border-radius:50%;
    background:radial-gradient(circle, rgba(232,93,78,.05) 0%, rgba(11,14,20,1) 72%);
    border:1px solid rgba(232,93,78,.35); overflow:hidden; flex:none;}
  .radar-rings{position:absolute; inset:0; border-radius:50%;
    background-image:repeating-radial-gradient(circle, transparent 0, transparent 38px, rgba(232,93,78,.18) 39px);}
  .radar-cross{position:absolute; inset:0;}
  .radar-cross::before, .radar-cross::after{content:''; position:absolute; background:rgba(232,93,78,.15);}
  .radar-cross::before{left:50%; top:0; bottom:0; width:1px; margin-left:-.5px;}
  .radar-cross::after{top:50%; left:0; right:0; height:1px; margin-top:-.5px;}
  .radar-sweep{position:absolute; inset:0; border-radius:50%;
    background:conic-gradient(from 0deg, rgba(232,93,78,.75), rgba(232,93,78,.25) 12%, rgba(232,93,78,0) 32%);
    animation:radarSpin 3.4s linear infinite; transform-origin:50% 50%;}
  @keyframes radarSpin{from{transform:rotate(0deg);} to{transform:rotate(360deg);}}
  .radar-blip{position:absolute; width:9px; height:9px; border-radius:50%; transform:translate(-50%,-50%);
    display:flex; align-items:center; justify-content:center;}
  .radar-blip::after{content:''; position:absolute; width:9px; height:9px; border-radius:50%; animation:blipPing 1.8s ease-out infinite;}
  .radar-blip.taken{background:var(--lime); box-shadow:0 0 7px var(--lime);}
  .radar-blip.taken::after{background:var(--lime);}
  .radar-blip.blocked{background:var(--coral); box-shadow:0 0 7px var(--coral);}
  .radar-blip.blocked::after{background:var(--coral);}
  @keyframes blipPing{0%{opacity:.55; transform:scale(1);} 100%{opacity:0; transform:scale(2.6);}}
  .radar-label{position:absolute; font-size:8.5px; color:var(--dim); white-space:nowrap; transform:translate(-50%, 6px);}
  .radar-center{position:absolute; left:50%; top:50%; width:5px; height:5px; margin:-2.5px; border-radius:50%; background:var(--coral); box-shadow:0 0 6px var(--coral);}
  .radar-legend{display:flex; gap:18px; font-size:10.5px; color:var(--muted);}
  .radar-legend span{display:inline-flex; align-items:center; gap:5px;}
  .radar-legend i{width:7px; height:7px; border-radius:50%; display:inline-block;}
  .radar-legend i.taken{background:var(--lime);} .radar-legend i.blocked{background:var(--coral);}
  .radar-empty{color:var(--dim); font-size:10.5px; text-align:center;}
  @media (prefers-reduced-motion:reduce){ .radar-sweep{animation:none;} .radar-blip::after{animation:none;} }
</style>
</head>
<body>
<canvas id="particleBg"></canvas>

<header>
  <div class="wrap headrow">
    <div class="brand-group">
      <canvas id="reactor" width="88" height="88" title="Live system core — pulse speed and color follow the AI confidence score and LIVE/DRY mode of your last processed signal"></canvas>
      <div class="brand"><span class="mark" id="brandDot"></span>APEX&nbsp;NEXUS<small>&nbsp;MISSION CONTROL</small></div>
    </div>
    <div style="display:flex;align-items:center;gap:10px;">
      <span class="hb"><span class="dot" id="hbDot"></span><span id="hbText">connecting…</span></span>
      <span class="modebadge dry" id="modeBadge" title="Tap to switch LIVE/DRY_RUN">—</span>
      <button class="icon-btn" id="btnSettings" title="Dashboard Settings" aria-label="Dashboard Settings">⚙️</button>
    </div>
  </div>
  <div class="wrap strip" id="stripRow">
    <div class="chip"><div class="k">Region</div><div class="v" id="cRegion">—</div></div>
    <div class="chip"><div class="k">Coins Discovered</div><div class="v" id="cProducts">—</div></div>
    <div class="chip"><div class="k">Open Positions</div><div class="v" id="cOpen">—</div></div>
    <div class="chip"><div class="k">Total Trades</div><div class="v" id="cTrades">—</div></div>
    <div class="chip"><div class="k">Armed</div><div class="v" id="cArmed">—</div></div>
    <div class="chip"><div class="k">API Key</div><div class="v" id="cCreds">—</div></div>
    <div class="chip"><div class="k">Circuit Breaker</div><div class="v" id="cCB">—</div></div>
  </div>
</header>

<div class="wrap"><div class="banner" id="banner"></div></div>
<div class="wrap"><div class="banner kill" id="killBanner"></div></div>

<main class="wrap">
  <div class="grid" style="grid-template-columns:1fr;">
    <section class="panel core-panel">
      <h2>Apex Core <span style="font-weight:400;">Sovereign AI System</span></h2>
      <div class="panel-body">
        <div class="apex-orb-wrap">
          <div class="apex-orb-glow"></div>
          <div class="apex-orb-ring ring1"></div>
          <div class="apex-orb-ring ring2"></div>
          <div class="apex-orb-ring ring3"></div>
          <div class="apex-logo-hex">
            <div class="apex-logo-hex-face">
              <span class="apex-logo-text">APEX</span>
            </div>
          </div>
          <div class="apex-orb-spark s1"></div>
          <div class="apex-orb-spark s2"></div>
          <div class="apex-orb-spark s3"></div>
        </div>
        <div class="apex-orb-caption"><span class="apex-orb-livedot"></span>APEX NEXUS &nbsp;·&nbsp; SYSTEMS ACTIVE</div>
      </div>
    </section>

    <section class="panel radar-panel">
      <h2>Signal Radar <span id="radarCount" style="font-weight:400;"></span></h2>
      <div class="panel-body">
        <div class="radar" id="radarDisc">
          <div class="radar-rings"></div>
          <div class="radar-cross"></div>
          <div class="radar-sweep"></div>
          <div class="radar-center"></div>
          <div id="radarBlips"></div>
        </div>
        <div class="radar-legend">
          <span><i class="taken"></i>order taken</span>
          <span><i class="blocked"></i>blocked / rejected</span>
        </div>
      </div>
    </section>
  </div>

  <div class="grid">
    <section class="panel">
      <h2>System Status</h2>
      <div class="panel-body">
        <div style="display:flex; align-items:baseline; gap:10px; flex-wrap:wrap;">
          <div class="bigstat ok" id="sysStatus">—</div>
          <div class="footprint-track" id="footTrack" title="scanning markets">
            <span class="foot">👣</span><span class="foot">👣</span><span class="foot">👣</span><span class="foot">👣</span>
          </div>
        </div>
        <div class="substats">
          <div>last ping<b id="sLastPing">—</b></div>
          <div>latency<b id="sLatency">—</b></div>
          <div>time drift<b id="sDrift">—</b></div>
          <div>region · base<b id="sRegionBase">—</b></div>
        </div>
      </div>
    </section>

    <section class="panel">
      <h2>Latest Signal Confidence</h2>
      <div class="panel-body">
        <div class="gaugewrap">
          <svg id="gaugeSvg" width="220" height="120" viewBox="0 0 220 120">
            <path d="M20,110 A90,90 0 0,1 200,110" fill="none" stroke="#232A38" stroke-width="14" stroke-linecap="round"/>
            <path id="gaugeArc" d="M20,110 A90,90 0 0,1 200,110" fill="none" stroke="url(#gaugeGrad)" stroke-width="14"
                  stroke-linecap="round" stroke-dasharray="283" stroke-dashoffset="283"/>
            <defs>
              <linearGradient id="gaugeGrad" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stop-color="#E85D4E"/>
                <stop offset="55%" stop-color="#E8A33D"/>
                <stop offset="100%" stop-color="#8FD14F"/>
              </linearGradient>
            </defs>
          </svg>
          <div class="gaugenum" id="gaugeNum">—</div>
          <div class="gaugelabel" id="gaugeLabel">no signal yet</div>
        </div>
      </div>
    </section>
  </div>

  <div class="grid" style="grid-template-columns:1fr; margin-top:16px;">
    <section class="panel" id="aiPanel">
      <h2>AI Decision Engine <span id="aiWhen" style="font-weight:400;"></span></h2>
      <div class="panel-body" id="aiBody">
        <div class="empty">No signal processed yet — this fills in the moment the first webhook arrives.</div>
      </div>
    </section>
  </div>

  <div class="grid" style="margin-top:16px;">
    <section class="panel">
      <h2>Equity Curve <span style="font-weight:400;">cumulative R, closed trades</span></h2>
      <div class="panel-body">
        <canvas id="equityCanvas"></canvas>
        <div class="equity-foot"><span id="eqStart">0.00R</span><span id="eqNow">0.00R</span></div>
      </div>
    </section>
    <section class="panel">
      <h2>Alert Center <span id="alertCount">0</span></h2>
      <div class="panel-body scroll" id="alertBody" style="max-height:220px;">
        <div class="alert-clear">No active alerts.</div>
      </div>
    </section>
  </div>

  <div class="grid" style="margin-top:16px;">
    <section class="panel">
      <h2>Order Flow <span style="font-weight:400; color:var(--dim);">Binance liquidations, 5m</span></h2>
      <div class="panel-body" id="orderFlowBody">
        <div class="perfrow"><span>Loading…</span><span class="v">—</span></div>
      </div>
    </section>
    <section class="panel">
      <h2>Execution Health <span style="font-weight:400; color:var(--dim);">Delta API</span></h2>
      <div class="panel-body" id="execHealthBody">
        <div class="perfrow"><span>Loading…</span><span class="v">—</span></div>
      </div>
    </section>
  </div>

  <div class="grid" style="margin-top:16px;">
    <section class="panel">
      <h2>System Health <span style="font-weight:400; color:var(--dim);">host process</span></h2>
      <div class="panel-body" id="sysHealthBody">
        <div class="perfrow"><span>Loading…</span><span class="v">—</span></div>
      </div>
    </section>
    <section class="panel">
      <h2>Architecture <span style="font-weight:400; color:var(--dim);">active modules</span></h2>
      <div class="panel-body scroll" id="archBody" style="max-height:260px;">
        <div class="perfrow"><span>Loading…</span><span class="v">—</span></div>
      </div>
    </section>
  </div>

  <div class="grid" style="margin-top:16px;">
    <section class="panel">
      <h2>Bot Status</h2>
      <div class="panel-body">
        <div class="kv"><span>Mode</span><b id="bMode">—</b></div>
        <div class="kv" id="bSizingRow" style="cursor:pointer;"><span>Dynamic Sizing <span style="color:var(--dim);font-size:10px;">(tap to toggle)</span></span><b id="bSizing">—</b></div>
        <div class="kv"><span>Shock Filter</span><b id="bShock">—</b></div>
        <div class="kv"><span>Telegram</span><b id="bTelegram">—</b></div>
        <div class="kv"><span>Auto Bracket Orders</span><b id="bBracket">—</b></div>
        <div class="kv"><span>Kill Switch</span><b id="bKill">—</b></div>
      </div>
    </section>

    <section class="panel">
      <h2>Account Balance <span id="balAge" style="font-weight:400;"></span></h2>
      <div class="panel-body">
        <div class="bigstat ok" id="balBig">—</div>
        <div class="substats" style="grid-template-columns:1fr;">
          <div id="balNote" style="color:var(--dim);"></div>
        </div>
      </div>
    </section>

    <section class="panel">
      <h2>Performance</h2>
      <div class="panel-body">
        <div class="perfrow"><span>Cumulative R (closed trades)</span><span class="v" id="pPnl">—</span></div>
        <div class="perfrow"><span>Win Rate</span><span class="v" id="pWinRate">—</span></div>
        <div class="perfrow"><span>Profit Factor</span><span class="v" id="pPF">—</span></div>
        <div class="perfrow"><span>Max Drawdown</span><span class="v" id="pMDD">—</span></div>
        <div class="perfrow"><span>Wins / Losses</span><span class="v" id="pWL">—</span></div>
        <div class="perfrow"><span>Trades Logged</span><span class="v" id="pTrades">—</span></div>
        <div class="perfrow"><span>Open Positions</span><span class="v" id="pOpen">—</span></div>
        <div class="perfrow"><span>Consecutive Losses</span><span class="v" id="pStreak">—</span></div>
        <div style="margin-top:10px; padding-top:10px; border-top:1px dashed var(--border); font-size:10.5px; color:var(--dim); line-height:1.5;">
          All figures above are computed live from your own closed TRADE_CLOSE records — nothing here is a target or an estimate.
          With very few closed trades these numbers will swing a lot; they become meaningful once you have a real sample size.
        </div>
      </div>
    </section>
  </div>

  <div class="grid" style="grid-template-columns:1fr; margin-top:16px;">
    <section class="panel">
      <h2>Signal Switchboard <span id="swCount"></span></h2>
      <div class="panel-body">
        <div class="switchgrid" id="switchGrid"></div>
      </div>
    </section>
  </div>

  <div class="grid" style="grid-template-columns:1fr; margin-top:16px;">
    <section class="panel">
      <h2>Trading Control <span id="ctrlActiveBadge" class="pill on">ACTIVE</span></h2>
      <div class="panel-body">
        <div class="modetoggle">
          <span class="lbl">LIVE MODE (real orders on Delta)</span>
          <div class="switch" id="liveSwitch"><div class="knob"></div></div>
        </div>
        <div class="ctrlgrid" style="margin-top:10px;">
          <button class="cbtn amber" id="btnPause2">Pause</button>
          <button class="cbtn lime" id="btnResume2">Resume</button>
          <button class="cbtn info" id="btnResetCB">Reset Circuit Breaker</button>
          <button class="cbtn violet" id="btnResetKill">Clear Kill Switch</button>
          <button class="cbtn coral" id="btnCloseAll2">Close All Positions</button>
          <button class="cbtn info" id="btnSelfCheck">Run Self-Check</button>
        </div>

        <div class="kill-swipe-wrap">
          <div class="kill-swipe-track" id="killSwipeTrack">
            <div class="kill-swipe-fill" id="killSwipeFill"></div>
            <span class="kill-swipe-label" id="killSwipeLabel">SWIPE TO ARM KILL SWITCH</span>
            <div class="kill-swipe-knob" id="killSwipeKnob">💀</div>
          </div>
          <div class="kill-swipe-sub">Blocks ALL new entries until you tap "Clear Kill Switch" above.</div>
        </div>
      </div>
    </section>
  </div>

  <div class="grid" style="margin-top:16px;">
    <section class="panel">
      <h2>Open Positions <span id="posCount">0</span></h2>
      <div class="panel-body scroll" id="posBody">
        <div class="empty">No open positions.<br>The bot is watching — nothing live right now.</div>
      </div>
    </section>
    <section class="panel">
      <h2>Recent Trades <span id="tradeCount">0</span></h2>
      <div class="panel-body scroll" id="tradeBody">
        <div class="empty">No trades logged yet.</div>
      </div>
    </section>
  </div>

  <div class="grid" style="grid-template-columns:1fr; margin-top:16px;">
    <section class="panel">
      <h2>Rejected / Blocked Orders <span id="rejCount">0</span></h2>
      <div class="panel-body scroll" id="rejBody">
        <div class="empty">Nothing blocked or rejected — clean run so far.</div>
      </div>
    </section>
  </div>

  <div class="grid" style="grid-template-columns:1fr; margin-top:16px;">
    <section class="panel">
      <h2>Self-Diagnostics <span id="selfCount">0</span> <button class="cbtn info" id="btnSelfCheck2" style="float:right; font-size:11px; padding:4px 10px;">Run Now</button></h2>
      <div class="panel-body scroll" id="selfBody" style="max-height:280px;">
        <div class="empty">The bot hasn't run its first self-check yet — first report lands within a few minutes of startup.</div>
      </div>
    </section>
  </div>

  <div class="grid" style="grid-template-columns:1fr; margin-top:16px;">
    <section class="panel">
      <h2>Raw Exchange Data <span id="rawCount">0</span></h2>
      <div class="panel-body scroll" id="rawBody" style="max-height:280px;">
        <div class="empty">No requests to Delta yet.</div>
      </div>
    </section>
  </div>

  <div class="grid" style="grid-template-columns:1fr; margin-top:16px;">
    <section class="panel">
      <h2>Ask APEX NEXUS <span style="color:var(--dim); font-weight:400; font-size:11px;">AI diagnostics chat</span></h2>
      <div class="panel-body">
        <div id="askBody" style="max-height:320px; overflow-y:auto; display:flex; flex-direction:column; gap:10px; margin-bottom:12px;">
          <div class="empty">Poochho kuch bhi bot ke bare mein — jaise "API key invalid kyun hai?" ya "kaunsa signal sabse accha perform kar raha hai?"</div>
        </div>
        <div style="display:flex; gap:8px;">
          <input id="askInput" type="text" placeholder="Apna sawal likho ya bolo..."
                 style="flex:1; background:var(--bg); border:1px solid var(--border); border-radius:8px;
                        color:var(--text); font-family:var(--mono); font-size:13px; padding:10px 12px;">
          <button class="cbtn info" id="btnAskMic" title="Bolke poochho" style="width:auto; padding:10px 14px;">🎤</button>
          <button class="cbtn info" id="btnAskSend" style="width:auto; padding:10px 18px;">Ask</button>
        </div>
        <div id="askMicStatus" style="display:none; margin-top:6px; font-size:11px; color:var(--dim);">🔴 Sun raha hoon... bolo</div>
      </div>
    </section>
  </div>
</main>

<div class="modal-overlay" id="settingsOverlay">
  <div class="modal-box">
    <div class="modal-head">
      <h3>Dashboard Settings</h3>
      <button class="modal-close" id="btnCloseSettings" type="button">✕</button>
    </div>
    <div class="modal-sub">Yahan se koi bhi section dashboard se hide kar sakte ho — bot/trading par koi asar nahi padega,
      sirf phone par kam dikhega aur dashboard halka rahega. Jab chaho, wapas ON karke dikha sakte ho.</div>
    <div id="settingsList" class="settings-list"></div>
    <div class="modal-actions">
      <button class="cbtn info" id="btnShowAllPanels" type="button">Sab Dikhao</button>
      <button class="cbtn danger" id="btnHideAllPanels" type="button">Sab Hide Karo</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<div class="ctrlbar">
  <div class="ctrl-row">
    <button class="btn pause" id="btnPause">Pause</button>
    <button class="btn resume" id="btnResume">Resume</button>
    <button class="btn danger" id="btnCloseAll">Close All</button>
  </div>
</div>

<script>
const TOKEN = window.location.pathname.split('/').filter(Boolean).pop();
const q = (extra) => `?key=${encodeURIComponent(TOKEN)}${extra || ''}`;
const ALL_SIGNALS = ["NEXUS","STRONG","FAST","WARP","GHOST","RECOVERY","PULLBACK","SCALP"];

// [DASHBOARD NEW — HIDE/SHOW PANELS] Every section on this page can be
// collapsed with the 👁 button in its header. This is purely visual: the
// bot keeps running and its data keeps being polled either way (hiding a
// panel is NOT the same as pausing the bot — use Trading Control for
// that). What collapsing DOES do is skip the expensive innerHTML re-render
// for that panel on every 5s poll, which is exactly what helps if the page
// starts feeling slow/laggy on a phone with a lot of history piled up
// (Raw Exchange Data and Self-Diagnostics are usually the heaviest).
// Collapsed state is remembered per-panel in localStorage.
function isCollapsed(el){
  return !!(el && el.classList.contains('collapsed'));
}

// [DASHBOARD NEW — SETTINGS] Every panel registers itself here (title,
// storage key, DOM refs, and an applyState() setter) so BOTH the per-panel
// 👁 Hide button AND the central ⚙️ Settings list can drive the exact same
// hide/show state without getting out of sync with each other.
const apexPanelRegistry = [];

function initCollapsiblePanels(){
  document.querySelectorAll('section.panel').forEach((section, idx) => {
    const h2 = section.querySelector('h2');
    const panelBody = section.querySelector('.panel-body');
    if(!h2 || !panelBody) return;

    // Stable-ish key: panel's own visible title text (falls back to index
    // if two panels ever end up with identical titles).
    const title = (h2.textContent || '').trim().replace(/\s+/g,' ');
    const storeKey = 'apexPanelHidden:' + (title || ('panel' + idx));

    // Wrap existing header content so the toggle button can sit on the
    // right without disturbing whatever badges/buttons already live there.
    const left = document.createElement('span');
    left.className = 'panel-header-left';
    while(h2.firstChild) left.appendChild(h2.firstChild);
    h2.appendChild(left);

    const btn = document.createElement('button');
    btn.className = 'panel-toggle';
    btn.type = 'button';

    const entry = {title, storeKey, panelBody, btn};

    function applyState(hidden, opts){
      panelBody.classList.toggle('collapsed', hidden);
      btn.textContent = hidden ? '🙈 Show' : '👁 Hide';
      btn.title = hidden ? 'Is section ko wapas dikhao' : 'Is section ko hide karo';
      localStorage.setItem(storeKey, hidden ? '1' : '0');
      if(entry.switchEl) entry.switchEl.classList.toggle('on', hidden);
      // Bring a just-un-hidden panel's data up to date immediately instead
      // of waiting up to 5s for the next poll (skip on first page load).
      if(!hidden && opts && opts.refresh) refreshAll();
    }
    entry.applyState = applyState;

    applyState(localStorage.getItem(storeKey) === '1');

    btn.onclick = (e) => {
      e.stopPropagation();
      applyState(!panelBody.classList.contains('collapsed'), {refresh:true});
    };

    h2.appendChild(btn);
    apexPanelRegistry.push(entry);
  });
}
initCollapsiblePanels();

// [DASHBOARD NEW — SETTINGS] Central place to hide/show any panel without
// hunting through the page for its individual 👁 button — handy for
// clearing several at once to lighten the dashboard. Purely visual, same
// as the per-panel toggle: never touches the bot or trading itself.
function buildSettingsModal(){
  const list = document.getElementById('settingsList');
  if(!list) return;
  list.innerHTML = apexPanelRegistry.map((entry, i) => `
    <div class="settings-row">
      <span class="settings-row-label">${entry.title || 'Panel ' + (i+1)}</span>
      <div class="switch lime" data-i="${i}"></div>
    </div>
  `).join('');
  list.querySelectorAll('.switch').forEach(sw => {
    const entry = apexPanelRegistry[Number(sw.dataset.i)];
    entry.switchEl = sw;
    sw.classList.toggle('on', isCollapsed(entry.panelBody));
    sw.onclick = () => entry.applyState(!isCollapsed(entry.panelBody), {refresh:true});
  });
}

function openSettings(){
  buildSettingsModal();
  document.getElementById('settingsOverlay').classList.add('show');
}
function closeSettings(){
  document.getElementById('settingsOverlay').classList.remove('show');
}
document.getElementById('btnSettings').onclick = openSettings;
document.getElementById('btnCloseSettings').onclick = closeSettings;
document.getElementById('settingsOverlay').addEventListener('click', (e) => {
  if(e.target.id === 'settingsOverlay') closeSettings();
});
document.getElementById('btnShowAllPanels').onclick = () => {
  apexPanelRegistry.forEach(entry => entry.applyState(false));
  refreshAll();
};
document.getElementById('btnHideAllPanels').onclick = () => {
  apexPanelRegistry.forEach(entry => entry.applyState(true));
};

function toast(msg){
  const el = document.getElementById('toast');
  el.textContent = msg; el.classList.add('show');
  setTimeout(()=>el.classList.remove('show'), 3200);
}
function timeAgo(iso){
  if(!iso) return '';
  const d = (Date.now() - new Date(iso+'Z').getTime())/1000;
  if(d < 60) return Math.floor(d)+'s ago';
  if(d < 3600) return Math.floor(d/60)+'m ago';
  return Math.floor(d/3600)+'h ago';
}
function heartbeat(ok){
  const dot = document.getElementById('hbDot');
  const txt = document.getElementById('hbText');
  dot.className = 'dot ' + (ok ? 'ok pulse' : 'bad');
  txt.textContent = ok ? 'updated just now' : 'connection lost — retrying';
  if(ok) setTimeout(()=>dot.classList.remove('pulse'), 650);
  const foot = document.getElementById('footTrack');
  if(foot) foot.style.animationPlayState = ok ? 'running' : 'paused';
  if(foot) foot.querySelectorAll('.foot').forEach(f => f.style.animationPlayState = ok ? 'running' : 'paused');
}

async function pull(path, extra){
  const t0 = performance.now();
  const r = await fetch(path + q(extra), {cache:'no-store'});
  const ms = Math.round(performance.now() - t0);
  if(!r.ok) throw new Error('HTTP '+r.status);
  const j = await r.json();
  return {data: j, ms};
}
async function push(path, extra){
  // [FIX] This used to return r.json() unconditionally — a 403 (wrong
  // secret) or 500 from the server still resolves fetch() successfully at
  // the network level, so every button appeared to "work" (showed a
  // success toast) even when the action never actually happened on the
  // backend. Checking r.ok and throwing means callers' catch blocks now
  // fire correctly and the toast tells the truth.
  const r = await fetch(path + q(extra), {cache:'no-store'});
  const j = await r.json().catch(() => ({}));
  if(!r.ok){
    throw new Error(j.detail || j.error || ('HTTP ' + r.status));
  }
  return j;
}

let lastConfig = null;

async function refreshConfig(){
  try{
    const {data:d, ms} = await pull('/config');
    lastConfig = d;

    document.getElementById('sysStatus').textContent = 'ONLINE';
    document.getElementById('sysStatus').className = 'bigstat ok';
    document.getElementById('sLastPing').textContent = new Date().toLocaleTimeString();
    document.getElementById('sLatency').textContent = ms + 'ms';
    const drift = d.time_drift && d.time_drift.drift_ms != null ? Math.round(d.time_drift.drift_ms) + 'ms' : '—';
    document.getElementById('sDrift').textContent = drift;
    document.getElementById('sRegionBase').textContent = (d.region || '—');

    document.getElementById('cRegion').textContent = d.region || '—';
    document.getElementById('cProducts').textContent = d.products_discovered ?? '—';
    document.getElementById('cProducts').className = 'v ' + (d.products_discovered > 0 ? 'ok' : 'danger');
    document.getElementById('cCreds').textContent = d.api_credentials_ok === true ? 'VALID' : d.api_credentials_ok === false ? 'INVALID' : 'checking…';
    document.getElementById('cCreds').className = 'v ' + (d.api_credentials_ok === true ? 'ok' : d.api_credentials_ok === false ? 'danger' : '');

    const cb = d.circuit_breaker || {};
    document.getElementById('cCB').textContent = cb.tripped ? 'TRIPPED' : 'clear';
    document.getElementById('cCB').className = 'v ' + (cb.tripped ? 'danger' : 'ok');
    document.getElementById('pStreak').textContent = (cb.consecutive_losses ?? '—') + ' / ' + (cb.max_consecutive_losses ?? '—');

    const badge = document.getElementById('modeBadge');
    badge.textContent = d.live_mode ? 'LIVE · REAL ORDERS' : 'DRY RUN';
    badge.className = 'modebadge ' + (d.live_mode ? 'live' : 'dry');
    if(window.ReactorCore) window.ReactorCore.set({live_mode: !!d.live_mode});

    document.getElementById('bMode').innerHTML = d.live_mode
      ? '<span class="pill" style="color:var(--coral);border-color:var(--coral)">LIVE</span>'
      : '<span class="pill" style="color:var(--info);border-color:var(--info)">DRY_RUN</span>';
    document.getElementById('bSizing').innerHTML = pillHtml(d.risk_based_sizing);
    document.getElementById('bShock').innerHTML = pillHtml(d.block_entries_during_shock !== false);
    document.getElementById('bTelegram').innerHTML = pillHtml(!!d.telegram_enabled);
    document.getElementById('bBracket').innerHTML = pillHtml(!!d.auto_bracket_orders);
    document.getElementById('bKill').innerHTML = d.kill_switch_active
      ? '<span class="pill off" style="color:var(--coral);border-color:var(--coral)">ARMED</span>'
      : '<span class="pill on">clear</span>';

    const sw = document.getElementById('liveSwitch');
    sw.className = 'switch' + (d.live_mode ? ' on' : '');

    const killBanner = document.getElementById('killBanner');
    if(d.kill_switch_active){
      killBanner.textContent = '🛑 KILL SWITCH ARMED — all new entries are blocked until cleared manually.';
      killBanner.classList.add('show');
    } else killBanner.classList.remove('show');
    window.dispatchEvent(new CustomEvent('apex:kill-switch-state', {detail: !!d.kill_switch_active}));

    renderSwitchboard(d.active_signals || [], d.all_known_signals || ALL_SIGNALS);
    heartbeat(true);
  }catch(e){ heartbeat(false); }
}

function pillHtml(on){
  return on ? '<span class="pill on">ON</span>' : '<span class="pill off">OFF</span>';
}

function renderSwitchboard(active, all){
  const set = new Set(active);
  const grid = document.getElementById('switchGrid');
  document.getElementById('swCount').textContent = active.length + '/' + all.length + ' active';
  grid.innerHTML = all.map(s => {
    const on = set.has(s);
    return `<div class="sw ${on?'on':'off'}" data-sig="${s}" data-on="${on}">
      <div class="name">${s}</div><div class="st">${on?'ON':'OFF'}</div>
    </div>`;
  }).join('');
  grid.querySelectorAll('.sw').forEach(el => {
    el.onclick = async () => {
      const sig = el.dataset.sig;
      const isOn = el.dataset.on === 'true';
      const action = isOn ? 'disable' : 'enable';
      try{
        await push(`/signals/${encodeURIComponent(TOKEN)}`, `&${action}=${encodeURIComponent(sig)}`);
        toast(`${sig} turned ${isOn?'OFF':'ON'}`);
        refreshConfig();
      }catch(e){ toast('Failed to toggle ' + sig); }
    };
  });
}

let lastStatus = null;
async function refreshStatus(){
  try{
    const {data:d} = await pull('/status');
    lastStatus = d;
    document.getElementById('cOpen').textContent = d.open_positions ?? '—';
    document.getElementById('cTrades').textContent = d.total_trades ?? '—';
    document.getElementById('pOpen').textContent = d.open_positions ?? '—';
    document.getElementById('pTrades').textContent = d.total_trades ?? '—';
  }catch(e){}
}

// [V9 ADD] Alert Center — every condition below reads a field the dashboard
// already fetches (lastConfig from /config, lastStatus from /status,
// lastBalance from /balance). Nothing is invented; if a signal isn't
// present in the polled data, its alert simply never fires.
let lastBalance = null;
const LOW_BALANCE_USD = 50; // heads-up threshold only, not a hard stop — adjust to taste

function renderAlertCenter(){
  const alerts = [];
  const cfg = lastConfig || {};
  const cb = cfg.circuit_breaker || {};

  if(cfg.kill_switch_active){
    alerts.push({level:'danger', icon:'🛑', text:'Kill switch is ARMED — all new entries are blocked until cleared manually.'});
  }
  if(cb.tripped){
    alerts.push({level:'danger', icon:'⚠️', text:`Circuit breaker tripped — ${cb.reason || 'daily loss / consecutive-loss limit reached.'}`});
  }
  if(cfg.api_credentials_ok === false){
    alerts.push({level:'danger', icon:'🔑', text:'Delta API credentials are invalid — live entries will be blocked until fixed.'});
  }
  if(cfg.products_discovered === 0){
    alerts.push({level:'danger', icon:'📡', text:'Zero products discovered from Delta — check network access and DELTA_REGION.'});
  }
  if(cfg.paused){
    alerts.push({level:'warn', icon:'⏸️', text:'Bot is paused — signals are being received but no new entries will be taken.'});
  }
  if(typeof cb.consecutive_losses === 'number' && typeof cb.max_consecutive_losses === 'number'
     && cb.max_consecutive_losses > 0 && cb.consecutive_losses >= cb.max_consecutive_losses - 1
     && cb.consecutive_losses < cb.max_consecutive_losses){
    alerts.push({level:'warn', icon:'📉', text:`${cb.consecutive_losses}/${cb.max_consecutive_losses} consecutive losses — one more trips the circuit breaker.`});
  }
  const drift = cfg.time_drift && typeof cfg.time_drift.drift_ms === 'number' ? Math.abs(cfg.time_drift.drift_ms) : null;
  if(drift != null && drift > 2000){
    alerts.push({level:'warn', icon:'🕒', text:`Clock drift vs Delta's server is ${Math.round(drift)}ms — signed requests may start failing if this grows.`});
  }
  if(lastBalance && lastBalance.balance != null && Number(lastBalance.balance) < LOW_BALANCE_USD){
    alerts.push({level:'warn', icon:'💰', text:`Account balance is low ($${Number(lastBalance.balance).toFixed(2)}) — check margin before the next entry.`});
  }
  if(lastBalance && lastBalance.error){
    alerts.push({level:'info', icon:'ℹ️', text:`Balance unavailable: ${lastBalance.error}`});
  }

  const body = document.getElementById('alertBody');
  document.getElementById('alertCount').textContent = alerts.length;

  const panel = document.getElementById('aiPanel');
  if(panel){
    panel.classList.remove('glow-ok','glow-bad');
    if(alerts.some(a => a.level === 'danger')) panel.classList.add('glow-bad');
  }

  if(isCollapsed(body)) return;
  if(!alerts.length){
    body.innerHTML = '<div class="alert-clear">No active alerts.</div>';
  } else {
    body.innerHTML = alerts.map(a => `<div class="alert-row ${a.level}"><span class="ic">${a.icon}</span><span>${a.text}</span></div>`).join('');
  }
}

async function refreshPositions(){
  try{
    const {data:d} = await pull('/positions');
    const list = d.positions || [];
    document.getElementById('posCount').textContent = list.length;
    document.getElementById('cArmed').textContent = (lastConfig && lastConfig.paused) ? 'PAUSED' : 'ARMED';
    document.getElementById('cArmed').className = 'v ' + ((lastConfig && lastConfig.paused) ? 'warn' : 'ok');
    const body = document.getElementById('posBody');
    if(isCollapsed(body)){
      if(list.length && list[0].confidence_score != null) paintGauge(Math.round(list[0].confidence_score));
      return;
    }
    if(!list.length){
      body.innerHTML = '<div class="empty">No open positions.<br>The bot is watching — nothing live right now.</div>';
    } else {
      body.innerHTML = list.map(p => {
        const conf = p.confidence_score != null ? Math.round(p.confidence_score) : null;
        const confCls = conf == null ? '' : conf >= 70 ? 'ok' : conf >= 50 ? 'warn' : 'danger';
        return `
        <div class="pos">
          <div class="pos-top">
            <span class="pos-sym">${p.symbol}</span>
            <span class="dir ${p.direction==='BUY'?'buy':'sell'}">${p.direction} · ${p.signal}</span>
          </div>
          <div class="pos-meta">opened ${timeAgo(p.entry_time)} · qty ${p.qty}${p.preset ? ' · ' + p.preset : ''}</div>
          <div class="pos-grid">
            <div><div class="k">SL</div><div class="v">${p.sl ?? '—'}</div></div>
            <div><div class="k">TP1</div><div class="v">${p.tp1 ?? '—'}</div></div>
            <div><div class="k">TP2</div><div class="v">${p.tp2 ?? '—'}</div></div>
            <div><div class="k">TP3</div><div class="v">${p.tp3 ?? '—'}</div></div>
          </div>
          <div class="pos-grid" style="grid-template-columns:repeat(2,1fr); margin-top:6px;">
            <div><div class="k">Confidence</div><div class="v ${confCls}">${conf != null ? conf+'%' : '—'}</div></div>
            <div><div class="k">Systems</div><div class="v">${p.systems ?? '—'}/6</div></div>
          </div>
        </div>`;
      }).join('');
    }

    // Gauge = most recent open position's confidence score, if any (fresher
    // than a closed trade's) — falls back to the latest ENTRY trade in
    // refreshTrades() below if there's nothing open right now.
    if(list.length && list[0].confidence_score != null){
      paintGauge(Math.round(list[0].confidence_score));
    }
  }catch(e){}
}

function paintGauge(val){
  val = Math.max(0, Math.min(100, val));
  const arc = document.getElementById('gaugeArc');
  const total = 283; // path length approximation for the drawn arc
  arc.setAttribute('stroke-dashoffset', String(total - (total * val/100)));
  document.getElementById('gaugeNum').textContent = val;
  const label = val >= 75 ? 'STRONG' : val >= 55 ? 'MODERATE' : val >= 35 ? 'WEAK' : 'VERY WEAK';
  document.getElementById('gaugeLabel').textContent = label;
}

async function refreshTrades(){
  try{
    const {data:d} = await pull('/trades', '&limit=60');
    const list = d.trades || [];
    lastTradesForRadar = list;
    document.getElementById('tradeCount').textContent = list.length;
    const body = document.getElementById('tradeBody');
    if(isCollapsed(body)){
      // Radar + equity curve still depend on this data, so we still parse
      // it below — only the heavy list HTML itself is skipped.
    } else if(!list.length){
      body.innerHTML = '<div class="empty">No trades logged yet.</div>';
    } else {
      body.innerHTML = list.map(t => {
        const cls = t.event === 'ENTRY' ? 'entry' : (String(t.event).toUpperCase().includes('SL') || String(t.event)==='TRADE_CLOSE' && /LOSS/.test(t.raw_result||'') ? 'loss' : 'win');
        const time = t.timestamp ? new Date(t.timestamp+'Z').toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}) : '';
        return `<div class="trade ${cls}">
          <span class="t-time">${time}</span>
          <span class="t-sym">${t.symbol}</span>
          <span class="t-ev">${t.event} · ${t.direction}</span>
          <span class="t-qty">${t.qty}</span>
        </div>`;
      }).join('');
    }

    // Cumulative R + win/loss count, parsed from TRADE_CLOSE raw_result JSON
    // (the only place real outcome/r_multiple data lives) — and gauge
    // fallback if no open position supplied one above.
    let cumR = 0, wins = 0, losses = 0, gaugeSet = false;
    let grossWinR = 0, grossLossR = 0, peak = 0, maxDD = 0;
    // [V9 ADD] Equity curve series — one point per TRADE_CLOSE, in
    // chronological order (list itself is DESC by id, so build then reverse).
    const eqSeries = [];
    for(const t of list){
      if(t.event === 'TRADE_CLOSE' && t.raw_result){
        try{
          const j = JSON.parse(t.raw_result);
          if(typeof j.r_multiple === 'number'){
            cumR += j.r_multiple;
            eqSeries.push(cumR);
            if(j.r_multiple > 0) grossWinR += j.r_multiple;
            else if(j.r_multiple < 0) grossLossR += Math.abs(j.r_multiple);
          }
          if(j.outcome === 'WIN') wins++;
          else if(j.outcome === 'LOSS') losses++;
        }catch(e){}
      }
      if(!gaugeSet && t.event === 'ENTRY' && t.confidence_score != null){
        paintGauge(Math.round(t.confidence_score));
        gaugeSet = true;
      }
    }
    eqSeries.reverse();
    // Max drawdown: worst peak-to-trough dip in the cumulative-R curve,
    // walked in chronological order (eqSeries is already reversed above).
    for(const v of eqSeries){
      if(v > peak) peak = v;
      const dd = peak - v;
      if(dd > maxDD) maxDD = dd;
    }
    drawEquityCurve(eqSeries);

    const pnlEl = document.getElementById('pPnl');
    pnlEl.textContent = (cumR >= 0 ? '+' : '') + cumR.toFixed(2) + 'R';
    pnlEl.className = 'v ' + (cumR > 0 ? 'pos' : cumR < 0 ? 'neg' : '');
    document.getElementById('pWL').textContent = `${wins}W / ${losses}L`;

    const decided = wins + losses;
    document.getElementById('pWinRate').textContent = decided ? `${(wins/decided*100).toFixed(1)}%` : '—';
    document.getElementById('pPF').textContent = grossLossR > 0 ? (grossWinR/grossLossR).toFixed(2)
      : (grossWinR > 0 ? '∞ (no losses yet)' : '—');
    document.getElementById('pMDD').textContent = eqSeries.length ? `-${maxDD.toFixed(2)}R` : '—';

    // [V9 ADD] AI Decision Engine — the most recent ENTRY row, in full.
    const latestEntry = list.find(t => t.event === 'ENTRY');
    renderAIEngine(latestEntry || null);
  }catch(e){}
}

// [V9 ADD] Draws a simple sparkline of cumulative R over time. No chart
// library — a couple dozen lines on a 2D canvas is plenty for this.
function drawEquityCurve(series){
  const canvas = document.getElementById('equityCanvas');
  if(!canvas) return;
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth || 300, h = 120;
  canvas.width = w * dpr; canvas.height = h * dpr;
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr,0,0,dpr,0,0);
  ctx.clearRect(0,0,w,h);
  document.getElementById('eqStart').textContent = (series.length ? '0.00R' : '0.00R');
  document.getElementById('eqNow').textContent = (series.length ? (series[series.length-1]>=0?'+':'')+series[series.length-1].toFixed(2)+'R' : '0.00R');
  if(series.length < 2){
    ctx.fillStyle = '#5B6472'; ctx.font = '11px JetBrains Mono, monospace';
    ctx.fillText('Not enough closed trades yet for a curve.', 8, h/2);
    return;
  }
  const full = [0, ...series];
  const min = Math.min(...full), max = Math.max(...full);
  const range = (max - min) || 1;
  const pad = 8;
  const stepX = (w - pad*2) / (full.length - 1);
  const toY = v => h - pad - ((v - min) / range) * (h - pad*2);

  // zero-line
  ctx.strokeStyle = 'rgba(138,148,166,.25)'; ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(0, toY(0)); ctx.lineTo(w, toY(0)); ctx.stroke();

  const last = full[full.length-1];
  const lineColor = last >= 0 ? '#8FD14F' : '#E85D4E';

  // filled area under the curve
  ctx.beginPath();
  ctx.moveTo(pad, toY(full[0]));
  full.forEach((v,i) => ctx.lineTo(pad + i*stepX, toY(v)));
  ctx.lineTo(pad + (full.length-1)*stepX, toY(0));
  ctx.lineTo(pad, toY(0));
  ctx.closePath();
  const grad = ctx.createLinearGradient(0,0,0,h);
  grad.addColorStop(0, lineColor + '33'); grad.addColorStop(1, lineColor + '00');
  ctx.fillStyle = grad; ctx.fill();

  // the line itself
  ctx.beginPath();
  ctx.moveTo(pad, toY(full[0]));
  full.forEach((v,i) => ctx.lineTo(pad + i*stepX, toY(v)));
  ctx.strokeStyle = lineColor; ctx.lineWidth = 2; ctx.lineJoin = 'round'; ctx.stroke();

  // dot on the last point
  ctx.beginPath();
  ctx.arc(pad + (full.length-1)*stepX, toY(last), 3.5, 0, Math.PI*2);
  ctx.fillStyle = lineColor; ctx.fill();
}

// [V9 ADD] Renders the reasoning behind the most recent ENTRY signal, using
// only columns the bot already logs for every trade (see init_db()'s
// migration list) — RSI/ADX/OFI/KNN/systems/premium_shield/ml_healthy/
// confidence_score/confidence_reason. If a field was never sent by Pine for
// that alert, it honestly shows '—' rather than guessing a number.
function renderAIEngine(t){
  const body = document.getElementById('aiBody');
  const when = document.getElementById('aiWhen');
  if(!t){
    body.innerHTML = '<div class="empty">No signal processed yet — this fills in the moment the first webhook arrives.</div>';
    when.textContent = '';
    return;
  }
  when.textContent = t.timestamp ? timeAgo(t.timestamp) : '';
  const conf = t.confidence_score != null ? Math.round(t.confidence_score) : null;
  if(window.ReactorCore && conf != null) window.ReactorCore.set({confidence_score: conf});
  const dirCls = t.direction === 'BUY' ? 'buy' : 'sell';
  const passFail = (v) => v == null ? '<span class="v">—</span>' : (v ? '<span class="v pass">PASS</span>' : '<span class="v fail">FAIL</span>');
  body.innerHTML = `
    <div class="ai-head">
      <span>
        <span class="ai-verdict ${dirCls}">${t.signal || '—'} · ${t.direction || '—'}</span>
        <span style="color:var(--dim); font-size:11px; margin-left:8px;">${t.symbol || ''}</span>
      </span>
      <span class="ai-conf">${conf != null ? conf + '%' : '—'} <span style="font-size:10px; color:var(--dim); font-weight:400;">confidence</span></span>
    </div>
    <div class="ai-checks">
      <div class="ai-chk"><div class="k">RSI</div><div class="v">${t.rsi != null ? Number(t.rsi).toFixed(1) : '—'}</div></div>
      <div class="ai-chk"><div class="k">ADX</div><div class="v">${t.adx != null ? Number(t.adx).toFixed(1) : '—'}</div></div>
      <div class="ai-chk"><div class="k">OFI %</div><div class="v">${t.ofi_pct != null ? Number(t.ofi_pct).toFixed(1) : '—'}</div></div>
      <div class="ai-chk"><div class="k">KNN</div><div class="v">${t.knn_score != null ? Number(t.knn_score).toFixed(2) : '—'}</div></div>
      <div class="ai-chk"><div class="k">Systems</div><div class="v">${t.systems != null ? t.systems + '/6' : '—'}</div></div>
      <div class="ai-chk"><div class="k">MTF Bars</div><div class="v">${t.mtf_align_bars != null ? t.mtf_align_bars : '—'}</div></div>
      <div class="ai-chk"><div class="k">Premium Shield</div>${passFail(t.premium_shield == null ? null : !!t.premium_shield)}</div>
      <div class="ai-chk"><div class="k">ML Healthy</div>${passFail(t.ml_healthy == null ? null : !!t.ml_healthy)}</div>
    </div>
    <div class="ai-bars">
      ${aiBar('Confidence Engine', conf)}
      ${aiBar('Systems Agreement', t.systems != null ? Math.round(t.systems/6*100) : null)}
      ${aiBar('Trend Strength (ADX)', t.adx != null ? Math.round(Math.min(100, t.adx*2)) : null)}
      ${aiBar('Pattern Match (KNN)', t.knn_score != null ? Math.round(Math.min(100, Math.max(0, t.knn_score*100))) : null)}
    </div>
    ${t.confidence_reason ? `<div class="ai-reason">${t.confidence_reason}</div>` : ''}
  `;
}

function aiBar(label, pct){
  const v = pct == null ? 0 : Math.max(0, Math.min(100, pct));
  return `<div class="ai-bar-row"><span class="lbl">${label}</span>
    <span class="ai-bar-track"><span class="ai-bar-fill" style="width:${v}%"></span></span>
    <span class="pct">${pct == null ? '—' : v+'%'}</span></div>`;
}

async function refreshBalance(){
  try{
    const {data:d} = await pull('/balance');
    lastBalance = d;
    const big = document.getElementById('balBig');
    const note = document.getElementById('balNote');
    const age = document.getElementById('balAge');
    if(d.balance != null){
      big.textContent = '$' + Number(d.balance).toLocaleString(undefined, {maximumFractionDigits:2});
      big.className = 'bigstat ok';
      note.textContent = 'live from Delta wallet';
    } else {
      big.textContent = '—';
      big.className = 'bigstat';
      note.textContent = d.error || 'balance unavailable';
    }
    age.textContent = d.cached_age_s != null ? `(${d.cached_age_s}s old)` : '';
  }catch(e){}
}

// [DASHBOARD NEW — ORDER FLOW / EXECUTION / SYSTEM HEALTH / ARCHITECTURE]
// All four read from real endpoints added alongside them server-side —
// none of these numbers are typed-in placeholders.
let lastOrderFlow = null;

async function refreshOrderFlow(){
  try{
    const {data:d} = await pull('/order-flow');
    lastOrderFlow = d;
    const body = document.getElementById('orderFlowBody');
    if(isCollapsed(body)) return;
    if(!d.has_data){
      body.innerHTML = '<div class="empty">Liquidation feed is live but has not seen an event yet in this window — normal on a quiet market. Checks back every 5s.</div>';
      return;
    }
    const biasLabel = d.bias > 0.15 ? 'BEARISH (sell liq. dominant)' : d.bias < -0.15 ? 'BULLISH (buy liq. dominant)' : 'NEUTRAL';
    const biasCls = d.bias > 0.15 ? 'neg' : d.bias < -0.15 ? 'pos' : '';
    body.innerHTML = `
      <div class="perfrow"><span>Net Flow Bias</span><span class="v ${biasCls}">${biasLabel}</span></div>
      <div class="perfrow"><span>Buy-side Liquidations</span><span class="v pos">${d.buy_liq_qty} <span style="color:var(--dim);font-weight:400;">(${d.buy_liq_count})</span></span></div>
      <div class="perfrow"><span>Sell-side Liquidations</span><span class="v neg">${d.sell_liq_qty} <span style="color:var(--dim);font-weight:400;">(${d.sell_liq_count})</span></span></div>
      <div class="perfrow"><span>Net Flow Qty</span><span class="v">${d.net_flow_qty >= 0 ? '+' : ''}${d.net_flow_qty}</span></div>
      <div style="margin-top:8px; font-size:10.5px; color:var(--dim);">BTCUSDT/ETHUSDT forced liquidations, Binance futures, trailing ${d.window_seconds}s window.</div>`;
  }catch(e){}
}

async function refreshExecutionHealth(){
  try{
    const {data:d} = await pull('/execution-stats', '&limit=100');
    const body = document.getElementById('execHealthBody');
    if(isCollapsed(body)) return;
    if(!d.count){
      body.innerHTML = '<div class="empty">No API calls logged yet.</div>';
      return;
    }
    const okCls = d.success_rate >= 99 ? 'pos' : d.success_rate >= 90 ? '' : 'neg';
    body.innerHTML = `
      <div class="perfrow"><span>Avg Response Time</span><span class="v">${d.avg_ms ?? '—'}ms</span></div>
      <div class="perfrow"><span>Fastest / Slowest</span><span class="v">${d.fastest_ms ?? '—'}ms / ${d.slowest_ms ?? '—'}ms</span></div>
      <div class="perfrow"><span>HTTP Success Rate</span><span class="v ${okCls}">${d.success_rate}%</span></div>
      <div class="perfrow"><span>Calls Sampled</span><span class="v">${d.count}</span></div>
      <div style="margin-top:8px; font-size:10.5px; color:var(--dim);">From the last ${d.count} raw Delta API responses — network/API latency only, not trade slippage.</div>`;
  }catch(e){}
}

async function refreshSystemHealth(){
  try{
    const {data:d} = await pull('/system-health');
    const body = document.getElementById('sysHealthBody');
    if(isCollapsed(body)) return;
    const uptimeH = (d.uptime_seconds / 3600).toFixed(1);
    if(!d.available){
      body.innerHTML = `<div class="perfrow"><span>Uptime</span><span class="v">${uptimeH}h</span></div>
        <div class="empty" style="margin-top:8px;">CPU/memory need <code>psutil</code> — add it to requirements.txt and redeploy to enable.</div>`;
      return;
    }
    const cpuCls = d.cpu_percent > 85 ? 'neg' : '';
    const memCls = d.memory_percent > 85 ? 'neg' : '';
    body.innerHTML = `
      <div class="perfrow"><span>Uptime</span><span class="v">${uptimeH}h</span></div>
      <div class="perfrow"><span>CPU Usage</span><span class="v ${cpuCls}">${d.cpu_percent}%</span></div>
      <div class="perfrow"><span>Memory</span><span class="v ${memCls}">${d.memory_percent}% (${d.memory_mb}MB)</span></div>
      <div class="perfrow"><span>Threads</span><span class="v">${d.thread_count}</span></div>`;
  }catch(e){}
}

// Real active modules, pulled from actual config/kill-switch/order-flow
// state — no invented "quantum" or "neural" layers, just what's genuinely
// running in this file.
function renderArchitecture(){
  const body = document.getElementById('archBody');
  if(isCollapsed(body)) return;
  const d = lastConfig || {};
  const cb = d.circuit_breaker || {};
  const of = lastOrderFlow || {};
  const rows = [
    ["Product Resolver", d.products_discovered > 0, `${d.products_discovered ?? 0} products discovered`],
    ["Credential Validator", d.api_credentials_ok === true, d.api_credentials_ok === true ? "VALID" : d.api_credentials_ok === false ? "INVALID" : "checking"],
    ["Confidence Engine", true, "scoring every signal"],
    ["Circuit Breaker", !cb.tripped, cb.tripped ? "TRIPPED" : "clear"],
    ["Kill Switch Protocol", true, d.kill_switch_active ? "ARMED" : "standby"],
    ["Dynamic Risk Sizing", !!d.risk_based_sizing, d.risk_based_sizing ? "ON" : "OFF"],
    ["Shock Filter", d.block_entries_during_shock !== false, d.block_entries_during_shock !== false ? "ON" : "OFF"],
    ["Auto Bracket Orders", !!d.auto_bracket_orders, d.auto_bracket_orders ? "ON" : "OFF"],
    ["Liquidation Feed", !!of.has_data, of.has_data ? "live data" : "connected, quiet"],
    ["Telegram Notifier", !!d.telegram_enabled, d.telegram_enabled ? "ON" : "OFF"],
    ["Self-Diagnostics", true, "runs every cycle"],
  ];
  body.innerHTML = rows.map(([name, ok, note]) => `
    <div class="perfrow"><span>${name}</span><span class="v ${ok ? 'pos' : ''}">${note}</span></div>
  `).join('');
}


async function refreshRejections(){
  try{
    const {data:d} = await pull('/rejections', '&limit=40');
    const list = d.rejections || [];
    lastRejectionsForRadar = list;
    document.getElementById('rejCount').textContent = list.length;
    const body = document.getElementById('rejBody');
    if(isCollapsed(body)) return;
    if(!list.length){
      body.innerHTML = '<div class="empty">Nothing blocked or rejected — clean run so far.</div>';
    } else {
      body.innerHTML = list.map(r => {
        const time = r.timestamp ? new Date(r.timestamp+'Z').toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}) : '';
        return `<div class="rej">
          <div class="rej-top">
            <span class="rej-sym">${r.symbol} <span style="color:var(--dim);font-weight:400;">${r.direction||''} · ${r.signal||''}</span></span>
            <span class="rej-time">${time}</span>
          </div>
          <div class="rej-reason">${(r.reason||'').replace(/_/g,' ')}</div>
          ${r.detail ? `<div class="rej-detail">${r.detail}</div>` : ''}
        </div>`;
      }).join('');
    }
  }catch(e){}
}

// [SELF-CHECK NEW] Polls the bot's own automated diagnostic feed — messages
// it wrote about itself, unprompted, on its own timer (see /self-reports
// and _self_check_loop() on the backend). No button, no request from the
// user each time; this just shows up as it arrives.
async function refreshSelfReports(){
  try{
    const {data:d} = await pull('/self-reports', '&limit=40');
    const list = d.self_reports || [];
    document.getElementById('selfCount').textContent = list.length;
    const body = document.getElementById('selfBody');
    if(isCollapsed(body)) return;
    if(!list.length){
      body.innerHTML = '<div class="empty">The bot hasn\'t run its first self-check yet — first report lands within a few minutes of startup.</div>';
      return;
    }
    const iconFor = (lvl) => lvl === 'danger' ? '🛑' : (lvl === 'warn' ? '⚠️' : 'ℹ️');
    body.innerHTML = list.map(s => {
      const time = s.timestamp ? new Date(s.timestamp+'Z').toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}) : '';
      return `<div class="alert-row ${s.level}">
        <span class="ic">${iconFor(s.level)}</span>
        <span>
          <strong style="text-transform:capitalize;">${(s.category||'').replace(/_/g,' ')}</strong>
          — ${s.message || ''}
          ${s.detail ? `<div style="color:var(--dim); font-size:11.5px; margin-top:2px;">${s.detail}</div>` : ''}
          <div style="color:var(--dim); font-size:11px; margin-top:2px;">${time}</div>
        </span>
      </div>`;
    }).join('');
  }catch(e){}
}

// [DASHBOARD NEW] The radar itself is real, not decoration: it pulls the
// most recent ENTRY trades (green — order actually taken) and the most
// recent rejections (red — blocked, with the reason on hover/tap), and
// places them around the ring so a glance tells you what the bot has
// actually been doing, not just that it's "running". Positions are laid
// out evenly around the circle by recency, newest first from 12 o'clock.
let lastTradesForRadar = [];
let lastRejectionsForRadar = [];

function renderRadar(){
  const items = [];
  for(const t of lastTradesForRadar){
    if(t.event === 'ENTRY') items.push({symbol:t.symbol, kind:'taken', ts:t.timestamp, detail:`${t.signal} ${t.direction}`});
  }
  for(const r of lastRejectionsForRadar){
    items.push({symbol:r.symbol, kind:'blocked', ts:r.timestamp, detail:(r.reason||'').replace(/_/g,' ')});
  }
  items.sort((a,b) => new Date(b.ts||0) - new Date(a.ts||0));
  const shown = items.slice(0, 10);

  document.getElementById('radarCount').textContent = shown.length ? `${shown.length} recent` : '';
  const wrap = document.getElementById('radarBlips');
  if(!shown.length){
    wrap.innerHTML = '';
    return;
  }
  const cx = 118, cy = 118, radius = 92;
  wrap.innerHTML = shown.map((it, i) => {
    const angle = (i / shown.length) * 2 * Math.PI - Math.PI / 2;
    const x = cx + radius * Math.cos(angle);
    const y = cy + radius * Math.sin(angle);
    const title = `${it.symbol} — ${it.kind === 'taken' ? 'order taken' : 'blocked'} (${it.detail})`;
    return `<div class="radar-blip ${it.kind}" style="left:${x}px; top:${y}px;" title="${title}"></div>
            <div class="radar-label" style="left:${x}px; top:${y}px;">${it.symbol}</div>`;
  }).join('');
}

// [DASHBOARD NEW] Real raw HTTP responses off the wire from Delta Exchange —
// see /raw-api-log and _capture_raw_api_response() on the backend. This is
// the most direct possible answer to "is it actually talking to Delta right
// now": the literal bytes that came back, not a derived summary.
async function refreshRawApiLog(){
  try{
    const {data:d} = await pull('/raw-api-log', '&limit=20');
    const list = d.raw_api_log || [];
    document.getElementById('rawCount').textContent = list.length;
    const body = document.getElementById('rawBody');
    if(isCollapsed(body)) return;
    if(!list.length){
      body.innerHTML = '<div class="empty">No requests to Delta yet.</div>';
      return;
    }
    body.innerHTML = list.map(r => {
      const time = r.timestamp ? new Date(r.timestamp).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'}) : '';
      const cls = r.ok ? 'info' : 'danger';
      const icon = r.ok ? '✅' : '🛑';
      const snippet = (r.body_snippet || '').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      return `<div class="alert-row ${cls}">
        <span class="ic">${icon}</span>
        <span>
          <strong>${r.method} ${r.path}</strong> — HTTP ${r.status_code} (${r.elapsed_ms ?? '?'}ms)
          <div style="color:var(--dim); font-size:11px; margin-top:2px;">${time}</div>
          <pre style="white-space:pre-wrap; word-break:break-all; font-size:11px; color:var(--dim); margin-top:4px; max-height:120px; overflow:auto;">${snippet}${r.truncated ? '…' : ''}</pre>
        </span>
      </div>`;
    }).join('');
  }catch(e){}
}

async function refreshAll(){
  await Promise.all([refreshConfig(), refreshStatus(), refreshPositions(), refreshTrades(),
                      refreshBalance(), refreshRejections(), refreshSelfReports(), refreshRawApiLog(),
                      refreshOrderFlow(), refreshExecutionHealth(), refreshSystemHealth()]);
  renderRadar();
  renderAlertCenter();
  renderArchitecture();
}

// [DASHBOARD NEW — AI Q&A] Client keeps the running chat history (just for
// follow-up context in the prompt) — the backend itself is stateless per
// request, nothing is persisted server-side beyond the bot's own DB.
let askHistory = [];

function askAppendBubble(role, text){
  const body = document.getElementById('askBody');
  const empty = body.querySelector('.empty');
  if(empty) empty.remove();
  const bubble = document.createElement('div');
  const isUser = role === 'user';
  bubble.style.cssText = `align-self:${isUser ? 'flex-end' : 'flex-start'}; max-width:88%; padding:9px 12px;
    border-radius:10px; font-size:12.5px; line-height:1.5; white-space:pre-wrap; word-break:break-word;
    background:${isUser ? 'var(--surface-hi)' : 'transparent'}; border:1px solid var(--border);
    color:${isUser ? 'var(--text)' : 'var(--muted)'};`;
  bubble.textContent = text;
  body.appendChild(bubble);
  body.scrollTop = body.scrollHeight;
  return bubble;
}

async function sendAskQuestion(){
  const input = document.getElementById('askInput');
  const question = input.value.trim();
  if(!question) return;
  input.value = '';
  document.getElementById('btnAskSend').disabled = true;
  askAppendBubble('user', question);
  const thinking = askAppendBubble('assistant', '…thinking');
  try{
    const r = await fetch(`/ask/${encodeURIComponent(TOKEN)}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({question, history: askHistory}),
    });
    const j = await r.json().catch(() => ({}));
    if(!r.ok || j.error){
      thinking.textContent = '⚠️ ' + (j.error || ('HTTP ' + r.status));
    } else {
      thinking.textContent = j.answer;
      askHistory.push({role:'user', content:question});
      askHistory.push({role:'assistant', content:j.answer});
      askHistory = askHistory.slice(-12);
    }
  }catch(e){
    thinking.textContent = '⚠️ Request failed: ' + e.message;
  }
  document.getElementById('btnAskSend').disabled = false;
  document.getElementById('askBody').scrollTop = document.getElementById('askBody').scrollHeight;
}
document.getElementById('btnAskSend').onclick = sendAskQuestion;
document.getElementById('askInput').addEventListener('keydown', (e) => {
  if(e.key === 'Enter') sendAskQuestion();
});

// [DASHBOARD NEW — VOICE INPUT] Mic button for "Ask APEX NEXUS". Uses the
// browser's built-in Web Speech API (SpeechRecognition) — no server-side
// change, no extra API key, no cost. Works in Chrome/Edge on desktop and
// Android; Safari/iOS support is inconsistent, so we detect support and
// just hide the mic button if it's missing instead of showing a broken
// control. Speech is only ever transcribed into the text input — the
// question still only gets sent when the operator hits Ask/Enter, same
// as typing, so this can't accidentally fire a question mid-sentence.
(function setupAskMic(){
  const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  const micBtn = document.getElementById('btnAskMic');
  const micStatus = document.getElementById('askMicStatus');
  const input = document.getElementById('askInput');

  if(!SpeechRecognition){
    micBtn.style.display = 'none'; // graceful fallback: typing still works fine
    return;
  }

  const recognition = new SpeechRecognition();
  recognition.lang = 'hi-IN';        // Hindi/Hinglish speech recognizes well under hi-IN
  recognition.interimResults = true;  // shows partial text while speaking
  recognition.continuous = false;     // stops automatically after one utterance
  recognition.maxAlternatives = 1;

  let listening = false;
  let baseText = '';

  function startListening(){
    baseText = input.value ? input.value + ' ' : '';
    try{ recognition.start(); }
    catch(e){ /* already started — ignore */ }
  }
  function stopListening(){
    try{ recognition.stop(); } catch(e){}
  }

  micBtn.onclick = () => {
    if(listening){ stopListening(); }
    else{ startListening(); }
  };

  recognition.onstart = () => {
    listening = true;
    micBtn.textContent = '⏹️';
    micBtn.style.borderColor = 'var(--danger, #e5484d)';
    micStatus.style.display = 'block';
  };

  recognition.onresult = (event) => {
    let transcript = '';
    for(let i = 0; i < event.results.length; i++){
      transcript += event.results[i][0].transcript;
    }
    input.value = baseText + transcript;
  };

  recognition.onerror = (event) => {
    micStatus.textContent = '⚠️ Mic error: ' + event.error + ' (phone settings me mic permission check karo)';
    micStatus.style.display = 'block';
  };

  recognition.onend = () => {
    listening = false;
    micBtn.textContent = '🎤';
    micBtn.style.borderColor = '';
    micStatus.style.display = 'none';
    input.focus();
  };
})();

// [FIX] Every handler below now wraps its push() call in try/catch. Before
// this fix, push() silently swallowed failures (see push() itself), so a
// wrong CONTROL_PASSWORD, a 403, or a dropped connection would leave the
// button LOOKING like it worked (or, after fixing push() to throw, would
// leave it doing nothing with no feedback at all — an unhandled promise
// rejection visible only in the browser console). Now every button either
// genuinely succeeds and shows the real toast, or fails and tells you why.
document.getElementById('btnPause').onclick = document.getElementById('btnPause2').onclick = async () => {
  try{
    await push(`/control/${encodeURIComponent(TOKEN)}/pause`);
    toast('Paused — no new entries. Exits still process.'); refreshAll();
  }catch(e){ toast('Pause failed: ' + e.message); }
};
document.getElementById('btnResume').onclick = document.getElementById('btnResume2').onclick = async () => {
  try{
    await push(`/control/${encodeURIComponent(TOKEN)}/resume`);
    toast('Resumed — new signals will be taken.'); refreshAll();
  }catch(e){ toast('Resume failed: ' + e.message); }
};
document.getElementById('btnCloseAll').onclick = document.getElementById('btnCloseAll2').onclick = async () => {
  if(!confirm('Close ALL open positions at market, right now?')) return;
  try{
    const d = await push(`/control/${encodeURIComponent(TOKEN)}/close-all`);
    toast(`Closed ${d.closed ?? 0} position(s).`); refreshAll();
  }catch(e){ toast('Close All failed: ' + e.message); }
};
document.getElementById('btnResetCB').onclick = async () => {
  try{
    await push(`/control/${encodeURIComponent(TOKEN)}/reset-circuit-breaker`);
    toast('Circuit breaker reset.'); refreshAll();
  }catch(e){ toast('Reset failed: ' + e.message); }
};
document.getElementById('btnSelfCheck').onclick = document.getElementById('btnSelfCheck2').onclick = async () => {
  try{
    const d = await push(`/control/${encodeURIComponent(TOKEN)}/self-check`);
    const p = d.performance;
    const si = d.system_integrity || {};
    const modeTxt = d.live_mode ? 'LIVE' : 'DRY_RUN';
    const sysTxt = si.all_ok ? 'system OK' : `system ISSUES (${(si.issues||[]).length})`;
    const perfTxt = p
      ? `${p.wins}/${p.n} wins (${p.win_rate}%), ${p.cum_r >= 0 ? '+' : ''}${p.cum_r}R`
      : 'no closed trades yet';
    toast(`Self-check done (${modeTxt}): ${sysTxt} — ${perfTxt}`);
    refreshAll();
  }catch(e){ toast('Self-check failed: ' + e.message); }
};
// [DASHBOARD NEW — SWIPE TO ARM] Drag the knob to the end of the track to
// arm the kill switch. Release early anywhere before the end and it snaps
// back — nothing fires unless the drag actually completes. No confirm()
// popup needed since the drag distance itself is the confirmation.
(function setupKillSwipe(){
  const track = document.getElementById('killSwipeTrack');
  const knob = document.getElementById('killSwipeKnob');
  const fill = document.getElementById('killSwipeFill');
  const label = document.getElementById('killSwipeLabel');
  if(!track || !knob) return;

  let dragging = false, maxX = 0, armed = false;

  function recalcMax(){
    maxX = track.clientWidth - knob.clientWidth - 4;
  }
  recalcMax();
  window.addEventListener('resize', recalcMax);

  function setArmedVisual(on){
    armed = on;
    track.classList.toggle('armed', on);
    label.textContent = on ? '🔴 KILL SWITCH ARMED' : 'SWIPE TO ARM KILL SWITCH';
    knob.style.left = (on ? maxX : 2) + 'px';
    fill.style.width = (on ? '100%' : '0');
  }

  function pointerDown(e){
    if(armed) return; // already armed — use "Clear Kill Switch" button to undo
    dragging = true;
    recalcMax();
    knob.setPointerCapture(e.pointerId);
  }
  function pointerMove(e){
    if(!dragging) return;
    const rect = track.getBoundingClientRect();
    let x = e.clientX - rect.left - knob.clientWidth / 2;
    x = Math.max(2, Math.min(maxX, x));
    knob.style.left = x + 'px';
    fill.style.width = (x + knob.clientWidth / 2) + 'px';
  }
  async function pointerUp(e){
    if(!dragging) return;
    dragging = false;
    const x = parseFloat(knob.style.left) || 2;
    if(x >= maxX - 4){
      setArmedVisual(true);
      try{
        await push(`/control/${encodeURIComponent(TOKEN)}/kill-switch`);
        toast('Kill switch ARMED — all new entries blocked.');
        refreshAll();
      }catch(err){ toast('Kill switch failed: ' + err.message); setArmedVisual(false); }
    } else {
      knob.style.left = '2px';
      fill.style.width = '0';
    }
  }

  knob.addEventListener('pointerdown', pointerDown);
  knob.addEventListener('pointermove', pointerMove);
  knob.addEventListener('pointerup', pointerUp);
  knob.addEventListener('pointercancel', pointerUp);

  // Keep the visual in sync if kill_switch_active changes from elsewhere
  // (e.g. cleared via the "Clear Kill Switch" button, or armed by the bot
  // itself on a circuit-breaker trip).
  window.addEventListener('apex:kill-switch-state', (e) => setArmedVisual(!!e.detail));
})();

document.getElementById('btnResetKill').onclick = async () => {
  try{
    await push(`/control/${encodeURIComponent(TOKEN)}/kill-switch/reset`);
    toast('Kill switch cleared.');
    window.dispatchEvent(new CustomEvent('apex:kill-switch-state', {detail:false}));
    refreshAll();
  }catch(e){ toast('Reset failed: ' + e.message); }
};
document.getElementById('modeBadge').onclick = document.getElementById('liveSwitch').onclick = async () => {
  const goingLive = !(lastConfig && lastConfig.live_mode);
  if(goingLive && !confirm('Switch to LIVE MODE? The bot will place REAL orders with real money on every new signal.')) return;
  try{
    const r = await fetch(`/mode/${encodeURIComponent(TOKEN)}?live_mode=${goingLive}` + '&key=' + encodeURIComponent(TOKEN));
    const d = await r.json();
    if(!r.ok) throw new Error(d.detail || d.error || ('HTTP ' + r.status));
    toast(d.live_mode ? 'Switched to LIVE MODE.' : 'Switched to DRY RUN.');
    refreshAll();
  }catch(e){ toast('Mode switch failed: ' + e.message); }
};
document.getElementById('bSizingRow').onclick = async () => {
  const goingOn = !(lastConfig && lastConfig.risk_based_sizing);
  try{
    await push(`/control/${encodeURIComponent(TOKEN)}/risk-sizing`, `&enabled=${goingOn}`);
    toast(`Dynamic sizing turned ${goingOn ? 'ON' : 'OFF'}.`); refreshAll();
  }catch(e){ toast('Toggle failed: ' + e.message); }
};

// [DASHBOARD FIX] On a phone, locking the screen or switching apps for a
// while doesn't just "pause" this tab — mobile browsers routinely discard
// the page's JS timers entirely (setInterval stops firing) and sometimes
// throttle/suspend the tab, so the 5s auto-refresh silently dies. Nothing
// about the BOT or its data is ever lost — everything lives in the server's
// database and keeps running regardless of whether any phone has this page
// open at all — but the DASHBOARD VIEW goes stale until something kicks it.
// This forces an immediate refresh the moment the tab becomes visible again
// (unlocking the phone, switching back to this app), so you're never staring
// at old data without realizing it.
document.addEventListener('visibilitychange', () => {
  if(document.visibilityState === 'visible') refreshAll();
});

// [V9 ADD] Ambient particle backdrop — purely decorative (see #particleBg
// CSS: fixed, z-index:-1, pointer-events:none), gives the Bloomberg-terminal
// "alive" feel without pulling in a canvas library. Respects
// prefers-reduced-motion by simply not starting.
(function initParticles(){
  const canvas = document.getElementById('particleBg');
  if(!canvas || window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  const ctx = canvas.getContext('2d');
  let w, h, dots = [];
  function resize(){
    w = canvas.width = window.innerWidth;
    h = canvas.height = window.innerHeight;
    const count = Math.round((w * h) / 28000); // density scales with screen size
    dots = Array.from({length: count}, () => ({
      x: Math.random()*w, y: Math.random()*h,
      vx: (Math.random()-0.5)*0.15, vy: (Math.random()-0.5)*0.15,
      r: Math.random()*1.4 + 0.4,
    }));
  }
  function tick(){
    ctx.clearRect(0,0,w,h);
    ctx.fillStyle = 'rgba(143,209,79,0.35)';
    for(const d of dots){
      d.x += d.vx; d.y += d.vy;
      if(d.x < 0) d.x = w; if(d.x > w) d.x = 0;
      if(d.y < 0) d.y = h; if(d.y > h) d.y = 0;
      ctx.beginPath(); ctx.arc(d.x, d.y, d.r, 0, Math.PI*2); ctx.fill();
    }
    requestAnimationFrame(tick);
  }
  window.addEventListener('resize', resize);
  resize(); tick();
})();

// [PREMIUM UI ADD] Reactor core — the dashboard's signature visual. This is
// intentionally NOT a decorative random animation: ring count/brightness
// tracks the real confidence_score of the most recent processed signal
// (same number shown in the AI Engine panel), and its color follows the
// real LIVE/DRY mode. window.ReactorCore.set(...) is called from
// renderAIEngine() and refreshStatus() below — search those for the hook.
window.ReactorCore = (function(){
  const canvas = document.getElementById('reactor');
  if(!canvas) return {set(){}};
  const ctx = canvas.getContext('2d');
  const reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  let confidence = 0;      // 0-100, real value once a signal has been processed
  let live = false;        // real LIVE/DRY state
  let t = 0;
  function color(){ return live ? '143,80,80' : (confidence >= 70 ? '143,209,79' : confidence >= 40 ? '232,163,61' : '63,224,208'); }
  function draw(){
    const w = canvas.width, h = canvas.height, cx = w/2, cy = h/2;
    ctx.clearRect(0,0,w,h);
    const rgb = color();
    const pulse = reduced ? 0 : Math.sin(t/32) * 2;
    // core disc
    const coreR = 8 + (confidence/100)*4;
    const grad = ctx.createRadialGradient(cx,cy,0,cx,cy,coreR+6);
    grad.addColorStop(0, `rgba(${rgb},0.9)`); grad.addColorStop(1, `rgba(${rgb},0)`);
    ctx.fillStyle = grad; ctx.beginPath(); ctx.arc(cx,cy,coreR+6,0,Math.PI*2); ctx.fill();
    ctx.fillStyle = `rgba(${rgb},1)`; ctx.beginPath(); ctx.arc(cx,cy,coreR/2.2,0,Math.PI*2); ctx.fill();
    // orbit rings — count scales with confidence so a real 0% state reads as idle, not fake-busy
    const ringCount = confidence > 0 ? 1 + Math.round(confidence/34) : 1;
    for(let i=0;i<ringCount;i++){
      const rr = 12 + i*7 + pulse;
      ctx.strokeStyle = `rgba(${rgb},${0.5 - i*0.12})`;
      ctx.lineWidth = 1.1;
      ctx.beginPath(); ctx.arc(cx,cy,rr,0,Math.PI*2); ctx.stroke();
    }
    if(!reduced){ t++; requestAnimationFrame(draw); }
  }
  draw();
  return {
    set({confidence_score, live_mode}){
      if(confidence_score != null) confidence = Math.max(0, Math.min(100, confidence_score));
      if(live_mode != null) live = !!live_mode;
      if(reduced) draw(); // static redraw on state change when motion is reduced
    }
  };
})();

refreshAll();
setInterval(refreshAll, 5000);
</script>
</body>
</html>"""


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


# ════════════════════════════════════════════════════════════════════════════════
# [DASHBOARD NEW — REAL CANDLES] Real OHLCV straight from Delta's public
# /v2/history/candles (same unsigned, no-auth market-data call
# get_last_traded_price() already uses — confirmed against Delta's own API
# docs). This is what lets the INFINITY dashboard's chart plot your bot's
# actual traded symbol instead of a simulated random walk. Goes through the
# shared delta_http Session like every other outbound Delta call in this
# file, so it inherits the User-Agent fix automatically. Cached briefly per
# (symbol, resolution, limit) so several open dashboard tabs polling every
# few seconds don't multiply into N calls to Delta per tab.
# ════════════════════════════════════════════════════════════════════════════════
_candles_cache = {}
_candles_cache_lock = threading.Lock()
CANDLES_CACHE_MAX_AGE_S = 15
_CANDLE_RESOLUTION_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600,
    "1d": 86400, "7d": 604800, "30d": 2592000, "1w": 604800, "2w": 1209600,
}


@app.route("/candles", methods=["GET"])
@require_key
def candles():
    """GET /candles?symbol=BTCUSD&resolution=1m&limit=100
    Proxies Delta's public history-candles endpoint so the browser dashboard
    never has to talk to Delta directly (avoids CORS entirely, and keeps API
    conventions — User-Agent, region, timeouts — in the one shared place).
    Returns { symbol, resolution, candles:[{time,open,high,low,close,volume}] },
    oldest first, same shape the dashboard's chart already expects.
    """
    symbol = (request.args.get("symbol") or "BTCUSD").strip().upper()
    resolution = (request.args.get("resolution") or "1m").strip()
    try:
        limit = max(10, min(int(request.args.get("limit", 100)), 500))
    except (TypeError, ValueError):
        limit = 100

    cache_key = (symbol, resolution, limit)
    now = time.time()
    with _candles_cache_lock:
        cached = _candles_cache.get(cache_key)
        if cached and (now - cached["ts"]) < CANDLES_CACHE_MAX_AGE_S:
            return jsonify({"symbol": symbol, "resolution": resolution, "candles": cached["candles"]})

    step = _CANDLE_RESOLUTION_SECONDS.get(resolution, 60)
    end_ts = int(now)
    start_ts = end_ts - step * (limit + 2)

    try:
        resp = delta_http.get(
            f"{BASE_URL}/v2/history/candles",
            params={"symbol": symbol, "resolution": resolution, "start": start_ts, "end": end_ts},
            timeout=6,
        )
        resp.raise_for_status()
        raw = resp.json().get("result", []) or []
    except Exception as e:
        log.debug(f"/candles failed for {symbol}/{resolution}: {e}")
        return jsonify({"symbol": symbol, "resolution": resolution, "candles": [], "error": "delta_unreachable"}), 200

    raw = sorted(raw, key=lambda c: c.get("time", 0))[-limit:]
    candles_out = [
        {
            "time": c.get("time"),
            "open": safe_float(c.get("open")),
            "high": safe_float(c.get("high")),
            "low": safe_float(c.get("low")),
            "close": safe_float(c.get("close")),
            "volume": safe_float(c.get("volume")),
        }
        for c in raw
    ]

    with _candles_cache_lock:
        _candles_cache[cache_key] = {"ts": now, "candles": candles_out}

    return jsonify({"symbol": symbol, "resolution": resolution, "candles": candles_out})


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

