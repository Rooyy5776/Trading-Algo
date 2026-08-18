# Trading-Algo
APEX NEXUS Trading Bot
# delta_data_pipeline.py — APEX NEXUS real-data master pipeline

Standalone Delta Exchange (**India**) OHLCV + derivatives data-extraction
pipeline. Upgrades the existing single-file pipeline in place — same two
classes (`DeltaHistoricalFetcher` REST, `DeltaLiveFeed` WebSocket), same
canonical `[open, high, low, close, volume]` output schema, now multi-symbol,
multi-timeframe, resumable, and audited by an independent data-quality
checker. Reads no API key/secret anywhere; touches public market-data
endpoints only.

## One-command usage

```bash
pip install -r requirements.txt
python delta_data_pipeline.py
```

Downloads 60 days of `BTCUSD,ETHUSD,SOLUSD,BNBUSD` × `1m,5m,15m,1h` into
`delta_dataset/`, validates every symbol against Delta's live product
catalog first, writes the manifest + hashes + data-quality report at the end.
Override any of that with flags or environment variables — see below.

## Before you trust any of this: read the source-verification block

Open `delta_data_pipeline.py` and read the docstring at the top
(`SECTION 13 — SOURCE VERIFICATION`) before changing `REST_BASE_URL` or
`WS_URL`. Short version:

| | old file said | now | source |
|---|---|---|---|
| REST host | `api.india.delta.exchange`, commented "GLOBAL" | same host, comment fixed to **INDIA** | docs.delta.exchange + a live fetch of `/v2/products` against this exact host |
| WS host | `wss://public-socket.india.delta.exchange` | `wss://socket.india.delta.exchange` (no `public-`) | 3 independent working code samples, none use a `public-socket.` host |
| `BTCUSD`/`ETHUSD` symbols | comment implied these were wrong for India | confirmed live: product_id 27 / 3136 on `api.india.delta.exchange` | live `/v2/products` fetch |
| `SOLUSD`/`BNBUSD` symbols | assumed | **not** individually confirmed from the dev sandbox | see "What was and wasn't tested" below — this is exactly why symbol validation is a runtime check, not an assumption |

## What was and wasn't tested (read this, not just the green checkmarks)

This was written in a sandbox with **no outbound network access from
Python**. Everything above the line was checked with an external
web-search/fetch tool (including one live, unauthenticated fetch of
`https://api.india.delta.exchange/v2/products`, which returned real,
current contracts) — never from training-data memory alone, and never from
this repo's own Python interpreter.

**Actually run in this sandbox and passing (16/16):**
`python3 test_pipeline.py` — pagination math, resume-from-checkpoint, 429
rate-limit handling, duplicate/gap/NaN/Inf/bad-OHLC/negative-volume
detection, causal aggregation correctness (incl. rejecting incomplete
trailing buckets), contract-spec field mapping against a payload shaped
exactly like the real live product response, manifest+SHA256 generation,
WebSocket message parsing (both field-naming conventions), graceful
soft-failure of the trades endpoint. All against **mocked** HTTP — this
proves the code is mechanically correct, not that Delta's API still matches
the mock today.

**Also run for real, end-to-end, with `main()` itself:** `test_cli_smoke.py`
— full CLI flag surface (`--contract-specs --aggregate-causal --trades`
etc.), confirmed the on-disk layout matches section 10.

**NOT done from this sandbox, because it requires outbound network:**
a real 60-day four-symbol download, and a real captured WebSocket candle
message. Run this once you have network:

```bash
python delta_data_pipeline.py --self-test
```

This is the actual small real-data test section 14 asks for (2 symbols,
short window, 1m + one higher timeframe, resume behavior, manifest
generation) — it writes a real pass/fail result to
`delta_dataset/reports/SELF_TEST_RESULT.json` instead of anyone just
claiming it passed. One honest note on it: each failed request retries with
exponential backoff (2s/4s/8s/16s/…) before giving up, which is the right
policy for a real 60-day pull but means `--self-test` can take a couple of
minutes to fail closed on a machine that's *actually* offline — that's
working as designed, not a hang.

## What Delta's public API does and doesn't expose (section 5, honestly)

| Data | Status | Notes |
|---|---|---|
| OHLCV candles | ✅ available | `/v2/history/candles`, as before |
| Mark price history | ✅ available | same endpoint, symbol prefixed `MARK:<symbol>` — this is Delta's own documented Symbology convention, not a guess. `MarkPriceHistoryFetcher` reuses all of `DeltaHistoricalFetcher`'s pagination/resume/dedup logic. |
| Public trades | ✅ available, path not body-verified | docs.delta.exchange confirms a "Get public trades" section exists; the page was too large to fetch in full while researching this, so the exact query parameter wasn't confirmed. `PublicTradesFetcher` fails soft (`NOT_AVAILABLE` + exactly what to check) instead of guessing. |
| Funding rate **history** | ❌ `NOT_AVAILABLE` | No dedicated public REST endpoint found in Delta's own endpoint index. A live `funding_rate` WebSocket channel exists (streams the current rate) but is not a queryable history. Current funding *parameters* (cap, method) are available per-symbol via `ContractSpecFetcher`, which is a contract setting, not a realized-rate history — kept separate deliberately. |
| Open interest **history** | ❌ not exchange-provided | No dedicated history endpoint found. Current OI *is* a live snapshot field on `/v2/tickers`. `OpenInterestSnapshotFetcher` polls that and appends to a local file — it only ever contains OI **from the point you started polling forward**, and is labeled `"source": "constructed_from_ticker_snapshot"` in every row so it's never mistaken for exchange history. |
| Contract metadata | ✅ available | `/v2/products/{symbol}`. Two fields the spec asked for — "lot/step size" and "minimum notional" — don't exist as literal fields on Delta's product object (contracts are integer-sized); both are recorded as `NOT_AVAILABLE` rather than guessed. |

## Storage layout (section 10)

```
delta_dataset/
  raw/SYMBOL/TIMEFRAME/raw.jsonl        # every field Delta returned, deduped by timestamp, untouched otherwise
  normalized/SYMBOL/TIMEFRAME/normalized.csv
  normalized/SYMBOL/TF_causal_from_1m/normalized.csv   # only when --aggregate-causal is used
  funding/                               # reserved; funding history is NOT_AVAILABLE (see table above)
  mark_price/SYMBOL/TIMEFRAME/mark_price.csv
  open_interest/SYMBOL/oi_snapshots.jsonl
  order_flow/SYMBOL/trades.jsonl
  contract_specs/SYMBOL.json
  manifests/MASTER_DATASET_MANIFEST.json, MASTER_DATASET_SHA256.txt
  reports/DATA_QUALITY_REPORT.md, SELF_TEST_RESULT.json, *_native_vs_causal.json
  checkpoints/SYMBOL_TIMEFRAME.json      # resume state; not in the original spec list, added because
                                          # section 3 explicitly requires resume/checkpoint and it needs a home
```

`funding/`, `mark_price/`, `open_interest/` are created lazily the first
time a run actually uses that feature, not on every run.

## CLI reference

```
--symbols BTCUSD,ETHUSD,SOLUSD,BNBUSD   comma-separated (default from DELTA_SYMBOLS env or built-in default)
--timeframes 1m,5m,15m,1h
--lookback-days 60
--start-time / --end-time                epoch seconds; overrides --lookback-days
--data-dir delta_dataset
--no-resume                              ignore any existing checkpoint, start clean
--validate-symbols                       just check symbols against live /v2/products and print, then exit
--contract-specs                         also fetch+save contract_specs/SYMBOL.json
--mark-price                             also fetch MARK:<symbol> price history alongside trade-price history
--funding                                attempt funding-rate history (will report NOT_AVAILABLE — see table above)
--open-interest N                        poll the live OI snapshot N times, 60s apart, instead of a history fetch
--trades                                 fetch a public-trades snapshot
--aggregate-causal                       build 5m/15m/1h from already-downloaded 1m data + a native-vs-aggregated report
--skip-history                           combine with the flags above to skip the main OHLCV pull
--self-test                              run the real section-14 small real-data test (needs network)
```

Every value also has a `DELTA_*` environment variable — see
`config.example.env` — so this deploys as a plain env-driven AWS/systemd job
with no code edits.

## Design choices worth knowing about

- **Raw vs normalized dedup**: both layers dedupe by exact timestamp on
  write (last-fetch-wins). Section 7 says "do not silently repair raw
  data" — this file's reading of that is: don't alter a *value* Delta sent,
  but collapsing an exact-duplicate row from overlapping pagination windows
  is the explicit "duplicate prevention" requirement in section 3, not
  data repair. If you want the alternate reading (raw = literally
  append-only, dupes and all), it's a one-line change in
  `append_raw_jsonl()` — flagged here rather than silently decided either way.
- **Causal aggregation** only ever emits a bucket once every constituent 1m
  bar is present *and* the bucket has fully closed — a gap in the 1m source
  causes that bucket to be skipped, never fabricated.
- **`DeltaLiveFeed`** now tries both short-field (`o/h/l/c/v/ts`) and
  long-field (`open/high/low/close/candle_start_time`) message shapes,
  because the old file's docstring and its own code actually disagreed with
  each other about which one Delta sends. It still logs the first raw
  message at INFO so you can confirm by eye on first real run.
