"""
Unit tests for delta_data_pipeline.py using MOCKED HTTP — no real network.
This proves the pipeline's LOGIC is correct (pagination math, resume,
dedup, data-quality checks, causal aggregation, manifest/hash generation,
contract-spec field mapping, WS message parsing). It does NOT prove Delta's
real API still matches these mocks today — that requires --self-test on a
machine with network. Run: python3 test_pipeline.py
"""
import json
import os
import shutil
import sys
import time
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(__file__))
import delta_data_pipeline as ddp

TEST_DIR = "/tmp/ddp_test_dataset"


def make_candles(start_ts, n, step=60, price=50000.0):
    out = []
    for i in range(n):
        t = start_ts + i * step
        out.append({"time": t, "open": price + i, "high": price + i + 5,
                     "low": price + i - 5, "close": price + i + 2, "volume": 10 + i})
    return out


class FakeResponse:
    def __init__(self, json_data, status_code=200, headers=None):
        self._json = json_data
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"status {self.status_code}")


class TestStandardize(unittest.TestCase):
    def test_dedup_and_sort(self):
        records = make_candles(1000, 3) + [make_candles(1000, 1)[0]]  # inject one exact dup
        df = ddp.standardize(records)
        self.assertEqual(len(df), 3)  # dup collapsed
        self.assertTrue(df.index.is_monotonic_increasing)
        self.assertEqual(list(df.columns), ddp.CANONICAL_COLUMNS)

    def test_empty(self):
        df = ddp.standardize([])
        self.assertEqual(len(df), 0)
        self.assertEqual(str(df.index.tz), "UTC")


class TestHistoricalFetchPagination(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(TEST_DIR, ignore_errors=True)
        os.makedirs(TEST_DIR, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(TEST_DIR, ignore_errors=True)

    def test_multi_page_pagination_and_save(self):
        """3 windows worth of 1m candles -> verify exactly 3 requests made
        with correctly-advancing start/end, and correct row count saved."""
        res_sec = 60
        window_span = ddp.MAX_CANDLES_PER_REQUEST * res_sec
        start = 1_700_000_000
        end = start + 3 * window_span  # forces exactly 3 pagination windows

        calls = []

        def fake_request(self_session, method, url, params=None, timeout=None, headers=None):
            calls.append(dict(params))
            s, e = params["start"], params["end"]
            n = min(50, max(0, (e - s) // res_sec))  # small page per window, doesn't need to hit the 2000 cap
            return FakeResponse({"success": True, "result": make_candles(s, n, step=res_sec)})

        with patch.object(ddp.requests.Session, "request", fake_request):
            hist = ddp.DeltaHistoricalFetcher(data_dir=TEST_DIR)
            df = hist.fetch_range("BTCUSD", "1m", start_ts=start, end_ts=end, resume=False)

        self.assertEqual(len(calls), 3, "should paginate into exactly 3 windows")
        self.assertEqual(calls[0]["start"], start)
        self.assertEqual(calls[1]["start"], calls[0]["end"])
        self.assertEqual(calls[2]["end"], end)
        self.assertEqual(len(df), 150)  # 3 windows * 50 rows
        self.assertTrue(os.path.exists(ddp.normalized_path("BTCUSD", "1m", TEST_DIR)))
        self.assertTrue(os.path.exists(ddp.raw_path("BTCUSD", "1m", TEST_DIR)))

    def test_resume_from_checkpoint(self):
        """Simulate a crash after window 1 of 3, then re-run with resume=True
        and confirm it starts from the checkpoint's cursor, not from scratch."""
        res_sec = 60
        window_span = ddp.MAX_CANDLES_PER_REQUEST * res_sec
        start = 1_700_000_000
        end = start + 3 * window_span
        calls = []

        def fake_request(self_session, method, url, params=None, timeout=None, headers=None):
            calls.append(dict(params))
            s, e = params["start"], params["end"]
            return FakeResponse({"success": True, "result": make_candles(s, 10, step=res_sec)})

        ckpt = ddp.Checkpoint(TEST_DIR, "ETHUSD", "1m")
        # hand-craft a checkpoint as if window 1 already completed
        ckpt.save(start_ts=start, end_ts=end, cursor_ts=start + window_span, complete=False)

        with patch.object(ddp.requests.Session, "request", fake_request):
            hist = ddp.DeltaHistoricalFetcher(data_dir=TEST_DIR)
            hist.fetch_range("ETHUSD", "1m", start_ts=start, end_ts=end, resume=True)

        self.assertEqual(len(calls), 2, "should only fetch the 2 remaining windows, not all 3")
        self.assertEqual(calls[0]["start"], start + window_span)

    def test_rate_limit_429_honors_reset_header(self):
        attempts = {"n": 0}

        def fake_request(self_session, method, url, params=None, timeout=None, headers=None):
            attempts["n"] += 1
            if attempts["n"] == 1:
                return FakeResponse({}, status_code=429, headers={"X-RATE-LIMIT-RESET": "1"})
            return FakeResponse({"success": True, "result": make_candles(params["start"], 5)})

        with patch.object(ddp.requests.Session, "request", fake_request):
            with patch("time.sleep", return_value=None):  # don't actually wait in tests
                hist = ddp.DeltaHistoricalFetcher(data_dir=TEST_DIR)
                df = hist.fetch_range("BTCUSD", "1m", start_ts=1_700_000_000, end_ts=1_700_000_300, resume=False)
        self.assertGreaterEqual(attempts["n"], 2)
        self.assertEqual(len(df), 5)


class TestDataQuality(unittest.TestCase):
    def test_flags_gaps_dupes_bad_ohlc_negative_volume(self):
        import pandas as pd
        recs = make_candles(1000, 5, step=60)
        # introduce a gap: skip candle index 2 by removing it, then re-add a duplicate of index 0
        recs = [recs[0], recs[0], recs[1], recs[3], recs[4]]
        df = ddp.standardize(recs)
        # standardize() already dedupes -> to actually test duplicate DETECTION we must
        # bypass standardize's own dedup and build the frame directly
        raw_df = pd.DataFrame(recs)
        raw_df["timestamp"] = pd.to_datetime(raw_df["time"], unit="s", utc=True)
        raw_df = raw_df.set_index("timestamp")[ddp.CANONICAL_COLUMNS]
        raw_df.loc[raw_df.index[0], "high"] = raw_df.loc[raw_df.index[0], "low"] - 100  # force invalid OHLC
        raw_df.loc[raw_df.index[-1], "volume"] = -5  # force negative volume

        report = ddp.validate_quality(raw_df, symbol="BTCUSD", timeframe="1m")
        self.assertGreaterEqual(report.duplicate_count, 1)
        self.assertGreaterEqual(report.gap_count, 1)
        self.assertGreaterEqual(report.ohlc_invalid_count, 1)
        self.assertGreaterEqual(report.negative_volume_count, 1)
        self.assertEqual(report.status, "FAIL")

    def test_clean_data_passes(self):
        df = ddp.standardize(make_candles(2_000_000, 10, step=60))
        report = ddp.validate_quality(df, symbol="BTCUSD", timeframe="1m")
        self.assertEqual(report.duplicate_count, 0)
        self.assertEqual(report.gap_count, 0)
        self.assertEqual(report.ohlc_invalid_count, 0)
        self.assertIn(report.status, ("PASS", "WARN"))


class TestCausalAggregation(unittest.TestCase):
    def test_aggregation_matches_manual_ohlcv_and_excludes_incomplete_bucket(self):
        import pandas as pd
        # 12 minutes of clean 1m data starting at a 5m-aligned boundary in the past
        start = int(pd.Timestamp("2024-01-01T00:00:00Z").timestamp())
        recs = make_candles(start, 12, step=60, price=100.0)
        df_1m = ddp.standardize(recs)
        agg = ddp.aggregate_causal(df_1m, "5m")
        # 12 minutes -> two complete 5m buckets (0-5, 5-10), remaining 2 min (10-12) incomplete -> dropped
        self.assertEqual(len(agg), 2)
        first_bucket = df_1m.iloc[0:5]
        self.assertAlmostEqual(agg.iloc[0]["open"], first_bucket.iloc[0]["open"])
        self.assertAlmostEqual(agg.iloc[0]["close"], first_bucket.iloc[-1]["close"])
        self.assertAlmostEqual(agg.iloc[0]["high"], first_bucket["high"].max())
        self.assertAlmostEqual(agg.iloc[0]["low"], first_bucket["low"].min())
        self.assertAlmostEqual(agg.iloc[0]["volume"], first_bucket["volume"].sum())

    def test_compare_native_vs_aggregated(self):
        import pandas as pd
        start = int(pd.Timestamp("2024-01-01T00:00:00Z").timestamp())
        df_1m = ddp.standardize(make_candles(start, 10, step=60, price=100.0))
        agg = ddp.aggregate_causal(df_1m, "5m")
        native = agg.copy()
        native.iloc[0, native.columns.get_loc("close")] += 1.0  # introduce a deliberate discrepancy
        report = ddp.compare_native_vs_aggregated(native, agg)
        self.assertEqual(report["common_timestamps"], len(agg))
        self.assertGreater(report["per_column_mean_abs_diff"]["close"], 0)


class TestContractSpecMapping(unittest.TestCase):
    """Uses a response shaped exactly like the REAL live /v2/products/{symbol}
    payload fetched from api.india.delta.exchange while writing this file."""

    LIVE_SHAPED_PRODUCT = {
        "id": 27, "symbol": "BTCUSD", "contract_type": "perpetual_futures", "state": "live",
        "trading_status": "operational", "tick_size": "0.5", "contract_value": "0.001",
        "position_size_limit": 229167, "default_leverage": "200.000000000000000000",
        "max_leverage_notional": "100000", "initial_margin": "0.5", "maintenance_margin": "0.25",
        "initial_margin_scaling_factor": "0.0000025", "maintenance_margin_scaling_factor": "0.00000125",
        "funding_method": "mark_price", "annualized_funding": "10.95",
        "product_specs": {"funding_clamp_value": 0.05},
        "quoting_asset": {"symbol": "USD"}, "settling_asset": {"symbol": "USD"}, "underlying_asset": {"symbol": "BTC"},
    }

    def test_get_spec_maps_all_real_fields_and_flags_missing_ones(self):
        def fake_request(self_session, method, url, params=None, timeout=None, headers=None):
            return FakeResponse({"success": True, "result": TestContractSpecMapping.LIVE_SHAPED_PRODUCT})

        with patch.object(ddp.requests.Session, "request", fake_request):
            csf = ddp.ContractSpecFetcher()
            spec = csf.get_spec("BTCUSD")

        self.assertTrue(spec["found"])
        self.assertEqual(spec["product_id"], 27)
        self.assertEqual(spec["tick_size"], "0.5")
        self.assertEqual(spec["settlement"]["settling_asset"], "USD")
        self.assertEqual(spec["leverage"]["max_leverage_notional"], "100000")
        self.assertEqual(spec["funding"]["funding_clamp_value"], 0.05)
        # fields genuinely absent from the live schema must stay NOT_AVAILABLE, never guessed
        self.assertEqual(spec["min_quantity"], ddp.NOT_AVAILABLE)
        self.assertEqual(spec["min_notional"], ddp.NOT_AVAILABLE)

    def test_validate_symbols_flags_dead_symbol(self):
        def fake_request(self_session, method, url, params=None, timeout=None, headers=None):
            if "DEADCOIN" in url:
                return FakeResponse({"success": True, "result": {}})
            return FakeResponse({"success": True, "result": TestContractSpecMapping.LIVE_SHAPED_PRODUCT})

        with patch.object(ddp.requests.Session, "request", fake_request):
            with patch("time.sleep", return_value=None):
                csf = ddp.ContractSpecFetcher()
                results = csf.validate_symbols(["BTCUSD", "DEADCOIN"])
        self.assertTrue(results["BTCUSD"]["valid_for_pipeline"])
        self.assertFalse(results["DEADCOIN"]["valid_for_pipeline"])


class TestManifestAndHashing(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(TEST_DIR, ignore_errors=True)
        os.makedirs(TEST_DIR, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(TEST_DIR, ignore_errors=True)

    def test_manifest_and_sha256_written_and_correct(self):
        df = ddp.standardize(make_candles(1_700_000_000, 20, step=60))
        path = ddp.normalized_path("BTCUSD", "1m")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=True, index_label="timestamp")
        q = ddp.validate_quality(df, symbol="BTCUSD", timeframe="1m")
        entry = ddp.manifest_entry_for_file(path, symbol="BTCUSD", timeframe="1m", quality=q)
        mpath, hpath = ddp.write_manifest_and_hashes([entry], data_dir=TEST_DIR)

        self.assertTrue(os.path.exists(mpath))
        self.assertTrue(os.path.exists(hpath))
        with open(mpath) as f:
            manifest = json.load(f)
        self.assertEqual(manifest["files"][0]["row_count"], 20)
        self.assertEqual(manifest["files"][0]["sha256"], ddp.sha256_file(path))
        with open(hpath) as f:
            hashes_content = f.read()
        self.assertIn(ddp.sha256_file(path), hashes_content)


class TestPublicTradesSoftFail(unittest.TestCase):
    def test_soft_fails_with_not_available_not_a_crash(self):
        def fake_request(self_session, method, url, params=None, timeout=None, headers=None):
            return FakeResponse({}, status_code=404)

        with patch.object(ddp.requests.Session, "request", fake_request):
            with patch("time.sleep", return_value=None):
                res = ddp.PublicTradesFetcher(data_dir=TEST_DIR).fetch("BTCUSD")
        self.assertEqual(res["status"], ddp.NOT_AVAILABLE)
        self.assertIn("reason", res)


class TestWSMessageParsing(unittest.TestCase):
    def test_short_field_convention(self):
        rec = ddp.DeltaLiveFeed._parse_candle({"o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "10", "ts": 1700000000000000})
        self.assertIsNotNone(rec)
        self.assertEqual(rec["open"], 1.0)
        self.assertEqual(rec["time"], 1700000000.0)

    def test_long_field_convention_fallback(self):
        rec = ddp.DeltaLiveFeed._parse_candle({"open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10,
                                                 "candle_start_time": 1700000000000000})
        self.assertIsNotNone(rec)
        self.assertEqual(rec["close"], 1.5)

    def test_unknown_shape_returns_none_not_crash(self):
        self.assertIsNone(ddp.DeltaLiveFeed._parse_candle({"garbage": 1}))


if __name__ == "__main__":
    unittest.main(verbosity=2)
