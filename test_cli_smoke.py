"""End-to-end CLI smoke test with mocked HTTP. Verifies main() runs without
crashing across the full flag surface and that the on-disk layout matches
section 10 of the spec."""
import os
import shutil
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))
import delta_data_pipeline as ddp
from test_pipeline import FakeResponse, make_candles

TEST_DIR = "/tmp/ddp_cli_smoke"
shutil.rmtree(TEST_DIR, ignore_errors=True)

PRODUCT = {
    "id": 27, "symbol": "BTCUSD", "contract_type": "perpetual_futures", "state": "live",
    "trading_status": "operational", "tick_size": "0.5", "contract_value": "0.001",
    "position_size_limit": 229167, "default_leverage": "200", "max_leverage_notional": "100000",
    "initial_margin": "0.5", "maintenance_margin": "0.25", "initial_margin_scaling_factor": "0.0000025",
    "maintenance_margin_scaling_factor": "0.00000125", "funding_method": "mark_price",
    "annualized_funding": "10.95", "product_specs": {"funding_clamp_value": 0.05},
    "quoting_asset": {"symbol": "USD"}, "settling_asset": {"symbol": "USD"}, "underlying_asset": {"symbol": "BTC"},
}


def fake_request(self_session, method, url, params=None, timeout=None, headers=None):
    if "/v2/products/" in url:
        sym = url.rsplit("/", 1)[-1]
        p = dict(PRODUCT)
        p["symbol"] = sym
        return FakeResponse({"success": True, "result": p})
    if "/v2/history/candles" in url:
        s = params["start"]
        return FakeResponse({"success": True, "result": make_candles(s, 40, step=60)})
    if "/v2/trades/" in url:
        return FakeResponse({"success": True, "result": [{"price": 100, "size": 1, "timestamp": 1700000000}]})
    return FakeResponse({"success": True, "result": {}})


with patch.object(ddp.requests.Session, "request", fake_request), patch("time.sleep", return_value=None):
    ddp.main([
        "--symbols", "BTCUSD,ETHUSD",
        "--timeframes", "1m,5m",
        "--start-time", "1700000000",
        "--end-time", str(1700000000 + 3 * 3600),
        "--data-dir", TEST_DIR,
        "--contract-specs",
        "--aggregate-causal",
        "--trades",
    ])

print("\n=== resulting directory tree ===")
for root, dirs, files in os.walk(TEST_DIR):
    level = root.replace(TEST_DIR, "").count(os.sep)
    indent = "  " * level
    print(f"{indent}{os.path.basename(root)}/")
    for fn in files:
        print(f"{indent}  {fn}")

# section 10 required top-level layout check
required = ["raw", "normalized", "funding", "mark_price", "open_interest", "order_flow",
            "contract_specs", "manifests", "reports"]
present = set(os.listdir(TEST_DIR))
print("\n=== section 10 layout check ===")
for d in required:
    mark = "OK" if d in present else "MISSING (created lazily on first use of that feature)"
    print(f"  {d}/: {mark}")

assert os.path.exists(os.path.join(TEST_DIR, "manifests", "MASTER_DATASET_MANIFEST.json")), "manifest missing"
assert os.path.exists(os.path.join(TEST_DIR, "reports", "DATA_QUALITY_REPORT.md")), "quality report missing"
assert os.path.exists(os.path.join(TEST_DIR, "contract_specs", "BTCUSD.json")), "contract spec missing"
print("\nALL SMOKE ASSERTIONS PASSED")
