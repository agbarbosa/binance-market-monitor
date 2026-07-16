from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "check_connectivity", REPO_ROOT / "scripts" / "check_connectivity.py"
)
assert SPEC is not None and SPEC.loader is not None
connectivity = importlib.util.module_from_spec(SPEC)
sys.modules["check_connectivity"] = connectivity
SPEC.loader.exec_module(connectivity)


def test_connectivity_without_live_remains_offline() -> None:
    completed = subprocess.run(
        [sys.executable, "scripts/check_connectivity.py"],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )

    payload = json.loads(completed.stdout)
    assert payload == {"status": "skipped", "reason": "pass --live to opt in"}


def test_connectivity_live_plan_contains_only_approved_public_endpoints() -> None:
    checks = connectivity.build_live_checks(symbol="BTCUSDT", timeout=1.0)

    assert {check.name for check in checks} == {
        "spot_exchange_info",
        "spot_depth",
        "spot_ws_message",
        "futures_market_ws_message",
        "futures_public_ws_message",
        "futures_ws_api_depth_snapshot",
    }
    urls = [check.url for check in checks]
    assert "https://data-api.binance.vision/api/v3/exchangeInfo?symbol=BTCUSDT" in urls
    assert "https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=100" in urls
    assert any(url.startswith("wss://data-stream.binance.vision") for url in urls)
    assert any(url.startswith("wss://fstream.binance.com/market/") for url in urls)
    assert any(url.startswith("wss://fstream.binance.com/public/") for url in urls)
    assert "wss://ws-fapi.binance.com/ws-fapi/v1" in urls
    forbidden = ("api_key", "signature", "listenKey", "order", "position", "openInterest")
    assert not any(term.lower() in url.lower() for url in urls for term in forbidden)


@pytest.mark.asyncio
async def test_connectivity_live_runner_reports_degraded_checks_without_raising() -> None:
    checks = [
        connectivity.LiveCheck("ok", "rest", "https://data-api.binance.vision/api/v3/ping"),
        connectivity.LiveCheck("bad", "rest", "https://data-api.binance.vision/api/v3/depth"),
    ]

    async def runner(check: connectivity.LiveCheck, timeout: float) -> connectivity.CheckResult:
        if check.name == "bad":
            raise TimeoutError("bounded timeout")
        return connectivity.CheckResult(check.name, check.kind, check.url, "ok", latency_ms=1)

    result = await connectivity.run_live_checks(checks, timeout=1.0, runner=runner)

    assert result["status"] == "degraded"
    assert [item["status"] for item in result["checks"]] == ["ok", "degraded"]
    assert result["checks"][1]["error"] == "bounded timeout"
