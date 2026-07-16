from __future__ import annotations

import json
import shutil
import socket
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

PLUGIN_PACKAGE = "integrations.hermes_plugins.binance_opportunities"
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
REMEDIATION = (
    "uv run --python 3.12 binance-market-monitor monitor --config "
    "configs/config.example.yaml --serve-api --no-futures"
)


def _candidate(
    symbol: str,
    score: str | int | float,
    *,
    detected_at: str = "2026-07-16T12:00:00+00:00",
    reasons: list[str] | None = None,
    features: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "candidate_id": f"stage1:{symbol}:1",
        "detected_at": detected_at,
        "symbol": symbol,
        "score": score,
        "features": features or {"return_5m": "0.08", "relative_volume": "3"},
        "reason_codes": reasons or ["momentum", "relative_volume"],
    }


def _alert(
    symbol: str,
    watch_score: str | int,
    *,
    detected_at: str = "2026-07-16T12:01:00+00:00",
    emitted_at: str = "2026-07-16T12:02:00+00:00",
    reasons: list[str] | None = None,
    confidence: str = "medium_high",
) -> dict[str, object]:
    return {
        "alert_id": f"alert:{symbol}:1",
        "detected_at": detected_at,
        "emitted_at": emitted_at,
        "symbol": symbol,
        "watch_score": watch_score,
        "evidence_confidence": confidence,
        "summary": f"{symbol} watch: public market activity detected; manual review suggested.",
        "reason_codes": reasons or ["multi_evidence_watch"],
        "metrics": {"spot_cvd": "1000", "open_interest_available": False},
        "data_quality": {"open_interest_available": False, "suppression": "none"},
        "invalidation": ["Alert expires if public data becomes stale."],
    }


class _JsonHandler(BaseHTTPRequestHandler):
    payloads: dict[str, Any] = {}
    statuses: dict[str, int] = {}
    raw_bodies: dict[str, bytes] = {}
    redirects: dict[str, str] = {}
    seen_methods: list[str] = []

    def do_GET(self) -> None:  # noqa: N802 - stdlib callback name.
        self.seen_methods.append("GET")
        if self.path in self.redirects:
            self.send_response(302)
            self.send_header("Location", self.redirects[self.path])
            self.end_headers()
            return
        status = self.statuses.get(self.path, 200)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if self.path in self.raw_bodies:
            self.wfile.write(self.raw_bodies[self.path])
            return
        payload = self.payloads.get(self.path, {})
        self.wfile.write(json.dumps(payload).encode("utf-8"))

    def log_message(self, format: str, *_args: object) -> None:  # noqa: A002
        return


@pytest.fixture
def monitor_api(monkeypatch: pytest.MonkeyPatch) -> Iterator[type[_JsonHandler]]:
    _JsonHandler.payloads = {
        "/candidates/current": {"candidates": []},
        "/alerts/recent": {"alerts": []},
        "/health/binance": {"status": "healthy"},
        "/metrics/summary": {"candidates": 0, "alerts": 0},
    }
    _JsonHandler.statuses = {}
    _JsonHandler.raw_bodies = {}
    _JsonHandler.redirects = {}
    _JsonHandler.seen_methods = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _JsonHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("BMM_HERMES_API_URL", f"http://127.0.0.1:{server.server_port}")
    try:
        yield _JsonHandler
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


def _call_tool(args: dict[str, object]) -> dict[str, Any]:
    from integrations.hermes_plugins.binance_opportunities.tools import (
        handle_binance_opportunities_list,
    )

    raw = handle_binance_opportunities_list(args)
    assert isinstance(raw, str)
    return json.loads(raw)


def test_register_wires_single_hermes_tool() -> None:
    plugin = pytest.importorskip(PLUGIN_PACKAGE)
    calls: list[dict[str, Any]] = []

    class FakeContext:
        def register_tool(self, **kwargs: Any) -> None:
            calls.append(kwargs)

    plugin.register(FakeContext())

    assert len(calls) == 1
    registered = calls[0]
    assert registered["name"] == "binance_opportunities_list"
    assert registered["toolset"] == "binance_opportunities"
    assert registered["schema"]["name"] == "binance_opportunities_list"
    assert callable(registered["handler"])


def test_plugin_loads_as_standalone_package_outside_repository(tmp_path: Path) -> None:
    """Hermes loads personal plugins from their own directory, not the repository package."""
    source = REPO_ROOT / "integrations/hermes_plugins/binance_opportunities"
    plugin_dir = tmp_path / "binance-opportunities"
    shutil.copytree(source, plugin_dir)
    script = """
import importlib.util
import pathlib
import sys

plugin_dir = pathlib.Path(sys.argv[1])
spec = importlib.util.spec_from_file_location(
    "standalone_binance_opportunities",
    plugin_dir / "__init__.py",
    submodule_search_locations=[str(plugin_dir)],
)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
calls = []
class FakeContext:
    def register_tool(self, **kwargs):
        calls.append(kwargs)
module.register(FakeContext())
assert calls[0]["name"] == "binance_opportunities_list"
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(plugin_dir)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_consolidates_candidate_with_most_recent_alert_and_dedupes_reason_codes(
    monitor_api: type[_JsonHandler],
) -> None:
    monitor_api.payloads["/candidates/current"] = {
        "candidates": [
            _candidate(
                "SOLUSDT",
                "82.5",
                reasons=["momentum", "relative_volume", "momentum"],
            )
        ]
    }
    monitor_api.payloads["/alerts/recent"] = {
        "alerts": [
            _alert("SOLUSDT", 70, emitted_at="2026-07-16T12:01:00+00:00"),
            _alert(
                "SOLUSDT",
                "90",
                emitted_at="2026-07-16T12:03:00+00:00",
                reasons=["relative_volume", "multi_evidence_watch"],
                confidence="high",
            ),
        ]
    }
    monitor_api.payloads["/health/binance"] = {"status": "healthy", "reason_codes": []}
    monitor_api.payloads["/metrics/summary"] = {"candidates": 1, "alerts": 2}

    response = _call_tool({})

    assert response["success"] is True
    assert response["source_api"]["base_url"].startswith("http://127.0.0.1:")
    assert response["monitor_health"] == {"status": "healthy", "reason_codes": []}
    assert response["metrics"] == {"candidates": 1, "alerts": 2}
    assert response["count"] == 1
    opportunity = response["opportunities"][0]
    assert opportunity["symbol"] == "SOLUSDT"
    assert opportunity["status"] == "confirmed_watch"
    assert opportunity["stage1_score"] == 82.5
    assert opportunity["watch_score"] == 90
    assert opportunity["evidence_confidence"] == "high"
    assert opportunity["detected_at"] == "2026-07-16T12:00:00+00:00"
    assert opportunity["emitted_at"] == "2026-07-16T12:03:00+00:00"
    assert opportunity["reason_codes"] == [
        "momentum",
        "relative_volume",
        "multi_evidence_watch",
    ]
    assert opportunity["features"] == {"return_5m": "0.08", "relative_volume": "3"}
    assert opportunity["summary"].startswith("SOLUSDT watch")
    assert opportunity["data_quality"] == {"open_interest_available": False, "suppression": "none"}
    assert opportunity["invalidation"] == ["Alert expires if public data becomes stale."]
    assert "Not financial advice" in response["disclaimer"]
    assert monitor_api.seen_methods == ["GET", "GET", "GET", "GET"]


def test_includes_recent_alert_without_current_candidate_as_confirmed_watch(
    monitor_api: type[_JsonHandler],
) -> None:
    monitor_api.payloads["/alerts/recent"] = {"alerts": [_alert("DOGEUSDT", 88)]}

    response = _call_tool({})

    assert response["count"] == 1
    assert response["opportunities"][0]["symbol"] == "DOGEUSDT"
    assert response["opportunities"][0]["status"] == "confirmed_watch"
    assert response["opportunities"][0]["stage1_score"] is None
    assert response["opportunities"][0]["watch_score"] == 88
    assert response["opportunities"][0]["features"] == {
        "spot_cvd": "1000",
        "open_interest_available": False,
    }


def test_sorts_confirmed_first_then_watch_score_stage1_score_and_symbol(
    monitor_api: type[_JsonHandler],
) -> None:
    monitor_api.payloads["/candidates/current"] = {
        "candidates": [
            _candidate("AAAUSDT", "95"),
            _candidate("CCCUSDT", "99"),
            _candidate("BBBUSDT", "50"),
            _candidate("DDDUSDT", "40"),
        ]
    }
    monitor_api.payloads["/alerts/recent"] = {
        "alerts": [
            _alert("BBBUSDT", 80),
            _alert("AAAUSDT", 80),
            _alert("ZZZUSDT", 70),
        ]
    }

    response = _call_tool({"limit": 10})

    assert [item["symbol"] for item in response["opportunities"]] == [
        "AAAUSDT",  # confirmed, same watch_score as BBB, higher stage1_score
        "BBBUSDT",
        "ZZZUSDT",  # confirmed alert-only before unconfirmed candidates
        "CCCUSDT",  # unconfirmed, higher stage1_score
        "DDDUSDT",
    ]


def test_filters_and_limit_are_applied_after_consolidation(monitor_api: type[_JsonHandler]) -> None:
    monitor_api.payloads["/candidates/current"] = {
        "candidates": [
            _candidate("AAAUSDT", "95"),
            _candidate("BBBUSDT", "90"),
            _candidate("CCCUSDT", "20"),
        ]
    }
    monitor_api.payloads["/alerts/recent"] = {
        "alerts": [_alert("AAAUSDT", 75), _alert("BBBUSDT", 55), _alert("ZZZUSDT", 90)]
    }

    response = _call_tool(
        {"limit": 2, "min_stage1_score": "80", "min_watch_score": "70", "confirmed_only": True}
    )

    assert response["count"] == 1
    assert [item["symbol"] for item in response["opportunities"]] == ["AAAUSDT"]


def test_invalid_arguments_return_json_error_without_calling_api(
    monitor_api: type[_JsonHandler],
) -> None:
    response = _call_tool({"limit": 51})

    assert response == {
        "success": False,
        "error": {
            "code": "invalid_arguments",
            "message": "limit must be an integer between 1 and 50.",
            "remediation": REMEDIATION,
        },
    }
    assert monitor_api.seen_methods == []


def test_api_unavailable_returns_json_error(monkeypatch: pytest.MonkeyPatch) -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    monkeypatch.setenv("BMM_HERMES_API_URL", f"http://127.0.0.1:{port}")

    response = _call_tool({})

    assert response["success"] is False
    assert response["error"]["code"] == "api_unavailable"
    assert response["error"]["remediation"] == REMEDIATION


def test_invalid_json_returns_json_error(monitor_api: type[_JsonHandler]) -> None:
    monitor_api.raw_bodies["/alerts/recent"] = b"not-json"

    response = _call_tool({})

    assert response["success"] is False
    assert response["error"]["code"] == "invalid_response"
    assert "valid JSON" in response["error"]["message"]


def test_oversized_response_returns_json_error(monitor_api: type[_JsonHandler]) -> None:
    monitor_api.raw_bodies["/candidates/current"] = b"{" + b'"x":"' + (b"a" * 1_100_000) + b'"}'

    response = _call_tool({})

    assert response["success"] is False
    assert response["error"]["code"] == "invalid_response"
    assert "too large" in response["error"]["message"]


def test_redirect_is_rejected_before_following_external_location(
    monitor_api: type[_JsonHandler],
) -> None:
    monitor_api.redirects["/metrics/summary"] = "https://example.invalid/metrics/summary"

    response = _call_tool({})

    assert response["success"] is False
    assert response["error"]["code"] == "invalid_response"
    assert "redirect" in response["error"]["message"].lower()


def test_non_loopback_api_url_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BMM_HERMES_API_URL", "http://binance.com:8000")

    response = _call_tool({})

    assert response == {
        "success": False,
        "error": {
            "code": "invalid_arguments",
            "message": (
                "BMM_HERMES_API_URL must use http and a loopback host "
                "(127.0.0.1, localhost, or ::1)."
            ),
            "remediation": REMEDIATION,
        },
    }


def test_tool_surface_does_not_expose_secrets_accounts_orders_or_open_interest() -> None:
    package_dir = Path("integrations/hermes_plugins/binance_opportunities")
    text = "\n".join(path.read_text() for path in package_dir.glob("*.py"))
    from integrations.hermes_plugins.binance_opportunities.schemas import (
        BINANCE_OPPORTUNITIES_LIST_SCHEMA,
    )

    schema_text = json.dumps(BINANCE_OPPORTUNITIES_LIST_SCHEMA).lower()
    combined = (text + schema_text).lower()
    forbidden = [
        "api_key",
        "secret",
        "signed",
        "account",
        "order_id",
        "listenkey",
        "proxy",
        "vpn",
    ]
    assert not any(term in combined for term in forbidden)
    assert "open interest" in schema_text
    assert "never uses" in schema_text
