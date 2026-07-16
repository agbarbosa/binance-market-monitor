from __future__ import annotations

import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType


def _load_watchdog() -> ModuleType:
    path = Path(__file__).resolve().parents[2] / "scripts" / "binance_alert_watchdog.py"
    spec = importlib.util.spec_from_file_location("binance_alert_watchdog", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _alert(alert_id: str = "alert:SOLUSDT:1") -> dict[str, object]:
    return {
        "alert_id": alert_id,
        "symbol": "SOLUSDT",
        "watch_score": 88,
        "evidence_confidence": "high",
        "summary": "SOLUSDT watch: abnormal spot-led activity.",
        "reason_codes": ["relative_volume", "multi_evidence_watch"],
        "invalidation": ["Expires if public data becomes stale."],
        "emitted_at": "2026-07-16T20:45:00+00:00",
    }


def test_first_run_bootstraps_without_replaying_existing_alerts(tmp_path: Path) -> None:
    watchdog = _load_watchdog()
    state_path = tmp_path / "state.json"

    output = watchdog.run_watchdog(
        fetcher=lambda: [_alert()],
        state_path=state_path,
        now=datetime(2026, 7, 16, 20, 46, tzinfo=UTC),
    )

    assert output == ""
    assert "alert:SOLUSDT:1" in state_path.read_text(encoding="utf-8")


def test_new_confirmed_alert_is_rendered_once_with_explanation(tmp_path: Path) -> None:
    watchdog = _load_watchdog()
    state_path = tmp_path / "state.json"
    now = datetime(2026, 7, 16, 20, 46, tzinfo=UTC)
    watchdog.run_watchdog(fetcher=lambda: [], state_path=state_path, now=now)

    first = watchdog.run_watchdog(fetcher=lambda: [_alert()], state_path=state_path, now=now)
    duplicate = watchdog.run_watchdog(fetcher=lambda: [_alert()], state_path=state_path, now=now)

    assert "Oportunidade Binance confirmada" in first
    assert "SOLUSDT" in first
    assert "88/100" in first
    assert "high" in first
    assert "relative_volume" in first
    assert "Expires if public data becomes stale." in first
    assert "não é recomendação financeira" in first
    assert duplicate == ""


def test_api_failure_is_notified_at_most_once_per_hour(tmp_path: Path) -> None:
    watchdog = _load_watchdog()
    state_path = tmp_path / "state.json"
    now = datetime(2026, 7, 16, 20, 46, tzinfo=UTC)

    def unavailable() -> list[dict[str, object]]:
        raise OSError("connection refused")

    first = watchdog.run_watchdog(fetcher=unavailable, state_path=state_path, now=now)
    repeated = watchdog.run_watchdog(
        fetcher=unavailable,
        state_path=state_path,
        now=now + timedelta(minutes=1),
    )
    hourly = watchdog.run_watchdog(
        fetcher=unavailable,
        state_path=state_path,
        now=now + timedelta(hours=1, minutes=1),
    )

    assert "Monitor Binance indisponível" in first
    assert "connection refused" in first
    assert repeated == ""
    assert "Monitor Binance indisponível" in hourly
