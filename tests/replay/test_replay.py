from __future__ import annotations

import json
from pathlib import Path

from binance_market_monitor.replay.runner import ReplayRunner


def _write_jsonl(path: Path, rows: list[object]) -> None:
    content = "\n".join(
        json.dumps(row) if isinstance(row, dict) else str(row) for row in rows
    )
    path.write_text(content, encoding="utf-8")


def test_replay_is_deterministic_for_corrupt_gap_and_duplicate_events(tmp_path) -> None:  # type: ignore[no-untyped-def]
    events = tmp_path / "events.jsonl"
    rows: list[object] = [
        {
            "type": "snapshot",
            "market": "spot",
            "symbol": "SOLUSDT",
            "last_update_id": 10,
            "bids": [["10", "1"]],
            "asks": [["11", "1"]],
        },
        {
            "type": "diff",
            "market": "spot",
            "symbol": "SOLUSDT",
            "first_update_id": 11,
            "final_update_id": 11,
            "bids": [["10.5", "1"]],
            "asks": [],
        },
        {
            "type": "diff",
            "market": "spot",
            "symbol": "SOLUSDT",
            "first_update_id": 11,
            "final_update_id": 11,
            "bids": [["10.5", "1"]],
            "asks": [],
        },
        "{not-json",
        {
            "type": "diff",
            "market": "spot",
            "symbol": "SOLUSDT",
            "first_update_id": 13,
            "final_update_id": 13,
            "bids": [],
            "asks": [],
        },
    ]
    _write_jsonl(events, rows)

    first = ReplayRunner().run(events)
    second = ReplayRunner().run(events)

    assert first.model_dump(mode="json") == second.model_dump(mode="json")
    assert first.processed_events == 4
    assert first.duplicate_events == 1
    assert first.corrupt_events == 1
    assert first.gap_events == 1
    assert first.books["spot:SOLUSDT"]["state"] == "resync_pending"
    assert first.candidates == []
    assert first.alerts == []
