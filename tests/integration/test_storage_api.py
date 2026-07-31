from __future__ import annotations

import json
from datetime import UTC, datetime

from fastapi.testclient import TestClient
from typer.testing import CliRunner

from binance_market_monitor.api.server import ApiState, create_app
from binance_market_monitor.app import build_app_state
from binance_market_monitor.cli import cli
from binance_market_monitor.orderbook.book import DepthSnapshot, LocalOrderBook
from binance_market_monitor.storage.parquet import ParquetEventStore

NOW = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)


def test_parquet_zstd_partitioned_rows_and_duckdb_queryable(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = ParquetEventStore(tmp_path)
    store.write_rows(
        "alerts",
        [
            {
                "event_time": NOW,
                "symbol": "SOLUSDT",
                "alert_id": "a1",
                "score": 88,
                "payload": json.dumps({"reason_codes": ["multi_evidence_watch"]}),
            }
        ],
    )

    files = list(tmp_path.glob("alerts/event_date=2026-07-16/symbol=SOLUSDT/*.parquet"))
    assert files
    metadata = store.file_metadata(files[0])
    assert metadata.compression == "ZSTD"

    rows = store.query("SELECT symbol, alert_id, score FROM alerts ORDER BY alert_id")
    assert rows == [{"symbol": "SOLUSDT", "alert_id": "a1", "score": 88}]


def test_fastapi_required_endpoints_localhost_defaults_and_no_secrets(tmp_path) -> None:  # type: ignore[no-untyped-def]
    book = LocalOrderBook(market="spot", symbol="SOLUSDT")
    book.apply_snapshot(DepthSnapshot(last_update_id=1, bids=(("10", "1"),), asks=(("11", "1"),)))
    state = ApiState(
        bind_host="127.0.0.1",
        storage_path=tmp_path,
        books={("spot", "SOLUSDT"): book},
        candidates=[{"symbol": "SOLUSDT", "score": "90"}],
        alerts=[{"alert_id": "a1", "symbol": "SOLUSDT"}],
        webhook_url="https://secret.example/webhook",
    )
    client = TestClient(create_app(state))

    for path in [
        "/health/live",
        "/health/ready",
        "/health/binance",
        "/metrics/summary",
        "/candidates/current",
        "/books/spot/SOLUSDT",
        "/alerts/recent",
    ]:
        response = client.get(path)
        assert response.status_code == 200
        assert "secret.example" not in response.text

    assert client.get("/health/ready").json()["api"]["bind_host"] == "127.0.0.1"


def test_cli_schema_replay_and_app_state_are_executable(tmp_path) -> None:  # type: ignore[no-untyped-def]
    events = tmp_path / "events.jsonl"
    events.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "type": "snapshot",
                        "market": "spot",
                        "symbol": "SOLUSDT",
                        "last_update_id": 10,
                        "bids": [["10", "1"]],
                        "asks": [["11", "1"]],
                    }
                ),
                json.dumps(
                    {
                        "type": "diff",
                        "market": "spot",
                        "symbol": "SOLUSDT",
                        "first_update_id": 11,
                        "final_update_id": 11,
                        "bids": [["10.5", "1"]],
                        "asks": [],
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    runner = CliRunner()
    schema_result = runner.invoke(cli, ["schema", "--output", str(tmp_path / "schemas")])
    assert schema_result.exit_code == 0
    replay_result = runner.invoke(cli, ["replay", str(events)])
    assert replay_result.exit_code == 0
    assert "processed=2" in replay_result.output

    app_state = build_app_state(storage_path=tmp_path)
    assert app_state.bind_host == "127.0.0.1"
