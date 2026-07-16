from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from binance_market_monitor.contracts.models import (
    AlertPayload,
    ConnectorHealth,
    LocalBookState,
    ManualAlertReview,
    NormalizedDepthDiffEvent,
    NormalizedDepthSnapshot,
    NormalizedTradeEvent,
    Stage1Candidate,
    Stage2Decision,
    contract_models,
)
from binance_market_monitor.contracts.schema import write_json_schemas


def test_decimal_contract_values_serialize_as_strings() -> None:
    trade = NormalizedTradeEvent(
        event_id="trade-1",
        exchange_event_time="2026-07-16T16:00:00Z",
        exchange_transaction_time="2026-07-16T16:00:00.050Z",
        local_receive_time="2026-07-16T16:00:00.100Z",
        market="spot",
        symbol="BTCUSDT",
        trade_id="123",
        price=Decimal("65432.10"),
        quantity=Decimal("0.125"),
        notional=Decimal("8179.0125"),
        aggressor_side="buyer_initiated",
    )

    payload = trade.model_dump(mode="json")

    assert payload["schema_version"] == "1.0"
    assert payload["price"] == "65432.10"
    assert payload["quantity"] == "0.125"
    assert payload["notional"] == "8179.0125"


def test_all_prd_contracts_generate_versioned_json_schema(tmp_path: Path) -> None:
    paths = write_json_schemas(tmp_path)

    expected_model_names = {model.__name__ for model in contract_models()}
    assert expected_model_names == {
        "NormalizedTradeEvent",
        "NormalizedDepthDiffEvent",
        "NormalizedDepthSnapshot",
        "LocalBookState",
        "Stage1Candidate",
        "Stage2Decision",
        "AlertPayload",
        "ConnectorHealth",
        "ManualAlertReview",
    }
    assert {path.stem.removesuffix(".schema.v1") for path in paths} == {
        "normalized_trade_event",
        "normalized_depth_diff_event",
        "normalized_depth_snapshot",
        "local_book_state",
        "stage1_candidate",
        "stage2_decision",
        "alert_payload",
        "connector_health",
        "manual_alert_review",
    }

    for path in paths:
        schema = json.loads(path.read_text(encoding="utf-8"))
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert schema["x-schema-version"] == "1.0"
        assert schema["title"] in expected_model_names
        assert "schema_version" in schema["properties"]


def test_contracts_cover_events_candidates_decisions_health_alerts_and_reviews() -> None:
    common_times = {
        "exchange_event_time": "2026-07-16T16:00:00Z",
        "local_receive_time": "2026-07-16T16:00:00.100Z",
    }
    depth_diff = NormalizedDepthDiffEvent(
        event_id="depth-diff-1",
        market="spot",
        symbol="BTCUSDT",
        first_update_id=10,
        final_update_id=12,
        previous_final_update_id=None,
        bids=[[Decimal("65431.00"), Decimal("1.2")]],
        asks=[[Decimal("65433.00"), Decimal("0.8")]],
        **common_times,
    )
    snapshot = NormalizedDepthSnapshot(
        snapshot_id="snapshot-1",
        captured_at="2026-07-16T16:00:01Z",
        market="spot",
        symbol="BTCUSDT",
        last_update_id=12,
        bids=[[Decimal("65431.00"), Decimal("1.2")]],
        asks=[[Decimal("65433.00"), Decimal("0.8")]],
    )
    book = LocalBookState(
        market="spot",
        symbol="BTCUSDT",
        state="valid",
        last_update_id=12,
        exchange_event_time="2026-07-16T16:00:00Z",
        local_receive_time="2026-07-16T16:00:00.100Z",
        age_ms=100,
        best_bid=Decimal("65431.00"),
        best_ask=Decimal("65433.00"),
        spread=Decimal("2.00"),
        depth_bands={"10bps": {"bid_notional": "100000", "ask_notional": "90000"}},
    )
    candidate = Stage1Candidate(
        candidate_id="candidate-1",
        detected_at="2026-07-16T16:00:05Z",
        symbol="BTCUSDT",
        score=Decimal("78.5"),
        score_version="mvp-1",
        executable_liquidity_score=Decimal("91.0"),
        features={"return_5m_pct": 1.2, "spot_cvd_5m": "125430.50"},
        thresholds={"relative_volume_5m_min": 2.0},
        reason_codes=["spot_relative_volume_high"],
        input_freshness_ms={"ticker": 250},
    )
    decision = Stage2Decision(
        decision_id="decision-1",
        calculated_at="2026-07-16T16:00:06Z",
        symbol="BTCUSDT",
        candidate_id="candidate-1",
        decision="watch",
        evidence_groups=["spot_flow", "liquidity"],
        evidence_confidence="medium_high",
        features={"basis_bps": 3.1, "spot_cvd_5m": "125430.50"},
        thresholds={"spread_bps_max": 4.0},
        reason_codes=["spread_acceptable"],
        open_interest_available=False,
    )
    alert = AlertPayload(
        alert_id="alert-1",
        dedupe_key="BTCUSDT:spot_led_momentum_watch:v1",
        detected_at="2026-07-16T16:00:00Z",
        emitted_at="2026-07-16T16:00:07Z",
        symbol="BTCUSDT",
        market="spot",
        alert_type="spot_led_momentum_watch",
        severity="medium",
        watch_score=78,
        score_version="mvp-1",
        evidence_confidence="medium_high",
        summary="Spot activity expanded with healthy liquidity.",
        reason_codes=["spot_relative_volume_high", "spread_acceptable"],
        metrics={"spot_cvd_5m": "125430.50", "spread_bps": 1.8},
        thresholds={"spread_bps_max": 4.0},
        data_quality={"spot_stream": "healthy", "open_interest_available": False},
        invalidation=["evidence no longer holds"],
    )
    health = ConnectorHealth(
        connector="spot_market_data_stream",
        status="healthy",
        checked_at="2026-07-16T16:00:08Z",
        last_event_at="2026-07-16T16:00:07Z",
        last_event_age_ms=1000,
        reconnect_count=0,
        events_received=100,
        events_rejected=0,
        reason_codes=[],
    )
    review = ManualAlertReview(
        alert_id="alert-1",
        review_status="unreviewed",
        reviewed_at=None,
        review_notes=None,
        reason_categories=[],
    )

    assert depth_diff.model_dump(mode="json")["bids"] == [["65431.00", "1.2"]]
    assert snapshot.model_dump(mode="json")["asks"] == [["65433.00", "0.8"]]
    assert book.model_dump(mode="json")["best_bid"] == "65431.00"
    assert candidate.model_dump(mode="json")["score"] == "78.5"
    assert decision.model_dump(mode="json")["open_interest_available"] is False
    assert alert.product == "binance_market_monitor"
    assert "Not financial advice" in alert.disclaimer
    assert health.model_dump(mode="json")["status"] == "healthy"
    assert review.model_dump(mode="json")["review_status"] == "unreviewed"
