from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from binance_market_monitor.alerts.engine import AlertEngine, AlertEngineConfig, AlertSuppression
from binance_market_monitor.alerts.webhook import AsyncWebhookDispatcher, WebhookConfig
from binance_market_monitor.scanner.stage1 import Stage1CandidateRecord
from binance_market_monitor.scanner.stage2 import (
    FuturesContext,
    LiquidationEvent,
    Stage2Input,
    TradePrint,
    classify_aggressor_side,
    compute_cvd,
    make_stage2_decision,
)

NOW = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)


def _candidate(symbol: str = "SOLUSDT") -> Stage1CandidateRecord:
    return Stage1CandidateRecord(
        candidate_id=f"stage1:{symbol}:1",
        detected_at=NOW,
        symbol=symbol,
        score=Decimal("80"),
        score_version="stage1-v1",
        executable_liquidity_score=Decimal("100000"),
        features={"return_5m": Decimal("0.08"), "relative_volume": Decimal("3")},
        thresholds={},
        reason_codes=["momentum", "relative_volume", "trade_acceleration"],
        input_freshness_ms={"ticker": 100, "book": 100},
    )


def test_aggressor_classification_and_cvd_for_spot_and_futures() -> None:
    assert classify_aggressor_side(is_buyer_maker=True) == "seller_initiated"
    assert classify_aggressor_side(is_buyer_maker=False) == "buyer_initiated"
    trades = [
        TradePrint(NOW, Decimal("10"), Decimal("2"), is_buyer_maker=False, market="spot"),
        TradePrint(NOW, Decimal("11"), Decimal("1"), is_buyer_maker=True, market="usd_m_futures"),
    ]

    cvd = compute_cvd(trades)

    assert cvd.value == Decimal("9")
    assert cvd.method == "binance_maker_side_approximation"
    assert cvd.reason_codes == ["cvd_approximated_from_public_trade_maker_side"]


def test_futures_context_has_basis_funding_liquidations_and_never_open_interest() -> None:
    context = FuturesContext.from_public_data(
        spot_price=Decimal("100"),
        mark_price=Decimal("102"),
        funding_rate=Decimal("0.0001"),
        liquidations=[LiquidationEvent(NOW, "SELL", Decimal("102"), Decimal("5"))],
        bid_depth_notional=Decimal("50000"),
        ask_depth_notional=Decimal("40000"),
    )

    assert context.basis_bps == Decimal("200")
    assert context.funding_rate == Decimal("0.0001")
    assert context.liquidation_notional == Decimal("510")
    assert context.liquidity_notional == Decimal("40000")
    assert context.open_interest_available is False
    assert "open_interest" not in context.features


def test_stage2_requires_multi_evidence_and_degrades_missing_futures_context() -> None:
    single = make_stage2_decision(
        Stage2Input(candidate=_candidate(), spot_cvd=Decimal("1000"), futures_context=None, now=NOW)
    )
    assert single.decision == "degraded"
    assert "futures_context_missing" in single.reason_codes
    assert single.open_interest_available is False

    context = FuturesContext.from_public_data(
        spot_price=Decimal("100"),
        mark_price=Decimal("102"),
        funding_rate=Decimal("0.0001"),
        liquidations=[LiquidationEvent(NOW, "SELL", Decimal("102"), Decimal("5"))],
        bid_depth_notional=Decimal("50000"),
        ask_depth_notional=Decimal("40000"),
    )
    decision = make_stage2_decision(
        Stage2Input(
            candidate=_candidate(), spot_cvd=Decimal("1000"), futures_context=context, now=NOW
        )
    )

    assert decision.decision == "watch"
    assert set(decision.evidence_groups) >= {"spot_flow", "futures_basis", "liquidations"}
    assert "multi_evidence_watch" in decision.reason_codes


def test_alert_engine_neutral_wording_taxonomy_suppression_cooldown_dedupe_continuation() -> None:
    engine = AlertEngine(AlertEngineConfig(cooldown_seconds=60))
    context = FuturesContext.from_public_data(
        spot_price=Decimal("100"),
        mark_price=Decimal("102"),
        funding_rate=Decimal("0.0001"),
        liquidations=[LiquidationEvent(NOW, "SELL", Decimal("102"), Decimal("5"))],
        bid_depth_notional=Decimal("50000"),
        ask_depth_notional=Decimal("40000"),
    )
    decision = make_stage2_decision(
        Stage2Input(
            candidate=_candidate(), spot_cvd=Decimal("1000"), futures_context=context, now=NOW
        )
    )

    stale = engine.maybe_alert(decision, now=NOW, stale=True)
    assert isinstance(stale, AlertSuppression)
    assert stale.reason_codes == ["suppressed_stale_data"]

    alert = engine.maybe_alert(decision, now=NOW)
    assert not isinstance(alert, AlertSuppression)
    assert alert.schema_version == "1.0"
    assert alert.alert_type == "spot_led_momentum_watch"
    assert "watch" in alert.summary.lower()
    forbidden = ("buy", "sell", "long", "short", "entry", "exit", "order")
    assert not any(word in alert.summary.lower() for word in forbidden)
    assert "Not financial advice" in alert.disclaimer
    assert "continuation" not in alert.reason_codes

    duplicate = engine.maybe_alert(decision, now=NOW + timedelta(seconds=10))
    assert isinstance(duplicate, AlertSuppression)
    assert duplicate.reason_codes == ["suppressed_cooldown"]

    continuation = engine.maybe_alert(decision, now=NOW + timedelta(seconds=61))
    assert not isinstance(continuation, AlertSuppression)
    assert "continuation_watch" in continuation.reason_codes


class FakeWebhookTransport:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls: list[tuple[str, dict[str, str], dict[str, object], float]] = []

    async def post_json(
        self, url: str, headers: dict[str, str], payload: dict[str, object], timeout_seconds: float
    ) -> int:
        self.calls.append((url, headers, payload, timeout_seconds))
        if len(self.calls) <= self.failures:
            raise TimeoutError("simulated timeout")
        return 202


@pytest.mark.asyncio
async def test_webhook_retry_idempotency_failure_persistence_and_disabled_mode() -> None:
    context = FuturesContext.from_public_data(
        spot_price=Decimal("100"),
        mark_price=Decimal("102"),
        funding_rate=Decimal("0.0001"),
        liquidations=[LiquidationEvent(NOW, "SELL", Decimal("102"), Decimal("5"))],
        bid_depth_notional=Decimal("50000"),
        ask_depth_notional=Decimal("40000"),
    )
    decision = make_stage2_decision(
        Stage2Input(
            candidate=_candidate(), spot_cvd=Decimal("1000"), futures_context=context, now=NOW
        )
    )
    alert = AlertEngine().maybe_alert(decision, now=NOW)
    assert not isinstance(alert, AlertSuppression)

    disabled = AsyncWebhookDispatcher(WebhookConfig(enabled=False, url=None))
    skipped = await disabled.send(alert)
    assert skipped.status == "disabled"

    transport = FakeWebhookTransport(failures=2)
    dispatcher = AsyncWebhookDispatcher(
        WebhookConfig(
            enabled=True, url="https://n8n.example/webhook", max_retries=2, timeout_seconds=0.01
        ),
        transport=transport,
        sleep=lambda _seconds: None,
        jitter=lambda _attempt: 0,
    )
    delivered = await dispatcher.send(alert)

    assert delivered.status == "delivered"
    assert len(transport.calls) == 3
    headers = transport.calls[-1][1]
    assert headers["Idempotency-Key"] == alert.dedupe_key

    failing = AsyncWebhookDispatcher(
        WebhookConfig(enabled=True, url="https://n8n.example/webhook", max_retries=1),
        transport=FakeWebhookTransport(failures=5),
        sleep=lambda _seconds: None,
        jitter=lambda _attempt: 0,
    )
    failed = await failing.send(alert)
    assert failed.status == "failed"
    assert failing.failed_deliveries[-1].alert_id == alert.alert_id
