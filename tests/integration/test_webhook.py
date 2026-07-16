from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from binance_market_monitor.alerts.engine import AlertEngine, AlertSuppression
from binance_market_monitor.alerts.webhook import AsyncWebhookDispatcher, WebhookConfig
from binance_market_monitor.scanner.stage1 import Stage1CandidateRecord
from binance_market_monitor.scanner.stage2 import (
    FuturesContext,
    LiquidationEvent,
    Stage2Input,
    make_stage2_decision,
)

NOW = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)


def _candidate() -> Stage1CandidateRecord:
    return Stage1CandidateRecord(
        candidate_id="stage1:SOLUSDT:1",
        detected_at=NOW,
        symbol="SOLUSDT",
        score=Decimal("80"),
        score_version="stage1-v1",
        executable_liquidity_score=Decimal("100000"),
        features={"return_5m": Decimal("0.08"), "relative_volume": Decimal("3")},
        thresholds={},
        reason_codes=["momentum", "relative_volume", "trade_acceleration"],
        input_freshness_ms={"ticker": 100, "book": 100},
    )


class FakeWebhookTransport:
    def __init__(self) -> None:
        self.calls = 0

    async def post_json(
        self, url: str, headers: dict[str, str], payload: dict[str, object], timeout_seconds: float
    ) -> int:
        self.calls += 1
        return 200


@pytest.mark.asyncio
async def test_webhook_async_send_does_not_require_real_network() -> None:
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

    transport = FakeWebhookTransport()
    dispatcher = AsyncWebhookDispatcher(
        WebhookConfig(enabled=True, url="https://n8n.example/webhook"),
        transport=transport,
        sleep=lambda _seconds: None,
    )

    result = await dispatcher.send(alert)

    assert result.status == "delivered"
    assert transport.calls == 1
