from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal

from binance_market_monitor.scanner.stage2 import Stage2DecisionRecord

AlertType = Literal[
    "spot_led_momentum_watch",
    "breakout_retest_watch",
    "liquidity_sweep_recovery_watch",
    "futures_led_divergence_warning",
    "liquidation_cascade_warning",
    "liquidity_deterioration_warning",
    "data_quality_warning",
]
Severity = Literal["low", "medium", "high", "critical"]


@dataclass(frozen=True, slots=True)
class AlertPayloadRecord:
    schema_version: str
    product: str
    event_type: str
    alert_id: str
    dedupe_key: str
    detected_at: datetime
    emitted_at: datetime
    symbol: str
    market: str
    alert_type: AlertType
    severity: Severity
    watch_score: int
    score_version: str
    evidence_confidence: str
    summary: str
    reason_codes: list[str]
    metrics: dict[str, Decimal | bool | None]
    thresholds: dict[str, Decimal | int]
    data_quality: dict[str, object]
    invalidation: list[str]
    disclaimer: str = "Watch alert only. Not financial advice. No trade recommendation."

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "product": self.product,
            "event_type": self.event_type,
            "alert_id": self.alert_id,
            "dedupe_key": self.dedupe_key,
            "detected_at": self.detected_at.isoformat(),
            "emitted_at": self.emitted_at.isoformat(),
            "symbol": self.symbol,
            "market": self.market,
            "alert_type": self.alert_type,
            "severity": self.severity,
            "watch_score": self.watch_score,
            "score_version": self.score_version,
            "evidence_confidence": self.evidence_confidence,
            "summary": self.summary,
            "reason_codes": self.reason_codes,
            "metrics": {
                key: str(value) if isinstance(value, Decimal) else value
                for key, value in self.metrics.items()
            },
            "thresholds": {
                key: str(value) if isinstance(value, Decimal) else value
                for key, value in self.thresholds.items()
            },
            "data_quality": self.data_quality,
            "invalidation": self.invalidation,
            "disclaimer": self.disclaimer,
        }


@dataclass(frozen=True, slots=True)
class AlertSuppression:
    reason_codes: list[str]


@dataclass(frozen=True, slots=True)
class AlertEngineConfig:
    cooldown_seconds: int = 900
    score_version: str = "alerts-v1"


@dataclass(slots=True)
class AlertEngine:
    config: AlertEngineConfig = field(default_factory=AlertEngineConfig)
    _last_emitted_at: dict[str, datetime] = field(default_factory=dict, init=False)
    _emission_counts: dict[str, int] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        pass

    def maybe_alert(
        self,
        decision: Stage2DecisionRecord,
        *,
        now: datetime,
        stale: bool = False,
        invalid_book: bool = False,
        warmup: bool = False,
        storage_available: bool = True,
    ) -> AlertPayloadRecord | AlertSuppression:
        suppressions = _suppression_reasons(stale, invalid_book, warmup, storage_available)
        if suppressions:
            return AlertSuppression(suppressions)
        if decision.decision != "watch":
            return AlertSuppression([f"suppressed_{decision.decision}"])
        dedupe_key = f"{decision.symbol}:{decision.reason_codes[-1]}"
        last = self._last_emitted_at.get(dedupe_key)
        if last is not None and now - last < timedelta(seconds=self.config.cooldown_seconds):
            return AlertSuppression(["suppressed_cooldown"])
        emitted_before = self._emission_counts.get(dedupe_key, 0)
        reason_codes = list(decision.reason_codes)
        if emitted_before:
            reason_codes.append("continuation_watch")
        self._last_emitted_at[dedupe_key] = now
        self._emission_counts[dedupe_key] = emitted_before + 1
        return AlertPayloadRecord(
            schema_version="1.0",
            product="binance_market_monitor",
            event_type="watch_alert",
            alert_id=f"alert:{decision.symbol}:{int(now.timestamp() * 1000)}:{emitted_before + 1}",
            dedupe_key=dedupe_key,
            detected_at=decision.calculated_at,
            emitted_at=now,
            symbol=decision.symbol,
            market="spot",
            alert_type="spot_led_momentum_watch",
            severity="medium",
            watch_score=_watch_score(decision),
            score_version=self.config.score_version,
            evidence_confidence=decision.evidence_confidence,
            summary=(
                f"{decision.symbol} watch: multi-factor public market activity detected; "
                "manual review suggested."
            ),
            reason_codes=reason_codes,
            metrics=decision.features,
            thresholds=decision.thresholds,
            data_quality={"open_interest_available": False, "suppression": "none"},
            invalidation=[
                "Alert expires if public data becomes stale or local book requires resync."
            ],
        )


def _suppression_reasons(
    stale: bool, invalid_book: bool, warmup: bool, storage_available: bool
) -> list[str]:
    if stale:
        return ["suppressed_stale_data"]
    if invalid_book:
        return ["suppressed_invalid_book"]
    if warmup:
        return ["suppressed_warmup"]
    if not storage_available:
        return ["suppressed_storage_unavailable"]
    return []


def _watch_score(decision: Stage2DecisionRecord) -> int:
    base = 45 + 10 * len(decision.evidence_groups)
    if decision.evidence_confidence in {"medium_high", "high"}:
        base += 15
    return min(100, base)
