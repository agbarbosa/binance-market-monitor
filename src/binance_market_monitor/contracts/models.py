from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, ClassVar, Final, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, PlainSerializer, PositiveInt

SCHEMA_VERSION: Final[Literal["1.0"]] = "1.0"
PRODUCT_NAME: Final[Literal["binance_market_monitor"]] = "binance_market_monitor"

Market: TypeAlias = Literal["spot", "usd_m_futures"]
BookState: TypeAlias = Literal["warming", "valid", "stale", "invalid", "resync_pending"]
AlertType: TypeAlias = Literal[
    "spot_led_momentum_watch",
    "breakout_retest_watch",
    "liquidity_sweep_recovery_watch",
    "futures_led_divergence_warning",
    "liquidation_cascade_warning",
    "liquidity_deterioration_warning",
    "data_quality_warning",
    "system_health_alert",
]
Severity: TypeAlias = Literal["low", "medium", "high", "critical"]
EvidenceConfidence: TypeAlias = Literal["low", "medium", "medium_high", "high"]
HealthStatus: TypeAlias = Literal["healthy", "degraded", "stale", "blocked", "unavailable"]
ReviewStatus: TypeAlias = Literal["useful", "noisy", "late", "misleading", "unreviewed"]
AggressorSide: TypeAlias = Literal["buyer_initiated", "seller_initiated", "unknown"]


def _decimal_to_string(value: Decimal) -> str:
    return str(value)


DecimalString: TypeAlias = Annotated[
    Decimal,
    PlainSerializer(_decimal_to_string, return_type=str, when_used="json"),
]
DepthLevel: TypeAlias = tuple[DecimalString, DecimalString]
MetricValue: TypeAlias = str | int | float | bool | None
MetricMap: TypeAlias = dict[str, MetricValue]
NestedMetricMap: TypeAlias = dict[str, MetricValue | MetricMap]


class ContractBase(BaseModel):
    """Base model for versioned machine-enforced contracts."""

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    schema_version: Literal["1.0"] = SCHEMA_VERSION


class NormalizedTradeEvent(ContractBase):
    """Normalized public aggregate trade event."""

    event_type: Literal["normalized_trade"] = "normalized_trade"
    event_id: str
    exchange_event_time: datetime
    exchange_transaction_time: datetime | None = None
    local_receive_time: datetime
    market: Market
    symbol: str
    trade_id: str
    price: DecimalString
    quantity: DecimalString
    notional: DecimalString
    aggressor_side: AggressorSide


class NormalizedDepthDiffEvent(ContractBase):
    """Normalized incremental depth event."""

    event_type: Literal["normalized_depth_diff"] = "normalized_depth_diff"
    event_id: str
    exchange_event_time: datetime
    local_receive_time: datetime
    market: Market
    symbol: str
    first_update_id: PositiveInt
    final_update_id: PositiveInt
    previous_final_update_id: PositiveInt | None = None
    bids: list[DepthLevel]
    asks: list[DepthLevel]


class NormalizedDepthSnapshot(ContractBase):
    """Official order-book snapshot contract."""

    event_type: Literal["normalized_depth_snapshot"] = "normalized_depth_snapshot"
    snapshot_id: str
    captured_at: datetime
    market: Market
    symbol: str
    last_update_id: PositiveInt
    bids: list[DepthLevel]
    asks: list[DepthLevel]


class LocalBookState(ContractBase):
    """Current local-book state exposed to features and health endpoints."""

    event_type: Literal["local_book_state"] = "local_book_state"
    market: Market
    symbol: str
    state: BookState
    last_update_id: PositiveInt | None
    exchange_event_time: datetime | None
    local_receive_time: datetime | None
    age_ms: NonNegativeInt | None
    best_bid: DecimalString | None
    best_ask: DecimalString | None
    spread: DecimalString | None
    depth_bands: dict[str, MetricMap] = Field(default_factory=dict)


class Stage1Candidate(ContractBase):
    """Ranked broad-scanner candidate."""

    event_type: Literal["stage1_candidate"] = "stage1_candidate"
    candidate_id: str
    detected_at: datetime
    symbol: str
    score: DecimalString
    score_version: str
    executable_liquidity_score: DecimalString
    features: MetricMap
    thresholds: MetricMap
    reason_codes: list[str]
    input_freshness_ms: dict[str, NonNegativeInt]


class Stage2Decision(ContractBase):
    """Deep-analysis decision record."""

    event_type: Literal["stage2_decision"] = "stage2_decision"
    decision_id: str
    calculated_at: datetime
    symbol: str
    candidate_id: str
    decision: Literal["watch", "suppress", "warm_up", "degraded"]
    evidence_groups: list[str]
    evidence_confidence: EvidenceConfidence
    features: MetricMap
    thresholds: MetricMap
    reason_codes: list[str]
    open_interest_available: Literal[False] = False


class AlertPayload(ContractBase):
    """Versioned low-frequency watch alert for n8n delivery."""

    product: Literal["binance_market_monitor"] = PRODUCT_NAME
    event_type: Literal["watch_alert"] = "watch_alert"
    alert_id: str
    dedupe_key: str
    detected_at: datetime
    emitted_at: datetime | None = None
    symbol: str
    market: Market
    alert_type: AlertType
    severity: Severity
    watch_score: int = Field(ge=0, le=100)
    score_version: str
    evidence_confidence: EvidenceConfidence
    summary: str
    reason_codes: list[str]
    metrics: MetricMap
    thresholds: MetricMap
    data_quality: NestedMetricMap
    invalidation: list[str]
    disclaimer: str = "Watch alert only. Not financial advice. No trade recommendation."


class ConnectorHealth(ContractBase):
    """Connector and data-quality health record."""

    event_type: Literal["connector_health"] = "connector_health"
    connector: str
    status: HealthStatus
    checked_at: datetime
    last_event_at: datetime | None = None
    last_event_age_ms: NonNegativeInt | None = None
    reconnect_count: NonNegativeInt = 0
    events_received: NonNegativeInt = 0
    events_rejected: NonNegativeInt = 0
    reason_codes: list[str] = Field(default_factory=list)


class ManualAlertReview(ContractBase):
    """Stored manual review feedback for a generated alert."""

    event_type: Literal["manual_alert_review"] = "manual_alert_review"
    alert_id: str
    review_status: ReviewStatus = "unreviewed"
    reviewed_at: datetime | None = None
    review_notes: str | None = None
    reason_categories: list[str] = Field(default_factory=list)


def contract_models() -> tuple[type[ContractBase], ...]:
    return (
        NormalizedTradeEvent,
        NormalizedDepthDiffEvent,
        NormalizedDepthSnapshot,
        LocalBookState,
        Stage1Candidate,
        Stage2Decision,
        AlertPayload,
        ConnectorHealth,
        ManualAlertReview,
    )
