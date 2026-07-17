from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Literal, Protocol

AggressorSide = Literal["buyer_initiated", "seller_initiated", "unknown"]
Market = Literal["spot", "usd_m_futures"]


@dataclass(frozen=True, slots=True)
class TradePrint:
    timestamp: datetime
    price: Decimal
    quantity: Decimal
    is_buyer_maker: bool | None
    market: Market


@dataclass(frozen=True, slots=True)
class CvdResult:
    value: Decimal
    method: str = "binance_maker_side_approximation"
    reason_codes: list[str] = field(
        default_factory=lambda: ["cvd_approximated_from_public_trade_maker_side"]
    )


@dataclass(frozen=True, slots=True)
class LiquidationEvent:
    timestamp: datetime
    side: Literal["BUY", "SELL"]
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class FuturesContext:
    basis_bps: Decimal
    funding_rate: Decimal | None
    liquidation_notional: Decimal
    liquidity_notional: Decimal
    open_interest_available: Literal[False]
    features: dict[str, Decimal | None]
    degraded: bool = False

    @classmethod
    def from_public_data(
        cls,
        *,
        spot_price: Decimal,
        mark_price: Decimal,
        funding_rate: Decimal | None,
        liquidations: list[LiquidationEvent],
        bid_depth_notional: Decimal,
        ask_depth_notional: Decimal,
    ) -> FuturesContext:
        basis_bps = (
            Decimal("0")
            if spot_price == 0
            else (mark_price - spot_price) / spot_price * Decimal("10000")
        )
        liquidation_notional = sum(
            (event.price * event.quantity for event in liquidations), Decimal("0")
        )
        liquidity_notional = min(bid_depth_notional, ask_depth_notional)
        return cls(
            basis_bps=basis_bps,
            funding_rate=funding_rate,
            liquidation_notional=liquidation_notional,
            liquidity_notional=liquidity_notional,
            open_interest_available=False,
            features={
                "basis_bps": basis_bps,
                "funding_rate": funding_rate,
                "liquidation_notional": liquidation_notional,
                "liquidity_notional": liquidity_notional,
            },
        )


class Stage2Candidate(Protocol):
    @property
    def candidate_id(self) -> str: ...

    @property
    def symbol(self) -> str: ...

    @property
    def executable_liquidity_score(self) -> Decimal: ...

    @property
    def features(self) -> dict[str, Decimal]: ...

    @property
    def reason_codes(self) -> list[str]: ...

    @property
    def thresholds(self) -> dict[str, Decimal | int]: ...


@dataclass(frozen=True, slots=True)
class Stage2Input:
    candidate: Stage2Candidate
    spot_cvd: Decimal
    futures_context: FuturesContext | None
    now: datetime
    min_evidence_groups: int = 2
    min_depth_notional: Decimal = Decimal("50000")


@dataclass(frozen=True, slots=True)
class Stage2DecisionRecord:
    decision_id: str
    calculated_at: datetime
    symbol: str
    candidate_id: str
    decision: Literal["watch", "suppress", "warm_up", "degraded"]
    evidence_groups: list[str]
    evidence_confidence: Literal["low", "medium", "medium_high", "high"]
    features: dict[str, Decimal | bool | None]
    thresholds: dict[str, Decimal | int]
    reason_codes: list[str]
    open_interest_available: Literal[False] = False


def classify_aggressor_side(is_buyer_maker: bool | None) -> AggressorSide:
    if is_buyer_maker is None:
        return "unknown"
    return "seller_initiated" if is_buyer_maker else "buyer_initiated"


def compute_cvd(trades: list[TradePrint]) -> CvdResult:
    total = Decimal("0")
    for trade in trades:
        notional = trade.price * trade.quantity
        side = classify_aggressor_side(trade.is_buyer_maker)
        if side == "buyer_initiated":
            total += notional
        elif side == "seller_initiated":
            total -= notional
    return CvdResult(value=total)


def make_stage2_decision(input_data: Stage2Input) -> Stage2DecisionRecord:
    candidate = input_data.candidate
    candidate_id = candidate.candidate_id
    symbol = candidate.symbol
    evidence_groups: list[str] = []
    reason_codes: list[str] = []
    return_5m = candidate.features.get("return_5m", Decimal("0"))
    features: dict[str, Decimal | bool | None] = {
        "spot_cvd": input_data.spot_cvd,
        "stage1_return_5m": return_5m,
        "stage1_relative_volume": candidate.features.get("relative_volume"),
        "stage1_executable_liquidity": candidate.executable_liquidity_score,
        "open_interest_available": False,
    }
    thresholds: dict[str, Decimal | int] = {
        "min_evidence_groups": input_data.min_evidence_groups,
        "min_basis_bps": Decimal("50"),
        "min_liquidation_notional": Decimal("100"),
        "min_depth_notional": input_data.min_depth_notional,
    }

    movement_reasons = {
        "momentum",
        "normalized_impulse",
        "price_rise_5m",
        "price_fall_5m",
    }
    activity_reasons = {"relative_volume", "trade_acceleration", "volume_expansion"}
    if movement_reasons.intersection(candidate.reason_codes) and activity_reasons.intersection(
        candidate.reason_codes
    ):
        evidence_groups.append("stage1_market_anomaly")
        reason_codes.append("stage1_hot_anomaly")

    flow_aligned = (return_5m > 0 and input_data.spot_cvd > 0) or (
        return_5m < 0 and input_data.spot_cvd < 0
    )
    if flow_aligned:
        evidence_groups.append("spot_flow")
        reason_codes.append("spot_cvd_aligned")
    elif input_data.spot_cvd != 0:
        reason_codes.append("spot_cvd_divergent")

    if {"local_high_proximity", "local_low_proximity"}.intersection(candidate.reason_codes):
        evidence_groups.append("structure")
        reason_codes.append("local_range_boundary_confirmed")

    spread_bps = candidate.features.get("spread_bps")
    if (
        spread_bps is not None
        and candidate.executable_liquidity_score >= input_data.min_depth_notional
        and spread_bps <= candidate.thresholds.get("max_spread_bps", spread_bps)
    ):
        evidence_groups.append("liquidity")
        reason_codes.append("spot_liquidity_confirmed")

    if input_data.futures_context is None:
        reason_codes.append("futures_context_missing")
    else:
        context = input_data.futures_context
        features.update(context.features)
        if abs(context.basis_bps) >= thresholds["min_basis_bps"]:
            evidence_groups.append("futures_basis")
            reason_codes.append("basis_expansion")
        if context.funding_rate is not None:
            reason_codes.append("funding_observed")
        if context.liquidation_notional >= thresholds["min_liquidation_notional"]:
            evidence_groups.append("liquidations")
            reason_codes.append("liquidation_cluster")
        if context.liquidity_notional < input_data.min_depth_notional:
            evidence_groups.append("futures_liquidity")
            reason_codes.append("liquidity_thinning")

    evidence_groups = list(dict.fromkeys(evidence_groups))
    spot_only_requirements = {"stage1_market_anomaly", "spot_flow"}
    spot_only_confirmed = (
        spot_only_requirements.issubset(evidence_groups)
        and len(evidence_groups) >= input_data.min_evidence_groups
    )
    multi_evidence_confirmed = (
        spot_only_confirmed
        if input_data.futures_context is None
        else len(evidence_groups) >= input_data.min_evidence_groups
    )
    if multi_evidence_confirmed:
        decision: Literal["watch", "suppress", "warm_up", "degraded"] = "watch"
        if input_data.futures_context is None:
            confidence: Literal["low", "medium", "medium_high", "high"] = "medium"
        elif len(evidence_groups) >= 4:
            confidence = "high"
        else:
            confidence = "medium_high"
        reason_codes.append("multi_evidence_watch")
    elif input_data.futures_context is None:
        decision = "degraded"
        confidence = "low"
        reason_codes.append("insufficient_spot_evidence")
    else:
        decision = "suppress"
        confidence = "low"
        reason_codes.append("suppressed_single_indicator")

    return Stage2DecisionRecord(
        decision_id=f"stage2:{symbol}:{int(input_data.now.timestamp() * 1000)}",
        calculated_at=input_data.now,
        symbol=symbol,
        candidate_id=candidate_id,
        decision=decision,
        evidence_groups=evidence_groups,
        evidence_confidence=confidence,
        features=features,
        thresholds=thresholds,
        reason_codes=list(dict.fromkeys(reason_codes)),
        open_interest_available=False,
    )
