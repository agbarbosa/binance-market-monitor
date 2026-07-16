from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from statistics import pstdev


@dataclass(frozen=True, slots=True)
class MarketSample:
    timestamp: datetime
    symbol: str
    price: Decimal
    volume: Decimal
    trade_count: int


@dataclass(frozen=True, slots=True)
class Stage1FeatureConfig:
    min_history_points: int = 3
    max_input_age_ms: int = 60_000
    min_relative_volume: Decimal = Decimal("2")
    min_trade_acceleration: Decimal = Decimal("2")
    min_volatility_normalized_impulse: Decimal = Decimal("1")
    max_spread_bps: Decimal = Decimal("5")
    min_liquidity_notional: Decimal = Decimal("50000")
    score_version: str = "stage1-v1"


@dataclass(frozen=True, slots=True)
class Stage1CandidateRecord:
    candidate_id: str
    detected_at: datetime
    symbol: str
    score: Decimal
    score_version: str
    executable_liquidity_score: Decimal
    features: dict[str, Decimal]
    thresholds: dict[str, Decimal | int]
    reason_codes: list[str]
    input_freshness_ms: dict[str, int]


@dataclass(frozen=True, slots=True)
class Stage1Suppression:
    symbol: str
    reason_codes: tuple[str, ...]


def calculate_stage1_candidate(
    *,
    symbol: str,
    samples: list[MarketSample],
    btc_samples: list[MarketSample],
    now: datetime,
    spread_bps: Decimal,
    bid_depth_notional: Decimal,
    ask_depth_notional: Decimal,
    config: Stage1FeatureConfig | None = None,
) -> Stage1CandidateRecord | None:
    config = Stage1FeatureConfig() if config is None else config
    normalized_samples = sorted(samples, key=lambda item: item.timestamp)
    if len(normalized_samples) < config.min_history_points:
        return None
    freshness = _age_ms(normalized_samples[-1].timestamp, now)
    if freshness > config.max_input_age_ms:
        return None
    if spread_bps > config.max_spread_bps:
        return None
    executable_liquidity = min(bid_depth_notional, ask_depth_notional)
    if executable_liquidity < config.min_liquidity_notional:
        return None

    features = _calculate_features(
        normalized_samples, sorted(btc_samples, key=lambda item: item.timestamp)
    )
    reason_codes: list[str] = []
    if features["return_5m"] > 0:
        reason_codes.append("momentum")
    if features["volatility_normalized_impulse"] >= config.min_volatility_normalized_impulse:
        reason_codes.append("normalized_impulse")
    if features["relative_volume"] >= config.min_relative_volume:
        reason_codes.append("relative_volume")
    if features["trade_count_acceleration"] >= config.min_trade_acceleration:
        reason_codes.append("trade_acceleration")
    if features["volume_expansion"] > Decimal("1"):
        reason_codes.append("volume_expansion")
    if features["btc_relative_return"] > 0:
        reason_codes.append("btc_relative_strength")
    if len(reason_codes) < 3:
        return None

    score = _score(features, spread_bps, executable_liquidity)
    thresholds: dict[str, Decimal | int] = {
        "min_history_points": config.min_history_points,
        "max_input_age_ms": config.max_input_age_ms,
        "min_relative_volume": config.min_relative_volume,
        "min_trade_acceleration": config.min_trade_acceleration,
        "max_spread_bps": config.max_spread_bps,
        "min_liquidity_notional": config.min_liquidity_notional,
    }
    return Stage1CandidateRecord(
        candidate_id=f"stage1:{symbol.upper()}:{int(_to_utc(now).timestamp() * 1000)}",
        detected_at=_to_utc(now),
        symbol=symbol.upper(),
        score=score,
        score_version=config.score_version,
        executable_liquidity_score=executable_liquidity,
        features={
            **features,
            "spread_bps": spread_bps,
            "executable_liquidity": executable_liquidity,
        },
        thresholds=thresholds,
        reason_codes=reason_codes,
        input_freshness_ms={"ticker": freshness, "book": 0},
    )


def rank_stage1_candidates(
    candidates: list[Stage1CandidateRecord],
) -> list[Stage1CandidateRecord]:
    return sorted(
        candidates,
        key=lambda item: (
            -item.score,
            -item.executable_liquidity_score,
            item.symbol,
            item.detected_at,
        ),
    )


def _calculate_features(
    samples: list[MarketSample], btc_samples: list[MarketSample]
) -> dict[str, Decimal]:
    first = samples[0]
    last = samples[-1]
    price_return = _safe_ratio(last.price - first.price, first.price)
    volume_baseline = first.volume
    trade_baseline = Decimal(first.trade_count)
    relative_volume = _safe_ratio(last.volume, volume_baseline)
    trade_count_acceleration = _safe_ratio(Decimal(last.trade_count), trade_baseline)
    returns = [
        _safe_ratio(samples[index].price - samples[index - 1].price, samples[index - 1].price)
        for index in range(1, len(samples))
    ]
    volatility = (
        Decimal(str(pstdev([float(value) for value in returns])))
        if len(returns) > 1
        else Decimal("0")
    )
    normalized_impulse = price_return / volatility if volatility > 0 else Decimal("0")
    recent_volume = _mean([sample.volume for sample in samples[-2:]])
    old_volume = _mean([sample.volume for sample in samples[:2]])
    btc_return = (
        _safe_ratio(btc_samples[-1].price - btc_samples[0].price, btc_samples[0].price)
        if btc_samples
        else Decimal("0")
    )
    price_range = _safe_ratio(
        max(sample.price for sample in samples) - min(sample.price for sample in samples),
        first.price,
    )
    return {
        "return_5m": price_return,
        "volatility_normalized_impulse": normalized_impulse,
        "relative_volume": relative_volume,
        "trade_count_acceleration": trade_count_acceleration,
        "volume_expansion": _safe_ratio(recent_volume, old_volume),
        "btc_relative_return": price_return - btc_return,
        "range": price_range,
    }


def _score(features: dict[str, Decimal], spread_bps: Decimal, liquidity: Decimal) -> Decimal:
    liquidity_component = min(liquidity / Decimal("50000"), Decimal("10"))
    spread_penalty = spread_bps / Decimal("10")
    raw = (
        features["volatility_normalized_impulse"] * Decimal("10")
        + features["relative_volume"] * Decimal("5")
        + features["trade_count_acceleration"] * Decimal("5")
        + features["volume_expansion"] * Decimal("3")
        + features["range"] * Decimal("100")
        + liquidity_component
        - spread_penalty
    )
    return raw.quantize(Decimal("0.0001"))


def _mean(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _safe_ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return numerator / denominator


def _age_ms(timestamp: datetime, now: datetime) -> int:
    return max(0, int((_to_utc(now) - _to_utc(timestamp)).total_seconds() * 1000))


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
