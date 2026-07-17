from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
    min_history_span_minutes: int = 15
    window_sample_tolerance_seconds: int = 90
    max_input_age_ms: int = 60_000
    min_relative_volume: Decimal = Decimal("2")
    min_trade_acceleration: Decimal = Decimal("2")
    min_volatility_normalized_impulse: Decimal = Decimal("1")
    min_price_move_5m: Decimal = Decimal("0.03")
    range_proximity_bps: Decimal = Decimal("50")
    max_spread_bps: Decimal = Decimal("5")
    min_liquidity_notional: Decimal = Decimal("50000")
    score_version: str = "stage1-hot-v2"


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
    history_span = normalized_samples[-1].timestamp - normalized_samples[0].timestamp
    if history_span < timedelta(minutes=config.min_history_span_minutes):
        return None
    if any(
        _sample_near_cutoff(
            normalized_samples,
            normalized_samples[-1].timestamp - timedelta(minutes=minutes),
            tolerance_seconds=config.window_sample_tolerance_seconds,
        )
        is None
        for minutes in (5, 10, 15)
    ):
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
        normalized_samples,
        sorted(btc_samples, key=lambda item: item.timestamp),
        config,
    )
    reason_codes: list[str] = []
    if features["return_5m"] > 0:
        reason_codes.append("momentum")
    if features["return_5m"] >= config.min_price_move_5m:
        reason_codes.append("price_rise_5m")
    if features["return_5m"] <= -config.min_price_move_5m:
        reason_codes.append("price_fall_5m")
    if abs(features["volatility_normalized_impulse"]) >= config.min_volatility_normalized_impulse:
        reason_codes.append("normalized_impulse")
    if features["relative_volume"] >= config.min_relative_volume:
        reason_codes.append("relative_volume")
    if features["trade_count_acceleration"] >= config.min_trade_acceleration:
        reason_codes.append("trade_acceleration")
    if features["volume_expansion"] > Decimal("1"):
        reason_codes.append("volume_expansion")
    if features["btc_relative_return"] > 0:
        reason_codes.append("btc_relative_strength")
    if features["high_proximity"] == Decimal("1"):
        reason_codes.append("local_high_proximity")
    if features["low_proximity"] == Decimal("1"):
        reason_codes.append("local_low_proximity")
    if len(reason_codes) < 3:
        return None

    score = _score(features, spread_bps, executable_liquidity)
    thresholds: dict[str, Decimal | int] = {
        "min_history_points": config.min_history_points,
        "min_history_span_minutes": config.min_history_span_minutes,
        "window_sample_tolerance_seconds": config.window_sample_tolerance_seconds,
        "max_input_age_ms": config.max_input_age_ms,
        "min_relative_volume": config.min_relative_volume,
        "min_trade_acceleration": config.min_trade_acceleration,
        "min_price_move_5m": config.min_price_move_5m,
        "range_proximity_bps": config.range_proximity_bps,
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
    samples: list[MarketSample],
    btc_samples: list[MarketSample],
    config: Stage1FeatureConfig,
) -> dict[str, Decimal]:
    last = samples[-1]
    five_minute_sample = _sample_near_cutoff(
        samples,
        last.timestamp - timedelta(minutes=5),
        tolerance_seconds=config.window_sample_tolerance_seconds,
    )
    ten_minute_sample = _sample_near_cutoff(
        samples,
        last.timestamp - timedelta(minutes=10),
        tolerance_seconds=config.window_sample_tolerance_seconds,
    )
    fifteen_minute_sample = _sample_near_cutoff(
        samples,
        last.timestamp - timedelta(minutes=15),
        tolerance_seconds=config.window_sample_tolerance_seconds,
    )
    if five_minute_sample is None or ten_minute_sample is None or fifteen_minute_sample is None:
        raise ValueError("required Stage 1 windows were not prevalidated")

    return_5m = _safe_ratio(last.price - five_minute_sample.price, five_minute_sample.price)
    return_15m = _safe_ratio(last.price - fifteen_minute_sample.price, fifteen_minute_sample.price)
    quote_volume_5m_delta = max(last.volume - five_minute_sample.volume, Decimal("0"))
    trade_count_5m_delta = max(last.trade_count - five_minute_sample.trade_count, 0)
    prior_quote_volume_delta = max(
        five_minute_sample.volume - ten_minute_sample.volume, Decimal("0")
    )
    prior_trade_count_delta = max(five_minute_sample.trade_count - ten_minute_sample.trade_count, 0)
    relative_volume = _safe_ratio(quote_volume_5m_delta, prior_quote_volume_delta)
    trade_count_acceleration = _safe_ratio(
        Decimal(trade_count_5m_delta), Decimal(prior_trade_count_delta)
    )

    returns = [
        _safe_ratio(samples[index].price - samples[index - 1].price, samples[index - 1].price)
        for index in range(1, len(samples))
    ]
    volatility = (
        Decimal(str(pstdev([float(value) for value in returns])))
        if len(returns) > 1
        else Decimal("0")
    )
    normalized_impulse = return_5m / volatility if volatility > 0 else Decimal("0")
    btc_return = _window_return(
        btc_samples,
        minutes=5,
        tolerance_seconds=config.window_sample_tolerance_seconds,
    )
    recent_window = [
        sample for sample in samples if sample.timestamp >= last.timestamp - timedelta(minutes=15)
    ]
    window = recent_window or samples
    window_high = max(sample.price for sample in window)
    window_low = min(sample.price for sample in window)
    proximity = config.range_proximity_bps / Decimal("10000")
    high_distance = _safe_ratio(window_high - last.price, window_high)
    low_distance = _safe_ratio(last.price - window_low, window_low)
    high_proximity = Decimal("1") if return_5m > 0 and high_distance <= proximity else Decimal("0")
    low_proximity = Decimal("1") if return_5m < 0 and low_distance <= proximity else Decimal("0")
    price_range = _safe_ratio(window_high - window_low, window_low)
    return {
        "return_5m": return_5m,
        "return_15m": return_15m,
        "volatility_normalized_impulse": normalized_impulse,
        "relative_volume": relative_volume,
        "trade_count_acceleration": trade_count_acceleration,
        "quote_volume_5m_delta": quote_volume_5m_delta,
        "trade_count_5m_delta": Decimal(trade_count_5m_delta),
        "volume_expansion": relative_volume,
        "btc_relative_return": return_5m - btc_return,
        "high_proximity": high_proximity,
        "low_proximity": low_proximity,
        "range": price_range,
    }


def _score(features: dict[str, Decimal], spread_bps: Decimal, liquidity: Decimal) -> Decimal:
    liquidity_component = min(liquidity / Decimal("50000"), Decimal("10"))
    spread_penalty = spread_bps / Decimal("10")
    raw = (
        abs(features["volatility_normalized_impulse"]) * Decimal("10")
        + features["relative_volume"] * Decimal("5")
        + features["trade_count_acceleration"] * Decimal("5")
        + features["volume_expansion"] * Decimal("3")
        + abs(features["return_5m"]) * Decimal("100")
        + abs(features["return_15m"]) * Decimal("50")
        + (features["high_proximity"] + features["low_proximity"]) * Decimal("2")
        + liquidity_component
        - spread_penalty
    )
    return raw.quantize(Decimal("0.0001"))


def _sample_at_or_before(samples: list[MarketSample], cutoff: datetime) -> MarketSample | None:
    eligible = [sample for sample in samples if sample.timestamp <= cutoff]
    return eligible[-1] if eligible else None


def _sample_near_cutoff(
    samples: list[MarketSample],
    cutoff: datetime,
    *,
    tolerance_seconds: int,
) -> MarketSample | None:
    sample = _sample_at_or_before(samples, cutoff)
    if sample is None:
        return None
    lag = _to_utc(cutoff) - _to_utc(sample.timestamp)
    if lag > timedelta(seconds=tolerance_seconds):
        return None
    return sample


def _window_return(samples: list[MarketSample], *, minutes: int, tolerance_seconds: int) -> Decimal:
    if not samples:
        return Decimal("0")
    last = samples[-1]
    start = _sample_near_cutoff(
        samples,
        last.timestamp - timedelta(minutes=minutes),
        tolerance_seconds=tolerance_seconds,
    )
    if start is None:
        return Decimal("0")
    return _safe_ratio(last.price - start.price, start.price)


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
