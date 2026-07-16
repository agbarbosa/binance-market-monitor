from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from binance_market_monitor.scanner.candidates import CandidateManager, CandidateManagerConfig
from binance_market_monitor.scanner.stage1 import (
    MarketSample,
    Stage1FeatureConfig,
    calculate_stage1_candidate,
    rank_stage1_candidates,
)

NOW = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)


def _samples(symbol: str = "SOLUSDT") -> list[MarketSample]:
    return [
        MarketSample(NOW - timedelta(minutes=5), symbol, Decimal("100"), Decimal("10"), 10),
        MarketSample(NOW - timedelta(minutes=4), symbol, Decimal("101"), Decimal("10"), 12),
        MarketSample(NOW - timedelta(minutes=3), symbol, Decimal("102"), Decimal("10"), 13),
        MarketSample(NOW - timedelta(minutes=2), symbol, Decimal("104"), Decimal("15"), 18),
        MarketSample(NOW - timedelta(minutes=1), symbol, Decimal("108"), Decimal("20"), 24),
        MarketSample(NOW, symbol, Decimal("112"), Decimal("40"), 40),
    ]


def test_stage1_features_from_history_reason_codes_and_gates() -> None:
    candidate = calculate_stage1_candidate(
        symbol="SOLUSDT",
        samples=_samples(),
        btc_samples=_samples("BTCUSDT"),
        now=NOW,
        spread_bps=Decimal("3"),
        bid_depth_notional=Decimal("60000"),
        ask_depth_notional=Decimal("65000"),
        config=Stage1FeatureConfig(
            min_history_points=6,
            max_input_age_ms=1_000,
            min_relative_volume=Decimal("2"),
            min_trade_acceleration=Decimal("2"),
            max_spread_bps=Decimal("5"),
            min_liquidity_notional=Decimal("50000"),
        ),
    )

    assert candidate is not None
    assert candidate.symbol == "SOLUSDT"
    assert candidate.features["return_5m"] == Decimal("0.12")
    assert candidate.features["relative_volume"] == Decimal("4")
    assert candidate.features["trade_count_acceleration"] == Decimal("4")
    assert candidate.features["volatility_normalized_impulse"] > Decimal("1")
    assert candidate.features["btc_relative_return"] == Decimal("0")
    assert set(candidate.reason_codes) >= {"momentum", "relative_volume", "trade_acceleration"}


def test_stage1_suppresses_stale_warmup_spread_and_liquidity() -> None:
    config = Stage1FeatureConfig(min_history_points=7)
    assert (
        calculate_stage1_candidate(
            symbol="SOLUSDT",
            samples=_samples(),
            btc_samples=_samples("BTCUSDT"),
            now=NOW,
            spread_bps=Decimal("1"),
            bid_depth_notional=Decimal("1"),
            ask_depth_notional=Decimal("1"),
            config=config,
        )
        is None
    )

    base_kwargs = dict(
        symbol="SOLUSDT",
        samples=_samples(),
        btc_samples=_samples("BTCUSDT"),
        now=NOW + timedelta(seconds=10),
        bid_depth_notional=Decimal("60000"),
        ask_depth_notional=Decimal("60000"),
        config=Stage1FeatureConfig(max_input_age_ms=1_000),
    )
    assert calculate_stage1_candidate(spread_bps=Decimal("1"), **base_kwargs) is None

    fresh_kwargs = {**base_kwargs, "now": NOW}
    assert calculate_stage1_candidate(spread_bps=Decimal("6"), **fresh_kwargs) is None
    assert (
        calculate_stage1_candidate(
            spread_bps=Decimal("1"),
            **{**fresh_kwargs, "bid_depth_notional": Decimal("1")},
        )
        is None
    )


def test_stage1_ranking_tie_break_is_score_liquidity_symbol_timestamp() -> None:
    first = calculate_stage1_candidate(
        symbol="AAAUSDT",
        samples=_samples("AAAUSDT"),
        btc_samples=_samples("BTCUSDT"),
        now=NOW + timedelta(seconds=2),
        spread_bps=Decimal("1"),
        bid_depth_notional=Decimal("70000"),
        ask_depth_notional=Decimal("70000"),
    )
    second = calculate_stage1_candidate(
        symbol="BBBUSDT",
        samples=_samples("BBBUSDT"),
        btc_samples=_samples("BTCUSDT"),
        now=NOW + timedelta(seconds=1),
        spread_bps=Decimal("1"),
        bid_depth_notional=Decimal("80000"),
        ask_depth_notional=Decimal("80000"),
    )
    third = calculate_stage1_candidate(
        symbol="AAAUSDT",
        samples=_samples("AAAUSDT"),
        btc_samples=_samples("BTCUSDT"),
        now=NOW,
        spread_bps=Decimal("1"),
        bid_depth_notional=Decimal("70000"),
        ask_depth_notional=Decimal("70000"),
    )

    ranked = rank_stage1_candidates([first, second, third])  # type: ignore[list-item]

    assert [item.symbol for item in ranked] == ["BBBUSDT", "AAAUSDT", "AAAUSDT"]
    assert ranked[1].detected_at == NOW


def test_candidate_manager_permanent_dynamic_hysteresis_residence_and_warmup() -> None:
    manager = CandidateManager(
        CandidateManagerConfig(
            permanent_symbols=("BTCUSDT", "ETHUSDT"),
            max_dynamic_candidates=2,
            min_residence_seconds=60,
            warmup_seconds=30,
            hysteresis_score_margin=Decimal("5"),
        ),
        started_at=NOW,
    )

    assert manager.update([], now=NOW + timedelta(seconds=10)).warmup is True
    assert manager.current_symbols == ("BTCUSDT", "ETHUSDT")

    candidates = [
        calculate_stage1_candidate(
            symbol=symbol,
            samples=_samples(symbol),
            btc_samples=_samples("BTCUSDT"),
            now=NOW + timedelta(seconds=31 + index),
            spread_bps=Decimal("1"),
            bid_depth_notional=Decimal(str(liquidity)),
            ask_depth_notional=Decimal(str(liquidity)),
        )
        for index, (symbol, liquidity) in enumerate(
            [("SOLUSDT", 90000), ("XRPUSDT", 80000), ("ADAUSDT", 70000)]
        )
    ]
    selection = manager.update(
        [candidate for candidate in candidates if candidate is not None],
        now=NOW + timedelta(seconds=31),
    )
    assert selection.warmup is False
    assert manager.current_symbols == ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")

    replacement = calculate_stage1_candidate(
        symbol="DOGEUSDT",
        samples=_samples("DOGEUSDT"),
        btc_samples=_samples("BTCUSDT"),
        now=NOW + timedelta(seconds=40),
        spread_bps=Decimal("1"),
        bid_depth_notional=Decimal("500000"),
        ask_depth_notional=Decimal("500000"),
    )
    manager.update(
        [replacement] if replacement is not None else [], now=NOW + timedelta(seconds=40)
    )
    assert "DOGEUSDT" not in manager.current_symbols

    manager.update(
        [replacement] if replacement is not None else [], now=NOW + timedelta(seconds=100)
    )
    assert "DOGEUSDT" in manager.current_symbols
    assert len(manager.dynamic_symbols) == 2
