from __future__ import annotations

from decimal import Decimal

import pytest

from binance_market_monitor.orderbook.book import DepthDiff, DepthSnapshot, LocalOrderBook


def test_spot_snapshot_bridge_buffer_obsolete_gap_zero_delete_and_decimal_state() -> None:
    book = LocalOrderBook(market="spot", symbol="ETHUSDT")
    book.buffer_diff(
        DepthDiff(first_update_id=100, final_update_id=101, bids=(("99", "2"),), asks=())
    )
    book.buffer_diff(
        DepthDiff(
            first_update_id=102,
            final_update_id=103,
            bids=(("100.50", "1.25"),),
            asks=(("101", "4"),),
        )
    )

    book.apply_snapshot(
        DepthSnapshot(last_update_id=100, bids=(("99", "1"),), asks=(("101", "2"),))
    )

    assert book.is_valid
    assert book.last_update_id == 103
    assert book.best_bid == Decimal("100.50")
    assert book.best_ask == Decimal("101")
    assert book.bids[Decimal("99")] == Decimal("2")
    assert book.bids[Decimal("100.50")] == Decimal("1.25")

    book.apply_diff(
        DepthDiff(first_update_id=101, final_update_id=102, bids=(("98", "7"),), asks=())
    )
    assert Decimal("98") not in book.bids

    book.apply_diff(
        DepthDiff(first_update_id=104, final_update_id=104, bids=(("100.50", "0"),), asks=())
    )
    assert Decimal("100.50") not in book.bids
    assert book.best_bid == Decimal("99")

    book.apply_diff(DepthDiff(first_update_id=106, final_update_id=106, bids=(), asks=()))
    assert not book.is_valid
    assert book.reason_codes[-1] == "gap_detected"


def test_spot_rejects_malformed_crossed_stale_and_unknown_depth_ranges() -> None:
    book = LocalOrderBook(market="spot", symbol="BADUSDT", max_event_age_ms=1000)
    book.apply_snapshot(DepthSnapshot(last_update_id=10, bids=(("10", "1"),), asks=(("11", "1"),)))

    with pytest.raises(ValueError):
        DepthDiff(first_update_id=11, final_update_id=11, bids=(("10", "-1"),), asks=())

    book.apply_diff(DepthDiff(first_update_id=11, final_update_id=11, bids=(("12", "1"),), asks=()))
    assert not book.is_valid
    assert book.reason_codes[-1] == "crossed_book"

    book.apply_snapshot(DepthSnapshot(last_update_id=20, bids=(("10", "1"),), asks=(("11", "1"),)))
    book.apply_diff(
        DepthDiff(first_update_id=21, final_update_id=21, bids=(), asks=(), event_age_ms=1001)
    )
    assert not book.is_valid
    assert book.reason_codes[-1] == "stale_depth_event"

    book.apply_snapshot(DepthSnapshot(last_update_id=30, bids=(("10", "2"),), asks=(("11", "2"),)))
    metrics = book.metrics(depth_bps=100)
    assert metrics["spread_bps"] == Decimal("1000")
    assert metrics["bid_depth_notional"] == Decimal("20")
    assert metrics["ask_depth_notional"] == Decimal("22")
    assert book.metrics(depth_bps=50)["range_known"] is False


def test_futures_bridge_requires_snapshot_coverage_and_pu_chain() -> None:
    book = LocalOrderBook(market="usd_m_futures", symbol="BTCUSDT")
    book.buffer_diff(
        DepthDiff(
            first_update_id=200,
            final_update_id=201,
            previous_final_update_id=199,
            bids=(("99", "2"),),
            asks=(),
        )
    )

    book.apply_snapshot(
        DepthSnapshot(last_update_id=200, bids=(("99", "1"),), asks=(("101", "2"),))
    )
    assert book.is_valid
    assert book.last_update_id == 201

    book.apply_diff(
        DepthDiff(
            first_update_id=202,
            final_update_id=202,
            previous_final_update_id=201,
            bids=(),
            asks=(("101", "0"),),
        )
    )
    assert book.best_ask is None

    book.apply_diff(
        DepthDiff(
            first_update_id=203, final_update_id=203, previous_final_update_id=999, bids=(), asks=()
        )
    )
    assert not book.is_valid
    assert book.reason_codes[-1] == "previous_update_mismatch"


def test_futures_snapshot_without_covering_buffered_event_requires_resync() -> None:
    book = LocalOrderBook(market="usd_m_futures", symbol="BNBUSDT")
    book.buffer_diff(
        DepthDiff(
            first_update_id=200, final_update_id=201, previous_final_update_id=199, bids=(), asks=()
        )
    )

    book.apply_snapshot(DepthSnapshot(last_update_id=199, bids=(("10", "1"),), asks=(("11", "1"),)))

    assert not book.is_valid
    assert book.state == "resync_pending"
    assert book.reason_codes[-1] == "snapshot_bridge_missing"
