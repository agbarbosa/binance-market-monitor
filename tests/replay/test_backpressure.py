from __future__ import annotations

import asyncio
from dataclasses import dataclass

from binance_market_monitor.queues import BoundedMarketQueue, QueueItem


@dataclass
class Recorder:
    invalidated: list[tuple[str, str, str]]
    losses: list[tuple[str, str]]

    def invalidate(self, market: str, symbol: str, reason: str) -> None:
        self.invalidated.append((market, symbol, reason))

    def mark_loss(self, market: str, symbol: str) -> None:
        self.losses.append((market, symbol))


def test_depth_overflow_invalidates_book_and_requires_resync() -> None:
    recorder = Recorder(invalidated=[], losses=[])
    queue: BoundedMarketQueue[str] = BoundedMarketQueue(
        maxsize=1,
        kind="depth",
        on_depth_overflow=recorder.invalidate,
        on_trade_loss=recorder.mark_loss,
    )

    assert queue.put_nowait(QueueItem(market="spot", symbol="ETHUSDT", payload="first")) is True
    assert queue.put_nowait(QueueItem(market="spot", symbol="ETHUSDT", payload="second")) is False

    assert recorder.invalidated == [("spot", "ETHUSDT", "depth_queue_overflow")]
    assert queue.overflow_count == 1
    assert queue.trade_loss_markers == {}


def test_trade_overflow_marks_loss_and_suppresses_repeated_markers() -> None:
    recorder = Recorder(invalidated=[], losses=[])
    queue: BoundedMarketQueue[str] = BoundedMarketQueue(
        maxsize=1,
        kind="trade",
        on_depth_overflow=recorder.invalidate,
        on_trade_loss=recorder.mark_loss,
    )

    assert queue.put_nowait(QueueItem(market="spot", symbol="SOLUSDT", payload="first")) is True
    assert queue.put_nowait(QueueItem(market="spot", symbol="SOLUSDT", payload="second")) is False
    assert queue.put_nowait(QueueItem(market="spot", symbol="SOLUSDT", payload="third")) is False

    assert recorder.losses == [("spot", "SOLUSDT")]
    assert queue.trade_loss_markers == {("spot", "SOLUSDT"): 2}
    assert recorder.invalidated == []


async def _roundtrip(queue: BoundedMarketQueue[str]) -> str:
    await queue.put(QueueItem(market="spot", symbol="BTCUSDT", payload="item"))
    item = await queue.get()
    return item.payload


def test_queue_async_roundtrip_is_bounded() -> None:
    queue: BoundedMarketQueue[str] = BoundedMarketQueue(maxsize=1, kind="trade")

    assert asyncio.run(_roundtrip(queue)) == "item"
