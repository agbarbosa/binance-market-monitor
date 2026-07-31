from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

QueueKind = Literal["depth", "trade"]


@dataclass(frozen=True, slots=True)
class QueueItem[T]:
    market: str
    symbol: str
    payload: T


class BoundedMarketQueue[T]:
    """Bounded async queue with explicit market-data overflow semantics."""

    def __init__(
        self,
        *,
        maxsize: int,
        kind: QueueKind,
        on_depth_overflow: Callable[[str, str, str], None] | None = None,
        on_trade_loss: Callable[[str, str], None] | None = None,
    ) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self.kind = kind
        self._queue: asyncio.Queue[QueueItem[T]] = asyncio.Queue(maxsize=maxsize)
        self._on_depth_overflow = on_depth_overflow
        self._on_trade_loss = on_trade_loss
        self.overflow_count = 0
        self.trade_loss_markers: dict[tuple[str, str], int] = defaultdict(int)

    @property
    def qsize(self) -> int:
        return self._queue.qsize()

    @property
    def maxsize(self) -> int:
        return self._queue.maxsize

    def put_nowait(self, item: QueueItem[T]) -> bool:
        if not self._queue.full():
            self._queue.put_nowait(item)
            return True
        self._record_overflow(item)
        return False

    async def put(self, item: QueueItem[T]) -> bool:
        return self.put_nowait(item)

    async def get(self) -> QueueItem[T]:
        return await self._queue.get()

    def _record_overflow(self, item: QueueItem[T]) -> None:
        self.overflow_count += 1
        if self.kind == "depth":
            if self._on_depth_overflow is not None:
                self._on_depth_overflow(item.market, item.symbol, "depth_queue_overflow")
            return
        key = (item.market, item.symbol)
        self.trade_loss_markers[key] += 1
        if self.trade_loss_markers[key] == 1 and self._on_trade_loss is not None:
            self._on_trade_loss(item.market, item.symbol)
