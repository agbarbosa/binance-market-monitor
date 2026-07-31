from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, cast

Market = Literal["spot", "usd_m_futures"]
BookState = Literal["warming", "valid", "stale", "invalid", "resync_pending"]
PriceLevelInput = tuple[str | Decimal, str | Decimal]


@dataclass(frozen=True, slots=True)
class DepthSnapshot:
    last_update_id: int
    bids: Iterable[PriceLevelInput]
    asks: Iterable[PriceLevelInput]

    def __post_init__(self) -> None:
        if self.last_update_id <= 0:
            raise ValueError("last_update_id must be positive")
        object.__setattr__(self, "bids", tuple(_normalize_levels(self.bids)))
        object.__setattr__(self, "asks", tuple(_normalize_levels(self.asks)))


@dataclass(frozen=True, slots=True)
class DepthDiff:
    first_update_id: int
    final_update_id: int
    bids: Iterable[PriceLevelInput]
    asks: Iterable[PriceLevelInput]
    previous_final_update_id: int | None = None
    event_age_ms: int = 0

    def __post_init__(self) -> None:
        if self.first_update_id <= 0 or self.final_update_id <= 0:
            raise ValueError("update ids must be positive")
        if self.final_update_id < self.first_update_id:
            raise ValueError("final_update_id must be >= first_update_id")
        if self.previous_final_update_id is not None and self.previous_final_update_id <= 0:
            raise ValueError("previous_final_update_id must be positive")
        if self.event_age_ms < 0:
            raise ValueError("event_age_ms must be non-negative")
        object.__setattr__(self, "bids", tuple(_normalize_levels(self.bids)))
        object.__setattr__(self, "asks", tuple(_normalize_levels(self.asks)))


class LocalOrderBook:
    """Deterministic Binance Spot/Futures local order-book state machine.

    The class deliberately contains no I/O and uses Decimal only, so replay and live ingestion
    produce byte-for-byte comparable logical state. Invalid books remain invalid until the caller
    applies a fresh snapshot, matching Binance's resync guidance after gaps or stale/crossed data.
    """

    def __init__(self, *, market: Market, symbol: str, max_event_age_ms: int = 3_000) -> None:
        if max_event_age_ms <= 0:
            raise ValueError("max_event_age_ms must be positive")
        self.market = market
        self.symbol = symbol.upper()
        self.max_event_age_ms = max_event_age_ms
        self.state: BookState = "warming"
        self.last_update_id: int | None = None
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.reason_codes: list[str] = []
        self._buffer: list[DepthDiff] = []
        self._known_depth_bps: int | None = None

    @property
    def is_valid(self) -> bool:
        return self.state == "valid"

    @property
    def best_bid(self) -> Decimal | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return min(self.asks) if self.asks else None

    @property
    def spread(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    def buffer_diff(self, diff: DepthDiff) -> None:
        if self.state == "warming":
            self._buffer.append(diff)
            self._buffer.sort(key=lambda item: item.final_update_id)
            return
        self.apply_diff(diff)

    def apply_snapshot(self, snapshot: DepthSnapshot) -> None:
        snapshot_bids = cast(tuple[tuple[Decimal, Decimal], ...], snapshot.bids)
        snapshot_asks = cast(tuple[tuple[Decimal, Decimal], ...], snapshot.asks)
        self.bids = {price: qty for price, qty in snapshot_bids if qty != 0}
        self.asks = {price: qty for price, qty in snapshot_asks if qty != 0}
        self.last_update_id = snapshot.last_update_id
        self.state = "valid"
        self.reason_codes = []
        if self._is_crossed():
            self._invalidate("crossed_book")
            return

        buffered = [
            diff for diff in self._buffer if diff.final_update_id >= snapshot.last_update_id
        ]
        self._buffer = []
        if not buffered:
            return

        bridge = self._find_snapshot_bridge(snapshot.last_update_id, buffered)
        if bridge is None:
            self._invalidate("snapshot_bridge_missing", state="resync_pending")
            return

        if self.market == "usd_m_futures" and bridge.previous_final_update_id is not None:
            self.last_update_id = bridge.previous_final_update_id
        for diff in buffered[buffered.index(bridge) :]:
            if not self.is_valid:
                return
            self.apply_diff(diff)

    def apply_diff(self, diff: DepthDiff) -> None:
        if self.state == "warming":
            self.buffer_diff(diff)
            return
        if not self.is_valid:
            return
        if diff.event_age_ms > self.max_event_age_ms:
            self._invalidate("stale_depth_event", state="stale")
            return
        if self.last_update_id is None:
            self._invalidate("missing_snapshot", state="resync_pending")
            return
        if diff.final_update_id <= self.last_update_id:
            return
        if self.market == "spot":
            expected = self.last_update_id + 1
            if not (diff.first_update_id <= expected <= diff.final_update_id):
                self._invalidate("gap_detected", state="resync_pending")
                return
        else:
            if diff.previous_final_update_id != self.last_update_id:
                self._invalidate("previous_update_mismatch", state="resync_pending")
                return
            expected = self.last_update_id + 1
            if diff.first_update_id > expected or diff.final_update_id < expected:
                self._invalidate("gap_detected", state="resync_pending")
                return

        diff_bids = cast(tuple[tuple[Decimal, Decimal], ...], diff.bids)
        diff_asks = cast(tuple[tuple[Decimal, Decimal], ...], diff.asks)
        self._apply_levels(self.bids, diff_bids)
        self._apply_levels(self.asks, diff_asks)
        self.last_update_id = diff.final_update_id
        if self._is_crossed():
            self._invalidate("crossed_book")

    def invalidate(self, reason: str, *, state: BookState = "resync_pending") -> None:
        self._invalidate(reason, state=state)

    def metrics(self, *, depth_bps: int) -> dict[str, Decimal | bool | None]:
        if depth_bps <= 0:
            raise ValueError("depth_bps must be positive")
        spread = self.spread
        midpoint = self._midpoint()
        range_known = self._known_depth_bps is None or depth_bps >= self._known_depth_bps
        if midpoint is None:
            return {
                "spread_bps": None,
                "bid_depth_notional": Decimal("0"),
                "ask_depth_notional": Decimal("0"),
                "range_known": range_known,
            }
        bid_depth = sum((price * qty for price, qty in self.bids.items()), Decimal("0"))
        ask_depth = sum((price * qty for price, qty in self.asks.items()), Decimal("0"))
        spread_base = self.best_bid or midpoint
        spread_bps = (spread / spread_base * Decimal(10_000)) if spread is not None else None
        self._known_depth_bps = depth_bps
        return {
            "spread_bps": spread_bps,
            "bid_depth_notional": bid_depth,
            "ask_depth_notional": ask_depth,
            "range_known": range_known,
        }

    def _find_snapshot_bridge(
        self, last_update_id: int, buffered: list[DepthDiff]
    ) -> DepthDiff | None:
        for diff in buffered:
            if (
                self.market == "spot"
                and diff.first_update_id <= last_update_id + 1 <= diff.final_update_id
            ):
                return diff
            if (
                self.market == "usd_m_futures"
                and diff.first_update_id <= last_update_id <= diff.final_update_id
            ):
                return diff
        return None

    def _apply_levels(
        self, side: dict[Decimal, Decimal], levels: tuple[tuple[Decimal, Decimal], ...]
    ) -> None:
        for price, quantity in levels:
            if quantity == 0:
                side.pop(price, None)
            else:
                side[price] = quantity

    def _midpoint(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / Decimal("2")

    def _is_crossed(self) -> bool:
        return (
            self.best_bid is not None
            and self.best_ask is not None
            and self.best_bid >= self.best_ask
        )

    def _invalidate(self, reason: str, *, state: BookState = "invalid") -> None:
        self.state = state
        self.reason_codes.append(reason)


def _normalize_levels(levels: Iterable[PriceLevelInput]) -> tuple[tuple[Decimal, Decimal], ...]:
    normalized: list[tuple[Decimal, Decimal]] = []
    for raw_price, raw_quantity in levels:
        price = Decimal(str(raw_price))
        quantity = Decimal(str(raw_quantity))
        if price <= 0:
            raise ValueError("depth price must be positive")
        if quantity < 0:
            raise ValueError("depth quantity must be non-negative")
        normalized.append((price, quantity))
    return tuple(normalized)
