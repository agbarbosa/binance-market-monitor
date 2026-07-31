from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from binance_market_monitor.orderbook.book import DepthDiff, DepthSnapshot, LocalOrderBook


class ReplayResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    processed_events: int
    duplicate_events: int
    corrupt_events: int
    gap_events: int
    books: dict[str, dict[str, str | int | None]]
    candidates: list[dict[str, Any]]
    alerts: list[dict[str, Any]]


class ReplayRunner:
    def run(self, path: str | Path) -> ReplayResult:
        books: dict[tuple[str, str], LocalOrderBook] = {}
        seen: set[tuple[str, str, int, int]] = set()
        processed = 0
        duplicates = 0
        corrupt = 0
        gaps = 0
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                corrupt += 1
                continue
            if not isinstance(event, dict):
                corrupt += 1
                continue
            try:
                event_type = str(event["type"])
                market = str(event["market"])
                symbol = str(event["symbol"]).upper()
                key = (market, symbol)
                book = books.setdefault(key, LocalOrderBook(market=market, symbol=symbol))  # type: ignore[arg-type]
                if event_type == "snapshot":
                    book.apply_snapshot(
                        DepthSnapshot(
                            last_update_id=int(event["last_update_id"]),
                            bids=tuple(tuple(level) for level in event.get("bids", [])),
                            asks=tuple(tuple(level) for level in event.get("asks", [])),
                        )
                    )
                elif event_type == "diff":
                    first_update_id = int(event["first_update_id"])
                    final_update_id = int(event["final_update_id"])
                    identity = (market, symbol, first_update_id, final_update_id)
                    processed += 1
                    if identity in seen:
                        duplicates += 1
                        continue
                    seen.add(identity)
                    before_state = book.state
                    book.apply_diff(
                        DepthDiff(
                            first_update_id=first_update_id,
                            final_update_id=final_update_id,
                            previous_final_update_id=event.get("previous_final_update_id"),
                            bids=tuple(tuple(level) for level in event.get("bids", [])),
                            asks=tuple(tuple(level) for level in event.get("asks", [])),
                        )
                    )
                    if before_state == "valid" and book.state == "resync_pending":
                        gaps += 1
                else:
                    corrupt += 1
                    continue
                if event_type != "diff":
                    processed += 1
            except (KeyError, TypeError, ValueError):
                corrupt += 1
        return ReplayResult(
            processed_events=processed,
            duplicate_events=duplicates,
            corrupt_events=corrupt,
            gap_events=gaps,
            books={
                f"{market}:{symbol}": _book_state(book)
                for (market, symbol), book in books.items()
            },
            candidates=[],
            alerts=[],
        )


def _book_state(book: LocalOrderBook) -> dict[str, str | int | None]:
    return {
        "state": book.state,
        "last_update_id": book.last_update_id,
        "best_bid": str(book.best_bid) if book.best_bid is not None else None,
        "best_ask": str(book.best_ask) if book.best_ask is not None else None,
    }
