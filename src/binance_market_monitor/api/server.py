from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import FastAPI

from binance_market_monitor.orderbook.book import LocalOrderBook


@dataclass(slots=True)
class ApiState:
    bind_host: str = "127.0.0.1"
    storage_path: Path = Path("data")
    books: dict[tuple[str, str], LocalOrderBook] = field(default_factory=dict)
    candidates: list[dict[str, Any]] = field(default_factory=list)
    alerts: list[dict[str, Any]] = field(default_factory=list)
    webhook_url: str | None = None
    health: dict[str, Any] = field(default_factory=lambda: {"status": "initialized"})
    metrics: dict[str, Any] = field(default_factory=dict)


def create_app(state: ApiState | None = None) -> FastAPI:
    app_state = ApiState() if state is None else state
    app = FastAPI(title="Binance Market Monitor", version="0.1.0")

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "healthy"}

    @app.get("/health/ready")
    def ready() -> dict[str, object]:
        return {
            "status": "ready",
            "api": {"bind_host": app_state.bind_host},
            "webhook": {"configured": app_state.webhook_url is not None, "url": "redacted"},
        }

    @app.get("/health/binance")
    def binance_health() -> dict[str, object]:
        return app_state.health

    @app.get("/metrics/summary")
    def metrics_summary() -> dict[str, object]:
        if app_state.metrics:
            return app_state.metrics
        return {
            "books": len(app_state.books),
            "candidates": len(app_state.candidates),
            "alerts": len(app_state.alerts),
        }

    @app.get("/candidates/current")
    def candidates_current() -> dict[str, object]:
        return {"candidates": app_state.candidates}

    @app.get("/books/{market}/{symbol}")
    def book_state(market: str, symbol: str) -> dict[str, object]:
        book = app_state.books.get((market, symbol.upper()))
        if book is None:
            return {"market": market, "symbol": symbol.upper(), "state": "unavailable"}
        return {
            "market": market,
            "symbol": symbol.upper(),
            "state": book.state,
            "last_update_id": book.last_update_id,
            "best_bid": str(book.best_bid) if book.best_bid is not None else None,
            "best_ask": str(book.best_ask) if book.best_ask is not None else None,
        }

    @app.get("/alerts/recent")
    def recent_alerts() -> dict[str, object]:
        return {"alerts": app_state.alerts[-100:]}

    app.state.monitor = app_state
    return app
