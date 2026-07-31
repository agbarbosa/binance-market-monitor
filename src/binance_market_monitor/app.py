from __future__ import annotations

import os
from pathlib import Path

from binance_market_monitor.api.server import ApiState, create_app


def build_app_state(*, storage_path: str | Path = "data") -> ApiState:
    return ApiState(
        bind_host=os.getenv("BMM_BIND_HOST", "127.0.0.1"),
        storage_path=Path(storage_path),
        webhook_url=os.getenv("BMM_WEBHOOK_URL"),
    )


def build_app() -> object:
    return create_app(build_app_state())


app = build_app()
