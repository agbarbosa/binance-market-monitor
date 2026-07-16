"""Hermes plugin exposing local Binance Market Monitor opportunities."""

from __future__ import annotations

from .schemas import BINANCE_OPPORTUNITIES_LIST_SCHEMA
from .tools import handle_binance_opportunities_list


def register(ctx) -> None:
    """Register the read-only opportunities tool with Hermes."""
    ctx.register_tool(
        name="binance_opportunities_list",
        toolset="binance_opportunities",
        schema=BINANCE_OPPORTUNITIES_LIST_SCHEMA,
        handler=handle_binance_opportunities_list,
        emoji="📈",
    )
