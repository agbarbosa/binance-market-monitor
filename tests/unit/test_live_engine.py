from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from binance_market_monitor.api.server import ApiState
from binance_market_monitor.config import AppConfig
from binance_market_monitor.runtime.engine import LiveMonitorEngine, LiveRuntimeConfig


class FakeRestTransport:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get_json(self, url: str, timeout_seconds: float) -> dict[str, Any]:
        self.calls.append(url)
        if "exchangeInfo" in url:
            return {
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                    },
                    {
                        "symbol": "ETHUSDT",
                        "baseAsset": "ETH",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                    },
                    {
                        "symbol": "FOOUSDT",
                        "baseAsset": "FOO",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                    },
                    {
                        "symbol": "LOWUSDT",
                        "baseAsset": "LOW",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                    },
                ]
            }
        if "ticker/24hr" in url:
            return {
                "items": [
                    {"symbol": "BTCUSDT", "quoteVolume": "500000000"},
                    {"symbol": "ETHUSDT", "quoteVolume": "400000000"},
                    {"symbol": "FOOUSDT", "quoteVolume": "50000000"},
                    {"symbol": "LOWUSDT", "quoteVolume": "10"},
                ]
            }
        if "depth" in url and "FOOUSDT" in url:
            return {"lastUpdateId": 100, "bids": [["10.00", "10000"]], "asks": [["10.01", "10000"]]}
        if "depth" in url:
            return {"lastUpdateId": 100, "bids": [["100", "1000"]], "asks": [["101", "1000"]]}
        raise AssertionError(f"unexpected REST URL {url}")


class FakeStreamTransport:
    def __init__(self) -> None:
        self.closed = False
        self.urls: list[str] = []

    async def stream_json(
        self,
        url: str,
        timeout_seconds: float,
        *,
        max_messages: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        self.urls.append(url)
        messages = _messages_for_url(url)
        for yielded, message in enumerate(messages):
            if max_messages is not None and yielded >= max_messages:
                return
            yield message

    async def aclose(self) -> None:
        self.closed = True


class FakeFuturesRpc:
    async def request_json(
        self, url: str, payload: dict[str, Any], timeout_seconds: float
    ) -> dict[str, Any]:
        assert url == "wss://ws-fapi.binance.com/ws-fapi/v1"
        assert payload["method"] == "depth"
        return {
            "status": 200,
            "result": {
                "lastUpdateId": 200,
                "bids": [["10.00", "20000"]],
                "asks": [["10.02", "20000"]],
            },
        }


def _messages_for_url(url: str) -> list[dict[str, Any]]:
    if "!miniticker@arr" in url:
        return [
            {"data": [{"s": "BTCUSDT", "c": "100", "q": "1000", "n": 10}]},
            {"data": [{"s": "BTCUSDT", "c": "101", "q": "1100", "n": 11}]},
            {"data": [{"s": "BTCUSDT", "c": "101", "q": "1200", "n": 12}]},
            {"data": [{"s": "FOOUSDT", "c": "10", "q": "100", "n": 10}]},
            {"data": [{"s": "FOOUSDT", "c": "12", "q": "300", "n": 30}]},
            {"data": [{"s": "FOOUSDT", "c": "15", "q": "900", "n": 90}]},
        ]
    if "!bookticker" in url:
        return [
            {"data": {"s": "BTCUSDT", "b": "100", "B": "1000", "a": "101", "A": "1000"}},
            {"data": {"s": "FOOUSDT", "b": "10.00", "B": "10000", "a": "10.01", "A": "10000"}},
        ]
    if "foousdt@depth" in url:
        return [
            {"data": {"U": 100, "u": 101, "b": [["10.00", "10001"]], "a": [["10.01", "10002"]]}}
        ]
    if "foousdt@aggtrade" in url:
        return [{"data": {"E": 1_786_000_000_000, "p": "15", "q": "10", "m": False}}]
    if "foousdt@markprice" in url:
        return [{"data": {"E": 1_786_000_000_000, "p": "15.20", "r": "0.0001"}}]
    if "foousdt@depth" in url and "fstream" in url:
        return [
            {
                "data": {
                    "U": 201,
                    "u": 201,
                    "pu": 200,
                    "b": [["10.00", "20001"]],
                    "a": [["10.02", "20002"]],
                }
            }
        ]
    return []


@pytest.mark.asyncio
async def test_live_engine_runs_bounded_offline_cycle_and_updates_api_state(tmp_path: Path) -> None:
    config = AppConfig.model_validate(
        {
            "universe": {
                "max_dynamic_candidates": 1,
                "min_quote_volume_usdt": "1000",
                "min_history_minutes": 0,
                "min_listing_age_days": 0,
            },
            "stage1": {"min_quote_volume_usdt": "1000", "max_spread_bps": 20},
            "storage": {"base_path": str(tmp_path)},
        }
    )
    api_state = ApiState(storage_path=tmp_path)
    stream = FakeStreamTransport()
    engine = LiveMonitorEngine(
        app_config=config,
        runtime_config=LiveRuntimeConfig(
            broad_stream_message_limit=8,
            deep_stream_message_limit=1,
            futures_enabled=True,
            warmup_min_trades=1,
        ),
        api_state=api_state,
        rest_transport=FakeRestTransport(),
        spot_stream_transport=stream,
        futures_stream_transport=stream,
        futures_rpc_transport=FakeFuturesRpc(),
        now=lambda: datetime(2026, 7, 16, tzinfo=UTC),
    )

    result = await engine.run_bounded()

    assert result.shutdown_reason == "bounded_complete"
    assert [candidate["symbol"] for candidate in api_state.candidates] == ["FOOUSDT"]
    assert set(engine.deep_symbols) == {"BTCUSDT", "ETHUSDT", "FOOUSDT"}
    assert api_state.books[("spot", "FOOUSDT")].is_valid
    assert api_state.books[("usd_m_futures", "FOOUSDT")].is_valid
    assert api_state.health["status"] == "running"
    assert api_state.metrics["processed_messages"] >= 10
    assert result.storage_rows_written > 0
    assert (tmp_path / "stage1_candidates").exists()
    await engine.shutdown()
    assert stream.closed is True
    assert api_state.health["status"] == "stopped"


@pytest.mark.asyncio
async def test_live_engine_shutdown_is_idempotent_with_bounded_queues(tmp_path: Path) -> None:
    engine = LiveMonitorEngine(
        app_config=AppConfig.model_validate({"storage": {"base_path": str(tmp_path)}}),
        runtime_config=LiveRuntimeConfig(queue_maxsize=1, broad_stream_message_limit=0),
        rest_transport=FakeRestTransport(),
        spot_stream_transport=FakeStreamTransport(),
    )

    await engine.shutdown()
    await engine.shutdown()

    assert engine.depth_queue.maxsize == 1
    assert engine.trade_queue.maxsize == 1
