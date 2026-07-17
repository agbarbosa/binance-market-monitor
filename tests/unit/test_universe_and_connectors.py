from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from binance_market_monitor.connectors.binance import (
    BinanceConnectorError,
    FuturesDepthSnapshotClient,
    FuturesStreamRoute,
    FuturesWebSocketURLBuilder,
    SpotRestClient,
    SpotRestURLBuilder,
    SpotWebSocketURLBuilder,
)
from binance_market_monitor.universe import (
    SpotSymbolMetadata,
    UniverseFilterConfig,
    filter_spot_universe,
    select_spot_universe,
)


class RecordingRestTransport:
    def __init__(
        self,
        response: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.response = {} if response is None else response
        self.error = error
        self.calls: list[tuple[str, float]] = []

    async def get_json(self, url: str, timeout_seconds: float) -> dict[str, Any]:
        self.calls.append((url, timeout_seconds))
        if self.error is not None:
            raise self.error
        return self.response


class RecordingWsRpcTransport:
    def __init__(
        self,
        response: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.response = {} if response is None else response
        self.error = error
        self.calls: list[tuple[str, dict[str, Any], float]] = []

    async def request_json(
        self, url: str, payload: dict[str, Any], timeout_seconds: float
    ) -> dict[str, Any]:
        self.calls.append((url, payload, timeout_seconds))
        if self.error is not None:
            raise self.error
        return self.response


def test_public_binance_url_builders_use_only_verified_market_data_routes() -> None:
    spot_rest = SpotRestURLBuilder()
    assert (
        spot_rest.exchange_info(symbol="BTCUSDT")
        == "https://data-api.binance.vision/api/v3/exchangeInfo?symbol=BTCUSDT"
    )
    assert (
        spot_rest.depth_snapshot(symbol="BTCUSDT", limit=100)
        == "https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=100"
    )

    spot_ws = SpotWebSocketURLBuilder()
    assert (
        spot_ws.single_stream("BTCUSDT", "depth@100ms")
        == "wss://data-stream.binance.vision/ws/btcusdt@depth@100ms"
    )
    assert (
        spot_ws.combined_streams(["BTCUSDT@miniTicker", "ETHUSDT@bookTicker"])
        == "wss://data-stream.binance.vision/stream?streams=btcusdt@miniticker/ethusdt@bookticker"
    )

    futures_ws = FuturesWebSocketURLBuilder()
    assert (
        futures_ws.single_stream(FuturesStreamRoute.MARKET, "btcusdt@aggTrade")
        == "wss://fstream.binance.com/market/ws/btcusdt@aggtrade"
    )
    assert (
        futures_ws.combined_streams(
            FuturesStreamRoute.MARKET,
            ["btcusdt@aggTrade", "ethusdt@markPrice@1s"],
        )
        == "wss://fstream.binance.com/market/stream?streams=btcusdt@aggtrade/ethusdt@markprice@1s"
    )
    assert (
        futures_ws.single_stream(FuturesStreamRoute.PUBLIC, "btcusdt@depth@100ms")
        == "wss://fstream.binance.com/public/ws/btcusdt@depth@100ms"
    )
    assert (
        futures_ws.combined_streams(
            FuturesStreamRoute.PUBLIC,
            ["btcusdt@depth@100ms", "ethusdt@bookTicker"],
        )
        == "wss://fstream.binance.com/public/stream?streams=btcusdt@depth@100ms/ethusdt@bookticker"
    )
    assert futures_ws.depth_snapshot_api_url() == "wss://ws-fapi.binance.com/ws-fapi/v1"
    assert futures_ws.depth_snapshot_payload("BTCUSDT", limit=100) == {
        "id": "depth-BTCUSDT-100",
        "method": "depth",
        "params": {"symbol": "BTCUSDT", "limit": 100},
    }


def test_public_binance_url_builders_reject_forbidden_private_or_unsupported_routes() -> None:
    spot_rest = SpotRestURLBuilder()
    for endpoint in ("account", "openOrders", "listenKey", "order", "positionRisk", "openInterest"):
        with pytest.raises(ValueError, match="forbidden"):
            spot_rest.api_path(endpoint)

    spot_ws = SpotWebSocketURLBuilder()
    futures_ws = FuturesWebSocketURLBuilder()
    for stream in ("listenKeyAbc", "btcusdt@order", "btcusdt@account", "btcusdt@openInterest"):
        with pytest.raises(ValueError, match="forbidden"):
            spot_ws.raw_stream(stream)
        with pytest.raises(ValueError, match="forbidden"):
            futures_ws.single_stream(FuturesStreamRoute.PUBLIC, stream)

    with pytest.raises(ValueError, match="invalid symbol"):
        spot_rest.depth_snapshot(symbol="../BTCUSDT", limit=100)

    with pytest.raises(ValueError, match="approved"):
        SpotRestURLBuilder(base_url="https://api.binance.com/api/v3")
    with pytest.raises(ValueError, match="approved"):
        SpotWebSocketURLBuilder(base_url="wss://stream.binance.com")
    with pytest.raises(ValueError, match="approved"):
        FuturesWebSocketURLBuilder(base_url="wss://fapi.binance.com")
    with pytest.raises(ValueError, match="approved"):
        FuturesWebSocketURLBuilder(depth_snapshot_url="wss://fapi.binance.com/ws-fapi/v1")


async def _exercise_spot_rest_client() -> RecordingRestTransport:
    transport = RecordingRestTransport(response={"serverTime": 1})
    client = SpotRestClient(transport=transport, timeout_seconds=2.5)

    assert await client.exchange_info("BTCUSDT") == {"serverTime": 1}

    return transport


def test_spot_rest_client_uses_injected_transport_without_opening_default_connections() -> None:
    transport = asyncio.run(_exercise_spot_rest_client())

    assert transport.calls == [
        ("https://data-api.binance.vision/api/v3/exchangeInfo?symbol=BTCUSDT", 2.5)
    ]


def test_connectors_wrap_transport_failures_with_route_context() -> None:
    async def exercise() -> None:
        spot_transport = RecordingRestTransport(error=TimeoutError("slow route"))
        spot_client = SpotRestClient(transport=spot_transport, timeout_seconds=0.25)
        with pytest.raises(BinanceConnectorError, match="exchangeInfo"):
            await spot_client.exchange_info("BTCUSDT")

        futures_transport = RecordingWsRpcTransport(error=TimeoutError("slow route"))
        futures_client = FuturesDepthSnapshotClient(
            transport=futures_transport,
            timeout_seconds=0.25,
        )
        with pytest.raises(BinanceConnectorError, match="depth"):
            await futures_client.depth_snapshot("BTCUSDT", limit=100)

    asyncio.run(exercise())


def test_futures_depth_snapshot_client_uses_ws_api_depth_method_and_injected_transport() -> None:
    async def exercise() -> RecordingWsRpcTransport:
        transport = RecordingWsRpcTransport(response={"status": 200, "result": {"lastUpdateId": 1}})
        client = FuturesDepthSnapshotClient(transport=transport, timeout_seconds=3.0)

        assert await client.depth_snapshot("BTCUSDT", limit=100) == {
            "status": 200,
            "result": {"lastUpdateId": 1},
        }
        return transport

    transport = asyncio.run(exercise())

    assert transport.calls == [
        (
            "wss://ws-fapi.binance.com/ws-fapi/v1",
            {
                "id": "depth-BTCUSDT-100",
                "method": "depth",
                "params": {"symbol": "BTCUSDT", "limit": 100},
            },
            3.0,
        )
    ]


def test_filter_spot_universe_excludes_ineligible_markets_deterministically() -> None:
    now = datetime(2026, 7, 16, tzinfo=UTC)
    config = UniverseFilterConfig(
        allowed_quote_assets=("USDT",),
        min_quote_volume=Decimal("1000000"),
        min_history_minutes=60,
        min_listing_age=timedelta(days=7),
        denied_symbols=("DENIEDUSDT",),
        now=now,
    )
    old_listing = now - timedelta(days=30)
    recent_listing = now - timedelta(days=2)
    symbols = [
        SpotSymbolMetadata("BTCUSDT", "BTC", "USDT", "TRADING", old_listing, 240),
        SpotSymbolMetadata("ETHUSDC", "ETH", "USDC", "TRADING", old_listing, 240),
        SpotSymbolMetadata("USDCUSDT", "USDC", "USDT", "TRADING", old_listing, 240),
        SpotSymbolMetadata("USD1USDT", "USD1", "USDT", "TRADING", old_listing, 240),
        SpotSymbolMetadata("RLUSDUSDT", "RLUSD", "USDT", "TRADING", old_listing, 240),
        SpotSymbolMetadata("EURUSDT", "EUR", "USDT", "TRADING", old_listing, 240),
        SpotSymbolMetadata("BTCUPUSDT", "BTCUP", "USDT", "TRADING", old_listing, 240),
        SpotSymbolMetadata("HALTEDUSDT", "HALTED", "USDT", "BREAK", old_listing, 240),
        SpotSymbolMetadata("LOWVOLUSDT", "LOWVOL", "USDT", "TRADING", old_listing, 240),
        SpotSymbolMetadata("NEWHISTUSDT", "NEWHIST", "USDT", "TRADING", old_listing, 30),
        SpotSymbolMetadata("NEWLISTUSDT", "NEWLIST", "USDT", "TRADING", recent_listing, 240),
        SpotSymbolMetadata("DENIEDUSDT", "DENIED", "USDT", "TRADING", old_listing, 240),
        SpotSymbolMetadata("JUPUSDT", "JUP", "USDT", "TRADING", old_listing, 240),
    ]

    result = filter_spot_universe(
        symbols,
        quote_volumes={
            "BTCUSDT": Decimal("5000000"),
            "LOWVOLUSDT": Decimal("999999"),
            "NEWHISTUSDT": Decimal("5000000"),
            "NEWLISTUSDT": Decimal("5000000"),
            "DENIEDUSDT": Decimal("5000000"),
            "JUPUSDT": Decimal("5000000"),
        },
        config=config,
    )

    assert [symbol.symbol for symbol in result.included] == ["BTCUSDT", "JUPUSDT"]
    assert result.exclusion_reasons == {
        "ETHUSDC": "quote_asset_not_allowed",
        "USDCUSDT": "stablecoin_pair",
        "USD1USDT": "stablecoin_pair",
        "RLUSDUSDT": "stablecoin_pair",
        "EURUSDT": "stablecoin_pair",
        "BTCUPUSDT": "leveraged_token_suffix",
        "HALTEDUSDT": "status_not_trading",
        "LOWVOLUSDT": "quote_volume_below_minimum",
        "NEWHISTUSDT": "insufficient_history",
        "NEWLISTUSDT": "recent_listing",
        "DENIEDUSDT": "denylist",
    }


def test_select_spot_universe_filters_then_ranks_and_caps_deterministically() -> None:
    symbols = [
        SpotSymbolMetadata("ETHBTC", "ETH", "BTC", "TRADING", None, 120),
        SpotSymbolMetadata("AAAUSDT", "AAA", "USDT", "TRADING", None, 120),
        SpotSymbolMetadata("CCCUSDT", "CCC", "USDT", "TRADING", None, 120),
        SpotSymbolMetadata("BBBUSDT", "BBB", "USDT", "TRADING", None, 120),
    ]

    result = select_spot_universe(
        symbols,
        quote_volumes={
            "AAAUSDT": Decimal("1000"),
            "BBBUSDT": Decimal("3000"),
            "CCCUSDT": Decimal("3000"),
        },
        config=UniverseFilterConfig(
            min_quote_volume=Decimal("100"),
            min_history_minutes=0,
            min_listing_age=timedelta(0),
        ),
        max_symbols=2,
    )

    assert [symbol.symbol for symbol in result.included] == ["BBBUSDT", "CCCUSDT"]
    assert result.exclusion_reasons == {
        "AAAUSDT": "universe_cardinality_cap",
        "ETHBTC": "quote_asset_not_allowed",
    }


def test_filter_spot_universe_requires_volume_and_history_inputs() -> None:
    config = UniverseFilterConfig(
        min_quote_volume=Decimal("1000"),
        min_history_minutes=60,
        now=datetime(2026, 7, 16, tzinfo=UTC),
    )
    symbols = [
        SpotSymbolMetadata("NOVOLUMEUSDT", "NOVOLUME", "USDT", "TRADING", None, 120),
        SpotSymbolMetadata("NOHISTORYUSDT", "NOHISTORY", "USDT", "TRADING", None, None),
    ]

    result = filter_spot_universe(symbols, quote_volumes={}, config=config)

    assert result.included == ()
    assert result.exclusion_reasons == {
        "NOVOLUMEUSDT": "missing_quote_volume",
        "NOHISTORYUSDT": "missing_quote_volume",
    }
