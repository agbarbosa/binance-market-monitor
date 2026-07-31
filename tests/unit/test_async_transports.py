from __future__ import annotations

import json

import httpx
import pytest

from binance_market_monitor.runtime.transports import (
    AsyncHttpxJsonRestTransport,
    AsyncWebsocketsJsonRpcTransport,
    AsyncWebsocketsJsonStreamTransport,
    JsonTransportError,
    ReconnectPolicy,
)


@pytest.mark.asyncio
async def test_httpx_rest_transport_decodes_json_and_closes_client() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://data-api.binance.vision/api/v3/exchangeInfo"
        return httpx.Response(200, json={"timezone": "UTC"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = AsyncHttpxJsonRestTransport(client=client)

    assert await transport.get_json("https://data-api.binance.vision/api/v3/exchangeInfo", 1.0) == {
        "timezone": "UTC"
    }

    await transport.aclose()
    assert client.is_closed


@pytest.mark.asyncio
async def test_httpx_rest_transport_rejects_non_object_json() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=["not", "an", "object"])

    transport = AsyncHttpxJsonRestTransport(
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )

    with pytest.raises(JsonTransportError, match="JSON object"):
        await transport.get_json("https://data-api.binance.vision/api/v3/exchangeInfo", 1.0)

    await transport.aclose()


class ScriptedWebSocket:
    def __init__(self, messages: list[str]) -> None:
        self.messages = list(messages)
        self.sent: list[str] = []
        self.closed = False

    async def __aenter__(self) -> ScriptedWebSocket:
        return self

    async def __aexit__(self, *_: object) -> None:
        self.closed = True

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def recv(self) -> str:
        if not self.messages:
            raise RuntimeError("socket exhausted")
        return self.messages.pop(0)


@pytest.mark.asyncio
async def test_websocket_rpc_transport_sends_payload_and_validates_json_object() -> None:
    sockets: list[ScriptedWebSocket] = []

    def connect(url: str, **_: object) -> ScriptedWebSocket:
        assert url == "wss://ws-fapi.binance.com/ws-fapi/v1"
        socket = ScriptedWebSocket([json.dumps({"status": 200, "result": {"lastUpdateId": 10}})])
        sockets.append(socket)
        return socket

    transport = AsyncWebsocketsJsonRpcTransport(connect=connect)
    response = await transport.request_json(
        "wss://ws-fapi.binance.com/ws-fapi/v1", {"id": "1", "method": "depth"}, 1.0
    )

    assert response == {"status": 200, "result": {"lastUpdateId": 10}}
    assert json.loads(sockets[0].sent[0]) == {"id": "1", "method": "depth"}
    assert sockets[0].closed is True


@pytest.mark.asyncio
async def test_stream_transport_reconnects_with_backoff_and_stops_after_limit() -> None:
    attempts = 0
    sleeps: list[float] = []

    def connect(_: str, **__: object) -> ScriptedWebSocket:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return ScriptedWebSocket(["not-json"])
        return ScriptedWebSocket(
            [
                json.dumps({"stream": "btcusdt@miniticker", "data": {"s": "BTCUSDT"}}),
                json.dumps({"stream": "ethusdt@miniticker", "data": {"s": "ETHUSDT"}}),
            ]
        )

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    transport = AsyncWebsocketsJsonStreamTransport(
        connect=connect,
        reconnect=ReconnectPolicy(
            initial_delay_seconds=0.1, max_delay_seconds=0.2, jitter_seconds=0
        ),
        sleep=sleep,
    )

    messages = [
        message
        async for message in transport.stream_json(
            "wss://data-stream.binance.vision/ws/!miniTicker@arr", 1.0, max_messages=2
        )
    ]

    assert [message["data"]["s"] for message in messages] == ["BTCUSDT", "ETHUSDT"]
    assert attempts == 2
    assert sleeps == [0.1]
