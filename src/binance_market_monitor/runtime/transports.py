from __future__ import annotations

import asyncio
import json
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol, cast

import httpx
import websockets
from websockets.typing import Subprotocol


class JsonTransportError(RuntimeError):
    """Raised when public JSON transports return invalid data or fail."""


class WebSocketSession(Protocol):
    async def __aenter__(self) -> WebSocketSession: ...
    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> object: ...
    async def send(self, payload: str) -> None: ...
    async def recv(self) -> str | bytes: ...


ConnectCallable = Callable[..., WebSocketSession]
SleepCallable = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    initial_delay_seconds: float = 0.25
    max_delay_seconds: float = 5.0
    jitter_seconds: float = 0.25
    max_attempts: int | None = None

    def delay_for_attempt(self, attempt_index: int) -> float:
        base = float(min(self.max_delay_seconds, self.initial_delay_seconds * (2**attempt_index)))
        jitter = (
            float(random.uniform(0, self.jitter_seconds))
            if self.jitter_seconds > 0
            else 0.0
        )  # noqa: S311 - non-security backoff jitter.
        return base + jitter


class AsyncHttpxJsonRestTransport:
    """Concrete bounded REST JSON transport using httpx.AsyncClient."""

    def __init__(self, *, client: httpx.AsyncClient | None = None) -> None:
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(follow_redirects=False)

    async def get_json(self, url: str, timeout_seconds: float) -> dict[str, Any]:
        payload = await self.get_json_value(url, timeout_seconds)
        if not isinstance(payload, dict):
            raise JsonTransportError("REST response must be a JSON object")
        return cast(dict[str, Any], payload)

    async def get_json_value(self, url: str, timeout_seconds: float) -> object:
        try:
            response = await self._client.get(url, timeout=timeout_seconds)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001 - network/JSON boundary wraps arbitrary failures.
            raise JsonTransportError(f"REST JSON request failed: {exc}") from exc

    async def post_json(
        self, url: str, headers: dict[str, str], payload: dict[str, object], timeout_seconds: float
    ) -> int:
        try:
            response = await self._client.post(
                url, headers=headers, json=payload, timeout=timeout_seconds
            )
            return response.status_code
        except Exception as exc:  # noqa: BLE001 - webhook boundary wraps arbitrary failures.
            raise JsonTransportError(f"REST JSON post failed: {exc}") from exc

    async def aclose(self) -> None:
        if self._owns_client or not self._client.is_closed:
            await self._client.aclose()


class AsyncWebsocketsJsonRpcTransport:
    """One-shot JSON-RPC-over-WebSocket transport for public Binance WS API calls."""

    def __init__(self, *, connect: ConnectCallable | None = None) -> None:
        self._connect = connect or _websockets_connect

    async def request_json(
        self, url: str, payload: dict[str, Any], timeout_seconds: float
    ) -> dict[str, Any]:
        try:
            async with self._connect(
                url, open_timeout=timeout_seconds, close_timeout=timeout_seconds
            ) as ws:
                await asyncio.wait_for(ws.send(json.dumps(payload)), timeout=timeout_seconds)
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout_seconds)
        except Exception as exc:  # noqa: BLE001 - WebSocket boundary wraps arbitrary failures.
            raise JsonTransportError(f"WebSocket RPC request failed: {exc}") from exc
        return _decode_json_object(raw, context="WebSocket RPC response")

    async def aclose(self) -> None:
        return None


class AsyncWebsocketsJsonStreamTransport:
    """Streaming JSON transport with reconnect/backoff and bounded message limits."""

    def __init__(
        self,
        *,
        connect: ConnectCallable | None = None,
        reconnect: ReconnectPolicy | None = None,
        sleep: SleepCallable = asyncio.sleep,
    ) -> None:
        self._connect = connect or _websockets_connect
        self._reconnect = reconnect or ReconnectPolicy()
        self._sleep = sleep
        self._closed = False

    async def stream_json(
        self,
        url: str,
        timeout_seconds: float,
        *,
        max_messages: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        yielded = 0
        attempt = 0
        while not self._closed:
            if self._reconnect.max_attempts is not None and attempt >= self._reconnect.max_attempts:
                return
            try:
                async with self._connect(
                    url,
                    open_timeout=timeout_seconds,
                    close_timeout=timeout_seconds,
                    ping_interval=20,
                    ping_timeout=timeout_seconds,
                    max_queue=32,
                ) as ws:
                    attempt = 0
                    while not self._closed:
                        if max_messages is not None and yielded >= max_messages:
                            return
                        raw = await asyncio.wait_for(ws.recv(), timeout=timeout_seconds)
                        yield _decode_json_object(raw, context="WebSocket stream message")
                        yielded += 1
            except Exception as exc:  # noqa: BLE001 - reconnect all stream boundary failures.
                attempt += 1
                if max_messages is not None and yielded >= max_messages:
                    return
                if (
                    self._reconnect.max_attempts is not None
                    and attempt >= self._reconnect.max_attempts
                ):
                    raise JsonTransportError(f"WebSocket stream exhausted retries: {exc}") from exc
                await self._sleep(self._reconnect.delay_for_attempt(attempt - 1))

    async def aclose(self) -> None:
        self._closed = True


def _decode_json_object(raw: str | bytes, *, context: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    except Exception as exc:  # noqa: BLE001 - invalid JSON is normalized.
        raise JsonTransportError(f"{context} was not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise JsonTransportError(f"{context} must be a JSON object")
    return cast(dict[str, Any], payload)


def _websockets_connect(
    uri: str,
    *,
    open_timeout: float | None = None,
    close_timeout: float | None = None,
    ping_interval: float | None = 20,
    ping_timeout: float | None = None,
    max_queue: int | None = 32,
    subprotocols: list[Subprotocol] | None = None,
) -> WebSocketSession:
    return cast(
        WebSocketSession,
        websockets.connect(
            uri,
            open_timeout=open_timeout,
            close_timeout=close_timeout,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
            max_queue=max_queue,
            subprotocols=subprotocols,
        ),
    )
