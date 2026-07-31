from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol
from urllib.parse import urlencode

SPOT_REST_BASE_URL = "https://data-api.binance.vision/api/v3"
SPOT_WS_BASE_URL = "wss://data-stream.binance.vision"
FUTURES_WS_BASE_URL = "wss://fstream.binance.com"
FUTURES_DEPTH_SNAPSHOT_WS_API_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"

FORBIDDEN_ROUTE_TERMS = (
    "apikey",
    "api_key",
    "signature",
    "signed",
    "order",
    "position",
    "account",
    "listenkey",
    "proxy",
    "vpn",
    "openinterest",
    "open_interest",
)

SPOT_ALLOWED_API_PATHS = frozenset(
    {
        "exchangeInfo",
        "depth",
        "ticker/24hr",
        "ticker/bookTicker",
        "ticker/price",
        "klines",
        "aggTrades",
    }
)

VALID_SNAPSHOT_LIMITS = frozenset({5, 10, 20, 50, 100, 500, 1000, 5000})


class BinanceConnectorError(RuntimeError):
    """Raised when an approved public Binance connector route fails."""


class BinanceRoutePolicyError(ValueError):
    """Raised when code attempts to build a forbidden or unapproved route."""


class FuturesStreamRoute(StrEnum):
    MARKET = "market"
    PUBLIC = "public"


def _require_approved_url(actual: str, expected: str) -> None:
    if actual != expected:
        raise BinanceRoutePolicyError(f"only approved Binance URL is allowed: {expected}")


class JsonRestTransport(Protocol):
    async def get_json(self, url: str, timeout_seconds: float) -> dict[str, Any]:
        """Return decoded JSON for a public REST URL."""


class JsonWsRpcTransport(Protocol):
    async def request_json(
        self, url: str, payload: dict[str, Any], timeout_seconds: float
    ) -> dict[str, Any]:
        """Send one JSON-RPC-like WebSocket API request and return decoded JSON."""


@dataclass(frozen=True)
class SpotRestURLBuilder:
    """Build Spot REST URLs exclusively on the verified market-data-only host."""

    base_url: str = SPOT_REST_BASE_URL

    def __post_init__(self) -> None:
        _require_approved_url(self.base_url, SPOT_REST_BASE_URL)

    def exchange_info(self, symbol: str | None = None) -> str:
        query = {"symbol": _normalize_symbol(symbol)} if symbol is not None else None
        return self.api_path("exchangeInfo", query=query)

    def depth_snapshot(self, symbol: str, limit: int = 100) -> str:
        _validate_snapshot_limit(limit)
        return self.api_path(
            "depth",
            query={"symbol": _normalize_symbol(symbol), "limit": str(limit)},
        )

    def api_path(self, path: str, query: Mapping[str, str] | None = None) -> str:
        _reject_forbidden(path)
        cleaned_path = path.strip("/")
        if cleaned_path not in SPOT_ALLOWED_API_PATHS:
            raise BinanceRoutePolicyError(f"forbidden or unsupported Spot REST endpoint: {path}")
        if ".." in cleaned_path or cleaned_path.startswith(("http://", "https://")):
            raise BinanceRoutePolicyError(f"forbidden Spot REST endpoint path: {path}")

        if query is not None:
            for key, value in query.items():
                _reject_forbidden(key)
                _reject_forbidden(value)
        url = f"{self.base_url}/{cleaned_path}"
        if query:
            return f"{url}?{urlencode(query)}"
        return url


@dataclass(frozen=True)
class SpotWebSocketURLBuilder:
    """Build Spot stream URLs exclusively on data-stream.binance.vision."""

    base_url: str = SPOT_WS_BASE_URL

    def __post_init__(self) -> None:
        _require_approved_url(self.base_url, SPOT_WS_BASE_URL)

    def single_stream(self, symbol: str, stream_name: str) -> str:
        stream = f"{_normalize_symbol(symbol).lower()}@{stream_name.lower()}"
        return self.raw_stream(stream)

    def raw_stream(self, stream: str) -> str:
        normalized_stream = _normalize_stream(stream)
        return f"{self.base_url}/ws/{normalized_stream}"

    def combined_streams(self, streams: Iterable[str]) -> str:
        normalized_streams = [_normalize_stream(stream) for stream in streams]
        if not normalized_streams:
            raise ValueError("at least one Spot stream is required")
        return f"{self.base_url}/stream?streams={'/'.join(normalized_streams)}"


@dataclass(frozen=True)
class FuturesWebSocketURLBuilder:
    """Build USD-M Futures URLs using only the verified split WebSocket routes."""

    base_url: str = FUTURES_WS_BASE_URL
    depth_snapshot_url: str = FUTURES_DEPTH_SNAPSHOT_WS_API_URL

    def __post_init__(self) -> None:
        _require_approved_url(self.base_url, FUTURES_WS_BASE_URL)
        _require_approved_url(self.depth_snapshot_url, FUTURES_DEPTH_SNAPSHOT_WS_API_URL)

    def single_stream(self, route: FuturesStreamRoute, stream: str) -> str:
        normalized_stream = _normalize_stream(stream)
        return f"{self.base_url}/{route.value}/ws/{normalized_stream}"

    def combined_streams(self, route: FuturesStreamRoute, streams: Iterable[str]) -> str:
        normalized_streams = [_normalize_stream(stream) for stream in streams]
        if not normalized_streams:
            raise ValueError("at least one Futures stream is required")
        return f"{self.base_url}/{route.value}/stream?streams={'/'.join(normalized_streams)}"

    def depth_snapshot_api_url(self) -> str:
        return self.depth_snapshot_url

    def depth_snapshot_payload(self, symbol: str, limit: int = 100) -> dict[str, Any]:
        normalized_symbol = _normalize_symbol(symbol)
        _validate_snapshot_limit(limit)
        return {
            "id": f"depth-{normalized_symbol}-{limit}",
            "method": "depth",
            "params": {"symbol": normalized_symbol, "limit": limit},
        }


@dataclass(frozen=True)
class SpotRestClient:
    """Spot REST connector with injected transport; construction opens no sockets."""

    transport: JsonRestTransport
    timeout_seconds: float = 5.0
    urls: SpotRestURLBuilder = SpotRestURLBuilder()

    async def exchange_info(self, symbol: str | None = None) -> dict[str, Any]:
        url = self.urls.exchange_info(symbol=symbol)
        return await self._get_json(url, route_name="exchangeInfo")

    async def depth_snapshot(self, symbol: str, limit: int = 100) -> dict[str, Any]:
        url = self.urls.depth_snapshot(symbol=symbol, limit=limit)
        return await self._get_json(url, route_name="depth")

    async def _get_json(self, url: str, route_name: str) -> dict[str, Any]:
        try:
            return await self.transport.get_json(url, self.timeout_seconds)
        except Exception as exc:  # noqa: BLE001 - boundary wraps arbitrary transport failures.
            message = f"Binance Spot REST {route_name} request failed: {exc}"
            raise BinanceConnectorError(message) from exc


@dataclass(frozen=True)
class FuturesDepthSnapshotClient:
    """Futures depth snapshot connector using the public WS-API `depth` method only."""

    transport: JsonWsRpcTransport
    timeout_seconds: float = 5.0
    urls: FuturesWebSocketURLBuilder = FuturesWebSocketURLBuilder()

    async def depth_snapshot(self, symbol: str, limit: int = 100) -> dict[str, Any]:
        url = self.urls.depth_snapshot_api_url()
        payload = self.urls.depth_snapshot_payload(symbol=symbol, limit=limit)
        try:
            return await self.transport.request_json(url, payload, self.timeout_seconds)
        except Exception as exc:  # noqa: BLE001 - boundary wraps arbitrary transport failures.
            message = f"Binance Futures WS-API depth request failed: {exc}"
            raise BinanceConnectorError(message) from exc


def _normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized.isalnum() or normalized != symbol.strip().upper():
        raise ValueError(f"invalid symbol: {symbol}")
    _reject_forbidden(normalized)
    return normalized


def _normalize_stream(stream: str) -> str:
    normalized = stream.strip().lower()
    if not normalized or not _contains_only_stream_characters(normalized):
        raise ValueError(f"invalid stream: {stream}")
    _reject_forbidden(normalized)
    return normalized


def _contains_only_stream_characters(value: str) -> bool:
    return all(character.isalnum() or character in {"@", "_", ".", "!"} for character in value)


def _reject_forbidden(value: str) -> None:
    compact_value = value.replace("-", "_").replace("/", "_").lower()
    for term in FORBIDDEN_ROUTE_TERMS:
        if term in compact_value:
            raise BinanceRoutePolicyError(f"forbidden Binance route term: {term}")


def _validate_snapshot_limit(limit: int) -> None:
    if limit not in VALID_SNAPSHOT_LIMITS:
        raise ValueError(f"unsupported Binance depth snapshot limit: {limit}")
