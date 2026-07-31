#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Literal

from binance_market_monitor.connectors.binance import (
    FuturesStreamRoute,
    FuturesWebSocketURLBuilder,
    SpotRestURLBuilder,
    SpotWebSocketURLBuilder,
)
from binance_market_monitor.runtime.transports import (
    AsyncHttpxJsonRestTransport,
    AsyncWebsocketsJsonRpcTransport,
    AsyncWebsocketsJsonStreamTransport,
    ReconnectPolicy,
)

CheckKind = Literal["rest", "ws", "ws_rpc"]
CheckStatus = Literal["ok", "degraded"]


@dataclass(frozen=True, slots=True)
class LiveCheck:
    name: str
    kind: CheckKind
    url: str
    payload: dict[str, object] | None = None


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    kind: str
    url: str
    status: CheckStatus
    latency_ms: int
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "name": self.name,
            "kind": self.kind,
            "url": self.url,
            "status": self.status,
            "latency_ms": self.latency_ms,
        }
        if self.error:
            payload["error"] = self.error
        return payload


Runner = Callable[[LiveCheck, float], Awaitable[CheckResult]]


def build_live_checks(*, symbol: str = "BTCUSDT", timeout: float = 5.0) -> list[LiveCheck]:
    del timeout
    symbol = symbol.upper()
    spot_rest = SpotRestURLBuilder()
    spot_ws = SpotWebSocketURLBuilder()
    futures_ws = FuturesWebSocketURLBuilder()
    return [
        LiveCheck("spot_exchange_info", "rest", spot_rest.exchange_info(symbol)),
        LiveCheck("spot_depth", "rest", spot_rest.depth_snapshot(symbol, limit=100)),
        LiveCheck("spot_ws_message", "ws", spot_ws.single_stream(symbol, "depth@100ms")),
        LiveCheck(
            "futures_market_ws_message",
            "ws",
            futures_ws.single_stream(FuturesStreamRoute.MARKET, f"{symbol.lower()}@aggTrade"),
        ),
        LiveCheck(
            "futures_public_ws_message",
            "ws",
            futures_ws.single_stream(FuturesStreamRoute.PUBLIC, f"{symbol.lower()}@depth@100ms"),
        ),
        LiveCheck(
            "futures_ws_api_depth_snapshot",
            "ws_rpc",
            futures_ws.depth_snapshot_api_url(),
            futures_ws.depth_snapshot_payload(symbol, limit=100),
        ),
    ]


async def run_live_checks(
    checks: Sequence[LiveCheck], *, timeout: float, runner: Runner | None = None
) -> dict[str, object]:
    actual_runner = runner or _run_single_check
    results: list[CheckResult] = []
    for check in checks:
        start = time.perf_counter()
        try:
            result = await asyncio.wait_for(actual_runner(check, timeout), timeout=timeout + 1)
        except Exception as exc:  # noqa: BLE001 - smoke must degrade, not crash.
            error = str(exc) or exc.__class__.__name__
            result = CheckResult(
                check.name,
                check.kind,
                check.url,
                "degraded",
                latency_ms=int((time.perf_counter() - start) * 1000),
                error=error,
            )
        results.append(result)
    status = "ok" if all(result.status == "ok" for result in results) else "degraded"
    return {
        "status": status,
        "checks": [result.to_dict() for result in results],
        "secret_policy": (
            "no api_key, signature, account, listenKey, orders, positions, proxy, VPN, "
            "or open interest"
        ),
    }


async def _run_single_check(check: LiveCheck, timeout: float) -> CheckResult:
    start = time.perf_counter()
    if check.kind == "rest":
        transport = AsyncHttpxJsonRestTransport()
        try:
            await transport.get_json(check.url, timeout)
        finally:
            await transport.aclose()
    elif check.kind == "ws":
        stream = AsyncWebsocketsJsonStreamTransport(
            reconnect=ReconnectPolicy(
                initial_delay_seconds=0.1, max_delay_seconds=0.1, max_attempts=1
            )
        )
        try:
            async for _ in stream.stream_json(check.url, timeout, max_messages=1):
                break
        finally:
            await stream.aclose()
    else:
        if check.payload is None:
            raise ValueError("ws_rpc check requires payload")
        rpc = AsyncWebsocketsJsonRpcTransport()
        await rpc.request_json(check.url, dict(check.payload), timeout)
    return CheckResult(
        check.name,
        check.kind,
        check.url,
        "ok",
        latency_ms=int((time.perf_counter() - start) * 1000),
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Opt-in no-auth Binance public connectivity check."
    )
    parser.add_argument("--live", action="store_true", help="Actually perform public checks.")
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--symbol", default="BTCUSDT")
    args = parser.parse_args()
    if not args.live:
        print(json.dumps({"status": "skipped", "reason": "pass --live to opt in"}))
        return 0
    result = asyncio.run(
        run_live_checks(
            build_live_checks(symbol=args.symbol, timeout=args.timeout), timeout=args.timeout
        )
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
