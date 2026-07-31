from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal, Protocol

from binance_market_monitor.alerts.engine import AlertPayloadRecord


class WebhookTransport(Protocol):
    async def post_json(
        self, url: str, headers: dict[str, str], payload: dict[str, object], timeout_seconds: float
    ) -> int: ...


@dataclass(frozen=True, slots=True)
class WebhookConfig:
    enabled: bool = False
    url: str | None = None
    timeout_seconds: float = 5.0
    max_retries: int = 3


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    status: Literal["disabled", "delivered", "failed"]
    attempts: int
    status_code: int | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class FailedDelivery:
    alert_id: str
    dedupe_key: str
    error: str
    attempts: int


class AsyncWebhookDispatcher:
    def __init__(
        self,
        config: WebhookConfig,
        *,
        transport: WebhookTransport | None = None,
        sleep: Callable[[float], Awaitable[None] | None] | None = None,
        jitter: Callable[[int], float] | None = None,
    ) -> None:
        if config.enabled and not config.url:
            raise ValueError("enabled webhook requires URL from environment/config")
        self.config = config
        self.transport = transport
        self._sleep = sleep or asyncio.sleep
        self._jitter = jitter or (lambda attempt: 0.05 * attempt)
        self.failed_deliveries: list[FailedDelivery] = []

    async def send(self, alert: AlertPayloadRecord) -> DeliveryResult:
        if not self.config.enabled:
            return DeliveryResult(status="disabled", attempts=0)
        if self.transport is None:
            raise RuntimeError("webhook transport must be injected for runtime delivery")
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": alert.dedupe_key,
        }
        attempts = self.config.max_retries + 1
        last_error: str | None = None
        for index in range(attempts):
            try:
                status_code = await self.transport.post_json(
                    self.config.url or "",
                    headers,
                    alert.to_dict(),
                    self.config.timeout_seconds,
                )
                if 200 <= status_code < 300:
                    return DeliveryResult("delivered", index + 1, status_code=status_code)
                last_error = f"HTTP {status_code}"
            except Exception as exc:  # noqa: BLE001 - transport boundary records arbitrary failures.
                last_error = str(exc)
            if index < attempts - 1:
                maybe_awaitable = self._sleep((2**index * 0.1) + self._jitter(index))
                if maybe_awaitable is not None:
                    await maybe_awaitable
        self.failed_deliveries.append(
            FailedDelivery(
                alert_id=alert.alert_id,
                dedupe_key=alert.dedupe_key,
                error=last_error or "unknown delivery failure",
                attempts=attempts,
            )
        )
        return DeliveryResult("failed", attempts, error=last_error)
