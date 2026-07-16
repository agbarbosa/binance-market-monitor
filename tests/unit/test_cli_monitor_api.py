from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

import binance_market_monitor.cli as cli_module
from binance_market_monitor.api.server import ApiState
from binance_market_monitor.config import AppConfig
from binance_market_monitor.runtime.engine import LiveRunResult, LiveRuntimeConfig


class NoopRestTransport:
    async def aclose(self) -> None:
        return None


class NoopStreamTransport:
    async def aclose(self) -> None:
        return None


class FakeEngine:
    created: list[FakeEngine] = []
    server_started: asyncio.Event | None = None

    def __init__(self, **kwargs: Any) -> None:
        self.api_state: ApiState = kwargs["api_state"]
        self.shutdown_calls = 0
        FakeEngine.created.append(self)

    async def run_bounded(self) -> LiveRunResult:
        if FakeEngine.server_started is not None:
            await asyncio.wait_for(FakeEngine.server_started.wait(), timeout=1)
        self.api_state.candidates = [{"symbol": "FOOUSDT"}]
        return LiveRunResult(
            shutdown_reason="bounded_complete",
            processed_messages=1,
            candidates=1,
            alerts=0,
            storage_rows_written=0,
        )

    async def run_forever(self, *, stop_event: asyncio.Event | None = None) -> LiveRunResult:
        if FakeEngine.server_started is not None:
            await asyncio.wait_for(FakeEngine.server_started.wait(), timeout=1)
        return LiveRunResult(
            shutdown_reason="stopped",
            processed_messages=2,
            candidates=len(self.api_state.candidates),
            alerts=0,
            storage_rows_written=0,
        )

    async def shutdown(self) -> None:
        self.shutdown_calls += 1
        self.api_state.health["status"] = "stopped"


class FakeUvicornConfig:
    def __init__(self, app: Any, *, host: str, port: int, log_level: str = "info") -> None:
        self.app = app
        self.host = host
        self.port = port
        self.log_level = log_level


class FakeUvicornServer:
    created: list[FakeUvicornServer] = []

    def __init__(self, config: FakeUvicornConfig) -> None:
        self.config = config
        self.should_exit = False
        self.served_state: ApiState | None = None
        FakeUvicornServer.created.append(self)

    async def serve(self) -> None:
        self.served_state = self.config.app.state.monitor
        if FakeEngine.server_started is not None:
            FakeEngine.server_started.set()
        while not self.should_exit:
            await asyncio.sleep(0.01)


@pytest.fixture(autouse=True)
def patch_monitor_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeEngine.created = []
    FakeEngine.server_started = None
    FakeUvicornServer.created = []
    monkeypatch.setattr(cli_module, "LiveMonitorEngine", FakeEngine)
    monkeypatch.setattr(cli_module.uvicorn, "Config", FakeUvicornConfig)
    monkeypatch.setattr(cli_module.uvicorn, "Server", FakeUvicornServer)
    monkeypatch.setattr(cli_module, "AsyncHttpxJsonRestTransport", NoopRestTransport)
    monkeypatch.setattr(cli_module, "AsyncWebsocketsJsonStreamTransport", NoopStreamTransport)
    monkeypatch.setattr(cli_module, "AsyncWebsocketsJsonRpcTransport", NoopStreamTransport)


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig.model_validate(
        {
            "storage": {"base_path": str(tmp_path)},
            "api": {"bind_host": "127.0.0.1", "bind_port": 8765},
        }
    )


@pytest.mark.asyncio
async def test_monitor_serves_fastapi_over_same_runtime_api_state(tmp_path: Path) -> None:
    FakeEngine.server_started = asyncio.Event()

    result = await cli_module._run_monitor(  # noqa: SLF001 - command lifecycle regression test.
        _config(tmp_path),
        LiveRuntimeConfig(),
        bounded=False,
        serve_api=True,
    )

    assert result.shutdown_reason == "stopped"
    assert len(FakeEngine.created) == 1
    assert len(FakeUvicornServer.created) == 1
    engine = FakeEngine.created[0]
    server = FakeUvicornServer.created[0]
    assert server.config.host == "127.0.0.1"
    assert server.config.port == 8765
    assert server.served_state is engine.api_state
    assert engine.shutdown_calls == 1
    assert server.should_exit is True


@pytest.mark.asyncio
async def test_bounded_monitor_does_not_start_api_unless_explicit(tmp_path: Path) -> None:
    result = await cli_module._run_monitor(  # noqa: SLF001 - command lifecycle regression test.
        _config(tmp_path),
        LiveRuntimeConfig(),
        bounded=True,
        serve_api=False,
    )

    assert result.shutdown_reason == "bounded_complete"
    assert len(FakeEngine.created) == 1
    assert FakeUvicornServer.created == []


@pytest.mark.asyncio
async def test_bounded_monitor_can_serve_api_for_the_bounded_lifecycle(tmp_path: Path) -> None:
    FakeEngine.server_started = asyncio.Event()

    result = await cli_module._run_monitor(  # noqa: SLF001 - command lifecycle regression test.
        _config(tmp_path),
        LiveRuntimeConfig(),
        bounded=True,
        serve_api=True,
    )

    assert result.shutdown_reason == "bounded_complete"
    assert FakeUvicornServer.created[0].served_state is FakeEngine.created[0].api_state
    assert FakeUvicornServer.created[0].should_exit is True


def test_monitor_serve_api_default_preserves_bounded_exit_behavior() -> None:
    assert cli_module._resolve_monitor_api_enabled(bounded=False, serve_api=None) is True  # noqa: SLF001
    assert cli_module._resolve_monitor_api_enabled(bounded=True, serve_api=None) is False  # noqa: SLF001
    assert cli_module._resolve_monitor_api_enabled(bounded=True, serve_api=True) is True  # noqa: SLF001
