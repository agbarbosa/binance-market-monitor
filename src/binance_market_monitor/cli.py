from __future__ import annotations

import asyncio
from pathlib import Path

import typer
import uvicorn

from binance_market_monitor.api.server import ApiState, create_app
from binance_market_monitor.app import build_app_state
from binance_market_monitor.config import AppConfig
from binance_market_monitor.contracts.schema import write_json_schemas
from binance_market_monitor.replay.runner import ReplayRunner
from binance_market_monitor.runtime.engine import (
    LiveMonitorEngine,
    LiveRunResult,
    LiveRuntimeConfig,
    runtime_config_from_app,
)
from binance_market_monitor.runtime.transports import (
    AsyncHttpxJsonRestTransport,
    AsyncWebsocketsJsonRpcTransport,
    AsyncWebsocketsJsonStreamTransport,
)

cli = typer.Typer(help="Read-only Binance market monitor operations.")
_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


@cli.command()
def schema(output: Path | None = None) -> None:
    output = output or Path("schemas")
    written = write_json_schemas(output)
    typer.echo(f"schemas={len(written)} output={output}")


@cli.command()
def replay(events: Path) -> None:
    result = ReplayRunner().run(events)
    typer.echo(
        f"processed={result.processed_events} duplicates={result.duplicate_events} "
        f"corrupt={result.corrupt_events} gaps={result.gap_events}"
    )


@cli.command()
def server(
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8000, "--port"),
) -> None:
    _require_loopback_host(host, context="server command")
    state = build_app_state()
    state.bind_host = host
    uvicorn.run(create_app(state), host=host, port=port)


@cli.command()
def monitor(
    config: Path = typer.Option(  # noqa: B008 - Typer command declaration.
        Path("configs/config.example.yaml"), "--config", exists=True
    ),
    bounded: bool = typer.Option(False, "--bounded", help="Run one bounded cycle and exit."),
    serve_api: bool | None = typer.Option(
        None,
        "--serve-api/--no-serve-api",
        help=(
            "Serve the read-only FastAPI over the collector's live ApiState. "
            "Defaults on for continuous monitor and off for --bounded."
        ),
    ),
    api_host: str | None = typer.Option(
        None,
        "--api-host",
        help="Loopback host for the monitor-owned API server; defaults to config api.bind_host.",
    ),
    api_port: int | None = typer.Option(
        None,
        "--api-port",
        min=1,
        max=65_535,
        help="Port for the monitor-owned API server; defaults to config api.bind_port.",
    ),
    broad_messages: int | None = typer.Option(None, "--broad-messages", min=0),
    deep_messages: int | None = typer.Option(None, "--deep-messages", min=0),
    futures: bool | None = typer.Option(None, "--futures/--no-futures"),
) -> None:
    app_config = AppConfig.from_yaml(config)
    app_config = _with_api_overrides(app_config, host=api_host, port=api_port)
    runtime = runtime_config_from_app(app_config)
    has_runtime_overrides = (
        broad_messages is not None
        or deep_messages is not None
        or futures is not None
        or not bounded
    )
    if has_runtime_overrides:
        runtime = LiveRuntimeConfig(
            rest_timeout_seconds=runtime.rest_timeout_seconds,
            ws_timeout_seconds=runtime.ws_timeout_seconds,
            queue_maxsize=runtime.queue_maxsize,
            stage1_history_points=runtime.stage1_history_points,
            broad_stream_message_limit=(
                broad_messages
                if broad_messages is not None
                else runtime.broad_stream_message_limit
                if bounded
                else 200
            ),
            deep_stream_message_limit=(
                deep_messages
                if deep_messages is not None
                else runtime.deep_stream_message_limit
                if bounded
                else 10
            ),
            futures_enabled=futures if futures is not None else runtime.futures_enabled,
            warmup_min_trades=runtime.warmup_min_trades,
            snapshot_limit=runtime.snapshot_limit,
            max_universe_symbols=runtime.max_universe_symbols,
            api_bind_host=app_config.api.bind_host,
            stage1_evaluation_interval_seconds=runtime.stage1_evaluation_interval_seconds,
            stage2_evaluation_interval_seconds=runtime.stage2_evaluation_interval_seconds,
        )
    _require_loopback_host(app_config.api.bind_host, context="monitor API")
    result = asyncio.run(
        _run_monitor(
            app_config,
            runtime,
            bounded=bounded,
            serve_api=_resolve_monitor_api_enabled(bounded=bounded, serve_api=serve_api),
        )
    )
    typer.echo(
        " ".join(
            [
                f"status={result.shutdown_reason}",
                f"processed={result.processed_messages}",
                f"candidates={result.candidates}",
                f"alerts={result.alerts}",
                f"storage_rows={result.storage_rows_written}",
            ]
        )
    )


def _resolve_monitor_api_enabled(*, bounded: bool, serve_api: bool | None) -> bool:
    if serve_api is not None:
        return serve_api
    return not bounded


def _with_api_overrides(
    app_config: AppConfig, *, host: str | None, port: int | None
) -> AppConfig:
    if host is None and port is None:
        return app_config
    api_config = app_config.api.model_copy(
        update={
            key: value
            for key, value in {"bind_host": host, "bind_port": port}.items()
            if value is not None
        }
    )
    return app_config.model_copy(update={"api": api_config})


def _require_loopback_host(host: str, *, context: str) -> None:
    if host not in _LOOPBACK_HOSTS:
        raise typer.BadParameter(f"{context} is localhost-only")


async def _run_monitor(
    app_config: AppConfig,
    runtime: LiveRuntimeConfig,
    *,
    bounded: bool,
    serve_api: bool,
) -> LiveRunResult:
    rest = AsyncHttpxJsonRestTransport()
    spot_stream = AsyncWebsocketsJsonStreamTransport()
    futures_stream = AsyncWebsocketsJsonStreamTransport()
    futures_rpc = AsyncWebsocketsJsonRpcTransport()
    api_state = ApiState(
        bind_host=app_config.api.bind_host,
        storage_path=app_config.storage.base_path,
        webhook_url=(
            app_config.webhook.url.get_secret_value()
            if app_config.webhook.url is not None
            else None
        ),
    )
    engine = LiveMonitorEngine(
        app_config=app_config,
        runtime_config=runtime,
        api_state=api_state,
        rest_transport=rest,
        spot_stream_transport=spot_stream,
        futures_stream_transport=futures_stream,
        futures_rpc_transport=futures_rpc,
        webhook_transport=rest,
    )
    try:
        if serve_api:
            return await _run_engine_with_api(engine, app_config, bounded=bounded)
        if bounded:
            return await engine.run_bounded()
        return await engine.run_forever()
    finally:
        await engine.shutdown()
        await rest.aclose()


async def _run_engine_with_api(
    engine: LiveMonitorEngine, app_config: AppConfig, *, bounded: bool
) -> LiveRunResult:
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(engine.api_state),
            host=app_config.api.bind_host,
            port=app_config.api.bind_port,
            log_level=app_config.logging.level.lower(),
        )
    )
    server_task = asyncio.create_task(server.serve())
    engine_task = asyncio.create_task(engine.run_bounded() if bounded else engine.run_forever())
    try:
        done, _ = await asyncio.wait(
            {engine_task, server_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if engine_task in done:
            return await engine_task
        await server_task
        raise RuntimeError("monitor API server stopped before collector completed")
    finally:
        if not engine_task.done():
            engine_task.cancel()
            await asyncio.gather(engine_task, return_exceptions=True)
        server.should_exit = True
        if not server_task.done():
            try:
                await asyncio.wait_for(server_task, timeout=5)
            except TimeoutError:
                server_task.cancel()
                await asyncio.gather(server_task, return_exceptions=True)


if __name__ == "__main__":
    cli()
