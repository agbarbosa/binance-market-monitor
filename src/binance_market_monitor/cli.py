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
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise typer.BadParameter("default server command is localhost-only")
    state = build_app_state()
    state.bind_host = host
    uvicorn.run(create_app(state), host=host, port=port)


@cli.command()
def monitor(
    config: Path = typer.Option(  # noqa: B008 - Typer command declaration.
        Path("configs/config.example.yaml"), "--config", exists=True
    ),
    bounded: bool = typer.Option(False, "--bounded", help="Run one bounded cycle and exit."),
    broad_messages: int | None = typer.Option(None, "--broad-messages", min=0),
    deep_messages: int | None = typer.Option(None, "--deep-messages", min=0),
    futures: bool | None = typer.Option(None, "--futures/--no-futures"),
) -> None:
    app_config = AppConfig.from_yaml(config)
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
        )
    if app_config.api.bind_host not in {"127.0.0.1", "localhost", "::1"}:
        raise typer.BadParameter("monitor defaults to localhost-only API bind")
    result = asyncio.run(_run_monitor(app_config, runtime, bounded=bounded))
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


async def _run_monitor(
    app_config: AppConfig, runtime: LiveRuntimeConfig, *, bounded: bool
) -> LiveRunResult:
    rest = AsyncHttpxJsonRestTransport()
    spot_stream = AsyncWebsocketsJsonStreamTransport()
    futures_stream = AsyncWebsocketsJsonStreamTransport()
    futures_rpc = AsyncWebsocketsJsonRpcTransport()
    engine = LiveMonitorEngine(
        app_config=app_config,
        runtime_config=runtime,
        api_state=ApiState(
            bind_host=app_config.api.bind_host,
            storage_path=app_config.storage.base_path,
            webhook_url=(
                app_config.webhook.url.get_secret_value()
                if app_config.webhook.url is not None
                else None
            ),
        ),
        rest_transport=rest,
        spot_stream_transport=spot_stream,
        futures_stream_transport=futures_stream,
        futures_rpc_transport=futures_rpc,
        webhook_transport=rest,
    )
    try:
        if bounded:
            return await engine.run_bounded()
        return await engine.run_forever()
    finally:
        await engine.shutdown()
        await rest.aclose()


if __name__ == "__main__":
    cli()
