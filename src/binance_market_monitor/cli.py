from __future__ import annotations

from pathlib import Path

import typer
import uvicorn

from binance_market_monitor.api.server import create_app
from binance_market_monitor.app import build_app_state
from binance_market_monitor.contracts.schema import write_json_schemas
from binance_market_monitor.replay.runner import ReplayRunner

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


if __name__ == "__main__":
    cli()
