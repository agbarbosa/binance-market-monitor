# Binance Market Monitor

Read-only Binance public market monitor MVP. The service reconstructs local books, scans public market data, emits neutral watch alerts, stores Parquet ZSTD partitions, supports deterministic offline replay, and exposes a localhost FastAPI API.

Safety guardrails:

- No account, credential, signed, order execution, proxy, VPN, or Open Interest inference code.
- Default API bind host is `127.0.0.1`.
- Standard Binance production account/API hosts are not required for runtime tests.
- Live connectivity check is opt-in: `uv run python scripts/check_connectivity.py --live`.

Common commands:

```bash
uv run pytest -q
uv run ruff check .
uv run mypy src
uv run binance-market-monitor schema --output schemas
uv run binance-market-monitor replay fixtures/events.jsonl
uv run uvicorn binance_market_monitor.app:app --host 127.0.0.1 --port 8000
```
