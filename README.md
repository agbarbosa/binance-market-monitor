# Binance Market Monitor

Read-only Binance public market monitor MVP. The service reconstructs local books, scans public market data, emits neutral watch alerts, stores Parquet ZSTD partitions, supports deterministic offline replay, and exposes a localhost FastAPI API backed by the live monitor runtime.

Safety guardrails:

- Python 3.12+ only.
- No account, credential, signed, order execution, proxy, VPN, or Open Interest inference code.
- Default API bind host is `127.0.0.1`.
- Standard Binance production account/API hosts are not required for runtime tests.
- Live connectivity check is opt-in: `uv run --python 3.12 python scripts/check_connectivity.py --live`.

Common commands:

```bash
uv run --python 3.12 pytest -q
uv run --python 3.12 ruff check .
uv run --python 3.12 mypy src
uv run --python 3.12 binance-market-monitor schema --output schemas
uv run --python 3.12 binance-market-monitor replay fixtures/events.jsonl
uv run --python 3.12 binance-market-monitor monitor --config configs/config.example.yaml --bounded --broad-messages 5 --deep-messages 1 --no-futures
uv run --python 3.12 binance-market-monitor monitor --config configs/config.example.yaml --serve-api
```

`monitor` serves the read-only FastAPI on the same in-memory `ApiState` used by the collector in continuous mode. `--bounded` exits after one cycle and does not start the API unless `--serve-api` is passed explicitly; if enabled for a bounded run, the API shuts down with that bounded lifecycle.

See `docs/runbook-live-runtime.md` for live smoke checks, bounded runtime, and Futures degradation notes.
