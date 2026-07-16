# Live async runtime runbook

## Safety model

The runtime is public-market-data only:

- Spot REST base: `https://data-api.binance.vision/api/v3`
- Spot WebSocket base: `wss://data-stream.binance.vision`
- USD-M Futures streams: split `wss://fstream.binance.com/market/...` and `/public/...`
- USD-M Futures depth snapshot: `wss://ws-fapi.binance.com/ws-fapi/v1` method `depth`

No code path needs API keys, signed requests, account data, listen keys, orders, positions, proxy/VPN, or Open Interest.

## Offline default checks

Standard tests are offline, use fake transports, and must run on Python 3.12:

```bash
uv run --python 3.12 pytest -q
uv run --python 3.12 ruff check .
uv run --python 3.12 mypy src
```

`python scripts/check_connectivity.py` performs no network unless `--live` is passed.

## Bounded live smoke

Use bounded limits when validating a VPS or CI host manually. By default this mode executes one collector cycle and exits without starting an API server:

```bash
uv run --python 3.12 python scripts/check_connectivity.py --live --timeout 5 --symbol BTCUSDT
uv run --python 3.12 binance-market-monitor monitor \
  --config configs/config.example.yaml \
  --bounded \
  --broad-messages 5 \
  --deep-messages 1 \
  --no-futures
```

If you explicitly need to inspect API endpoints during a bounded smoke, add `--serve-api`; the server is still lifecycle-bound to that run and shuts down when the bounded cycle completes:

```bash
uv run --python 3.12 binance-market-monitor monitor \
  --config configs/config.example.yaml \
  --bounded \
  --serve-api \
  --broad-messages 5 \
  --deep-messages 1 \
  --no-futures
```

The connectivity script returns JSON. Individual endpoints may be `degraded` if Binance or the network blocks a route; do not invent success.

## Continuous monitor with live read-only API

```bash
uv run --python 3.12 binance-market-monitor monitor --config configs/config.example.yaml --serve-api
```

Continuous `monitor` mode serves FastAPI by default unless `--no-serve-api` is passed. The API is created inside the monitor lifecycle and reads the same in-memory `ApiState` mutated by `LiveMonitorEngine`, so `/metrics/summary`, `/candidates/current`, `/books/{market}/{symbol}`, and `/alerts/recent` reflect the running collector rather than a disconnected app instance. The CLI keeps API-facing configuration localhost-only by default. Runtime dependencies are constructed inside the command, so importing `binance_market_monitor` does not open network sockets.

Docker Compose runs this functional monitor path and publishes only on loopback:

```bash
docker compose up --build
curl http://127.0.0.1:8000/health/ready
```

## Futures context

Futures is optional and degrades safely. Enable only for public context:

```bash
uv run --python 3.12 binance-market-monitor monitor --config configs/config.example.yaml --futures --serve-api
```

The implementation uses public agg/mark/depth stream data and WS-API `depth` snapshots only. Open Interest remains intentionally unavailable.
