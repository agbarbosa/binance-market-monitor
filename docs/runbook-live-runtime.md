# Live async runtime runbook

## Safety model

The runtime is public-market-data only:

- Spot REST base: `https://data-api.binance.vision/api/v3`
- Spot WebSocket base: `wss://data-stream.binance.vision`
- USD-M Futures streams: split `wss://fstream.binance.com/market/...` and `/public/...`
- USD-M Futures depth snapshot: `wss://ws-fapi.binance.com/ws-fapi/v1` method `depth`

No code path needs API keys, signed requests, account data, listen keys, orders, positions, proxy/VPN, or Open Interest.

## Offline default checks

Standard tests are offline and use fake transports:

```bash
uv run pytest -q
uv run ruff check .
uv run mypy src
```

`python scripts/check_connectivity.py` performs no network unless `--live` is passed.

## Bounded live smoke

Use bounded limits when validating a VPS or CI host manually:

```bash
uv run python scripts/check_connectivity.py --live --timeout 5 --symbol BTCUSDT
uv run binance-market-monitor monitor \
  --config configs/config.example.yaml \
  --bounded \
  --broad-messages 5 \
  --deep-messages 1 \
  --no-futures
```

The connectivity script returns JSON. Individual endpoints may be `degraded` if Binance or the network blocks a route; do not invent success.

## Continuous monitor

```bash
uv run binance-market-monitor monitor --config configs/config.example.yaml
```

The CLI keeps API-facing configuration localhost-only by default. Runtime dependencies are constructed inside the command, so importing `binance_market_monitor` does not open network sockets.

## Futures context

Futures is optional and degrades safely. Enable only for public context:

```bash
uv run binance-market-monitor monitor --config configs/config.example.yaml --futures
```

The implementation uses public agg/mark/depth stream data and WS-API `depth` snapshots only. Open Interest remains intentionally unavailable.
