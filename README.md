# Binance Market Monitor

Private, read-only market intelligence monitor for detecting and explaining unusual market conditions worth manual review.

## Product status

**Status:** PRD / pre-implementation

The MVP will:

- scan the active Binance Spot `USDT` universe using public market data;
- dynamically deep-analyze the strongest candidates;
- use USD-M Futures public streams as optional contextual alignment;
- produce explainable **watch alerts** through n8n and Telegram;
- persist replayable data in Parquet and query it with DuckDB;
- run without Binance credentials or trading permissions.

The MVP will **not** place orders, access account data, recommend trades, promise returns, or bypass Binance geographic restrictions.

Watch alerts are ranking and explanation aids only. They are not probability estimates or trade recommendations.

## Product requirements

See [docs/2026-07-16-mvp-prd.md](docs/2026-07-16-mvp-prd.md).

## Verified VPS connectivity

Verified on `2026-07-16`:

- Spot market-data-only REST: working (`data-api.binance.vision`)
- Spot market-data-only WebSocket: working (`data-stream.binance.vision`)
- Spot depth snapshot through market-data-only REST: working
- USD-M Futures split WebSocket routes: working
- USD-M Futures WebSocket API depth snapshot: working
- Standard Binance Spot and Futures REST: HTTP `451`

The implementation must use only officially documented, legally available endpoints and degrade safely when a source is unavailable.

See the [2026-07-16 connectivity verification record](docs/2026-07-16-connectivity-verification.md) for the tested routes and limitations.

## Repository workflow

All changes follow:

```text
branch → pull request → manual approval
```

No automatic merge and no direct feature commits to `main`.
