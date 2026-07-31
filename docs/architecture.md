# Architecture

The MVP is a modular monolith under `src/binance_market_monitor`.

- `connectors`: approved public Binance URL builders and injectable transports.
- `orderbook`: deterministic Decimal Spot/Futures local-book reconstruction.
- `queues`: bounded ingestion queues with explicit depth resync and trade-loss markers.
- `scanner`: Stage 1 broad features and Stage 2 multi-evidence decisioning.
- `alerts`: versioned neutral watch payloads and async webhook delivery.
- `storage`: Parquet ZSTD partitioned event store queryable through DuckDB.
- `replay`: offline deterministic JSONL replay for corrupt/gap/duplicate events.
- `api`: FastAPI endpoints bound to localhost by default.

All default tests are offline. Live connectors are intentionally minimal and injected at boundaries.
