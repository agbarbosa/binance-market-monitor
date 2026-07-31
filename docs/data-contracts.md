# Data Contracts

Contracts are versioned at schema version `1.0` and generated from Pydantic models in `binance_market_monitor.contracts.models`.

Generate JSON Schema:

```bash
uv run --python 3.12 binance-market-monitor schema --output schemas
```

Decimal values are serialized as strings at JSON boundaries to avoid binary floating point drift. Open Interest is always represented as unavailable (`open_interest_available: false`) and is never inferred from public data.
