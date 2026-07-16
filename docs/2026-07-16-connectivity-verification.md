# Binance connectivity verification — 2026-07-16

## Purpose

Record the public, no-auth Binance routes verified from the intended VPS before implementation of Binance Market Monitor.

This is evidence for the MVP PRD, not a guarantee of future availability. The implementation must include an opt-in smoke script because Binance routes, schemas, limits, and regional availability can change.

## Test conditions

- Date: `2026-07-16`
- Environment: intended Linux VPS
- Authentication: none
- Binance account data: not accessed
- Orders: not placed
- Proxy or VPN bypass: not used
- Secrets captured: none

## Results

| Capability | Tested route | Result |
| --- | --- | --- |
| Spot exchange metadata | `https://data-api.binance.vision/api/v3/exchangeInfo?symbol=BTCUSDT` | HTTP `200` |
| Spot order-book snapshot | `https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=100` | HTTP `200`; `lastUpdateId` present; 100 bids and 100 asks |
| Spot depth stream | `wss://data-stream.binance.vision/ws/btcusdt@depth@100ms` | Depth events received |
| Standard Spot REST | `https://api.binance.com/api/v3/exchangeInfo?symbol=BTCUSDT` | HTTP `451` |
| Standard Spot stream | `wss://stream.binance.com/ws/btcusdt@depth` | HTTP `451` |
| USD-M Futures aggregate trades | `wss://fstream.binance.com/market/ws/btcusdt@aggTrade` | `aggTrade` events received |
| USD-M Futures depth | `wss://fstream.binance.com/public/ws/btcusdt@depth@100ms` | `depthUpdate` events received |
| Futures depth snapshot | `wss://ws-fapi.binance.com/ws-fapi/v1`, method `depth` | Status `200`; `lastUpdateId` present; 100 bids and 100 asks |
| Standard USD-M Futures REST | `https://fapi.binance.com/fapi/v1/exchangeInfo` | HTTP `451` |
| Futures Open Interest over WS API | Candidate method names tested | Unsupported/unknown methods; excluded from MVP |

## Verified routing rules

Current USD-M Futures direct streams require the `/ws/` segment:

```text
wss://fstream.binance.com/market/ws/<stream>
wss://fstream.binance.com/public/ws/<stream>
```

Combined-stream forms use:

```text
wss://fstream.binance.com/market/stream?streams=<stream1>/<stream2>
wss://fstream.binance.com/public/stream?streams=<stream1>/<stream2>
```

A bare route such as `.../market/<stream>` or `.../public/<stream>` is not valid.

## Product consequences

1. Spot metadata, history, and snapshots must use the official market-data-only REST family.
2. Spot live ingestion must use the official market-data-only WebSocket family.
3. Futures ingestion must use the current split `/market` and `/public` WebSocket route families.
4. Futures depth snapshots may use the public WebSocket API `depth` method.
5. Standard endpoints returning HTTP `451` are not runtime dependencies.
6. The application must not attempt geographic bypass.
7. Open Interest is unavailable in the MVP and must be reported as such.
8. Endpoint health must be observable and unavailable sources must degrade safely.

## Required implementation artifact

The implementation must add an opt-in, no-auth script at:

```text
scripts/check_connectivity.py
```

It must:

- test only approved public endpoints;
- use bounded timeouts;
- redact URLs containing sensitive query values if any are added later;
- output machine-readable status without market payload dumps;
- return nonzero when a required route is unavailable;
- distinguish expected HTTP `451`, timeout, DNS, TLS, WebSocket handshake, schema, and sequence failures;
- remain skipped in default offline CI.

## References

- [MVP PRD](2026-07-16-mvp-prd.md)
- [Binance Spot market-data-only URLs](https://developers.binance.com/en/docs/products/spot/faqs/market_data_only)
- [Binance Spot WebSocket streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
- [Binance USD-M Futures market streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams)
- [Binance Futures WebSocket API order book](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/websocket-api/Order-Book)
