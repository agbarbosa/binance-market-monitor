# Operational Runbook

## Start API locally

```bash
uv run uvicorn binance_market_monitor.app:app --host 127.0.0.1 --port 8000
```

## Health checks

- `/health/live`
- `/health/ready`
- `/health/binance`
- `/metrics/summary`

## Replay

```bash
uv run binance-market-monitor replay path/to/events.jsonl
```

Replay counts corrupt events, duplicate diffs, and gap invalidations without requiring live network access.

## Connectivity

Connectivity is opt-in and no-auth:

```bash
uv run python scripts/check_connectivity.py --live --timeout 5
```

## Security constraints

Do not add account endpoints, credentials, signatures, order execution, proxy/VPN runtime dependencies, or Open Interest inference. Webhook URL must come from environment/config and is redacted from health output.
