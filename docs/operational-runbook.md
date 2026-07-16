# Operational Runbook

## Start functional monitor and API locally

Use the `monitor` command rather than a standalone `uvicorn` process so the read-only API observes the same live `ApiState` updated by the collector:

```bash
uv run --python 3.12 binance-market-monitor monitor --config configs/config.example.yaml --serve-api
```

For bounded smoke tests, omit `--serve-api` to run one collector cycle and exit without a lingering server:

```bash
uv run --python 3.12 binance-market-monitor monitor --config configs/config.example.yaml --bounded --broad-messages 5 --deep-messages 1 --no-futures
```

## Health checks

- `/health/live`
- `/health/ready`
- `/health/binance`
- `/metrics/summary`

## Replay

```bash
uv run --python 3.12 binance-market-monitor replay path/to/events.jsonl
```

Replay counts corrupt events, duplicate diffs, and gap invalidations without requiring live network access.

## Connectivity

Connectivity is opt-in and no-auth:

```bash
uv run --python 3.12 python scripts/check_connectivity.py --live --timeout 5
```

## Security constraints

Do not add account endpoints, credentials, signatures, order execution, proxy/VPN runtime dependencies, or Open Interest inference. Webhook URL must come from environment/config and is redacted from health output.
