# Binance Opportunities Hermes Plugin

Hermes personal plugin for the Binance Market Monitor functional MVP.

It registers one tool:

- `binance_opportunities_list`

The tool reads only the monitor's local read-only HTTP API:

- `GET /candidates/current`
- `GET /alerts/recent`
- `GET /health/binance`
- `GET /metrics/summary`

It consolidates current Stage1 candidates with the latest alert for each symbol, includes recent alert-only symbols, sorts confirmed watches first, and returns JSON with health, metrics, opportunities, and a financial-advice disclaimer.

## Safety model

- stdlib-only plugin code.
- Compatible with Hermes Python 3.11.
- Does not import `binance_market_monitor`, which requires Python 3.12.
- Uses GET only.
- Default API base URL is fixed to `http://127.0.0.1:8000`.
- Optional `BMM_HERMES_API_URL` is accepted only for `http` loopback hosts: `127.0.0.1`, `localhost`, or `::1`.
- Rejects non-loopback hosts and non-HTTP schemes.
- Uses a short timeout and response-size cap.
- Rejects redirects instead of following them.
- No credentials, trading endpoint usage, user portfolio access, trade placement, external routing, or Open Interest lookup.

## Start the monitor API

From the repository root:

```bash
uv run --python 3.12 binance-market-monitor monitor \
  --config configs/config.example.yaml \
  --serve-api \
  --no-futures
```

The tool returns a JSON `api_unavailable` error with this command as remediation if the API is not running.

## Install in Hermes by symlink

Assuming this repository is at `/root/binance-market-monitor` and Hermes profile home is `~/.hermes`:

```bash
mkdir -p ~/.hermes/plugins
ln -sfn /root/binance-market-monitor/integrations/hermes_plugins/binance_opportunities \
  ~/.hermes/plugins/binance-opportunities
hermes plugins enable binance-opportunities
```

If using the `senior-dev` profile explicitly, install into that profile home instead:

```bash
mkdir -p ~/.hermes/profiles/senior-dev/plugins
ln -sfn /root/binance-market-monitor/integrations/hermes_plugins/binance_opportunities \
  ~/.hermes/profiles/senior-dev/plugins/binance-opportunities
hermes --profile senior-dev plugins enable binance-opportunities
```

Restart Hermes CLI or gateway after enabling the plugin:

```bash
hermes gateway restart
```

## Example Hermes prompt

```text
Liste as oportunidades Binance atuais. Traga só confirmadas, limite 10, com score watch mínimo 70, e explique os reason_codes em linguagem simples.
```

Hermes should call `binance_opportunities_list` with arguments similar to:

```json
{
  "limit": 10,
  "min_stage1_score": 0,
  "min_watch_score": 70,
  "confirmed_only": true
}
```
