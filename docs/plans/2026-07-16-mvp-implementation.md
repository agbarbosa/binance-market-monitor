# Binance Market Monitor Functional MVP Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Build a runnable read-only Binance Market Monitor MVP that satisfies the PRD with offline tests, replay, storage, FastAPI, Docker, and an opt-in connectivity script.

**Architecture:** Modular monolith under `src/binance_market_monitor`, with connector interfaces isolated from deterministic domain logic. Async ingestion uses bounded queues and explicit overflow policies; scanner, order-book, alert, storage, and replay code are deterministic and testable offline. External Binance access is limited to approved public no-auth endpoints and is never required by default tests.

**Tech Stack:** Python 3.12+ metadata, Pydantic v2, asyncio, httpx, websockets, DuckDB, PyArrow/Parquet ZSTD, FastAPI/Uvicorn, Typer CLI, pytest, ruff, mypy, Docker.

---

## Acceptance guardrails

- Stay on branch `feat/functional-mvp`; never merge or push.
- Do not add Binance API keys, signed endpoints, account clients, order/execution models, proxy/VPN logic, or Open Interest inference.
- Keep default runtime localhost-only (`127.0.0.1`).
- Keep default tests offline; live connectivity script must be opt-in.
- Store webhook URL only via environment variable; never log or expose it.

---

### Task 1: Project metadata, config, and contracts tests

**Objective:** Define reproducible project metadata and write failing tests for typed config and versioned contracts.

**Files:**
- Create: `pyproject.toml`
- Create: `configs/config.example.yaml`
- Create: `.env.example`
- Create: `tests/unit/test_config.py`
- Create: `tests/unit/test_contracts.py`

**Step 1: Write failing tests**
- Test config validation loads YAML, overrides webhook URL from env, redacts secrets, rejects public unauthenticated bind host.
- Test contracts serialize decimal values as strings and generate JSON Schema for every PRD contract.

**Step 2: Run tests to verify failure**
Run: `uv run pytest tests/unit/test_config.py tests/unit/test_contracts.py -q`
Expected: FAIL because package and models do not exist.

**Step 3: Implement minimal code**
- Add `binance_market_monitor.config` with Pydantic Settings-like YAML/env loader.
- Add `binance_market_monitor.contracts.models` with normalized events, candidates, decisions, alerts, health, and review records.
- Add `binance_market_monitor.contracts.schema` to write schema files.

**Step 4: Run tests to verify pass**
Run: `uv run pytest tests/unit/test_config.py tests/unit/test_contracts.py -q`
Expected: PASS.

**Step 5: Commit**
`git add -A && git commit -m "feat: add typed config and contracts"`

---

### Task 2: Universe filtering and connector interfaces

**Objective:** Add approved public connector URL builders and deterministic Spot universe filtering.

**Files:**
- Create: `src/binance_market_monitor/connectors/binance.py`
- Create: `src/binance_market_monitor/universe.py`
- Create: `tests/unit/test_universe_and_connectors.py`

**TDD cycle:**
1. Test that Spot REST URLs use `data-api.binance.vision`, Spot WS uses `data-stream.binance.vision`, Futures split WS paths include `/market/ws/`, `/market/stream?streams=`, `/public/ws/`, `/public/stream?streams=`, and depth snapshots use WS-API.
2. Test that stablecoin-stablecoin, leveraged-token suffixes, denied symbols, non-trading symbols, and low-volume markets are excluded.
3. Run failing tests.
4. Implement connector protocols/clients with dependency-injected transports and the universe filter.
5. Run passing tests and commit `feat: add public connectors and universe filters`.

---

### Task 3: Local order books and backpressure

**Objective:** Implement deterministic Spot/Futures local-book reconstruction and explicit bounded queue overflow policies.

**Files:**
- Create: `src/binance_market_monitor/orderbook/book.py`
- Create: `src/binance_market_monitor/queues.py`
- Create: `tests/unit/test_orderbook.py`
- Create: `tests/replay/test_backpressure.py`

**TDD cycle:**
1. Test Spot snapshot bridge `U <= lastUpdateId + 1 <= u`, obsolete update ignore, gap invalidation, zero quantity deletion, crossed-book invalidation, and Decimal canonical state.
2. Test Futures bridge `U <= lastUpdateId <= u` and `pu == prior u` for subsequent events.
3. Test depth queue overflow invalidates affected books and trade shedding marks loss.
4. Run failing tests.
5. Implement book state machine and backpressure queues.
6. Run tests and commit `feat: add deterministic order books and backpressure`.

---

### Task 4: Stage 1 features, ranking, and candidate management

**Objective:** Build lightweight Stage 1 features, deterministic ranking, and hysteresis-based deep candidate selection.

**Files:**
- Create: `src/binance_market_monitor/scanner/stage1.py`
- Create: `src/binance_market_monitor/scanner/candidates.py`
- Create: `tests/unit/test_stage1.py`

**TDD cycle:**
1. Test return, volatility-normalized impulse, relative volume, trade-count acceleration, spread gate, BTC-relative movement, reason codes, thresholds, stale/liquidity suppression.
2. Test ranking tie-break: score desc, liquidity desc, symbol asc, detection timestamp asc.
3. Test permanent symbols, max dynamic candidates, minimum residence time, warm-up state.
4. Run failing tests.
5. Implement deterministic feature functions and candidate manager.
6. Run tests and commit `feat: add stage one scanner and candidate manager`.

---

### Task 5: Stage 2, Futures context, and alert engine

**Objective:** Add CVD/liquidity/Futures context calculations and explainable multi-factor alert decisions.

**Files:**
- Create: `src/binance_market_monitor/scanner/stage2.py`
- Create: `src/binance_market_monitor/alerts/engine.py`
- Create: `src/binance_market_monitor/alerts/webhook.py`
- Create: `tests/unit/test_stage2_alerts.py`
- Create: `tests/integration/test_webhook.py`

**TDD cycle:**
1. Test maker-side aggressor classification and CVD approximation labeling.
2. Test basis, funding context, liquidation metrics, `open_interest_available: false`, and degraded missing Futures context.
3. Test no single-indicator market alert, reason codes, forbidden execution wording, stale/invalid/warm-up suppression, cooldown/dedupe/continuation.
4. Test webhook retries, idempotency key, timeout/failure persistence, and disabled mode.
5. Run failing tests.
6. Implement Stage 2 decisions, alert engine, and webhook dispatcher.
7. Run tests and commit `feat: add stage two alerts and webhook delivery`.

---

### Task 6: Storage, replay, FastAPI, CLI, and operations

**Objective:** Make the service executable with Parquet/DuckDB storage, offline replay, required API endpoints, CLI, docs, and Docker.

**Files:**
- Create: `src/binance_market_monitor/storage/parquet.py`
- Create: `src/binance_market_monitor/replay/runner.py`
- Create: `src/binance_market_monitor/api/server.py`
- Create: `src/binance_market_monitor/app.py`
- Create: `src/binance_market_monitor/cli.py`
- Create: `tests/integration/test_storage_api.py`
- Create: `tests/replay/test_replay.py`
- Create: `scripts/check_connectivity.py`
- Create/Modify: `README.md`, `docs/architecture.md`, `docs/data-contracts.md`, `docs/operational-runbook.md`, `Dockerfile`, `docker-compose.yml`

**TDD cycle:**
1. Test Parquet ZSTD writes partitioned rows and DuckDB queries views.
2. Test replay produces content-equivalent candidates and alerts, duplicate diffs do not corrupt books, corrupted events are counted not fatal.
3. Test FastAPI endpoints: `/health/live`, `/health/ready`, `/health/binance`, `/metrics/summary`, `/candidates/current`, `/books/{market}/{symbol}`, `/alerts/recent`; no secrets exposed; localhost default.
4. Test CLI schema/replay/server commands where practical.
5. Run failing tests.
6. Implement storage, replay, API, app state, CLI, connectivity script, docs, and Docker files.
7. Run tests and commit `feat: add storage replay api and operations`.

---

### Task 7: Final verification and review

**Objective:** Prove the artifact is executable and safe.

**Commands:**
- `uv lock`
- `uv run pytest -q`
- `uv run ruff check .`
- `uv run mypy src`
- `uv build`
- `docker build -t binance-market-monitor:mvp .` if Docker daemon is available
- Optional smoke: `uv run python scripts/check_connectivity.py --live --timeout 5` if network allows.

**Review checklist:**
- No secrets, signed endpoints, account/order/proxy code, or Open Interest inference.
- Standard `api.binance.com`, `stream.binance.com`, and `fapi.binance.com` are not runtime dependencies.
- Alerts use neutral watch wording and include disclaimer.
- Health responses redact webhook URL.
- Branch clean and local commits only.

**Commit:**
- Commit any fixes with conventional messages.
