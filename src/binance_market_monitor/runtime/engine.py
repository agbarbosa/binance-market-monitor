from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, cast

from binance_market_monitor.alerts.engine import (
    AlertEngine,
    AlertEngineConfig,
    AlertPayloadRecord,
    AlertSuppression,
)
from binance_market_monitor.alerts.webhook import AsyncWebhookDispatcher, WebhookConfig
from binance_market_monitor.api.server import ApiState
from binance_market_monitor.config import AppConfig
from binance_market_monitor.connectors.binance import (
    FUTURES_DEPTH_SNAPSHOT_WS_API_URL,
    BinanceConnectorError,
    FuturesDepthSnapshotClient,
    FuturesStreamRoute,
    FuturesWebSocketURLBuilder,
    SpotRestClient,
    SpotRestURLBuilder,
    SpotWebSocketURLBuilder,
)
from binance_market_monitor.orderbook.book import DepthDiff, DepthSnapshot, LocalOrderBook
from binance_market_monitor.queues import BoundedMarketQueue, QueueItem
from binance_market_monitor.scanner.stage1 import (
    MarketSample,
    Stage1CandidateRecord,
    Stage1FeatureConfig,
    calculate_stage1_candidate,
    rank_stage1_candidates,
)
from binance_market_monitor.scanner.stage2 import (
    FuturesContext,
    Stage2Input,
    TradePrint,
    compute_cvd,
    make_stage2_decision,
)
from binance_market_monitor.storage.parquet import ParquetEventStore
from binance_market_monitor.universe import (
    SpotSymbolMetadata,
    UniverseFilterConfig,
    select_spot_universe,
)


class RestTransport(Protocol):
    async def get_json(self, url: str, timeout_seconds: float) -> dict[str, Any]: ...


class WsRpcTransport(Protocol):
    async def request_json(
        self, url: str, payload: dict[str, Any], timeout_seconds: float
    ) -> dict[str, Any]: ...


class StreamTransport(Protocol):
    def stream_json(
        self,
        url: str,
        timeout_seconds: float,
        *,
        max_messages: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]: ...

    async def aclose(self) -> None: ...


@dataclass(frozen=True, slots=True)
class LiveRuntimeConfig:
    rest_timeout_seconds: float = 5.0
    ws_timeout_seconds: float = 10.0
    queue_maxsize: int = 2_000
    stage1_history_points: int = 1_200
    broad_stream_message_limit: int | None = None
    deep_stream_message_limit: int | None = None
    futures_enabled: bool = False
    warmup_min_trades: int = 3
    snapshot_limit: int = 100
    max_universe_symbols: int = 200
    api_bind_host: str = "127.0.0.1"
    stage1_evaluation_interval_seconds: float = 1.0
    stage2_evaluation_interval_seconds: float = 1.0


@dataclass(frozen=True, slots=True)
class LiveRunResult:
    shutdown_reason: str
    processed_messages: int
    candidates: int
    alerts: int
    storage_rows_written: int


@dataclass(slots=True)
class _BookTicker:
    bid: Decimal
    bid_qty: Decimal
    ask: Decimal
    ask_qty: Decimal
    updated_at: datetime


@dataclass(slots=True)
class _FuturesSymbolState:
    mark_price: Decimal | None = None
    funding_rate: Decimal | None = None
    book: LocalOrderBook | None = None
    degraded: bool = False


class LiveMonitorEngine:
    """Bounded async public-data-only live ingestion engine.

    Construction performs no network I/O. All network dependencies are injectable and the standard
    test suite uses fakes. `run_bounded()` powers tests/smoke runs; `run_forever()` powers CLI.
    """

    def __init__(
        self,
        *,
        app_config: AppConfig | None = None,
        runtime_config: LiveRuntimeConfig | None = None,
        api_state: ApiState | None = None,
        rest_transport: RestTransport,
        spot_stream_transport: StreamTransport,
        futures_stream_transport: StreamTransport | None = None,
        futures_rpc_transport: WsRpcTransport | None = None,
        webhook_transport: object | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.app_config = app_config or AppConfig()
        self.runtime_config = runtime_config or LiveRuntimeConfig()
        self.api_state = api_state or ApiState(storage_path=self.app_config.storage.base_path)
        self.rest_transport = rest_transport
        self.spot_stream_transport = spot_stream_transport
        self.futures_stream_transport = futures_stream_transport
        self.futures_rpc_transport = futures_rpc_transport
        self._now = now or (lambda: datetime.now(tz=UTC))
        self.spot_rest = SpotRestClient(
            transport=rest_transport,
            timeout_seconds=self.runtime_config.rest_timeout_seconds,
        )
        self.futures_snapshot = (
            FuturesDepthSnapshotClient(
                transport=futures_rpc_transport,
                timeout_seconds=self.runtime_config.ws_timeout_seconds,
            )
            if futures_rpc_transport is not None
            else None
        )
        self.storage = ParquetEventStore(self.app_config.storage.base_path)
        self.alert_engine = AlertEngine(
            AlertEngineConfig(cooldown_seconds=self.app_config.alerts.cooldown_seconds)
        )
        webhook_url = (
            self.app_config.webhook.url.get_secret_value()
            if self.app_config.webhook.url is not None
            else None
        )
        self.webhook = AsyncWebhookDispatcher(
            WebhookConfig(
                enabled=self.app_config.webhook.enabled,
                url=webhook_url,
                timeout_seconds=self.app_config.webhook.timeout_seconds,
                max_retries=self.app_config.webhook.max_retries,
            ),
            transport=cast(Any, webhook_transport),
        )
        self.depth_queue: BoundedMarketQueue[dict[str, Any]] = BoundedMarketQueue(
            maxsize=self.runtime_config.queue_maxsize,
            kind="depth",
            on_depth_overflow=self._mark_book_overflow,
        )
        self.trade_queue: BoundedMarketQueue[dict[str, Any]] = BoundedMarketQueue(
            maxsize=self.runtime_config.queue_maxsize,
            kind="trade",
        )
        self.histories: dict[str, deque[MarketSample]] = defaultdict(
            lambda: deque(maxlen=self.runtime_config.stage1_history_points)
        )
        self.book_tickers: dict[str, _BookTicker] = {}
        self.spot_books: dict[str, LocalOrderBook] = {}
        self.spot_book_updated_at: dict[str, datetime] = {}
        self.trades: dict[str, list[TradePrint]] = defaultdict(list)
        self.futures_trades: dict[str, list[TradePrint]] = defaultdict(list)
        self.futures: dict[str, _FuturesSymbolState] = defaultdict(_FuturesSymbolState)
        self.universe: list[str] = []
        self.deep_symbols: list[str] = []
        self._shutdown = False
        self._processed_messages = 0
        self._storage_rows_written = 0
        self._tasks: set[asyncio.Task[Any]] = set()
        self._stream_tasks: dict[tuple[str, str], asyncio.Task[Any]] = {}
        self._update_health("initialized")

    async def run_bounded(self) -> LiveRunResult:
        self._update_health("starting")
        await self.bootstrap()
        await self.consume_broad_spot_streams()
        candidates = await self.evaluate_stage1()
        self._set_deep_symbols(candidates)
        await self.consume_deep_spot_streams()
        if self.runtime_config.futures_enabled:
            await self.consume_futures_context(candidates)
        await self.evaluate_stage2(candidates)
        self._update_health("running")
        return LiveRunResult(
            shutdown_reason="bounded_complete",
            processed_messages=self._processed_messages,
            candidates=len(self.api_state.candidates),
            alerts=len(self.api_state.alerts),
            storage_rows_written=self._storage_rows_written,
        )

    async def run_forever(self, *, stop_event: asyncio.Event | None = None) -> LiveRunResult:
        stop = stop_event or asyncio.Event()
        self._update_health("starting")
        await self.bootstrap()
        self._start_broad_spot_tasks()
        self._update_health("running")
        try:
            while not stop.is_set() and not self._shutdown:
                candidates = await self.evaluate_stage1()
                self._set_deep_symbols(candidates)
                self._reconcile_spot_deep_tasks()
                if self.runtime_config.futures_enabled:
                    self._reconcile_futures_tasks(candidates)
                await self.evaluate_stage2(candidates)
                interval_seconds = min(
                    self.runtime_config.stage1_evaluation_interval_seconds,
                    self.runtime_config.stage2_evaluation_interval_seconds,
                )
                await _sleep_until_stop(stop, interval_seconds)
        finally:
            await self.shutdown()
        return LiveRunResult(
            shutdown_reason="stopped",
            processed_messages=self._processed_messages,
            candidates=len(self.api_state.candidates),
            alerts=len(self.api_state.alerts),
            storage_rows_written=self._storage_rows_written,
        )

    async def bootstrap(self) -> None:
        exchange_info = await self.spot_rest.exchange_info()
        symbols = _parse_exchange_symbols(exchange_info)
        quote_volumes = await self._load_quote_volumes()
        filter_result = select_spot_universe(
            symbols,
            quote_volumes=quote_volumes,
            config=UniverseFilterConfig(
                allowed_quote_assets=self.app_config.universe.eligible_quote_assets,
                min_quote_volume=Decimal(self.app_config.universe.min_quote_volume_usdt),
                min_history_minutes=self.app_config.universe.min_history_minutes,
                min_listing_age=timedelta(days=self.app_config.universe.min_listing_age_days),
                denied_symbols=self.app_config.universe.denied_symbols,
                now=self._now(),
            ),
            max_symbols=self.runtime_config.max_universe_symbols,
        )
        self.universe = [symbol.symbol for symbol in filter_result.included]
        self.api_state.health.update(
            {
                "universe_symbols": len(self.universe),
                "universe_symbol_names": list(self.universe),
                "universe_exclusion_counts": _count_values(filter_result.exclusion_reasons),
            }
        )
        self._update_metrics()

    async def consume_broad_spot_streams(self) -> None:
        spot_urls = SpotWebSocketURLBuilder()
        rolling_url = spot_urls.raw_stream("!ticker_1h@arr")
        book_url = spot_urls.raw_stream("!bookTicker")
        await asyncio.gather(
            self._consume_stream(
                rolling_url,
                self._handle_spot_rolling_ticker,
                self.spot_stream_transport,
                max_messages=self.runtime_config.broad_stream_message_limit,
            ),
            self._consume_stream(
                book_url,
                self._handle_spot_book_ticker,
                self.spot_stream_transport,
                max_messages=self.runtime_config.broad_stream_message_limit,
            ),
        )

    def _start_broad_spot_tasks(self) -> None:
        spot_urls = SpotWebSocketURLBuilder()
        self._ensure_stream_task(
            ("spot_broad", "rolling_ticker_1h"),
            self._consume_stream(
                spot_urls.raw_stream("!ticker_1h@arr"),
                self._handle_spot_rolling_ticker,
                self.spot_stream_transport,
                max_messages=None,
            ),
        )
        self._ensure_stream_task(
            ("spot_broad", "book_ticker"),
            self._consume_stream(
                spot_urls.raw_stream("!bookTicker"),
                self._handle_spot_book_ticker,
                self.spot_stream_transport,
                max_messages=None,
            ),
        )

    async def evaluate_stage1(self) -> list[Stage1CandidateRecord]:
        candidates: list[Stage1CandidateRecord] = []
        btc_samples = list(self.histories.get("BTCUSDT", []))
        now = self._now()
        for symbol in self.universe:
            ticker = self.book_tickers.get(symbol)
            if ticker is None:
                continue
            spread_bps = _spread_bps(ticker)
            candidate = calculate_stage1_candidate(
                symbol=symbol,
                samples=list(self.histories.get(symbol, [])),
                btc_samples=btc_samples,
                now=now,
                spread_bps=spread_bps,
                bid_depth_notional=ticker.bid * ticker.bid_qty,
                ask_depth_notional=ticker.ask * ticker.ask_qty,
                config=Stage1FeatureConfig(
                    min_history_points=3,
                    min_history_span_minutes=self.app_config.stage1.min_history_span_minutes,
                    window_sample_tolerance_seconds=(
                        self.app_config.stage1.window_sample_tolerance_seconds
                    ),
                    min_relative_volume=Decimal(str(self.app_config.stage1.min_relative_volume_5m)),
                    min_trade_acceleration=Decimal(
                        str(self.app_config.stage1.min_trade_acceleration_5m)
                    ),
                    min_price_move_5m=Decimal(str(self.app_config.stage1.min_price_move_5m)),
                    range_proximity_bps=Decimal(str(self.app_config.stage1.range_proximity_bps)),
                    max_spread_bps=Decimal(str(self.app_config.stage1.max_spread_bps)),
                    min_liquidity_notional=Decimal(self.app_config.stage2.min_depth_notional_usdt),
                ),
            )
            if candidate is not None:
                candidates.append(candidate)
        ranked = rank_stage1_candidates(candidates)[
            : self.app_config.universe.max_dynamic_candidates
        ]
        self.api_state.candidates = [_candidate_to_dict(candidate) for candidate in ranked]
        self._write_rows("stage1_candidates", self.api_state.candidates)
        self._update_metrics()
        return ranked

    async def consume_deep_spot_streams(self) -> None:
        tasks: list[asyncio.Task[Any]] = []
        for symbol in self.deep_symbols:
            tasks.extend(
                [
                    asyncio.create_task(self._sync_spot_book(symbol)),
                    asyncio.create_task(self._consume_symbol_trades(symbol)),
                    asyncio.create_task(self._consume_symbol_book_ticker(symbol)),
                    asyncio.create_task(self._consume_symbol_closed_klines(symbol)),
                ]
            )
        if tasks:
            await asyncio.gather(*tasks)

    def _reconcile_spot_deep_tasks(self) -> None:
        for symbol in self.deep_symbols:
            self._ensure_stream_task(("spot_depth", symbol), self._sync_spot_book(symbol))
            self._ensure_stream_task(("spot_trade", symbol), self._consume_symbol_trades(symbol))
            self._ensure_stream_task(
                ("spot_book_ticker", symbol), self._consume_symbol_book_ticker(symbol)
            )
            self._ensure_stream_task(
                ("spot_kline_closed", symbol), self._consume_symbol_closed_klines(symbol)
            )

    async def consume_futures_context(self, candidates: list[Stage1CandidateRecord]) -> None:
        if self.futures_stream_transport is None:
            for candidate in candidates:
                self.futures[candidate.symbol].degraded = True
            return
        await asyncio.gather(
            *(
                self._consume_futures_context_for_symbol(candidate.symbol)
                for candidate in candidates
            )
        )

    def _reconcile_futures_tasks(self, candidates: list[Stage1CandidateRecord]) -> None:
        if self.futures_stream_transport is None:
            for candidate in candidates:
                self.futures[candidate.symbol].degraded = True
            return
        for candidate in candidates:
            symbol = candidate.symbol
            self._ensure_stream_task(
                ("futures_context", symbol), self._consume_futures_context_for_symbol(symbol)
            )

    async def _consume_futures_context_for_symbol(self, symbol: str) -> None:
        tasks = [
            asyncio.create_task(self._sync_futures_book(symbol)),
            asyncio.create_task(self._consume_futures_trade(symbol)),
            asyncio.create_task(self._consume_futures_mark_price(symbol)),
        ]
        try:
            await asyncio.gather(*tasks)
        except Exception:  # noqa: BLE001 - optional Futures context degrades safely.
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.futures[symbol].degraded = True

    async def evaluate_stage2(self, candidates: list[Stage1CandidateRecord]) -> None:
        rows: list[dict[str, object]] = []
        alerts: list[dict[str, object]] = []
        for candidate in candidates:
            now = self._now()
            all_trades = self.trades.get(candidate.symbol, [])
            trades = _fresh_trades(
                all_trades,
                now=now,
                max_age_ms=self.app_config.stale_thresholds.aggregate_trade_ms,
            )
            self.trades[candidate.symbol] = trades
            trade_data_stale = bool(all_trades) and not trades
            spot_cvd = compute_cvd(trades).value
            decision = make_stage2_decision(
                Stage2Input(
                    candidate=candidate,
                    spot_cvd=spot_cvd,
                    futures_context=self._futures_context(candidate.symbol),
                    now=now,
                    min_evidence_groups=self.app_config.stage2.min_evidence_groups,
                    min_depth_notional=Decimal(self.app_config.stage2.min_depth_notional_usdt),
                )
            )
            row = _stage2_to_dict(decision)
            rows.append(row)
            book = self.spot_books.get(candidate.symbol)
            book_ticker = self.book_tickers.get(candidate.symbol)
            book_ticker_stale = book_ticker is None or (
                _datetime_age_ms(book_ticker.updated_at, now)
                > self.app_config.stale_thresholds.book_ticker_ms
            )
            depth_book_updated_at = self.spot_book_updated_at.get(candidate.symbol)
            depth_book_stale = depth_book_updated_at is None or (
                _datetime_age_ms(depth_book_updated_at, now)
                > self.app_config.stale_thresholds.depth_book_ms
            )
            maybe_alert = self.alert_engine.maybe_alert(
                decision,
                now=now,
                stale=(
                    candidate.input_freshness_ms.get("ticker", 0)
                    > self.app_config.stale_thresholds.ticker_ms
                    or trade_data_stale
                    or book_ticker_stale
                    or depth_book_stale
                ),
                invalid_book=book is None or not book.is_valid,
                warmup=len(trades) < self.runtime_config.warmup_min_trades,
                storage_available=True,
            )
            if isinstance(maybe_alert, AlertPayloadRecord):
                alert = maybe_alert.to_dict()
                alerts.append(alert)
                await self.webhook.send(maybe_alert)
            elif isinstance(maybe_alert, AlertSuppression):
                self.api_state.health["last_alert_suppression"] = maybe_alert.reason_codes
        self._write_rows("stage2_decisions", rows)
        self.api_state.alerts.extend(alerts)
        self._write_rows("alerts", alerts)
        self._update_metrics()

    async def shutdown(self) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.spot_stream_transport.aclose()
        if (
            self.futures_stream_transport is not None
            and self.futures_stream_transport is not self.spot_stream_transport
        ):
            await self.futures_stream_transport.aclose()
        self._update_health("stopped")

    async def _load_quote_volumes(self) -> dict[str, Decimal]:
        url = SpotRestURLBuilder().api_path("ticker/24hr")
        try:
            raw = await _get_json_value(
                self.rest_transport, url, self.runtime_config.rest_timeout_seconds
            )
        except Exception as exc:
            raise BinanceConnectorError("all-market 24h ticker request failed") from exc
        parsed = _parse_ticker_volumes(raw)
        if not parsed:
            raise BinanceConnectorError("all-market 24h ticker returned no usable quote volumes")
        return parsed

    async def _consume_stream(
        self,
        url: str,
        handler: Callable[[dict[str, Any]], None],
        transport: StreamTransport,
        *,
        max_messages: int | None,
    ) -> None:
        async for message in transport.stream_json(
            url,
            self.runtime_config.ws_timeout_seconds,
            max_messages=max_messages,
        ):
            handler(message)
            self._record_processed_message()

    def _handle_spot_rolling_ticker(self, message: dict[str, Any]) -> None:
        payload = message.get("data", message)
        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("s", "")).upper()
            if symbol not in self.universe and symbol != "BTCUSDT":
                continue
            price = _decimal(item.get("c"))
            volume = _decimal(item.get("q"))
            trade_count_value = _decimal(item.get("n"))
            if price is None or volume is None or trade_count_value is None:
                continue
            self.histories[symbol].append(
                MarketSample(
                    timestamp=_event_time(item, self._now()),
                    symbol=symbol,
                    price=price,
                    volume=volume,
                    trade_count=int(trade_count_value),
                )
            )

    def _handle_spot_book_ticker(self, message: dict[str, Any]) -> None:
        payload = message.get("data", message)
        if not isinstance(payload, dict):
            return
        symbol = str(payload.get("s", "")).upper()
        bid = _decimal(payload.get("b"))
        bid_qty = _decimal(payload.get("B"))
        ask = _decimal(payload.get("a"))
        ask_qty = _decimal(payload.get("A"))
        if not symbol or bid is None or bid_qty is None or ask is None or ask_qty is None:
            return
        self.book_tickers[symbol] = _BookTicker(
            bid,
            bid_qty,
            ask,
            ask_qty,
            _event_time(payload, self._now()),
        )

    def _handle_spot_closed_kline(self, message: dict[str, Any]) -> None:
        payload = message.get("data", message)
        if not isinstance(payload, dict):
            return
        kline = payload.get("k")
        if not isinstance(kline, dict) or not bool(kline.get("x")):
            return
        symbol = str(kline.get("s") or payload.get("s") or "").upper()
        if symbol not in self.universe and symbol != "BTCUSDT":
            return
        price = _decimal(kline.get("c"))
        volume = _decimal(kline.get("q") or kline.get("v"))
        if price is None or volume is None:
            return
        trade_count = int(_decimal(kline.get("n")) or 0)
        self.histories[symbol].append(
            MarketSample(
                timestamp=_event_time(payload, self._now()),
                symbol=symbol,
                price=price,
                volume=volume,
                trade_count=trade_count,
            )
        )

    def _set_deep_symbols(self, candidates: list[Stage1CandidateRecord]) -> None:
        symbols = [symbol.upper() for symbol in self.app_config.universe.permanent_deep_symbols]
        symbols.extend(candidate.symbol for candidate in candidates)
        self.deep_symbols = list(dict.fromkeys(symbols))

    async def _sync_spot_book(self, symbol: str) -> None:
        book = self.spot_books.get(symbol)
        if book is None or book.state != "warming" and not book.is_valid:
            book = LocalOrderBook(market="spot", symbol=symbol)
            self.spot_books[symbol] = book
            self.spot_book_updated_at.pop(symbol, None)
        self.api_state.books[("spot", symbol)] = book
        urls = SpotWebSocketURLBuilder()
        stream_url = urls.single_stream(symbol, "depth@100ms")
        async for message in self.spot_stream_transport.stream_json(
            stream_url,
            self.runtime_config.ws_timeout_seconds,
            max_messages=self.runtime_config.deep_stream_message_limit,
        ):
            received_at = self._now()
            diff = _spot_depth_diff(message, now=received_at)
            if diff is None:
                continue
            if book.state == "warming":
                book.buffer_diff(diff)
                await self._record_depth_queue_item("spot", symbol, message)
                self._record_processed_message()
                await self._apply_spot_snapshot(symbol, book)
                continue
            self._apply_spot_book_diff(symbol, book, diff, received_at=received_at)
            await self._record_depth_queue_item("spot", symbol, message)
            self._record_processed_message()
            if not book.is_valid:
                book = LocalOrderBook(market="spot", symbol=symbol)
                self.spot_books[symbol] = book
                self.api_state.books[("spot", symbol)] = book
                continue

    def _apply_spot_book_diff(
        self,
        symbol: str,
        book: LocalOrderBook,
        diff: DepthDiff,
        *,
        received_at: datetime,
    ) -> None:
        previous_update_id = book.last_update_id
        book.apply_diff(diff)
        if not book.is_valid:
            self.spot_book_updated_at.pop(symbol, None)
        elif book.last_update_id != previous_update_id:
            self.spot_book_updated_at[symbol] = received_at

    async def _apply_spot_snapshot(self, symbol: str, book: LocalOrderBook) -> None:
        snapshot_payload = await self.spot_rest.depth_snapshot(
            symbol, self.runtime_config.snapshot_limit
        )
        book.apply_snapshot(_depth_snapshot_from_payload(snapshot_payload))
        if book.is_valid:
            self.spot_book_updated_at[symbol] = self._now()
        else:
            self.spot_book_updated_at.pop(symbol, None)
        self.api_state.books[("spot", symbol)] = book

    async def _consume_symbol_trades(self, symbol: str) -> None:
        urls = SpotWebSocketURLBuilder()
        stream_url = urls.single_stream(symbol, "aggTrade")
        async for message in self.spot_stream_transport.stream_json(
            stream_url,
            self.runtime_config.ws_timeout_seconds,
            max_messages=self.runtime_config.deep_stream_message_limit,
        ):
            trade = _trade_from_agg(message, market="spot", now=self._now())
            if trade is not None:
                self.trades[symbol].append(trade)
                await self._record_trade_queue_item("spot", symbol, message)
                self._record_processed_message()

    async def _consume_symbol_book_ticker(self, symbol: str) -> None:
        urls = SpotWebSocketURLBuilder()
        stream_url = urls.single_stream(symbol, "bookTicker")
        async for message in self.spot_stream_transport.stream_json(
            stream_url,
            self.runtime_config.ws_timeout_seconds,
            max_messages=self.runtime_config.deep_stream_message_limit,
        ):
            self._handle_spot_book_ticker(message)
            self._record_processed_message()

    async def _consume_symbol_closed_klines(self, symbol: str) -> None:
        urls = SpotWebSocketURLBuilder()
        stream_url = urls.single_stream(symbol, "kline_1m")
        async for message in self.spot_stream_transport.stream_json(
            stream_url,
            self.runtime_config.ws_timeout_seconds,
            max_messages=self.runtime_config.deep_stream_message_limit,
        ):
            self._handle_spot_closed_kline(message)
            self._record_processed_message()

    async def _sync_futures_book(self, symbol: str) -> None:
        if self.futures_snapshot is None or self.futures_stream_transport is None:
            self.futures[symbol].degraded = True
            return
        state = self.futures[symbol]
        book = state.book
        if book is None or book.state != "warming" and not book.is_valid:
            book = LocalOrderBook(market="usd_m_futures", symbol=symbol)
            state.book = book
        self.api_state.books[("usd_m_futures", symbol)] = book
        urls = FuturesWebSocketURLBuilder()
        stream_url = urls.single_stream(FuturesStreamRoute.PUBLIC, f"{symbol.lower()}@depth@100ms")
        async for message in self.futures_stream_transport.stream_json(
            stream_url,
            self.runtime_config.ws_timeout_seconds,
            max_messages=self.runtime_config.deep_stream_message_limit,
        ):
            diff = _futures_depth_diff(message, now=self._now())
            if diff is None:
                continue
            if book.state == "warming":
                book.buffer_diff(diff)
                self._record_processed_message()
                await self._apply_futures_snapshot(symbol, book)
                continue
            book.apply_diff(diff)
            self._record_processed_message()
            if not book.is_valid:
                book = LocalOrderBook(market="usd_m_futures", symbol=symbol)
                state.book = book
                self.api_state.books[("usd_m_futures", symbol)] = book
                continue

    async def _apply_futures_snapshot(self, symbol: str, book: LocalOrderBook) -> None:
        if self.futures_snapshot is None:
            self.futures[symbol].degraded = True
            return
        response = await self.futures_snapshot.depth_snapshot(
            symbol, self.runtime_config.snapshot_limit
        )
        result = response.get("result", response)
        if not isinstance(result, dict):
            raise BinanceConnectorError("Futures snapshot response missing result object")
        book.apply_snapshot(_depth_snapshot_from_payload(result))
        self.futures[symbol].book = book
        self.api_state.books[("usd_m_futures", symbol)] = book

    async def _consume_futures_trade(self, symbol: str) -> None:
        if self.futures_stream_transport is None:
            return
        urls = FuturesWebSocketURLBuilder()
        stream_url = urls.single_stream(FuturesStreamRoute.MARKET, f"{symbol.lower()}@aggTrade")
        async for message in self.futures_stream_transport.stream_json(
            stream_url,
            self.runtime_config.ws_timeout_seconds,
            max_messages=self.runtime_config.deep_stream_message_limit,
        ):
            trade = _trade_from_agg(message, market="usd_m_futures", now=self._now())
            if trade is not None:
                self.futures_trades[symbol].append(trade)
                self._record_processed_message()

    async def _consume_futures_mark_price(self, symbol: str) -> None:
        if self.futures_stream_transport is None:
            return
        urls = FuturesWebSocketURLBuilder()
        stream_url = urls.single_stream(FuturesStreamRoute.MARKET, f"{symbol.lower()}@markPrice@1s")
        async for message in self.futures_stream_transport.stream_json(
            stream_url,
            self.runtime_config.ws_timeout_seconds,
            max_messages=self.runtime_config.deep_stream_message_limit,
        ):
            payload = message.get("data", message)
            if isinstance(payload, dict):
                self.futures[symbol].mark_price = _decimal(payload.get("p"))
                self.futures[symbol].funding_rate = _decimal(payload.get("r"))
                self._record_processed_message()

    def _futures_context(self, symbol: str) -> FuturesContext | None:
        state = self.futures.get(symbol)
        ticker = self.book_tickers.get(symbol)
        if state is None or state.book is None or ticker is None or state.mark_price is None:
            return None
        metrics = state.book.metrics(depth_bps=100)
        bid_depth = cast(Decimal, metrics["bid_depth_notional"])
        ask_depth = cast(Decimal, metrics["ask_depth_notional"])
        return FuturesContext.from_public_data(
            spot_price=(ticker.bid + ticker.ask) / Decimal("2"),
            mark_price=state.mark_price,
            funding_rate=state.funding_rate,
            liquidations=[],
            bid_depth_notional=bid_depth,
            ask_depth_notional=ask_depth,
        )

    def _ensure_stream_task(self, key: tuple[str, str], coro: Coroutine[Any, Any, None]) -> None:
        existing = self._stream_tasks.get(key)
        if existing is not None and not existing.done():
            coro.close()
            return
        task = asyncio.create_task(coro)
        self._stream_tasks[key] = task
        self._tasks.add(task)

        def _discard(done_task: asyncio.Task[Any]) -> None:
            self._tasks.discard(done_task)
            if self._stream_tasks.get(key) is done_task:
                self._stream_tasks.pop(key, None)
            if not done_task.cancelled() and done_task.exception() is not None:
                self.api_state.health["last_stream_error"] = str(done_task.exception())

        task.add_done_callback(_discard)

    async def _record_depth_queue_item(
        self, market: str, symbol: str, message: dict[str, Any]
    ) -> None:
        accepted = await self.depth_queue.put(
            QueueItem(market, symbol, cast(dict[str, Any], message.get("data", message)))
        )
        if accepted:
            await self.depth_queue.get()

    async def _record_trade_queue_item(
        self, market: str, symbol: str, message: dict[str, Any]
    ) -> None:
        accepted = await self.trade_queue.put(
            QueueItem(market, symbol, cast(dict[str, Any], message.get("data", message)))
        )
        if accepted:
            await self.trade_queue.get()

    def _record_processed_message(self) -> None:
        self._processed_messages += 1
        self._update_metrics()

    def _write_rows(self, table: str, rows: list[dict[str, object]]) -> None:
        if not rows:
            return
        self.storage.write_rows(table, rows)
        self._storage_rows_written += len(rows)

    def _mark_book_overflow(self, market: str, symbol: str, reason: str) -> None:
        book = self.api_state.books.get((market, symbol))
        if book is not None:
            book.invalidate(reason)
        if market == "spot":
            self.spot_book_updated_at.pop(symbol, None)

    def _update_health(self, status: str) -> None:
        self.api_state.health.update(
            {
                "status": status,
                "updated_at": self._now().isoformat(),
                "futures_enabled": self.runtime_config.futures_enabled,
                "deep_symbol_names": list(self.deep_symbols),
            }
        )

    def _update_metrics(self) -> None:
        self.api_state.metrics.update(
            {
                "processed_messages": self._processed_messages,
                "depth_queue_size": self.depth_queue.qsize,
                "trade_queue_size": self.trade_queue.qsize,
                "books": len(self.api_state.books),
                "candidates": len(self.api_state.candidates),
                "alerts": len(self.api_state.alerts),
            }
        )


def _parse_exchange_symbols(payload: dict[str, Any]) -> list[SpotSymbolMetadata]:
    raw_symbols = payload.get("symbols", [])
    if not isinstance(raw_symbols, list):
        return []
    symbols: list[SpotSymbolMetadata] = []
    for item in raw_symbols:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).upper()
        base = str(item.get("baseAsset", "")).upper()
        quote = str(item.get("quoteAsset", "")).upper()
        status = str(item.get("status", "")).upper()
        if symbol and base and quote:
            symbols.append(
                SpotSymbolMetadata(
                    symbol=symbol,
                    base_asset=base,
                    quote_asset=quote,
                    status=status,
                    listed_at=None,
                    history_minutes=10_000,
                )
            )
    return symbols


def _count_values(values: dict[str, str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values.values():
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


async def _get_json_value(transport: RestTransport, url: str, timeout_seconds: float) -> object:
    method = getattr(transport, "get_json_value", None)
    if method is not None:
        return await cast(Any, method)(url, timeout_seconds)
    return await transport.get_json(url, timeout_seconds)


async def _sleep_until_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        return


def _parse_ticker_volumes(raw: object) -> dict[str, Decimal]:
    if isinstance(raw, dict) and "items" in raw:
        raw = raw["items"]
    if isinstance(raw, dict):
        symbol = str(raw.get("symbol", "")).upper()
        volume = _decimal(raw.get("quoteVolume"))
        return {symbol: volume} if symbol and volume is not None else {}
    if isinstance(raw, list):
        volumes: dict[str, Decimal] = {}
        for item in raw:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "")).upper()
            volume = _decimal(item.get("quoteVolume"))
            if symbol and volume is not None:
                volumes[symbol] = volume
        return volumes
    return {}


def _spot_depth_diff(message: dict[str, Any], *, now: datetime) -> DepthDiff | None:
    payload = message.get("data", message)
    if not isinstance(payload, dict):
        return None
    first = _int(payload.get("U"))
    final = _int(payload.get("u"))
    if first is None or final is None:
        return None
    return DepthDiff(
        first_update_id=first,
        final_update_id=final,
        bids=cast(list[tuple[str, str]], payload.get("b", [])),
        asks=cast(list[tuple[str, str]], payload.get("a", [])),
        event_age_ms=_age_ms(payload.get("E"), now),
    )


def _futures_depth_diff(message: dict[str, Any], *, now: datetime) -> DepthDiff | None:
    payload = message.get("data", message)
    if not isinstance(payload, dict):
        return None
    first = _int(payload.get("U"))
    final = _int(payload.get("u"))
    previous = _int(payload.get("pu"))
    if first is None or final is None:
        return None
    return DepthDiff(
        first_update_id=first,
        final_update_id=final,
        previous_final_update_id=previous,
        bids=cast(list[tuple[str, str]], payload.get("b", [])),
        asks=cast(list[tuple[str, str]], payload.get("a", [])),
        event_age_ms=_age_ms(payload.get("E"), now),
    )


def _depth_snapshot_from_payload(payload: dict[str, Any]) -> DepthSnapshot:
    last_update_id = _int(payload.get("lastUpdateId")) or _int(payload.get("last_update_id"))
    if last_update_id is None:
        raise BinanceConnectorError("depth snapshot missing lastUpdateId")
    return DepthSnapshot(
        last_update_id=last_update_id,
        bids=cast(list[tuple[str, str]], payload.get("bids", [])),
        asks=cast(list[tuple[str, str]], payload.get("asks", [])),
    )


def _trade_from_agg(message: dict[str, Any], *, market: str, now: datetime) -> TradePrint | None:
    payload = message.get("data", message)
    if not isinstance(payload, dict):
        return None
    price = _decimal(payload.get("p"))
    quantity = _decimal(payload.get("q"))
    if price is None or quantity is None:
        return None
    return TradePrint(
        timestamp=_event_time(payload, now),
        price=price,
        quantity=quantity,
        is_buyer_maker=bool(payload.get("m")) if "m" in payload else None,
        market=cast(Any, market),
    )


def _event_time(payload: dict[str, Any], fallback: datetime) -> datetime:
    event_ms = _int(payload.get("E"))
    if event_ms is None:
        data = payload.get("data")
        if isinstance(data, dict):
            event_ms = _int(data.get("E"))
    if event_ms is None:
        return fallback if fallback.tzinfo else fallback.replace(tzinfo=UTC)
    return datetime.fromtimestamp(event_ms / 1000, tz=UTC)


def _age_ms(event_ms: object, now: datetime) -> int:
    parsed = _int(event_ms)
    if parsed is None:
        return 0
    event_time = datetime.fromtimestamp(parsed / 1000, tz=UTC)
    return max(0, int((now.astimezone(UTC) - event_time).total_seconds() * 1000))


def _datetime_age_ms(timestamp: datetime, now: datetime) -> int:
    timestamp_utc = timestamp.astimezone(UTC) if timestamp.tzinfo else timestamp.replace(tzinfo=UTC)
    now_utc = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
    return max(0, int((now_utc - timestamp_utc).total_seconds() * 1000))


def _fresh_trades(trades: list[TradePrint], *, now: datetime, max_age_ms: int) -> list[TradePrint]:
    now_utc = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
    fresh: list[TradePrint] = []
    for trade in trades:
        timestamp = (
            trade.timestamp.astimezone(UTC)
            if trade.timestamp.tzinfo
            else trade.timestamp.replace(tzinfo=UTC)
        )
        age_ms = int((now_utc - timestamp).total_seconds() * 1000)
        if 0 <= age_ms <= max_age_ms:
            fresh.append(trade)
    return fresh


def _spread_bps(ticker: _BookTicker) -> Decimal:
    if ticker.bid <= 0:
        return Decimal("999999")
    return (ticker.ask - ticker.bid) / ticker.bid * Decimal("10000")


def _candidate_to_dict(candidate: Stage1CandidateRecord) -> dict[str, object]:
    return {
        "candidate_id": candidate.candidate_id,
        "event_time": candidate.detected_at,
        "detected_at": candidate.detected_at.isoformat(),
        "symbol": candidate.symbol,
        "score": str(candidate.score),
        "score_version": candidate.score_version,
        "executable_liquidity_score": str(candidate.executable_liquidity_score),
        "features": {key: str(value) for key, value in candidate.features.items()},
        "thresholds": {key: str(value) for key, value in candidate.thresholds.items()},
        "reason_codes": candidate.reason_codes,
        "input_freshness_ms": candidate.input_freshness_ms,
    }


def _stage2_to_dict(decision: Any) -> dict[str, object]:
    return {
        "decision_id": decision.decision_id,
        "event_time": decision.calculated_at,
        "calculated_at": decision.calculated_at.isoformat(),
        "symbol": decision.symbol,
        "candidate_id": decision.candidate_id,
        "decision": decision.decision,
        "evidence_groups": decision.evidence_groups,
        "evidence_confidence": decision.evidence_confidence,
        "features": {key: str(value) for key, value in decision.features.items()},
        "thresholds": {key: str(value) for key, value in decision.thresholds.items()},
        "reason_codes": decision.reason_codes,
        "open_interest_available": False,
    }


def _decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def runtime_config_from_app(app_config: AppConfig) -> LiveRuntimeConfig:
    runtime = getattr(app_config, "runtime", None)
    if runtime is None:
        return LiveRuntimeConfig(api_bind_host=app_config.api.bind_host)
    return LiveRuntimeConfig(
        rest_timeout_seconds=runtime.rest_timeout_seconds,
        ws_timeout_seconds=runtime.ws_timeout_seconds,
        queue_maxsize=runtime.queue_maxsize,
        stage1_history_points=runtime.stage1_history_points,
        broad_stream_message_limit=runtime.broad_stream_message_limit,
        deep_stream_message_limit=runtime.deep_stream_message_limit,
        futures_enabled=runtime.futures_enabled,
        warmup_min_trades=runtime.warmup_min_trades,
        snapshot_limit=runtime.snapshot_limit,
        max_universe_symbols=runtime.max_universe_symbols,
        api_bind_host=app_config.api.bind_host,
        stage1_evaluation_interval_seconds=runtime.stage1_evaluation_interval_seconds,
        stage2_evaluation_interval_seconds=runtime.stage2_evaluation_interval_seconds,
    )


__all__ = [
    "FUTURES_DEPTH_SNAPSHOT_WS_API_URL",
    "LiveMonitorEngine",
    "LiveRunResult",
    "LiveRuntimeConfig",
    "runtime_config_from_app",
]
