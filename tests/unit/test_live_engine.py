from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from binance_market_monitor.api.server import ApiState
from binance_market_monitor.config import AppConfig
from binance_market_monitor.connectors.binance import BinanceConnectorError
from binance_market_monitor.orderbook.book import DepthDiff, DepthSnapshot, LocalOrderBook
from binance_market_monitor.runtime import engine as engine_module
from binance_market_monitor.runtime.engine import LiveMonitorEngine, LiveRuntimeConfig
from binance_market_monitor.scanner.stage1 import Stage1CandidateRecord
from binance_market_monitor.scanner.stage2 import TradePrint


class FakeRestTransport:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get_json(self, url: str, timeout_seconds: float) -> dict[str, Any]:
        self.calls.append(url)
        if "exchangeInfo" in url:
            return {
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                    },
                    {
                        "symbol": "ETHUSDT",
                        "baseAsset": "ETH",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                    },
                    {
                        "symbol": "FOOUSDT",
                        "baseAsset": "FOO",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                    },
                    {
                        "symbol": "LOWUSDT",
                        "baseAsset": "LOW",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                    },
                ]
            }
        if "ticker/24hr" in url:
            return {
                "items": [
                    {"symbol": "BTCUSDT", "quoteVolume": "500000000"},
                    {"symbol": "ETHUSDT", "quoteVolume": "400000000"},
                    {"symbol": "FOOUSDT", "quoteVolume": "50000000"},
                    {"symbol": "LOWUSDT", "quoteVolume": "10"},
                ]
            }
        if "depth" in url and "FOOUSDT" in url:
            return {"lastUpdateId": 100, "bids": [["10.00", "10000"]], "asks": [["10.01", "10000"]]}
        if "depth" in url:
            return {"lastUpdateId": 100, "bids": [["100", "1000"]], "asks": [["101", "1000"]]}
        raise AssertionError(f"unexpected REST URL {url}")


class FakeStreamTransport:
    def __init__(self) -> None:
        self.closed = False
        self.urls: list[str] = []

    async def stream_json(
        self,
        url: str,
        timeout_seconds: float,
        *,
        max_messages: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        self.urls.append(url)
        messages = _messages_for_url(url)
        for yielded, message in enumerate(messages):
            if max_messages is not None and yielded >= max_messages:
                return
            yield message

    async def aclose(self) -> None:
        self.closed = True


class FakeFuturesRpc:
    async def request_json(
        self, url: str, payload: dict[str, Any], timeout_seconds: float
    ) -> dict[str, Any]:
        assert url == "wss://ws-fapi.binance.com/ws-fapi/v1"
        assert payload["method"] == "depth"
        return {
            "status": 200,
            "result": {
                "lastUpdateId": 200,
                "bids": [["10.00", "20000"]],
                "asks": [["10.02", "20000"]],
            },
        }


class CountingRestTransport(FakeRestTransport):
    def __init__(self, *, snapshots: list[dict[str, Any]] | None = None) -> None:
        super().__init__()
        self.snapshots = list(snapshots or [])
        self.exchange_info_calls = 0
        self.depth_snapshot_calls = 0

    async def get_json(self, url: str, timeout_seconds: float) -> dict[str, Any]:
        if "exchangeInfo" in url:
            self.exchange_info_calls += 1
        if "depth" in url:
            self.depth_snapshot_calls += 1
            if self.snapshots:
                return self.snapshots.pop(0)
        return await super().get_json(url, timeout_seconds)


class FailingFuturesStreamTransport(FakeStreamTransport):
    async def stream_json(
        self,
        url: str,
        timeout_seconds: float,
        *,
        max_messages: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        if False:  # pragma: no cover - keeps this function typed as an async generator.
            yield {}
        raise RuntimeError(f"futures stream unavailable: {url}")


class FailingFuturesRpc:
    async def request_json(
        self, url: str, payload: dict[str, Any], timeout_seconds: float
    ) -> dict[str, Any]:
        raise RuntimeError("futures snapshot unavailable")


_STOP = object()


class InteractiveStreamTransport:
    def __init__(self) -> None:
        self.closed = False
        self.urls: list[str] = []
        self._queues: dict[str, asyncio.Queue[dict[str, Any] | object]] = {}

    async def stream_json(
        self,
        url: str,
        timeout_seconds: float,
        *,
        max_messages: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        self.urls.append(url)
        queue = self._queues.setdefault(url, asyncio.Queue())
        yielded = 0
        while not self.closed:
            if max_messages is not None and yielded >= max_messages:
                return
            message = await queue.get()
            if message is _STOP:
                return
            yielded += 1
            if isinstance(message, dict):
                yield message

    async def aclose(self) -> None:
        self.closed = True
        for queue in self._queues.values():
            queue.put_nowait(_STOP)

    async def wait_for_url(self, needle: str) -> str:
        for _ in range(100):
            for url in self.urls:
                if needle in url:
                    return url
            await asyncio.sleep(0.01)
        raise AssertionError(f"stream URL containing {needle!r} was not opened; opened={self.urls}")

    async def push(self, needle: str, message: dict[str, Any]) -> None:
        url = await self.wait_for_url(needle)
        self._queues.setdefault(url, asyncio.Queue()).put_nowait(message)


class ScriptedStreamTransport:
    def __init__(self, messages_by_needle: dict[str, list[dict[str, Any]]]) -> None:
        self.messages_by_needle = messages_by_needle
        self.closed = False
        self.urls: list[str] = []

    async def stream_json(
        self,
        url: str,
        timeout_seconds: float,
        *,
        max_messages: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        self.urls.append(url)
        messages = next(
            (items for needle, items in self.messages_by_needle.items() if needle in url), []
        )
        for yielded, message in enumerate(messages):
            if max_messages is not None and yielded >= max_messages:
                return
            yield message

    async def aclose(self) -> None:
        self.closed = True


def _test_config(tmp_path: Path) -> AppConfig:
    return AppConfig.model_validate(
        {
            "universe": {
                "max_dynamic_candidates": 1,
                "min_quote_volume_usdt": "1000",
                "min_history_minutes": 0,
                "min_listing_age_days": 0,
            },
            "stage1": {"min_quote_volume_usdt": "1000", "max_spread_bps": 20},
            "storage": {"base_path": str(tmp_path)},
        }
    )


async def _wait_for_candidate(api_state: ApiState, *, timeout_seconds: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        if api_state.candidates:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("candidate was not produced before timeout")


async def _feed_stage1_messages(stream: InteractiveStreamTransport) -> None:
    for message in _messages_for_url("!ticker_1h@arr"):
        await stream.push("!ticker_1h@arr", message)
    for message in _messages_for_url("!bookticker"):
        await stream.push("!bookticker", message)


def _messages_for_url(url: str) -> list[dict[str, Any]]:
    if "!ticker_1h@arr" in url:
        return [
            {"data": [{"s": "BTCUSDT", "E": 1_784_159_100_000, "c": "99", "q": "800", "n": 8}]},
            {"data": [{"s": "BTCUSDT", "E": 1_784_159_400_000, "c": "100", "q": "1000", "n": 10}]},
            {"data": [{"s": "BTCUSDT", "E": 1_784_159_700_000, "c": "101", "q": "1100", "n": 11}]},
            {"data": [{"s": "BTCUSDT", "E": 1_784_160_000_000, "c": "101", "q": "1200", "n": 12}]},
            {"data": [{"s": "FOOUSDT", "E": 1_784_159_100_000, "c": "9", "q": "50", "n": 5}]},
            {"data": [{"s": "FOOUSDT", "E": 1_784_159_400_000, "c": "10", "q": "100", "n": 10}]},
            {"data": [{"s": "FOOUSDT", "E": 1_784_159_700_000, "c": "12", "q": "300", "n": 30}]},
            {"data": [{"s": "FOOUSDT", "E": 1_784_160_000_000, "c": "15", "q": "900", "n": 90}]},
        ]
    if "!bookticker" in url:
        return [
            {"data": {"s": "BTCUSDT", "b": "100", "B": "1000", "a": "101", "A": "1000"}},
            {"data": {"s": "FOOUSDT", "b": "10.00", "B": "10000", "a": "10.01", "A": "10000"}},
        ]
    if "foousdt@depth" in url:
        return [
            {"data": {"U": 100, "u": 101, "b": [["10.00", "10001"]], "a": [["10.01", "10002"]]}}
        ]
    if "foousdt@aggtrade" in url:
        return [{"data": {"E": 1_786_000_000_000, "p": "15", "q": "10", "m": False}}]
    if "foousdt@markprice" in url:
        return [{"data": {"E": 1_786_000_000_000, "p": "15.20", "r": "0.0001"}}]
    if "foousdt@depth" in url and "fstream" in url:
        return [
            {
                "data": {
                    "U": 201,
                    "u": 201,
                    "pu": 200,
                    "b": [["10.00", "20001"]],
                    "a": [["10.02", "20002"]],
                }
            }
        ]
    return []


@pytest.mark.asyncio
async def test_live_engine_runs_bounded_offline_cycle_and_updates_api_state(tmp_path: Path) -> None:
    config = AppConfig.model_validate(
        {
            "universe": {
                "max_dynamic_candidates": 1,
                "min_quote_volume_usdt": "1000",
                "min_history_minutes": 0,
                "min_listing_age_days": 0,
            },
            "stage1": {"min_quote_volume_usdt": "1000", "max_spread_bps": 20},
            "storage": {"base_path": str(tmp_path)},
        }
    )
    api_state = ApiState(storage_path=tmp_path)
    stream = FakeStreamTransport()
    engine = LiveMonitorEngine(
        app_config=config,
        runtime_config=LiveRuntimeConfig(
            broad_stream_message_limit=8,
            deep_stream_message_limit=1,
            futures_enabled=True,
            warmup_min_trades=1,
        ),
        api_state=api_state,
        rest_transport=FakeRestTransport(),
        spot_stream_transport=stream,
        futures_stream_transport=stream,
        futures_rpc_transport=FakeFuturesRpc(),
        now=lambda: datetime(2026, 7, 16, tzinfo=UTC),
    )

    result = await engine.run_bounded()

    assert result.shutdown_reason == "bounded_complete"
    assert [candidate["symbol"] for candidate in api_state.candidates] == ["FOOUSDT"]
    assert set(engine.deep_symbols) == {"BTCUSDT", "ETHUSDT", "FOOUSDT"}
    assert api_state.books[("spot", "FOOUSDT")].is_valid
    assert api_state.books[("usd_m_futures", "FOOUSDT")].is_valid
    assert api_state.health["status"] == "running"
    assert api_state.health["universe_symbol_names"] == ["BTCUSDT", "ETHUSDT", "FOOUSDT"]
    assert api_state.metrics["processed_messages"] >= 10
    assert result.storage_rows_written > 0
    assert (tmp_path / "stage1_candidates").exists()
    await engine.shutdown()
    assert stream.closed is True
    assert api_state.health["status"] == "stopped"


@pytest.mark.asyncio
async def test_live_engine_shutdown_is_idempotent_with_bounded_queues(tmp_path: Path) -> None:
    engine = LiveMonitorEngine(
        app_config=AppConfig.model_validate({"storage": {"base_path": str(tmp_path)}}),
        runtime_config=LiveRuntimeConfig(queue_maxsize=1, broad_stream_message_limit=0),
        rest_transport=FakeRestTransport(),
        spot_stream_transport=FakeStreamTransport(),
    )

    await engine.shutdown()
    await engine.shutdown()

    assert engine.depth_queue.maxsize == 1
    assert engine.trade_queue.maxsize == 1


@pytest.mark.asyncio
async def test_run_forever_consumes_broad_streams_concurrently_and_evaluates_stage1(
    tmp_path: Path,
) -> None:
    stream = InteractiveStreamTransport()
    stop_event = asyncio.Event()
    api_state = ApiState(storage_path=tmp_path)
    engine = LiveMonitorEngine(
        app_config=_test_config(tmp_path),
        runtime_config=LiveRuntimeConfig(deep_stream_message_limit=0),
        api_state=api_state,
        rest_transport=CountingRestTransport(),
        spot_stream_transport=stream,
        now=lambda: datetime(2026, 7, 16, tzinfo=UTC),
    )
    task = asyncio.create_task(engine.run_forever(stop_event=stop_event))

    try:
        await stream.wait_for_url("!ticker_1h@arr")
        await stream.wait_for_url("!bookticker")
        await _feed_stage1_messages(stream)
        await _wait_for_candidate(api_state)
        assert [candidate["symbol"] for candidate in api_state.candidates] == ["FOOUSDT"]
    finally:
        stop_event.set()
        await engine.shutdown()
        try:
            await asyncio.wait_for(task, timeout=1)
        except TimeoutError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    assert stream.closed is True


@pytest.mark.asyncio
async def test_run_forever_bootstraps_once_across_evaluation_cycles(tmp_path: Path) -> None:
    rest = CountingRestTransport()
    stream = InteractiveStreamTransport()
    stop_event = asyncio.Event()
    engine = LiveMonitorEngine(
        app_config=_test_config(tmp_path),
        runtime_config=LiveRuntimeConfig(deep_stream_message_limit=0),
        api_state=ApiState(storage_path=tmp_path),
        rest_transport=rest,
        spot_stream_transport=stream,
        now=lambda: datetime(2026, 7, 16, tzinfo=UTC),
    )
    task = asyncio.create_task(engine.run_forever(stop_event=stop_event))

    try:
        await stream.wait_for_url("!ticker_1h@arr")
        await stream.wait_for_url("!bookticker")
        await _feed_stage1_messages(stream)
        await _wait_for_candidate(engine.api_state)
        await asyncio.sleep(1.1)
    finally:
        stop_event.set()
        await engine.shutdown()
        try:
            await asyncio.wait_for(task, timeout=1)
        except TimeoutError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    assert rest.exchange_info_calls == 1


@pytest.mark.asyncio
async def test_bootstrap_filters_and_ranks_eligible_usdt_symbols_before_cap(tmp_path: Path) -> None:
    class OrderingRestTransport(FakeRestTransport):
        async def get_json(self, url: str, timeout_seconds: float) -> dict[str, Any]:
            if "exchangeInfo" in url:
                return {
                    "symbols": [
                        {
                            "symbol": "ETHBTC",
                            "baseAsset": "ETH",
                            "quoteAsset": "BTC",
                            "status": "TRADING",
                        },
                        {
                            "symbol": "LTCBTC",
                            "baseAsset": "LTC",
                            "quoteAsset": "BTC",
                            "status": "TRADING",
                        },
                        {
                            "symbol": "AAABTC",
                            "baseAsset": "AAA",
                            "quoteAsset": "BTC",
                            "status": "TRADING",
                        },
                        {
                            "symbol": "BBBUSDT",
                            "baseAsset": "BBB",
                            "quoteAsset": "USDT",
                            "status": "TRADING",
                        },
                        {
                            "symbol": "AAAUSDT",
                            "baseAsset": "AAA",
                            "quoteAsset": "USDT",
                            "status": "TRADING",
                        },
                        {
                            "symbol": "CCCUSDT",
                            "baseAsset": "CCC",
                            "quoteAsset": "USDT",
                            "status": "TRADING",
                        },
                    ]
                }
            if "ticker/24hr" in url:
                return {
                    "items": [
                        {"symbol": "AAAUSDT", "quoteVolume": "1000"},
                        {"symbol": "BBBUSDT", "quoteVolume": "3000"},
                        {"symbol": "CCCUSDT", "quoteVolume": "2000"},
                    ]
                }
            return await super().get_json(url, timeout_seconds)

    engine = LiveMonitorEngine(
        app_config=AppConfig.model_validate(
            {
                "universe": {
                    "min_quote_volume_usdt": "100",
                    "min_history_minutes": 0,
                    "min_listing_age_days": 0,
                },
                "storage": {"base_path": str(tmp_path)},
            }
        ),
        runtime_config=LiveRuntimeConfig(max_universe_symbols=2),
        rest_transport=OrderingRestTransport(),
        spot_stream_transport=FakeStreamTransport(),
    )

    await engine.bootstrap()

    assert engine.universe == ["BBBUSDT", "CCCUSDT"]
    assert engine.api_state.health["universe_symbols"] == 2
    assert engine.api_state.health["universe_symbol_names"] == ["BBBUSDT", "CCCUSDT"]


@pytest.mark.asyncio
async def test_bootstrap_fails_closed_when_all_market_volume_snapshot_is_unavailable(
    tmp_path: Path,
) -> None:
    class EmptyVolumeTransport(FakeRestTransport):
        async def get_json(self, url: str, timeout_seconds: float) -> dict[str, Any]:
            if "ticker/24hr" in url:
                return {"items": []}
            return await super().get_json(url, timeout_seconds)

    engine = LiveMonitorEngine(
        app_config=_test_config(tmp_path),
        rest_transport=EmptyVolumeTransport(),
        spot_stream_transport=FakeStreamTransport(),
    )

    with pytest.raises(BinanceConnectorError, match="all-market 24h ticker"):
        await engine.bootstrap()


def test_rolling_ticker_uses_real_trade_count_and_rejects_missing_count(tmp_path: Path) -> None:
    engine = LiveMonitorEngine(
        app_config=_test_config(tmp_path),
        rest_transport=FakeRestTransport(),
        spot_stream_transport=FakeStreamTransport(),
    )
    engine.universe = ["FOOUSDT"]

    engine._handle_spot_rolling_ticker(  # noqa: SLF001 - verifies public-feed normalization.
        {"data": [{"s": "FOOUSDT", "E": 1_784_160_000_000, "c": "15", "q": "900", "n": 90}]}
    )
    engine._handle_spot_rolling_ticker(  # noqa: SLF001 - malformed events must be ignored.
        {"data": [{"s": "FOOUSDT", "E": 1_784_160_001_000, "c": "16", "q": "950"}]}
    )

    assert len(engine.histories["FOOUSDT"]) == 1
    assert engine.histories["FOOUSDT"][0].trade_count == 90


def test_fresh_trades_excludes_stale_cvd_inputs() -> None:
    now = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    stale = TradePrint(now - timedelta(seconds=11), Decimal("10"), Decimal("2"), False, "spot")
    fresh = TradePrint(now - timedelta(seconds=5), Decimal("11"), Decimal("3"), False, "spot")

    assert engine_module._fresh_trades([stale, fresh], now=now, max_age_ms=10_000) == [fresh]


@pytest.mark.asyncio
async def test_stage2_suppresses_valid_but_stale_local_depth_book(tmp_path: Path) -> None:
    now = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    engine = LiveMonitorEngine(
        app_config=_test_config(tmp_path),
        runtime_config=LiveRuntimeConfig(warmup_min_trades=1),
        api_state=ApiState(storage_path=tmp_path),
        rest_transport=FakeRestTransport(),
        spot_stream_transport=FakeStreamTransport(),
        now=lambda: now,
    )
    engine._handle_spot_book_ticker(  # noqa: SLF001 - deterministic runtime regression.
        {"data": {"s": "FOOUSDT", "b": "10.00", "B": "10000", "a": "10.01", "A": "10000"}}
    )
    book = LocalOrderBook(market="spot", symbol="FOOUSDT")
    book.apply_snapshot(
        DepthSnapshot(
            last_update_id=100,
            bids=[("10.00", "10000")],
            asks=[("10.01", "10000")],
        )
    )
    engine.spot_books["FOOUSDT"] = book
    engine.spot_book_updated_at["FOOUSDT"] = now - timedelta(seconds=4)
    engine.trades["FOOUSDT"] = [
        TradePrint(now - timedelta(seconds=1), Decimal("10"), Decimal("10"), False, "spot")
    ]
    candidate = Stage1CandidateRecord(
        candidate_id="stage1:FOOUSDT:1",
        detected_at=now,
        symbol="FOOUSDT",
        score=Decimal("80"),
        score_version="stage1-hot-v2",
        executable_liquidity_score=Decimal("100000"),
        features={
            "return_5m": Decimal("0.08"),
            "relative_volume": Decimal("3"),
            "spread_bps": Decimal("2"),
        },
        thresholds={"max_spread_bps": Decimal("5")},
        reason_codes=["momentum", "relative_volume", "trade_acceleration"],
        input_freshness_ms={"ticker": 0, "book": 0},
    )

    await engine.evaluate_stage2([candidate])

    assert engine.api_state.alerts == []
    assert engine.api_state.health["last_alert_suppression"] == ["suppressed_stale_data"]


def test_obsolete_depth_diff_does_not_refresh_local_book_timestamp(tmp_path: Path) -> None:
    now = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    engine = LiveMonitorEngine(
        app_config=_test_config(tmp_path),
        runtime_config=LiveRuntimeConfig(),
        api_state=ApiState(storage_path=tmp_path),
        rest_transport=FakeRestTransport(),
        spot_stream_transport=FakeStreamTransport(),
        now=lambda: now,
    )
    book = LocalOrderBook(market="spot", symbol="FOOUSDT")
    book.apply_snapshot(
        DepthSnapshot(
            last_update_id=100,
            bids=[("10.00", "10000")],
            asks=[("10.01", "10000")],
        )
    )
    previous_update = now - timedelta(seconds=2)
    engine.spot_book_updated_at["FOOUSDT"] = previous_update

    engine._apply_spot_book_diff(  # noqa: SLF001 - deterministic runtime regression.
        "FOOUSDT",
        book,
        DepthDiff(
            first_update_id=90,
            final_update_id=100,
            bids=[("10.00", "9999")],
            asks=[],
        ),
        received_at=now,
    )

    assert engine.spot_book_updated_at["FOOUSDT"] == previous_update


def test_depth_queue_overflow_clears_local_book_timestamp(tmp_path: Path) -> None:
    engine = LiveMonitorEngine(
        app_config=_test_config(tmp_path),
        runtime_config=LiveRuntimeConfig(),
        api_state=ApiState(storage_path=tmp_path),
        rest_transport=FakeRestTransport(),
        spot_stream_transport=FakeStreamTransport(),
        now=lambda: datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
    )
    book = LocalOrderBook(market="spot", symbol="FOOUSDT")
    engine.api_state.books[("spot", "FOOUSDT")] = book
    engine.spot_book_updated_at["FOOUSDT"] = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)

    engine._mark_book_overflow(  # noqa: SLF001 - deterministic runtime regression.
        "spot", "FOOUSDT", "depth_queue_overflow"
    )

    assert not book.is_valid
    assert "FOOUSDT" not in engine.spot_book_updated_at


@pytest.mark.asyncio
async def test_sync_spot_book_continues_applying_diffs_after_snapshot(tmp_path: Path) -> None:
    stream = ScriptedStreamTransport(
        {
            "foousdt@depth": [
                {"data": {"U": 100, "u": 101, "b": [["10.00", "10001"]], "a": []}},
                {"data": {"U": 102, "u": 102, "b": [["10.00", "10002"]], "a": []}},
            ]
        }
    )
    engine = LiveMonitorEngine(
        app_config=_test_config(tmp_path),
        runtime_config=LiveRuntimeConfig(deep_stream_message_limit=2),
        rest_transport=CountingRestTransport(
            snapshots=[
                {
                    "lastUpdateId": 100,
                    "bids": [["9.99", "1"]],
                    "asks": [["10.01", "1"]],
                }
            ]
        ),
        spot_stream_transport=stream,
    )

    await engine._sync_spot_book("FOOUSDT")  # noqa: SLF001 - regression coverage for engine sync.

    book = engine.api_state.books[("spot", "FOOUSDT")]
    assert book.is_valid
    assert book.last_update_id == 102
    assert str(book.bids[next(price for price in book.bids if str(price) == "10.00")]) == "10002"
    assert engine.depth_queue.qsize == 0


@pytest.mark.asyncio
async def test_sync_spot_book_resyncs_after_gap(tmp_path: Path) -> None:
    rest = CountingRestTransport(
        snapshots=[
            {"lastUpdateId": 100, "bids": [["9.99", "1"]], "asks": [["10.01", "1"]]},
            {"lastUpdateId": 200, "bids": [["10.00", "1"]], "asks": [["10.02", "1"]]},
        ]
    )
    stream = ScriptedStreamTransport(
        {
            "foousdt@depth": [
                {"data": {"U": 100, "u": 101, "b": [["10.00", "2"]], "a": []}},
                {"data": {"U": 105, "u": 105, "b": [["10.00", "3"]], "a": []}},
                {"data": {"U": 200, "u": 201, "b": [["10.00", "4"]], "a": []}},
            ]
        }
    )
    engine = LiveMonitorEngine(
        app_config=_test_config(tmp_path),
        runtime_config=LiveRuntimeConfig(deep_stream_message_limit=3),
        rest_transport=rest,
        spot_stream_transport=stream,
    )

    await engine._sync_spot_book("FOOUSDT")  # noqa: SLF001 - regression coverage for resync.

    book = engine.api_state.books[("spot", "FOOUSDT")]
    assert rest.depth_snapshot_calls == 2
    assert book.is_valid
    assert book.last_update_id == 201


@pytest.mark.asyncio
async def test_bounded_cycle_marks_futures_degraded_without_blocking_stage2(
    tmp_path: Path,
) -> None:
    engine = LiveMonitorEngine(
        app_config=_test_config(tmp_path),
        runtime_config=LiveRuntimeConfig(
            broad_stream_message_limit=8,
            deep_stream_message_limit=1,
            futures_enabled=True,
            warmup_min_trades=1,
        ),
        api_state=ApiState(storage_path=tmp_path),
        rest_transport=FakeRestTransport(),
        spot_stream_transport=FakeStreamTransport(),
        futures_stream_transport=FailingFuturesStreamTransport(),
        futures_rpc_transport=FailingFuturesRpc(),
        now=lambda: datetime(2026, 7, 16, tzinfo=UTC),
    )

    result = await engine.run_bounded()

    assert result.shutdown_reason == "bounded_complete"
    assert [candidate["symbol"] for candidate in engine.api_state.candidates] == ["FOOUSDT"]
    assert engine.futures["FOOUSDT"].degraded is True
    assert result.storage_rows_written > len(engine.api_state.candidates)
