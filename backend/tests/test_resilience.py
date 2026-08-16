"""Reconnection, error handling, back-pressure and data-quality gating."""

from __future__ import annotations

import asyncio

import pytest

from app.core.bus import EventBus, Topic
from app.core.clock import now_ms
from app.db.repository import BatchWriter
from app.marketdata.base import Emit, ExchangeAdapter
from app.marketdata.engine import MarketDataEngine
from app.marketdata.types import DataSource, MarketTick


class FlakyAdapter(ExchangeAdapter):
    """Drops its connection `failures` times, then stays up."""

    name = "flaky"

    def __init__(self, failures: int = 3) -> None:
        super().__init__("BTCUSDT")
        self.failures = failures
        self.attempts = 0
        self.connected_event = asyncio.Event()

    async def _stream_once(self, emit: Emit) -> None:
        self.attempts += 1
        if self.attempts <= self.failures:
            raise ConnectionError(f"simulated drop #{self.attempts}")
        self.state.connected = True
        self.connected_event.set()
        await asyncio.sleep(3600)


async def test_adapter_reconnects_after_failures():
    adapter = FlakyAdapter(failures=3)
    task = asyncio.create_task(adapter.run(lambda _m: None))
    try:
        await asyncio.wait_for(adapter.connected_event.wait(), timeout=15)
    finally:
        adapter.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert adapter.attempts == 4
    assert adapter.state.reconnects == 3
    assert "simulated drop" in (adapter.state.last_error or "")


async def test_reconnect_delay_grows():
    """Backoff must widen so a venue outage is not hammered."""
    adapter = FlakyAdapter(failures=3)
    stamps: list[float] = []
    original = asyncio.sleep

    async def spy(delay, *a, **kw):
        stamps.append(delay)
        return await original(0, *a, **kw)  # do not actually wait in the test

    task = asyncio.create_task(adapter.run(lambda _m: None))
    asyncio.get_running_loop()
    try:
        import app.marketdata.base as base_mod

        base_mod.asyncio.sleep = spy  # type: ignore[assignment]
        await asyncio.wait_for(adapter.connected_event.wait(), timeout=10)
    finally:
        import app.marketdata.base as base_mod

        base_mod.asyncio.sleep = original  # type: ignore[assignment]
        adapter.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert len(stamps) >= 3
    # Jitter is +-50%, so compare the midpoints rather than exact values.
    assert stamps[-1] > stamps[0]


async def test_adapter_never_dies_on_an_unexpected_exception():
    class ExplodingAdapter(ExchangeAdapter):
        name = "exploding"

        def __init__(self):
            super().__init__("BTCUSDT")
            self.count = 0

        async def _stream_once(self, emit):
            self.count += 1
            raise ValueError("not a connection error at all")

    adapter = ExplodingAdapter()
    task = asyncio.create_task(adapter.run(lambda _m: None))
    await asyncio.sleep(0.5)
    adapter.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert adapter.count >= 1  # kept trying instead of crashing the process


# ------------------------------------------------------------ data quality
def _tick(ts: int, spread_bps: float = 0.5, latency: int = 10) -> MarketTick:
    mid = 100_000.0
    spread = mid * spread_bps / 10_000
    return MarketTick(
        exchange="synthetic", symbol="BTCUSDT", ts=ts, exchange_ts=ts - latency,
        bid_price=mid - spread / 2, bid_qty=1.0, ask_price=mid + spread / 2,
        ask_qty=1.0, mid=mid, micro_price=mid, spread=spread,
        spread_bps=spread_bps, last_price=mid, latency_ms=latency,
        book_synced=True, source=DataSource.SYNTHETIC,
    )


def test_data_quality_is_zero_before_any_data(settings):
    engine = MarketDataEngine(settings, EventBus())
    q = engine.data_quality()
    assert q["score"] == 0.0
    assert q["ok"] is False


def test_data_quality_is_zero_when_the_feed_goes_stale(settings):
    engine = MarketDataEngine(settings, EventBus())
    engine.started_at = now_ms() - 60_000
    engine.last_tick = _tick(now_ms() - 30_000)
    q = engine.data_quality()
    assert q["score"] == 0.0
    assert any("stale" in r for r in q["reasons"])


def test_data_quality_is_zero_when_the_book_is_desynced(settings):
    engine = MarketDataEngine(settings, EventBus())
    engine.started_at = now_ms() - 60_000
    engine.last_tick = _tick(now_ms())
    engine.book.synced = False
    engine.book.desync_reason = "sequence gap"
    q = engine.data_quality()
    assert q["score"] == 0.0
    assert any("not synced" in r for r in q["reasons"])


def test_data_quality_penalises_a_wide_spread(settings):
    engine = MarketDataEngine(settings, EventBus())
    engine.started_at = now_ms() - 60_000
    engine.book.synced = True
    engine.last_tick = _tick(now_ms(), spread_bps=50.0)
    q = engine.data_quality()
    assert q["score"] < 1.0
    assert any("spread" in r for r in q["reasons"])


def test_warmup_blocks_readiness_even_when_data_is_clean(settings):
    settings.min_warmup_seconds = 60.0
    engine = MarketDataEngine(settings, EventBus())
    engine.started_at = now_ms() - 1000
    engine.book.synced = True
    engine.last_tick = _tick(now_ms())
    q = engine.data_quality()
    assert q["warmup_complete"] is False
    assert q["ok"] is False


# ------------------------------------------------------------- back-pressure
def test_bus_drops_oldest_instead_of_blocking_the_feed():
    bus = EventBus(maxsize=4)
    sub = bus.subscribe(Topic.TICK, maxsize=4)
    for i in range(20):
        bus.publish(Topic.TICK, i)  # nobody is consuming
    assert sub.queue.qsize() == 4
    assert sub.dropped == 16
    # The newest data survived: a slow consumer gets fresh data, not a backlog.
    assert sub.queue.get_nowait() == 16


def test_bus_publish_never_raises_without_subscribers():
    bus = EventBus()
    bus.publish(Topic.TICK, {"x": 1})
    assert bus.published[Topic.TICK] == 1


# ------------------------------------------------------------ database I/O
async def test_batch_writer_requeues_rows_when_the_database_fails(settings, monkeypatch):
    from app.db import repository as repo_mod

    writer = BatchWriter(settings)

    class Boom:
        async def __aenter__(self):
            raise ConnectionError("database is down")

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(repo_mod, "session_scope", lambda: Boom())
    writer.add(repo_mod.MarketTickRow, {"ts": 1})
    writer.add(repo_mod.MarketTickRow, {"ts": 2})

    with pytest.raises(ConnectionError):
        await writer.flush()
    # Rows are preserved for the next attempt rather than silently lost.
    assert writer.pending == 2


async def test_batch_writer_reports_its_own_health(settings):
    writer = BatchWriter(settings)
    health = writer.health()
    assert health["healthy"] is True
    assert health["pending_rows"] == 0


def test_engine_records_errors_without_raising(settings):
    engine = MarketDataEngine(settings, EventBus())
    engine.record_error("test", "something went wrong")
    assert engine.errors[-1]["component"] == "test"
    assert len(engine.health()["errors_recent"]) == 1


def test_ingest_survives_malformed_messages(settings):
    engine = MarketDataEngine(settings, EventBus())
    engine._on_message(object())  # unknown type: ignored
    engine._on_message(None)
    assert engine.counters["tickers"] == 0
