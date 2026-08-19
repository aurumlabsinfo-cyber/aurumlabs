"""Data engine integration: replayed events must produce ready, healthy books."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from aurum.adapters.replay import ReplayFeed
from aurum.bus import EventBus
from aurum.config import Config
from aurum.domain import FeedState, now_ms
from aurum.market.data_engine import DataEngine
from aurum.market.orderbook import BookState
from aurum.storage.repositories import Repositories

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


async def run_engine(config: Config, replay_file: Path, repos: Repositories, seconds: float = 3.0):
    feed = ReplayFeed(SYMBOLS, replay_file, speed=0.0)
    bus = EventBus()
    engine = DataEngine(config, feed, bus, repos)
    await engine.start()
    deadline = asyncio.get_running_loop().time() + seconds
    while asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.05)
        if all(engine.states[s].book.ready for s in SYMBOLS) and engine.processed > 4000:
            break
    return engine


@pytest.mark.asyncio
async def test_replayed_feed_builds_ready_books(config: Config, replay_file: Path, repos: Repositories) -> None:
    config.market.symbols = [s for s in config.market.symbols if s.symbol in SYMBOLS]
    engine = await run_engine(config, replay_file, repos)
    try:
        assert engine.processed > 1000
        for symbol in SYMBOLS:
            state = engine.states[symbol]
            assert state.book.state is BookState.READY, f"{symbol} book not ready"
            view = state.book.top(10)
            assert view.best_bid is not None and view.best_ask is not None
            assert view.best_bid < view.best_ask, f"{symbol} book is crossed"
            assert state.book.stats.sequence_gaps == 0
            assert len(state.trades) > 0
            assert len(state.tops) > 0
            assert state.mark_price > 0
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_quality_reports_live_and_tradable(config: Config, replay_file: Path, repos: Repositories) -> None:
    config.market.symbols = [s for s in config.market.symbols if s.symbol in SYMBOLS]
    config.quality.min_events_per_min = 10
    engine = await run_engine(config, replay_file, repos)
    try:
        await asyncio.sleep(0.7)  # let one quality tick land
        for symbol in SYMBOLS:
            quality = engine.quality(symbol)
            assert quality is not None, f"no quality for {symbol}"
            # The replay clock is historical, so events read as stale against
            # wall time; what must hold is that the assessment exists, scores
            # the book, and refuses to call a stale feed tradable.
            assert quality.book_levels >= 5 or quality.state is FeedState.STALE
        health = engine.health()
        assert health["live"] is False, "a replay feed must never report itself as live"
        assert health["feed_kind"] == "replay"
        assert health["symbols_configured"] == 3
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_liquidity_and_flow_history_is_populated(
    config: Config, replay_file: Path, repos: Repositories
) -> None:
    config.market.symbols = [s for s in config.market.symbols if s.symbol in SYMBOLS]
    engine = await run_engine(config, replay_file, repos)
    try:
        state = engine.states["BTCUSDT"]
        assert len(state.liquidity) > 50
        assert any(d.added_bid > 0 or d.removed_bid > 0 for d in state.liquidity)
        assert any(d.added_ask > 0 or d.removed_ask > 0 for d in state.liquidity)
        buys = sum(1 for t in state.trades if t.aggressor.value == "BUY")
        sells = len(state.trades) - buys
        assert buys > 0 and sells > 0, "trade tape must carry both aggressor sides"
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_events_are_persisted_and_readable(
    config: Config, replay_file: Path, repos: Repositories
) -> None:
    config.market.symbols = [s for s in config.market.symbols if s.symbol in SYMBOLS]
    engine = await run_engine(config, replay_file, repos)
    try:
        repos.db.flush(timeout=5.0)
        count = repos.db.scalar("SELECT COUNT(*) FROM market_events", default=0)
        assert count > 500
        row = repos.db.query_one("SELECT * FROM market_events WHERE symbol = 'BTCUSDT' LIMIT 1")
        assert row is not None and row["source"] == "replay"
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_book_snapshot_is_refreshed_before_it_can_go_stale(
    config: Config, replay_file: Path, repos: Repositories
) -> None:
    """An ageing snapshot schedules a resync rather than silently blocking.

    The diff chain keeps a READY book correct against what the venue sent, but
    only a fresh snapshot re-verifies it. Without the refresh, every symbol
    eventually flags NO_SNAPSHOT and the data-quality gate blocks the entire
    system on a book that is provably in sync.
    """
    config.market.symbols = [s for s in config.market.symbols if s.symbol in SYMBOLS]
    # Force the refresh threshold to fire almost immediately, and remove the
    # inter-resync cooldown so the test does not wait out a production delay.
    config.quality.snapshot_max_age_s = 1.0
    config.market.resync_cooldown_s = 0.0
    engine = await run_engine(config, replay_file, repos, seconds=6.0)
    try:
        # The refresh only fires while the feed is flowing, and this replay is
        # consumed as fast as the loop allows, so hold the feed's liveness
        # marker fresh across the window under test. Poll for the outcome rather
        # than sleeping a fixed time: the quality loop ticks twice a second and a
        # loaded machine can miss a fixed window without anything being wrong.
        deadline = asyncio.get_running_loop().time() + 8.0
        refreshes = 0
        while asyncio.get_running_loop().time() < deadline:
            engine.last_recv_ms = now_ms()
            await asyncio.sleep(0.1)
            refreshes = sum(engine.states[s].book.stats.refreshes for s in SYMBOLS)
            if refreshes > 0:
                break
        assert refreshes > 0, (
            "no periodic refresh was attempted; snapshots would age out and block trading"
        )

        stats = [engine.states[s].book.stats for s in SYMBOLS]
        # Attempting is not enough — a refresh that always rolls back leaves the
        # snapshot ageing exactly as if it had never run.
        landed = sum(s.refreshes - s.refresh_rollbacks for s in stats)
        assert landed > 0, (
            f"every refresh rolled back ({refreshes} attempted); the snapshot never "
            "actually got any younger"
        )

        # The refresh must be safe: whether or not the snapshot could be joined,
        # a book that was correct before is still correct after.
        for symbol in SYMBOLS:
            assert engine.states[symbol].book.state is not BookState.DESYNCED, (
                f"{symbol} was desynced by its own periodic refresh"
            )
        assert sum(s.sequence_gaps for s in stats) == 0, (
            "the refresh broke the diff chain it was supposed to re-verify"
        )
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_a_dead_feed_does_not_trigger_a_pointless_resync(
    config: Config, replay_file: Path, repos: Repositories
) -> None:
    """Refreshing a snapshot needs a feed. Without one it only destroys the book.

    ``begin_resync`` clears the book and waits for a snapshot to rejoin. When
    the feed has already stopped, no diff will ever arrive to join it, so the
    refresh converts a correct-but-idle book into a permanently desynced one and
    the reported reason becomes DESYNCED instead of the truth, which is that the
    feed died.
    """
    config.market.symbols = [s for s in config.market.symbols if s.symbol in SYMBOLS]
    config.quality.snapshot_max_age_s = 1.0
    config.market.resync_cooldown_s = 0.0
    engine = await run_engine(config, replay_file, repos, seconds=6.0)
    try:
        # Let the replay finish so the feed is silent, then let several quality
        # ticks pass.
        while not engine.feed.finished.is_set():
            await asyncio.sleep(0.1)
        engine.last_recv_ms = 1  # far in the past: the feed is unmistakably dead
        before = sum(engine.states[s].book.stats.resyncs for s in SYMBOLS)
        await asyncio.sleep(1.6)
        after = sum(engine.states[s].book.stats.resyncs for s in SYMBOLS)
        assert after == before, "a dead feed triggered a resync that can never complete"

        for symbol in SYMBOLS:
            quality = engine.quality(symbol)
            assert quality is not None
            # DISCONNECTED when the adapter said so, STALE when it simply went
            # quiet. Either is the truth; DESYNCED would not be, because the
            # book was correct until a pointless refresh cleared it.
            assert quality.state in (FeedState.DISCONNECTED, FeedState.STALE), (
                f"{symbol} reports {quality.state.value}; a dead feed must not be "
                "reported as a desync caused by our own refresh"
            )
            assert not quality.tradable
    finally:
        await engine.stop()
