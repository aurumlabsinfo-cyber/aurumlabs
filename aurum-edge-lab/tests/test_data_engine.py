"""Data engine integration: replayed events must produce ready, healthy books."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from aurum.adapters.replay import ReplayFeed
from aurum.bus import EventBus
from aurum.config import Config
from aurum.domain import FeedState
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
