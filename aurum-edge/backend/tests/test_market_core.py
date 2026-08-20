"""The Market Core against a wire-level Bybit double."""

from __future__ import annotations

import asyncio

from aurum_edge.config import BybitConfig, Config
from aurum_edge.scan.book import BookState
from aurum_edge.scan.bybit_rest import BybitRest
from aurum_edge.scan.market_core import MarketCore
from aurum_edge.scan.scanner import Scanner
from aurum_edge.scan.snapshot import FeedSource
from aurum_edge.util.clock import Clock

from .fakebybit.server import FakeBybit


async def build_core(cfg: Config, fake: FakeBybit) -> MarketCore:
    rest = BybitRest(
        BybitConfig(api_key=fake.api_key, api_secret=fake.api_secret),
        base_url=fake.rest_url,
    )
    core = MarketCore(cfg, Clock(), rest, source=FeedSource.BYBIT, ws_url=fake.ws_public_url)
    await core.discover_universe()
    await core.start()
    assert await core.wait_ready(5.0)
    return core


async def feed_symbol(fake: FakeBybit, symbol: str, steps: int = 12, drift: float = 0.02) -> None:
    """A clean snapshot then a walk, with trades and a ticker."""
    await fake.push_book(symbol, bid=99.99, ask=100.01, snapshot=True)
    await fake.push_ticker(symbol)
    for i in range(steps):
        price = 100.0 + i * drift
        await fake.push_book(symbol, bid=price - 0.01, ask=price + 0.01, snapshot=True)
        await fake.push_trade(symbol, price=price, size=2.0, side="Buy")
        await asyncio.sleep(0.01)


async def test_universe_is_discovered_and_filtered(cfg: Config, fake_bybit: FakeBybit) -> None:
    rest = BybitRest(BybitConfig(), base_url=fake_bybit.rest_url)
    core = MarketCore(cfg, Clock(), rest, ws_url=fake_bybit.ws_public_url)
    universe = await core.discover_universe()
    assert universe == fake_bybit.symbols          # ranked by turnover
    assert all(core.instruments[s].status == "Trading" for s in universe)
    assert core.instruments["BTCUSDT"].qty_step == 0.001
    await rest.close()


async def test_illiquid_symbols_are_excluded(cfg: Config, fake_bybit: FakeBybit) -> None:
    import dataclasses

    tight = dataclasses.replace(cfg, scan=dataclasses.replace(
        cfg.scan, min_turnover_24h_usd=499_500_000
    ))
    rest = BybitRest(BybitConfig(), base_url=fake_bybit.rest_url)
    core = MarketCore(tight, Clock(), rest, ws_url=fake_bybit.ws_public_url)
    universe = await core.discover_universe()
    assert universe == ["BTCUSDT"], "only the most liquid symbol clears the filter"
    await rest.close()


async def test_snapshots_are_built_from_the_live_feed(cfg: Config, fake_bybit: FakeBybit) -> None:
    core = await build_core(cfg, fake_bybit)
    try:
        await feed_symbol(fake_bybit, "BTCUSDT")
        await asyncio.sleep(0.1)
        snapshots = {s.symbol: s for s in core.snapshots()}
        snap = snapshots["BTCUSDT"]
        assert snap.source == "bybit"
        assert snap.book_state == "OK"
        assert snap.bid < snap.ask
        assert snap.trades_60s > 0
        assert snap.open_interest == 12345.0
        assert snap.latency_ms >= 0
        assert snap.book_age_ms < 2_000
    finally:
        await core.stop()
        await core.rest.close()


async def test_a_sequence_gap_makes_the_symbol_untradable(
    cfg: Config, fake_bybit: FakeBybit
) -> None:
    core = await build_core(cfg, fake_bybit)
    try:
        await fake_bybit.push_book("BTCUSDT", snapshot=True)
        await fake_bybit.push_book("BTCUSDT")
        await asyncio.sleep(0.08)
        assert core.states["BTCUSDT"].book.state is BookState.OK

        await fake_bybit.push_book("BTCUSDT", skip_sequence=True)
        await asyncio.sleep(0.08)
        state = core.states["BTCUSDT"]
        assert state.book.state is BookState.RESYNC
        assert state.book.gaps == 1

        snapshot = core.snapshot("BTCUSDT")
        assert snapshot is None or snapshot.tradable is False
        assert core.health()["focus_books_ok"] < core.health()["focus_size"]
    finally:
        await core.stop()
        await core.rest.close()


async def test_reconnect_invalidates_books_and_notifies(
    cfg: Config, fake_bybit: FakeBybit
) -> None:
    core = await build_core(cfg, fake_bybit)
    notified: list[str] = []

    async def hook(name: str) -> None:
        notified.append(name)

    core.on_reconnect_hook = hook
    try:
        await feed_symbol(fake_bybit, "BTCUSDT", steps=4)
        await asyncio.sleep(0.08)
        assert core.states["BTCUSDT"].book.state is BookState.OK

        await fake_bybit.drop_sockets()
        for _ in range(80):
            await asyncio.sleep(0.05)
            if notified:
                break
        assert notified, "the engine must be told about a reconnect"
        # every book on that connection is a guess until it is re-snapshotted
        assert core.states["BTCUSDT"].book.state is BookState.RESYNC
        snapshot = core.snapshot("BTCUSDT")
        assert snapshot is None or snapshot.tradable is False
    finally:
        await core.stop()
        await core.rest.close()


async def test_scanner_ranks_the_moving_symbol_first(cfg: Config, fake_bybit: FakeBybit) -> None:
    core = await build_core(cfg, fake_bybit)
    try:
        # BTC runs, ETH sits still
        await feed_symbol(fake_bybit, "ETHUSDT", steps=12, drift=0.0)
        await feed_symbol(fake_bybit, "BTCUSDT", steps=12, drift=0.05)
        await asyncio.sleep(0.1)

        scanner = Scanner(min_score=-10.0)     # rank everything, so order is testable
        result = scanner.scan(core.snapshots(), 0.0)
        ranked = [o.symbol for o in result.ranked]
        assert ranked[0] == "BTCUSDT"
        assert result.ranked[0].side == "LONG"
        assert result.considered >= 2
    finally:
        await core.stop()
        await core.rest.close()


async def test_health_is_honest_about_the_feed(cfg: Config, fake_bybit: FakeBybit) -> None:
    core = await build_core(cfg, fake_bybit)
    try:
        health = core.health()
        assert health["source"] == "bybit"
        assert health["connections_live"] == health["connections_total"] >= 1
        assert health["symbols"] == len(fake_bybit.symbols)
        assert set(health["focus"]) <= set(fake_bybit.symbols)
    finally:
        await core.stop()
        await core.rest.close()
