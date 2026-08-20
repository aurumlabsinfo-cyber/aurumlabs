"""The live proof: this suite talks to the real Bybit.

Skipped by default so the offline suite stays fast and deterministic.  Run it
with a real network connection:

    AURUM_LIVE_TESTS=1 python -m pytest tests/test_live_bybit.py -v

or, equivalently, ``python -m aurum_edge test-bybit``.

If it fails with a connection error, the exchange is not reachable from this
machine (firewall, proxy or an egress policy) - not a bug in the client.  The
offline suite deliberately cannot substitute for this: nothing in it may ever be
presented as evidence that Bybit is connected.
"""

from __future__ import annotations

import asyncio
import os
import time

import pytest

from aurum_edge.config import BybitConfig, load_config
from aurum_edge.scan.bybit_rest import BybitRest
from aurum_edge.scan.bybit_ws import WsConnection

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("AURUM_LIVE_TESTS") != "1",
        reason="set AURUM_LIVE_TESTS=1 to run tests against the real Bybit",
    ),
]


@pytest.fixture
def bybit() -> BybitConfig:
    return load_config().bybit


async def test_rest_is_reachable_and_the_clock_agrees(bybit: BybitConfig) -> None:
    rest = BybitRest(bybit)
    try:
        result = await rest.server_time()
        server_ms = float(result["timeNano"]) / 1e6
        skew = abs(time.time() * 1000.0 - server_ms)
        assert skew < 5_000, f"local clock is {skew:.0f}ms away from Bybit"
    finally:
        await rest.close()


async def test_the_usdt_perpetual_universe_is_real(bybit: BybitConfig) -> None:
    rest = BybitRest(bybit)
    try:
        instruments = await rest.instruments()
        tickers = await rest.tickers()
        perps = [
            i for i in instruments
            if i.get("contractType") == "LinearPerpetual"
            and i.get("quoteCoin") == "USDT"
            and i.get("status") == "Trading"
        ]
        assert len(perps) > 100, "Bybit lists hundreds of USDT perpetuals"
        liquid = [t for t in tickers if float(t.get("turnover24h", 0) or 0) > 25_000_000]
        assert len(liquid) > 20
        assert any(t["symbol"] == "BTCUSDT" for t in tickers)
    finally:
        await rest.close()


async def test_public_stream_delivers_a_synchronised_book(bybit: BybitConfig) -> None:
    messages: list[dict] = []
    conn = WsConnection("live-public", bybit.ws_public, messages.append, ping_interval_s=20.0)
    await conn.start(["orderbook.50.BTCUSDT", "publicTrade.BTCUSDT", "tickers.BTCUSDT"])
    assert await conn.wait_live(20.0), "public websocket did not go live"
    await asyncio.sleep(6.0)
    await conn.stop()

    books = [m for m in messages if m["topic"].startswith("orderbook")]
    trades = [m for m in messages if m["topic"].startswith("publicTrade")]
    assert books, "no order book data"
    assert books[0]["type"] == "snapshot", "the first book message must be a snapshot"
    assert trades, "no public trades"

    # update ids must be contiguous within a session
    ids = [int(m["data"]["u"]) for m in books]
    gaps = [b - a for a, b in zip(ids, ids[1:]) if b - a != 1]
    assert not gaps, f"the exchange stream had sequence gaps: {gaps[:5]}"

    # and the data must be fresh
    latency = time.time() * 1000.0 - float(books[-1]["ts"])
    assert latency < 5_000, f"last message was {latency:.0f}ms old"


async def test_the_book_is_not_crossed(bybit: BybitConfig) -> None:
    from aurum_edge.scan.book import BookState, OrderBook

    book = OrderBook("BTCUSDT", depth_topic="orderbook.50")
    conn = WsConnection(
        "live-book", bybit.ws_public,
        lambda m: book.apply(m, time.time() * 1000.0), ping_interval_s=20.0,
    )
    await conn.start(["orderbook.50.BTCUSDT"])
    assert await conn.wait_live(20.0)
    await asyncio.sleep(6.0)
    await conn.stop()

    assert book.state is BookState.OK, book.resync_reason
    assert book.best_bid_price and book.best_ask_price
    assert book.best_bid_price < book.best_ask_price
    assert book.gaps == 0
    assert 0 < book.spread_bps() < 50


@pytest.mark.skipif(
    not (os.environ.get("BYBIT_API_KEY") and os.environ.get("BYBIT_API_SECRET")),
    reason="no API credentials configured",
)
async def test_private_side_authenticates(bybit: BybitConfig) -> None:
    rest = BybitRest(bybit)
    try:
        wallet = await rest.wallet_balance()
        assert "totalEquity" in wallet
        await rest.positions()
        await rest.open_orders()
    finally:
        await rest.close()

    received: list[dict] = []
    conn = WsConnection(
        "live-private", bybit.ws_private, received.append,
        api_key=bybit.api_key, api_secret=bybit.api_secret, private=True,
        stale_feed_ms=float("inf"),
    )
    await conn.start(["wallet", "order", "execution", "position"])
    live = await conn.wait_live(20.0)
    await conn.stop()
    assert live, "private websocket did not authenticate"
