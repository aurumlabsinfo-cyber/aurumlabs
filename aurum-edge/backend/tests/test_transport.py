"""REST signing and websocket behaviour, against a wire-level Bybit double."""

from __future__ import annotations

import asyncio
import json

import pytest

from aurum_edge.config import BybitConfig
from aurum_edge.scan.bybit_rest import BybitError, BybitRest
from aurum_edge.scan.bybit_ws import WsConnection, WsState

from .fakebybit.server import FakeBybit


@pytest.fixture
def rest(fake_bybit: FakeBybit) -> BybitRest:
    cfg = BybitConfig(api_key=fake_bybit.api_key, api_secret=fake_bybit.api_secret)
    return BybitRest(cfg, base_url=fake_bybit.rest_url)


# ------------------------------------------------------------------- REST

async def test_public_reads(rest: BybitRest, fake_bybit: FakeBybit) -> None:
    server_time = await rest.server_time()
    assert "timeSecond" in server_time
    instruments = await rest.instruments()
    assert {i["symbol"] for i in instruments} == set(fake_bybit.symbols)
    tickers = await rest.tickers()
    assert len(tickers) == len(fake_bybit.symbols)
    await rest.close()


async def test_private_requests_are_signed_correctly(
    rest: BybitRest, fake_bybit: FakeBybit
) -> None:
    await rest.wallet_balance()
    headers = fake_bybit.signed_requests[-1]
    assert headers["X-BAPI-API-KEY"] == fake_bybit.api_key
    # GET signs the query string
    assert fake_bybit.verify_signature(headers, "accountType=UNIFIED")

    await rest.place_order(
        symbol="BTCUSDT", side="Buy", qty="1", order_link_id="AEtest1", order_type="Market"
    )
    headers = fake_bybit.signed_requests[-1]
    body = json.dumps(
        {
            "category": "linear", "symbol": "BTCUSDT", "side": "Buy", "orderType": "Market",
            "qty": "1", "orderLinkId": "AEtest1", "timeInForce": "IOC", "positionIdx": 0,
        },
        separators=(",", ":"),
    )
    assert fake_bybit.verify_signature(headers, body)
    await rest.close()


async def test_error_code_becomes_a_typed_exception(rest: BybitRest, fake_bybit: FakeBybit) -> None:
    fake_bybit.order_error = (110007, "insufficient balance")
    with pytest.raises(BybitError) as exc:
        await rest.place_order("BTCUSDT", "Buy", "1", "AEtest2")
    assert exc.value.ret_code == 110007
    await rest.close()


async def test_duplicate_order_link_id_is_refused_by_the_venue(
    rest: BybitRest, fake_bybit: FakeBybit
) -> None:
    fake_bybit.auto_fill = False
    await rest.place_order("BTCUSDT", "Buy", "1", "AEsame")
    with pytest.raises(BybitError) as exc:
        await rest.place_order("BTCUSDT", "Buy", "1", "AEsame")
    assert exc.value.ret_code == 110072
    assert len(fake_bybit.orders) == 1
    await rest.close()


# --------------------------------------------------------------- websocket

async def collect(messages: list[dict]) -> None:
    pass


async def test_subscribe_and_receive(fake_bybit: FakeBybit) -> None:
    received: list[dict] = []
    conn = WsConnection(
        "public", fake_bybit.ws_public_url, received.append,
        ping_interval_s=5.0, stale_feed_ms=5_000.0,
    )
    await conn.start(["orderbook.50.BTCUSDT", "publicTrade.BTCUSDT"])
    assert await conn.wait_live(5.0)
    assert conn.state is WsState.LIVE

    await fake_bybit.push_book("BTCUSDT", snapshot=True)
    await fake_bybit.push_trade("BTCUSDT")
    await asyncio.sleep(0.1)
    topics = {m["topic"] for m in received}
    assert topics == {"orderbook.50.BTCUSDT", "publicTrade.BTCUSDT"}
    await conn.stop()


async def test_topics_are_batched_ten_at_a_time(fake_bybit: FakeBybit) -> None:
    conn = WsConnection("public", fake_bybit.ws_public_url, lambda m: None, subscribe_batch=10)
    topics = [f"orderbook.1.SYM{i}USDT" for i in range(25)]
    await conn.start(topics)
    assert await conn.wait_live(5.0)
    await asyncio.sleep(0.1)
    assert all(len(batch) <= 10 for batch in fake_bybit.subscribe_calls)
    assert sum(len(batch) for batch in fake_bybit.subscribe_calls) == 25
    await conn.stop()


async def test_reconnect_resubscribes_and_fires_the_hook(fake_bybit: FakeBybit) -> None:
    received: list[dict] = []
    reconnects: list[str] = []

    async def on_reconnect(name: str) -> None:
        reconnects.append(name)

    conn = WsConnection(
        "public", fake_bybit.ws_public_url, received.append,
        reconnect_base_delay_s=0.05, ping_interval_s=5.0, on_reconnect=on_reconnect,
    )
    await conn.start(["orderbook.50.BTCUSDT"])
    assert await conn.wait_live(5.0)
    first_session = conn.session_id

    await fake_bybit.drop_sockets()
    for _ in range(80):
        await asyncio.sleep(0.05)
        if conn.is_live and conn.session_id > first_session:
            break
    assert conn.session_id > first_session, "connection did not come back"
    assert reconnects == ["public"], "the reconnect hook must fire so entries can be blocked"
    assert "orderbook.50.BTCUSDT" in fake_bybit.topics(), "topics were not resubscribed"

    received.clear()
    await fake_bybit.push_book("BTCUSDT", snapshot=True)
    await asyncio.sleep(0.1)
    assert received, "no data after the reconnect"
    await conn.stop()


async def test_stale_feed_is_detected_even_though_the_socket_is_open(
    fake_bybit: FakeBybit,
) -> None:
    conn = WsConnection(
        "public", fake_bybit.ws_public_url, lambda m: None,
        ping_interval_s=0.15, ping_timeout_s=0.15, stale_feed_ms=200.0,
        reconnect_base_delay_s=0.05,
    )
    await conn.start(["orderbook.50.BTCUSDT"])
    assert await conn.wait_live(5.0)
    first_session = conn.session_id

    fake_bybit.silence = True          # socket stays open, data stops
    for _ in range(60):
        await asyncio.sleep(0.05)
        if conn.session_id > first_session:
            break
    assert conn.session_id > first_session, "a silent but open socket must be torn down"
    assert conn.reconnects >= 1
    await conn.stop()


async def test_missing_pong_forces_a_reconnect(fake_bybit: FakeBybit) -> None:
    conn = WsConnection(
        "public", fake_bybit.ws_public_url, lambda m: None,
        ping_interval_s=0.1, ping_timeout_s=0.1, stale_feed_ms=1e9,
        reconnect_base_delay_s=0.05,
    )
    await conn.start(["orderbook.50.BTCUSDT"])
    assert await conn.wait_live(5.0)
    fake_bybit.answer_ping = False
    first_session = conn.session_id
    for _ in range(80):
        await asyncio.sleep(0.05)
        if conn.session_id > first_session:
            break
    assert conn.session_id > first_session
    await conn.stop()


async def test_private_stream_authenticates(fake_bybit: FakeBybit) -> None:
    received: list[dict] = []
    conn = WsConnection(
        "private", fake_bybit.ws_private_url, received.append,
        api_key=fake_bybit.api_key, api_secret=fake_bybit.api_secret, private=True,
        ping_interval_s=5.0, stale_feed_ms=1e9,
    )
    await conn.start(["wallet", "execution", "order", "position"])
    assert await conn.wait_live(5.0)
    await fake_bybit.push_wallet(1234.0, 1000.0)
    await asyncio.sleep(0.1)
    assert any(m["topic"] == "wallet" for m in received)
    await conn.stop()


async def test_rejected_auth_does_not_go_live(fake_bybit: FakeBybit) -> None:
    fake_bybit.reject_auth = True
    conn = WsConnection(
        "private", fake_bybit.ws_private_url, lambda m: None,
        api_key=fake_bybit.api_key, api_secret="wrong", private=True,
        ping_interval_s=5.0, stale_feed_ms=1e9, reconnect_base_delay_s=0.05,
    )
    await conn.start(["wallet"])
    assert not await conn.wait_live(1.0)
    assert conn.state is not WsState.LIVE
    await conn.stop()


async def test_health_reports_are_truthful(fake_bybit: FakeBybit) -> None:
    conn = WsConnection("public", fake_bybit.ws_public_url, lambda m: None, ping_interval_s=5.0)
    await conn.start(["orderbook.50.BTCUSDT"])
    assert await conn.wait_live(5.0)
    health = conn.health()
    assert health["live"] is True and health["stale"] is False
    assert health["topics"] == 1
    await conn.stop()
    assert conn.state is WsState.STOPPED
