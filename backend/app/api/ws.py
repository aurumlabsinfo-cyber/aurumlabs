"""WebSocket endpoints.

Four streams, all push-only:

* ``/ws/market``    top of book + trades
* ``/ws/orderbook`` throttled depth snapshots
* ``/ws/signals``   signal lifecycle events (created / trigger hit / expired)
* ``/ws/dashboard`` everything the UI needs in one socket

Every frame carries ``server_ts``. The dashboard uses it to estimate its offset
from the server clock, so the 5 second countdown is driven by the backend's
`triggered_at`/`expires_at` rather than by an unsynchronised browser timer.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import orjson
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.bus import Subscription, Topic
from app.core.clock import now_ms
from app.core.logging_conf import get_logger
from app.marketdata.orderbook import LocalOrderBook
from app.marketdata.types import MarketTick, Trade
from app.services.container import get_services

log = get_logger(__name__)
router = APIRouter()

_connections = 0


def _dumps(payload: Any) -> str:
    return orjson.dumps(payload, option=orjson.OPT_SERIALIZE_NUMPY).decode()


def _tick_payload(tick: MarketTick) -> dict[str, Any]:
    return {
        "type": "tick",
        "server_ts": now_ms(),
        "data": {
            "ts": tick.ts,
            "exchange_ts": tick.exchange_ts,
            "latency_ms": tick.latency_ms,
            "price": tick.mid,
            "bid": tick.bid_price,
            "bid_qty": tick.bid_qty,
            "ask": tick.ask_price,
            "ask_qty": tick.ask_qty,
            "spread": tick.spread,
            "spread_bps": tick.spread_bps,
            "micro_price": tick.micro_price,
            "last_price": tick.last_price,
            "book_synced": tick.book_synced,
            "is_synthetic": tick.is_synthetic,
        },
    }


def _trade_payload(trade: Trade) -> dict[str, Any]:
    return {
        "type": "trade",
        "server_ts": now_ms(),
        "data": {
            "ts": trade.server_ts,
            "price": trade.price,
            "quantity": trade.quantity,
            "notional": trade.notional,
            "side": trade.aggressor.value,
            "trade_id": trade.trade_id,
        },
    }


def _book_payload(book: LocalOrderBook, levels: int = 20) -> dict[str, Any]:
    return {"type": "orderbook", "server_ts": now_ms(),
            "data": book.snapshot_dict(levels=levels)}


class WSSession:
    """Owns one browser connection and its bus subscriptions."""

    def __init__(self, ws: WebSocket, name: str) -> None:
        self.ws = ws
        self.name = name
        self.subs: list[Subscription] = []
        self.alive = True

    async def accept(self) -> bool:
        global _connections
        svc = get_services()
        if _connections >= svc.settings.ws_max_connections:
            await self.ws.close(code=1013, reason="too many connections")
            return False
        await self.ws.accept()
        _connections += 1
        return True

    async def close(self) -> None:
        global _connections
        self.alive = False
        for sub in self.subs:
            sub.close()
        self.subs.clear()
        _connections = max(0, _connections - 1)
        with contextlib.suppress(Exception):
            await self.ws.close()

    def subscribe(self, topic: str, maxsize: int = 256) -> Subscription:
        sub = get_services().bus.subscribe(topic, maxsize=maxsize)
        self.subs.append(sub)
        return sub

    async def send(self, payload: dict[str, Any]) -> None:
        await self.ws.send_text(_dumps(payload))

    async def pump(self, sub: Subscription, transform) -> None:
        try:
            while self.alive:
                msg = await sub.queue.get()
                payload = transform(msg)
                if payload is not None:
                    await self.send(payload)
        except (WebSocketDisconnect, RuntimeError, ConnectionError):
            self.alive = False
        except asyncio.CancelledError:
            raise

    async def heartbeat(self, interval: float = 5.0) -> None:
        """Keeps the socket warm and gives the client a clock reference."""
        try:
            while self.alive:
                await asyncio.sleep(interval)
                await self.send({"type": "heartbeat", "server_ts": now_ms()})
        except (WebSocketDisconnect, RuntimeError, ConnectionError):
            self.alive = False
        except asyncio.CancelledError:
            raise

    async def drain_client(self) -> None:
        """Read and discard client frames so disconnects are detected promptly."""
        try:
            while self.alive:
                await self.ws.receive_text()
        except Exception:  # noqa: BLE001
            self.alive = False


async def _run(session: WSSession, tasks: list) -> None:
    done, pending = await asyncio.wait(
        [asyncio.create_task(t) for t in tasks],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        with contextlib.suppress(Exception):
            task.result()


@router.websocket("/ws/market")
async def ws_market(ws: WebSocket) -> None:
    session = WSSession(ws, "market")
    if not await session.accept():
        return
    svc = get_services()
    try:
        if svc.market.last_tick:
            await session.send(_tick_payload(svc.market.last_tick))
        ticks = session.subscribe(Topic.TICK, 512)
        trades = session.subscribe(Topic.TRADE, 512)
        await _run(
            session,
            [
                session.pump(ticks, _tick_payload),
                session.pump(trades, _trade_payload),
                session.heartbeat(),
                session.drain_client(),
            ],
        )
    finally:
        await session.close()


@router.websocket("/ws/orderbook")
async def ws_orderbook(ws: WebSocket) -> None:
    session = WSSession(ws, "orderbook")
    if not await session.accept():
        return
    svc = get_services()
    try:
        async def stream() -> None:
            # The book changes 10x/s; the eye cannot use that, so throttle.
            while session.alive:
                await session.send(_book_payload(svc.market.book))
                await asyncio.sleep(0.25)

        await _run(session, [stream(), session.heartbeat(), session.drain_client()])
    finally:
        await session.close()


@router.websocket("/ws/signals")
async def ws_signals(ws: WebSocket) -> None:
    session = WSSession(ws, "signals")
    if not await session.accept():
        return
    svc = get_services()
    try:
        await session.send(
            {"type": "snapshot", "server_ts": now_ms(), "data": svc.signals.snapshot()}
        )
        signals = session.subscribe(Topic.SIGNAL, 256)
        await _run(
            session,
            [
                session.pump(
                    signals,
                    lambda m: {"type": "signal", "server_ts": now_ms(), "data": m},
                ),
                session.heartbeat(2.0),
                session.drain_client(),
            ],
        )
    finally:
        await session.close()


@router.websocket("/ws/dashboard")
async def ws_dashboard(ws: WebSocket) -> None:
    """One socket carrying everything the UI renders."""
    session = WSSession(ws, "dashboard")
    if not await session.accept():
        return
    svc = get_services()
    try:
        await session.send(
            {
                "type": "hello",
                "server_ts": now_ms(),
                "data": {
                    "symbol": svc.settings.symbol,
                    "horizon_s": svc.settings.signal_horizon_s,
                    "is_synthetic": svc.market.is_synthetic,
                    "source": svc.market.source.value,
                    "payout": svc.settings.binary_payout,
                    "payout_status": (
                        "KNOWN" if svc.settings.binary_payout is not None
                        else "PAYOUT UNKNOWN"
                    ),
                },
            }
        )
        await session.send(
            {"type": "signal_snapshot", "server_ts": now_ms(),
             "data": svc.signals.snapshot()}
        )
        if svc.market.last_tick:
            await session.send(_tick_payload(svc.market.last_tick))

        ticks = session.subscribe(Topic.TICK, 512)
        trades = session.subscribe(Topic.TRADE, 256)
        signals = session.subscribe(Topic.SIGNAL, 256)
        agents = session.subscribe(Topic.AGENTS, 64)

        async def slow_state() -> None:
            while session.alive:
                await session.send(
                    {
                        "type": "state",
                        "server_ts": now_ms(),
                        "data": {
                            "health": await svc.health(),
                            "orderbook": svc.market.book.snapshot_dict(levels=12),
                            "features": svc.features.latest,
                            # Ranked gate counts, so the dashboard can say WHY
                            # nothing is being emitted instead of only that
                            # nothing is.
                            "diagnostics": svc.signals.diagnostics(top=5),
                        },
                    }
                )
                await asyncio.sleep(1.0)

        await _run(
            session,
            [
                session.pump(ticks, _tick_payload),
                session.pump(trades, _trade_payload),
                session.pump(
                    signals,
                    lambda m: {"type": "signal", "server_ts": now_ms(), "data": m},
                ),
                session.pump(
                    agents,
                    lambda m: {"type": "agents", "server_ts": now_ms(), "data": m},
                ),
                slow_state(),
                session.heartbeat(2.0),
                session.drain_client(),
            ],
        )
    finally:
        await session.close()


def connection_count() -> int:
    return _connections
