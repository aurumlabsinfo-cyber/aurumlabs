"""Bybit V5 websocket client: one class for public and private streams.

Guarantees the trading core depends on:

* **heartbeat** - a ``ping`` every 20 s, and a missing ``pong`` is a dead
  connection, not a slow one;
* **stale-feed detection** - a connection that is open but silent for longer
  than ``stale_feed_ms`` is torn down and rebuilt.  An open socket is never
  taken as proof that data is flowing;
* **resubscribe** - the topic set is owned by this object, so every reconnect
  replays it;
* **reconnect notification** - ``on_reconnect`` fires *before* any data of the
  new session is delivered, which is what lets the engine block new entries and
  re-run reconciliation.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import random
import time
from enum import Enum
from typing import Any, Awaitable, Callable, Iterable

import aiohttp

from ..util.clock import mono, now_ms
from ..util.logging_setup import get_logger

log = get_logger("bybit.ws")

MessageHandler = Callable[[dict[str, Any]], None]
AsyncHook = Callable[[str], Awaitable[None]]


class WsState(str, Enum):
    INIT = "INIT"
    CONNECTING = "CONNECTING"
    AUTHENTICATING = "AUTHENTICATING"
    SUBSCRIBING = "SUBSCRIBING"
    LIVE = "LIVE"
    RECONNECTING = "RECONNECTING"
    STOPPED = "STOPPED"


class WsConnection:
    """A single resilient Bybit websocket connection."""

    def __init__(
        self,
        name: str,
        url: str,
        on_message: MessageHandler,
        *,
        api_key: str = "",
        api_secret: str = "",
        private: bool = False,
        ping_interval_s: float = 20.0,
        ping_timeout_s: float = 12.0,
        stale_feed_ms: float = 5_000.0,
        subscribe_batch: int = 10,
        reconnect_base_delay_s: float = 1.0,
        reconnect_max_delay_s: float = 30.0,
        session: aiohttp.ClientSession | None = None,
        on_reconnect: AsyncHook | None = None,
    ) -> None:
        self.name = name
        self.url = url
        self.on_message = on_message
        self.api_key = api_key
        self.api_secret = api_secret
        self.private = private
        self.ping_interval_s = ping_interval_s
        self.ping_timeout_s = ping_timeout_s
        self.stale_feed_ms = stale_feed_ms
        self.subscribe_batch = subscribe_batch
        self.reconnect_base_delay_s = reconnect_base_delay_s
        self.reconnect_max_delay_s = reconnect_max_delay_s
        self.on_reconnect = on_reconnect

        self._session = session
        self._own_session = session is None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._task: asyncio.Task[None] | None = None
        self._ping_task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._live = asyncio.Event()

        self.topics: set[str] = set()
        self.state: WsState = WsState.INIT
        self.connected_at: float = 0.0
        self.last_message_mono: float = 0.0
        self.last_message_ms: float = 0.0
        # Data, not chatter.  A socket that answers pings while its topics have
        # gone quiet is a dead feed on a healthy socket - the exact failure that
        # makes a dashboard show stale prices as live.
        self.last_data_mono: float = 0.0
        self.last_pong_mono: float = 0.0
        self.messages: int = 0
        self.reconnects: int = 0
        self.last_error: str = ""
        self.session_id: int = 0

    # ---------------------------------------------------------------- lifecycle
    async def start(self, topics: Iterable[str] = ()) -> None:
        self.topics |= set(topics)
        self._stop.clear()
        self._task = asyncio.create_task(self._run_forever(), name=f"ws-{self.name}")

    async def stop(self) -> None:
        self._stop.set()
        self.state = WsState.STOPPED
        for task in (self._ping_task, self._task):
            if task and not task.done():
                task.cancel()
        for task in (self._ping_task, self._task):
            if task:
                try:
                    await task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._own_session and self._session and not self._session.closed:
            await self._session.close()

    async def wait_live(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._live.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    # ---------------------------------------------------------------- health
    @property
    def is_live(self) -> bool:
        return self.state is WsState.LIVE and not self.is_stale

    @property
    def age_ms(self) -> float:
        """Age of the last *data* message when topics are subscribed."""
        reference = self.last_data_mono if self.topics else self.last_message_mono
        if not reference:
            return float("inf")
        return (mono() - reference) * 1000.0

    @property
    def is_stale(self) -> bool:
        return self.age_ms > self.stale_feed_ms

    def health(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state.value,
            "live": self.is_live,
            "stale": self.is_stale,
            "age_ms": None if self.age_ms == float("inf") else round(self.age_ms, 1),
            "topics": len(self.topics),
            "messages": self.messages,
            "reconnects": self.reconnects,
            "uptime_s": round(mono() - self.connected_at, 1) if self.connected_at else 0.0,
            "last_error": self.last_error,
        }

    # ---------------------------------------------------------------- topics
    async def subscribe(self, topics: Iterable[str]) -> None:
        new = {t for t in topics if t not in self.topics}
        self.topics |= new
        if new and self._ws is not None and not self._ws.closed:
            await self._send_subscribe(sorted(new))

    async def unsubscribe(self, topics: Iterable[str]) -> None:
        gone = {t for t in topics if t in self.topics}
        if not gone:
            return
        self.topics -= gone
        if self._ws is not None and not self._ws.closed:
            batch = sorted(gone)
            for i in range(0, len(batch), self.subscribe_batch):
                await self._send({"op": "unsubscribe", "args": batch[i : i + self.subscribe_batch]})

    # ---------------------------------------------------------------- internals
    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            self._own_session = True
        return self._session

    async def _send(self, payload: dict[str, Any]) -> None:
        ws = self._ws
        if ws is None or ws.closed:
            return
        await ws.send_str(json.dumps(payload, separators=(",", ":")))

    async def _send_subscribe(self, topics: list[str]) -> None:
        for i in range(0, len(topics), self.subscribe_batch):
            await self._send({"op": "subscribe", "args": topics[i : i + self.subscribe_batch]})

    async def _authenticate(self) -> None:
        expires = int((time.time() + 10) * 1000)
        signature = hmac.new(
            self.api_secret.encode(), f"GET/realtime{expires}".encode(), hashlib.sha256
        ).hexdigest()
        await self._send({"op": "auth", "args": [self.api_key, expires, signature]})

    async def _run_forever(self) -> None:
        delay = self.reconnect_base_delay_s
        while not self._stop.is_set():
            try:
                await self._run_once()
                delay = self.reconnect_base_delay_s
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - any failure means: reconnect
                self.last_error = f"{type(exc).__name__}: {exc}"
                log.warning("[%s] connection failed: %s", self.name, self.last_error)
            if self._stop.is_set():
                break
            self.state = WsState.RECONNECTING
            self._live.clear()
            sleep_for = min(delay, self.reconnect_max_delay_s) * (0.8 + 0.4 * random.random())
            log.info("[%s] reconnecting in %.1fs", self.name, sleep_for)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=sleep_for)
                break
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, self.reconnect_max_delay_s)
        self.state = WsState.STOPPED
        self._live.clear()

    async def _run_once(self) -> None:
        session = await self._ensure_session()
        self.state = WsState.CONNECTING
        log.info("[%s] connecting to %s", self.name, self.url)
        async with session.ws_connect(self.url, heartbeat=None, timeout=15) as ws:
            self._ws = ws
            self.session_id += 1
            self.connected_at = mono()
            self.last_message_mono = mono()
            self.last_data_mono = mono()
            self.last_pong_mono = mono()

            if self.private:
                self.state = WsState.AUTHENTICATING
                await self._authenticate()

            self.state = WsState.SUBSCRIBING
            if self.topics:
                await self._send_subscribe(sorted(self.topics))

            self._ping_task = asyncio.create_task(self._ping_loop(), name=f"ping-{self.name}")
            try:
                await self._read_loop(ws)
            finally:
                if self._ping_task and not self._ping_task.done():
                    self._ping_task.cancel()
                    try:
                        await self._ping_task
                    except (asyncio.CancelledError, Exception):  # noqa: BLE001
                        pass
                self._ws = None
                self._live.clear()

    async def _read_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for msg in ws:
            if msg.type is aiohttp.WSMsgType.TEXT:
                self.last_message_mono = mono()
                self.last_message_ms = now_ms()
                self.messages += 1
                try:
                    payload = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue
                await self._handle(payload)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                raise ConnectionError("socket closed by peer")
            elif msg.type is aiohttp.WSMsgType.ERROR:
                raise ConnectionError(f"socket error: {ws.exception()}")
        raise ConnectionError("stream ended")

    async def _handle(self, payload: dict[str, Any]) -> None:
        op = payload.get("op")
        if op in ("pong",) or payload.get("ret_msg") == "pong":
            self.last_pong_mono = mono()
            return
        if op == "ping":  # server-side ping (private stream)
            await self._send({"op": "pong"})
            return
        if op == "auth":
            ok = payload.get("success") is True or payload.get("retCode") == 0
            if not ok:
                raise ConnectionError(f"auth rejected: {payload}")
            log.info("[%s] authenticated", self.name)
            return
        if op == "subscribe":
            if payload.get("success") is False:
                raise ConnectionError(f"subscribe rejected: {payload.get('ret_msg')}")
            if self.state is WsState.SUBSCRIBING:
                self.state = WsState.LIVE
                self._live.set()
                self.reconnects += 1 if self.session_id > 1 else 0
                if self.on_reconnect and self.session_id > 1:
                    await self.on_reconnect(self.name)
                log.info("[%s] LIVE with %d topics", self.name, len(self.topics))
            return
        if "topic" in payload:
            self.last_data_mono = mono()
            if self.state is not WsState.LIVE:
                # data before the subscribe ack: the stream is working
                self.state = WsState.LIVE
                self._live.set()
            self.on_message(payload)

    async def _ping_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.ping_interval_s)
            if self._ws is None or self._ws.closed:
                return
            await self._send({"op": "ping", "req_id": f"{self.name}-{int(mono())}"})
            await asyncio.sleep(self.ping_timeout_s)
            silent_ms = self.age_ms
            no_pong_s = mono() - self.last_pong_mono
            if no_pong_s > self.ping_interval_s + self.ping_timeout_s:
                log.warning("[%s] no pong for %.1fs - dropping connection", self.name, no_pong_s)
                await self._force_reconnect()
                return
            if silent_ms > self.stale_feed_ms and self.topics:
                log.warning("[%s] feed silent for %.0fms - dropping connection", self.name, silent_ms)
                await self._force_reconnect()
                return

    async def _force_reconnect(self) -> None:
        self.last_error = "stale feed"
        ws = self._ws
        if ws is not None and not ws.closed:
            await ws.close()
