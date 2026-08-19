"""WebSocket broadcaster for ``/ws/live``.

One task builds the state payload on a fixed interval and pushes it to every
connected client.  Building it once per interval rather than once per client is
the difference between a dashboard and a load generator: the payload assembles
the whole market, wallet and research state, and ten open browser tabs should
not multiply that work by ten.

A client that cannot keep up is disconnected rather than allowed to back up
memory.  A dashboard showing state from two minutes ago is worse than one that
reconnects.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Callable

from fastapi import WebSocket, WebSocketDisconnect

from ..logging_setup import get_logger

log = get_logger("api.ws")


class LiveBroadcaster:
    def __init__(self, build_payload: Callable[[], dict[str, Any]], interval_ms: int = 500) -> None:
        self.build_payload = build_payload
        self.interval_s = interval_ms / 1000.0
        self.clients: set[WebSocket] = set()
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self.sent = 0
        self.dropped_clients = 0

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="ws-broadcast")

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        for client in list(self.clients):
            try:
                await client.close()
            except Exception:  # noqa: BLE001 - shutdown must not raise
                pass
        self.clients.clear()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.clients.add(websocket)
        log.info("ws client connected", extra={"clients": len(self.clients)})
        # Send the current state immediately so a fresh tab is not blank until
        # the next tick.
        try:
            await websocket.send_text(json.dumps(self._payload(), default=str))
        except Exception:  # noqa: BLE001
            self.clients.discard(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        self.clients.discard(websocket)
        log.info("ws client disconnected", extra={"clients": len(self.clients)})

    def _payload(self) -> dict[str, Any]:
        try:
            return self.build_payload()
        except Exception as exc:  # noqa: BLE001 - a broken payload must not kill the socket
            log.exception("ws payload build failed")
            return {"type": "error", "error": f"{type(exc).__name__}: {exc}"}

    async def _loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.interval_s)
            if not self.clients:
                continue
            message = json.dumps(self._payload(), default=str)
            stale: list[WebSocket] = []
            for client in list(self.clients):
                try:
                    await client.send_text(message)
                    self.sent += 1
                except (WebSocketDisconnect, RuntimeError, Exception):  # noqa: BLE001
                    stale.append(client)
            for client in stale:
                self.clients.discard(client)
                self.dropped_clients += 1

    def stats(self) -> dict[str, Any]:
        return {
            "clients": len(self.clients),
            "interval_ms": int(self.interval_s * 1000),
            "messages_sent": self.sent,
            "dropped_clients": self.dropped_clients,
        }
