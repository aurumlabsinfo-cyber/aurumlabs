"""HTTP + websocket API.

The trading core does not know this file exists.  The engine runs, trades and
records with the API down; this layer only reads ``engine.state()`` and pushes
it.  That is what makes "the frontend shows exactly the state of the backend"
checkable: there is one serialiser and the dashboard renders its output.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable

from aiohttp import WSMsgType, web

from ..config import Config
from ..engine import Engine
from ..util.logging_setup import get_logger

log = get_logger("api")


def _json(payload: Any, status: int = 200) -> web.Response:
    return web.json_response(
        payload, status=status, dumps=lambda o: json.dumps(o, default=str)
    )


class ApiServer:
    def __init__(self, cfg: Config, engine: Engine) -> None:
        self.cfg = cfg
        self.engine = engine
        self.app = web.Application(middlewares=[self._cors_middleware])
        self.sockets: set[web.WebSocketResponse] = set()
        self.runner: web.AppRunner | None = None
        self.bound_port: int = cfg.api.port
        self._push_task: asyncio.Task[None] | None = None
        self._setup_routes()

    # ---------------------------------------------------------------- routes
    def _setup_routes(self) -> None:
        app = self.app
        app.router.add_get("/api/state", self.get_state)
        app.router.add_get("/api/health", self.get_health)
        app.router.add_get("/api/diagnose", self.get_diagnose)
        app.router.add_get("/api/opportunities", self.get_opportunities)
        app.router.add_get("/api/positions", self.get_positions)
        app.router.add_get("/api/trades", self.get_trades)
        app.router.add_get("/api/decisions", self.get_decisions)
        app.router.add_get("/api/stats", self.get_stats)
        app.router.add_get("/api/model", self.get_model)
        app.router.add_post("/api/control/kill", self.post_kill)
        app.router.add_post("/api/control/resume", self.post_resume)
        app.router.add_post("/api/control/flatten", self.post_flatten)
        app.router.add_post("/api/control/reconcile", self.post_reconcile)
        app.router.add_post("/api/learn/run", self.post_learn)
        app.router.add_post("/api/model/rollback", self.post_rollback)
        app.router.add_get("/ws", self.websocket)
        app.router.add_get("/", self.index)

    @web.middleware
    async def _cors_middleware(
        self, request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
    ) -> web.StreamResponse:
        if request.method == "OPTIONS":
            return web.Response(headers=self._cors_headers())
        response = await handler(request)
        for key, value in self._cors_headers().items():
            response.headers.setdefault(key, value)
        return response

    def _cors_headers(self) -> dict[str, str]:
        return {
            "Access-Control-Allow-Origin": self.cfg.api.cors_origin,
            "Access-Control-Allow-Headers": "Content-Type, X-Aurum-Token",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        }

    def _authorised(self, request: web.Request) -> bool:
        if not self.cfg.api.token:
            return True
        supplied = request.headers.get("X-Aurum-Token") or request.query.get("token", "")
        return supplied == self.cfg.api.token

    # ---------------------------------------------------------------- reads
    async def index(self, request: web.Request) -> web.Response:
        return _json(
            {
                "service": "aurum-edge",
                "version": self.engine.state()["version"],
                "mode": self.cfg.mode.upper(),
                "endpoints": [
                    "/api/state", "/api/health", "/api/diagnose", "/api/opportunities",
                    "/api/positions", "/api/trades", "/api/decisions", "/api/stats",
                    "/api/model", "/ws",
                ],
            }
        )

    async def get_state(self, request: web.Request) -> web.Response:
        return _json(self.engine.state())

    async def get_health(self, request: web.Request) -> web.Response:
        payload = self.engine.health.snapshot()
        status = 200 if payload["trading_allowed"] else 503
        return _json(payload, status=status)

    async def get_diagnose(self, request: web.Request) -> web.Response:
        return _json(self.engine.diagnose())

    async def get_opportunities(self, request: web.Request) -> web.Response:
        limit = int(request.query.get("limit", 20))
        scan = self.engine.scanner.last_result
        return _json(scan.to_dict(limit) if scan else {"ranked": [], "considered": 0})

    async def get_positions(self, request: web.Request) -> web.Response:
        return _json(self.engine.execution.open_positions(self.engine.last_snapshots))

    async def get_trades(self, request: web.Request) -> web.Response:
        limit = int(request.query.get("limit", 100))
        run = request.query.get("run", "")
        return _json(self.engine.repo.trades(run_id=run or None, limit=limit))

    async def get_decisions(self, request: web.Request) -> web.Response:
        limit = int(request.query.get("limit", 50))
        action = request.query.get("action") or None
        return _json(self.engine.repo.recent_decisions(limit=limit, action=action))

    async def get_stats(self, request: web.Request) -> web.Response:
        return _json(
            {
                "run": self.engine.repo.stats().to_dict(),
                "all_runs": self.engine.stats_all_runs(),
                "by_symbol": self.engine.repo.symbol_stats(),
            }
        )

    async def get_model(self, request: web.Request) -> web.Response:
        return _json(self.engine.state()["model"])

    # ---------------------------------------------------------------- controls
    async def post_kill(self, request: web.Request) -> web.Response:
        if not self._authorised(request):
            return _json({"error": "unauthorised"}, status=401)
        body = await _body(request)
        reason = body.get("reason", "requested from the dashboard")
        self.engine.engage_kill_switch(reason)
        closed = await self.engine.flatten("kill switch")
        return _json({"ok": True, "kill_switch": True, "positions_closed": closed})

    async def post_resume(self, request: web.Request) -> web.Response:
        if not self._authorised(request):
            return _json({"error": "unauthorised"}, status=401)
        await self.engine.release_kill_switch()
        return _json({"ok": True, "kill_switch": False})

    async def post_flatten(self, request: web.Request) -> web.Response:
        if not self._authorised(request):
            return _json({"error": "unauthorised"}, status=401)
        closed = await self.engine.flatten("manual flatten")
        return _json({"ok": True, "positions_closed": closed})

    async def post_reconcile(self, request: web.Request) -> web.Response:
        if not self._authorised(request):
            return _json({"error": "unauthorised"}, status=401)
        result = await self.engine.reconciler.reconcile("api")
        return _json(result.to_dict())

    async def post_learn(self, request: web.Request) -> web.Response:
        if not self._authorised(request):
            return _json({"error": "unauthorised"}, status=401)
        return _json(await self.engine.run_learning())

    async def post_rollback(self, request: web.Request) -> web.Response:
        if not self._authorised(request):
            return _json({"error": "unauthorised"}, status=401)
        model, detail = await asyncio.to_thread(self.engine.learning.rollback)
        if model is not None:
            self.engine.champion = model
        return _json({"ok": model is not None, "detail": detail,
                      "champion": self.engine.champion.version})

    # ---------------------------------------------------------------- websocket
    async def websocket(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=20.0)
        await ws.prepare(request)
        self.sockets.add(ws)
        log.info("dashboard connected (%d live)", len(self.sockets))
        try:
            await ws.send_str(json.dumps(
                {"type": "state", "data": self.engine.state()}, default=str
            ))
            async for msg in ws:
                if msg.type is WSMsgType.TEXT and msg.data == "ping":
                    await ws.send_str("pong")
                elif msg.type is WSMsgType.ERROR:
                    break
        finally:
            self.sockets.discard(ws)
            log.info("dashboard disconnected (%d live)", len(self.sockets))
        return ws

    async def _push_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.api.push_interval_s)
            if not self.sockets:
                continue
            try:
                payload = json.dumps(
                    {"type": "state", "data": self.engine.state()}, default=str
                )
            except Exception as exc:  # noqa: BLE001
                log.exception("could not serialise state: %s", exc)
                continue
            for ws in list(self.sockets):
                if ws.closed:
                    self.sockets.discard(ws)
                    continue
                try:
                    await ws.send_str(payload)
                except Exception:  # noqa: BLE001 - a dead socket must not stop the rest
                    self.sockets.discard(ws)

    # ---------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        self.runner = web.AppRunner(self.app, access_log=None)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.cfg.api.host, self.cfg.api.port)
        await site.start()
        sockets = getattr(site._server, "sockets", None)  # type: ignore[union-attr]
        if sockets:
            self.bound_port = sockets[0].getsockname()[1]
        self._push_task = asyncio.create_task(self._push_loop(), name="api-push")
        log.info("API listening on http://%s:%d", self.cfg.api.host, self.bound_port)

    async def stop(self) -> None:
        if self._push_task:
            self._push_task.cancel()
            try:
                await self._push_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        for ws in list(self.sockets):
            await ws.close()
        if self.runner:
            await self.runner.cleanup()


async def _body(request: web.Request) -> dict[str, Any]:
    try:
        return await request.json()
    except Exception:  # noqa: BLE001 - an empty body is fine
        return {}
