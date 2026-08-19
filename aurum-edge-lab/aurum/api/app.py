"""FastAPI application.

The runtime's lifetime is the app's lifetime: it starts before the first request
is served and stops after the last, so there is no window in which a route can
observe a half-built system.  Routes that arrive before startup finishes get a
503 with a reason rather than an exception trace.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from ..config import Config
from ..logging_setup import get_logger
from ..runtime import AurumRuntime
from .routes import router
from .ws import LiveBroadcaster

log = get_logger("api")


def live_payload(runtime: AurumRuntime) -> dict[str, Any]:
    """The state a dashboard needs on every tick.

    Kept deliberately smaller than the sum of the REST endpoints: this goes out
    twice a second to every client, so it carries live state and leaves history
    to the REST routes that can be called on demand.
    """
    champion = runtime.lifecycle.champion()
    return {
        "type": "state",
        "ts_ms": runtime.data.data_time_ms or None,
        "status": runtime._status(),
        "feed": {
            "kind": runtime.feed.kind,
            "live": runtime.feed.kind == "live",
            "state": runtime.feed.state.value,
            "symbols_live": len(runtime.data.connected_symbols()),
            "symbols_configured": len(runtime.data.symbols),
            "events_processed": runtime.data.processed,
        },
        "markets": runtime.data.market_summary(),
        "wallet": runtime.wallet.to_dict(),
        "cycle": runtime.cycles.to_dict(),
        "positions": [p.to_dict() for p in runtime.broker.open_positions().values()],
        "recent_trades": [t.to_dict() for t in runtime.broker.recent_trades(limit=15)],
        "champion": champion.to_dict() if champion else None,
        "no_edge_reason": runtime.director.no_edge_reason,
        "strategy_counts": runtime.lifecycle.counts(),
        "research": {
            "cycles_run": runtime.director.cycles_run,
            "hypotheses": len(runtime.director.hypotheses),
            "validated": runtime.director.lab.validated,
            "rejected": runtime.director.lab.rejected,
            "memory_entries": runtime.director.memory.stats()["entries"],
            "shadow_pending": len(runtime.director.shadow_pending),
        },
        "agents": [
            {
                "name": state["name"],
                "family": state["family"],
                "runs": state["metrics"]["runs"],
                "proposed": state["metrics"]["proposed"],
                "blocked_by_memory": state["metrics"]["blocked_by_memory"],
                "errors": state["metrics"]["errors"],
                "last_run_ms": state["metrics"]["last_run_ms"],
            }
            for state in runtime.director.agent_states()
        ],
        "data_quality": runtime.data.gate.summary(runtime.data.qualities),
        "diagnostics": {
            "summary": runtime.diagnostics.summary_sentence(),
            "accepted": runtime.diagnostics.accepted,
            "evaluated": runtime.diagnostics.evaluated,
            "top_rejections": runtime.diagnostics.report()["rejections"][:6],
        },
        "risk": runtime.risk.circuit_breaker_state(),
        "errors": runtime.errors[-5:],
    }


def route_paths(app: FastAPI) -> list[str]:
    """Every path the app serves.

    FastAPI 0.141 keeps an included router as a single ``_IncludedRouter`` entry
    in ``app.routes`` and resolves its children at request time, so walking
    ``app.routes`` alone reports six paths for an app that serves thirty. The
    wrapper's ``original_router`` is where the rest live.
    """
    found: set[str] = set()

    def walk(routes: Any) -> None:
        for route in routes:
            path = getattr(route, "path", None)
            if isinstance(path, str) and path:
                found.add(path)
            nested = getattr(route, "original_router", None)
            if nested is not None:
                walk(nested.routes)
            children = getattr(route, "routes", None)
            if children:
                walk(children)

    walk(app.routes)
    return sorted(found)


def create_app(config: Config, *, runtime: AurumRuntime | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = runtime or AurumRuntime(config)
        app.state.runtime = engine
        await engine.start()
        broadcaster = LiveBroadcaster(
            lambda: live_payload(engine), interval_ms=config.api.ws_broadcast_interval_ms
        )
        app.state.broadcaster = broadcaster
        await broadcaster.start()
        log.info(
            "api ready",
            extra={"host": config.api.host, "port": config.api.port, "feed": engine.feed.kind},
        )
        try:
            yield
        finally:
            await broadcaster.stop()
            await engine.stop()

    app = FastAPI(
        title=config.app.name,
        version=config.app.version,
        description=(
            "Autonomous research engine for crypto perpetual futures. Paper trading only: "
            "this service has no live-order code path and holds no venue credentials."
        ),
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.api.cors_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )
    app.include_router(router)

    @app.get("/")
    def index() -> dict[str, Any]:
        return {
            "name": config.app.name,
            "version": config.app.version,
            "paper_trading_only": True,
            "endpoints": route_paths(app),
        }

    @app.websocket("/ws/live")
    async def ws_live(websocket: WebSocket) -> None:
        broadcaster: LiveBroadcaster = websocket.app.state.broadcaster
        await broadcaster.connect(websocket)
        try:
            while True:
                # The client sends nothing; this keeps the connection open and
                # notices a disconnect promptly.
                await websocket.receive_text()
        except WebSocketDisconnect:
            broadcaster.disconnect(websocket)
        except Exception:  # noqa: BLE001
            broadcaster.disconnect(websocket)

    return app
