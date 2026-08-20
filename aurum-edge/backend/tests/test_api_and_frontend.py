"""The API, and the contract between the dashboard and the backend.

The dashboard declares in app.js exactly which fields it renders.  This module
reads that declaration out of the JavaScript and asserts the backend really
produces every one of them - so "the frontend shows the state of the backend"
is a checked property, not a claim.
"""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path
from typing import Any

import aiohttp
import pytest
import pytest_asyncio

from aurum_edge.api.server import ApiServer
from aurum_edge.config import Config
from aurum_edge.engine import Engine
from aurum_edge.execute.broker import PaperBroker
from aurum_edge.simulator import SimulatedMarketCore
from aurum_edge.storage.db import Database
from aurum_edge.util.clock import Clock

FRONTEND = Path(__file__).resolve().parents[2] / "frontend"


def required_state_paths() -> list[str]:
    source = (FRONTEND / "app.js").read_text(encoding="utf-8")
    match = re.search(r"const REQUIRED_STATE_PATHS = \[(.*?)\];", source, re.S)
    assert match, "app.js must declare REQUIRED_STATE_PATHS"
    return re.findall(r'"([^"]+)"', match.group(1))


def resolve(payload: Any, path: str) -> Any:
    node = payload
    for part in path.split("."):
        assert isinstance(node, dict), f"{path}: {part} is not reachable"
        assert part in node, f"backend state is missing '{path}'"
        node = node[part]
    return node


@pytest_asyncio.fixture
async def running(cfg: Config):
    """A live engine on a simulated feed, with the API in front of it."""
    clock = Clock()
    market = SimulatedMarketCore(cfg, clock, symbols=4, seed=5, step_ms=25.0)
    engine = Engine(
        cfg, clock, market=market, broker=PaperBroker(cfg, clock), db=Database(cfg.db_path)
    )
    await engine.start()
    api = ApiServer(cfg, engine)
    await api.start()
    await asyncio.sleep(1.2)          # let a few cycles run
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
    base = f"http://127.0.0.1:{api.bound_port}"
    try:
        yield engine, api, session, base
    finally:
        await session.close()
        await api.stop()
        await engine.stop(flatten=False)


async def test_state_endpoint_serves_the_whole_dashboard(running) -> None:
    engine, api, session, base = running
    async with session.get(f"{base}/api/state") as resp:
        assert resp.status == 200
        payload = await resp.json()

    missing = []
    for path in required_state_paths():
        try:
            resolve(payload, path)
        except AssertionError as exc:
            missing.append(str(exc))
    assert not missing, "the dashboard reads fields the backend does not send:\n" + "\n".join(missing)


async def test_frontend_declares_what_it_renders(running) -> None:
    paths = required_state_paths()
    assert len(paths) > 30
    assert "stats.net_pnl_eur" in paths and "stats.fees_eur" in paths
    assert "stats.slippage_eur" in paths and "stats.expectancy_eur" in paths
    assert "mode" in paths and "feed_source" in paths
    index = (FRONTEND / "index.html").read_text(encoding="utf-8")
    assert 'src="app.js"' in index and 'href="styles.css"' in index
    assert (FRONTEND / "serve.py").is_file()


async def test_dashboard_numbers_come_from_the_backend_not_the_browser(running) -> None:
    """The frontend must not recompute P&L: the identity is already in the payload."""
    engine, api, session, base = running
    async with session.get(f"{base}/api/state") as resp:
        payload = await resp.json()
    stats = payload["stats"]
    if stats["trades"]:
        assert stats["gross_pnl_eur"] - stats["fees_eur"] - stats["slippage_eur"] == pytest.approx(
            stats["net_pnl_eur"], abs=1e-6
        )
    source = (FRONTEND / "app.js").read_text(encoding="utf-8")
    for forbidden in ("gross_pnl_eur -", "* 0.00055", "net = gross"):
        assert forbidden not in source, "the dashboard must not do its own accounting"


async def test_every_read_endpoint_answers(running) -> None:
    engine, api, session, base = running
    for path in (
        "/", "/api/state", "/api/diagnose", "/api/opportunities", "/api/positions",
        "/api/trades", "/api/decisions", "/api/stats", "/api/model",
    ):
        async with session.get(f"{base}{path}") as resp:
            assert resp.status == 200, path
            assert await resp.json() is not None
    async with session.get(f"{base}/api/health") as resp:
        assert resp.status in (200, 503)      # 503 when trading is blocked, by design
        health = await resp.json()
    assert "components" in health and "trading_allowed" in health


async def test_diagnose_always_explains(running) -> None:
    engine, api, session, base = running
    async with session.get(f"{base}/api/diagnose") as resp:
        payload = await resp.json()
    assert "trading_allowed" in payload
    explanation = (
        payload["gate_reasons"]
        or payload["top_candidates"]
        or payload["scan_skipped"]
        or payload["reject_summary"]
    )
    assert explanation, "diagnose must never return an empty answer"
    assert payload["symbols_in_universe"] > 0
    for candidate in payload["top_candidates"]:
        assert candidate["reasons"], "a candidate without reasons is a bug"


async def test_websocket_pushes_the_same_state(running) -> None:
    engine, api, session, base = running
    async with session.ws_connect(f"{base}/ws") as ws:
        message = await asyncio.wait_for(ws.receive(), timeout=5.0)
        pushed = json.loads(message.data)
        assert pushed["type"] == "state"
        async with session.get(f"{base}/api/state") as resp:
            fetched = await resp.json()
        # same shape, same source of truth
        assert set(pushed["data"].keys()) == set(fetched.keys())
        assert pushed["data"]["run_id"] == fetched["run_id"]
        assert pushed["data"]["db_path"] == fetched["db_path"]

        second = await asyncio.wait_for(ws.receive(), timeout=5.0)
        assert json.loads(second.data)["data"]["ts_ms"] >= pushed["data"]["ts_ms"]


async def test_kill_switch_stops_trading_and_shows_up_in_state(running) -> None:
    engine, api, session, base = running
    async with session.post(f"{base}/api/control/kill", json={"reason": "test"}) as resp:
        result = await resp.json()
    assert result["kill_switch"] is True

    async with session.get(f"{base}/api/state") as resp:
        payload = await resp.json()
    assert payload["risk"]["kill_switch"] is True
    assert "test" in payload["risk"]["kill_reason"]
    assert payload["health"]["trading_allowed"] is False
    assert payload["execution"]["entries_blocked"] is True


async def test_backend_keeps_running_without_a_frontend(cfg: Config) -> None:
    """The trading core must not depend on the dashboard in any way."""
    clock = Clock()
    market = SimulatedMarketCore(cfg, clock, symbols=4, seed=9, step_ms=25.0)
    engine = Engine(
        cfg, clock, market=market, broker=PaperBroker(cfg, clock), db=Database(cfg.db_path)
    )
    await engine.start()          # no ApiServer at all
    try:
        await asyncio.sleep(1.0)
        assert engine.cycles > 3
        assert engine.scanner.last_result is not None
        assert engine.scanner.last_result.considered > 0
        assert engine.state()["health"]["components"]
    finally:
        await engine.stop(flatten=False)
    assert engine.repo.db.write_failures == 0


async def test_engine_module_never_imports_the_api(cfg: Config) -> None:
    engine_source = (
        Path(__file__).resolve().parents[1] / "aurum_edge" / "engine.py"
    ).read_text(encoding="utf-8")
    assert "from .api" not in engine_source and "import api" not in engine_source
