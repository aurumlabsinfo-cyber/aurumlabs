"""The dashboard, in a real browser, against a real backend.

Skipped when Playwright is not installed.  What it proves: the page loads, the
websocket connects, and the numbers on screen are the numbers the backend just
returned - not a placeholder, not a cached render.
"""

from __future__ import annotations

import asyncio
import http.server
import socketserver
import threading
from pathlib import Path

import aiohttp
import pytest

from aurum_edge.api.server import ApiServer
from aurum_edge.config import Config
from aurum_edge.engine import Engine
from aurum_edge.execute.broker import PaperBroker
from aurum_edge.simulator import SimulatedMarketCore
from aurum_edge.storage.db import Database
from aurum_edge.util.clock import Clock

async_playwright = pytest.importorskip(
    "playwright.async_api", reason="playwright is not installed"
).async_playwright

FRONTEND = Path(__file__).resolve().parents[2] / "frontend"


def chromium_path() -> str | None:
    """The pre-installed browser, wherever this machine keeps it."""
    for candidate in Path("/opt/pw-browsers").glob("chromium*/chrome-linux/chrome"):
        return str(candidate)
    return None


class _Server(threading.Thread):
    """Serves the frontend directory, exactly as frontend/serve.py does."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        handler = lambda *a, **k: http.server.SimpleHTTPRequestHandler(  # noqa: E731
            *a, directory=str(FRONTEND), **k
        )
        socketserver.TCPServer.allow_reuse_address = True
        self.httpd = socketserver.TCPServer(("127.0.0.1", 0), handler)
        self.port = self.httpd.server_address[1]

    def run(self) -> None:
        self.httpd.serve_forever()

    def stop(self) -> None:
        self.httpd.shutdown()


@pytest.mark.slow
async def test_dashboard_renders_the_backend_state(cfg: Config) -> None:
    clock = Clock()
    market = SimulatedMarketCore(cfg, clock, symbols=6, seed=4, step_ms=25.0)
    engine = Engine(
        cfg, clock, market=market, broker=PaperBroker(cfg, clock), db=Database(cfg.db_path)
    )
    await engine.start()
    api = ApiServer(cfg, engine)
    await api.start()
    ui = _Server()
    ui.start()
    await asyncio.sleep(2.0)

    try:
        api_url = f"http://127.0.0.1:{api.bound_port}"
        page_url = f"http://127.0.0.1:{ui.port}/index.html?api={api_url}"

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                executable_path=chromium_path(), args=["--no-sandbox"]
            )
            page = await browser.new_page()
            errors: list[str] = []
            page.on("pageerror", lambda exc: errors.append(str(exc)))
            page.on("console", lambda msg: errors.append(msg.text)
                    if msg.type == "error" else None)

            await page.goto(page_url)
            await page.wait_for_selector("#kpis .kpi", timeout=15_000)
            await page.wait_for_function(
                "document.querySelector('#link-badge').textContent.includes('LIVE')",
                timeout=15_000,
            )
            await asyncio.sleep(1.5)

            assert not errors, f"the dashboard logged errors: {errors[:3]}"

            mode = await page.inner_text("#mode-badge")
            feed = await page.inner_text("#feed-badge")
            assert mode == "PAPER"
            assert "FAKE" in feed, "a simulated feed must be visible as such"

            # the components panel mirrors the backend's health object
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{api_url}/api/state") as resp:
                    state = await resp.json()
            rendered = await page.eval_on_selector_all(
                "#components .component .name", "els => els.map(e => e.textContent)"
            )
            for component in state["health"]["components"]:
                assert any(component["name"] in text for text in rendered), component["name"]

            body = await page.inner_text("body")
            assert state["run_id"] in body
            assert state["db_path"] in body
            assert f"{state['market']['symbols']}" in body

            # tables are filled, or explicitly say they are empty
            for table in ("positions", "opportunities", "trades", "reasons"):
                rows = await page.eval_on_selector_all(f"#{table} tbody tr", "els => els.length")
                assert rows >= 1, f"table {table} rendered nothing at all"

            await page.screenshot(path="/tmp/aurum_dashboard.png", full_page=True)
            await browser.close()
    finally:
        ui.stop()
        await api.stop()
        await engine.stop(flatten=False)


@pytest.mark.slow
async def test_dashboard_greys_out_when_the_backend_dies(cfg: Config) -> None:
    """An old screen must never be readable as a live one."""
    clock = Clock()
    market = SimulatedMarketCore(cfg, clock, symbols=4, seed=6, step_ms=25.0)
    engine = Engine(
        cfg, clock, market=market, broker=PaperBroker(cfg, clock), db=Database(cfg.db_path)
    )
    await engine.start()
    api = ApiServer(cfg, engine)
    await api.start()
    ui = _Server()
    ui.start()
    await asyncio.sleep(1.5)

    try:
        api_url = f"http://127.0.0.1:{api.bound_port}"
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                executable_path=chromium_path(), args=["--no-sandbox"]
            )
            page = await browser.new_page()
            await page.goto(f"http://127.0.0.1:{ui.port}/index.html?api={api_url}")
            await page.wait_for_selector("#kpis .kpi", timeout=15_000)

            await api.stop()                      # the backend goes away
            await page.wait_for_function(
                "document.getElementById('banner').textContent.includes('OLD')",
                timeout=20_000,
            )
            banner = await page.inner_text("#banner")
            assert "not live" in banner.lower() or "OLD" in banner
            opacity = await page.evaluate("getComputedStyle(document.body).opacity")
            assert float(opacity) < 1.0, "a dead backend must visibly grey the screen"
            await browser.close()
    finally:
        ui.stop()
        await engine.stop(flatten=False)
