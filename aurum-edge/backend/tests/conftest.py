"""Shared fixtures."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from aurum_edge.config import ApiConfig, BybitConfig, Config, DecideConfig, ExecuteConfig, ScanConfig
from aurum_edge.storage.db import Database
from aurum_edge.storage.repo import Repo
from aurum_edge.util.clock import ManualClock

from .fakebybit.server import FakeBybit


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "aurum_test.sqlite3")


@pytest.fixture
def db(db_path: str) -> Database:
    database = Database(db_path, run_id="run_test").open()
    yield database
    database.close()


@pytest.fixture
def repo(db: Database) -> Repo:
    return Repo(db)


@pytest.fixture
def cfg(db_path: str) -> Config:
    """Paper configuration with fast timings, pointed at a throwaway database."""
    return Config(
        mode="paper",
        db_path=db_path,
        paper_start_equity_eur=1000.0,
        bybit=BybitConfig(api_key="testkey", api_secret="testsecret"),
        scan=ScanConfig(
            scan_interval_s=0.05,
            focus_size=8,
            stale_feed_ms=1_000.0,
            ping_interval_s=0.2,
            ping_timeout_s=0.2,
            reconnect_base_delay_s=0.05,
            reconnect_max_delay_s=0.2,
            universe_refresh_s=1e9,
            focus_refresh_s=1e9,
        ),
        decide=DecideConfig(),
        execute=ExecuteConfig(paper_latency_ms=5.0, fill_timeout_s=2.0,
                              reconcile_interval_s=0.0),
        api=ApiConfig(host="127.0.0.1", port=0),
    )


def with_changes(cfg: Config, **changes: Any) -> Config:
    return dataclasses.replace(cfg, **changes)


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest_asyncio.fixture
async def fake_bybit() -> FakeBybit:
    server = await FakeBybit().start()
    yield server
    await server.stop()
