"""Shared fixtures.

Note what is *not* here: nothing in the test suite reaches a network.  Feeds are
exercised through recorded frames and the replay adapter, which is exactly the
boundary the live adapter is written against.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from aurum.config import Config, load_config  # noqa: E402
from aurum.storage import SqliteDatabase  # noqa: E402
from aurum.storage.repositories import Repositories  # noqa: E402


@pytest.fixture
def config(tmp_path: Path) -> Config:
    cfg = load_config(
        ROOT / "config.yaml",
        use_env=False,
        overrides={
            "app": {"data_dir": str(tmp_path), "log_level": "WARNING"},
            "diagnostics": {"export_dir": str(tmp_path / "exports")},
            "research": {"min_warmup_s": 0.0},
        },
    )
    return cfg


@pytest.fixture(scope="session")
def replay_file(tmp_path_factory) -> Path:
    """A small deterministic replay file, generated once for the whole session.

    Built by the developer tool in ``tools/`` rather than by anything inside the
    ``aurum`` package — the runtime has no market-data generator to import.
    """
    sys.path.insert(0, str(ROOT / "tools"))
    from make_replay import generate  # noqa: PLC0415

    path = tmp_path_factory.mktemp("replay") / "replay.jsonl"
    generate(
        path,
        minutes=1.0,
        seed=11,
        symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        step_ms=100,
        snapshot_every_s=30.0,
    )
    return path


@pytest.fixture
def db(tmp_path: Path):
    database = SqliteDatabase(tmp_path / "test.db", batch_size=8, flush_interval_ms=20)
    database.connect()
    database.create_schema()
    yield database
    database.close()


@pytest.fixture
def repos(db) -> Repositories:
    return Repositories(db)
