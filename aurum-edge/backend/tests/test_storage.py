"""The canonical database: migrations, validation, dead letter, arithmetic."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from aurum_edge.storage.db import Database, SchemaError
from aurum_edge.storage.repo import Repo, compute_stats
from aurum_edge.storage.schema import EXPECTED_SCHEMA, MIGRATIONS, SCHEMA_VERSION


def test_migrations_apply_once_and_are_recorded(db_path: str) -> None:
    first = Database(db_path).open()
    assert first.schema_version() == SCHEMA_VERSION
    assert first.applied_versions() == {version for version, _, _ in MIGRATIONS}
    first.close()

    second = Database(db_path).open()
    assert second.migrate() == []          # nothing left to apply
    assert second.schema_version() == SCHEMA_VERSION
    second.close()


def test_path_is_absolute_and_single(db_path: str) -> None:
    relative = Database("./relative_test.sqlite3")
    assert Path(relative.path).is_absolute()
    db = Database(db_path).open()
    stored = db.query_one("SELECT value FROM schema_meta WHERE key='db_path'")
    assert stored["value"] == db.path
    db.close()


def test_every_expected_table_exists(db: Database) -> None:
    for table, columns in EXPECTED_SCHEMA.items():
        info = db.query(f"PRAGMA table_info({table})")
        present = {row["name"] for row in info}
        assert not set(columns) - present, f"{table} is missing columns"


def test_validation_refuses_a_drifted_database(db_path: str) -> None:
    db = Database(db_path).open()
    db.conn.execute("ALTER TABLE trades RENAME TO trades_old")
    db.conn.commit()
    with pytest.raises(SchemaError) as exc:
        db.validate()
    assert "trades" in str(exc.value)
    db.close()


def test_failed_write_is_dead_lettered_not_lost(db: Database) -> None:
    ok = db.insert("trades", {"not_a_column": 1})
    assert ok is None
    assert db.write_failures == 1
    assert db.pending_write_failures() == 1
    row = db.query_one("SELECT * FROM write_failures WHERE replayed=0")
    assert row["table_name"] == "trades"
    assert "not_a_column" in row["payload_json"]


def test_dead_letter_can_be_replayed_after_repair(db: Database) -> None:
    db.conn.execute("ALTER TABLE kv RENAME TO kv_broken")
    db.conn.commit()
    assert db.insert("kv", {"key": "a", "value": "b", "updated_at": 1.0}) is None
    assert db.pending_write_failures() == 1

    db.conn.execute("ALTER TABLE kv_broken RENAME TO kv")
    db.conn.commit()
    replayed, failed = db.replay_write_failures()
    assert (replayed, failed) == (1, 0)
    assert db.kv_get("a") == "b"
    assert db.pending_write_failures() == 0


def test_run_id_is_stamped_on_every_row(repo: Repo) -> None:
    repo.db.start_run("paper", "fake", "champion-x", "{}", "1.0.0")
    repo.save_equity({"ts_ms": 1.0, "equity_eur": 10, "free_eur": 10, "used_eur": 0,
                      "exposure_eur": 0, "open_positions": 0, "realized_eur": 0,
                      "unrealized_eur": 0})
    row = repo.db.query_one("SELECT run_id FROM equity")
    assert row["run_id"] == repo.db.run_id


def _trade(net: float, gross: float, fees: float, slip: float, hold: float = 10.0,
           entry: float = 0.0) -> dict:
    return {
        "net_pnl_eur": net, "gross_pnl_eur": gross, "fees_eur": fees, "slippage_eur": slip,
        "hold_s": hold, "entry_ts": entry, "exit_ts": entry + hold * 1000,
    }


def test_stats_arithmetic_is_exact() -> None:
    trades = [
        _trade(net=2.0, gross=2.6, fees=0.5, slip=0.1, entry=0),
        _trade(net=-1.0, gross=-0.4, fees=0.5, slip=0.1, entry=60_000),
        _trade(net=3.0, gross=3.6, fees=0.5, slip=0.1, entry=120_000),
    ]
    stats = compute_stats(trades)
    assert stats.trades == 3
    assert stats.wins == 2 and stats.losses == 1
    assert stats.win_rate == pytest.approx(2 / 3)
    assert stats.net_pnl_eur == pytest.approx(4.0)
    assert stats.gross_pnl_eur == pytest.approx(5.8)
    assert stats.fees_eur == pytest.approx(1.5)
    assert stats.slippage_eur == pytest.approx(0.3)
    # the identity the dashboard shows must hold on aggregates too
    assert stats.gross_pnl_eur - stats.fees_eur - stats.slippage_eur == pytest.approx(
        stats.net_pnl_eur
    )
    assert stats.expectancy_eur == pytest.approx(4.0 / 3)
    assert stats.avg_win_eur == pytest.approx(2.5)
    assert stats.avg_loss_eur == pytest.approx(-1.0)
    assert stats.max_drawdown_eur == pytest.approx(1.0)
    assert stats.trades_per_hour > 0


def test_execution_ids_are_unique(repo: Repo) -> None:
    row = {
        "exec_id": "e1", "order_link_id": "AEx", "exchange_order_id": "o1",
        "symbol": "BTCUSDT", "side": "Buy", "price": 1.0, "qty": 1.0, "fee": 0.0,
        "is_maker": 0, "ts_ms": 1.0, "raw_json": "{}",
    }
    repo.save_execution(row)
    repo.save_execution(row)          # upsert, not a duplicate
    count = repo.db.query_one("SELECT COUNT(*) AS n FROM executions")
    assert count["n"] == 1
    assert repo.execution_exists("e1")


def test_write_failures_do_not_raise_into_the_trading_loop(db: Database) -> None:
    """A broken write must degrade health, never kill the cycle."""
    try:
        db.insert("orders", {"nonexistent": "x"})
    except sqlite3.Error:  # pragma: no cover - would be the bug
        pytest.fail("a failed write must not raise into the caller")
    assert db.last_write_error is not None
