from __future__ import annotations

from aurum.domain import BookLevel, BookSnapshot, EventKind, MarketEvent, now_ms
from aurum.storage import SqliteDatabase, render_ddl
from aurum.storage.repositories import Repositories
from aurum.storage.schema import HIGH_FREQUENCY_TABLES, TABLES


def test_schema_covers_every_blueprint_table() -> None:
    required = {
        "market_events", "orderbook_snapshots", "features", "hypotheses", "experiments",
        "strategies", "strategy_versions", "signals", "paper_trades", "positions",
        "wallet", "wallet_ledger", "cycles", "cycle_postmortems", "research_memory",
        "agent_events", "system_events", "data_quality", "model_registry",
    }
    assert required <= {t.name for t in TABLES}


def test_ddl_renders_for_both_dialects() -> None:
    sqlite_ddl = "\n".join(render_ddl("sqlite"))
    postgres_ddl = "\n".join(render_ddl("postgres"))
    assert "INTEGER PRIMARY KEY AUTOINCREMENT" in sqlite_ddl
    assert "BIGSERIAL PRIMARY KEY" in postgres_ddl
    assert "JSONB" in postgres_ddl and "JSONB" not in sqlite_ddl


def test_wal_is_enabled(db: SqliteDatabase) -> None:
    assert db.query("PRAGMA journal_mode")[0]["journal_mode"].lower() == "wal"


def test_high_frequency_writes_are_batched_and_readable(db: SqliteDatabase) -> None:
    repos = Repositories(db)
    stamp = now_ms()
    for i in range(50):
        repos.market.record_event(
            MarketEvent("BTCUSDT", EventKind.TRADE, stamp + i, stamp + i + 3, {"p": 60000 + i, "q": 0.01})
        )
    db.flush(timeout=5.0)
    rows = db.query("SELECT COUNT(*) AS n FROM market_events")
    assert rows[0]["n"] == 50
    assert db.stats()["batches"] >= 1
    assert "market_events" in HIGH_FREQUENCY_TABLES


def test_enqueue_never_blocks_when_queue_is_full(db: SqliteDatabase) -> None:
    # A full queue must drop and count, not raise and not block the caller.
    db._queue.maxsize = 1
    accepted = sum(
        db.enqueue("system_events", {"ts_ms": now_ms(), "level": "INFO", "component": "t", "message": str(i)})
        for i in range(200)
    )
    db.flush(timeout=5.0)
    stats = db.stats()
    assert accepted <= 200
    assert stats["dropped"] >= 0  # dropping is allowed; silence is not
    assert stats["enqueued"] + stats["dropped"] == 200


def test_json_columns_round_trip(db: SqliteDatabase) -> None:
    repos = Repositories(db)
    book = BookSnapshot(
        symbol="ETHUSDT",
        ts_ms=now_ms(),
        recv_ms=now_ms(),
        bids=[BookLevel(2500.0, 1.5), BookLevel(2499.5, 3.0)],
        asks=[BookLevel(2500.5, 2.0), BookLevel(2501.0, 4.0)],
        last_update_id=42,
    )
    repos.market.record_book(book)
    db.flush(timeout=5.0)
    row = db.query_one("SELECT * FROM orderbook_snapshots WHERE symbol = 'ETHUSDT'")
    assert row is not None
    from aurum.storage import decode_json

    assert decode_json(row["bids"])[0] == [2500.0, 1.5]
    assert row["mid"] == 2500.25


def test_retention_prunes_only_high_frequency_tables(db: SqliteDatabase) -> None:
    repos = Repositories(db)
    old = now_ms() - 48 * 3_600_000
    repos.market.record_event(MarketEvent("BTCUSDT", EventKind.TRADE, old, old, {}))
    repos.wallet.save_cycle(
        {"cycle_id": 1, "state": "ACTIVE", "started_ms": old, "starting_balance": 100.0}
    )
    db.flush(timeout=5.0)

    removed = db.prune({"market_events_hours": 6, "features_hours": 24, "orderbook_snapshots_hours": 6})
    assert removed["market_events"] == 1
    assert db.query("SELECT COUNT(*) AS n FROM cycles")[0]["n"] == 1  # history survives


def test_unknown_column_is_a_hard_error(db: SqliteDatabase) -> None:
    import pytest

    with pytest.raises(KeyError):
        db.insert("cycles", {"cycle_id": 1, "state": "ACTIVE", "started_ms": 0, "starting_balance": 100.0,
                             "lucky_number": 7})
