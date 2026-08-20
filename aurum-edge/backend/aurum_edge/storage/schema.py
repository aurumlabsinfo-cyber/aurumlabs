"""Versioned schema for the one canonical AURUM EDGE database.

Rules that the rest of the system relies on:

* migrations are append-only and numbered; a migration is never edited in place;
* ``EXPECTED_SCHEMA`` is checked at start-up, so a database that drifted from the
  code refuses to start instead of losing writes at run time;
* every table carries ``run_id`` so a run can be isolated, compared or replayed.
"""

from __future__ import annotations

SCHEMA_VERSION = 4


MIGRATIONS: list[tuple[int, str, tuple[str, ...]]] = [
    (
        1,
        "core",
        (
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS migrations (
                version    INTEGER PRIMARY KEY,
                name       TEXT NOT NULL,
                applied_at REAL NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id         TEXT PRIMARY KEY,
                started_at     REAL NOT NULL,
                ended_at       REAL,
                mode           TEXT NOT NULL,
                feed_source    TEXT NOT NULL,
                model_version  TEXT,
                code_version   TEXT,
                schema_version INTEGER NOT NULL,
                config_json    TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id    TEXT NOT NULL,
                ts_ms     REAL NOT NULL,
                symbol    TEXT NOT NULL,
                source    TEXT NOT NULL,
                data_json TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_snapshots_run_symbol ON snapshots(run_id, symbol, ts_ms)",
            """
            CREATE TABLE IF NOT EXISTS decisions (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id            TEXT NOT NULL,
                ts_ms             REAL NOT NULL,
                symbol            TEXT NOT NULL,
                action            TEXT NOT NULL,
                side              TEXT,
                quality           REAL NOT NULL,
                probability       REAL NOT NULL,
                expected_move_bps REAL NOT NULL,
                expected_cost_eur REAL NOT NULL,
                margin_eur        REAL NOT NULL,
                leverage          REAL NOT NULL,
                notional_eur      REAL NOT NULL,
                target_eur        REAL NOT NULL,
                max_loss_eur      REAL NOT NULL,
                max_hold_s        REAL NOT NULL,
                expectancy_eur    REAL NOT NULL,
                reason            TEXT NOT NULL,
                reasons_json      TEXT NOT NULL,
                features_json     TEXT NOT NULL,
                snapshot_id       INTEGER,
                model_version     TEXT NOT NULL,
                shadow            INTEGER NOT NULL DEFAULT 0,
                executed          INTEGER NOT NULL DEFAULT 0
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_decisions_run_ts ON decisions(run_id, ts_ms)",
            "CREATE INDEX IF NOT EXISTS idx_decisions_action ON decisions(run_id, action, ts_ms)",
            """
            CREATE TABLE IF NOT EXISTS reject_counters (
                run_id        TEXT NOT NULL,
                minute_bucket INTEGER NOT NULL,
                reason        TEXT NOT NULL,
                count         INTEGER NOT NULL,
                PRIMARY KEY (run_id, minute_bucket, reason)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS orders (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id            TEXT NOT NULL,
                order_link_id     TEXT NOT NULL UNIQUE,
                exchange_order_id TEXT,
                decision_id       INTEGER,
                symbol            TEXT NOT NULL,
                side              TEXT NOT NULL,
                order_type        TEXT NOT NULL,
                qty               REAL NOT NULL,
                price             REAL,
                reduce_only       INTEGER NOT NULL DEFAULT 0,
                status            TEXT NOT NULL,
                intent_ts         REAL NOT NULL,
                ack_ts            REAL,
                filled_ts         REAL,
                filled_qty        REAL NOT NULL DEFAULT 0,
                avg_price         REAL,
                fee_usdt          REAL NOT NULL DEFAULT 0,
                reject_reason     TEXT,
                raw_json          TEXT
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_orders_run ON orders(run_id, intent_ts)",
            """
            CREATE TABLE IF NOT EXISTS executions (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id            TEXT NOT NULL,
                exec_id           TEXT NOT NULL UNIQUE,
                order_link_id     TEXT,
                exchange_order_id TEXT,
                symbol            TEXT NOT NULL,
                side              TEXT NOT NULL,
                price             REAL NOT NULL,
                qty               REAL NOT NULL,
                fee               REAL NOT NULL,
                is_maker          INTEGER NOT NULL DEFAULT 0,
                ts_ms             REAL NOT NULL,
                raw_json          TEXT
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_exec_link ON executions(order_link_id)",
            """
            CREATE TABLE IF NOT EXISTS positions (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id       TEXT NOT NULL,
                position_key TEXT NOT NULL UNIQUE,
                symbol       TEXT NOT NULL,
                side         TEXT NOT NULL,
                qty          REAL NOT NULL,
                entry_price  REAL NOT NULL,
                leverage     REAL NOT NULL,
                margin_eur   REAL NOT NULL,
                opened_ts    REAL NOT NULL,
                closed_ts    REAL,
                status       TEXT NOT NULL,
                decision_id  INTEGER
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS trades (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id              TEXT NOT NULL,
                position_key        TEXT NOT NULL UNIQUE,
                symbol              TEXT NOT NULL,
                side                TEXT NOT NULL,
                decision_id         INTEGER,
                snapshot_id         INTEGER,
                model_version       TEXT NOT NULL,
                mode                TEXT NOT NULL,
                entry_ts            REAL NOT NULL,
                exit_ts             REAL NOT NULL,
                hold_s              REAL NOT NULL,
                entry_price         REAL NOT NULL,
                exit_price          REAL NOT NULL,
                entry_ref_price     REAL NOT NULL,
                exit_ref_price      REAL NOT NULL,
                qty                 REAL NOT NULL,
                notional_eur        REAL NOT NULL,
                leverage            REAL NOT NULL,
                margin_eur          REAL NOT NULL,
                expected_cost_eur   REAL NOT NULL,
                entry_fee_eur       REAL NOT NULL,
                exit_fee_eur        REAL NOT NULL,
                fees_eur            REAL NOT NULL,
                slippage_entry_bps  REAL NOT NULL,
                slippage_exit_bps   REAL NOT NULL,
                slippage_eur        REAL NOT NULL,
                gross_pnl_eur       REAL NOT NULL,
                net_pnl_eur         REAL NOT NULL,
                mfe_eur             REAL NOT NULL,
                mae_eur             REAL NOT NULL,
                entry_reason        TEXT NOT NULL,
                exit_reason         TEXT NOT NULL,
                prediction_json     TEXT NOT NULL,
                evolution_json      TEXT NOT NULL,
                features_json       TEXT NOT NULL,
                label               INTEGER NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_trades_run ON trades(run_id, exit_ts)",
            "CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol, exit_ts)",
            """
            CREATE TABLE IF NOT EXISTS equity (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id         TEXT NOT NULL,
                ts_ms          REAL NOT NULL,
                equity_eur     REAL NOT NULL,
                free_eur       REAL NOT NULL,
                used_eur       REAL NOT NULL,
                exposure_eur   REAL NOT NULL,
                open_positions INTEGER NOT NULL,
                realized_eur   REAL NOT NULL,
                unrealized_eur REAL NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_equity_run ON equity(run_id, ts_ms)",
            """
            CREATE TABLE IF NOT EXISTS model_versions (
                version      TEXT PRIMARY KEY,
                created_ts   REAL NOT NULL,
                kind         TEXT NOT NULL,
                status       TEXT NOT NULL,
                parent       TEXT,
                params_json  TEXT NOT NULL,
                metrics_json TEXT NOT NULL,
                promoted_ts  REAL,
                retired_ts   REAL,
                notes        TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS model_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_ms       REAL NOT NULL,
                version     TEXT NOT NULL,
                event       TEXT NOT NULL,
                detail_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS health_events (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id    TEXT NOT NULL,
                ts_ms     REAL NOT NULL,
                component TEXT NOT NULL,
                state     TEXT NOT NULL,
                detail    TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS write_failures (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_ms        REAL NOT NULL,
                table_name   TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                error        TEXT NOT NULL,
                replayed     INTEGER NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS kv (
                key       TEXT PRIMARY KEY,
                value     TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """,
        ),
    ),
    (
        2,
        "symbol_cost_stats",
        (
            """
            CREATE TABLE IF NOT EXISTS symbol_stats (
                symbol            TEXT PRIMARY KEY,
                trades            INTEGER NOT NULL DEFAULT 0,
                wins              INTEGER NOT NULL DEFAULT 0,
                net_eur           REAL NOT NULL DEFAULT 0,
                slippage_bps_mean REAL NOT NULL DEFAULT 0,
                updated_at        REAL NOT NULL
            )
            """,
        ),
    ),
    (
        3,
        "reconciliation_log",
        (
            """
            CREATE TABLE IF NOT EXISTS reconciliations (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id         TEXT NOT NULL,
                ts_ms          REAL NOT NULL,
                trigger        TEXT NOT NULL,
                wallet_json    TEXT NOT NULL,
                positions_json TEXT NOT NULL,
                orders_json    TEXT NOT NULL,
                diff_json      TEXT NOT NULL,
                outcome        TEXT NOT NULL
            )
            """,
        ),
    ),
    (
        4,
        "decision_outcomes",
        (
            # What actually happened after a decision - including after a NO TRADE.
            # This is what turns rejections and shadow decisions into training data
            # instead of a blind spot.
            "ALTER TABLE decisions ADD COLUMN outcome_move_bps REAL",
            "ALTER TABLE decisions ADD COLUMN outcome_cost_bps REAL",
            "ALTER TABLE decisions ADD COLUMN outcome_label INTEGER",
            "ALTER TABLE decisions ADD COLUMN outcome_ts REAL",
            "ALTER TABLE decisions ADD COLUMN outcome_horizon_s REAL",
            "CREATE INDEX IF NOT EXISTS idx_decisions_outcome "
            "ON decisions(outcome_label, ts_ms)",
        ),
    ),
]


# table -> required columns.  Checked at start-up against PRAGMA table_info.
EXPECTED_SCHEMA: dict[str, tuple[str, ...]] = {
    "schema_meta": ("key", "value"),
    "migrations": ("version", "name", "applied_at"),
    "runs": (
        "run_id", "started_at", "ended_at", "mode", "feed_source", "model_version",
        "code_version", "schema_version", "config_json",
    ),
    "snapshots": ("id", "run_id", "ts_ms", "symbol", "source", "data_json"),
    "decisions": (
        "id", "run_id", "ts_ms", "symbol", "action", "side", "quality", "probability",
        "expected_move_bps", "expected_cost_eur", "margin_eur", "leverage",
        "notional_eur", "target_eur", "max_loss_eur", "max_hold_s", "expectancy_eur",
        "reason", "reasons_json", "features_json", "snapshot_id", "model_version",
        "shadow", "executed", "outcome_move_bps", "outcome_cost_bps", "outcome_label",
        "outcome_ts", "outcome_horizon_s",
    ),
    "reject_counters": ("run_id", "minute_bucket", "reason", "count"),
    "orders": (
        "id", "run_id", "order_link_id", "exchange_order_id", "decision_id", "symbol",
        "side", "order_type", "qty", "price", "reduce_only", "status", "intent_ts",
        "ack_ts", "filled_ts", "filled_qty", "avg_price", "fee_usdt", "reject_reason",
        "raw_json",
    ),
    "executions": (
        "id", "run_id", "exec_id", "order_link_id", "exchange_order_id", "symbol",
        "side", "price", "qty", "fee", "is_maker", "ts_ms", "raw_json",
    ),
    "positions": (
        "id", "run_id", "position_key", "symbol", "side", "qty", "entry_price",
        "leverage", "margin_eur", "opened_ts", "closed_ts", "status", "decision_id",
    ),
    "trades": (
        "id", "run_id", "position_key", "symbol", "side", "decision_id", "snapshot_id",
        "model_version", "mode", "entry_ts", "exit_ts", "hold_s", "entry_price",
        "exit_price", "entry_ref_price", "exit_ref_price", "qty", "notional_eur",
        "leverage", "margin_eur", "expected_cost_eur", "entry_fee_eur", "exit_fee_eur",
        "fees_eur", "slippage_entry_bps", "slippage_exit_bps", "slippage_eur",
        "gross_pnl_eur", "net_pnl_eur", "mfe_eur", "mae_eur", "entry_reason",
        "exit_reason", "prediction_json", "evolution_json", "features_json", "label",
    ),
    "equity": (
        "id", "run_id", "ts_ms", "equity_eur", "free_eur", "used_eur", "exposure_eur",
        "open_positions", "realized_eur", "unrealized_eur",
    ),
    "model_versions": (
        "version", "created_ts", "kind", "status", "parent", "params_json",
        "metrics_json", "promoted_ts", "retired_ts", "notes",
    ),
    "model_events": ("id", "ts_ms", "version", "event", "detail_json"),
    "health_events": ("id", "run_id", "ts_ms", "component", "state", "detail"),
    "write_failures": ("id", "ts_ms", "table_name", "payload_json", "error", "replayed"),
    "kv": ("key", "value", "updated_at"),
    "symbol_stats": (
        "symbol", "trades", "wins", "net_eur", "slippage_bps_mean", "updated_at",
    ),
    "reconciliations": (
        "id", "run_id", "ts_ms", "trigger", "wallet_json", "positions_json",
        "orders_json", "diff_json", "outcome",
    ),
}
