"""Database schema.

The DDL is described once, dialect-neutrally, and rendered per driver.  That is
what makes the SQLite MVP a migration path rather than a dead end: the same
table list produces PostgreSQL DDL with ``BIGSERIAL`` and ``JSONB`` instead of
``INTEGER PRIMARY KEY AUTOINCREMENT`` and ``TEXT``.

Every table that is written on the hot path carries a ``(symbol, ts_ms)`` or
``ts_ms`` index, because the only queries anyone ever runs against them are
"what happened to this symbol between these two instants?".
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Logical column types.
ID = "id"          # surrogate primary key
INT = "int"
BIG = "big"        # millisecond timestamps and sequence numbers
REAL = "real"
TEXT = "text"
JSON = "json"
BOOL = "bool"

_SQLITE_TYPES = {
    ID: "INTEGER PRIMARY KEY AUTOINCREMENT",
    INT: "INTEGER",
    BIG: "INTEGER",
    REAL: "REAL",
    TEXT: "TEXT",
    JSON: "TEXT",
    BOOL: "INTEGER",
}

_POSTGRES_TYPES = {
    ID: "BIGSERIAL PRIMARY KEY",
    INT: "INTEGER",
    BIG: "BIGINT",
    REAL: "DOUBLE PRECISION",
    TEXT: "TEXT",
    JSON: "JSONB",
    BOOL: "BOOLEAN",
}


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    unique: bool = False
    default: str | None = None


@dataclass(frozen=True)
class Index:
    name: str
    columns: tuple[str, ...]
    unique: bool = False


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[Column, ...]
    indexes: tuple[Index, ...] = field(default_factory=tuple)
    #: high-frequency tables are written through the batching background writer
    #: and are subject to retention pruning
    high_frequency: bool = False
    retention_key: str | None = None

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns if c.type != ID)


def _c(name: str, type_: str, *, nullable: bool = True, unique: bool = False, default: str | None = None) -> Column:
    return Column(name, type_, nullable, unique, default)


TABLES: tuple[Table, ...] = (
    # ---------------------------------------------------------------- market
    Table(
        "market_events",
        (
            _c("id", ID),
            _c("symbol", TEXT, nullable=False),
            _c("kind", TEXT, nullable=False),
            _c("ts_ms", BIG, nullable=False),
            _c("recv_ms", BIG, nullable=False),
            _c("latency_ms", INT),
            _c("source", TEXT),
            _c("payload", JSON),
        ),
        (Index("ix_market_events_symbol_ts", ("symbol", "ts_ms")), Index("ix_market_events_ts", ("ts_ms",))),
        high_frequency=True,
        retention_key="market_events_hours",
    ),
    Table(
        "orderbook_snapshots",
        (
            _c("id", ID),
            _c("symbol", TEXT, nullable=False),
            _c("ts_ms", BIG, nullable=False),
            _c("last_update_id", BIG),
            _c("best_bid", REAL),
            _c("best_ask", REAL),
            _c("mid", REAL),
            _c("spread_bps", REAL),
            _c("bid_depth_10", REAL),
            _c("ask_depth_10", REAL),
            _c("is_crossed", BOOL),
            _c("bids", JSON),
            _c("asks", JSON),
        ),
        (Index("ix_book_symbol_ts", ("symbol", "ts_ms")),),
        high_frequency=True,
        retention_key="orderbook_snapshots_hours",
    ),
    Table(
        "features",
        (
            _c("id", ID),
            _c("symbol", TEXT, nullable=False),
            _c("ts_ms", BIG, nullable=False),
            _c("regime", TEXT),
            _c("quality_score", REAL),
            _c("tradable", BOOL),
            _c("mid", REAL),
            _c("microprice", REAL),
            _c("spread_bps", REAL),
            _c("values_json", JSON),
        ),
        (Index("ix_features_symbol_ts", ("symbol", "ts_ms")),),
        high_frequency=True,
        retention_key="features_hours",
    ),
    Table(
        "data_quality",
        (
            _c("id", ID),
            _c("symbol", TEXT, nullable=False),
            _c("ts_ms", BIG, nullable=False),
            _c("score", REAL),
            _c("state", TEXT),
            _c("tradable", BOOL),
            _c("flags", JSON),
            _c("latency_ms", REAL),
            _c("spread_bps", REAL),
            _c("events_per_min", REAL),
            _c("sequence_gaps", INT),
            _c("resyncs", INT),
        ),
        (Index("ix_quality_symbol_ts", ("symbol", "ts_ms")),),
        high_frequency=True,
        retention_key="features_hours",
    ),
    # -------------------------------------------------------------- research
    Table(
        "hypotheses",
        (
            _c("id", ID),
            _c("hypothesis_id", TEXT, nullable=False, unique=True),
            _c("parent_id", TEXT),
            _c("agent", TEXT, nullable=False),
            _c("family", TEXT),
            _c("signal_symbol", TEXT, nullable=False),
            _c("execution_symbol", TEXT, nullable=False),
            _c("direction", TEXT, nullable=False),
            _c("conditions", JSON, nullable=False),
            _c("entry_delay_ms", INT),
            _c("horizon_ms", INT, nullable=False),
            _c("regime_filter", TEXT),
            _c("cost_model_version", TEXT),
            _c("dataset_start_ms", BIG),
            _c("dataset_end_ms", BIG),
            _c("sample_count", INT),
            _c("validation_status", TEXT),
            _c("fingerprint", TEXT, nullable=False),
            _c("description", TEXT),
            _c("created_ms", BIG, nullable=False),
            _c("updated_ms", BIG),
            _c("cycle_id", INT),
        ),
        (
            Index("ix_hypotheses_fingerprint", ("fingerprint",)),
            Index("ix_hypotheses_agent", ("agent",)),
            Index("ix_hypotheses_status", ("validation_status",)),
        ),
    ),
    Table(
        "experiments",
        (
            _c("id", ID),
            _c("experiment_id", TEXT, nullable=False, unique=True),
            _c("hypothesis_id", TEXT, nullable=False),
            _c("stage", TEXT, nullable=False),
            _c("passed", BOOL),
            _c("reason", TEXT),
            _c("metrics", JSON),
            _c("folds", JSON),
            _c("samples", INT),
            _c("p_value", REAL),
            _c("adjusted_p_value", REAL),
            _c("score", REAL),
            _c("dataset_start_ms", BIG),
            _c("dataset_end_ms", BIG),
            _c("created_ms", BIG, nullable=False),
        ),
        (
            Index("ix_experiments_hypothesis", ("hypothesis_id",)),
            Index("ix_experiments_stage", ("stage",)),
            Index("ix_experiments_created", ("created_ms",)),
        ),
    ),
    Table(
        "research_memory",
        (
            _c("id", ID),
            _c("fingerprint", TEXT, nullable=False, unique=True),
            _c("agent", TEXT),
            _c("family", TEXT),
            _c("signal_symbol", TEXT),
            _c("execution_symbol", TEXT),
            _c("horizon_ms", INT),
            _c("outcome", TEXT, nullable=False),
            _c("reason", TEXT),
            _c("net_edge_bps", REAL),
            _c("samples", INT),
            _c("tests", INT),
            _c("first_seen_ms", BIG),
            _c("last_seen_ms", BIG),
            _c("retest_after_ms", BIG),
            _c("conditions", JSON),
        ),
        (Index("ix_memory_outcome", ("outcome",)), Index("ix_memory_agent", ("agent",))),
    ),
    Table(
        "agent_events",
        (
            _c("id", ID),
            _c("ts_ms", BIG, nullable=False),
            _c("agent", TEXT, nullable=False),
            _c("kind", TEXT, nullable=False),
            _c("severity", TEXT),
            _c("message", TEXT),
            _c("detail", JSON),
        ),
        (Index("ix_agent_events_ts", ("ts_ms",)), Index("ix_agent_events_agent", ("agent",))),
        high_frequency=True,
        retention_key="features_hours",
    ),
    # ------------------------------------------------------------ strategies
    Table(
        "strategies",
        (
            _c("id", ID),
            _c("strategy_id", TEXT, nullable=False, unique=True),
            _c("hypothesis_id", TEXT, nullable=False),
            _c("name", TEXT),
            _c("state", TEXT, nullable=False),
            _c("version", INT, nullable=False),
            _c("score", REAL),
            _c("created_ms", BIG, nullable=False),
            _c("updated_ms", BIG),
            _c("promoted_ms", BIG),
            _c("retired_ms", BIG),
            _c("cycle_id", INT),
            _c("metrics", JSON),
        ),
        (Index("ix_strategies_state", ("state",)), Index("ix_strategies_score", ("score",))),
    ),
    Table(
        "strategy_versions",
        (
            _c("id", ID),
            _c("strategy_id", TEXT, nullable=False),
            _c("version", INT, nullable=False),
            _c("state", TEXT, nullable=False),
            _c("previous_state", TEXT),
            _c("reason", TEXT),
            _c("evidence", JSON),
            _c("definition", JSON),
            _c("created_ms", BIG, nullable=False),
        ),
        (Index("ix_versions_strategy", ("strategy_id", "version")),),
    ),
    Table(
        "model_registry",
        (
            _c("id", ID),
            _c("model_id", TEXT, nullable=False, unique=True),
            _c("kind", TEXT, nullable=False),
            _c("strategy_id", TEXT),
            _c("version", INT),
            _c("params", JSON),
            _c("calibration", JSON),
            _c("metrics", JSON),
            _c("created_ms", BIG, nullable=False),
            _c("active", BOOL),
        ),
        (Index("ix_model_registry_strategy", ("strategy_id",)),),
    ),
    # ------------------------------------------------------------- execution
    Table(
        "signals",
        (
            _c("id", ID),
            _c("signal_id", TEXT, nullable=False, unique=True),
            _c("ts_ms", BIG, nullable=False),
            _c("strategy_id", TEXT),
            _c("hypothesis_id", TEXT),
            _c("symbol", TEXT, nullable=False),
            _c("direction", TEXT),
            _c("confidence", REAL),
            _c("expected_edge_bps", REAL),
            _c("expected_cost_bps", REAL),
            _c("net_edge_bps", REAL),
            _c("horizon_ms", INT),
            _c("regime", TEXT),
            _c("accepted", BOOL),
            _c("shadow", BOOL),
            _c("exploration", BOOL),
            _c("rejection", TEXT),
            _c("rejection_detail", TEXT),
            _c("features", JSON),
            _c("cycle_id", INT),
        ),
        (
            Index("ix_signals_ts", ("ts_ms",)),
            Index("ix_signals_symbol_ts", ("symbol", "ts_ms")),
            Index("ix_signals_rejection", ("rejection",)),
        ),
        high_frequency=True,
        retention_key="features_hours",
    ),
    Table(
        "positions",
        (
            _c("id", ID),
            _c("position_id", TEXT, nullable=False, unique=True),
            _c("symbol", TEXT, nullable=False),
            _c("direction", TEXT, nullable=False),
            _c("qty", REAL),
            _c("entry_ts_ms", BIG),
            _c("entry_price", REAL),
            _c("requested_entry_price", REAL),
            _c("notional_eur", REAL),
            _c("margin_eur", REAL),
            _c("entry_fee_eur", REAL),
            _c("entry_slippage_bps", REAL),
            _c("stop_bps", REAL),
            _c("target_bps", REAL),
            _c("horizon_ms", INT),
            _c("strategy_id", TEXT),
            _c("hypothesis_id", TEXT),
            _c("signal_id", TEXT),
            _c("cycle_id", INT),
            _c("status", TEXT),
            _c("closed_ts_ms", BIG),
            _c("features", JSON),
        ),
        (Index("ix_positions_status", ("status",)), Index("ix_positions_symbol", ("symbol",))),
    ),
    Table(
        "paper_trades",
        (
            _c("id", ID),
            _c("trade_id", TEXT, nullable=False, unique=True),
            _c("position_id", TEXT, nullable=False),
            _c("symbol", TEXT, nullable=False),
            _c("direction", TEXT, nullable=False),
            _c("qty", REAL),
            _c("entry_ts_ms", BIG, nullable=False),
            _c("exit_ts_ms", BIG, nullable=False),
            _c("holding_ms", BIG),
            _c("requested_entry_price", REAL),
            _c("entry_price", REAL),
            _c("requested_exit_price", REAL),
            _c("exit_price", REAL),
            _c("gross_pnl_eur", REAL),
            _c("fees_eur", REAL),
            _c("net_pnl_eur", REAL),
            _c("return_bps", REAL),
            _c("net_return_bps", REAL),
            _c("entry_slippage_bps", REAL),
            _c("exit_slippage_bps", REAL),
            _c("cost_bps", REAL),
            _c("expected_edge_bps", REAL),
            _c("exit_reason", TEXT),
            _c("strategy_id", TEXT),
            _c("strategy_version", INT),
            _c("hypothesis_id", TEXT),
            _c("signal_id", TEXT),
            _c("cycle_id", INT),
            _c("regime", TEXT),
            _c("cost_model_version", TEXT),
            _c("features", JSON),
        ),
        (
            Index("ix_trades_exit_ts", ("exit_ts_ms",)),
            Index("ix_trades_cycle", ("cycle_id",)),
            Index("ix_trades_strategy", ("strategy_id",)),
        ),
    ),
    # ---------------------------------------------------------------- wallet
    Table(
        "wallet",
        (
            _c("id", ID),
            _c("cycle_id", INT, nullable=False),
            _c("ts_ms", BIG, nullable=False),
            _c("starting_balance", REAL, nullable=False),
            _c("balance", REAL, nullable=False),
            _c("available", REAL, nullable=False),
            _c("reserved", REAL, nullable=False),
            _c("equity", REAL, nullable=False),
            _c("realized_pnl", REAL),
            _c("unrealized_pnl", REAL),
            _c("peak_equity", REAL),
            _c("drawdown_pct", REAL),
            _c("trades", INT),
            _c("wins", INT),
            _c("losses", INT),
            _c("currency", TEXT),
        ),
        (Index("ix_wallet_cycle_ts", ("cycle_id", "ts_ms")),),
    ),
    Table(
        "wallet_ledger",
        (
            _c("id", ID),
            _c("ts_ms", BIG, nullable=False),
            _c("cycle_id", INT, nullable=False),
            _c("kind", TEXT, nullable=False),
            _c("amount", REAL, nullable=False),
            _c("balance_after", REAL, nullable=False),
            _c("reference", TEXT),
            _c("detail", TEXT),
        ),
        (Index("ix_ledger_cycle_ts", ("cycle_id", "ts_ms")), Index("ix_ledger_ref", ("reference",))),
    ),
    Table(
        "cycles",
        (
            _c("id", ID),
            _c("cycle_id", INT, nullable=False, unique=True),
            _c("state", TEXT, nullable=False),
            _c("started_ms", BIG, nullable=False),
            _c("ended_ms", BIG),
            _c("starting_balance", REAL, nullable=False),
            _c("final_balance", REAL),
            _c("peak_equity", REAL),
            _c("max_drawdown_pct", REAL),
            _c("trades", INT),
            _c("wins", INT),
            _c("losses", INT),
            _c("net_pnl", REAL),
            _c("champion_strategy_id", TEXT),
            _c("end_reason", TEXT),
        ),
        (Index("ix_cycles_state", ("state",)),),
    ),
    Table(
        "cycle_postmortems",
        (
            _c("id", ID),
            _c("cycle_id", INT, nullable=False),
            _c("created_ms", BIG, nullable=False),
            _c("verdict", TEXT, nullable=False),
            _c("primary_cause", TEXT),
            _c("causes", JSON),
            _c("evidence", JSON),
            _c("recommendations", JSON),
            _c("trades_analyzed", INT),
            _c("net_pnl", REAL),
        ),
        (Index("ix_postmortem_cycle", ("cycle_id",)),),
    ),
    # ---------------------------------------------------------------- system
    Table(
        "system_events",
        (
            _c("id", ID),
            _c("ts_ms", BIG, nullable=False),
            _c("level", TEXT, nullable=False),
            _c("component", TEXT, nullable=False),
            _c("kind", TEXT),
            _c("message", TEXT),
            _c("detail", JSON),
        ),
        (Index("ix_system_events_ts", ("ts_ms",)), Index("ix_system_events_level", ("level",))),
    ),
)

TABLES_BY_NAME: dict[str, Table] = {t.name: t for t in TABLES}

#: Tables whose rows are produced faster than a synchronous write can absorb.
HIGH_FREQUENCY_TABLES: frozenset[str] = frozenset(t.name for t in TABLES if t.high_frequency)


def render_ddl(dialect: str = "sqlite") -> list[str]:
    """CREATE TABLE / CREATE INDEX statements for one dialect."""
    types = _SQLITE_TYPES if dialect == "sqlite" else _POSTGRES_TYPES
    statements: list[str] = []
    for table in TABLES:
        cols: list[str] = []
        for column in table.columns:
            piece = f"{column.name} {types[column.type]}"
            if column.type != ID:
                if not column.nullable:
                    piece += " NOT NULL"
                if column.unique:
                    piece += " UNIQUE"
                if column.default is not None:
                    piece += f" DEFAULT {column.default}"
            cols.append(piece)
        statements.append(f"CREATE TABLE IF NOT EXISTS {table.name} (\n  " + ",\n  ".join(cols) + "\n)")
        for index in table.indexes:
            unique = "UNIQUE " if index.unique else ""
            statements.append(
                f"CREATE {unique}INDEX IF NOT EXISTS {index.name} ON {table.name} ({', '.join(index.columns)})"
            )
    return statements


def insert_sql(table_name: str, columns: tuple[str, ...] | None = None) -> str:
    """Parameterised INSERT in the neutral ``?`` style; drivers translate."""
    table = TABLES_BY_NAME[table_name]
    names = columns or table.column_names
    placeholders = ", ".join("?" for _ in names)
    return f"INSERT INTO {table.name} ({', '.join(names)}) VALUES ({placeholders})"
