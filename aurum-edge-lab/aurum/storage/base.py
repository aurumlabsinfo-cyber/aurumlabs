"""Storage abstraction.

Everything above this line speaks in rows and dictionaries; only the driver
below it knows SQL dialects, parameter styles or connection pools.  That is the
whole point — the MVP runs on SQLite, and moving to PostgreSQL means writing one
more subclass, not editing the research engine.

Writes come in two flavours and the distinction is deliberate:

``execute``       synchronous, ordered, durable.  Used for anything a human or a
                  later cycle must be able to trust: wallet, ledger, cycles,
                  strategies, trades.
``enqueue``       non-blocking, batched by a background writer.  Used for the
                  high-frequency tables.  Dropping a market_events row under
                  extreme load is survivable; stalling the socket reader to
                  write it is not.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any, Iterable, Sequence

from .schema import HIGH_FREQUENCY_TABLES, TABLES_BY_NAME, insert_sql


def encode_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"), default=str)


def decode_json(value: Any, default: Any = None) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


class Database(ABC):
    """Minimal relational surface the rest of the system is allowed to use."""

    dialect: str = "sqlite"

    # ------------------------------------------------------------ lifecycle
    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def create_schema(self) -> None: ...

    # --------------------------------------------------------------- writes
    @abstractmethod
    def execute(self, sql: str, params: Sequence[Any] = ()) -> int: ...

    @abstractmethod
    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> int: ...

    @abstractmethod
    def enqueue(self, table: str, row: dict[str, Any]) -> bool: ...

    @abstractmethod
    def flush(self, timeout: float = 5.0) -> int: ...

    # ---------------------------------------------------------------- reads
    @abstractmethod
    def query(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]: ...

    def query_one(self, sql: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def scalar(self, sql: str, params: Sequence[Any] = (), default: Any = None) -> Any:
        row = self.query_one(sql, params)
        if not row:
            return default
        return next(iter(row.values()), default)

    # ------------------------------------------------------------ utilities
    @abstractmethod
    def stats(self) -> dict[str, Any]: ...

    def insert(self, table: str, row: dict[str, Any]) -> int:
        """Synchronous insert of one row, JSON columns encoded automatically."""
        prepared = self.prepare_row(table, row)
        columns = tuple(prepared.keys())
        return self.execute(insert_sql(table, columns), tuple(prepared.values()))

    def insert_many(self, table: str, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        prepared = [self.prepare_row(table, row) for row in rows]
        columns = tuple(prepared[0].keys())
        payload = [tuple(row.get(col) for col in columns) for row in prepared]
        return self.executemany(insert_sql(table, columns), payload)

    def prepare_row(self, table: str, row: dict[str, Any]) -> dict[str, Any]:
        """Coerce a Python row into driver-safe values (JSON, bools)."""
        spec = TABLES_BY_NAME.get(table)
        if spec is None:
            raise KeyError(f"unknown table {table!r}")
        known = {c.name: c for c in spec.columns}
        out: dict[str, Any] = {}
        for key, value in row.items():
            column = known.get(key)
            if column is None:
                raise KeyError(f"unknown column {table}.{key}")
            if column.type == "json":
                out[key] = encode_json(value) if self.dialect == "sqlite" else value
            elif column.type == "bool":
                out[key] = int(bool(value)) if value is not None else None
            else:
                out[key] = value
        return out

    def write(self, table: str, row: dict[str, Any]) -> None:
        """Route a row to the right write path for its table."""
        if table in HIGH_FREQUENCY_TABLES:
            self.enqueue(table, row)
        else:
            self.insert(table, row)
