"""Accesso a SQLite: scritture accodate, letture che non bloccano il realtime.

Due regole non negoziabili.

**Il percorso caldo non aspetta il disco.** Il feed produce migliaia di righe
al minuto; scriverle una alla volta dal loop di decisione legherebbe la
latenza del segnale a quella del filesystem. Le righe vengono accodate in
memoria e scaricate a blocchi da un thread dedicato.

**Un guasto su una tabella non ne travolge altre.** Ogni tabella ha il suo
`try`: un errore su una non deve far perdere le righe delle altre, e il guasto
compare in `/health` invece di essere silenzioso.
"""

from __future__ import annotations

import json
import queue
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterable

from .schema import BATCH_COLUMNS, SCHEMA, SCHEMA_VERSION, UPSERT_KEYS


def now_ms() -> int:
    return int(time.time() * 1000)


class Database:
    """Un writer, molti lettori. In WAL non si bloccano a vicenda."""

    def __init__(self, path: str, flush_ms: int = 500) -> None:
        self.path = path
        self.flush_ms = max(50, flush_ms)
        self._batch: "queue.Queue[tuple[str, tuple]]" = queue.Queue(maxsize=200_000)
        self._upsert: "queue.Queue[tuple[str, dict]]" = queue.Queue(maxsize=50_000)
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._running = False
        self.stats = {"queued": 0, "written": 0, "dropped": 0, "failures": 0}
        self.last_error: str | None = None
        self.healthy = True

        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(SCHEMA)
            self.migrated = self._migrate()
            self._conn.execute(
                "INSERT INTO meta(key,value) VALUES('schema_version',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(SCHEMA_VERSION),))
            self._conn.commit()

    # -------------------------------------------------------------- schema
    def _migrate(self) -> list[str]:
        """Allinea un database gia' esistente allo schema corrente.

        `CREATE TABLE IF NOT EXISTS` non tocca una tabella che c'e' gia': senza
        questo passo, un database scritto da una versione precedente farebbe
        fallire ogni INSERT che usa una colonna nuova — in silenzio.
        """
        added: list[str] = []
        wanted = dict(BATCH_COLUMNS)
        for table, cols in wanted.items():
            try:
                have = {r["name"] for r in
                        self._conn.execute(f"PRAGMA table_info({table})")}
            except sqlite3.Error:
                continue
            if not have:
                continue
            for col in cols:
                if col not in have:
                    self._conn.execute(
                        f"ALTER TABLE {table} ADD COLUMN {col}")
                    added.append(f"{table}.{col}")
        return added

    # -------------------------------------------------------------- scrittura
    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, name="db-writer",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        self.flush()
        with self._lock:
            self._conn.commit()
            self._conn.close()

    def add(self, table: str, row: dict[str, Any]) -> None:
        """Accoda una riga per una tabella append-only."""
        cols = BATCH_COLUMNS[table]
        try:
            self._batch.put_nowait((table, tuple(row.get(c) for c in cols)))
            self.stats["queued"] += 1
        except queue.Full:
            self.stats["dropped"] += 1

    def upsert(self, table: str, row: dict[str, Any]) -> None:
        """Accoda una riga con chiave: la riga si aggiorna, non si duplica."""
        try:
            self._upsert.put_nowait((table, dict(row)))
            self.stats["queued"] += 1
        except queue.Full:
            self.stats["dropped"] += 1

    def event(self, component: str, level: str, message: str,
              detail: Any = None) -> None:
        self.add("system_events", {
            "ts": now_ms(), "component": component, "level": level,
            "message": message,
            "detail": json.dumps(detail, default=str) if detail is not None else None,
        })

    def _loop(self) -> None:
        interval = self.flush_ms / 1000.0
        while self._running:
            time.sleep(interval)
            try:
                self.flush()
            except Exception as exc:  # noqa: BLE001 - il motore non muore mai
                self.stats["failures"] += 1
                self.last_error = f"{type(exc).__name__}: {exc}"
                self.healthy = False

    def flush(self) -> None:
        batches: dict[str, list[tuple]] = {}
        while True:
            try:
                table, values = self._batch.get_nowait()
            except queue.Empty:
                break
            batches.setdefault(table, []).append(values)

        upserts: list[tuple[str, dict]] = []
        while True:
            try:
                upserts.append(self._upsert.get_nowait())
            except queue.Empty:
                break

        if not batches and not upserts:
            return

        failures: list[str] = []
        with self._lock:
            cur = self._conn.cursor()
            for table, rows in batches.items():
                cols = BATCH_COLUMNS[table]
                sql = (f"INSERT INTO {table} ({','.join(cols)}) "
                       f"VALUES ({','.join('?' * len(cols))})")
                try:
                    cur.executemany(sql, rows)
                    self.stats["written"] += len(rows)
                except sqlite3.Error as exc:
                    failures.append(f"{table}: {exc}")
                    self.stats["dropped"] += len(rows)
            for table, row in upserts:
                key = UPSERT_KEYS.get(table)
                keys = list(row)
                if not keys:
                    continue
                sql = (f"INSERT INTO {table} ({','.join(keys)}) "
                       f"VALUES ({','.join('?' * len(keys))})")
                if key:
                    key_cols = {k.strip() for k in key.split(",")}
                    updates = [f"{k}=excluded.{k}" for k in keys
                               if k not in key_cols]
                    if updates:
                        sql += (f" ON CONFLICT({key}) DO UPDATE SET "
                                + ", ".join(updates))
                    else:
                        sql += f" ON CONFLICT({key}) DO NOTHING"
                try:
                    cur.execute(sql, [row[k] for k in keys])
                    self.stats["written"] += 1
                except sqlite3.Error as exc:
                    failures.append(f"{table}: {exc}")
                    self.stats["dropped"] += 1
            self._conn.commit()
        if failures:
            self.stats["failures"] += len(failures)
            self.last_error = failures[0]
            self.healthy = False

    # --------------------------------------------------------------- lettura
    @property
    def in_memory(self) -> bool:
        return self.path == ":memory:"

    def reader(self) -> sqlite3.Connection:
        """Una connessione per leggere. Su disco e' nuova, in memoria e' quella.

        Con `:memory:` ogni connessione apre un database PROPRIO e vuoto: la
        seconda non vede nulla di cio' che ha scritto la prima. E' cosi' che
        nascevano gli `no such table: wallet_ledger` dei test, dove il
        database e' in memoria — la tabella esisteva, solo in un altro
        database. Qui si riusa la connessione del writer, protetta dallo
        stesso lock che protegge le scritture.
        """
        if self.in_memory:
            return self._conn
        conn = sqlite3.connect(self.path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _release(self, conn: sqlite3.Connection) -> None:
        """Chiude una connessione di lettura, ma mai quella del writer."""
        if conn is not self._conn:
            conn.close()

    def query(self, sql: str, params: Iterable = ()) -> list[dict]:
        conn = self.reader()
        try:
            if conn is self._conn:
                with self._lock:
                    return [dict(r) for r in conn.execute(sql, tuple(params))]
            return [dict(r) for r in conn.execute(sql, tuple(params))]
        finally:
            self._release(conn)

    def scalar(self, sql: str, params: Iterable = ()) -> Any:
        rows = self.query(sql, params)
        if not rows:
            return None
        return next(iter(rows[0].values()))

    def counts(self) -> dict[str, int]:
        tables = ("market_ticks", "features", "decisions", "signals",
                  "signal_updates", "shadow_decisions", "wallet_ledger",
                  "wallet_cycles", "model_versions", "booster_decisions",
                  "setups", "research_experiments", "news_events")
        out: dict[str, int] = {}
        for t in tables:
            try:
                value = self.scalar(f"SELECT COUNT(*) FROM {t}")
                out[t] = int(value) if value is not None else -1
            except sqlite3.Error:
                out[t] = -1
        return out

    def health(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "healthy": self.healthy,
            "last_error": self.last_error,
            "stats": dict(self.stats),
            "migrated": self.migrated,
            "schema_version": SCHEMA_VERSION,
        }
