"""Archivio SQLite. Una regola sola: da una riga si deve poter rifare tutto.

La regola pratica che governa lo schema e' questa: **ogni previsione emessa
viene scritta prima di conoscerne l'esito, e l'esito viene attaccato dopo**.
Non e' un dettaglio contabile, e' l'unica difesa contro il modo piu' comune di
mentire a se stessi con i dati — guardare indietro e ricordare le volte in cui
si aveva ragione.

Note tecniche:

* i tempi sono millisecondi epoch UTC (`INTEGER`), ovunque.
* `INSERT OR REPLACE` sulle serie storiche: un backfill ripetuto e' idempotente
  e non duplica le barre.
* WAL attivo: il collector puo' scrivere mentre la dashboard legge.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Sequence

from .. import config

SCHEMA_VERSION = 1

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- ----------------------------------------------------------------- mercato
-- La griglia storica: una riga per simbolo e per minuto.
CREATE TABLE IF NOT EXISTS bars (
    symbol     TEXT    NOT NULL,
    ts         INTEGER NOT NULL,          -- apertura della barra
    open       REAL    NOT NULL,
    high       REAL    NOT NULL,
    low        REAL    NOT NULL,
    close      REAL    NOT NULL,
    volume     REAL,                      -- base (BTC)
    turnover   REAL,                      -- quote (USDT)
    source     TEXT    NOT NULL DEFAULT 'BYBIT',
    PRIMARY KEY (symbol, ts)
);
CREATE INDEX IF NOT EXISTS ix_bars_ts ON bars(ts);

-- Open interest: serie separata perche' ha un passo suo e una storia piu'
-- corta di quella dei prezzi. Tenerla nella stessa tabella delle barre
-- costringerebbe a inventare valori dove l'archivio non arriva.
CREATE TABLE IF NOT EXISTS open_interest (
    symbol   TEXT    NOT NULL,
    ts       INTEGER NOT NULL,
    interval TEXT    NOT NULL,
    value    REAL    NOT NULL,
    PRIMARY KEY (symbol, interval, ts)
);
CREATE INDEX IF NOT EXISTS ix_oi_ts ON open_interest(ts);

CREATE TABLE IF NOT EXISTS funding (
    symbol TEXT    NOT NULL,
    ts     INTEGER NOT NULL,
    rate   REAL    NOT NULL,
    PRIMARY KEY (symbol, ts)
);

CREATE TABLE IF NOT EXISTS account_ratio (
    symbol     TEXT    NOT NULL,
    ts         INTEGER NOT NULL,
    period     TEXT    NOT NULL,
    buy_ratio  REAL    NOT NULL,
    sell_ratio REAL    NOT NULL,
    PRIMARY KEY (symbol, period, ts)
);

-- Flusso ordini aggregato al minuto, costruito in avanti dal collector.
-- Bybit non offre uno storico degli scambi pubblici: questa tabella cresce
-- solo mentre il collector gira, e la ricerca sa che prima di quel momento il
-- CVD non esiste invece di ricostruirlo per finta dal segno delle barre.
CREATE TABLE IF NOT EXISTS flow (
    symbol       TEXT    NOT NULL,
    ts           INTEGER NOT NULL,          -- minuto
    taker_buy    REAL    NOT NULL DEFAULT 0,
    taker_sell   REAL    NOT NULL DEFAULT 0,
    buy_notional REAL    NOT NULL DEFAULT 0,
    sell_notional REAL   NOT NULL DEFAULT 0,
    trades       INTEGER NOT NULL DEFAULT 0,
    cvd          REAL,                       -- cumulato dall'avvio della serie
    PRIMARY KEY (symbol, ts)
);

-- Fotografie live: ticker + libro. Una riga per poll.
CREATE TABLE IF NOT EXISTS snapshots (
    ts                  INTEGER PRIMARY KEY,
    symbol              TEXT    NOT NULL,
    last_price          REAL,
    mark_price          REAL,
    index_price         REAL,
    basis_bps           REAL,
    bid1                REAL,
    ask1                REAL,
    spread_bps          REAL,
    book_imbalance      REAL,
    book_imbalance_top  REAL,
    book_notional_imb   REAL,
    volume_24h          REAL,
    turnover_24h        REAL,
    open_interest       REAL,
    open_interest_value REAL,
    funding_rate        REAL,
    next_funding_ms     INTEGER,
    payload             TEXT
);

CREATE TABLE IF NOT EXISTS news (
    id        TEXT PRIMARY KEY,             -- hash di link+titolo
    ts        INTEGER NOT NULL,
    source    TEXT NOT NULL,
    title     TEXT NOT NULL,
    link      TEXT,
    summary   TEXT,
    impact    REAL,                         -- 0..1, dal lessico di feeds.py
    direction TEXT                          -- BULL | BEAR | NEUTRAL
);
CREATE INDEX IF NOT EXISTS ix_news_ts ON news(ts);

-- --------------------------------------------------------------- previsioni
-- Scritte PRIMA di sapere come va a finire. `outcome_*` resta NULL finche' il
-- tempo non passa; `score_forecasts` lo riempie guardando le barre.
CREATE TABLE IF NOT EXISTS forecasts (
    forecast_id     TEXT PRIMARY KEY,
    ts              INTEGER NOT NULL,
    symbol          TEXT NOT NULL,
    horizon_min     INTEGER NOT NULL,
    verdict         TEXT NOT NULL,          -- LONG | SHORT | WAIT
    p_long          REAL NOT NULL,
    p_short         REAL NOT NULL,
    p_flat          REAL NOT NULL,
    price           REAL NOT NULL,
    target_low      REAL,
    target_high     REAL,
    expected_move_pct REAL,
    expected_duration_min REAL,
    invalidation    REAL,
    quality         REAL,
    regime          TEXT,
    model_id        TEXT,
    edge_id         TEXT,
    reasons         TEXT,                   -- JSON
    features        TEXT,                   -- JSON: il vettore causale
    outcome_ts      INTEGER,
    outcome_price   REAL,
    outcome_return_bps REAL,
    outcome_label   TEXT,                   -- LONG | SHORT | FLAT
    outcome_mfe_bps REAL,
    outcome_mae_bps REAL,
    outcome_correct INTEGER,                -- 1/0, NULL se WAIT o non scaduto
    time_to_mfe_min REAL
);
CREATE INDEX IF NOT EXISTS ix_forecasts_ts ON forecasts(ts);
CREATE INDEX IF NOT EXISTS ix_forecasts_open ON forecasts(outcome_ts);

-- ------------------------------------------------------------------- edge
CREATE TABLE IF NOT EXISTS edges (
    edge_id      TEXT PRIMARY KEY,
    family       TEXT NOT NULL,
    label        TEXT NOT NULL,
    definition   TEXT NOT NULL,             -- JSON: condizioni e parametri
    state        TEXT NOT NULL,             -- DISCOVERED|VALIDATING|SHADOW|
                                            -- VALIDATED|DECAYING|REJECTED
    direction    TEXT,                      -- LONG | SHORT
    created_ts   INTEGER NOT NULL,
    updated_ts   INTEGER NOT NULL,
    state_ts     INTEGER NOT NULL,
    metrics      TEXT,                      -- JSON: l'ultimo referto completo
    reject_reason TEXT,
    history      TEXT                       -- JSON: le transizioni di stato
);
CREATE INDEX IF NOT EXISTS ix_edges_state ON edges(state);

-- Ogni volta che un edge si attiva, si registra il caso e il suo esito. E'
-- l'equivalente delle "shadow decisions" del vecchio Aurum, ma su previsioni
-- invece che su ordini: e' cio' che permette di misurare un edge che non e'
-- ancora promosso, senza rischiare niente.
CREATE TABLE IF NOT EXISTS edge_observations (
    edge_id     TEXT NOT NULL,
    ts          INTEGER NOT NULL,
    direction   TEXT NOT NULL,
    probability REAL,
    regime      TEXT,
    label       TEXT,                       -- l'esito reale a orizzonte
    return_bps  REAL,
    mfe_bps     REAL,
    mae_bps     REAL,
    time_to_mfe_min REAL,
    correct     INTEGER,
    is_live     INTEGER NOT NULL DEFAULT 0, -- 0 storico, 1 osservato in ombra
    PRIMARY KEY (edge_id, ts)
);
CREATE INDEX IF NOT EXISTS ix_edgeobs_edge ON edge_observations(edge_id);

CREATE TABLE IF NOT EXISTS research_runs (
    run_id     TEXT PRIMARY KEY,
    started_ts INTEGER NOT NULL,
    ended_ts   INTEGER,
    rows       INTEGER,
    span_from  INTEGER,
    span_to    INTEGER,
    candidates INTEGER,
    promoted   INTEGER,
    rejected   INTEGER,
    status     TEXT,
    report     TEXT                          -- JSON: il referto completo
);

-- Champion e challenger. Il confronto e' sulla capacita' predittiva: nessuna
-- colonna in questa tabella parla di euro, e non e' una svista.
CREATE TABLE IF NOT EXISTS models (
    model_id    TEXT PRIMARY KEY,
    created_ts  INTEGER NOT NULL,
    role        TEXT NOT NULL,               -- CHAMPION | CHALLENGER | RETIRED
    kind        TEXT NOT NULL,
    horizon_min INTEGER NOT NULL,
    params      TEXT NOT NULL,               -- JSON: pesi e calibrazione
    metrics     TEXT,                        -- JSON: referto OOS
    trained_from INTEGER,
    trained_to   INTEGER,
    rows        INTEGER,
    notes       TEXT
);
CREATE INDEX IF NOT EXISTS ix_models_role ON models(role);
"""


class Store:
    """Accesso a SQLite. Una connessione per thread, nessuna magia."""

    def __init__(self, path: str | None = None) -> None:
        self.path = path or config.DB_PATH
        self._local = threading.local()
        self._init_lock = threading.Lock()
        self._initialised = False

    # ------------------------------------------------------------ connessione
    @property
    def conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=30.0,
                                   isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=30000")
            self._local.conn = conn
        self._ensure_schema(conn)
        return conn

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        if self._initialised:
            return
        with self._init_lock:
            if self._initialised:
                return
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),))
            self._initialised = True

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except Exception:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    # ------------------------------------------------------------------ meta
    def set_meta(self, key: str, value: Any) -> None:
        self.conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                          (key, json.dumps(value) if not isinstance(value, str)
                           else value))

    def get_meta(self, key: str, default: Any = None) -> Any:
        row = self.conn.execute("SELECT value FROM meta WHERE key = ?",
                                (key,)).fetchone()
        if row is None:
            return default
        try:
            return json.loads(row["value"])
        except (json.JSONDecodeError, TypeError):
            return row["value"]

    # ------------------------------------------------------------------ bars
    def upsert_bars(self, symbol: str, bars: Iterable[Any],
                    source: str = "BYBIT") -> int:
        rows = [(symbol, b.start_ms, b.open, b.high, b.low, b.close,
                 b.volume, b.turnover, source) for b in bars]
        if not rows:
            return 0
        with self.tx() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO bars"
                "(symbol, ts, open, high, low, close, volume, turnover, source)"
                " VALUES(?,?,?,?,?,?,?,?,?)", rows)
        return len(rows)

    def bars(self, symbol: str, *, start_ms: int | None = None,
             end_ms: int | None = None, limit: int | None = None,
             newest_first: bool = False) -> list[sqlite3.Row]:
        sql = "SELECT * FROM bars WHERE symbol = ?"
        args: list[Any] = [symbol]
        if start_ms is not None:
            sql += " AND ts >= ?"
            args.append(start_ms)
        if end_ms is not None:
            sql += " AND ts <= ?"
            args.append(end_ms)
        sql += " ORDER BY ts DESC" if newest_first else " ORDER BY ts ASC"
        if limit:
            sql += " LIMIT ?"
            args.append(limit)
        return self.conn.execute(sql, args).fetchall()

    def bar_span(self, symbol: str) -> tuple[int | None, int | None, int]:
        row = self.conn.execute(
            "SELECT MIN(ts) a, MAX(ts) b, COUNT(*) n FROM bars WHERE symbol = ?",
            (symbol,)).fetchone()
        return (row["a"], row["b"], row["n"] or 0)

    # ---------------------------------------------------------------- serie
    def upsert_open_interest(self, symbol: str, interval: str,
                             points: Iterable[Any]) -> int:
        rows = [(symbol, p.ts_ms, interval, p.open_interest) for p in points]
        if not rows:
            return 0
        with self.tx() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO open_interest"
                "(symbol, ts, interval, value) VALUES(?,?,?,?)", rows)
        return len(rows)

    def open_interest(self, symbol: str, interval: str = "5min", *,
                      start_ms: int | None = None,
                      end_ms: int | None = None) -> list[sqlite3.Row]:
        sql = ("SELECT ts, value FROM open_interest "
               "WHERE symbol = ? AND interval = ?")
        args: list[Any] = [symbol, interval]
        if start_ms is not None:
            sql += " AND ts >= ?"
            args.append(start_ms)
        if end_ms is not None:
            sql += " AND ts <= ?"
            args.append(end_ms)
        return self.conn.execute(sql + " ORDER BY ts ASC", args).fetchall()

    def upsert_funding(self, symbol: str, points: Iterable[Any]) -> int:
        rows = [(symbol, p.ts_ms, p.rate) for p in points]
        if not rows:
            return 0
        with self.tx() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO funding(symbol, ts, rate) VALUES(?,?,?)",
                rows)
        return len(rows)

    def funding(self, symbol: str, *, start_ms: int | None = None,
                end_ms: int | None = None) -> list[sqlite3.Row]:
        sql = "SELECT ts, rate FROM funding WHERE symbol = ?"
        args: list[Any] = [symbol]
        if start_ms is not None:
            sql += " AND ts >= ?"
            args.append(start_ms)
        if end_ms is not None:
            sql += " AND ts <= ?"
            args.append(end_ms)
        return self.conn.execute(sql + " ORDER BY ts ASC", args).fetchall()

    def upsert_account_ratio(self, symbol: str, period: str,
                             points: Iterable[Any]) -> int:
        rows = [(symbol, p.ts_ms, period, p.buy_ratio, p.sell_ratio)
                for p in points]
        if not rows:
            return 0
        with self.tx() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO account_ratio"
                "(symbol, ts, period, buy_ratio, sell_ratio) VALUES(?,?,?,?,?)",
                rows)
        return len(rows)

    def account_ratio(self, symbol: str, period: str = "5min", *,
                      start_ms: int | None = None) -> list[sqlite3.Row]:
        sql = ("SELECT ts, buy_ratio, sell_ratio FROM account_ratio "
               "WHERE symbol = ? AND period = ?")
        args: list[Any] = [symbol, period]
        if start_ms is not None:
            sql += " AND ts >= ?"
            args.append(start_ms)
        return self.conn.execute(sql + " ORDER BY ts ASC", args).fetchall()

    # ----------------------------------------------------------------- flow
    def add_flow(self, symbol: str, ts: int, *, taker_buy: float,
                 taker_sell: float, buy_notional: float, sell_notional: float,
                 trades: int) -> None:
        """Somma il flusso dentro il minuto e ricalcola il CVD cumulato.

        Il CVD e' cumulativo per costruzione: si somma il delta del minuto al
        CVD del minuto precedente. Se manca il precedente (avvio, riavvio,
        buco), la serie riparte da quel delta e la ricerca vede una
        discontinuita' invece di un salto inventato.
        """
        with self.tx() as conn:
            prev = conn.execute(
                "SELECT cvd FROM flow WHERE symbol = ? AND ts < ? "
                "ORDER BY ts DESC LIMIT 1", (symbol, ts)).fetchone()
            base = prev["cvd"] if prev and prev["cvd"] is not None else 0.0
            cur = conn.execute(
                "SELECT taker_buy, taker_sell, buy_notional, sell_notional, trades "
                "FROM flow WHERE symbol = ? AND ts = ?", (symbol, ts)).fetchone()
            tb = (cur["taker_buy"] if cur else 0.0) + taker_buy
            ts_ = (cur["taker_sell"] if cur else 0.0) + taker_sell
            bn = (cur["buy_notional"] if cur else 0.0) + buy_notional
            sn = (cur["sell_notional"] if cur else 0.0) + sell_notional
            tr = (cur["trades"] if cur else 0) + trades
            conn.execute(
                "INSERT OR REPLACE INTO flow"
                "(symbol, ts, taker_buy, taker_sell, buy_notional, sell_notional,"
                " trades, cvd) VALUES(?,?,?,?,?,?,?,?)",
                (symbol, ts, tb, ts_, bn, sn, tr, base + (tb - ts_)))

    def flow(self, symbol: str, *, start_ms: int | None = None,
             end_ms: int | None = None) -> list[sqlite3.Row]:
        sql = "SELECT * FROM flow WHERE symbol = ?"
        args: list[Any] = [symbol]
        if start_ms is not None:
            sql += " AND ts >= ?"
            args.append(start_ms)
        if end_ms is not None:
            sql += " AND ts <= ?"
            args.append(end_ms)
        return self.conn.execute(sql + " ORDER BY ts ASC", args).fetchall()

    # ------------------------------------------------------------ snapshots
    def add_snapshot(self, row: dict[str, Any]) -> None:
        cols = ("ts", "symbol", "last_price", "mark_price", "index_price",
                "basis_bps", "bid1", "ask1", "spread_bps", "book_imbalance",
                "book_imbalance_top", "book_notional_imb", "volume_24h",
                "turnover_24h", "open_interest", "open_interest_value",
                "funding_rate", "next_funding_ms", "payload")
        values = [row.get(c) for c in cols]
        placeholders = ",".join("?" * len(cols))
        self.conn.execute(
            f"INSERT OR REPLACE INTO snapshots({','.join(cols)}) "
            f"VALUES({placeholders})", values)

    def latest_snapshot(self, symbol: str | None = None) -> sqlite3.Row | None:
        if symbol:
            return self.conn.execute(
                "SELECT * FROM snapshots WHERE symbol = ? ORDER BY ts DESC LIMIT 1",
                (symbol,)).fetchone()
        return self.conn.execute(
            "SELECT * FROM snapshots ORDER BY ts DESC LIMIT 1").fetchone()

    def snapshots(self, symbol: str, *, start_ms: int | None = None,
                  limit: int = 500) -> list[sqlite3.Row]:
        sql = "SELECT * FROM snapshots WHERE symbol = ?"
        args: list[Any] = [symbol]
        if start_ms is not None:
            sql += " AND ts >= ?"
            args.append(start_ms)
        sql += " ORDER BY ts DESC LIMIT ?"
        args.append(limit)
        rows = self.conn.execute(sql, args).fetchall()
        return list(reversed(rows))

    # ------------------------------------------------------------------ news
    def upsert_news(self, items: Iterable[dict[str, Any]]) -> int:
        rows = [(i["id"], i["ts"], i["source"], i["title"], i.get("link"),
                 i.get("summary"), i.get("impact"), i.get("direction"))
                for i in items]
        if not rows:
            return 0
        with self.tx() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO news"
                "(id, ts, source, title, link, summary, impact, direction)"
                " VALUES(?,?,?,?,?,?,?,?)", rows)
        return len(rows)

    def news(self, *, since_ms: int | None = None,
             limit: int = 50) -> list[sqlite3.Row]:
        sql = "SELECT * FROM news"
        args: list[Any] = []
        if since_ms is not None:
            sql += " WHERE ts >= ?"
            args.append(since_ms)
        sql += " ORDER BY ts DESC LIMIT ?"
        args.append(limit)
        return self.conn.execute(sql, args).fetchall()

    # ------------------------------------------------------------- forecasts
    def add_forecast(self, row: dict[str, Any]) -> None:
        cols = ("forecast_id", "ts", "symbol", "horizon_min", "verdict",
                "p_long", "p_short", "p_flat", "price", "target_low",
                "target_high", "expected_move_pct", "expected_duration_min",
                "invalidation", "quality", "regime", "model_id", "edge_id",
                "reasons", "features")
        values = [row.get(c) for c in cols]
        self.conn.execute(
            f"INSERT OR REPLACE INTO forecasts({','.join(cols)}) "
            f"VALUES({','.join('?' * len(cols))})", values)

    def score_forecast(self, forecast_id: str, **outcome: Any) -> None:
        cols = ("outcome_ts", "outcome_price", "outcome_return_bps",
                "outcome_label", "outcome_mfe_bps", "outcome_mae_bps",
                "outcome_correct", "time_to_mfe_min")
        sets = ", ".join(f"{c} = ?" for c in cols)
        self.conn.execute(f"UPDATE forecasts SET {sets} WHERE forecast_id = ?",
                          [outcome.get(c) for c in cols] + [forecast_id])

    def pending_forecasts(self, before_ms: int) -> list[sqlite3.Row]:
        """Previsioni scadute e non ancora giudicate."""
        return self.conn.execute(
            "SELECT * FROM forecasts WHERE outcome_ts IS NULL "
            "AND ts + horizon_min * 60000 <= ? ORDER BY ts ASC LIMIT 2000",
            (before_ms,)).fetchall()

    def forecasts(self, *, since_ms: int | None = None, limit: int = 200,
                  scored_only: bool = False) -> list[sqlite3.Row]:
        sql = "SELECT * FROM forecasts WHERE 1=1"
        args: list[Any] = []
        if since_ms is not None:
            sql += " AND ts >= ?"
            args.append(since_ms)
        if scored_only:
            sql += " AND outcome_ts IS NOT NULL"
        sql += " ORDER BY ts DESC LIMIT ?"
        args.append(limit)
        return self.conn.execute(sql, args).fetchall()

    def latest_forecast(self) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM forecasts ORDER BY ts DESC LIMIT 1").fetchone()

    # ------------------------------------------------------------------ edge
    def upsert_edge(self, edge: dict[str, Any]) -> None:
        cols = ("edge_id", "family", "label", "definition", "state",
                "direction", "created_ts", "updated_ts", "state_ts",
                "metrics", "reject_reason", "history")
        self.conn.execute(
            f"INSERT OR REPLACE INTO edges({','.join(cols)}) "
            f"VALUES({','.join('?' * len(cols))})",
            [edge.get(c) for c in cols])

    def edge(self, edge_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM edges WHERE edge_id = ?",
                                 (edge_id,)).fetchone()

    def edges(self, states: Sequence[str] | None = None) -> list[sqlite3.Row]:
        if states:
            marks = ",".join("?" * len(states))
            return self.conn.execute(
                f"SELECT * FROM edges WHERE state IN ({marks}) "
                "ORDER BY updated_ts DESC", list(states)).fetchall()
        return self.conn.execute(
            "SELECT * FROM edges ORDER BY updated_ts DESC").fetchall()

    def add_edge_observations(self, rows: Iterable[dict[str, Any]]) -> int:
        cols = ("edge_id", "ts", "direction", "probability", "regime", "label",
                "return_bps", "mfe_bps", "mae_bps", "time_to_mfe_min",
                "correct", "is_live")
        data = [[r.get(c) for c in cols] for r in rows]
        if not data:
            return 0
        with self.tx() as conn:
            conn.executemany(
                f"INSERT OR REPLACE INTO edge_observations({','.join(cols)}) "
                f"VALUES({','.join('?' * len(cols))})", data)
        return len(data)

    def edge_observations(self, edge_id: str, *,
                          limit: int | None = None) -> list[sqlite3.Row]:
        sql = ("SELECT * FROM edge_observations WHERE edge_id = ? "
               "ORDER BY ts ASC")
        args: list[Any] = [edge_id]
        if limit:
            sql += " LIMIT ?"
            args.append(limit)
        return self.conn.execute(sql, args).fetchall()

    # -------------------------------------------------------------- research
    def add_research_run(self, run: dict[str, Any]) -> None:
        cols = ("run_id", "started_ts", "ended_ts", "rows", "span_from",
                "span_to", "candidates", "promoted", "rejected", "status",
                "report")
        self.conn.execute(
            f"INSERT OR REPLACE INTO research_runs({','.join(cols)}) "
            f"VALUES({','.join('?' * len(cols))})", [run.get(c) for c in cols])

    def latest_research_run(self) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM research_runs ORDER BY started_ts DESC LIMIT 1"
        ).fetchone()

    def research_runs(self, limit: int = 20) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT run_id, started_ts, ended_ts, rows, candidates, promoted,"
            " rejected, status FROM research_runs ORDER BY started_ts DESC "
            "LIMIT ?", (limit,)).fetchall()

    # ---------------------------------------------------------------- models
    def save_model(self, model: dict[str, Any]) -> None:
        cols = ("model_id", "created_ts", "role", "kind", "horizon_min",
                "params", "metrics", "trained_from", "trained_to", "rows",
                "notes")
        self.conn.execute(
            f"INSERT OR REPLACE INTO models({','.join(cols)}) "
            f"VALUES({','.join('?' * len(cols))})", [model.get(c) for c in cols])

    def set_model_role(self, model_id: str, role: str) -> None:
        self.conn.execute("UPDATE models SET role = ? WHERE model_id = ?",
                          (role, model_id))

    def model_by_role(self, role: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM models WHERE role = ? ORDER BY created_ts DESC LIMIT 1",
            (role,)).fetchone()

    def models(self, limit: int = 20) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT model_id, created_ts, role, kind, horizon_min, metrics, rows"
            " FROM models ORDER BY created_ts DESC LIMIT ?", (limit,)).fetchall()

    # ----------------------------------------------------------------- stato
    def coverage(self, symbol: str | None = None) -> dict[str, Any]:
        """Cosa c'e' davvero nel database. La dashboard lo mostra tale e quale."""
        symbol = symbol or config.SYMBOL
        lo, hi, n = self.bar_span(symbol)
        counts = {}
        for table in ("bars", "open_interest", "funding", "account_ratio",
                      "flow", "snapshots", "news", "forecasts",
                      "edge_observations"):
            counts[table] = self.conn.execute(
                f"SELECT COUNT(*) c FROM {table}").fetchone()["c"]
        scored = self.conn.execute(
            "SELECT COUNT(*) c FROM forecasts WHERE outcome_ts IS NOT NULL"
        ).fetchone()["c"]
        return {
            "symbol": symbol,
            "bars_from": lo, "bars_to": hi, "bars": n,
            "days": round((hi - lo) / 86_400_000, 2) if lo and hi else 0.0,
            "tables": counts,
            "forecasts_scored": scored,
        }


_DEFAULT: Store | None = None


def default_store(path: str | None = None) -> Store:
    global _DEFAULT
    if _DEFAULT is None or (path and path != _DEFAULT.path):
        _DEFAULT = Store(path)
    return _DEFAULT
