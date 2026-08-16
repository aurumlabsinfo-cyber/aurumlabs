#!/usr/bin/env python3
"""
AURUM ENGINE - il motore completo in un unico file.

Feed di mercato -> order book locale -> feature microstrutturali -> agenti /
AURUM BURST-15 -> segnali binari PAPER-ONLY con trigger e countdown verificati
lato motore, tutto registrato su database e riaddestrato da solo.

    python3 aurum_engine.py run                      # live su Binance, dashboard su :8000
    python3 aurum_engine.py run --strategy burst15   # strategia AURUM BURST-15
    python3 aurum_engine.py run --source sim         # simulatore, senza rete
    python3 aurum_engine.py check                    # il venue e' raggiungibile?
    python3 aurum_engine.py stats                    # statistiche del paper trading
    python3 aurum_engine.py backtest                 # walk-forward su cio' che ha registrato
    python3 aurum_engine.py burst                    # replay di BURST-15, sessioni incluse
    python3 aurum_engine.py shadow                   # anche le finestre NON tradate

NESSUNA DIPENDENZA. Solo la libreria standard di Python (3.9+): il database e'
SQLite, il client WebSocket e il server HTTP sono scritti qui dentro, e
l'apprendimento e' una regressione logistica implementata a mano. Niente pip,
niente Docker, niente PostgreSQL.

SOLO CARTA. Non esiste in questo file alcun percorso che possa inviare un
ordine a un venue: nessuna chiave privata, nessuna firma, nessun endpoint di
trading. Il P&L e' simulato.

Cosa c'e' dentro, rispetto al progetto completo:
  * feed Binance spot (book ticker + trade + profondita' sequenziata), Coinbase,
    simulatore offline e replay da CSV;
  * order book locale con risincronizzazione automatica;
  * ~50 feature causali a 10 Hz;
  * gli 8 agenti + motore decisionale con i cancelli NO TRADE;
  * AURUM BURST-15: finestra operativa da 15 minuti, ingresso a tempo, sessione
    con cooldown, stop-loss e take-profit;
  * ciclo di vita del segnale e paper trading su database;
  * diagnostica: quale cancello sta bloccando, in classifica;
  * apprendimento automatico: dataset causale, walk-forward con purge, modello
    logistico, classificazione dell'edge, attivazione solo se PROVEN/PROMISING;
  * statistiche, calibrazione, Monte Carlo;
  * dashboard HTML + API JSON su http://localhost:8000

Cosa resta nel repository completo e NON e' qui: la ricerca esaustiva di
strategie con correzione per test multipli (`app/ml/search.py`), l'importatore
dell'archivio storico Binance, i modelli ad alberi (xgboost/lightgbm) e il
frontend Next.js. Vedi README.md.
"""

from __future__ import annotations

import argparse
import base64
import bisect
import csv
import hashlib
import json
import math
import os
import queue
import random
import socket
import sqlite3
import ssl
import struct
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Iterable, Iterator

VERSION = "1.0.0"

# --------------------------------------------------------------------------- #
#  OROLOGIO
# --------------------------------------------------------------------------- #

#: Sostituito in modalita' replay: tutto il motore legge l'ora da qui, cosi' un
#: backtest e una sessione live percorrono esattamente lo stesso codice.
_clock: Callable[[], int] = lambda: int(time.time() * 1000)


def now_ms() -> int:
    return _clock()


def set_clock(fn: Callable[[], int]) -> None:
    global _clock
    _clock = fn


def fmt_ts(ms: int | None) -> str:
    if not ms:
        return "-"
    return time.strftime("%H:%M:%S", time.localtime(ms / 1000)) + f".{ms % 1000:03d}"


# --------------------------------------------------------------------------- #
#  CONFIG
# --------------------------------------------------------------------------- #


@dataclass
class Config:
    """Ogni valore e' sovrascrivibile da riga di comando o da variabile
    d'ambiente con lo stesso nome in maiuscolo (SYMBOL, SIGNAL_MIN_EDGE, ...)."""

    # ---------------------------------------------------------------- venue
    source: str = "binance"          # binance | coinbase | sim | csv
    symbol: str = "BTCUSDT"
    tick_size: float = 0.01
    proxy: str | None = None         # http://host:porta, per REST e WebSocket
    ws_stale_timeout_s: float = 20.0
    orderbook_depth_limit: int = 1000
    orderbook_stream_speed_ms: int = 100

    # ------------------------------------------------------------- database
    db_path: str = "aurum.db"
    persist_ticks: bool = True
    persist_trades: bool = True
    persist_features: bool = True
    db_flush_interval_ms: int = 500
    #: Una finestra NO TRADE viene valutata 10 volte al secondo. Scriverne una
    #: riga ciascuna significa ~864k righe al giorno di "non e' successo
    #: niente": qui se ne tiene solo un battito.
    no_trade_row_interval_ms: int = 5000
    shadow_interval_ms: int = 1000

    # -------------------------------------------------------------- feature
    feature_interval_ms: int = 100
    tick_buffer_s: int = 300
    trade_buffer_s: int = 300
    volatility_window_s: float = 30.0
    large_trade_quantile: float = 0.95
    book_wall_multiple: float = 3.0

    # --------------------------------------------------------- data gating
    max_spread_bps: float = 1.0
    max_latency_ms: float = 750.0
    max_feed_gap_ms: float = 2000.0
    min_data_quality: float = 0.75
    min_warmup_s: float = 60.0
    max_zero_move_fraction: float = 0.35
    min_expected_move_ticks: float = 2.0
    min_l1_notional: float = 500.0

    # -------------------------------------------------------------- segnali
    strategy: str = "ensemble"       # ensemble | burst15
    signal_enabled: bool = True
    horizon_s: float = 5.0
    #: Consenso fra gli agenti: accordo x confidenza media. E' una grandezza
    #: diversa dalla confidenza direzionale e ha una sua manopola.
    min_agreement: float = 0.35
    #: Confidenza direzionale. Vale sempre 0.5 + edge, quindi la soglia
    #: effettiva e' max(min_confidence, 0.5 + min_edge).
    min_confidence: float = 0.55
    min_edge: float = 0.05
    #: L'anomaly detector calcola la gravita' come (rilievi)/3. I rilievi HARD
    #: vietano da soli; quelli soft solo oltre questa soglia.
    anomaly_max_severity: float = 0.5
    cooldown_ms: int = 3000
    max_concurrent: int = 1
    wait_timeout_s: float = 30.0
    trigger_sigma_k: float = 0.35
    trigger_min_ticks: float = 1.0
    trigger_max_bps: float = 8.0
    trigger_price_source: str = "mid"   # mid | last | micro

    # ------------------------------------------------- AURUM BURST-15
    burst_n5_min: int = 40
    burst_r10_min_bps: float = 0.5
    burst_require_ofi_agree: bool = True
    burst_entry_delay_ms: int = 1000
    burst_session_s: int = 900
    burst_cooldown_ms: int = 6000
    burst_max_trades_session: int = 40
    burst_stop_loss_units: float = -6.0
    burst_take_profit_units: float = 15.0
    burst_max_staleness_ms: int = 2000
    burst_min_data_quality: float = 0.75
    burst_auto_restart: bool = True
    #: Usato SOLO per lo stop-loss di sessione quando payout non e' noto.
    burst_assumed_payout: float = 0.8

    # -------------------------------------------------------- paper trading
    #: Payout del broker (0.8 = +80% se vinci). Vuoto = PAYOUT SCONOSCIUTO: il
    #: motore rifiuta di dichiarare un P&L monetario invece di inventarlo.
    payout: float | None = None
    stake: float = 1.0

    # ------------------------------------------------------ apprendimento
    auto_retrain: bool = True
    retrain_interval_s: float = 1800.0
    retrain_initial_delay_s: float = 600.0
    retrain_splits: int = 5
    ml_min_samples: int = 5000
    ml_embargo_s: float = 30.0
    ml_max_rows: int = 60000        # tetto per tenere l'addestramento in secondi
    ml_epochs: int = 40
    ml_learning_rate: float = 0.08
    ml_l2: float = 1e-4

    # ------------------------------------------------------------ interfaccia
    http_port: int = 8000
    http_host: str = "127.0.0.1"
    quiet: bool = False
    json_out: bool = False

    # ------------------------------------------------------------------ misc
    csv_path: str | None = None
    sim_seed: int | None = None

    def __post_init__(self) -> None:
        # Le finestre che dipendono dall'orizzonte crescono con lui, a meno che
        # non siano state scelte esplicitamente altrove.
        h = self.horizon_s
        self.tick_buffer_s = max(self.tick_buffer_s, int(h * 4))
        self.trade_buffer_s = max(self.trade_buffer_s, int(h * 4))
        self.volatility_window_s = max(self.volatility_window_s, h * 2)
        self.ml_embargo_s = max(self.ml_embargo_s, h * 2)

    @property
    def horizon_ms(self) -> int:
        return int(self.horizon_s * 1000)

    @property
    def effective_min_confidence(self) -> float:
        """La confidenza che un segnale deve davvero raggiungere.

        Confidenza = 0.5 + edge per costruzione: prendere il massimo fa si' che
        vinca la piu' stretta delle due e che nessuna delle due sia
        configurazione morta.
        """
        return max(self.min_confidence, 0.5 + self.min_edge)

    @property
    def payout_for_risk(self) -> float:
        return self.payout if self.payout is not None else self.burst_assumed_payout

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_env(cls) -> "Config":
        cfg = cls()
        for name, value in vars(cfg).items():
            raw = os.environ.get(name.upper())
            if raw is None or raw == "":
                continue
            setattr(cfg, name, _coerce(raw, value))
        cfg.__post_init__()
        return cfg


def _coerce(raw: str, current: Any) -> Any:
    if isinstance(current, bool):
        return raw.strip().lower() in ("1", "true", "yes", "on")
    if isinstance(current, int) and not isinstance(current, bool):
        return int(float(raw))
    if isinstance(current, float):
        return float(raw)
    return raw


# --------------------------------------------------------------------------- #
#  STATISTICA
# --------------------------------------------------------------------------- #


def wilson_interval(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Intervallo di confidenza di Wilson: regge anche con pochi campioni,
    dove quello normale produce estremi fuori da [0, 1]."""
    if n <= 0:
        return (0.0, 1.0)
    p = successes / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, centre - half), min(1.0, centre + half))


def binomial_p_value(successes: int, n: int, p0: float = 0.5) -> float:
    """P-value bilaterale contro una moneta equa, via approssimazione normale
    con correzione di continuita' (esatto sarebbe inutilmente lento qui)."""
    if n <= 0:
        return 1.0
    mean = n * p0
    sd = math.sqrt(n * p0 * (1 - p0))
    if sd <= 0:
        return 1.0
    z = (abs(successes - mean) - 0.5) / sd
    return max(0.0, min(1.0, 2.0 * (1.0 - _norm_cdf(z))))


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def breakeven_win_rate(payout: float) -> float:
    return 1.0 / (1.0 + payout)


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    idx = min(len(s) - 1, max(0, int(q * len(s))))
    return s[idx]


# --------------------------------------------------------------------------- #
#  DATABASE (SQLite)
# --------------------------------------------------------------------------- #

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS market_ticks (
    ts INTEGER NOT NULL, exchange TEXT, symbol TEXT,
    bid_price REAL, bid_qty REAL, ask_price REAL, ask_qty REAL,
    mid REAL, micro_price REAL, spread REAL, spread_bps REAL,
    last_price REAL, latency_ms REAL, book_synced INTEGER, is_synthetic INTEGER
);
CREATE INDEX IF NOT EXISTS ix_ticks_ts ON market_ticks(ts);

CREATE TABLE IF NOT EXISTS trades (
    ts INTEGER NOT NULL, exchange TEXT, symbol TEXT, trade_id INTEGER,
    price REAL, quantity REAL, notional REAL,
    is_buyer_maker INTEGER, aggressor TEXT, is_synthetic INTEGER
);
CREATE INDEX IF NOT EXISTS ix_trades_ts ON trades(ts);

CREATE TABLE IF NOT EXISTS features (
    ts INTEGER NOT NULL, exchange TEXT, symbol TEXT, mid REAL, spread_bps REAL,
    book_synced INTEGER, data_quality REAL, regime TEXT,
    payload TEXT, is_synthetic INTEGER
);
CREATE INDEX IF NOT EXISTS ix_features_ts ON features(ts);

CREATE TABLE IF NOT EXISTS signals (
    signal_id TEXT, ts INTEGER NOT NULL, symbol TEXT, direction TEXT,
    status TEXT, strategy TEXT, reference_price REAL, trigger_price REAL,
    horizon_s REAL, confidence REAL, prob_up REAL, prob_down REAL, edge REAL,
    regime TEXT, no_trade_reasons TEXT, detail TEXT, model_id TEXT,
    data_quality REAL, is_synthetic INTEGER
);
CREATE INDEX IF NOT EXISTS ix_signals_ts ON signals(ts);

CREATE TABLE IF NOT EXISTS paper_trades (
    signal_id TEXT PRIMARY KEY, ts INTEGER NOT NULL, symbol TEXT,
    strategy TEXT, direction TEXT, status TEXT, entry_mode TEXT,
    reference_price REAL, trigger_price REAL, entry_price REAL,
    expiry_price REAL, confidence REAL, edge REAL, regime TEXT, horizon_s REAL,
    triggered_at INTEGER, expires_at INTEGER, settled_at INTEGER,
    result TEXT, pnl_units REAL, payout REAL, stake REAL,
    data_quality REAL, features TEXT, is_synthetic INTEGER
);
CREATE INDEX IF NOT EXISTS ix_paper_ts ON paper_trades(ts);

CREATE TABLE IF NOT EXISTS shadow_decisions (
    ts INTEGER NOT NULL, symbol TEXT, lean TEXT, prob_up REAL, confidence REAL,
    edge REAL, horizon_s REAL, reference_price REAL, regime TEXT,
    emitted INTEGER, blocked_by TEXT, data_quality REAL, model_id TEXT,
    is_synthetic INTEGER
);
CREATE INDEX IF NOT EXISTS ix_shadow_ts ON shadow_decisions(ts);

CREATE TABLE IF NOT EXISTS model_versions (
    model_id TEXT PRIMARY KEY, ts INTEGER NOT NULL, algorithm TEXT,
    horizon_s REAL, symbol TEXT, n_train INTEGER, feature_names TEXT,
    metrics TEXT, edge_classification TEXT, artifact TEXT, is_active INTEGER
);

CREATE TABLE IF NOT EXISTS events (
    ts INTEGER NOT NULL, component TEXT, kind TEXT, severity TEXT, detail TEXT
);
CREATE INDEX IF NOT EXISTS ix_events_ts ON events(ts);
"""

#: Colonne per tabella, nell'ordine dell'INSERT. Tenerle qui evita di costruire
#: SQL dinamico riga per riga sul percorso caldo.
COLUMNS: dict[str, tuple[str, ...]] = {
    "market_ticks": (
        "ts", "exchange", "symbol", "bid_price", "bid_qty", "ask_price",
        "ask_qty", "mid", "micro_price", "spread", "spread_bps", "last_price",
        "latency_ms", "book_synced", "is_synthetic",
    ),
    "trades": (
        "ts", "exchange", "symbol", "trade_id", "price", "quantity", "notional",
        "is_buyer_maker", "aggressor", "is_synthetic",
    ),
    "features": (
        "ts", "exchange", "symbol", "mid", "spread_bps", "book_synced",
        "data_quality", "regime", "payload", "is_synthetic",
    ),
    "signals": (
        "signal_id", "ts", "symbol", "direction", "status", "strategy",
        "reference_price", "trigger_price", "horizon_s", "confidence",
        "prob_up", "prob_down", "edge", "regime", "no_trade_reasons", "detail",
        "model_id", "data_quality", "is_synthetic",
    ),
    "shadow_decisions": (
        "ts", "symbol", "lean", "prob_up", "confidence", "edge", "horizon_s",
        "reference_price", "regime", "emitted", "blocked_by", "data_quality",
        "model_id", "is_synthetic",
    ),
    "events": ("ts", "component", "kind", "severity", "detail"),
}


class Store:
    """Persistenza batched su SQLite.

    Il feed produce migliaia di righe al minuto: scriverle una INSERT alla
    volta dal percorso caldo legherebbe la latenza del feed a quella del disco.
    Le righe vengono accodate in memoria e scritte a blocchi da un thread
    dedicato. Se il database si rompe, il motore continua a operare su carta e
    il guasto compare in /health - non viene mai nascosto.
    """

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.path = cfg.db_path
        self._queue: "queue.Queue[tuple[str, tuple]]" = queue.Queue(maxsize=200_000)
        self._paper: "queue.Queue[dict]" = queue.Queue(maxsize=20_000)
        self._thread: threading.Thread | None = None
        self._running = False
        self._write_lock = threading.Lock()
        self.stats = {"queued": 0, "written": 0, "dropped": 0, "failures": 0}
        self.last_error: str | None = None
        self.healthy = True
        self._conn = self._connect()
        with self._write_lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def reader(self) -> sqlite3.Connection:
        """Connessione di sola lettura, una per thread chiamante.

        In WAL i lettori non bloccano lo scrittore e viceversa, quindi le
        statistiche e l'addestramento possono girare mentre il feed scrive.
        """
        conn = sqlite3.connect(self.path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    # ---------------------------------------------------------------- scrittura
    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._loop, name="db-writer", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        self.flush()
        with self._write_lock:
            self._conn.commit()
            self._conn.close()

    def add(self, table: str, row: dict[str, Any]) -> None:
        cols = COLUMNS[table]
        try:
            self._queue.put_nowait((table, tuple(row.get(c) for c in cols)))
            self.stats["queued"] += 1
        except queue.Full:
            self.stats["dropped"] += 1

    def upsert_paper_trade(self, row: dict[str, Any]) -> None:
        try:
            self._paper.put_nowait(row)
        except queue.Full:
            self.stats["dropped"] += 1

    def _loop(self) -> None:
        interval = self.cfg.db_flush_interval_ms / 1000.0
        while self._running:
            time.sleep(interval)
            try:
                self.flush()
            except Exception as exc:  # noqa: BLE001 - il motore non deve morire
                self.stats["failures"] += 1
                self.last_error = f"{type(exc).__name__}: {exc}"
                self.healthy = False

    def flush(self) -> None:
        batches: dict[str, list[tuple]] = {}
        while True:
            try:
                table, values = self._queue.get_nowait()
            except queue.Empty:
                break
            batches.setdefault(table, []).append(values)

        papers: list[dict] = []
        while True:
            try:
                papers.append(self._paper.get_nowait())
            except queue.Empty:
                break

        if not batches and not papers:
            return

        with self._write_lock:
            cur = self._conn.cursor()
            for table, rows in batches.items():
                cols = COLUMNS[table]
                sql = (
                    f"INSERT INTO {table} ({','.join(cols)}) "
                    f"VALUES ({','.join('?' * len(cols))})"
                )
                cur.executemany(sql, rows)
                self.stats["written"] += len(rows)
            for row in papers:
                keys = list(row)
                sql = (
                    f"INSERT INTO paper_trades ({','.join(keys)}) "
                    f"VALUES ({','.join('?' * len(keys))}) "
                    f"ON CONFLICT(signal_id) DO UPDATE SET "
                    + ", ".join(f"{k}=excluded.{k}" for k in keys if k != "signal_id")
                )
                cur.execute(sql, [row[k] for k in keys])
                self.stats["written"] += 1
            self._conn.commit()
        self.healthy = True

    # ----------------------------------------------------------------- lettura
    def counts(self) -> dict[str, int]:
        conn = self.reader()
        try:
            out = {}
            for table in (
                "market_ticks", "trades", "features", "signals", "paper_trades",
                "shadow_decisions", "model_versions",
            ):
                out[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            return out
        finally:
            conn.close()

    def paper_trades(
        self, limit: int | None = None, include_synthetic: bool = True
    ) -> list[dict]:
        conn = self.reader()
        try:
            sql = "SELECT * FROM paper_trades"
            if not include_synthetic:
                sql += " WHERE is_synthetic = 0"
            sql += " ORDER BY ts DESC"
            if limit:
                sql += f" LIMIT {int(limit)}"
            return [dict(r) for r in conn.execute(sql)]
        finally:
            conn.close()

    def event(self, component: str, kind: str, severity: str, detail: Any) -> None:
        self.add(
            "events",
            {
                "ts": now_ms(), "component": component, "kind": kind,
                "severity": severity, "detail": json.dumps(detail, default=str),
            },
        )

    def set_active_model(self, model_id: str) -> None:
        with self._write_lock:
            self._conn.execute("UPDATE model_versions SET is_active = 0")
            self._conn.execute(
                "UPDATE model_versions SET is_active = 1 WHERE model_id = ?",
                (model_id,),
            )
            self._conn.commit()

    def save_model(self, row: dict[str, Any]) -> None:
        keys = list(row)
        with self._write_lock:
            self._conn.execute(
                f"INSERT OR REPLACE INTO model_versions ({','.join(keys)}) "
                f"VALUES ({','.join('?' * len(keys))})",
                [row[k] for k in keys],
            )
            self._conn.commit()

    def models(self) -> list[dict]:
        conn = self.reader()
        try:
            return [
                dict(r)
                for r in conn.execute(
                    "SELECT model_id, ts, algorithm, horizon_s, n_train, metrics,"
                    " edge_classification, is_active FROM model_versions"
                    " ORDER BY ts DESC LIMIT 25"
                )
            ]
        finally:
            conn.close()

    def active_model_artifact(self) -> dict | None:
        conn = self.reader()
        try:
            row = conn.execute(
                "SELECT artifact FROM model_versions WHERE is_active = 1"
                " ORDER BY ts DESC LIMIT 1"
            ).fetchone()
            return json.loads(row["artifact"]) if row else None
        finally:
            conn.close()


# --------------------------------------------------------------------------- #
#  RETE: WebSocket minimale + HTTP, entrambi con supporto proxy
# --------------------------------------------------------------------------- #

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
OP_CONT, OP_TEXT, OP_BIN, OP_CLOSE, OP_PING, OP_PONG = 0x0, 0x1, 0x2, 0x8, 0x9, 0xA


class WebSocketError(Exception):
    pass


def _split_url(url: str) -> tuple[str, str, int, str]:
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    scheme = parts.scheme
    host = parts.hostname or ""
    port = parts.port or (443 if scheme in ("wss", "https") else 80)
    path = parts.path or "/"
    if parts.query:
        path += "?" + parts.query
    return scheme, host, port, path


def _connect(host: str, port: int, timeout: float) -> socket.socket:
    """Connessione TCP con un limite di tempo COMPLESSIVO.

    `socket.create_connection` prova ogni indirizzo restituito dal DNS con il
    timeout intero: sette record A e otto secondi diventano un minuto di attesa
    apparentemente immotivata. Qui il budget e' uno solo per tutta l'operazione.
    """
    deadline = time.time() + timeout
    last: Exception | None = None
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise WebSocketError(f"DNS fallito per {host}: {exc}") from exc
    for family, kind, proto, _, addr in infos:
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        sock = socket.socket(family, kind, proto)
        sock.settimeout(remaining)
        try:
            sock.connect(addr)
            return sock
        except OSError as exc:
            last = exc
            sock.close()
    raise WebSocketError(f"connessione a {host}:{port} non riuscita: {last}")


def _open_socket(
    host: str, port: int, proxy: str | None, timeout: float, tls: bool
) -> socket.socket:
    """Socket verso host:port, eventualmente scavando un tunnel nel proxy.

    Il proxy HTTP e' l'unica via d'uscita in molte reti aziendali e in alcuni
    paesi: se e' configurato deve funzionare davvero, non essere ignorato in
    silenzio come accadeva prima.
    """
    if proxy:
        _, phost, pport, _ = _split_url(proxy if "//" in proxy else "http://" + proxy)
        sock = _connect(phost, pport, timeout)
        req = (
            f"CONNECT {host}:{port} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            f"Proxy-Connection: keep-alive\r\n\r\n"
        )
        sock.sendall(req.encode())
        header = b""
        while b"\r\n\r\n" not in header:
            chunk = sock.recv(4096)
            if not chunk:
                raise WebSocketError("il proxy ha chiuso durante la CONNECT")
            header += chunk
            if len(header) > 65536:
                raise WebSocketError("risposta del proxy troppo lunga")
        status = header.split(b"\r\n", 1)[0].decode("latin-1")
        if " 200" not in status:
            raise WebSocketError(f"il proxy ha rifiutato la CONNECT: {status}")
    else:
        sock = _connect(host, port, timeout)

    if tls:
        ctx = ssl.create_default_context()
        sock = ctx.wrap_socket(sock, server_hostname=host)
    return sock


class WebSocket:
    """Client WebSocket RFC 6455, ridotto a cio' che serve a un feed di sola
    lettura: handshake, frammentazione, ping/pong, chiusura pulita."""

    def __init__(self, url: str, proxy: str | None = None, timeout: float = 15.0):
        scheme, host, port, path = _split_url(url)
        if scheme not in ("ws", "wss"):
            raise WebSocketError(f"schema non supportato: {scheme}")
        self.url = url
        self.host = host
        self.sock = _open_socket(host, port, proxy, timeout, tls=(scheme == "wss"))
        self._buf = b""
        self._frag_op: int | None = None
        self._frag: bytearray = bytearray()
        self.closed = False
        self._handshake(host, port, path)
        self.sock.settimeout(0.0)  # poll() imposta il timeout a ogni chiamata

    # ---------------------------------------------------------------- setup
    def _handshake(self, host: str, port: int, path: str) -> None:
        key = base64.b64encode(os.urandom(16)).decode()
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            f"User-Agent: aurum-engine/{VERSION}\r\n\r\n"
        )
        self.sock.sendall(request.encode())

        self.sock.settimeout(15.0)
        header = b""
        while b"\r\n\r\n" not in header:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise WebSocketError("connessione chiusa durante l'handshake")
            header += chunk
            if len(header) > 65536:
                raise WebSocketError("handshake troppo lungo")
        head, _, rest = header.partition(b"\r\n\r\n")
        self._buf = rest
        lines = head.decode("latin-1").split("\r\n")
        if "101" not in lines[0]:
            raise WebSocketError(f"upgrade rifiutato: {lines[0]}")
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        expected = base64.b64encode(
            hashlib.sha1((key + WS_GUID).encode()).digest()
        ).decode()
        if headers.get("sec-websocket-accept") != expected:
            raise WebSocketError("Sec-WebSocket-Accept non valido")

    # ----------------------------------------------------------------- invio
    def _send(self, opcode: int, payload: bytes = b"") -> None:
        if self.closed:
            return
        header = bytearray()
        header.append(0x80 | opcode)
        mask = os.urandom(4)
        length = len(payload)
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.append(0x80 | 126)
            header += struct.pack(">H", length)
        else:
            header.append(0x80 | 127)
            header += struct.pack(">Q", length)
        header += mask
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        try:
            self.sock.settimeout(10.0)
            self.sock.sendall(bytes(header) + masked)
        except OSError as exc:
            raise WebSocketError(f"invio fallito: {exc}") from exc

    def send_text(self, text: str) -> None:
        self._send(OP_TEXT, text.encode())

    # --------------------------------------------------------------- ricezione
    def poll(self, timeout: float = 0.02) -> list[str]:
        """Legge cio' che e' disponibile e restituisce i messaggi di testo
        completi. Non blocca oltre `timeout`."""
        if self.closed:
            return []
        try:
            self.sock.settimeout(timeout)
            chunk = self.sock.recv(65536)
            if not chunk:
                raise WebSocketError("connessione chiusa dal server")
            self._buf += chunk
            # In TLS un singolo record puo' contenere piu' frame: senza questo
            # ciclo resterebbero in attesa fino al prossimo pacchetto.
            while isinstance(self.sock, ssl.SSLSocket) and self.sock.pending():
                self._buf += self.sock.recv(self.sock.pending())
        except (socket.timeout, ssl.SSLWantReadError):
            pass
        except OSError as exc:
            raise WebSocketError(f"lettura fallita: {exc}") from exc

        out: list[str] = []
        while True:
            frame = self._next_frame()
            if frame is None:
                break
            opcode, fin, payload = frame
            if opcode == OP_PING:
                self._send(OP_PONG, payload)
                continue
            if opcode == OP_PONG:
                continue
            if opcode == OP_CLOSE:
                self.closed = True
                raise WebSocketError("il server ha chiuso la connessione")
            if opcode in (OP_TEXT, OP_BIN):
                if fin:
                    out.append(payload.decode("utf-8", "replace"))
                else:
                    self._frag_op = opcode
                    self._frag = bytearray(payload)
            elif opcode == OP_CONT:
                self._frag += payload
                if fin:
                    out.append(bytes(self._frag).decode("utf-8", "replace"))
                    self._frag = bytearray()
                    self._frag_op = None
        return out

    def _next_frame(self) -> tuple[int, bool, bytes] | None:
        buf = self._buf
        if len(buf) < 2:
            return None
        b0, b1 = buf[0], buf[1]
        fin = bool(b0 & 0x80)
        opcode = b0 & 0x0F
        masked = bool(b1 & 0x80)
        length = b1 & 0x7F
        idx = 2
        if length == 126:
            if len(buf) < 4:
                return None
            length = struct.unpack(">H", buf[2:4])[0]
            idx = 4
        elif length == 127:
            if len(buf) < 10:
                return None
            length = struct.unpack(">Q", buf[2:10])[0]
            idx = 10
        mask = b""
        if masked:  # un server non dovrebbe mascherare, ma se lo fa va gestito
            if len(buf) < idx + 4:
                return None
            mask = buf[idx:idx + 4]
            idx += 4
        if len(buf) < idx + length:
            return None
        payload = buf[idx:idx + length]
        if masked:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self._buf = buf[idx + length:]
        return opcode, fin, payload

    def close(self) -> None:
        if self.closed:
            return
        try:
            self._send(OP_CLOSE, struct.pack(">H", 1000))
        except Exception:  # noqa: BLE001 - stiamo comunque chiudendo
            pass
        self.closed = True
        try:
            self.sock.close()
        except OSError:
            pass


def http_get(
    url: str, proxy: str | None = None, timeout: float = 10.0
) -> tuple[int, bytes]:
    """GET con supporto proxy, sulla stessa strada del WebSocket."""
    from urllib import request as urlrequest

    opener = urlrequest.build_opener(
        urlrequest.ProxyHandler({"http": proxy, "https": proxy} if proxy else {})
    )
    req = urlrequest.Request(url, headers={"User-Agent": f"aurum-engine/{VERSION}"})
    try:
        with opener.open(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except Exception as exc:  # noqa: BLE001 - il chiamante decide cosa farne
        code = getattr(exc, "code", 0) or 0
        body = b""
        try:
            body = exc.read()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            body = str(exc).encode()
        return int(code), body


def http_get_json(url: str, proxy: str | None = None, timeout: float = 10.0) -> Any:
    status, body = http_get(url, proxy, timeout)
    if status != 200:
        raise RuntimeError(f"HTTP {status} da {url}: {body[:200]!r}")
    return json.loads(body)


# --------------------------------------------------------------------------- #
#  TIPI CANONICI E ORDER BOOK
# --------------------------------------------------------------------------- #


@dataclass
class Tick:
    ts: int
    exchange: str
    symbol: str
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float
    last_price: float | None = None
    latency_ms: float = 0.0
    book_synced: bool = False
    is_synthetic: bool = False

    @property
    def mid(self) -> float:
        return (self.bid_price + self.ask_price) / 2.0

    @property
    def spread(self) -> float:
        return self.ask_price - self.bid_price

    @property
    def spread_bps(self) -> float:
        m = self.mid
        return (self.spread / m * 10_000.0) if m > 0 else 0.0

    @property
    def micro_price(self) -> float:
        """Prezzo pesato per le size al tocco: dove sta davvero il valore equo
        quando i due lati del book hanno spessore diverso."""
        total = self.bid_qty + self.ask_qty
        if total <= 0:
            return self.mid
        return (self.bid_price * self.ask_qty + self.ask_price * self.bid_qty) / total

    def row(self) -> dict[str, Any]:
        return {
            "ts": self.ts, "exchange": self.exchange, "symbol": self.symbol,
            "bid_price": self.bid_price, "bid_qty": self.bid_qty,
            "ask_price": self.ask_price, "ask_qty": self.ask_qty,
            "mid": self.mid, "micro_price": self.micro_price,
            "spread": self.spread, "spread_bps": self.spread_bps,
            "last_price": self.last_price, "latency_ms": self.latency_ms,
            "book_synced": int(self.book_synced), "is_synthetic": int(self.is_synthetic),
        }


@dataclass
class TradePrint:
    ts: int
    exchange: str
    symbol: str
    trade_id: int
    price: float
    quantity: float
    is_buyer_maker: bool
    is_synthetic: bool = False

    @property
    def notional(self) -> float:
        return self.price * self.quantity

    @property
    def is_buy(self) -> bool:
        """Aggressore. Se il compratore era il maker, ad attraversare lo spread
        e' stato il venditore."""
        return not self.is_buyer_maker

    def row(self) -> dict[str, Any]:
        return {
            "ts": self.ts, "exchange": self.exchange, "symbol": self.symbol,
            "trade_id": self.trade_id, "price": self.price,
            "quantity": self.quantity, "notional": self.notional,
            "is_buyer_maker": int(self.is_buyer_maker),
            "aggressor": "BUY" if self.is_buy else "SELL",
            "is_synthetic": int(self.is_synthetic),
        }


@dataclass
class DepthUpdate:
    ts: int
    first_id: int
    final_id: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]


@dataclass
class DepthSnapshot:
    ts: int
    last_update_id: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]


class OrderBook:
    """Book locale con validazione della sequenza.

    Un buco nella numerazione degli aggiornamenti significa che il book non
    descrive piu' il mercato: si dichiara desincronizzato e chiede uno
    snapshot, invece di continuare a servire uno stato inventato.
    """

    def __init__(self, exchange: str, symbol: str) -> None:
        self.exchange = exchange
        self.symbol = symbol
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id = 0
        self.synced = False
        self.desync_reason = "non inizializzato"
        self.applied = 0
        self.gaps = 0
        self.resyncs = 0
        self._pending: list[DepthUpdate] = []

    def buffer(self, upd: DepthUpdate) -> None:
        self._pending.append(upd)
        if len(self._pending) > 5000:
            self._pending = self._pending[-2500:]

    def apply_snapshot(self, snap: DepthSnapshot) -> bool:
        self.bids = {p: q for p, q in snap.bids if q > 0}
        self.asks = {p: q for p, q in snap.asks if q > 0}
        self.last_update_id = snap.last_update_id
        self.synced = True
        self.desync_reason = ""
        self.resyncs += 1
        # I diff arrivati durante lo snapshot vengono riapplicati in ordine;
        # quelli piu' vecchi dello snapshot sono gia' contenuti in esso.
        pending, self._pending = self._pending, []
        for upd in pending:
            if upd.final_id <= self.last_update_id:
                continue
            if not self.apply(upd):
                return False
        return True

    def apply(self, upd: DepthUpdate) -> bool:
        if not self.synced:
            self.buffer(upd)
            return False
        if upd.final_id <= self.last_update_id:
            return True  # gia' visto
        if upd.first_id > self.last_update_id + 1:
            self.synced = False
            self.gaps += 1
            self.desync_reason = (
                f"buco nella sequenza: {upd.first_id} dopo {self.last_update_id}"
            )
            self.buffer(upd)
            return False
        for price, qty in upd.bids:
            if qty <= 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for price, qty in upd.asks:
            if qty <= 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty
        self.last_update_id = upd.final_id
        self.applied += 1
        return True

    def desync(self, reason: str) -> None:
        self.synced = False
        self.desync_reason = reason

    def top(self, n: int) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        bids = sorted(self.bids.items(), key=lambda kv: -kv[0])[:n]
        asks = sorted(self.asks.items(), key=lambda kv: kv[0])[:n]
        return bids, asks

    def best(self) -> tuple[float | None, float | None]:
        return (max(self.bids) if self.bids else None,
                min(self.asks) if self.asks else None)

    def mid(self) -> float | None:
        b, a = self.best()
        return (b + a) / 2.0 if (b is not None and a is not None) else None

    def depth_qty(self, n: int) -> tuple[float, float]:
        bids, asks = self.top(n)
        return sum(q for _, q in bids), sum(q for _, q in asks)

    def depth_notional(self, n: int) -> tuple[float, float]:
        bids, asks = self.top(n)
        return sum(p * q for p, q in bids), sum(p * q for p, q in asks)

    def depth_within_bps(self, bps: float) -> tuple[float, float]:
        m = self.mid()
        if m is None:
            return (0.0, 0.0)
        lo, hi = m * (1 - bps / 10_000.0), m * (1 + bps / 10_000.0)
        return (
            sum(q for p, q in self.bids.items() if p >= lo),
            sum(q for p, q in self.asks.items() if p <= hi),
        )

    def snapshot_dict(self, levels: int = 20) -> dict[str, Any]:
        bids, asks = self.top(levels)
        return {
            "synced": self.synced,
            "desync_reason": self.desync_reason,
            "last_update_id": self.last_update_id,
            "bids": [[p, q] for p, q in bids],
            "asks": [[p, q] for p, q in asks],
            "levels_bid": len(self.bids),
            "levels_ask": len(self.asks),
            "resyncs": self.resyncs,
            "gaps": self.gaps,
        }


# --------------------------------------------------------------------------- #
#  FEED
# --------------------------------------------------------------------------- #


class Feed:
    """Interfaccia comune. `poll` restituisce eventi gia' canonici."""

    name = "abstract"
    supports_depth = False
    synthetic = False

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.symbol = cfg.symbol
        self.connected = False
        self.messages = 0
        self.reconnects = 0
        self.last_error: str | None = None
        self.last_message_ts: int | None = None

    def poll(self, timeout: float) -> list[Any]:
        raise NotImplementedError

    def fetch_snapshot(self, limit: int) -> DepthSnapshot | None:
        return None

    def close(self) -> None:
        return None

    def state(self) -> dict[str, Any]:
        return {
            "name": self.name, "connected": self.connected,
            "messages": self.messages, "reconnects": self.reconnects,
            "last_error": self.last_error, "synthetic": self.synthetic,
            "last_message_ts": self.last_message_ts,
        }


class BinanceFeed(Feed):
    """Binance spot, endpoint pubblici: nessuna chiave e' richiesta o accettata.

    Stream: <symbol>@trade, <symbol>@bookTicker, <symbol>@depth@100ms.
    Snapshot REST: /api/v3/depth?limit=1000.
    """

    name = "binance_spot"
    supports_depth = True

    WS_BASE = "wss://stream.binance.com:9443"
    REST_BASE = "https://api.binance.com"

    def __init__(self, cfg: Config) -> None:
        super().__init__(cfg)
        self.ws: WebSocket | None = None
        self._next_attempt = 0.0
        self._backoff = 1.0

    @property
    def ws_url(self) -> str:
        s = self.symbol.lower()
        streams = f"{s}@trade/{s}@bookTicker/{s}@depth@{self.cfg.orderbook_stream_speed_ms}ms"
        return f"{self.WS_BASE}/stream?streams={streams}"

    def _connect(self) -> None:
        if time.time() < self._next_attempt:
            return
        try:
            self.ws = WebSocket(self.ws_url, proxy=self.cfg.proxy)
            self.connected = True
            self.last_error = None
            self._backoff = 1.0
        except Exception as exc:  # noqa: BLE001 - il feed non deve mai morire
            self.connected = False
            self.last_error = f"{type(exc).__name__}: {exc}"
            self._next_attempt = time.time() + self._backoff * (0.5 + random.random())
            self._backoff = min(self._backoff * 2, 30.0)
            self.reconnects += 1

    def poll(self, timeout: float) -> list[Any]:
        if self.ws is None or self.ws.closed:
            self._connect()
            if self.ws is None or self.ws.closed:
                return []
        try:
            raw = self.ws.poll(timeout)
        except WebSocketError as exc:
            self.last_error = str(exc)
            self.connected = False
            try:
                self.ws.close()
            except Exception:  # noqa: BLE001
                pass
            self.ws = None
            self.reconnects += 1
            self._next_attempt = time.time() + self._backoff
            self._backoff = min(self._backoff * 2, 30.0)
            return []
        out: list[Any] = []
        for text in raw:
            self.messages += 1
            self.last_message_ts = now_ms()
            try:
                event = self._parse(json.loads(text))
            except (ValueError, KeyError) as exc:
                self.last_error = f"frame illeggibile: {exc}"
                continue
            if event is not None:
                out.append(event)
        return out

    def _parse(self, msg: dict) -> Any:
        data = msg.get("data") or msg
        ts = now_ms()
        etype = data.get("e")
        if etype == "trade":
            return TradePrint(
                ts=ts, exchange=self.name, symbol=data["s"],
                trade_id=int(data["t"]), price=float(data["p"]),
                quantity=float(data["q"]), is_buyer_maker=bool(data["m"]),
            )
        if etype == "depthUpdate":
            return DepthUpdate(
                ts=ts, first_id=int(data["U"]), final_id=int(data["u"]),
                bids=[(float(p), float(q)) for p, q in data.get("b", [])],
                asks=[(float(p), float(q)) for p, q in data.get("a", [])],
            )
        if "b" in data and "a" in data and "u" in data and "e" not in data:
            # bookTicker: nessun campo "e", solo u/s/b/B/a/A
            return Tick(
                ts=ts, exchange=self.name, symbol=data.get("s", self.symbol),
                bid_price=float(data["b"]), bid_qty=float(data["B"]),
                ask_price=float(data["a"]), ask_qty=float(data["A"]),
            )
        return None

    def fetch_snapshot(self, limit: int) -> DepthSnapshot | None:
        url = f"{self.REST_BASE}/api/v3/depth?symbol={self.symbol}&limit={limit}"
        try:
            payload = http_get_json(url, self.cfg.proxy)
        except Exception as exc:  # noqa: BLE001
            self.last_error = f"snapshot fallito: {type(exc).__name__}: {exc}"
            return None
        return DepthSnapshot(
            ts=now_ms(),
            last_update_id=int(payload["lastUpdateId"]),
            bids=[(float(p), float(q)) for p, q in payload["bids"]],
            asks=[(float(p), float(q)) for p, q in payload["asks"]],
        )

    def close(self) -> None:
        if self.ws:
            self.ws.close()


class CoinbaseFeed(Feed):
    """Coinbase Exchange: alternativa quando Binance non e' raggiungibile.

    Niente profondita': gli agenti che leggono il book si astengono invece di
    inventare un valore.
    """

    name = "coinbase"
    supports_depth = False
    WS_URL = "wss://ws-feed.exchange.coinbase.com"

    def __init__(self, cfg: Config) -> None:
        super().__init__(cfg)
        self.product = self._product(cfg.symbol)
        self.ws: WebSocket | None = None
        self._seq = 0
        self._next_attempt = 0.0
        self._backoff = 1.0

    @staticmethod
    def _product(symbol: str) -> str:
        s = symbol.upper()
        for quote in ("USDT", "USDC", "USD", "EUR", "GBP"):
            if s.endswith(quote):
                return f"{s[:-len(quote)]}-{quote}"
        return s

    def _connect(self) -> None:
        if time.time() < self._next_attempt:
            return
        try:
            self.ws = WebSocket(self.WS_URL, proxy=self.cfg.proxy)
            self.ws.send_text(json.dumps({
                "type": "subscribe",
                "product_ids": [self.product],
                "channels": ["ticker", "matches"],
            }))
            self.connected = True
            self.last_error = None
            self._backoff = 1.0
        except Exception as exc:  # noqa: BLE001
            self.connected = False
            self.last_error = f"{type(exc).__name__}: {exc}"
            self._next_attempt = time.time() + self._backoff * (0.5 + random.random())
            self._backoff = min(self._backoff * 2, 30.0)
            self.reconnects += 1

    def poll(self, timeout: float) -> list[Any]:
        if self.ws is None or self.ws.closed:
            self._connect()
            if self.ws is None or self.ws.closed:
                return []
        try:
            raw = self.ws.poll(timeout)
        except WebSocketError as exc:
            self.last_error = str(exc)
            self.connected = False
            self.ws = None
            self.reconnects += 1
            return []
        out: list[Any] = []
        for text in raw:
            self.messages += 1
            self.last_message_ts = now_ms()
            try:
                msg = json.loads(text)
            except ValueError:
                continue
            mtype = msg.get("type")
            ts = now_ms()
            if mtype == "ticker" and msg.get("best_bid") and msg.get("best_ask"):
                out.append(Tick(
                    ts=ts, exchange=self.name, symbol=self.symbol,
                    bid_price=float(msg["best_bid"]),
                    bid_qty=float(msg.get("best_bid_size") or 0.0),
                    ask_price=float(msg["best_ask"]),
                    ask_qty=float(msg.get("best_ask_size") or 0.0),
                    last_price=float(msg["price"]) if msg.get("price") else None,
                ))
            elif mtype in ("match", "last_match"):
                self._seq += 1
                out.append(TradePrint(
                    ts=ts, exchange=self.name, symbol=self.symbol,
                    trade_id=int(msg.get("trade_id") or self._seq),
                    price=float(msg["price"]), quantity=float(msg["size"]),
                    # Coinbase riporta il lato MAKER.
                    is_buyer_maker=(msg.get("side") == "buy"),
                ))
            elif mtype == "error":
                self.last_error = str(msg.get("message"))
        return out

    def close(self) -> None:
        if self.ws:
            self.ws.close()


class SyntheticFeed(Feed):
    """DATI GENERATI DA UN MODELLO, non dati di mercato.

    Serve solo a far girare la macchina senza rete. Tutto cio' che produce e'
    marcato is_synthetic ovunque, e la ricerca dell'edge lo esclude.
    """

    name = "synthetic"
    supports_depth = True
    synthetic = True

    def __init__(self, cfg: Config) -> None:
        super().__init__(cfg)
        self.rng = random.Random(cfg.sim_seed)
        self.price = 100_000.0
        self.drift = 0.0
        self.tick_size = cfg.tick_size
        self._update_id = 1_000_000
        self._trade_id = 0
        self._next = time.time()
        self.connected = True

    def _step(self, dt: float) -> None:
        self.drift = 0.97 * self.drift + self.rng.gauss(0, 0.35)
        sigma = self.price * (4.0 / 10_000.0) * math.sqrt(dt)
        self.price = max(1.0, self.price + self.drift * sigma * 0.5 + self.rng.gauss(0, sigma))

    def _round(self, p: float) -> float:
        return round(round(p / self.tick_size) * self.tick_size, 8)

    def _sides(self) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        half = max(self.tick_size, self._round(self.price * 0.00002))
        bids, asks = [], []
        for i in range(30):
            decay = math.exp(-i / 12.0)
            bids.append((self._round(self.price - half - i * self.tick_size * 5),
                         round(self.rng.uniform(0.05, 2.5) * decay, 6)))
            asks.append((self._round(self.price + half + i * self.tick_size * 5),
                         round(self.rng.uniform(0.05, 2.5) * decay, 6)))
        return bids, asks

    def poll(self, timeout: float) -> list[Any]:
        now = time.time()
        if now < self._next:
            time.sleep(min(timeout, self._next - now))
            return []
        dt = 0.1
        self._next = now + dt
        self._step(dt)
        self.messages += 1
        self.last_message_ts = now_ms()
        ts = now_ms()
        bids, asks = self._sides()
        self._update_id += 1
        out: list[Any] = [
            Tick(ts=ts, exchange=self.name, symbol=self.symbol,
                 bid_price=bids[0][0], bid_qty=bids[0][1],
                 ask_price=asks[0][0], ask_qty=asks[0][1], is_synthetic=True),
            DepthUpdate(ts=ts, first_id=self._update_id, final_id=self._update_id,
                        bids=bids, asks=asks),
        ]
        for _ in range(self.rng.randint(0, 4)):
            buy = self.rng.random() < 0.5 + 0.15 * math.tanh(self.drift)
            self._trade_id += 1
            out.append(TradePrint(
                ts=ts, exchange=self.name, symbol=self.symbol,
                trade_id=self._trade_id,
                price=asks[0][0] if buy else bids[0][0],
                quantity=round(abs(self.rng.gauss(0.05, 0.12)) + 0.001, 6),
                is_buyer_maker=not buy, is_synthetic=True,
            ))
        return out

    def fetch_snapshot(self, limit: int) -> DepthSnapshot:
        bids, asks = self._sides()
        return DepthSnapshot(ts=now_ms(), last_update_id=self._update_id,
                             bids=bids, asks=asks)


class CsvFeed(Feed):
    """Replay da CSV di trade: ts,price,quantity[,notional][,aggressor].

    Stesso formato del file usato dallo script originale, cosi' un archivio
    gia' scaricato resta utilizzabile. Non contiene il book, quindi mid = ultimo
    prezzo scambiato e le feature di profondita' restano non disponibili.
    """

    name = "csv"
    supports_depth = False

    def __init__(self, cfg: Config) -> None:
        super().__init__(cfg)
        if not cfg.csv_path:
            raise ValueError("serve --csv per la modalita' csv")
        self.rows = self._load(cfg.csv_path)
        self.i = 0
        self.connected = True
        self.virtual_ts = self.rows[0][0] if self.rows else now_ms()
        # In replay l'orologio segue i dati, e va spostato PRIMA che qualunque
        # componente registri il proprio istante di avvio: altrimenti il
        # riscaldamento verrebbe misurato fra due epoche diverse e non
        # finirebbe mai.
        set_clock(lambda: self.virtual_ts)

    @staticmethod
    def _load(path: str) -> list[tuple[int, float, float, bool]]:
        out: list[tuple[int, float, float, bool]] = []
        with open(path, newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                try:
                    ts = int(float(row["ts"]))
                    price = float(row["price"])
                    qty = float(row.get("quantity") or
                                (float(row["notional"]) / price if row.get("notional") else 0.0))
                except (KeyError, TypeError, ValueError) as exc:
                    raise ValueError(f"riga CSV non valida: {row}") from exc
                aggressor = (row.get("aggressor") or "BUY").strip().upper()
                is_buyer_maker = aggressor == "SELL"
                # ts in secondi -> millisecondi, riconosciuto dalla grandezza
                if ts < 10_000_000_000:
                    ts *= 1000
                out.append((ts, price, qty, is_buyer_maker))
        out.sort(key=lambda r: r[0])
        return out

    def poll(self, timeout: float) -> list[Any]:
        if self.i >= len(self.rows):
            self.connected = False
            return []
        # Un blocco per chiamata: tutti i trade con lo stesso millisecondo.
        ts = self.rows[self.i][0]
        out: list[Any] = []
        while self.i < len(self.rows) and self.rows[self.i][0] == ts:
            _, price, qty, maker = self.rows[self.i]
            self.i += 1
            self.messages += 1
            out.append(TradePrint(
                ts=ts, exchange=self.name, symbol=self.symbol,
                trade_id=self.i, price=price, quantity=qty, is_buyer_maker=maker,
            ))
        # Senza book, il tocco viene approssimato da un tick attorno all'ultimo
        # prezzo. E' dichiarato: spread e profondita' non sono dati reali qui.
        last = out[-1].price
        half = self.cfg.tick_size / 2.0
        out.insert(0, Tick(
            ts=ts, exchange=self.name, symbol=self.symbol,
            bid_price=last - half, bid_qty=0.0,
            ask_price=last + half, ask_qty=0.0, last_price=last,
        ))
        self.virtual_ts = ts
        self.last_message_ts = ts
        return out

    @property
    def exhausted(self) -> bool:
        return self.i >= len(self.rows)


def build_feed(cfg: Config) -> Feed:
    return {
        "binance": BinanceFeed,
        "coinbase": CoinbaseFeed,
        "sim": SyntheticFeed,
        "csv": CsvFeed,
    }[cfg.source](cfg)


# --------------------------------------------------------------------------- #
#  STRUTTURE ROLLING E INDICATORI
# --------------------------------------------------------------------------- #


class TimeSeries:
    """Serie temporale scalare con lookup O(log n).

    Due liste parallele invece di una deque di tuple: `bisect` puo' rispondere
    a "quanto valeva 250 ms fa" senza scorrere tutto.
    """

    def __init__(self, window_ms: int) -> None:
        self.window_ms = window_ms
        self.ts: list[int] = []
        self.values: list[float] = []

    def append(self, ts: int, value: float) -> None:
        if self.ts and ts < self.ts[-1]:
            ts = self.ts[-1]  # i feed possono consegnare fuori ordine
        self.ts.append(ts)
        self.values.append(value)
        cutoff = ts - self.window_ms
        if self.ts[0] < cutoff:
            idx = bisect.bisect_left(self.ts, cutoff)
            if idx > 0:
                del self.ts[:idx]
                del self.values[:idx]

    def __len__(self) -> int:
        return len(self.ts)

    @property
    def last(self) -> float | None:
        return self.values[-1] if self.values else None

    def span_ms(self) -> int:
        return (self.ts[-1] - self.ts[0]) if len(self.ts) > 1 else 0

    def value_at_or_before(self, ts: int) -> float | None:
        if not self.ts:
            return None
        idx = bisect.bisect_right(self.ts, ts) - 1
        return self.values[idx] if idx >= 0 else None

    def value_ago(self, ms: int) -> float | None:
        if not self.ts:
            return None
        target = self.ts[-1] - ms
        if self.ts[0] > target:
            return None  # storia insufficiente: NON si estrapola
        return self.value_at_or_before(target)

    def returns_since(self, ms: int) -> float | None:
        past = self.value_ago(ms)
        cur = self.last
        if past is None or cur is None or past <= 0:
            return None
        return (cur - past) / past * 10_000.0

    def slice_since(self, ts: int) -> list[float]:
        return self.values[bisect.bisect_left(self.ts, ts):]

    def range_position(self, ms: int) -> float | None:
        if not self.ts:
            return None
        start = self.ts[-1] - ms
        if self.ts[0] > start:
            return None
        vals = self.slice_since(start)
        if len(vals) < 2:
            return None
        lo, hi = min(vals), max(vals)
        if hi <= lo:
            return None
        return (self.values[-1] - lo) / (hi - lo)

    def realized_vol_bps(self, ms: int) -> float | None:
        if not self.ts:
            return None
        vals = self.slice_since(self.ts[-1] - ms)
        if len(vals) < 3:
            return None
        rets = [
            math.log(vals[i] / vals[i - 1])
            for i in range(1, len(vals))
            if vals[i] > 0 and vals[i - 1] > 0
        ]
        if len(rets) < 2:
            return None
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
        return math.sqrt(var) * 10_000.0

    def _windows(self, horizon_ms: int, lookback_ms: int) -> Iterator[tuple[float, float]]:
        end = self.ts[-1]
        start = end - lookback_ms
        # Le finestre si sovrappongono di proposito: 30s a passo 5s darebbero
        # cinque campioni, troppo pochi per leggerci qualsiasi cosa.
        step = max(horizon_ms // 10, 100)
        t = start + horizon_ms
        while t <= end:
            a = self.value_at_or_before(t - horizon_ms)
            b = self.value_at_or_before(t)
            if a is not None and b is not None:
                yield a, b
            t += step

    def sigma_over(self, horizon_ms: int, lookback_ms: int) -> float | None:
        """Deviazione standard dei ritorni misurati su `horizon_ms`, in bps:
        quanto si sposta il prezzo, tipicamente, in un orizzonte."""
        if len(self.ts) < 5 or self.ts[0] > self.ts[-1] - lookback_ms:
            return None
        samples = [
            (b - a) / a * 10_000.0
            for a, b in self._windows(horizon_ms, lookback_ms)
            if a > 0
        ]
        if len(samples) < 5:
            return None
        mean = sum(samples) / len(samples)
        var = sum((s - mean) ** 2 for s in samples) / (len(samples) - 1)
        return math.sqrt(var)

    def zero_move_fraction(
        self, horizon_ms: int, lookback_ms: int, tolerance: float = 0.0
    ) -> float | None:
        """Quota di finestre lunghe un orizzonte che finiscono dove sono
        cominciate. Su un'opzione binaria che non paga il pareggio, questo e' il
        fatto economico dominante, quindi si misura invece di ignorarlo."""
        if len(self.ts) < 5 or self.ts[0] > self.ts[-1] - lookback_ms:
            return None
        flat = total = 0
        for a, b in self._windows(horizon_ms, lookback_ms):
            total += 1
            if abs(b - a) <= tolerance:
                flat += 1
        return (flat / total) if total >= 5 else None


@dataclass
class TradeRecord:
    ts: int
    price: float
    quantity: float
    notional: float
    is_buy: bool


class TradeWindow:
    def __init__(self, window_ms: int) -> None:
        self.window_ms = window_ms
        self.trades: deque[TradeRecord] = deque()

    def append(self, rec: TradeRecord) -> None:
        self.trades.append(rec)
        self.trim(rec.ts)

    def trim(self, now: int) -> None:
        cutoff = now - self.window_ms
        while self.trades and self.trades[0].ts < cutoff:
            self.trades.popleft()

    def since(self, ms: int) -> list[TradeRecord]:
        if not self.trades:
            return []
        cutoff = self.trades[-1].ts - ms
        return [t for t in self.trades if t.ts >= cutoff]

    def flow(self, ms: int) -> dict[str, float]:
        recs = self.since(ms)
        buy_q = sum(r.quantity for r in recs if r.is_buy)
        sell_q = sum(r.quantity for r in recs if not r.is_buy)
        buy_n = sum(r.notional for r in recs if r.is_buy)
        sell_n = sum(r.notional for r in recs if not r.is_buy)
        total_q = buy_q + sell_q
        total_n = buy_n + sell_n
        n = len(recs)
        return {
            "count": float(n),
            "buy_volume": buy_q, "sell_volume": sell_q,
            "buy_notional": buy_n, "sell_notional": sell_n,
            "volume_imbalance": ((buy_q - sell_q) / total_q) if total_q > 0 else 0.0,
            # Pesato per controvalore: una stampa da 10 BTC e duecento da
            # polvere sono lo stesso numero di trade e informazioni diverse.
            "ofi_notional": ((buy_n - sell_n) / total_n) if total_n > 0 else 0.0,
            "trade_intensity": n / (ms / 1000.0) if ms > 0 else 0.0,
            "avg_trade_size": (total_q / n) if n else 0.0,
        }

    def consecutive(self) -> tuple[int, int]:
        buys = sells = 0
        for rec in reversed(self.trades):
            if rec.is_buy:
                if sells:
                    break
                buys += 1
            else:
                if buys:
                    break
                sells += 1
        return buys, sells

    def large(self, threshold: float, ms: int) -> dict[str, float]:
        recs = [r for r in self.since(ms) if r.notional >= threshold]
        return {
            "large_trade_count": float(len(recs)),
            "large_buy_notional": sum(r.notional for r in recs if r.is_buy),
            "large_sell_notional": sum(r.notional for r in recs if not r.is_buy),
        }

    def notional_quantile(self, q: float) -> float | None:
        if len(self.trades) < 20:
            return None
        return percentile([r.notional for r in self.trades], q)


@dataclass
class Bar:
    open_ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


class BarAggregator:
    def __init__(self, interval_ms: int = 1000, maxlen: int = 900) -> None:
        self.interval_ms = interval_ms
        self.bars: deque[Bar] = deque(maxlen=maxlen)
        self.current: Bar | None = None

    def add_price(self, ts: int, price: float) -> None:
        bucket = ts - (ts % self.interval_ms)
        if self.current is None or bucket > self.current.open_ts:
            if self.current is not None:
                self.bars.append(self.current)
            self.current = Bar(bucket, price, price, price, price)
        else:
            c = self.current
            c.high = max(c.high, price)
            c.low = min(c.low, price)
            c.close = price

    def add_trade(self, ts: int, price: float, qty: float) -> None:
        self.add_price(ts, price)
        if self.current is not None:
            self.current.volume += qty

    def closed(self) -> list[Bar]:
        return list(self.bars)


def ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    out = sum(values[:period]) / period
    for v in values[period:]:
        out = v * k + out * (1 - k)
    return out


def rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) < period + 1:
        return None
    gains = losses = 0.0
    for i in range(1, period + 1):
        diff = values[i] - values[i - 1]
        gains += max(diff, 0.0)
        losses += max(-diff, 0.0)
    avg_g, avg_l = gains / period, losses / period
    for i in range(period + 1, len(values)):
        diff = values[i] - values[i - 1]
        avg_g = (avg_g * (period - 1) + max(diff, 0.0)) / period
        avg_l = (avg_l * (period - 1) + max(-diff, 0.0)) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - 100.0 / (1.0 + rs)


def bollinger(values: list[float], period: int = 20, k: float = 2.0):
    if len(values) < period:
        return None
    window = values[-period:]
    mean = sum(window) / period
    var = sum((v - mean) ** 2 for v in window) / period
    sd = math.sqrt(var)
    z = ((values[-1] - mean) / sd) if sd > 0 else 0.0
    return mean, mean + k * sd, mean - k * sd, z


def atr(bars: list[Bar], period: int = 14) -> float | None:
    if len(bars) < period + 1:
        return None
    trs = []
    for i in range(1, len(bars)):
        prev, cur = bars[i - 1], bars[i]
        trs.append(max(cur.high - cur.low, abs(cur.high - prev.close),
                       abs(cur.low - prev.close)))
    return sum(trs[-period:]) / period


def vwap(bars: list[Bar]) -> float | None:
    total_v = sum(b.volume for b in bars)
    if total_v <= 0:
        return None
    return sum(((b.high + b.low + b.close) / 3.0) * b.volume for b in bars) / total_v


def consistency(values: list[float | None]) -> float | None:
    """+1 se ogni orizzonte concorda sulla direzione, -1 se si contraddicono."""
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return None
    signs = [1 if v > 0 else (-1 if v < 0 else 0) for v in vals]
    return sum(signs) / len(signs)


# --------------------------------------------------------------------------- #
#  MARKET DATA: feed -> book -> stato canonico
# --------------------------------------------------------------------------- #

RETURN_HORIZONS_MS = (100, 250, 500, 1000, 2000, 3000, 5000, 10000)
LONG_RETURN_HORIZONS_MS = (15_000, 60_000, 300_000, 900_000)
FLOW_WINDOWS_MS = (1000, 5000, 30000)


class MarketData:
    """L'unica fonte di verita' su "com'e' il mercato adesso".

    Non fabbrica mai un valore: se il book non e' sincronizzato o il feed e'
    fermo, lo dichiara e il motore dei segnali si ferma.
    """

    def __init__(self, cfg: Config, feed: Feed, store: Store | None = None) -> None:
        self.cfg = cfg
        self.feed = feed
        self.store = store
        self.book = OrderBook(feed.name, cfg.symbol)
        self.last_tick: Tick | None = None
        self.last_trade: TradePrint | None = None
        self.started_at = now_ms()
        self.counters = {"ticks": 0, "trades": 0, "depth": 0, "snapshots": 0}
        self.latencies: deque[float] = deque(maxlen=500)
        self.errors: deque[dict] = deque(maxlen=100)
        self._need_snapshot = feed.supports_depth
        self._last_snapshot_attempt = 0.0
        self.listeners: list[Callable[[Any], None]] = []

    @property
    def is_synthetic(self) -> bool:
        return self.feed.synthetic

    @property
    def feed_age_ms(self) -> float | None:
        return (now_ms() - self.last_tick.ts) if self.last_tick else None

    @property
    def uptime_s(self) -> float:
        return (now_ms() - self.started_at) / 1000.0

    def record_error(self, component: str, message: str) -> None:
        entry = {"ts": now_ms(), "component": component, "message": message}
        self.errors.append(entry)
        if self.store:
            self.store.event(component, "error", "ERROR", message)

    # ------------------------------------------------------------- ingestione
    def pump(self, timeout: float) -> int:
        events = self.feed.poll(timeout)
        for event in events:
            try:
                self.ingest(event)
            except Exception as exc:  # noqa: BLE001 - l'ingestione non muore mai
                self.record_error("ingest", f"{type(exc).__name__}: {exc}")
        self._maybe_resync()
        return len(events)

    def ingest(self, event: Any) -> None:
        if isinstance(event, Tick):
            self.counters["ticks"] += 1
            event.book_synced = self.book.synced
            if self.last_trade is not None:
                event.last_price = self.last_trade.price
            self.last_tick = event
            self.latencies.append(event.latency_ms)
            if self.store and self.cfg.persist_ticks:
                self.store.add("market_ticks", event.row())
        elif isinstance(event, TradePrint):
            self.counters["trades"] += 1
            self.last_trade = event
            if self.store and self.cfg.persist_trades:
                self.store.add("trades", event.row())
        elif isinstance(event, DepthUpdate):
            self.counters["depth"] += 1
            if not self.book.apply(event):
                self._need_snapshot = True
        for listener in self.listeners:
            listener(event)

    def _maybe_resync(self) -> None:
        if not self._need_snapshot or not self.feed.supports_depth:
            return
        now = time.time()
        if now - self._last_snapshot_attempt < 1.0:
            return
        self._last_snapshot_attempt = now
        snap = self.feed.fetch_snapshot(self.cfg.orderbook_depth_limit)
        if snap is None:
            self.book.desync("snapshot REST non riuscito")
            return
        self.counters["snapshots"] += 1
        if self.book.apply_snapshot(snap):
            self._need_snapshot = False

    # ---------------------------------------------------------------- qualita'
    def data_quality(self) -> dict[str, Any]:
        """Punteggio 0..1 spiegabile. Un guasto grave lo porta a 0."""
        cfg = self.cfg
        reasons: list[str] = []
        if self.last_tick is None:
            return {"score": 0.0, "ok": False, "warmup_complete": False,
                    "reasons": ["nessun dato di mercato ricevuto"]}

        score = 1.0
        age = self.feed_age_ms or 0.0
        if age > cfg.max_feed_gap_ms:
            reasons.append(f"feed fermo da {age:.0f}ms")
            score = 0.0
        elif age > cfg.max_feed_gap_ms / 2:
            reasons.append("feed lento")
            score -= 0.2

        if self.feed.supports_depth and not self.book.synced:
            reasons.append(f"book non sincronizzato: {self.book.desync_reason}")
            score = 0.0

        p95 = percentile(list(self.latencies), 0.95) or 0.0
        if p95 > cfg.max_latency_ms:
            reasons.append(f"latenza p95 {p95:.0f}ms > {cfg.max_latency_ms:.0f}ms")
            score = 0.0
        elif p95 > cfg.max_latency_ms / 2:
            score -= 0.15

        spread_bps = self.last_tick.spread_bps
        if spread_bps > cfg.max_spread_bps:
            reasons.append(f"spread {spread_bps:.2f}bps > {cfg.max_spread_bps:.2f}bps")
            score -= 0.3
        if spread_bps <= 0:
            reasons.append("spread non positivo")
            score = 0.0

        warm = self.uptime_s >= cfg.min_warmup_s
        if not warm:
            reasons.append(
                f"riscaldamento ({self.uptime_s:.0f}s / {cfg.min_warmup_s:.0f}s)"
            )
        score = max(0.0, min(1.0, score))
        return {
            "score": round(score, 3),
            "ok": score >= cfg.min_data_quality and warm,
            "warmup_complete": warm,
            "reasons": reasons,
        }

    def snapshot(self) -> dict[str, Any]:
        t = self.last_tick
        return {
            "symbol": self.cfg.symbol,
            "exchange": self.feed.name,
            "is_synthetic": self.is_synthetic,
            "ts": t.ts if t else None,
            "price": t.mid if t else None,
            "last_price": self.last_trade.price if self.last_trade else None,
            "bid": t.bid_price if t else None,
            "ask": t.ask_price if t else None,
            "bid_qty": t.bid_qty if t else None,
            "ask_qty": t.ask_qty if t else None,
            "spread": t.spread if t else None,
            "spread_bps": t.spread_bps if t else None,
            "micro_price": t.micro_price if t else None,
            "book_synced": self.book.synced,
        }

    def health(self) -> dict[str, Any]:
        return {
            "symbol": self.cfg.symbol,
            "source": self.feed.name,
            "is_synthetic": self.is_synthetic,
            "uptime_s": round(self.uptime_s, 1),
            "counters": dict(self.counters),
            "feed_age_ms": self.feed_age_ms,
            "latency_p95_ms": percentile(list(self.latencies), 0.95),
            "orderbook": self.book.snapshot_dict(levels=5),
            "adapter": self.feed.state(),
            "data_quality": self.data_quality(),
            "errors_recent": list(self.errors)[-5:],
        }


# --------------------------------------------------------------------------- #
#  FEATURE ENGINE
# --------------------------------------------------------------------------- #


class FeatureEngine:
    """Vettore di feature causali a cadenza fissa.

    "Causale" significa che ogni valore e' calcolato con informazione
    disponibile al proprio timestamp o prima: niente riempimenti dal futuro,
    niente finestre centrate. E' questo che rende la tabella `features`
    utilizzabile per un apprendimento onesto.
    """

    def __init__(self, cfg: Config, market: MarketData) -> None:
        self.cfg = cfg
        self.market = market
        window = cfg.tick_buffer_s * 1000
        self.mid = TimeSeries(window)
        self.bid_depth = TimeSeries(60_000)
        self.ask_depth = TimeSeries(60_000)
        self.trades = TradeWindow(cfg.trade_buffer_s * 1000)
        self.bars = BarAggregator(1000, 900)
        self.latest: dict[str, Any] | None = None
        self.computed = 0
        self._last_threshold: float | None = None
        market.listeners.append(self.on_event)

    def on_event(self, event: Any) -> None:
        if isinstance(event, Tick):
            self.mid.append(event.ts, event.mid)
            self.bars.add_price(event.ts, event.mid)
        elif isinstance(event, TradePrint):
            self.trades.append(TradeRecord(
                ts=event.ts, price=event.price, quantity=event.quantity,
                notional=event.notional, is_buy=event.is_buy,
            ))
            self.bars.add_trade(event.ts, event.price, event.quantity)

    def compute(self, ts: int | None = None) -> dict[str, Any] | None:
        tick = self.market.last_tick
        if tick is None or self.mid.last is None:
            return None
        cfg = self.cfg
        ts = ts if ts is not None else now_ms()
        book = self.market.book
        quality = self.market.data_quality()
        f: dict[str, Any] = {}

        # -------------------------------------------------------- price action
        for ms in RETURN_HORIZONS_MS:
            f[f"return_{ms}ms"] = self.mid.returns_since(ms)
        r1 = f.get("return_1000ms")
        prev_r1 = None
        if len(self.mid) > 3:
            past, past2 = self.mid.value_ago(1000), self.mid.value_ago(2000)
            if past and past2 and past2 > 0:
                prev_r1 = (past - past2) / past2 * 10_000.0
        f["velocity_bps_s"] = r1
        f["acceleration_bps_s2"] = (
            (r1 - prev_r1) if (r1 is not None and prev_r1 is not None) else None
        )
        f["momentum_5s"] = f.get("return_5000ms")
        f["momentum_consistency"] = consistency(
            [f.get(f"return_{ms}ms") for ms in (500, 1000, 2000, 3000, 5000)]
        )

        buffer_ms = cfg.tick_buffer_s * 1000
        for ms in LONG_RETURN_HORIZONS_MS:
            f[f"return_{ms}ms"] = self.mid.returns_since(ms) if ms <= buffer_ms else None
            label = ms // 60_000
            if label:
                f[f"range_position_{label}m"] = (
                    self.mid.range_position(ms) if ms <= buffer_ms else None
                )
        f["long_momentum_consistency"] = consistency(
            [f.get(f"return_{ms}ms") for ms in LONG_RETURN_HORIZONS_MS]
        )

        # ---------------------------------------------------------- order book
        f["mid"] = tick.mid
        f["micro_price"] = tick.micro_price
        f["micro_price_dev_bps"] = (
            (tick.micro_price - tick.mid) / tick.mid * 10_000.0 if tick.mid else None
        )
        f["spread"] = tick.spread
        f["spread_bps"] = tick.spread_bps
        top = tick.bid_qty + tick.ask_qty
        bid_notional = tick.bid_qty * tick.bid_price
        ask_notional = tick.ask_qty * tick.ask_price
        # Sul mercato vero il tocco e' spesso un ordine da polvere contro
        # diecimila dollari dall'altro lato: sotto una soglia di controvalore
        # questo rapporto e' rumore, e viene dichiarato non disponibile.
        if top > 0 and min(bid_notional, ask_notional) >= cfg.min_l1_notional:
            f["book_imbalance_l1"] = (tick.bid_qty - tick.ask_qty) / top
        else:
            f["book_imbalance_l1"] = None
        f["l1_dust"] = min(bid_notional, ask_notional) < cfg.min_l1_notional
        f["bid_qty_l1"] = tick.bid_qty
        f["ask_qty_l1"] = tick.ask_qty
        f.update(self._book_features(book, ts))

        # ----------------------------------------------------------- order flow
        self.trades.trim(ts)
        threshold = (
            self.trades.notional_quantile(cfg.large_trade_quantile)
            or self._last_threshold
        )
        self._last_threshold = threshold
        for ms in FLOW_WINDOWS_MS:
            flow = self.trades.flow(ms)
            tag = f"{ms // 1000}s"
            f[f"buy_volume_{tag}"] = flow["buy_volume"]
            f[f"sell_volume_{tag}"] = flow["sell_volume"]
            f[f"volume_imbalance_{tag}"] = flow["volume_imbalance"]
            f[f"ofi_notional_{tag}"] = flow["ofi_notional"]
            f[f"trade_intensity_{tag}"] = flow["trade_intensity"]
            f[f"avg_trade_size_{tag}"] = flow["avg_trade_size"]
            f[f"trade_count_{tag}"] = flow["count"]
            f[f"aggressive_buy_notional_{tag}"] = flow["buy_notional"]
            f[f"aggressive_sell_notional_{tag}"] = flow["sell_notional"]
        buys, sells = self.trades.consecutive()
        f["consecutive_buys"] = float(buys)
        f["consecutive_sells"] = float(sells)
        if threshold:
            f.update(self.trades.large(threshold, 5000))
            f["large_trade_threshold_notional"] = threshold
        else:
            f["large_trade_count"] = 0.0
            f["large_buy_notional"] = 0.0
            f["large_sell_notional"] = 0.0
            f["large_trade_threshold_notional"] = None

        # ----------------------------------------------------------- volatilita'
        f["realized_vol_1s_bps"] = self.mid.realized_vol_bps(1000)
        f["realized_vol_5s_bps"] = self.mid.realized_vol_bps(5000)
        f["realized_vol_30s_bps"] = self.mid.realized_vol_bps(30000)
        v5, v30 = f["realized_vol_5s_bps"], f["realized_vol_30s_bps"]
        f["vol_ratio_5s_30s"] = (v5 / v30) if (v5 and v30 and v30 > 0) else None
        f["vol_acceleration"] = (
            (v5 - v30) if (v5 is not None and v30 is not None) else None
        )
        lookback = int(cfg.volatility_window_s * 1000)
        f["sigma_horizon_bps"] = self.mid.sigma_over(cfg.horizon_ms, lookback)
        f["zero_move_fraction"] = self.mid.zero_move_fraction(
            cfg.horizon_ms, lookback, tolerance=cfg.tick_size / 2.0
        )
        f["expected_move_ticks"] = (
            f["sigma_horizon_bps"] / 10_000.0 * tick.mid / cfg.tick_size
            if f["sigma_horizon_bps"] and cfg.tick_size > 0 else None
        )

        # ---------------------------------------------- indicatori classici
        bars = self.bars.closed()
        closes = [b.close for b in bars]
        f["ema_9"] = ema(closes, 9)
        f["ema_21"] = ema(closes, 21)
        f["ema_spread_bps"] = (
            (f["ema_9"] - f["ema_21"]) / f["ema_21"] * 10_000.0
            if f["ema_9"] and f["ema_21"] else None
        )
        f["rsi_14"] = rsi(closes, 14)
        f["vwap_60s"] = vwap(bars[-60:]) if bars else None
        f["vwap_deviation_bps"] = (
            (tick.mid - f["vwap_60s"]) / f["vwap_60s"] * 10_000.0
            if f["vwap_60s"] else None
        )
        bb = bollinger(closes, 20, 2.0)
        if bb:
            f["bb_mid"], f["bb_upper"], f["bb_lower"], f["bb_z"] = bb
        else:
            f["bb_mid"] = f["bb_upper"] = f["bb_lower"] = f["bb_z"] = None
        f["atr_14"] = atr(bars, 14)

        # ------------------------------------------------------------- meta
        f["latency_ms"] = tick.latency_ms
        f["book_synced"] = book.synced
        f["data_quality"] = quality["score"]
        f["history_span_ms"] = self.mid.span_ms()
        f["tick_count"] = len(self.mid)

        vector = {
            "ts": ts, "exchange": tick.exchange, "symbol": tick.symbol,
            "is_synthetic": tick.is_synthetic, "features": f,
        }
        self.latest = vector
        self.computed += 1
        return vector

    def _book_features(self, book: OrderBook, ts: int) -> dict[str, Any]:
        empty = {
            "depth_imbalance_5": None, "depth_imbalance_20": None,
            "depth_notional_bid_20": None, "depth_notional_ask_20": None,
            "liquidity_concentration_bid": None, "liquidity_concentration_ask": None,
            "bid_wall_distance_bps": None, "ask_wall_distance_bps": None,
            "bid_wall_size": None, "ask_wall_size": None,
            "liquidity_removal_bid": None, "liquidity_removal_ask": None,
            "depth_within_5bps_bid": None, "depth_within_5bps_ask": None,
            "book_levels_bid": len(book.bids), "book_levels_ask": len(book.asks),
        }
        if not book.synced or not book.bids or not book.asks:
            return empty

        out: dict[str, Any] = {}
        mid = book.mid()
        bid5, ask5 = book.depth_qty(5)
        bid20, ask20 = book.depth_qty(20)
        nb20, na20 = book.depth_notional(20)
        out["depth_imbalance_5"] = (
            (bid5 - ask5) / (bid5 + ask5) if (bid5 + ask5) > 0 else 0.0
        )
        out["depth_imbalance_20"] = (
            (bid20 - ask20) / (bid20 + ask20) if (bid20 + ask20) > 0 else 0.0
        )
        out["depth_notional_bid_20"] = nb20
        out["depth_notional_ask_20"] = na20

        bids, asks = book.top(20)
        out["liquidity_concentration_bid"] = (
            max(q for _, q in bids) / bid20 if bid20 > 0 else None
        )
        out["liquidity_concentration_ask"] = (
            max(q for _, q in asks) / ask20 if ask20 > 0 else None
        )
        # Un "muro" e' un livello sensibilmente piu' grande della media locale.
        k = self.cfg.book_wall_multiple
        avg_b = bid20 / max(len(bids), 1)
        avg_a = ask20 / max(len(asks), 1)
        bid_wall = next(((p, q) for p, q in bids if q >= k * avg_b), None)
        ask_wall = next(((p, q) for p, q in asks if q >= k * avg_a), None)
        out["bid_wall_size"] = bid_wall[1] if bid_wall else None
        out["ask_wall_size"] = ask_wall[1] if ask_wall else None
        out["bid_wall_distance_bps"] = (
            (mid - bid_wall[0]) / mid * 10_000.0 if bid_wall and mid else None
        )
        out["ask_wall_distance_bps"] = (
            (ask_wall[0] - mid) / mid * 10_000.0 if ask_wall and mid else None
        )

        wb, wa = book.depth_within_bps(5.0)
        out["depth_within_5bps_bid"] = wb
        out["depth_within_5bps_ask"] = wa

        self.bid_depth.append(ts, bid20)
        self.ask_depth.append(ts, ask20)
        prev_b = self.bid_depth.value_ago(1000)
        prev_a = self.ask_depth.value_ago(1000)
        out["liquidity_removal_bid"] = (
            (prev_b - bid20) / prev_b if prev_b and prev_b > 0 else None
        )
        out["liquidity_removal_ask"] = (
            (prev_a - ask20) / prev_a if prev_a and prev_a > 0 else None
        )
        out["book_levels_bid"] = len(book.bids)
        out["book_levels_ask"] = len(book.asks)
        return out


# --------------------------------------------------------------------------- #
#  AGENTI
# --------------------------------------------------------------------------- #

UP, DOWN, NO_TRADE = "UP", "DOWN", "NO_TRADE"
(TREND_UP, TREND_DOWN, RANGE, BREAKOUT, HIGH_VOL, LOW_VOL, EXHAUSTION,
 UNKNOWN) = ("TREND_UP", "TREND_DOWN", "RANGE", "BREAKOUT", "HIGH_VOLATILITY",
             "LOW_VOLATILITY", "EXHAUSTION", "UNKNOWN")


def squash(value: float, scale: float) -> float:
    """Porta una quantita' illimitata in (-1, 1) con un ginocchio morbido."""
    return math.tanh(value / scale) if scale > 0 else 0.0


@dataclass
class AgentOutput:
    agent: str
    direction: str
    confidence: float
    score: float
    reason: str
    features_used: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent": self.agent, "direction": self.direction,
            "confidence": round(self.confidence, 4), "score": round(self.score, 4),
            "reason": self.reason, "extra": self.extra,
        }


class Ctx:
    def __init__(self, ts: int, features: dict, quality: float, book_synced: bool):
        self.ts = ts
        self.features = features
        self.data_quality = quality
        self.book_synced = book_synced
        self.regime = UNKNOWN

    def f(self, name: str, default: Any = None) -> Any:
        v = self.features.get(name, default)
        return default if v is None else v

    def has(self, *names: str) -> bool:
        return all(self.features.get(n) is not None for n in names)


class Agent:
    name = "abstract"
    weight = 1.0

    def __init__(self, cfg: Config | None = None) -> None:
        # Gli agenti che filtrano su una manopola dell'operatore la leggono da
        # qui. Prima erano costanti nel codice, e chi allentava il parametro
        # nell'ambiente non otteneva alcun effetto.
        self.cfg = cfg

    def setting(self, name: str, default: Any) -> Any:
        return getattr(self.cfg, name, default) if self.cfg else default

    def evaluate(self, ctx: Ctx) -> AgentOutput:
        try:
            return self._evaluate(ctx)
        except Exception as exc:  # noqa: BLE001 - un agente rotto non ferma gli altri
            return self.abstain(f"errore nell'agente: {type(exc).__name__}: {exc}")

    def _evaluate(self, ctx: Ctx) -> AgentOutput:
        raise NotImplementedError

    def abstain(self, reason: str) -> AgentOutput:
        return AgentOutput(self.name, NO_TRADE, 0.0, 0.0, reason)

    def emit(self, score: float, confidence: float, reason: str,
             used: list[str], min_confidence: float = 0.5,
             extra: dict | None = None) -> AgentOutput:
        score = max(-1.0, min(1.0, score))
        confidence = max(0.0, min(1.0, confidence))
        direction = NO_TRADE
        if confidence >= min_confidence and score != 0:
            direction = UP if score > 0 else DOWN
        return AgentOutput(self.name, direction, confidence, score, reason,
                           used, extra or {})


class PriceActionAgent(Agent):
    name, weight = "price_action", 1.0

    def _evaluate(self, ctx: Ctx) -> AgentOutput:
        if not ctx.has("return_1000ms", "return_2000ms"):
            return self.abstain("storia dei prezzi insufficiente")
        r05, r1, r2 = ctx.f("return_500ms", 0.0), ctx.f("return_1000ms", 0.0), ctx.f("return_2000ms", 0.0)
        cons = ctx.f("momentum_consistency", 0.0)
        vol = ctx.f("realized_vol_5s_bps", 0.0) or 1.0
        # Il movimento va normalizzato per la volatilita': 2bps significano cose
        # molto diverse in un book calmo e in uno violento.
        norm = (0.5 * r05 + 0.3 * r1 + 0.2 * r2) / max(vol, 0.5)
        score = squash(norm, 1.5) * (0.5 + 0.5 * abs(cons))
        conf = min(0.95, abs(score) * 0.9 + 0.1 * abs(cons))
        return self.emit(score, conf,
                         f"ritorno breve {norm:+.2f}s, coerenza {cons:+.2f}",
                         ["return_500ms", "return_1000ms", "return_2000ms"], 0.35)


class OrderBookAgent(Agent):
    name, weight = "order_book", 1.6

    def _evaluate(self, ctx: Ctx) -> AgentOutput:
        if not ctx.book_synced:
            return self.abstain("book non sincronizzato")
        if not ctx.has("depth_imbalance_5"):
            return self.abstain("profondita' non disponibile")
        l1_raw = ctx.features.get("book_imbalance_l1")
        d5, d20 = ctx.f("depth_imbalance_5", 0.0), ctx.f("depth_imbalance_20", 0.0)
        micro_dev = ctx.f("micro_price_dev_bps", 0.0)
        spread = ctx.f("spread_bps", 1.0) or 1.0
        micro_term = 0.2 * squash(micro_dev / max(spread * 0.5, 0.05), 1.0)
        if l1_raw is not None:
            score = 0.35 * l1_raw + 0.3 * d5 + 0.15 * d20 + micro_term
            l1_text = f"L1 {l1_raw:+.2f}"
        else:
            # Quando il tocco e' polvere il suo peso va sulle misure di
            # profondita', che restano significative.
            score = 0.5 * d5 + 0.3 * d20 + micro_term
            l1_text = "L1 polvere (ignorato)"
        note = ""
        bid_wall = ctx.features.get("bid_wall_distance_bps")
        ask_wall = ctx.features.get("ask_wall_distance_bps")
        if ask_wall is not None and ask_wall < 2.0 and score > 0:
            score *= 0.5
            note = f"; muro ask a {ask_wall:.1f}bps"
        if bid_wall is not None and bid_wall < 2.0 and score < 0:
            score *= 0.5
            note = f"; muro bid a {bid_wall:.1f}bps"
        return self.emit(score, min(0.95, abs(score) * 1.1),
                         f"{l1_text}, depth5 {d5:+.2f}, micro {micro_dev:+.2f}bps{note}",
                         ["book_imbalance_l1", "depth_imbalance_5"], 0.35)


class OrderFlowAgent(Agent):
    name, weight = "order_flow", 1.8

    def _evaluate(self, ctx: Ctx) -> AgentOutput:
        if not ctx.has("volume_imbalance_1s", "volume_imbalance_5s"):
            return self.abstain("nessun flusso di scambi")
        count1 = ctx.f("trade_count_1s", 0.0)
        if count1 < 2:
            return self.abstain("flusso troppo rado per leggerlo")
        vi1, vi5 = ctx.f("volume_imbalance_1s", 0.0), ctx.f("volume_imbalance_5s", 0.0)
        buys, sells = ctx.f("consecutive_buys", 0.0), ctx.f("consecutive_sells", 0.0)
        lb, ls = ctx.f("large_buy_notional", 0.0), ctx.f("large_sell_notional", 0.0)
        streak = squash(buys - sells, 6.0)
        large = squash((lb - ls) / max(lb + ls, 1.0) * 2.0, 1.5) if (lb + ls) > 0 else 0.0
        score = 0.4 * vi1 + 0.3 * vi5 + 0.2 * streak + 0.1 * large
        activity = min(1.0, count1 / 8.0)
        conf = min(0.95, abs(score) * 1.15 * (0.4 + 0.6 * activity))
        return self.emit(score, conf,
                         f"flusso 1s {vi1:+.2f} / 5s {vi5:+.2f}, "
                         f"serie +{int(buys)}/-{int(sells)}, {int(count1)} trade/s",
                         ["volume_imbalance_1s", "volume_imbalance_5s"], 0.35)


class VolatilityAgent(Agent):
    """Non sceglie un lato: dice se *un* lato vale la pena."""

    name, weight = "volatility", 0.8

    def _evaluate(self, ctx: Ctx) -> AgentOutput:
        v5 = ctx.features.get("realized_vol_5s_bps")
        v30 = ctx.features.get("realized_vol_30s_bps")
        if v5 is None or v30 is None:
            return self.abstain("volatilita' non ancora stimabile")
        spread = ctx.f("spread_bps", 1.0) or 1.0
        edge_ratio = v5 / max(spread, 0.05)
        # Su un venue vero lo spread e' un tick, quindi "batte lo spread" e'
        # quasi sempre vero e non filtra niente. Cio' che decide davvero e' se
        # il prezzo si muove: le due soglie vengono dalla configurazione.
        min_ticks = self.setting("min_expected_move_ticks", 2.0)
        max_flat = self.setting("max_zero_move_fraction", 0.35)
        expected = ctx.features.get("expected_move_ticks")
        zero_move = ctx.features.get("zero_move_fraction")
        moves_enough = expected is None or expected >= min_ticks
        rarely_flat = zero_move is None or zero_move <= max_flat
        tradable = edge_ratio > 0.8 and moves_enough and rarely_flat
        conf = min(0.9, edge_ratio / 3.0) if tradable else 0.0
        reason = f"vol5s {v5:.2f}bps vs spread {spread:.2f}bps (x{edge_ratio:.2f})"
        if not moves_enough:
            reason += f"; movimento atteso {expected:.1f} tick sotto {min_ticks:g}"
        if not rarely_flat:
            reason += f"; {zero_move:.0%} delle finestre senza alcun movimento"
        out = self.emit(0.0, conf, reason,
                        ["realized_vol_5s_bps", "expected_move_ticks",
                         "zero_move_fraction"], 0.99,
                        {"tradable": tradable, "vol_spread_ratio": edge_ratio,
                         "expected_move_ticks": expected,
                         "zero_move_fraction": zero_move})
        # Mai direzionale: uno score qui inietterebbe un'opinione di momentum
        # nell'aggregato con il peso di questo agente.
        out.direction = NO_TRADE
        out.confidence = conf
        return out


class MomentumAgent(Agent):
    name, weight = "momentum", 1.1

    def _evaluate(self, ctx: Ctx) -> AgentOutput:
        if not ctx.has("return_5000ms"):
            return self.abstain("storia insufficiente per il momentum")
        r2, r5 = ctx.f("return_2000ms", 0.0), ctx.f("return_5000ms", 0.0)
        accel = ctx.f("acceleration_bps_s2", 0.0)
        ema_spread = ctx.f("ema_spread_bps", 0.0)
        flow = ctx.f("volume_imbalance_5s", 0.0)
        vol = ctx.f("realized_vol_30s_bps", 0.0) or 1.0
        trend = (0.5 * r5 + 0.5 * r2) / max(vol, 0.5)
        confirmation = 1.0 if trend * flow > 0 else 0.45
        score = squash(trend, 2.0) * confirmation + 0.15 * squash(ema_spread, 3.0)
        score += 0.1 * squash(accel, 3.0)
        return self.emit(score, min(0.92, abs(score) * confirmation),
                         f"trend {trend:+.2f}s, ema {ema_spread:+.2f}bps, flusso "
                         f"{'conferma' if confirmation > 0.5 else 'diverge'}",
                         ["return_2000ms", "return_5000ms"], 0.4)


class MeanReversionAgent(Agent):
    name, weight = "mean_reversion", 1.1

    def _evaluate(self, ctx: Ctx) -> AgentOutput:
        z = ctx.features.get("bb_z")
        if z is None:
            return self.abstain("statistiche di banda non disponibili")
        vwap_dev = ctx.f("vwap_deviation_bps", 0.0)
        r1 = ctx.f("return_1000ms", 0.0)
        rsi_v = ctx.f("rsi_14", 50.0)
        vol = ctx.f("realized_vol_30s_bps", 0.0) or 1.0
        # Si sfuma solo uno strappo vero, e solo quando l'impulso sta gia'
        # perdendo forza.
        stretch = -squash(z, 1.6)
        rsi_term = -squash((rsi_v - 50.0) / 20.0, 1.0) * 0.5
        vwap_term = -squash(vwap_dev / max(vol, 0.5), 2.0) * 0.5
        exhausting = 1.0 if (r1 * z) < 0 else 0.5
        score = (0.55 * stretch + 0.25 * rsi_term + 0.20 * vwap_term) * exhausting
        conf = min(0.9, abs(score) * (0.9 if abs(z) > 1.5 else 0.5))
        return self.emit(score, conf,
                         f"z {z:+.2f}, vwap {vwap_dev:+.2f}bps, rsi {rsi_v:.0f}",
                         ["bb_z", "vwap_deviation_bps", "rsi_14"], 0.4)


class MarketRegimeAgent(Agent):
    name, weight = "market_regime", 0.6

    def classify(self, ctx: Ctx) -> tuple[str, float, str]:
        v5 = ctx.features.get("realized_vol_5s_bps")
        v30 = ctx.features.get("realized_vol_30s_bps")
        r5 = ctx.features.get("return_5000ms")
        cons = abs(ctx.features.get("momentum_consistency") or 0.0)
        if v30 is None or r5 is None:
            return (UNKNOWN, 0.0, "storia insufficiente per classificare")
        ratio = (v5 / v30) if (v5 and v30 and v30 > 0) else 1.0
        strength = abs(r5) / max(v30, 0.5)
        if ratio > 2.2 and strength > 1.5:
            return (BREAKOUT, min(0.9, ratio / 3.0),
                    f"esplosione di volatilita' {ratio:.1f}x con {strength:.1f}s")
        if ratio > 1.8:
            return (HIGH_VOL, min(0.85, ratio / 3.0), f"vol breve {ratio:.1f}x")
        if ratio < 0.45:
            return (LOW_VOL, min(0.8, 1.0 - ratio), f"compressione {ratio:.2f}x")
        if strength > 1.2 and cons > 0.6:
            return (TREND_UP if r5 > 0 else TREND_DOWN, min(0.9, strength / 2.5),
                    f"direzionale {strength:.1f}s, coerenza {cons:.2f}")
        if strength > 1.2 and cons < 0.3:
            return (EXHAUSTION, 0.55, f"movimento {strength:.1f}s ma orizzonti discordi")
        return (RANGE, 0.6, f"nessuna direzione dominante ({strength:.1f}s)")

    def _evaluate(self, ctx: Ctx) -> AgentOutput:
        regime, conf, reason = self.classify(ctx)
        bias = {TREND_UP: 0.4, TREND_DOWN: -0.4}.get(regime, 0.0)
        out = self.emit(bias, conf, f"{regime}: {reason}",
                        ["realized_vol_5s_bps", "return_5000ms"], 0.6,
                        {"regime": regime})
        if regime == UNKNOWN:
            out.direction = NO_TRADE
        return out


class AnomalyDetector(Agent):
    """Non da' mai una direzione. Puo' solo mettere il veto.

    I rilievi sono divisi in HARD e SOFT: uno hard descrive un mercato rotto e
    vieta da solo, uno soft e' un segnale d'allarme comune su un venue vero e
    vieta solo se se ne accumulano. Trattare ogni rilievo soft come un veto e'
    il motivo per cui il motore poteva restare fermo per un'intera sessione
    normale.
    """

    name, weight = "anomaly", 0.0
    HARD = ("spread non valido", "spread anomalo", "picco di prezzo",
            "book desincronizzato", "qualita' dati", "latenza")

    def _evaluate(self, ctx: Ctx) -> AgentOutput:
        f = ctx.features
        anomalies: list[str] = []
        spread = f.get("spread_bps")
        vol30, vol5 = f.get("realized_vol_30s_bps"), f.get("realized_vol_5s_bps")
        r100 = f.get("return_100ms")

        if spread is None or spread <= 0:
            anomalies.append("spread non valido")
        elif vol30 and spread > max(8.0, 6.0 * vol30):
            anomalies.append(f"spread anomalo {spread:.2f}bps")
        if r100 is not None and vol5 and abs(r100) > 8.0 * max(vol5, 0.5):
            anomalies.append(f"picco di prezzo {r100:+.1f}bps in 100ms")
        rem_b, rem_a = f.get("liquidity_removal_bid"), f.get("liquidity_removal_ask")
        if rem_b is not None and rem_b > 0.6:
            anomalies.append("liquidita' bid sparita")
        if rem_a is not None and rem_a > 0.6:
            anomalies.append("liquidita' ask sparita")
        db, da = f.get("depth_notional_bid_20"), f.get("depth_notional_ask_20")
        if db is not None and da is not None and (db + da) > 0:
            if min(db, da) / (db + da) < 0.08:
                anomalies.append("book tutto da un lato")
        i1, i30 = f.get("trade_intensity_1s"), f.get("trade_intensity_30s")
        if i1 is not None and i30 and i30 > 0.5 and i1 > 12.0 * i30:
            anomalies.append(f"raffica di volumi {i1 / i30:.0f}x")
        latency = f.get("latency_ms")
        if latency is not None and latency > 1000:
            anomalies.append(f"latenza del feed {latency:.0f}ms")
        if not ctx.book_synced:
            anomalies.append("book desincronizzato")
        if ctx.data_quality < 0.5:
            anomalies.append(f"qualita' dati {ctx.data_quality:.2f}")

        severity = min(1.0, len(anomalies) / 3.0)
        hard = [a for a in anomalies if a.startswith(self.HARD)]
        return AgentOutput(
            self.name, NO_TRADE, severity, 0.0,
            "; ".join(anomalies) if anomalies else "nessuna anomalia",
            ["spread_bps", "return_100ms", "latency_ms"],
            {"anomalies": anomalies, "anomaly_detected": bool(anomalies),
             "hard_anomalies": hard, "severity": round(severity, 3)},
        )


def build_agents(cfg: Config | None = None) -> list[Agent]:
    return [PriceActionAgent(cfg), OrderBookAgent(cfg), OrderFlowAgent(cfg),
            VolatilityAgent(cfg), MomentumAgent(cfg), MeanReversionAgent(cfg),
            MarketRegimeAgent(cfg), AnomalyDetector(cfg)]


# --------------------------------------------------------------------------- #
#  MOTORE DECISIONALE
# --------------------------------------------------------------------------- #


@dataclass
class Decision:
    ts: int
    symbol: str
    exchange: str
    direction: str
    prob_up: float
    prob_down: float
    prob_neutral: float
    confidence: float
    edge: float
    regime: str
    reference_price: float
    trigger_price: float | None
    horizon_s: float
    no_trade_reasons: list[str]
    agents: list[AgentOutput] = field(default_factory=list)
    data_quality: float = 0.0
    model_id: str | None = None
    model_prob_up: float | None = None
    calibrated: bool = False
    #: "TRIGGER" aspetta che il prezzo tocchi il livello; "DELAY" entra a
    #: mercato dopo `entry_delay_ms` (e' quello che fa BURST-15).
    entry_mode: str = "TRIGGER"
    entry_delay_ms: int = 0
    strategy: str = "ensemble"
    #: Da che parte pendeva l'aggregato PRIMA dei cancelli. Sopravvive anche a
    #: un NO TRADE, cosi' una finestra scartata resta un'osservazione.
    lean: str = NO_TRADE
    lean_confidence: float = 0.0
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def is_trade(self) -> bool:
        return self.direction in (UP, DOWN)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": self.ts, "symbol": self.symbol, "exchange": self.exchange,
            "direction": self.direction, "prob_up": round(self.prob_up, 4),
            "prob_down": round(self.prob_down, 4),
            "prob_neutral": round(self.prob_neutral, 4),
            "confidence": round(self.confidence, 4), "edge": round(self.edge, 4),
            "regime": self.regime, "reference_price": self.reference_price,
            "trigger_price": self.trigger_price, "horizon_s": self.horizon_s,
            "entry_mode": self.entry_mode, "entry_delay_ms": self.entry_delay_ms,
            "strategy": self.strategy, "no_trade_reasons": self.no_trade_reasons,
            "lean": self.lean, "lean_confidence": round(self.lean_confidence, 4),
            "data_quality": round(self.data_quality, 3), "model_id": self.model_id,
            "model_prob_up": self.model_prob_up, "calibrated": self.calibrated,
            "agents": [a.to_dict() for a in self.agents], "detail": self.detail,
        }


class DecisionEngine:
    """Trasforma le opinioni degli agenti in UP, DOWN o NO TRADE.

    NO TRADE e' un risultato di prima classe, non un fallimento. I cancelli
    girano PRIMA di qualsiasi punteggio: forzare un segnale dal rumore e' il
    modo piu' facile di prendersi in giro.
    """

    def __init__(self, cfg: Config, model: "Model | None" = None,
                 performance: Callable[[], dict[str, float]] | None = None) -> None:
        self.cfg = cfg
        self.agents = build_agents(cfg)
        self.regime_agent = next(
            a for a in self.agents if isinstance(a, MarketRegimeAgent)
        )
        self.model = model
        self.performance = performance

    def decide(self, vector: dict, market: dict, health: dict) -> Decision:
        cfg = self.cfg
        f = vector.get("features", {})
        ts = vector.get("ts") or now_ms()
        quality = health.get("data_quality", {})
        dq = float(quality.get("score", 0.0))
        book_synced = bool(f.get("book_synced"))
        ref_price = market.get("price") or f.get("mid") or 0.0

        ctx = Ctx(ts, f, dq, book_synced)
        regime, regime_conf, regime_reason = self.regime_agent.classify(ctx)
        ctx.regime = regime
        outputs = [a.evaluate(ctx) for a in self.agents]
        by_name = {o.agent: o for o in outputs}

        # ------------------------------------------------------ cancelli duri
        reasons: list[str] = []
        if not cfg.signal_enabled:
            reasons.append("motore dei segnali disattivato")
        if not quality.get("warmup_complete", False):
            reasons.append("motore in riscaldamento")
        if dq < cfg.min_data_quality:
            reasons.append(f"qualita' dati {dq:.2f} < {cfg.min_data_quality:.2f}")
        for r in quality.get("reasons", []):
            if r not in reasons:
                reasons.append(r)
        if not book_synced:
            reasons.append("book non sincronizzato")
        spread_bps = f.get("spread_bps")
        if spread_bps is None:
            reasons.append("spread non disponibile")
        elif spread_bps > cfg.max_spread_bps:
            reasons.append(
                f"spread {spread_bps:.2f}bps oltre il limite {cfg.max_spread_bps:.2f}"
            )
        latency = f.get("latency_ms")
        if latency is not None and latency > cfg.max_latency_ms:
            reasons.append(f"latenza {latency:.0f}ms oltre {cfg.max_latency_ms:.0f}ms")

        anomaly = by_name.get("anomaly")
        if anomaly and anomaly.extra.get("anomaly_detected"):
            hard = anomaly.extra.get("hard_anomalies") or []
            severity = float(anomaly.extra.get("severity") or 0.0)
            if hard:
                reasons.append("anomalia: " + "; ".join(hard))
            elif severity > cfg.anomaly_max_severity:
                reasons.append(
                    f"gravita' anomalie {severity:.2f} oltre "
                    f"{cfg.anomaly_max_severity:.2f}: {anomaly.reason}"
                )
        if regime == UNKNOWN:
            reasons.append("regime di mercato sconosciuto")
        missing = [k for k in ("return_1000ms", "volume_imbalance_1s",
                               "depth_imbalance_5") if f.get(k) is None]
        if missing:
            reasons.append(f"feature incomplete: {', '.join(missing)}")

        zero_move = f.get("zero_move_fraction")
        if zero_move is not None and zero_move > cfg.max_zero_move_fraction:
            reasons.append(
                f"{zero_move:.0%} delle finestre da {cfg.horizon_s:g}s non ha avuto "
                f"alcun movimento (limite {cfg.max_zero_move_fraction:.0%})"
            )
        expected = f.get("expected_move_ticks")
        if expected is not None and expected < cfg.min_expected_move_ticks:
            reasons.append(
                f"movimento atteso {expected:.1f} tick sotto il minimo di "
                f"{cfg.min_expected_move_ticks:g}"
            )
        vol_agent = by_name.get("volatility")
        if vol_agent and not vol_agent.extra.get("tradable", False):
            reasons.append("movimento atteso troppo piccolo per essere sfruttabile")

        # -------------------------------------------------------- aggregazione
        agg, weights = self._aggregate(outputs, regime)
        p_up, p_down, p_neutral = self._probabilities(agg, outputs)

        model_prob = model_id = None
        calibrated = False
        if self.model is not None and self.model.ready:
            out = self.model.predict(f)
            if out is None:
                reasons.append("modello non applicabile a queste feature")
            elif out.get("out_of_distribution"):
                reasons.append("modello fuori distribuzione: " + str(out.get("reason")))
            else:
                model_prob = float(out["prob_up"])
                model_id = out.get("model_id")
                calibrated = bool(out.get("calibrated"))
                # Il modello pesa meta' dell'ensemble solo se validato fuori
                # campione (calibrated=True).
                w = 0.5 if calibrated else 0.3
                directional = p_up / max(p_up + p_down, 1e-9)
                blended = (1 - w) * directional + w * model_prob
                mass = p_up + p_down
                p_up, p_down = blended * mass, (1 - blended) * mass

        directional = p_up / max(p_up + p_down, 1e-9)
        edge = abs(directional - 0.5)
        confidence = max(directional, 1 - directional)
        mass = p_up + p_down

        # Due domande diverse, due manopole diverse:
        #   mass       - gli agenti concordano abbastanza da rivendicare l'esito?
        #   confidence - dato che concordano, quanto e' netta la direzione?
        min_conf = cfg.effective_min_confidence
        if mass < cfg.min_agreement:
            reasons.append(f"accordo fra agenti {mass:.2f} sotto {cfg.min_agreement:.2f}")
        if confidence < min_conf:
            reasons.append(
                f"confidenza {confidence:.2f} sotto {min_conf:.2f} "
                f"(edge {edge:.3f} contro il minimo {cfg.min_edge:.3f})"
            )

        direction = NO_TRADE
        trigger = None
        if not reasons and ref_price > 0:
            direction = UP if directional > 0.5 else DOWN
            trigger = self._trigger_price(direction, ref_price, f)
            if trigger is None:
                reasons.append("trigger non dimensionabile: manca la volatilita'")
                direction = NO_TRADE

        return Decision(
            ts=ts, symbol=vector.get("symbol", cfg.symbol),
            exchange=vector.get("exchange", ""), direction=direction,
            prob_up=p_up, prob_down=p_down, prob_neutral=p_neutral,
            confidence=confidence if direction != NO_TRADE else 0.0,
            edge=edge, regime=regime, reference_price=ref_price,
            trigger_price=trigger, horizon_s=cfg.horizon_s,
            no_trade_reasons=reasons, agents=outputs, data_quality=dq,
            model_id=model_id, model_prob_up=model_prob, calibrated=calibrated,
            strategy="ensemble",
            lean=UP if directional > 0.5 else DOWN, lean_confidence=confidence,
            detail={
                "regime_reason": regime_reason,
                "regime_confidence": round(regime_conf, 3),
                "weights": weights, "agreement_mass": round(mass, 4),
                "sigma_horizon_bps": f.get("sigma_horizon_bps"),
                "thresholds": {
                    "min_agreement": cfg.min_agreement,
                    "min_confidence": round(min_conf, 4),
                    "min_edge": cfg.min_edge,
                },
            },
        )

    def _aggregate(self, outputs: list[AgentOutput], regime: str):
        """Media pesata per confidenza dei punteggi, corretta dal regime.

        Momentum e mean reversion sono strutturalmente opposti: e' il regime a
        decidere quale dei due puo' parlare a voce alta.
        """
        multiplier = {
            TREND_UP: {"momentum": 1.4, "mean_reversion": 0.45},
            TREND_DOWN: {"momentum": 1.4, "mean_reversion": 0.45},
            RANGE: {"momentum": 0.6, "mean_reversion": 1.35},
            BREAKOUT: {"momentum": 1.5, "mean_reversion": 0.3, "order_flow": 1.2},
            EXHAUSTION: {"momentum": 0.5, "mean_reversion": 1.3},
            HIGH_VOL: {"price_action": 0.8, "order_book": 1.1},
            LOW_VOL: {"order_book": 1.2, "order_flow": 1.1},
        }.get(regime, {})

        hit_rates: dict[str, float] = {}
        if self.performance is not None:
            try:
                hit_rates = self.performance() or {}
            except Exception:  # noqa: BLE001 - lo storico e' facoltativo
                hit_rates = {}

        num = den = 0.0
        used: dict[str, float] = {}
        by_name = {o.agent: o for o in outputs}
        for agent in self.agents:
            out = by_name.get(agent.name)
            if out is None or agent.weight <= 0:
                continue
            if out.direction == NO_TRADE and out.score == 0:
                continue
            w = agent.weight * multiplier.get(agent.name, 1.0)
            hr = hit_rates.get(agent.name)
            if hr is not None:
                w *= max(0.6, min(1.4, 1.0 + (hr - 0.5) * 2.0))
            contribution = w * out.confidence
            num += out.score * contribution
            den += contribution
            used[agent.name] = round(w, 3)
        return ((num / den) if den > 0 else 0.0, used)

    def _probabilities(self, agg: float, outputs: list[AgentOutput]):
        """Divide la massa di probabilita' fra UP, DOWN e NEUTRAL.

        `mass` e' quanta parte dello spazio degli esiti gli agenti sono
        disposti a rivendicare: dipende dall'accordo, non dall'ampiezza del
        movimento. Quel che resta e' P(NEUTRAL). Sono uscite di un modello, non
        frequenze calibrate, finche' il rapporto di calibrazione non dice
        altrimenti.
        """
        directional = [o for o in outputs
                       if o.direction in (UP, DOWN) and o.confidence > 0]
        if not directional:
            return (0.0, 0.0, 1.0)
        ups = sum(o.confidence for o in directional if o.direction == UP)
        downs = sum(o.confidence for o in directional if o.direction == DOWN)
        total = ups + downs
        agreement = abs(ups - downs) / total if total > 0 else 0.0
        avg_conf = total / len(directional)
        mass = max(0.0, min(0.98, agreement * avg_conf))
        p_dir_up = 1.0 / (1.0 + math.exp(-3.0 * agg))
        return (p_dir_up * mass, (1.0 - p_dir_up) * mass, 1.0 - mass)

    def _trigger_price(self, direction: str, ref: float, f: dict) -> float | None:
        """Dove il prezzo deve passare prima che parta il countdown.

        Dimensionato sulla volatilita' osservata sull'orizzonte, cosi' e'
        raggiungibile in un mercato calmo e non banale in uno veloce.
        """
        cfg = self.cfg
        sigma = f.get("sigma_horizon_bps") or f.get("realized_vol_5s_bps")
        if not sigma or sigma <= 0:
            return None
        offset_bps = min(cfg.trigger_max_bps, cfg.trigger_sigma_k * sigma)
        offset = max(ref * offset_bps / 10_000.0, cfg.trigger_min_ticks * cfg.tick_size)
        raw = ref + offset if direction == UP else ref - offset
        ticks = round(raw / cfg.tick_size)
        price = round(ticks * cfg.tick_size, 8)
        if direction == UP and price <= ref:
            price = round((ticks + 1) * cfg.tick_size, 8)
        if direction == DOWN and price >= ref:
            price = round((ticks - 1) * cfg.tick_size, 8)
        return price


# --------------------------------------------------------------------------- #
#  AURUM BURST-15
# --------------------------------------------------------------------------- #

N5_FEATURE, R10_FEATURE, OFI_FEATURE = (
    "trade_count_5s", "return_10000ms", "ofi_notional_5s"
)


@dataclass
class BurstSession:
    start_ts: int
    end_ts: int
    pnl_units: float = 0.0
    trades: int = 0
    wins: int = 0
    losses: int = 0
    ties: int = 0
    last_entry_ts: int = -10 ** 18
    closed_reason: str | None = None
    open_signals: set = field(default_factory=set)

    def is_open(self, ts: int) -> bool:
        return self.closed_reason is None and ts < self.end_ts

    def to_dict(self, ts: int | None = None) -> dict[str, Any]:
        ts = ts or now_ms()
        return {
            "start_ts": self.start_ts, "end_ts": self.end_ts,
            "remaining_ms": max(0, self.end_ts - ts),
            "pnl_units": round(self.pnl_units, 4), "trades": self.trades,
            "wins": self.wins, "losses": self.losses, "ties": self.ties,
            "open_signals": len(self.open_signals),
            "closed_reason": self.closed_reason, "is_open": self.is_open(ts),
        }


class BurstStrategy:
    """Finestra operativa di 15 minuti su orizzonte di 5 secondi.

    Non prevede dove sara' il prezzo fra quindici minuti: apre una FINESTRA di
    quindici minuti e, dentro quella, prende solo le raffiche di tape.

        n5    = trade negli ultimi 5s        >= BURST_N5_MIN
        |r10| = |ritorno a 10s| in bps       >= BURST_R10_MIN_BPS
        ofi5  = squilibrio di flusso a 5s    concorde con r10
        direzione = segno di r10

    L'ingresso e' A TEMPO, non a prezzo: BURST_ENTRY_DELAY_MS dopo il trigger,
    a mercato. Non si aspetta un livello perche' la premessa e' che il
    movimento sia gia' in corso.
    """

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session: BurstSession | None = None
        self.sessions_completed = 0
        self.history: list[dict] = []

    @property
    def payout(self) -> float:
        """Payout usato per l'aritmetica di rischio della sessione.

        Quello del broker se e' noto. Se non lo e', la sessione ha comunque
        bisogno di un numero contro cui misurare lo stop-loss: usa quello
        assunto e lo dichiara. Il P&L monetario riportato altrove continua a
        dire PAYOUT SCONOSCIUTO invece di inventarlo.
        """
        return self.cfg.payout_for_risk

    @property
    def payout_is_assumed(self) -> bool:
        return self.cfg.payout is None

    def open_session(self, ts: int | None = None) -> BurstSession:
        ts = ts or now_ms()
        self.session = BurstSession(ts, ts + self.cfg.burst_session_s * 1000)
        return self.session

    def close_session(self, reason: str, ts: int | None = None) -> None:
        if self.session is None or self.session.closed_reason:
            return
        self.session.closed_reason = reason
        self.sessions_completed += 1
        self.history.append({**self.session.to_dict(ts), "closed_at": ts or now_ms()})
        self.history = self.history[-50:]

    def _current(self, ts: int) -> BurstSession | None:
        s = self.session
        if s is not None and s.closed_reason is None and ts >= s.end_ts:
            self.close_session("finestra conclusa", ts)
            s = self.session
        if s is None:
            return self.open_session(ts) if self.cfg.burst_auto_restart else None
        if s.closed_reason:
            # Una sessione andata in stop NON riapre prima della sua scadenza
            # naturale: riaprirla subito trasformerebbe lo stop-loss in un
            # consiglio.
            if self.cfg.burst_auto_restart and ts >= s.end_ts:
                return self.open_session(ts)
            return None
        return s

    def on_entry(self, signal_id: str, ts: int) -> None:
        if self.session is None:
            return
        self.session.trades += 1
        self.session.last_entry_ts = ts
        self.session.open_signals.add(signal_id)

    def on_settled(self, signal_id: str, result: str | None, ts: int | None = None) -> None:
        s = self.session
        if s is None:
            return
        s.open_signals.discard(signal_id)
        if result == "WIN":
            s.pnl_units += self.payout * self.cfg.stake
            s.wins += 1
        elif result == "LOSS":
            s.pnl_units -= self.cfg.stake
            s.losses += 1
        elif result == "TIE":
            s.ties += 1
        else:
            return  # CANCELLED: mai entrato, non costa e non conta
        if s.pnl_units <= self.cfg.burst_stop_loss_units:
            self.close_session("stop loss di sessione", ts)
        elif s.pnl_units >= self.cfg.burst_take_profit_units:
            self.close_session("take profit di sessione", ts)

    def decide(self, vector: dict, market: dict, health: dict) -> Decision:
        cfg = self.cfg
        f = vector.get("features", {})
        ts = int(vector.get("ts") or now_ms())
        quality = health.get("data_quality", {})
        dq = float(quality.get("score", 0.0))
        ref_price = market.get("price") or f.get("mid") or 0.0

        reasons: list[str] = []
        if not cfg.signal_enabled:
            reasons.append("motore dei segnali disattivato")
        if not quality.get("warmup_complete", False):
            reasons.append("motore in riscaldamento")

        session = self._current(ts)
        if session is None:
            closed = self.session.closed_reason if self.session else "nessuna sessione"
            reasons.append(f"nessuna sessione aperta: {closed}")
        elif session.trades >= cfg.burst_max_trades_session:
            self.close_session("massimo di trade raggiunto", ts)
            reasons.append("massimo di trade della sessione raggiunto")
        elif ts - session.last_entry_ts < cfg.burst_cooldown_ms:
            left = cfg.burst_cooldown_ms - (ts - session.last_entry_ts)
            reasons.append(f"cooldown, mancano {left}ms")
        elif session.open_signals:
            reasons.append("un trade della sessione e' ancora aperto")

        staleness = health.get("feed_age_ms")
        if staleness is None:
            reasons.append("nessun dato di mercato ricevuto")
        elif staleness > cfg.burst_max_staleness_ms:
            reasons.append(f"feed fermo da {staleness:.0f}ms")
        if dq < cfg.burst_min_data_quality:
            reasons.append(f"qualita' dati {dq:.2f} < {cfg.burst_min_data_quality:.2f}")
        for r in quality.get("reasons", []):
            if r not in reasons:
                reasons.append(r)

        n5, r10, ofi5 = f.get(N5_FEATURE), f.get(R10_FEATURE), f.get(OFI_FEATURE)
        if n5 is None or r10 is None:
            reasons.append("feature del burst non disponibili (servono 10s di storia)")
        else:
            if n5 < cfg.burst_n5_min:
                reasons.append(f"tape troppo calma: {n5:.0f} trade/5s < {cfg.burst_n5_min}")
            if abs(r10) < cfg.burst_r10_min_bps:
                reasons.append(
                    f"movimento {abs(r10):.2f}bps < {cfg.burst_r10_min_bps:g}bps su 10s"
                )
            if r10 == 0:
                reasons.append("nessun movimento a 10s da seguire")
            elif cfg.burst_require_ofi_agree and ofi5 is not None:
                if (ofi5 > 0) != (r10 > 0):
                    reasons.append(
                        f"flusso discorde: ofi5 {ofi5:+.2f} contro r10 {r10:+.2f}bps"
                    )

        lean = NO_TRADE
        if r10 is not None and r10 != 0:
            lean = UP if r10 > 0 else DOWN
        direction = NO_TRADE
        if not reasons and ref_price > 0 and lean != NO_TRADE:
            direction = lean
        elif not reasons:
            reasons.append("prezzo di riferimento assente")

        # BURST-15 e' una regola, non un modello: non rivendica una probabilita'
        # calibrata. Questo numero dice quanto il trigger ha superato le proprie
        # soglie, e serve a ordinare i segnali e a riempire la calibrazione.
        strength = self._strength(n5, r10, ofi5)
        confidence = 0.5 + 0.5 * strength if direction != NO_TRADE else 0.0
        p_up = confidence if lean == UP else (1.0 - confidence)

        return Decision(
            ts=ts, symbol=vector.get("symbol", cfg.symbol),
            exchange=vector.get("exchange", ""), direction=direction,
            prob_up=p_up if direction != NO_TRADE else 0.0,
            prob_down=(1.0 - p_up) if direction != NO_TRADE else 0.0,
            prob_neutral=1.0 if direction == NO_TRADE else 0.0,
            confidence=confidence, edge=abs(confidence - 0.5),
            regime=BREAKOUT if direction != NO_TRADE else RANGE,
            reference_price=ref_price,
            trigger_price=ref_price if direction != NO_TRADE else None,
            horizon_s=cfg.horizon_s, no_trade_reasons=reasons, agents=[],
            data_quality=dq, entry_mode="DELAY",
            entry_delay_ms=cfg.burst_entry_delay_ms, strategy="burst15",
            lean=lean, lean_confidence=0.5 + 0.5 * strength,
            detail={
                "strategy": "burst15", "n5": n5, "r10_bps": r10, "ofi5": ofi5,
                "thresholds": {
                    "n5_min": cfg.burst_n5_min,
                    "r10_min_bps": cfg.burst_r10_min_bps,
                    "require_ofi_agree": cfg.burst_require_ofi_agree,
                },
                "session": session.to_dict(ts) if session else None,
                "payout_assumed": self.payout_is_assumed,
            },
        )

    def _strength(self, n5, r10, ofi5) -> float:
        if n5 is None or r10 is None:
            return 0.0
        cfg = self.cfg
        move = abs(r10) / max(cfg.burst_r10_min_bps, 1e-9)
        tape = n5 / max(cfg.burst_n5_min, 1)
        flow = abs(ofi5) if ofi5 is not None else 0.0
        raw = 0.5 * math.tanh(move - 1.0) + 0.3 * math.tanh(tape - 1.0) + 0.2 * flow
        return max(0.0, min(1.0, raw))

    def status(self) -> dict[str, Any]:
        ts = now_ms()
        return {
            "strategy": "burst15",
            "session": self.session.to_dict(ts) if self.session else None,
            "sessions_completed": self.sessions_completed,
            "recent_sessions": self.history[-10:][::-1],
            "payout_used_for_session_risk": self.payout,
            "payout_is_assumed": self.payout_is_assumed,
            "config": {
                "n5_min": self.cfg.burst_n5_min,
                "r10_min_bps": self.cfg.burst_r10_min_bps,
                "require_ofi_agree": self.cfg.burst_require_ofi_agree,
                "horizon_s": self.cfg.horizon_s,
                "entry_delay_ms": self.cfg.burst_entry_delay_ms,
                "session_s": self.cfg.burst_session_s,
                "cooldown_ms": self.cfg.burst_cooldown_ms,
                "max_trades_session": self.cfg.burst_max_trades_session,
                "stop_loss_units": self.cfg.burst_stop_loss_units,
                "take_profit_units": self.cfg.burst_take_profit_units,
            },
            "note": (
                "P&L di sessione in unita' di puntata, solo su carta. Senza "
                "BINARY_PAYOUT lo stop-loss usa il payout assunto; il P&L "
                "monetario altrove resta PAYOUT SCONOSCIUTO."
            ),
        }


# --------------------------------------------------------------------------- #
#  CICLO DI VITA DEL SEGNALE E PAPER TRADING
# --------------------------------------------------------------------------- #

WAITING, TRIGGERED, ACTIVE, EXPIRED = "WAITING", "TRIGGERED", "ACTIVE", "EXPIRED"
WIN, LOSS, TIE, CANCELLED = "WIN", "LOSS", "TIE", "CANCELLED"

import re as _re

#: I numeri dentro un motivo di blocco sono la misura, non la categoria: li si
#: comprime per poter contare "cosa ci ferma" invece di diecimila stringhe.
_NUMBERS = _re.compile(r"[-+]?\d[\d_.,]*")


def gate_key(reason: str) -> str:
    return _NUMBERS.sub("N", reason).strip()


@dataclass
class LiveSignal:
    signal_id: str
    symbol: str
    exchange: str
    strategy: str
    direction: str
    status: str
    reference_price: float
    trigger_price: float
    horizon_s: float
    confidence: float
    prob_up: float
    prob_down: float
    edge: float
    regime: str
    created_at: int
    expires_wait_at: int
    entry_mode: str = "TRIGGER"
    entry_delay_ms: int = 0
    triggered_at: int | None = None
    expires_at: int | None = None
    settled_at: int | None = None
    entry_price: float | None = None
    expiry_price: float | None = None
    result: str | None = None
    pnl_units: float | None = None
    data_quality: float = 0.0
    model_id: str | None = None
    is_synthetic: bool = False
    features: dict = field(default_factory=dict)

    def to_dict(self, server_ts: int | None = None) -> dict[str, Any]:
        d = {k: v for k, v in asdict(self).items() if k != "features"}
        ts = server_ts or now_ms()
        d["server_ts"] = ts
        # Countdown guidato dal motore: il client lo disegna, non lo inventa.
        if self.status in (ACTIVE, TRIGGERED) and self.expires_at:
            d["remaining_ms"] = max(0, self.expires_at - ts)
            d["wait_remaining_ms"] = None
        elif self.status == WAITING:
            d["remaining_ms"] = None
            d["wait_remaining_ms"] = max(0, self.expires_wait_at - ts)
        else:
            d["remaining_ms"] = 0
            d["wait_remaining_ms"] = None
        return d


class SignalEngine:
    """Dalla decisione al risultato:

        ANALISI -> SEGNALE -> ATTESA TRIGGER -> TRIGGER -> ATTIVO -> ESITO

    Il trigger e il countdown appartengono al motore, non all'interfaccia; e
    nulla viene eseguito su un venue: qui si scrivono trade su carta.
    """

    def __init__(self, cfg: Config, market: MarketData, features: FeatureEngine,
                 store: Store | None, model: "Model | None" = None) -> None:
        self.cfg = cfg
        self.market = market
        self.features = features
        self.store = store
        self.decisions = DecisionEngine(cfg, model=model,
                                        performance=self.agent_hit_rates)
        self.burst = BurstStrategy(cfg)
        self.active: dict[str, LiveSignal] = {}
        self.history: list[LiveSignal] = []
        self.last_decision: Decision | None = None
        self.last_signal_ts = 0
        self.started_at = now_ms()
        self.counters = {"decisions": 0, "signals": 0, "no_trade": 0,
                         "triggered": 0, "expired": 0, "cancelled": 0,
                         "wins": 0, "losses": 0, "ties": 0}
        self.gate_counter: dict[str, int] = {}
        self._last_shadow_ts = 0
        self._last_no_trade_ts = 0
        self._last_no_trade_key: tuple = ()
        self.on_event: Callable[[str, LiveSignal], None] | None = None

    @property
    def strategy(self) -> str:
        return self.cfg.strategy

    # ------------------------------------------------------------- decisione
    def evaluate(self, vector: dict) -> Decision:
        market = self.market.snapshot()
        health = self.market.health()
        engine = self.burst if self.strategy == "burst15" else self.decisions
        decision = engine.decide(vector, market, health)
        self.counters["decisions"] += 1
        self.last_decision = decision
        for reason in decision.no_trade_reasons:
            key = gate_key(reason)
            self.gate_counter[key] = self.gate_counter.get(key, 0) + 1

        if not decision.is_trade:
            self.counters["no_trade"] += 1
            self._record_no_trade(decision)
            self._shadow(decision, vector, False, decision.no_trade_reasons)
            return decision

        if len(self.active) >= self.cfg.max_concurrent:
            self._bump_gate("massimo di segnali contemporanei")
            self._shadow(decision, vector, False, ["max concurrent"])
            return decision
        # BURST-15 ha il proprio cooldown di sessione: sovrapporgli quello
        # dell'ensemble modificherebbe in silenzio la regola della strategia.
        if self.strategy != "burst15" and (
            decision.ts - self.last_signal_ts < self.cfg.cooldown_ms
        ):
            self._bump_gate("cooldown")
            self._shadow(decision, vector, False, ["cooldown"])
            return decision

        self._shadow(decision, vector, True, [])
        self._create(decision, vector)
        return decision

    def _bump_gate(self, key: str) -> None:
        self.gate_counter[key] = self.gate_counter.get(key, 0) + 1

    def _create(self, decision: Decision, vector: dict) -> LiveSignal:
        sid = uuid.uuid4().hex[:16]
        ts = now_ms()
        sig = LiveSignal(
            signal_id=sid, symbol=decision.symbol, exchange=decision.exchange,
            strategy=decision.strategy, direction=decision.direction,
            status=WAITING, reference_price=decision.reference_price,
            trigger_price=float(decision.trigger_price or 0.0),
            horizon_s=decision.horizon_s, confidence=decision.confidence,
            prob_up=decision.prob_up, prob_down=decision.prob_down,
            edge=decision.edge, regime=decision.regime, created_at=ts,
            expires_wait_at=(
                ts + decision.entry_delay_ms if decision.entry_mode == "DELAY"
                else ts + int(self.cfg.wait_timeout_s * 1000)
            ),
            entry_mode=decision.entry_mode, entry_delay_ms=decision.entry_delay_ms,
            data_quality=decision.data_quality, model_id=decision.model_id,
            is_synthetic=self.market.is_synthetic,
            features=vector.get("features", {}),
        )
        self.active[sid] = sig
        self.last_signal_ts = decision.ts
        self.counters["signals"] += 1
        if self.strategy == "burst15":
            self.burst.on_entry(sid, decision.ts)
        self._record_signal_row(decision, sid)
        self._write_paper(sig)
        self._emit("signal_created", sig)
        return sig

    # -------------------------------------------------------------- monitor
    def tick_check(self) -> None:
        """Controllo di trigger e scadenza, indipendente da qualsiasi client."""
        if not self.active:
            return
        price = self._price()
        ts = now_ms()
        for sid in list(self.active):
            sig = self.active.get(sid)
            if sig is None:
                continue
            if sig.status == WAITING:
                if sig.entry_mode == "DELAY":
                    # Ingresso a tempo: decide l'orologio, non un livello.
                    if ts >= sig.created_at + sig.entry_delay_ms:
                        if price is None:
                            self._cancel(sig, ts, "nessun prezzo all'ora di ingresso")
                        else:
                            self._trigger(sig, price, ts)
                elif price is not None and self._touched(sig, price):
                    self._trigger(sig, price, ts)
                elif ts >= sig.expires_wait_at:
                    self._cancel(sig, ts, "trigger non raggiunto nella finestra")
            elif sig.status in (TRIGGERED, ACTIVE) and sig.expires_at:
                if ts >= sig.expires_at:
                    self._expire(sig, price, ts)

    def _price(self) -> float | None:
        tick = self.market.last_tick
        if tick is None:
            return None
        src = self.cfg.trigger_price_source
        if src == "last":
            return tick.last_price or tick.mid
        if src == "micro":
            return tick.micro_price
        return tick.mid

    @staticmethod
    def _touched(sig: LiveSignal, price: float) -> bool:
        return price >= sig.trigger_price if sig.direction == UP else price <= sig.trigger_price

    def _trigger(self, sig: LiveSignal, price: float, ts: int) -> None:
        sig.status = TRIGGERED
        sig.triggered_at = ts
        sig.expires_at = ts + int(sig.horizon_s * 1000)
        # L'ingresso e' il prezzo davvero osservato al tocco: puo' superare il
        # trigger su un movimento veloce, e registrarlo cosi' e' onesto.
        sig.entry_price = price
        self.counters["triggered"] += 1
        self._emit("trigger_hit", sig)
        sig.status = ACTIVE
        self._emit("trade_active", sig)
        self._write_paper(sig)

    def _expire(self, sig: LiveSignal, price: float | None, ts: int) -> None:
        sig.status = EXPIRED
        sig.expiry_price = price
        sig.settled_at = ts
        self.counters["expired"] += 1
        entry = sig.entry_price
        if price is None or entry is None:
            sig.result = CANCELLED
            sig.status = CANCELLED
            self.counters["cancelled"] += 1
        else:
            if price == entry:
                sig.result = sig.status = TIE
                self.counters["ties"] += 1
            elif (price > entry) == (sig.direction == UP):
                sig.result = sig.status = WIN
                self.counters["wins"] += 1
            else:
                sig.result = sig.status = LOSS
                self.counters["losses"] += 1
            sig.pnl_units = self._pnl(sig)
        self._finish(sig)

    def _cancel(self, sig: LiveSignal, ts: int, reason: str) -> None:
        sig.status = sig.result = CANCELLED
        sig.settled_at = ts
        self.counters["cancelled"] += 1
        self._emit("signal_cancelled", sig)
        self._finish(sig)

    def _finish(self, sig: LiveSignal) -> None:
        self.active.pop(sig.signal_id, None)
        if sig.strategy == "burst15":
            self.burst.on_settled(sig.signal_id, sig.result, sig.settled_at)
        self.history.append(sig)
        self.history = self.history[-1000:]
        self._emit("signal_settled", sig)
        self._write_paper(sig)

    def _pnl(self, sig: LiveSignal) -> float | None:
        """P&L in unita' di puntata. None quando il payout non e' noto.

        Il payout e' una proprieta' del broker, non del mercato: senza, un P&L
        monetario non e' definito e qui si dichiara invece di inventarlo.
        """
        payout = self.cfg.payout
        if payout is None or sig.result is None:
            return None
        if sig.result == WIN:
            return self.cfg.stake * payout
        if sig.result == LOSS:
            return -self.cfg.stake
        return 0.0

    def _emit(self, event: str, sig: LiveSignal) -> None:
        if self.on_event:
            try:
                self.on_event(event, sig)
            except Exception:  # noqa: BLE001 - l'interfaccia non ferma il motore
                pass

    # ---------------------------------------------------------- persistenza
    def _record_no_trade(self, decision: Decision) -> None:
        """Battito per le finestre NO TRADE, non una riga ciascuna.

        A 10 Hz una riga per finestra sono ~864k righe al giorno che dicono
        "non e' successo niente": e' cio' che intasava lo scrittore e rendeva
        introvabili i segnali veri. Il registro per l'apprendimento e'
        shadow_decisions, che ha una sua cadenza.
        """
        key = tuple(sorted({gate_key(r) for r in decision.no_trade_reasons}))
        if key == self._last_no_trade_key and (
            decision.ts - self._last_no_trade_ts < self.cfg.no_trade_row_interval_ms
        ):
            return
        self._last_no_trade_key = key
        self._last_no_trade_ts = decision.ts
        self._record_signal_row(decision, None)

    def _record_signal_row(self, decision: Decision, sid: str | None) -> None:
        if not self.store:
            return
        self.store.add("signals", {
            "signal_id": sid or f"nt-{uuid.uuid4().hex[:12]}", "ts": decision.ts,
            "symbol": decision.symbol, "direction": decision.direction,
            "status": WAITING if sid else CANCELLED, "strategy": decision.strategy,
            "reference_price": decision.reference_price,
            "trigger_price": decision.trigger_price, "horizon_s": decision.horizon_s,
            "confidence": decision.confidence, "prob_up": decision.prob_up,
            "prob_down": decision.prob_down, "edge": decision.edge,
            "regime": decision.regime,
            "no_trade_reasons": json.dumps(decision.no_trade_reasons),
            "detail": json.dumps(decision.detail, default=str),
            "model_id": decision.model_id, "data_quality": decision.data_quality,
            "is_synthetic": int(self.market.is_synthetic),
        })

    def _shadow(self, decision: Decision, vector: dict, emitted: bool,
                blocked: list[str]) -> None:
        """Registra la pendenza su OGNI finestra, emessa o no.

        Addestrare solo sui segnali emessi insegna il filtro del motore, non il
        mercato. Queste righe non contengono l'esito: viene risolto dopo, dal
        prezzo a ts + orizzonte, quindi non possono contrabbandare informazione
        che al momento non c'era.
        """
        if not self.store:
            return
        if decision.ts - self._last_shadow_ts < self.cfg.shadow_interval_ms:
            return
        self._last_shadow_ts = decision.ts
        self.store.add("shadow_decisions", {
            "ts": decision.ts, "symbol": decision.symbol, "lean": decision.lean,
            "prob_up": decision.prob_up, "confidence": decision.lean_confidence,
            "edge": decision.edge, "horizon_s": decision.horizon_s,
            "reference_price": decision.reference_price, "regime": decision.regime,
            "emitted": int(emitted), "blocked_by": json.dumps(blocked),
            "data_quality": decision.data_quality, "model_id": decision.model_id,
            "is_synthetic": int(vector.get("is_synthetic", False)),
        })

    def _write_paper(self, sig: LiveSignal) -> None:
        if not self.store:
            return
        self.store.upsert_paper_trade({
            "signal_id": sig.signal_id, "ts": sig.created_at, "symbol": sig.symbol,
            "strategy": sig.strategy, "direction": sig.direction,
            "status": sig.status, "entry_mode": sig.entry_mode,
            "reference_price": sig.reference_price,
            "trigger_price": sig.trigger_price, "entry_price": sig.entry_price,
            "expiry_price": sig.expiry_price, "confidence": sig.confidence,
            "edge": sig.edge, "regime": sig.regime, "horizon_s": sig.horizon_s,
            "triggered_at": sig.triggered_at, "expires_at": sig.expires_at,
            "settled_at": sig.settled_at, "result": sig.result,
            "pnl_units": sig.pnl_units, "payout": self.cfg.payout,
            "stake": self.cfg.stake, "data_quality": sig.data_quality,
            "features": json.dumps(_finite(sig.features), default=str),
            "is_synthetic": int(sig.is_synthetic),
        })

    # ------------------------------------------------------------------ viste
    def agent_hit_rates(self) -> dict[str, float]:
        """Tasso di successo per agente sui segnali chiusi di recente.

        Rientra nei pesi dell'ensemble. Contano solo WIN/LOSS e un agente che
        si e' astenuto non viene giudicato.
        """
        settled = [s for s in self.history[-300:] if s.result in (WIN, LOSS)]
        if len(settled) < 20:
            return {}
        tally: dict[str, list[int]] = {}
        for sig in settled:
            actual_up = (
                sig.expiry_price is not None and sig.entry_price is not None
                and sig.expiry_price > sig.entry_price
            )
            for name, out in (sig.features.get("_agents") or {}).items():
                if out not in (UP, DOWN):
                    continue
                tally.setdefault(name, []).append(int((out == UP) == actual_up))
        return {k: sum(v) / len(v) for k, v in tally.items() if len(v) >= 10}

    def snapshot(self) -> dict[str, Any]:
        ts = now_ms()
        return {
            "server_ts": ts, "strategy": self.strategy,
            "active": [s.to_dict(ts) for s in self.active.values()],
            "last_settled": [s.to_dict(ts) for s in self.history[-20:]][::-1],
            "counters": dict(self.counters),
            "last_decision": self.last_decision.to_dict() if self.last_decision else None,
            "burst_session": (
                self.burst.session.to_dict(ts)
                if self.strategy == "burst15" and self.burst.session else None
            ),
            "is_synthetic": self.market.is_synthetic,
        }

    def current(self) -> dict[str, Any] | None:
        ts = now_ms()
        if self.active:
            return next(iter(self.active.values())).to_dict(ts)
        return self.history[-1].to_dict(ts) if self.history else None

    def diagnostics(self, top: int = 12) -> dict[str, Any]:
        """Perche' i segnali arrivano, o non arrivano, in un oggetto solo.

        I cancelli sono ordinati per quante volte hanno bloccato: il vincolo
        che stringe e' la prima riga, non qualcosa da indovinare guardando
        l'ultima finestra da 100 ms.
        """
        ts = now_ms()
        decisions = max(self.counters["decisions"], 1)
        ranked = sorted(self.gate_counter.items(), key=lambda kv: -kv[1])[:top]
        gates = [{"gate": k, "count": v, "share_of_decisions": round(v / decisions, 4)}
                 for k, v in ranked]
        uptime = max((ts - self.started_at) / 1000.0, 1e-9)
        emitted = self.counters["signals"]
        quality = self.market.data_quality()

        if self.market.last_tick is None:
            verdict = ("NESSUN DATO DI MERCATO. Il motore non ha mai ricevuto un "
                       "tick, quindi ogni finestra e' correttamente NO TRADE. "
                       "Guarda il feed e gli errori dell'adapter, non le soglie.")
        elif self.market.feed.supports_depth and not self.market.book.synced:
            verdict = (f"BOOK NON SINCRONIZZATO ({self.market.book.desync_reason}). "
                       "Ogni finestra resta NO TRADE finche' lo snapshot REST non "
                       "riesce: verifica che l'endpoint REST sia raggiungibile "
                       "(--proxy se non lo e').")
        elif not quality.get("warmup_complete"):
            verdict = (f"RISCALDAMENTO ({self.market.uptime_s:.0f}s di "
                       f"{self.cfg.min_warmup_s:.0f}s). Nessun segnale viene "
                       "emesso durante il riscaldamento, per scelta.")
        elif emitted == 0 and gates:
            verdict = (f"Ancora nessun segnale. Il cancello che scatta di piu' e' "
                       f"'{gates[0]['gate']}' ({gates[0]['share_of_decisions']:.0%} "
                       "delle finestre). Guarda `shadow` prima di allentarlo: un "
                       "cancello che scarta finestre perdenti sta facendo il suo "
                       "lavoro.")
        else:
            verdict = (f"{emitted} segnali emessi "
                       f"({emitted / uptime * 3600.0:.1f}/ora).")

        return {
            "server_ts": ts, "strategy": self.strategy, "verdict": verdict,
            "uptime_s": round(uptime, 1),
            "decisions_evaluated": self.counters["decisions"],
            "signals_emitted": emitted,
            "signals_per_hour": round(emitted / uptime * 3600.0, 2),
            "emission_rate": round(emitted / decisions, 5),
            "blocking_gates": gates,
            "binding_gate": gates[0]["gate"] if gates else None,
            "last_decision_reasons": (
                self.last_decision.no_trade_reasons if self.last_decision else []
            ),
            "thresholds": {
                "min_agreement": self.cfg.min_agreement,
                "min_confidence": self.cfg.min_confidence,
                "min_edge": self.cfg.min_edge,
                "effective_min_confidence": round(self.cfg.effective_min_confidence, 4),
                "min_data_quality": self.cfg.min_data_quality,
                "max_spread_bps": self.cfg.max_spread_bps,
                "min_expected_move_ticks": self.cfg.min_expected_move_ticks,
                "max_zero_move_fraction": self.cfg.max_zero_move_fraction,
                "anomaly_max_severity": self.cfg.anomaly_max_severity,
            },
            "feed": {
                "source": self.market.feed.name,
                "is_synthetic": self.market.is_synthetic,
                "feed_age_ms": self.market.feed_age_ms,
                "book_synced": self.market.book.synced,
                "book_desync_reason": self.market.book.desync_reason,
                "adapter": self.market.feed.state(),
                "data_quality": quality,
            },
            "burst": self.burst.status() if self.strategy == "burst15" else None,
            "note": ("Un cancello che scatta spesso non e' automaticamente "
                     "sbagliato. `shadow` misura se le finestre che ha scartato "
                     "avrebbero vinto: e' l'unica prova che dice se ti protegge "
                     "o ti costa."),
        }


def _finite(value: Any) -> Any:
    """Toglie NaN/Inf perche' la serializzazione JSON non possa fallire."""
    if isinstance(value, dict):
        return {k: _finite(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite(v) for v in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


# --------------------------------------------------------------------------- #
#  STATISTICHE
# --------------------------------------------------------------------------- #


def summarise(trades: list[dict], payout: float | None, stake: float = 1.0,
              no_trade_count: int | None = None) -> dict[str, Any]:
    """Riassunto onesto del paper trading.

    Senza payout il P&L monetario non e' definito: si dichiara PAYOUT
    SCONOSCIUTO invece di stamparne uno inventato.
    """
    settled = [t for t in trades if t.get("result") in (WIN, LOSS, TIE)]
    wins = sum(1 for t in settled if t["result"] == WIN)
    losses = sum(1 for t in settled if t["result"] == LOSS)
    ties = sum(1 for t in settled if t["result"] == TIE)
    decided = wins + losses
    cancelled = sum(1 for t in trades if t.get("result") == CANCELLED)

    out: dict[str, Any] = {
        "signals": len(trades), "settled": len(settled), "cancelled": cancelled,
        "wins": wins, "losses": losses, "ties": ties, "decided": decided,
        "tie_fraction": round(ties / len(settled), 4) if settled else None,
        "win_rate_decided": round(wins / decided, 4) if decided else None,
        "no_trade_windows": no_trade_count,
        "payout_status": "NOTO" if payout is not None else "PAYOUT SCONOSCIUTO",
    }
    if decided:
        lo, hi = wilson_interval(wins, decided)
        out["win_rate_ci95"] = [round(lo, 4), round(hi, 4)]
        out["p_value_vs_coinflip"] = round(binomial_p_value(wins, decided), 6)
        out["beats_chance"] = bool(lo > 0.5)
    if payout is not None:
        pnl = wins * stake * payout - losses * stake
        out["pnl_units"] = round(pnl, 4)
        out["ev_per_trade_units"] = round(pnl / len(settled), 5) if settled else None
        out["breakeven_win_rate"] = round(breakeven_win_rate(payout), 4)
        if decided:
            out["above_breakeven"] = bool(
                wins / decided > breakeven_win_rate(payout)
            )
        equity = peak = dd = 0.0
        for t in sorted(settled, key=lambda r: r.get("settled_at") or r["ts"]):
            equity += (stake * payout if t["result"] == WIN
                       else -stake if t["result"] == LOSS else 0.0)
            peak = max(peak, equity)
            dd = min(dd, equity - peak)
        out["max_drawdown_units"] = round(dd, 4)
    else:
        out["note"] = ("Payout non impostato: il P&L monetario non e' definito. "
                       "Passalo con --payout 0.8 oppure PAYOUT=0.8. "
                       "Pareggio = 1 / (1 + payout).")
    return out


def calibration_report(trades: list[dict], buckets: int = 5) -> dict[str, Any]:
    """Confronta la confidenza dichiarata con la frequenza realizzata.

    E' l'unica cosa che trasforma un numero di confidenza in una probabilita'.
    """
    settled = [t for t in trades if t.get("result") in (WIN, LOSS)]
    if not settled:
        return {"status": "NESSUN DATO", "buckets": []}
    edges = [0.5 + i * 0.5 / buckets for i in range(buckets + 1)]
    out = []
    for lo, hi in zip(edges, edges[1:]):
        sel = [t for t in settled if lo <= (t.get("confidence") or 0) < hi + 1e-9]
        if not sel:
            continue
        wins = sum(1 for t in sel if t["result"] == WIN)
        w_lo, w_hi = wilson_interval(wins, len(sel))
        out.append({
            "bucket": f"{lo:.2f}-{hi:.2f}", "n": len(sel),
            "stated_confidence": round(sum(t["confidence"] for t in sel) / len(sel), 4),
            "realised_win_rate": round(wins / len(sel), 4),
            "ci95": [round(w_lo, 4), round(w_hi, 4)],
        })
    return {
        "status": "OK", "buckets": out,
        "note": ("Se la frequenza realizzata sta sistematicamente sotto la "
                 "confidenza dichiarata, il motore e' sovrasicuro e i suoi "
                 "numeri non sono probabilita'."),
    }


def monte_carlo(results: list[str], payout: float | None, stake: float = 1.0,
                simulations: int = 5000, bankroll: float = 20.0,
                seed: int = 7) -> dict[str, Any]:
    """Riordina gli esiti osservati per vedere quanto conta l'ordine.

    Non e' una previsione: e' la stessa sequenza vissuta in ordini diversi.
    """
    decided = [r for r in results if r in (WIN, LOSS)]
    if not decided or payout is None:
        return {"status": "NON CALCOLABILE",
                "reason": "servono esiti decisi e un payout noto"}
    rng = random.Random(seed)
    finals, ruins, drawdowns = [], 0, []
    for _ in range(simulations):
        seq = decided[:]
        rng.shuffle(seq)
        equity = bankroll
        peak = equity
        worst = 0.0
        ruined = False
        for r in seq:
            equity += stake * payout if r == WIN else -stake
            peak = max(peak, equity)
            worst = min(worst, equity - peak)
            if equity <= 0:
                ruined = True
                break
        finals.append(equity)
        drawdowns.append(worst)
        ruins += int(ruined)
    finals.sort()
    return {
        "status": "OK", "simulations": simulations, "trades_per_run": len(decided),
        "starting_bankroll": bankroll,
        "final_median": round(percentile(finals, 0.5) or 0.0, 3),
        "final_p05": round(percentile(finals, 0.05) or 0.0, 3),
        "final_p95": round(percentile(finals, 0.95) or 0.0, 3),
        "probability_of_ruin": round(ruins / simulations, 4),
        "median_max_drawdown": round(percentile(sorted(drawdowns), 0.5) or 0.0, 3),
    }


# --------------------------------------------------------------------------- #
#  APPRENDIMENTO: dataset causale, walk-forward, modello logistico
# --------------------------------------------------------------------------- #

#: Colonne che sono metadati, non predittori. Il prezzo assoluto in particolare
#: NON deve entrare: un modello che vede "mid" impara il livello di quella
#: settimana, non il mercato.
NON_FEATURES = {
    "mid", "micro_price", "book_synced", "data_quality", "history_span_ms",
    "tick_count", "bb_mid", "bb_upper", "bb_lower", "vwap_60s", "ema_9",
    "ema_21", "large_trade_threshold_notional", "l1_dust", "spread",
    "bid_qty_l1", "ask_qty_l1", "depth_notional_bid_20", "depth_notional_ask_20",
    "bid_wall_size", "ask_wall_size", "atr_14",
}


@dataclass
class Dataset:
    rows: list[list[float]]
    y: list[int]
    ts: list[int]
    entry: list[float]
    exit: list[float]
    names: list[str]
    horizon_s: float
    meta: dict[str, Any] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.y)

    def describe(self) -> dict[str, Any]:
        return {
            "rows": len(self), "features": len(self.names),
            "horizon_s": self.horizon_s,
            "class_balance_up": round(sum(self.y) / len(self), 4) if self.y else None,
            "duration_minutes": (
                round((self.ts[-1] - self.ts[0]) / 60000, 2) if len(self) > 1 else 0
            ),
            **self.meta,
        }


def build_dataset(store: Store, cfg: Config, horizon_s: float,
                  include_synthetic: bool = False,
                  max_rows: int | None = None,
                  tolerance_ms: int = 750) -> Dataset:
    """Attacca le etichette a righe di feature causali.

    La proprieta' che conta piu' di tutte: le feature vengono STRETTAMENTE dal
    passato e l'etichetta STRETTAMENTE dal futuro, con il confine sul timestamp
    della riga. Se il tick a ts + orizzonte non e' stato registrato, la riga si
    scarta: un'etichetta riempita in avanti e' un'etichetta inventata.
    """
    conn = store.reader()
    try:
        where = "" if include_synthetic else " WHERE is_synthetic = 0"
        frows = conn.execute(
            f"SELECT ts, mid, payload FROM features{where} ORDER BY ts ASC"
        ).fetchall()
        trows = conn.execute(
            f"SELECT ts, mid FROM market_ticks{where} ORDER BY ts ASC"
        ).fetchall()
    finally:
        conn.close()

    if not frows or not trows:
        return Dataset([], [], [], [], [], [], horizon_s, {"error": "nessun dato"})

    tick_ts = [r["ts"] for r in trows]
    tick_mid = [r["mid"] for r in trows]
    horizon_ms = int(horizon_s * 1000)

    parsed: list[tuple[int, float, dict]] = []
    for r in frows:
        try:
            payload = json.loads(r["payload"]) if r["payload"] else {}
        except ValueError:
            continue
        parsed.append((r["ts"], r["mid"], payload))

    if max_rows and len(parsed) > max_rows:
        # Sottocampionamento uniforme: tenere solo la coda darebbe un modello
        # addestrato su un'ora sola di mercato.
        step = len(parsed) / max_rows
        parsed = [parsed[int(i * step)] for i in range(max_rows)]

    # Le colonne sono l'unione delle chiavi numeriche viste, ordinate: cosi'
    # l'ordine dei pesi e' stabile fra addestramento e inferenza.
    names: list[str] = sorted({
        k for _, _, payload in parsed[:2000]
        for k, v in payload.items()
        if k not in NON_FEATURES and isinstance(v, (int, float))
        and not isinstance(v, bool)
    })

    rows: list[list[float]] = []
    ys: list[int] = []
    tss: list[int] = []
    entries: list[float] = []
    exits: list[float] = []
    dropped_no_future = ties = 0

    for ts, mid, payload in parsed:
        target = ts + horizon_ms
        idx = bisect.bisect_left(tick_ts, target)
        if idx >= len(tick_ts) or tick_ts[idx] - target > tolerance_ms:
            dropped_no_future += 1
            continue
        exit_price = tick_mid[idx]
        if mid is None or mid <= 0:
            continue
        if exit_price == mid:
            ties += 1
            continue  # il pareggio non e' ne' UP ne' DOWN
        row = []
        for name in names:
            v = payload.get(name)
            row.append(float(v) if isinstance(v, (int, float))
                       and not isinstance(v, bool)
                       and math.isfinite(float(v)) else 0.0)
        rows.append(row)
        ys.append(1 if exit_price > mid else 0)
        tss.append(ts)
        entries.append(mid)
        exits.append(exit_price)

    # Le colonne costanti non portano informazione e destabilizzano il modello.
    keep = [
        i for i in range(len(names))
        if len({r[i] for r in rows[:5000]}) > 1
    ] if rows else []
    if keep and len(keep) < len(names):
        names = [names[i] for i in keep]
        rows = [[r[i] for i in keep] for r in rows]

    return Dataset(
        rows, ys, tss, entries, exits, names, horizon_s,
        {"dropped_no_future_tick": dropped_no_future, "ties_dropped": ties,
         "rows_scanned": len(parsed)},
    )


@dataclass
class Model:
    """Regressione logistica con guardia fuori distribuzione.

    Un modello puo' parlare solo di stati di mercato che assomigliano a quelli
    su cui e' stato stimato: quando il vettore corrente e' molto fuori,
    `predict` dichiara out_of_distribution e il motore lo traduce in NO TRADE -
    che e' la risposta giusta, non un limite.
    """

    names: list[str]
    mean: list[float]
    std: list[float]
    weights: list[float]
    bias: float
    horizon_s: float
    calibrated: bool = False
    platt: tuple[float, float] | None = None   # (a, b) su log-odds
    metadata: dict[str, Any] = field(default_factory=dict)
    model_id: str = ""
    ood_z: float = 6.0
    ood_max_features: int = 3

    @property
    def ready(self) -> bool:
        return bool(self.names and self.weights)

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)

    @classmethod
    def from_json(cls, raw: str | dict) -> "Model":
        data = json.loads(raw) if isinstance(raw, str) else raw
        platt = data.get("platt")
        data["platt"] = tuple(platt) if platt else None
        return cls(**data)

    def _logit(self, row: list[float]) -> float:
        z = self.bias
        for i, w in enumerate(self.weights):
            sd = self.std[i] or 1.0
            z += w * ((row[i] - self.mean[i]) / sd)
        return z

    def predict_row(self, row: list[float]) -> float:
        z = self._logit(row)
        if self.platt:
            a, b = self.platt
            z = a * z + b
        return 1.0 / (1.0 + math.exp(-max(-40.0, min(40.0, z))))

    def predict(self, features: dict) -> dict[str, Any] | None:
        if not self.ready:
            return None
        row: list[float] = []
        missing = 0
        offenders: list[str] = []
        for i, name in enumerate(self.names):
            v = features.get(name)
            if v is None or not isinstance(v, (int, float)) or isinstance(v, bool) \
                    or not math.isfinite(float(v)):
                missing += 1
                row.append(self.mean[i])
                continue
            v = float(v)
            row.append(v)
            sd = self.std[i] or 0.0
            if sd > 0 and abs((v - self.mean[i]) / sd) > self.ood_z:
                offenders.append(name)
        # Le soglie scalano col numero di colonne: con poche feature una soglia
        # fissa di 3 "colpevoli" non potrebbe mai scattare, e la guardia
        # sarebbe decorativa.
        max_missing = max(1, len(self.names) // 10)
        max_offenders = min(self.ood_max_features, max(1, len(self.names) // 3))
        if missing > max_missing:
            return {"out_of_distribution": True, "model_id": self.model_id,
                    "reason": f"{missing} feature non disponibili"}
        if len(offenders) > max_offenders:
            return {"out_of_distribution": True, "model_id": self.model_id,
                    "reason": "fuori distribuzione: " + ", ".join(offenders[:5])}
        return {"prob_up": self.predict_row(row), "model_id": self.model_id,
                "calibrated": self.calibrated, "out_of_distribution": False}


def _standardise(rows: list[list[float]]) -> tuple[list[float], list[float]]:
    n = len(rows)
    cols = len(rows[0])
    mean = [0.0] * cols
    for row in rows:
        for i, v in enumerate(row):
            mean[i] += v
    mean = [m / n for m in mean]
    var = [0.0] * cols
    for row in rows:
        for i, v in enumerate(row):
            var[i] += (v - mean[i]) ** 2
    std = [math.sqrt(v / max(n - 1, 1)) or 1.0 for v in var]
    return mean, [s if s > 1e-12 else 1.0 for s in std]


def fit_logistic(rows: list[list[float]], y: list[int], cfg: Config,
                 seed: int = 7) -> tuple[list[float], float, list[float], list[float]]:
    """SGD a minibatch con L2 e riequilibrio delle classi.

    Deliberatamente poco profonda: a cinque secondi il rapporto segnale/rumore
    e' brutale, e un modello complesso impara la sessione, non il mercato.
    """
    mean, std = _standardise(rows)
    cols = len(rows[0])
    w = [0.0] * cols
    b = 0.0
    ups = sum(y)
    downs = len(y) - ups
    w_up = (len(y) / (2 * ups)) if ups else 1.0
    w_down = (len(y) / (2 * downs)) if downs else 1.0

    idx = list(range(len(rows)))
    rng = random.Random(seed)
    lr = cfg.ml_learning_rate
    batch = 64
    for epoch in range(cfg.ml_epochs):
        rng.shuffle(idx)
        step = lr / (1.0 + 0.5 * epoch)
        for start in range(0, len(idx), batch):
            chunk = idx[start:start + batch]
            gw = [0.0] * cols
            gb = 0.0
            for j in chunk:
                row = rows[j]
                z = b
                for i in range(cols):
                    z += w[i] * ((row[i] - mean[i]) / std[i])
                p = 1.0 / (1.0 + math.exp(-max(-40.0, min(40.0, z))))
                weight = w_up if y[j] == 1 else w_down
                err = (p - y[j]) * weight
                for i in range(cols):
                    gw[i] += err * ((row[i] - mean[i]) / std[i])
                gb += err
            m = len(chunk)
            for i in range(cols):
                w[i] -= step * (gw[i] / m + cfg.ml_l2 * w[i])
            b -= step * (gb / m)
    return w, b, mean, std


def _fit_platt(scores: list[float], y: list[int]) -> tuple[float, float]:
    """Calibrazione di Platt: una logistica a una dimensione sul log-odds.

    Serve per trasformare un punteggio in una probabilita' confrontabile con la
    frequenza realizzata. Va stimata su righe che il modello NON ha visto, o
    impara i propri residui.
    """
    a, b = 1.0, 0.0
    for epoch in range(200):
        step = 0.05 / (1.0 + 0.05 * epoch)
        ga = gb = 0.0
        for s, target in zip(scores, y):
            z = max(-40.0, min(40.0, a * s + b))
            p = 1.0 / (1.0 + math.exp(-z))
            err = p - target
            ga += err * s
            gb += err
        n = max(len(scores), 1)
        a -= step * ga / n
        b -= step * gb / n
    return a, b


@dataclass
class Fold:
    train: tuple[int, int]
    test: tuple[int, int]
    purge_gap_ms: int


def walk_forward_folds(ts: list[int], n_splits: int, horizon_s: float,
                       embargo_s: float) -> list[Fold]:
    """Fold espansivi con un intervallo di purga fra train e test.

    Senza la purga il fold di test vede il futuro del fold di addestramento:
    l'etichetta a t dipende dal prezzo a t + orizzonte, quindi le righe a
    cavallo del confine sono la stessa informazione da due lati.
    """
    n = len(ts)
    if n < (n_splits + 1) * 50:
        return []
    gap_ms = int((horizon_s + embargo_s) * 1000)
    block = n // (n_splits + 1)
    folds: list[Fold] = []
    for k in range(1, n_splits + 1):
        train_end = block * k
        test_end = min(n, block * (k + 1))
        cut_ts = ts[train_end - 1] + gap_ms
        test_start = bisect.bisect_left(ts, cut_ts, lo=train_end, hi=test_end)
        if test_end - test_start < 30 or train_end < 100:
            continue
        folds.append(Fold((0, train_end), (test_start, test_end), gap_ms))
    return folds


def run_walk_forward(ds: Dataset, cfg: Config, n_splits: int) -> dict[str, Any]:
    folds = walk_forward_folds(ds.ts, n_splits, ds.horizon_s, cfg.ml_embargo_s)
    if not folds:
        return {"error": "dati insufficienti per fold fuori campione"}
    fold_reports = []
    all_correct = all_n = 0
    for i, fold in enumerate(folds):
        tr0, tr1 = fold.train
        te0, te1 = fold.test
        w, b, mean, std = fit_logistic(ds.rows[tr0:tr1], ds.y[tr0:tr1], cfg,
                                       seed=7 + i)
        model = Model(ds.names, mean, std, w, b, ds.horizon_s)
        correct = 0
        for row, truth in zip(ds.rows[te0:te1], ds.y[te0:te1]):
            pred = 1 if model.predict_row(row) >= 0.5 else 0
            correct += int(pred == truth)
        n = te1 - te0
        all_correct += correct
        all_n += n
        fold_reports.append({
            "fold": i, "train_rows": tr1 - tr0, "test_rows": n,
            "accuracy": round(correct / n, 4) if n else None,
            "purge_gap_ms": fold.purge_gap_ms,
        })
    lo, hi = wilson_interval(all_correct, all_n)
    return {
        "folds": fold_reports,
        "out_of_sample": {
            "n": all_n, "correct": all_correct,
            "accuracy": round(all_correct / all_n, 5) if all_n else None,
            "accuracy_ci95": [round(lo, 5), round(hi, 5)],
            "p_value_vs_coinflip": round(binomial_p_value(all_correct, all_n), 6),
        },
        "fold_consistency": round(
            sum(1 for f in fold_reports if (f["accuracy"] or 0) > 0.5)
            / len(fold_reports), 4
        ),
    }


EDGE_PROVEN = "EDGE DIMOSTRATO"
EDGE_PROMISING = "PROMETTENTE"
EDGE_NONE = "NESSUN EDGE ROBUSTO IDENTIFICATO"
EDGE_INCONCLUSIVE = "INCONCLUSIVO"


def classify_edge(report: dict, cfg: Config, min_rows: int) -> dict[str, Any]:
    """Verdetto sul risultato fuori campione.

    Il criterio e' il limite INFERIORE dell'intervallo, mai la stima puntuale:
    un modello il cui vantaggio potrebbe essere zero non ha un vantaggio.
    """
    oos = report.get("out_of_sample") or {}
    n = int(oos.get("n") or 0)
    acc = oos.get("accuracy")
    ci = oos.get("accuracy_ci95")
    notes: list[str] = []
    if not n or acc is None or not ci:
        return {"classification": EDGE_INCONCLUSIVE,
                "notes": ["nessun risultato fuori campione"]}
    if n < min_rows:
        notes.append(f"solo {n} righe fuori campione (ne servono {min_rows})")
        return {"classification": EDGE_INCONCLUSIVE, "notes": notes,
                "out_of_sample": oos}

    lo = ci[0]
    # `or 1.0` qui sarebbe un bug silenzioso: un p-value di 0.0 e' il risultato
    # piu' forte possibile ed e' falsy, quindi verrebbe scambiato per il piu'
    # debole e nessun modello potrebbe mai essere promosso.
    p = oos.get("p_value_vs_coinflip")
    p = 1.0 if p is None else float(p)
    consistency = report.get("fold_consistency")
    consistency = 0.0 if consistency is None else float(consistency)
    payout = cfg.payout
    threshold = breakeven_win_rate(payout) if payout is not None else 0.5
    if payout is not None:
        notes.append(
            f"con payout {payout:g} il pareggio e' a {threshold:.4f}"
        )
    else:
        notes.append("payout sconosciuto: il giudizio e' solo statistico, "
                     "non economico")

    if lo > threshold and p < 0.01 and consistency >= 0.8:
        cls = EDGE_PROVEN
        notes.append(f"limite inferiore {lo:.4f} sopra {threshold:.4f}, "
                     f"p={p:.4g}, {consistency:.0%} dei fold positivi")
    elif lo > threshold and p < 0.05:
        cls = EDGE_PROMISING
        notes.append(f"limite inferiore {lo:.4f} sopra soglia ma coerenza fra "
                     f"fold solo {consistency:.0%}")
    else:
        cls = EDGE_NONE
        notes.append(f"accuratezza {acc:.4f}, limite inferiore {lo:.4f} contro "
                     f"soglia {threshold:.4f} (p={p:.4g})")
    notes.append("Le finestre sovrapposte sono correlate: gli intervalli sono "
                 "piu' stretti del vero. Un risultato va rivalidato su dati "
                 "che questa analisi non ha mai visto.")
    return {"classification": cls, "notes": notes, "out_of_sample": oos,
            "fold_consistency": consistency}


def train_and_validate(store: Store, cfg: Config, include_synthetic: bool = False,
                       n_splits: int | None = None) -> dict[str, Any]:
    """collect -> walk-forward -> classifica -> (eventualmente) addestra."""
    n_splits = n_splits or cfg.retrain_splits
    ds = build_dataset(store, cfg, cfg.horizon_s, include_synthetic,
                       max_rows=cfg.ml_max_rows)
    report: dict[str, Any] = {
        "generated_at": now_ms(), "symbol": cfg.symbol,
        "horizon_s": cfg.horizon_s, "dataset": ds.describe(),
        "include_synthetic": include_synthetic,
        "payout": cfg.payout,
        "payout_status": "NOTO" if cfg.payout is not None else "PAYOUT SCONOSCIUTO",
    }
    if include_synthetic:
        report["warning"] = ("RIGHE SINTETICHE INCLUSE: questo descrive il "
                             "simulatore, non il mercato, e non puo' stabilire "
                             "niente.")
    if len(ds) < 300:
        report["status"] = "DATI INSUFFICIENTI"
        report["conclusion"] = (
            f"{len(ds)} righe etichettate: non bastano per testare alcunche'. "
            "Lascia registrare il motore contro il feed vero."
        )
        return report

    wf = run_walk_forward(ds, cfg, n_splits)
    if "error" in wf:
        report["status"] = "DATI INSUFFICIENTI"
        report["conclusion"] = wf["error"]
        return report
    report["walk_forward"] = wf
    report["edge"] = classify_edge(wf, cfg, min_rows=max(500, cfg.ml_min_samples // 10))
    report["status"] = "COMPLETO"
    report["conclusion"] = report["edge"]["classification"] + ". " + " ".join(
        report["edge"]["notes"]
    )
    return report


def fit_final_model(ds: Dataset, cfg: Config) -> Model:
    """Riaddestra su tutto e calibra su una coda tenuta da parte.

    La coda e' separata dall'addestramento da orizzonte + embargo, esattamente
    come i fold: calibrare sulle righe di addestramento produrrebbe un modello
    dall'aria sicura che ha imparato i propri residui.
    """
    n = len(ds)
    cut = int(n * 0.8)
    gap_ms = int((ds.horizon_s + cfg.ml_embargo_s) * 1000)
    cal_start = bisect.bisect_left(ds.ts, ds.ts[cut - 1] + gap_ms, lo=cut)
    cal_rows = n - cal_start
    can_calibrate = cal_rows >= 500 and len(set(ds.y[cal_start:])) == 2

    if can_calibrate:
        w, b, mean, std = fit_logistic(ds.rows[:cut], ds.y[:cut], cfg)
        model = Model(ds.names, mean, std, w, b, ds.horizon_s)
        scores = [model._logit(r) for r in ds.rows[cal_start:]]
        model.platt = _fit_platt(scores, ds.y[cal_start:])
        model.calibrated = True
        model.metadata = {"train_rows": cut, "calibration_rows": cal_rows,
                          "purge_gap_ms": gap_ms}
    else:
        w, b, mean, std = fit_logistic(ds.rows, ds.y, cfg)
        model = Model(ds.names, mean, std, w, b, ds.horizon_s)
        model.calibrated = False
        model.metadata = {"train_rows": n,
                          "calibration": f"saltata: {cal_rows} righe di coda"}
    model.model_id = f"logistic_h{ds.horizon_s:g}s_{now_ms()}"
    return model


class Retrainer:
    """Riaddestramento periodico su tutto cio' che e' stato registrato.

    Il motore NON impara dal singolo trade appena chiuso, e non deve:
    aggiornarsi sull'ultimo esito e' il modo in cui un sistema finisce a
    inseguire il rumore con crescente sicurezza. Imparare qui significa
    rieseguire l'intera pipeline validata, su una pianificazione.

    Un modello viene attivato SOLO se il suo edge fuori campione si classifica
    come DIMOSTRATO o PROMETTENTE. Un giro che conclude NESSUN EDGE ROBUSTO
    lascia il motore live esattamente com'era.
    """

    def __init__(self, cfg: Config, store: Store,
                 on_model: Callable[[Model], None] | None = None) -> None:
        self.cfg = cfg
        self.store = store
        self.on_model = on_model
        self.thread: threading.Thread | None = None
        self._running = False
        self.runs = 0
        self.activations = 0
        self.last_run_ts: int | None = None
        self.last_result: dict[str, Any] | None = None
        self.last_error: str | None = None

    def start(self) -> None:
        if not self.cfg.auto_retrain:
            return
        self._running = True
        self.thread = threading.Thread(target=self._loop, name="retrain", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self._running = False

    def _loop(self) -> None:
        # Mai riaddestrare subito all'avvio: non c'e' niente di nuovo da
        # imparare nei primi secondi.
        deadline = time.time() + self.cfg.retrain_initial_delay_s
        while self._running and time.time() < deadline:
            time.sleep(0.5)
        while self._running:
            try:
                self.run_once()
            except Exception as exc:  # noqa: BLE001 - il ciclo non muore mai
                self.last_error = f"{type(exc).__name__}: {exc}"
            deadline = time.time() + self.cfg.retrain_interval_s
            while self._running and time.time() < deadline:
                time.sleep(0.5)

    def run_once(self, include_synthetic: bool | None = None) -> dict[str, Any]:
        cfg = self.cfg
        include_synthetic = (
            include_synthetic if include_synthetic is not None
            else False
        )
        counts = self.store.counts()
        if counts.get("features", 0) < cfg.ml_min_samples:
            self.last_run_ts = now_ms()
            self.last_result = {
                "skipped": "dati insufficienti",
                "feature_rows": counts.get("features", 0),
                "needed": cfg.ml_min_samples,
            }
            return self.last_result

        report = train_and_validate(self.store, cfg, include_synthetic)
        self.runs += 1
        self.last_run_ts = now_ms()
        edge = (report.get("edge") or {}).get("classification")
        activated = False
        model_id = None

        if edge in (EDGE_PROVEN, EDGE_PROMISING):
            ds = build_dataset(self.store, cfg, cfg.horizon_s, include_synthetic,
                               max_rows=cfg.ml_max_rows)
            if len(ds) >= 300:
                model = fit_final_model(ds, cfg)
                model_id = model.model_id
                self.store.save_model({
                    "model_id": model.model_id, "ts": now_ms(),
                    "algorithm": "logistic", "horizon_s": cfg.horizon_s,
                    "symbol": cfg.symbol, "n_train": len(ds),
                    "feature_names": json.dumps(ds.names),
                    "metrics": json.dumps(report.get("walk_forward"), default=str),
                    "edge_classification": edge,
                    "artifact": model.to_json(), "is_active": 0,
                })
                self.store.set_active_model(model.model_id)
                if self.on_model:
                    self.on_model(model)
                activated = True
                self.activations += 1

        self.last_result = {
            "ran_at": self.last_run_ts, "edge": edge,
            "conclusion": report.get("conclusion"), "model_id": model_id,
            "activated": activated, "rows": report.get("dataset", {}).get("rows"),
            "note": ("Un modello validato ha sostituito il precedente."
                     if activated else
                     "Nessun modello attivato: il giro non ha superato la "
                     "classificazione dell'edge, il motore live resta com'era."),
        }
        return self.last_result

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.cfg.auto_retrain,
            "interval_s": self.cfg.retrain_interval_s,
            "runs": self.runs, "activations": self.activations,
            "last_run_ts": self.last_run_ts, "last_result": self.last_result,
            "last_error": self.last_error,
            "policy": ("Riaddestra su ogni finestra registrata, non solo sui "
                       "segnali emessi, e attiva solo un modello il cui edge "
                       "walk-forward risulta DIMOSTRATO o PROMETTENTE."),
        }


# --------------------------------------------------------------------------- #
#  SHADOW: le finestre che NON sono state tradate
# --------------------------------------------------------------------------- #


def shadow_report(store: Store, include_synthetic: bool = True) -> dict[str, Any]:
    """Tasso di successo della pendenza su OGNI finestra valutata.

    Giudicare il motore solo sui segnali emessi risponde alla domanda
    sbagliata: misura come e' andata nei momenti che il suo stesso filtro
    aveva gia' scelto. Qui si confrontano le finestre emesse con quelle
    bloccate, cancello per cancello.
    """
    conn = store.reader()
    try:
        where = "" if include_synthetic else " WHERE is_synthetic = 0"
        srows = conn.execute(
            f"SELECT ts, lean, confidence, horizon_s, reference_price, regime,"
            f" emitted, blocked_by FROM shadow_decisions{where} ORDER BY ts ASC"
        ).fetchall()
        trows = conn.execute(
            f"SELECT ts, mid FROM market_ticks{where} ORDER BY ts ASC"
        ).fetchall()
    finally:
        conn.close()

    if not srows or not trows:
        return {"status": "NESSUN DATO",
                "note": "nessuna finestra registrata: fai girare il motore."}

    tick_ts = [r["ts"] for r in trows]
    tick_mid = [r["mid"] for r in trows]
    scored: list[dict] = []
    for r in srows:
        if r["lean"] not in (UP, DOWN) or not r["reference_price"]:
            continue
        target = r["ts"] + int((r["horizon_s"] or 5.0) * 1000)
        idx = bisect.bisect_left(tick_ts, target)
        # L'ultimo orizzonte di dati non ha futuro registrato: si scarta, non
        # si riempie in avanti.
        if idx >= len(tick_ts) or tick_ts[idx] - target > 2000:
            continue
        entry = float(r["reference_price"])
        move = tick_mid[idx] - entry
        result = TIE if move == 0 else (
            WIN if (move > 0) == (r["lean"] == UP) else LOSS
        )
        try:
            blocked = json.loads(r["blocked_by"] or "[]")
        except ValueError:
            blocked = []
        scored.append({"result": result, "emitted": bool(r["emitted"]),
                       "blocked": blocked, "confidence": r["confidence"] or 0.0,
                       "regime": r["regime"]})

    if not scored:
        return {"status": "NESSUN DATO",
                "note": ("nessuna finestra ha ancora un futuro registrato. "
                         "L'ultimo orizzonte e' sempre irrisolvibile: e' corretto.")}

    def rate(rows: list[dict]) -> dict[str, Any]:
        if not rows:
            return {"n": 0}
        wins = sum(1 for r in rows if r["result"] == WIN)
        ties = sum(1 for r in rows if r["result"] == TIE)
        decided = len(rows) - ties
        if not decided:
            return {"n": len(rows), "ties": ties, "win_rate": None}
        lo, hi = wilson_interval(wins, decided)
        return {"n": len(rows), "decided": decided, "ties": ties,
                "win_rate": round(wins / decided, 4),
                "ci95": [round(lo, 4), round(hi, 4)]}

    emitted = [r for r in scored if r["emitted"]]
    blocked = [r for r in scored if not r["emitted"]]
    by_gate: dict[str, Any] = {}
    for gate in sorted({gate_key(g) for r in blocked for g in r["blocked"]}):
        sel = [r for r in blocked
               if any(gate_key(g) == gate for g in r["blocked"])]
        by_gate[gate] = rate(sel)

    out = {
        "status": "OK", "windows": rate(scored), "emitted": rate(emitted),
        "blocked": rate(blocked), "by_block_reason": by_gate,
        "note": ("Il tasso qui e' l'accuratezza della PENDENZA, non un "
                 "risultato negoziabile: nessun trigger doveva essere "
                 "raggiunto e nessun payout e' applicato. Confronta `emitted` "
                 "con `blocked` per giudicare i cancelli."),
    }
    e, b = out["emitted"], out["blocked"]
    if e.get("win_rate") is not None and b.get("win_rate") is not None:
        delta = e["win_rate"] - b["win_rate"]
        out["gate_value"] = {
            "emitted_minus_blocked": round(delta, 4),
            "verdict": ("i cancelli selezionano finestre migliori" if delta > 0.02
                        else "i cancelli selezionano finestre peggiori" if delta < -0.02
                        else "i cancelli non fanno differenza misurabile"),
            "caveat": ("Le finestre sovrapposte non sono indipendenti: un delta "
                       "piccolo va letto come nessun delta."),
        }
    return out


# --------------------------------------------------------------------------- #
#  REPLAY DI BURST-15 SU DATI REGISTRATI
# --------------------------------------------------------------------------- #


def burst_replay(store: Store, cfg: Config, include_synthetic: bool = True,
                 payout: float | None = None) -> dict[str, Any]:
    """Rigioca la strategia intera - finestre, cooldown, stop-loss - sui dati
    registrati. Ingressi e scadenze sono risolti dalla serie dei tick: una riga
    il cui futuro non e' registrato viene scartata, mai riempita in avanti."""
    conn = store.reader()
    try:
        where = "" if include_synthetic else " WHERE is_synthetic = 0"
        frows = conn.execute(
            f"SELECT ts, payload FROM features{where} ORDER BY ts ASC"
        ).fetchall()
        trows = conn.execute(
            f"SELECT ts, mid FROM market_ticks{where} ORDER BY ts ASC"
        ).fetchall()
    finally:
        conn.close()
    if not frows or not trows:
        return {"status": "NESSUN DATO"}

    payout = payout if payout is not None else cfg.payout
    payout_known = payout is not None
    eff_payout = float(payout) if payout_known else cfg.burst_assumed_payout
    tick_ts = [r["ts"] for r in trows]
    tick_mid = [r["mid"] for r in trows]
    horizon_ms = cfg.horizon_ms

    def price_at(target: int) -> tuple[int, float] | None:
        idx = bisect.bisect_left(tick_ts, target)
        if idx >= len(tick_ts) or tick_ts[idx] - target > 750:
            return None
        return tick_ts[idx], tick_mid[idx]

    trades: list[dict] = []
    sessions: list[dict] = []
    cur: dict | None = None
    last_entry = -10 ** 18
    triggers = unfilled = rows_scanned = 0

    for r in frows:
        try:
            f = json.loads(r["payload"] or "{}")
        except ValueError:
            continue
        rows_scanned += 1
        ts = r["ts"]
        n5, r10, ofi = f.get(N5_FEATURE), f.get(R10_FEATURE), f.get(OFI_FEATURE)

        if cur is None or ts >= cur["end_ts"] or cur["closed"]:
            if cur is not None and cur["trades"]:
                sessions.append(cur)
            if cur is not None and cur["closed"] and ts < cur["end_ts"]:
                continue  # sessione in stop: ferma fino alla fine della finestra
            cur = {"start_ts": ts, "end_ts": ts + cfg.burst_session_s * 1000,
                   "pnl": 0.0, "trades": 0, "wins": 0, "losses": 0, "ties": 0,
                   "closed": None}

        if n5 is None or r10 is None or r10 == 0:
            continue
        if n5 < cfg.burst_n5_min or abs(r10) < cfg.burst_r10_min_bps:
            continue
        if cfg.burst_require_ofi_agree and ofi is not None and (ofi > 0) != (r10 > 0):
            continue
        triggers += 1

        if cur["trades"] >= cfg.burst_max_trades_session:
            cur["closed"] = "massimo di trade"
            continue
        if ts - last_entry < cfg.burst_cooldown_ms:
            continue

        entry = price_at(ts + cfg.burst_entry_delay_ms)
        if entry is None:
            unfilled += 1
            continue
        entry_ts, entry_px = entry
        exit_ = price_at(entry_ts + horizon_ms)
        if exit_ is None:
            unfilled += 1
            continue
        _, exit_px = exit_

        direction = UP if r10 > 0 else DOWN
        if exit_px == entry_px:
            result, pnl = TIE, 0.0
        elif (exit_px > entry_px) == (direction == UP):
            result, pnl = WIN, eff_payout * cfg.stake
        else:
            result, pnl = LOSS, -cfg.stake
        trades.append({"ts": ts, "direction": direction, "entry": entry_px,
                       "exit": exit_px, "result": result, "pnl": pnl})
        cur["trades"] += 1
        cur["pnl"] += pnl
        cur[{WIN: "wins", LOSS: "losses", TIE: "ties"}[result]] += 1
        last_entry = ts
        if cur["pnl"] <= cfg.burst_stop_loss_units:
            cur["closed"] = "stop loss di sessione"
        elif cur["pnl"] >= cfg.burst_take_profit_units:
            cur["closed"] = "take profit di sessione"

    if cur is not None and cur["trades"]:
        sessions.append(cur)

    n = len(trades)
    if n == 0:
        return {"status": "NESSUN TRADE", "rows_scanned": rows_scanned,
                "triggers": triggers,
                "conclusion": ("la regola non e' mai scattata su questi dati. "
                               "Abbassa BURST_N5_MIN / BURST_R10_MIN_BPS, oppure "
                               "controlla che trade_count_5s e return_10000ms "
                               "siano popolati.")}

    wins = sum(1 for t in trades if t["result"] == WIN)
    losses = sum(1 for t in trades if t["result"] == LOSS)
    ties = n - wins - losses
    decided = wins + losses
    pnl = sum(t["pnl"] for t in trades)
    equity = peak = dd = 0.0
    for t in trades:
        equity += t["pnl"]
        peak = max(peak, equity)
        dd = min(dd, equity - peak)
    lo, hi = wilson_interval(wins, decided) if decided else (None, None)
    be = breakeven_win_rate(eff_payout)
    session_pnl = [s["pnl"] for s in sessions]

    if not decided:
        conclusion = "Ogni finestra e' finita in pareggio: niente da concludere."
    elif lo is not None and lo > be:
        conclusion = (
            f"Tasso di vittoria {wins / decided:.4f}, il cui limite inferiore al "
            f"95% ({lo:.4f}) sta sopra il pareggio di {be:.4f} per questo payout. "
            "E' un risultato su dati registrati, non una promessa: le finestre a "
            "5s si sovrappongono e sono correlate, quindi va rivalidato su dati "
            "mai visti prima di agire."
        )
    else:
        conclusion = (
            f"Tasso di vittoria {wins / decided:.4f}; il limite inferiore al 95% "
            f"e' {lo:.4f}, sotto il pareggio di {be:.4f}. {EDGE_NONE}."
        )
    if not payout_known:
        conclusion += (" Il payout e' ASSUNTO (BINARY_PAYOUT non impostato): "
                       "il P&L e' un'illustrazione, non un risultato.")

    return {
        "status": "COMPLETO", "rows_scanned": rows_scanned, "triggers": triggers,
        "unfilled_triggers": unfilled, "trades": n, "wins": wins,
        "losses": losses, "ties": ties,
        "tie_fraction": round(ties / n, 4),
        "win_rate_decided": round(wins / decided, 4) if decided else None,
        "win_rate_ci95": [round(lo, 4), round(hi, 4)] if decided else None,
        "p_value_vs_coinflip": (
            round(binomial_p_value(wins, decided), 6) if decided else None
        ),
        "payout_used": eff_payout,
        "payout_status": "NOTO" if payout_known else "ASSUNTO",
        "breakeven_win_rate": round(be, 4),
        "ev_per_trade_units": round(pnl / n, 5),
        "pnl_units": round(pnl, 3),
        "max_drawdown_units": round(dd, 3),
        "sessions": {
            "count": len(sessions),
            "green_fraction": (
                round(sum(1 for p in session_pnl if p > 0) / len(sessions), 4)
                if sessions else None
            ),
            "mean_pnl_units": (
                round(sum(session_pnl) / len(sessions), 4) if sessions else None
            ),
            "worst": round(min(session_pnl), 3) if sessions else None,
            "best": round(max(session_pnl), 3) if sessions else None,
            "stopped_out": sum(1 for s in sessions
                               if s["closed"] == "stop loss di sessione"),
            "took_profit": sum(1 for s in sessions
                               if s["closed"] == "take profit di sessione"),
        },
        "config": {
            "n5_min": cfg.burst_n5_min, "r10_min_bps": cfg.burst_r10_min_bps,
            "require_ofi_agree": cfg.burst_require_ofi_agree,
            "horizon_s": cfg.horizon_s, "entry_delay_ms": cfg.burst_entry_delay_ms,
            "session_s": cfg.burst_session_s, "cooldown_ms": cfg.burst_cooldown_ms,
        },
        "conclusion": conclusion,
    }


# --------------------------------------------------------------------------- #
#  DASHBOARD + API HTTP
# --------------------------------------------------------------------------- #

DASHBOARD_HTML = """<!doctype html>
<html lang="it"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AURUM ENGINE</title>
<style>
:root{--bg:#0b0e13;--panel:#131822;--panel2:#1a2130;--line:#232c3d;--fg:#e8edf6;
--muted:#8a94a8;--up:#22c55e;--down:#ef4444;--warn:#f59e0b;--accent:#60a5fa}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 ui-monospace,
SFMono-Regular,Menlo,monospace}
.wrap{max-width:1100px;margin:0 auto;padding:16px}
.grid{display:grid;gap:12px;grid-template-columns:repeat(auto-fit,minmax(300px,1fr))}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px}
.label{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.08em}
.big{font-size:34px;font-weight:700;letter-spacing:-.02em}
.huge{font-size:60px;font-weight:800;line-height:1}
.up{color:var(--up)}.down{color:var(--down)}.warn{color:var(--warn)}
.muted{color:var(--muted)}
.row{display:flex;justify-content:space-between;gap:10px;align-items:baseline}
.card{border:2px solid var(--line);text-align:center}
.card.up{border-color:rgba(34,197,94,.45);background:rgba(34,197,94,.06)}
.card.down{border-color:rgba(239,68,68,.45);background:rgba(239,68,68,.06)}
table{width:100%;border-collapse:collapse}
td{padding:3px 0;vertical-align:top}
td.n{text-align:right;color:var(--muted);white-space:nowrap}
.bar{height:6px;background:var(--panel2);border-radius:3px;overflow:hidden}
.bar>div{height:100%;background:var(--accent)}
.pill{display:inline-block;padding:2px 8px;border:1px solid var(--line);
border-radius:999px;font-size:11px}
.banner{background:rgba(245,158,11,.12);border:1px solid rgba(245,158,11,.4);
color:var(--warn);border-radius:8px;padding:8px 12px;margin-bottom:12px;font-size:12px}
h2{font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);
margin:0 0 10px}
</style></head><body><div class="wrap">
<div id="banner"></div>
<div class="row" style="margin-bottom:12px">
  <div><span class="label">AURUM ENGINE</span> <span class="pill" id="strategy">-</span>
  <span class="pill" id="source">-</span></div>
  <div class="label" id="clock">-</div>
</div>
<div class="panel card" id="card" style="margin-bottom:12px">
  <div class="row"><div><div class="label" id="symbol">BTC/USDT</div>
  <div class="big" id="price">-</div></div>
  <div style="text-align:right"><div class="label">Stato</div>
  <div id="status">-</div></div></div>
  <div id="signalbox" style="margin-top:18px"></div>
</div>
<div class="grid">
  <div class="panel"><h2>Diagnostica</h2><div id="verdict" class="muted"></div>
    <div style="margin-top:10px" id="gates"></div></div>
  <div class="panel"><h2>Sessione BURST-15</h2><div id="session" class="muted">-</div></div>
  <div class="panel"><h2>Paper trading</h2><div id="stats"></div></div>
  <div class="panel"><h2>Apprendimento</h2><div id="learn"></div></div>
  <div class="panel"><h2>Salute</h2><div id="health"></div></div>
  <div class="panel"><h2>Ultimi esiti</h2><div id="history"></div></div>
</div>
<p class="muted" style="font-size:11px;margin-top:16px">
SOLO CARTA. Nessun ordine viene mai inviato ad alcun venue. Un tasso di
vittoria non e' un edge finche' il limite inferiore del suo intervallo non
supera il pareggio del payout.</p>
</div>
<script>
const $=(id)=>document.getElementById(id);
const money=(v)=>v==null?'-':Number(v).toLocaleString('it-IT',
  {minimumFractionDigits:2,maximumFractionDigits:2});
const pct=(v)=>v==null?'-':(100*v).toFixed(1)+'%';
function tbl(rows){return '<table>'+rows.map(r=>
  `<tr><td>${r[0]}</td><td class="n">${r[1]}</td></tr>`).join('')+'</table>';}

let offset=0;
async function get(p){const r=await fetch(p);return r.json();}

async function tick(){
  try{
    const [d,s,m,st,rt]=await Promise.all([get('/diagnostics'),get('/signals/current'),
      get('/market'),get('/statistics'),get('/retrain')]);
    offset=(d.server_ts||Date.now())-Date.now();
    $('strategy').textContent=d.strategy;
    $('source').textContent=d.feed.source+(d.feed.is_synthetic?' (SIMULATO)':'');
    $('clock').textContent=new Date().toLocaleTimeString('it-IT');
    $('banner').innerHTML=d.feed.is_synthetic?
      '<div class="banner">DATI SIMULATI: output generato da un modello, non dal '+
      'mercato. Non descrive nulla di reale.</div>':'';
    $('price').textContent='$'+money(m.price);
    $('symbol').textContent=(m.symbol||'').replace('USDT','/USDT');

    const sig=s.signal;
    const card=$('card');
    card.className='panel card'+(sig&&sig.direction==='UP'?' up':
      sig&&sig.direction==='DOWN'?' down':'');
    $('status').textContent=sig?sig.status:'IN ANALISI';
    if(sig&&(sig.status!=='CANCELLED')&&sig.direction!=='NO_TRADE'){
      const rem=sig.remaining_ms!=null?Math.max(0,Math.ceil(sig.remaining_ms/1000)):null;
      const wait=sig.wait_remaining_ms!=null?Math.ceil(sig.wait_remaining_ms/1000):null;
      $('signalbox').innerHTML=
        `<div class="huge ${sig.direction==='UP'?'up':'down'}">`+
        `${sig.direction==='UP'?'&#8593; SU':'&#8595; GIU'}</div>`+
        `<div class="grid" style="margin-top:14px">`+
        `<div><div class="label">Trigger</div><div>$${money(sig.trigger_price)}</div></div>`+
        `<div><div class="label">Durata</div><div>${sig.horizon_s}s</div></div>`+
        `<div><div class="label">Confidenza</div><div>${pct(sig.confidence)}</div></div>`+
        `<div><div class="label">Entry</div><div>${sig.entry_price?'$'+money(sig.entry_price):'-'}</div></div>`+
        `</div>`+
        (rem!=null?`<div style="margin-top:14px"><div class="label">Countdown</div>
          <div class="huge">${rem}</div></div>`:
         wait!=null?`<div style="margin-top:14px" class="muted">
          ${sig.entry_mode==='DELAY'?'Ingresso a mercato fra':'In attesa del trigger'} ${wait}s</div>`:'')+
        (sig.result?`<div style="margin-top:10px" class="pill">${sig.result}</div>`:'');
    }else{
      const gl=(d.blocking_gates||[]).slice(0,3).map(g=>
        `<tr><td>${g.gate}</td><td class="n warn">${pct(g.share_of_decisions)}</td></tr>`).join('');
      $('signalbox').innerHTML='<div class="huge muted">NO TRADE</div>'+
        `<div class="muted" style="margin-top:8px;font-size:12px">`+
        `${d.signals_emitted} segnali in ${Math.round(d.uptime_s)}s &middot; `+
        `${d.decisions_evaluated} finestre valutate</div>`+
        (gl?`<div style="margin-top:12px;text-align:left"><div class="label">Cosa blocca, nel tempo</div>
             <table>${gl}</table></div>`:'');
    }

    $('verdict').textContent=d.verdict;
    $('gates').innerHTML=tbl((d.blocking_gates||[]).slice(0,6).map(g=>
      [g.gate,pct(g.share_of_decisions)]));

    const b=d.burst&&d.burst.session;
    $('session').innerHTML=b?tbl([
      ['P&amp;L sessione (unita)',b.pnl_units],
      ['Trade',`${b.trades} (${b.wins}V/${b.losses}P/${b.ties}=)`],
      ['Restano',Math.round(b.remaining_ms/1000)+'s'],
      ['Stato',b.closed_reason||'aperta']]):
      '<span class="muted">BURST-15 non attiva (SIGNAL_STRATEGY=burst15)</span>';

    $('stats').innerHTML=tbl([
      ['Segnali',st.signals],['Chiusi',st.settled],
      ['Vinti / Persi / Pari',`${st.wins} / ${st.losses} / ${st.ties}`],
      ['Win rate (decisi)',st.win_rate_decided!=null?pct(st.win_rate_decided):'-'],
      ['IC 95%',st.win_rate_ci95?`${pct(st.win_rate_ci95[0])} - ${pct(st.win_rate_ci95[1])}`:'-'],
      ['Pareggio richiesto',st.breakeven_win_rate!=null?pct(st.breakeven_win_rate):st.payout_status],
      ['P&amp;L (unita)',st.pnl_units!=null?st.pnl_units:'-'],
      ['Drawdown max',st.max_drawdown_units!=null?st.max_drawdown_units:'-']]);

    $('learn').innerHTML=tbl([
      ['Attivo',rt.enabled?'si':'no'],['Cicli',rt.runs],
      ['Modelli attivati',rt.activations],
      ['Ultimo esito',rt.last_result?(rt.last_result.edge||rt.last_result.skipped||'-'):'-'],
      ['Nota',rt.last_result&&rt.last_result.note?rt.last_result.note:'-']]);

    $('health').innerHTML=tbl([
      ['Feed',d.feed.adapter.connected?'connesso':'<span class="warn">disconnesso</span>'],
      ['Eta feed',d.feed.feed_age_ms!=null?Math.round(d.feed.feed_age_ms)+'ms':'-'],
      ['Book',d.feed.book_synced?'sincronizzato':'<span class="warn">'+
        (d.feed.book_desync_reason||'no')+'</span>'],
      ['Qualita dati',pct(d.feed.data_quality.score)],
      ['Errore adapter',d.feed.adapter.last_error||'-']]);

    const h=(s.history||[]).slice(0,8).map(x=>
      [`${x.direction} ${x.result||x.status}`,
       x.pnl_units!=null?x.pnl_units.toFixed(2):'-']);
    $('history').innerHTML=h.length?tbl(h):'<span class="muted">nessuno ancora</span>';
  }catch(e){$('verdict').textContent='motore non raggiungibile: '+e;}
}
tick();setInterval(tick,700);
</script></body></html>"""


def make_http_server(engine: "Engine"):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):  # noqa: A003 - silenzio: il log e' il nostro
            return

        def _send(self, payload: Any, status: int = 200,
                  content_type: str = "application/json") -> None:
            body = (payload if isinstance(payload, bytes)
                    else json.dumps(_finite(payload), default=str).encode())
            self.send_response(status)
            self.send_header("Content-Type", content_type + "; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - firma imposta da BaseHTTPRequestHandler
            path = self.path.split("?")[0].rstrip("/") or "/"
            try:
                self._send(*self._route(path))
            except Exception as exc:  # noqa: BLE001 - un errore API non ferma il motore
                self._send({"error": f"{type(exc).__name__}: {exc}"}, 500)

        def do_POST(self) -> None:  # noqa: N802
            path = self.path.split("?")[0].rstrip("/") or "/"
            try:
                if path == "/retrain":
                    self._send(engine.retrainer.run_once())
                elif path == "/burst/session/start":
                    engine.signals.burst.close_session("chiusa dall'operatore")
                    self._send(engine.signals.burst.open_session().to_dict())
                elif path == "/burst/session/stop":
                    engine.signals.burst.close_session("chiusa dall'operatore")
                    self._send(engine.signals.burst.status())
                else:
                    self._send({"error": "endpoint sconosciuto"}, 404)
            except Exception as exc:  # noqa: BLE001
                self._send({"error": f"{type(exc).__name__}: {exc}"}, 500)

        def _route(self, path: str):
            cfg = engine.cfg
            if path == "/":
                return DASHBOARD_HTML.encode(), 200, "text/html"
            if path == "/health":
                return engine.health(),
            if path == "/diagnostics":
                return engine.signals.diagnostics(),
            if path == "/market":
                return {**engine.market.snapshot(),
                        "data_quality": engine.market.data_quality(),
                        "server_ts": now_ms()},
            if path == "/orderbook":
                return engine.market.book.snapshot_dict(levels=20),
            if path == "/features":
                return {"latest": _finite(engine.features.latest),
                        "computed": engine.features.computed},
            if path == "/signals":
                return engine.signals.snapshot(),
            if path == "/signals/current":
                return {"server_ts": now_ms(), "signal": engine.signals.current(),
                        "history": [s.to_dict() for s in
                                    engine.signals.history[-10:]][::-1],
                        "no_trade_reasons": (
                            engine.signals.last_decision.no_trade_reasons
                            if engine.signals.last_decision else [])},
            if path == "/burst/session":
                return {**engine.signals.burst.status(),
                        "active_strategy": cfg.strategy},
            if path == "/statistics":
                trades = engine.store.paper_trades() if engine.store else []
                return summarise(trades, cfg.payout, cfg.stake,
                                 engine.signals.counters["no_trade"]),
            if path == "/statistics/calibration":
                trades = engine.store.paper_trades() if engine.store else []
                return calibration_report(trades),
            if path == "/statistics/montecarlo":
                trades = engine.store.paper_trades() if engine.store else []
                return monte_carlo([t.get("result") or "" for t in trades],
                                   cfg.payout, cfg.stake),
            if path == "/shadow":
                return shadow_report(engine.store) if engine.store else {"status": "NESSUN DB"},
            if path == "/retrain":
                return engine.retrainer.status(),
            if path == "/models":
                return {"active": engine.model.model_id if engine.model else None,
                        "versions": engine.store.models() if engine.store else []},
            if path == "/config":
                return {k: v for k, v in cfg.to_dict().items()},
            if path == "/db":
                return {"path": cfg.db_path,
                        "counts": engine.store.counts() if engine.store else {},
                        "writer": engine.store.stats if engine.store else {}},
            return {"error": "endpoint sconosciuto", "try": [
                "/", "/health", "/diagnostics", "/market", "/signals",
                "/signals/current", "/burst/session", "/statistics", "/shadow",
                "/retrain", "/models", "/config", "/db"]}, 404

    server = ThreadingHTTPServer((engine.cfg.http_host, engine.cfg.http_port), Handler)
    server.daemon_threads = True
    return server


# --------------------------------------------------------------------------- #
#  MOTORE: orchestrazione e ciclo principale
# --------------------------------------------------------------------------- #


class Engine:
    def __init__(self, cfg: Config, store: Store | None = None) -> None:
        self.cfg = cfg
        self.store = store if store is not None else Store(cfg)
        self.feed = build_feed(cfg)
        self.market = MarketData(cfg, self.feed, self.store)
        self.features = FeatureEngine(cfg, self.market)
        self.model: Model | None = None
        self._load_active_model()
        self.signals = SignalEngine(cfg, self.market, self.features, self.store,
                                    self.model)
        self.retrainer = Retrainer(cfg, self.store, on_model=self._activate_model)
        self.http = None
        self.started_at = now_ms()
        self._last_feature_ts = 0
        self._last_status = 0.0
        self._running = False
        self.signals.on_event = self._on_signal_event

    # ------------------------------------------------------------------ setup
    def _load_active_model(self) -> None:
        try:
            artifact = self.store.active_model_artifact()
            if artifact:
                self.model = Model.from_json(artifact)
        except Exception as exc:  # noqa: BLE001 - un modello rotto non blocca il feed
            self.market.record_error("model", f"caricamento fallito: {exc}")

    def _activate_model(self, model: Model) -> None:
        self.model = model
        self.signals.decisions.model = model
        self._log(f"modello attivato: {model.model_id} "
                  f"(calibrato: {'si' if model.calibrated else 'no'})")

    def start(self) -> None:
        self.store.start()
        self._running = True
        if self.cfg.http_port:
            try:
                self.http = make_http_server(self)
                threading.Thread(target=self.http.serve_forever,
                                 name="http", daemon=True).start()
                self._log(f"dashboard su http://{self.cfg.http_host}:{self.cfg.http_port}")
            except OSError as exc:
                self._log(f"porta {self.cfg.http_port} non disponibile: {exc}")
        self.retrainer.start()

    def stop(self) -> None:
        self._running = False
        self.retrainer.stop()
        if self.http:
            self.http.shutdown()
        self.feed.close()
        self.store.stop()

    # ------------------------------------------------------------ ciclo vitale
    def run(self) -> None:
        cfg = self.cfg
        replay = cfg.source == "csv"
        if replay:
            # In replay l'orologio segue i dati: lo stesso codice percorre sia
            # una sessione live sia un backtest.
            set_clock(lambda: getattr(self.feed, "virtual_ts", 0) or 0)
        self.start()
        self._log(f"AURUM ENGINE {VERSION} - {cfg.source} / {cfg.symbol} / "
                  f"strategia {cfg.strategy} / orizzonte {cfg.horizon_s:g}s")
        if self.market.is_synthetic:
            self._log("ATTENZIONE: dati SIMULATI, generati da un modello. "
                      "Non descrivono il mercato.")
        if cfg.payout is None:
            self._log("BINARY_PAYOUT non impostato: il P&L monetario restera' "
                      "PAYOUT SCONOSCIUTO (usa --payout 0.8).")
        try:
            while self._running:
                self.market.pump(0.0 if replay else 0.02)
                ts = now_ms()
                if ts - self._last_feature_ts >= cfg.feature_interval_ms:
                    self._last_feature_ts = ts
                    vector = self.features.compute(ts)
                    if vector is not None:
                        if self.store and cfg.persist_features:
                            f = vector["features"]
                            self.store.add("features", {
                                "ts": vector["ts"], "exchange": vector["exchange"],
                                "symbol": vector["symbol"], "mid": f.get("mid"),
                                "spread_bps": f.get("spread_bps"),
                                "book_synced": int(bool(f.get("book_synced"))),
                                "data_quality": f.get("data_quality"),
                                "regime": None,
                                "payload": json.dumps(_finite(f)),
                                "is_synthetic": int(vector["is_synthetic"]),
                            })
                        self.signals.evaluate(vector)
                self.signals.tick_check()
                self._status_line()
                if replay and getattr(self.feed, "exhausted", False):
                    # I dati sono finiti e l'orologio virtuale con essi: un
                    # segnale ancora aperto non potra' mai scadere. Va
                    # annullato, non risolto con un prezzo che non esiste.
                    for sid in list(self.signals.active):
                        sig = self.signals.active[sid]
                        self.signals._cancel(sig, now_ms(),
                                             "dati finiti prima della scadenza")
                    break
        except KeyboardInterrupt:
            print()
            self._log("interrotto")
        finally:
            self.stop()

    # ------------------------------------------------------------------- viste
    def health(self) -> dict[str, Any]:
        market = self.market.health()
        quality = market["data_quality"]
        components = {
            "feed": {"status": "UP" if self.feed.connected else "DOWN",
                     "detail": self.feed.state()},
            "order_book": {"status": "UP" if self.market.book.synced else "DOWN",
                           "detail": market["orderbook"]},
            "database": {"status": "UP" if self.store.healthy else "DEGRADED",
                         "detail": {**self.store.stats,
                                    "last_error": self.store.last_error}},
            "signals": {"status": "UP", "detail": self.signals.counters},
            "model": {"status": "UP" if self.model else "DISABLED",
                      "detail": {"model_id": self.model.model_id if self.model else None,
                                 "calibrated": self.model.calibrated if self.model else None}},
            "learning": {"status": "UP" if self.cfg.auto_retrain else "DISABLED",
                         "detail": self.retrainer.status()},
        }
        overall = ("HEALTHY"
                   if all(c["status"] in ("UP", "DISABLED") for c in components.values())
                   else "DEGRADED")
        return {
            "status": overall, "version": VERSION,
            "uptime_s": round((now_ms() - self.started_at) / 1000.0, 1),
            "server_ts": now_ms(), "symbol": self.cfg.symbol,
            "source": self.feed.name, "is_synthetic": self.market.is_synthetic,
            "synthetic_warning": (
                "DATI SIMULATI: questa istanza gira sul simulatore, non su un "
                "venue. Niente qui descrive il mercato reale."
                if self.market.is_synthetic else None
            ),
            "mode": "SOLO CARTA - nessun ordine viene inviato ad alcun venue",
            "components": components, "market": market, "data_quality": quality,
        }

    # ------------------------------------------------------------------ output
    def _log(self, message: str) -> None:
        if self.cfg.quiet:
            return
        sys.stdout.write(f"\r\033[K[{fmt_ts(now_ms())}] {message}\n")
        sys.stdout.flush()

    def _on_signal_event(self, event: str, sig: LiveSignal) -> None:
        if self.cfg.json_out:
            print(json.dumps({"event": event, "signal": sig.to_dict()}, default=str),
                  flush=True)
            return
        if self.cfg.quiet:
            return
        arrow = "^ SU" if sig.direction == UP else "v GIU"
        if event == "signal_created":
            entry = (f"ingresso fra {sig.entry_delay_ms}ms"
                     if sig.entry_mode == "DELAY"
                     else f"trigger {sig.trigger_price:.2f}")
            self._log(f"SEGNALE {arrow}  rif {sig.reference_price:.2f}  {entry}  "
                      f"conf {sig.confidence:.0%}")
        elif event == "trade_active":
            self._log(f"  ingresso a {sig.entry_price:.2f} - countdown "
                      f"{sig.horizon_s:g}s")
        elif event == "signal_settled":
            pnl = f"  pnl {sig.pnl_units:+.2f}u" if sig.pnl_units is not None else ""
            self._log(f"  esito {sig.result}  uscita "
                      f"{sig.expiry_price if sig.expiry_price else 0:.2f}{pnl}")

    def _status_line(self) -> None:
        if self.cfg.quiet or self.cfg.json_out:
            return
        now = time.time()
        if now - self._last_status < 1.0:
            return
        self._last_status = now
        c = self.signals.counters
        tick = self.market.last_tick
        price = f"{tick.mid:,.2f}" if tick else "-"
        gate = ""
        if not self.signals.active and self.signals.gate_counter:
            top = max(self.signals.gate_counter.items(), key=lambda kv: kv[1])
            gate = f" | blocca: {top[0][:46]}"
        session = ""
        if self.cfg.strategy == "burst15" and self.signals.burst.session:
            s = self.signals.burst.session
            session = f" | sessione {s.pnl_units:+.1f}u {s.trades}t"
        sys.stdout.write(
            f"\r\033[K{price}  dec {c['decisions']}  seg {c['signals']}  "
            f"V/P/= {c['wins']}/{c['losses']}/{c['ties']}{session}{gate}"
        )
        sys.stdout.flush()


# --------------------------------------------------------------------------- #
#  COMANDI
# --------------------------------------------------------------------------- #


def cmd_run(cfg: Config, args) -> int:
    Engine(cfg).run()
    return 0


def cmd_check(cfg: Config, args) -> int:
    """Il venue e' raggiungibile? E' la prima domanda quando "non arrivano
    segnali": nessuna soglia risolve una rete che non passa.

    Ogni passo viene stampato PRIMA di essere tentato, cosi' un blocco si vede
    dove avviene invece di apparire come un programma fermo.
    """
    timeout = float(getattr(args, "timeout", 8.0) or 8.0)

    def say(text: str) -> None:
        print(text, flush=True)

    say(f"AURUM ENGINE {VERSION} - controllo connettivita' (timeout {timeout:g}s)")
    say(f"  sorgente : {cfg.source}   simbolo: {cfg.symbol}")
    say(f"  proxy    : {cfg.proxy or 'nessuno (connessione diretta)'}")
    ok = True

    if cfg.source == "binance":
        feed = BinanceFeed(cfg)
        rest_url = f"{BinanceFeed.REST_BASE}/api/v3/time"
        ws_url = feed.ws_url
    elif cfg.source == "coinbase":
        feed = CoinbaseFeed(cfg)
        rest_url = "https://api.exchange.coinbase.com/time"
        ws_url = feed.WS_URL
    else:
        say("  sorgente locale: niente da verificare in rete.")
        return 0

    from urllib.parse import urlsplit
    host = urlsplit(rest_url).hostname or ""
    say(f"  DNS      : {host} ...")
    try:
        t0 = time.time()
        addr = socket.getaddrinfo(host, 443)[0][4][0]
        say(f"             {addr} ({(time.time() - t0) * 1000:.0f}ms)")
    except socket.gaierror as exc:
        say(f"             FALLITO: {exc}. Il nome non si risolve: e' un "
            f"problema di DNS, non del venue.")
        return 1

    say(f"  REST     : {rest_url} ...")
    t0 = time.time()
    status, body = http_get(rest_url, cfg.proxy, timeout=timeout)
    dt = (time.time() - t0) * 1000
    if status == 200:
        say(f"             OK ({dt:.0f}ms) {body[:60].decode('utf-8', 'replace')}")
    else:
        ok = False
        say(f"             FALLITO ({dt:.0f}ms, HTTP {status or '-'}) "
            f"{body[:160].decode('utf-8', 'replace')}")
        if status in (451, 403):
            say("             richiesta rifiutata: puo' essere la regione "
                "bloccata dal venue oppure la tua rete che non consente "
                "l'uscita. Usa --proxy, o --source coinbase.")
        elif status == 0:
            say("             nessuna risposta: rete bloccata, TLS intercettato "
                "o proxy necessario.")

    say(f"  WebSocket: {ws_url[:70]} ...")
    try:
        t0 = time.time()
        ws = WebSocket(ws_url, proxy=cfg.proxy, timeout=timeout)
        say(f"             handshake OK ({(time.time() - t0) * 1000:.0f}ms)")
        if cfg.source == "coinbase":
            ws.send_text(json.dumps({"type": "subscribe",
                                     "product_ids": [feed.product],
                                     "channels": ["ticker"]}))
        deadline = time.time() + min(timeout, 10.0)
        got = 0
        while time.time() < deadline and got < 3:
            got += len(ws.poll(0.5))
        say(f"             {got} messaggi ricevuti")
        ok = ok and got > 0
        ws.close()
    except Exception as exc:  # noqa: BLE001 - e' un diagnostico: riporta e basta
        ok = False
        say(f"             FALLITO {type(exc).__name__}: {exc}")

    say("")
    say("  ESITO: " + ("tutto raggiungibile, il motore puo' partire."
                       if ok else
                       "il feed NON e' raggiungibile. Il motore partirebbe, non "
                       "riceverebbe nulla e direbbe NO TRADE per sempre.\n"
                       "         Prova: --proxy http://host:porta, oppure "
                       "--source coinbase, oppure --source sim per lavorare "
                       "offline."))
    return 0 if ok else 1


def _open_existing(cfg: Config) -> Store:
    """Apre il database per un comando di sola lettura.

    SQLite crea allegramente un file vuoto se il percorso non esiste, e il
    comando risponderebbe "zero segnali, zero trade" facendo credere che il
    motore non abbia mai registrato niente. Quasi sempre significa solo che sei
    in un'altra cartella rispetto a quella in cui gira il motore.
    """
    if cfg.db_path != ":memory:" and not os.path.exists(cfg.db_path):
        raise SystemExit(
            f"database non trovato: {os.path.abspath(cfg.db_path)}\n"
            f"Il motore scrive nella cartella da cui lo lanci. Spostati li',\n"
            f"oppure indica il percorso completo con --db /percorso/aurum.db"
        )
    return Store(cfg)


def cmd_stats(cfg: Config, args) -> int:
    store = _open_existing(cfg)
    trades = store.paper_trades()
    report = summarise(trades, cfg.payout, cfg.stake)
    report["calibration"] = calibration_report(trades)
    report["monte_carlo"] = monte_carlo([t.get("result") or "" for t in trades],
                                        cfg.payout, cfg.stake)
    report["database"] = {"path": cfg.db_path, "counts": store.counts()}
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    store.stop()
    return 0


def cmd_backtest(cfg: Config, args) -> int:
    store = _open_existing(cfg)
    report = train_and_validate(store, cfg, include_synthetic=args.include_synthetic)
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    store.stop()
    return 0


def cmd_burst(cfg: Config, args) -> int:
    store = _open_existing(cfg)
    report = burst_replay(store, cfg, include_synthetic=args.include_synthetic,
                          payout=cfg.payout)
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    store.stop()
    return 0


def cmd_burst_grid(cfg: Config, args) -> int:
    """La stessa griglia dello script originale, su dati registrati."""
    store = _open_existing(cfg)
    print(f"{'n5':>5}{'|r10|':>8}{'ofi':>5}{'trade':>8}{'win':>8}"
          f"{'EV':>9}{'PnL':>9}{'sess':>7}")
    for n5 in (10, 20, 40, 60, 100):
        for r10 in (0.2, 0.5, 1.0, 2.0):
            for agree in (True, False):
                cfg.burst_n5_min = n5
                cfg.burst_r10_min_bps = r10
                cfg.burst_require_ofi_agree = agree
                rep = burst_replay(store, cfg, include_synthetic=args.include_synthetic)
                if rep.get("trades", 0) < args.min_trades:
                    continue
                print(f"{n5:5d}{r10:8.1f}{'si' if agree else 'no':>5}"
                      f"{rep['trades']:8d}{rep['win_rate_decided'] or 0:8.3f}"
                      f"{rep['ev_per_trade_units']:+9.4f}{rep['pnl_units']:+9.1f}"
                      f"{rep['sessions']['count']:7d}")
    print("\nUna griglia e' una ricerca: la riga migliore qui e' il massimo di "
          "tanti numeri casuali finche' non regge una correzione per test "
          "multipli. Trattala come un'ipotesi, non come un risultato.")
    store.stop()
    return 0


def cmd_shadow(cfg: Config, args) -> int:
    store = _open_existing(cfg)
    print(json.dumps(shadow_report(store, args.include_synthetic), indent=2,
                     ensure_ascii=False, default=str))
    store.stop()
    return 0


def cmd_config(cfg: Config, args) -> int:
    print(json.dumps(cfg.to_dict(), indent=2, ensure_ascii=False, default=str))
    return 0


# --------------------------------------------------------------------------- #
#  SELFTEST
# --------------------------------------------------------------------------- #


def _ws_test_server(payloads: list[tuple[int, bool, bytes]]) -> tuple[str, threading.Thread]:
    """Server WebSocket minimale che invia esattamente i frame richiesti.

    Serve a provare il client contro frammentazione, ping e lunghezze estese
    senza dipendere dalla rete.
    """
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]

    def serve() -> None:
        conn, _ = srv.accept()
        data = b""
        while b"\r\n\r\n" not in data:
            data += conn.recv(4096)
        key = ""
        for line in data.decode("latin-1").split("\r\n"):
            if line.lower().startswith("sec-websocket-key:"):
                key = line.split(":", 1)[1].strip()
        accept = base64.b64encode(
            hashlib.sha1((key + WS_GUID).encode()).digest()
        ).decode()
        conn.sendall(
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
            b"Connection: Upgrade\r\nSec-WebSocket-Accept: " + accept.encode()
            + b"\r\n\r\n"
        )
        for opcode, fin, payload in payloads:
            header = bytearray()
            header.append((0x80 if fin else 0x00) | opcode)
            n = len(payload)
            if n < 126:
                header.append(n)
            elif n < 65536:
                header.append(126)
                header += struct.pack(">H", n)
            else:
                header.append(127)
                header += struct.pack(">Q", n)
            conn.sendall(bytes(header) + payload)  # il server non maschera
            time.sleep(0.01)
        time.sleep(0.5)
        conn.close()
        srv.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    return f"ws://127.0.0.1:{port}/", thread


def cmd_selftest(cfg: Config, args) -> int:
    """Verifica il file su se stesso: framing, feature, strategia, database,
    apprendimento. Nessuna rete esterna, nessun servizio da installare."""
    results: list[tuple[str, bool, str]] = []

    def check(name: str, fn: Callable[[], str]) -> None:
        try:
            detail = fn()
            results.append((name, True, detail))
        except AssertionError as exc:
            results.append((name, False, str(exc)))
        except Exception as exc:  # noqa: BLE001
            results.append((name, False, f"{type(exc).__name__}: {exc}"))

    # ------------------------------------------------------------- websocket
    def t_ws() -> str:
        big = b'{"big":"' + b"x" * 500 + b'"}'
        url, _ = _ws_test_server([
            (OP_TEXT, True, b'{"a":1}'),
            (OP_TEXT, False, b'{"frag":'),          # frammento 1
            (OP_CONT, True, b'"ok"}'),              # frammento 2
            (OP_PING, True, b"hb"),                 # deve rispondere pong
            (OP_TEXT, True, big),                   # lunghezza estesa
        ])
        ws = WebSocket(url)
        got: list[str] = []
        deadline = time.time() + 5
        while time.time() < deadline and len(got) < 3:
            got.extend(ws.poll(0.1))
        ws.close()
        assert got[0] == '{"a":1}', f"primo frame: {got[0]!r}"
        assert got[1] == '{"frag":"ok"}', f"riassemblaggio: {got[1]!r}"
        assert json.loads(got[2])["big"].startswith("xxx"), "frame lungo"
        return f"{len(got)} messaggi, frammentazione e ping gestiti"

    # ---------------------------------------------------------------- order book
    def t_book() -> str:
        book = OrderBook("test", "BTCUSDT")
        book.apply_snapshot(DepthSnapshot(0, 100, [(99.0, 1.0)], [(101.0, 2.0)]))
        assert book.synced, "snapshot non applicato"
        assert book.apply(DepthUpdate(0, 101, 102, [(99.0, 3.0)], []))
        assert book.bids[99.0] == 3.0, "aggiornamento non applicato"
        # Un buco nella sequenza deve desincronizzare, non essere ignorato.
        assert not book.apply(DepthUpdate(0, 200, 201, [], []))
        assert not book.synced, "il buco non ha desincronizzato il book"
        return "sequenza validata, buco rilevato"

    # ------------------------------------------------------------------ feature
    def t_features() -> str:
        c = Config(source="sim", min_warmup_s=0.0, sim_seed=1, db_path=":memory:")
        market = MarketData(c, SyntheticFeed(c), None)
        fe = FeatureEngine(c, market)
        base = now_ms() - 30_000
        price = 100_000.0
        for i in range(300):
            ts = base + i * 100
            price += 0.5
            market.ingest(Tick(ts=ts, exchange="t", symbol="BTCUSDT",
                               bid_price=price - 0.01, bid_qty=5.0,
                               ask_price=price + 0.01, ask_qty=5.0))
            market.ingest(TradePrint(ts=ts, exchange="t", symbol="BTCUSDT",
                                     trade_id=i, price=price, quantity=0.5,
                                     is_buyer_maker=False))
        vec = fe.compute(base + 299 * 100)
        f = vec["features"]
        assert f["return_10000ms"] is not None, "manca il ritorno a 10s"
        assert f["return_10000ms"] > 0, "prezzo in salita ma ritorno negativo"
        assert f["trade_count_5s"] >= 40, f"conteggio trade {f['trade_count_5s']}"
        assert abs(f["ofi_notional_5s"] - 1.0) < 1e-9, "OFI su soli acquisti"
        return (f"r10={f['return_10000ms']:.2f}bps, n5={f['trade_count_5s']:.0f}, "
                f"ofi={f['ofi_notional_5s']:.2f}")

    # ---------------------------------------------------------------- burst-15
    def t_burst() -> str:
        c = Config(strategy="burst15", payout=0.8, min_warmup_s=0.0)
        strat = BurstStrategy(c)
        vec = {"ts": now_ms(), "symbol": "BTCUSDT", "exchange": "t",
               "features": {"trade_count_5s": 60.0, "return_10000ms": 1.5,
                            "ofi_notional_5s": 0.4, "mid": 100_000.0}}
        health = {"feed_age_ms": 100.0,
                  "data_quality": {"score": 1.0, "reasons": [], "warmup_complete": True}}
        market = {"price": 100_000.0}
        d = strat.decide(vec, market, health)
        assert d.direction == UP, f"non ha sparato: {d.no_trade_reasons}"
        assert d.entry_mode == "DELAY", "l'ingresso deve essere a tempo"

        # flusso discorde -> niente trade
        bad = json.loads(json.dumps(vec))
        bad["features"]["ofi_notional_5s"] = -0.6
        assert strat.decide(bad, market, health).direction == NO_TRADE, \
            "il flusso discorde non ha bloccato"

        # tape rada -> niente trade
        thin = json.loads(json.dumps(vec))
        thin["features"]["trade_count_5s"] = 5.0
        assert strat.decide(thin, market, health).direction == NO_TRADE, \
            "la tape rada non ha bloccato"

        # sei sconfitte chiudono la sessione allo stop-loss
        ts = now_ms()
        for i in range(6):
            strat.on_entry(f"s{i}", ts)
            strat.on_settled(f"s{i}", LOSS, ts)
        assert strat.session.closed_reason == "stop loss di sessione", \
            f"stop loss non scattato: {strat.session.closed_reason}"
        after = strat.decide({**vec, "ts": ts + 60_000}, market, health)
        assert after.direction == NO_TRADE, "ha operato dopo lo stop di sessione"
        return "trigger, flusso, tape e stop-loss di sessione rispettati"

    # ------------------------------------------------------- cancelli ensemble
    def t_gates() -> str:
        c = Config(min_warmup_s=0.0)
        # La confidenza e' 0.5 + edge: la soglia effettiva e' la piu' stretta.
        c.min_confidence, c.min_edge = 0.55, 0.05
        assert abs(c.effective_min_confidence - 0.55) < 1e-9
        c.min_edge = 0.20
        assert abs(c.effective_min_confidence - 0.70) < 1e-9, \
            "min_edge deve poter vincere su min_confidence"
        # L'agente volatilita' deve leggere la configurazione, non costanti.
        agent = VolatilityAgent(Config(min_expected_move_ticks=0.5))
        ctx = Ctx(now_ms(), {"realized_vol_5s_bps": 2.0, "realized_vol_30s_bps": 2.0,
                             "spread_bps": 0.1, "expected_move_ticks": 1.0,
                             "zero_move_fraction": 0.1}, 1.0, True)
        assert agent.evaluate(ctx).extra["tradable"], \
            "soglia allentata ignorata dall'agente"
        strict = VolatilityAgent(Config(min_expected_move_ticks=5.0))
        assert not strict.evaluate(ctx).extra["tradable"], "soglia stretta ignorata"
        return "soglie coerenti e lette dagli agenti"

    # ---------------------------------------------------------------- database
    def t_db() -> str:
        import tempfile
        path = os.path.join(tempfile.mkdtemp(), "selftest.db")
        c = Config(db_path=path)
        store = Store(c)
        store.start()
        store.add("market_ticks", Tick(ts=1, exchange="t", symbol="X",
                                       bid_price=1.0, bid_qty=1.0, ask_price=2.0,
                                       ask_qty=1.0).row())
        store.upsert_paper_trade({"signal_id": "abc", "ts": 1, "symbol": "X",
                                  "direction": UP, "status": WAITING,
                                  "result": None, "pnl_units": None})
        store.upsert_paper_trade({"signal_id": "abc", "ts": 1, "symbol": "X",
                                  "direction": UP, "status": WIN, "result": WIN,
                                  "pnl_units": 0.8})
        store.flush()
        counts = store.counts()
        trades = store.paper_trades()
        store.stop()
        os.unlink(path)
        assert counts["market_ticks"] == 1, counts
        assert len(trades) == 1 and trades[0]["result"] == WIN, \
            "l'upsert non ha aggiornato la riga"
        return f"scrittura batch e upsert ok ({counts['market_ticks']} tick)"

    # ------------------------------------------------------------ apprendimento
    def t_learning() -> str:
        rng = random.Random(3)
        rows, ys, tss = [], [], []
        t0 = 1_700_000_000_000
        # 6000 righe a 100ms sono 600s: dopo la purga di orizzonte + embargo
        # restano ancora abbastanza righe di coda per calibrare.
        for i in range(6000):
            signal = rng.gauss(0, 1)
            noise = rng.gauss(0, 1)
            rows.append([signal, noise])
            ys.append(1 if signal + 0.4 * noise > 0 else 0)
            tss.append(t0 + i * 100)
        cfg_ml = Config(ml_epochs=12)
        ds = Dataset(rows, ys, tss, [100.0] * len(rows), [100.0] * len(rows),
                     ["signal", "noise"], 5.0)
        report = run_walk_forward(ds, cfg_ml, 3)
        acc = report["out_of_sample"]["accuracy"]
        assert acc > 0.75, f"non ha imparato una relazione ovvia: {acc}"
        model = fit_final_model(ds, cfg_ml)
        assert model.calibrated, "calibrazione non eseguita"
        p_up = model.predict({"signal": 3.0, "noise": 0.0})["prob_up"]
        p_down = model.predict({"signal": -3.0, "noise": 0.0})["prob_up"]
        assert p_up > 0.6 > p_down, f"inferenza incoerente: {p_up:.2f}/{p_down:.2f}"
        # La guardia fuori distribuzione deve scattare su input assurdi.
        far = model.predict({"signal": 500.0, "noise": 500.0})
        assert far.get("out_of_distribution"), "guardia OOD non scattata"
        # Su puro rumore il verdetto deve essere: nessun edge.
        rows2 = [[rng.gauss(0, 1), rng.gauss(0, 1)] for _ in range(len(rows))]
        ys2 = [rng.randint(0, 1) for _ in range(len(rows))]
        ds2 = Dataset(rows2, ys2, tss, [100.0] * len(rows), [100.0] * len(rows),
                      ["a", "b"], 5.0)
        edge = classify_edge(run_walk_forward(ds2, cfg_ml, 3), cfg_ml, 100)
        assert edge["classification"] == EDGE_NONE, \
            f"ha visto un edge nel rumore: {edge['classification']}"
        return (f"accuratezza fuori campione {acc:.3f}, calibrato, "
                f"rumore classificato correttamente")

    # ------------------------------------------------------------- statistica
    def t_stats() -> str:
        lo, hi = wilson_interval(60, 100)
        assert 0.49 < lo < 0.51 and 0.68 < hi < 0.70, (lo, hi)
        assert abs(breakeven_win_rate(0.8) - 0.5556) < 1e-3
        trades = ([{"result": WIN, "ts": i, "confidence": 0.7} for i in range(60)]
                  + [{"result": LOSS, "ts": 60 + i, "confidence": 0.7} for i in range(40)])
        rep = summarise(trades, payout=0.8)
        assert rep["win_rate_decided"] == 0.6
        assert rep["above_breakeven"] is True
        blind = summarise(trades, payout=None)
        assert "pnl_units" not in blind, "P&L dichiarato senza payout noto"
        return "Wilson, pareggio e rifiuto del P&L senza payout"

    # ------------------------------------------------------ motore end-to-end
    def t_engine() -> str:
        """Motore intero sul percorso di replay: orologio virtuale, database
        vero, ciclo di vita completo dal segnale all'esito."""
        import tempfile
        tmp = tempfile.mkdtemp()
        csv_path = os.path.join(tmp, "trades.csv")
        db_path = os.path.join(tmp, "e2e.db")
        rng = random.Random(11)
        t0 = 1_700_000_000_000
        price = 100_000.0
        with open(csv_path, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["ts", "price", "quantity", "aggressor"])
            for i in range(1200):          # 120 secondi a 10 stampe/s
                drift = 0.6 if (i // 200) % 2 == 0 else -0.6
                price = max(1.0, price + drift + rng.gauss(0, 0.4))
                for _ in range(rng.randint(8, 14)):
                    w.writerow([t0 + i * 100, round(price, 2), 0.05,
                                "BUY" if drift > 0 else "SELL"])

        saved_clock = _clock
        try:
            c = Config(source="csv", csv_path=csv_path, strategy="burst15",
                       db_path=db_path, payout=0.8, http_port=0, quiet=True,
                       auto_retrain=False, min_warmup_s=15.0,
                       burst_n5_min=20, burst_r10_min_bps=0.5,
                       burst_cooldown_ms=3000, feature_interval_ms=100)
            engine = Engine(c)
            engine.run()
            counters = dict(engine.signals.counters)
            store = Store(c)
            trades = store.paper_trades()
            counts = store.counts()
            shadow = shadow_report(store)
            replay = burst_replay(store, c)
            store.stop()
        finally:
            set_clock(saved_clock)

        assert counters["signals"] >= 2, f"segnali emessi: {counters}"
        assert counters["triggered"] >= 2, "ingressi non eseguiti"
        settled = counters["wins"] + counters["losses"] + counters["ties"]
        assert settled >= 2, f"nessun esito registrato: {counters}"
        assert counts["features"] > 500, f"feature non persistite: {counts}"
        assert len(trades) >= 2, "paper trade non scritti sul database"
        assert all(t["entry_mode"] == "DELAY" for t in trades), \
            "BURST-15 deve entrare a tempo"
        assert shadow["status"] == "OK", f"shadow: {shadow}"
        assert replay["status"] == "COMPLETO", f"replay: {replay.get('status')}"
        return (f"{counters['signals']} segnali, {settled} esiti, "
                f"{counts['features']} feature, {counts['shadow_decisions']} shadow, "
                f"replay {replay['trades']} trade")

    check("websocket (framing, frammenti, ping, lunghezze)", t_ws)
    check("order book (sequenza e desync)", t_book)
    check("feature engine (r10, n5, ofi)", t_features)
    check("burst-15 (trigger e sessione)", t_burst)
    check("cancelli e soglie", t_gates)
    check("database sqlite", t_db)
    check("apprendimento (walk-forward, calibrazione, OOD)", t_learning)
    check("statistica", t_stats)
    check("motore end-to-end", t_engine)

    print(f"\nAURUM ENGINE {VERSION} - selftest\n")
    failed = 0
    for name, ok, detail in results:
        mark = "  OK  " if ok else " FAIL "
        print(f"[{mark}] {name}\n         {detail}")
        failed += int(not ok)
    print(f"\n{len(results) - failed}/{len(results)} verifiche superate")
    return 1 if failed else 0


# --------------------------------------------------------------------------- #
#  CLI
# --------------------------------------------------------------------------- #

COMMANDS: dict[str, Callable[[Config, Any], int]] = {
    "run": cmd_run,
    "check": cmd_check,
    "stats": cmd_stats,
    "backtest": cmd_backtest,
    "burst": cmd_burst,
    "burst-grid": cmd_burst_grid,
    "shadow": cmd_shadow,
    "config": cmd_config,
    "selftest": cmd_selftest,
}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="aurum_engine.py",
        description=f"AURUM ENGINE {VERSION} - motore completo, un file solo, "
                    f"solo carta.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""esempi:
  %(prog)s check                          il venue e' raggiungibile?
  %(prog)s run                            live su Binance, dashboard su :8000
  %(prog)s run --strategy burst15 --payout 0.8
  %(prog)s run --source sim               simulatore, senza rete
  %(prog)s run --source csv --csv trades.csv
  %(prog)s run --proxy http://127.0.0.1:3128
  %(prog)s stats                          statistiche del paper trading
  %(prog)s backtest                       walk-forward su cio' che ha registrato
  %(prog)s burst                          replay di BURST-15, sessioni incluse
  %(prog)s shadow                         anche le finestre NON tradate
  %(prog)s selftest                       verifica il file su se stesso
""")
    p.add_argument("command", choices=sorted(COMMANDS))
    p.add_argument("--source", choices=["binance", "coinbase", "sim", "csv"])
    p.add_argument("--symbol")
    p.add_argument("--strategy", choices=["ensemble", "burst15"])
    p.add_argument("--horizon", type=float, help="orizzonte in secondi")
    p.add_argument("--payout", type=float,
                   help="payout del broker, es. 0.8. Senza, nessun P&L monetario")
    p.add_argument("--stake", type=float)
    p.add_argument("--db", help="percorso del database SQLite")
    p.add_argument("--proxy", help="http://host:porta per REST e WebSocket")
    p.add_argument("--csv", help="file di trade per --source csv")
    p.add_argument("--port", type=int, help="porta della dashboard (0 = niente)")
    p.add_argument("--host", help="indirizzo di ascolto della dashboard")
    p.add_argument("--warmup", type=float, help="secondi di riscaldamento")
    p.add_argument("--n5", type=int, help="BURST-15: trade minimi in 5s")
    p.add_argument("--r10", type=float, help="BURST-15: movimento minimo a 10s in bps")
    p.add_argument("--no-ofi", action="store_true",
                   help="BURST-15: non richiedere l'accordo del flusso")
    p.add_argument("--no-learn", action="store_true",
                   help="disattiva il riaddestramento automatico")
    p.add_argument("--include-synthetic", action="store_true",
                   help="INQUINA il report: le righe del simulatore non sono mercato")
    p.add_argument("--min-trades", type=int, default=30, help="soglia per burst-grid")
    p.add_argument("--json", action="store_true", help="una riga JSON per evento")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--seed", type=int, help="seme del simulatore")
    p.add_argument("--timeout", type=float, default=8.0,
                   help="timeout di rete per `check`, in secondi")
    return p


def apply_args(cfg: Config, args) -> Config:
    for attr, name in (("source", "source"), ("symbol", "symbol"),
                       ("strategy", "strategy"), ("db", "db_path"),
                       ("proxy", "proxy"), ("csv", "csv_path"),
                       ("host", "http_host")):
        value = getattr(args, attr, None)
        if value is not None:
            setattr(cfg, name, value)
    if args.horizon is not None:
        cfg.horizon_s = args.horizon
    if args.payout is not None:
        cfg.payout = args.payout
    if args.stake is not None:
        cfg.stake = args.stake
    if args.port is not None:
        cfg.http_port = args.port
    if args.warmup is not None:
        cfg.min_warmup_s = args.warmup
    if args.n5 is not None:
        cfg.burst_n5_min = args.n5
    if args.r10 is not None:
        cfg.burst_r10_min_bps = args.r10
    if args.no_ofi:
        cfg.burst_require_ofi_agree = False
    if args.no_learn:
        cfg.auto_retrain = False
    if args.seed is not None:
        cfg.sim_seed = args.seed
    cfg.json_out = bool(args.json)
    cfg.quiet = bool(args.quiet) or cfg.json_out
    if cfg.source == "csv" and not cfg.csv_path:
        raise SystemExit("--source csv richiede --csv percorso/trades.csv")
    cfg.__post_init__()
    return cfg


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = apply_args(Config.from_env(), args)
    return COMMANDS[args.command](cfg, args)


if __name__ == "__main__":
    sys.exit(main())












