"""PASSAGGIO 1: cercare, aprire e giudicare lo storico del vecchio Aurum.

Questo modulo non "importa" ciecamente: apre ogni archivio che trova, conta le
righe vere, guarda l'intervallo temporale e poi **dice se serve a qualcosa**.
Il giudizio e' la parte importante, e segue tre criteri:

1. **E' lo stesso mercato?** Un modello su EUR/USD a 60 secondi non contiene
   informazione su BTCUSDT perpetual a 30 minuti. Non e' poco utile: e' un
   altro problema.
2. **E' un prezzo o un'opinione?** Barre, tick, volumi, open interest sono
   fatti riutilizzabili. Confidence, probabilita', punteggi di un vecchio
   motore sono opinioni gia' smentite o gia' confermate: riusarle come feature
   e' il modo piu' elegante di ereditare un errore.
3. **Ha un'etichetta ricostruibile senza il futuro?** Se da una riga non si
   puo' risalire al prezzo a +30 minuti guardando solo dati registrati, quella
   riga non e' addestrabile.

Le vecchie `confidence` non vengono mai riportate come verita': se il
riutilizzo e' possibile, vengono **rivalidate** contro l'esito reale, e quasi
sempre e' li' che muoiono.
"""

from __future__ import annotations

import gzip
import os
import shutil
import sqlite3
import tarfile
import tempfile
from dataclasses import dataclass, field
from typing import Any

from ..util import timeutil

# Tabelle il cui contenuto e' un FATTO di mercato: riutilizzabile se il
# simbolo coincide.
FACT_TABLES = {
    "market_ticks", "candles", "bars", "trades", "order_book_snapshots",
    "order_book_updates", "klines", "ticks",
}
# Tabelle che contengono OPINIONI di un vecchio motore: utili per capire cosa
# e' stato fatto, mai come feature di un modello nuovo.
OPINION_TABLES = {
    "decisions", "signals", "signal_updates", "preview_snapshots",
    "shadow_decisions", "features", "setups", "edges", "research_findings",
    "model_versions", "booster_rules", "wallet_cycles", "trades_executed",
}

SEARCH_NAMES = ("*.db", "*.sqlite", "*.sqlite3", "*.db.gz", "*.tar.gz",
                "*.db-wal", "*.log", "*.jsonl", "*.csv")


@dataclass
class ArchiveReport:
    path: str
    kind: str                    # SQLITE | GZIP | TAR | LOG | CSV | UNKNOWN
    size_bytes: int
    readable: bool = False
    error: str | None = None
    tables: dict[str, int] = field(default_factory=dict)
    time_span: dict[str, Any] = field(default_factory=dict)
    symbols: list[str] = field(default_factory=list)
    verdict: str = "DA VALUTARE"
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path, "kind": self.kind, "size_bytes": self.size_bytes,
            "readable": self.readable, "error": self.error,
            "tables": self.tables, "time_span": self.time_span,
            "symbols": self.symbols, "verdict": self.verdict,
            "reason": self.reason,
        }


def find_archives(roots: list[str]) -> list[str]:
    """Cerca ricorsivamente gli archivi nei percorsi dati."""
    import fnmatch

    found: list[str] = []
    skip_dirs = {".git", "node_modules", "__pycache__", ".venv", "venv",
                 ".next", "dist", "build", ".pytest_cache"}
    for root in roots:
        if not os.path.isdir(root):
            if os.path.isfile(root):
                found.append(root)
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in skip_dirs]
            for name in filenames:
                if any(fnmatch.fnmatch(name, pat) for pat in SEARCH_NAMES):
                    found.append(os.path.join(dirpath, name))
    return sorted(set(found))


def _classify(path: str) -> str:
    low = path.lower()
    if low.endswith((".db", ".sqlite", ".sqlite3")):
        return "SQLITE"
    if low.endswith(".db.gz"):
        return "GZIP"
    if low.endswith(".tar.gz") or low.endswith(".tgz"):
        return "TAR"
    if low.endswith(".log"):
        return "LOG"
    if low.endswith(".csv"):
        return "CSV"
    if low.endswith(".jsonl"):
        return "JSONL"
    return "UNKNOWN"


def _sqlite_report(db_path: str, report: ArchiveReport) -> None:
    """Apre in sola lettura e riassume. Non scrive mai sull'archivio originale."""
    uri = f"file:{os.path.abspath(db_path)}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        names = [r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        report.readable = True
        lo_all: int | None = None
        hi_all: int | None = None
        symbols: set[str] = set()
        for name in names:
            try:
                count = conn.execute(
                    f'SELECT COUNT(*) c FROM "{name}"').fetchone()["c"]
            except sqlite3.DatabaseError as exc:
                report.tables[name] = -1
                report.error = f"{name}: {exc}"
                continue
            report.tables[name] = count
            if count == 0:
                continue
            cols = {r["name"] for r in conn.execute(f'PRAGMA table_info("{name}")')}
            ts_col = next((c for c in ("ts", "entry_ts", "created_ts", "time",
                                       "timestamp", "started_ts")
                           if c in cols), None)
            if ts_col:
                try:
                    row = conn.execute(
                        f'SELECT MIN("{ts_col}") a, MAX("{ts_col}") b '
                        f'FROM "{name}"').fetchone()
                    a, b = row["a"], row["b"]
                    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                        a, b = int(a), int(b)
                        # Alcune tabelle vecchie usano secondi, altre ms.
                        if a and a < 10 ** 12:
                            a, b = a * 1000, b * 1000
                        lo_all = a if lo_all is None else min(lo_all, a)
                        hi_all = b if hi_all is None else max(hi_all, b)
                except sqlite3.DatabaseError:
                    pass
            if "symbol" in cols:
                try:
                    for r in conn.execute(
                            f'SELECT DISTINCT symbol FROM "{name}" LIMIT 20'):
                        if r["symbol"]:
                            symbols.add(str(r["symbol"]))
                except sqlite3.DatabaseError:
                    pass
        report.symbols = sorted(symbols)
        if lo_all and hi_all:
            report.time_span = {
                "from_ms": lo_all, "to_ms": hi_all,
                "from": timeutil.iso(lo_all), "to": timeutil.iso(hi_all),
                "days": round((hi_all - lo_all) / 86_400_000, 2),
            }
    finally:
        conn.close()


def _judge(report: ArchiveReport, target_symbol: str) -> None:
    """Il verdetto. Duro per progetto: quasi tutto lo storico vecchio si scarta."""
    if not report.readable:
        report.verdict = "SCARTARE"
        report.reason = ("Non apribile: " + (report.error or "formato non gestito")
                         + ". Un archivio che non si legge non si valida.")
        return

    rows = sum(c for c in report.tables.values() if c > 0)
    if rows == 0:
        report.verdict = "SCARTARE"
        report.reason = ("Schema presente ma zero righe. E' la struttura di un "
                         "motore mai eseguito, o eseguito e mai persistito.")
        return

    fact_rows = sum(c for t, c in report.tables.items()
                    if t in FACT_TABLES and c > 0)
    opinion_rows = sum(c for t, c in report.tables.items()
                       if t in OPINION_TABLES and c > 0)

    symbol_ok = (not report.symbols) or any(
        s.upper().replace("/", "").replace("-", "").startswith(target_symbol[:3])
        for s in report.symbols)

    if fact_rows == 0:
        # Un file di testo, CSV o JSONL non ha tabelle riconoscibili: la
        # scansione ne conta solo le righe. Dirgli che contiene "opinioni del
        # vecchio motore" e' falso — puo' essere un registro di WhatsApp o
        # l'export di un cliente — e un verdetto falso e' peggio di un verdetto
        # vago, perche' sembra un'analisi.
        if opinion_rows == 0:
            report.verdict = "NON PERTINENTE"
            report.reason = (
                f"{rows} righe di testo senza tabelle di mercato riconoscibili. "
                "La scansione sa contarle ma non sa interpretarle: questo file "
                "non riguarda il problema.")
            return
        report.verdict = "SOLO DIAGNOSTICA"
        report.reason = (
            f"{opinion_rows} righe di sole opinioni del vecchio motore "
            "(decisioni, segnali, confidence). Utili per capire cosa e' stato "
            "provato; inutilizzabili come feature, perche' sarebbero l'output "
            "di un modello dentro l'input di un altro.")
        return

    if not symbol_ok:
        report.verdict = "SCARTARE"
        report.reason = (
            f"Fatti di mercato presenti ({fact_rows} righe) ma su "
            f"{', '.join(report.symbols[:5])}, non su {target_symbol}. "
            "Un altro mercato e' un altro problema, non meno dati.")
        return

    days = report.time_span.get("days", 0.0)
    if days < 1:
        report.verdict = "INSUFFICIENTE"
        report.reason = (
            f"{fact_rows} righe di mercato su {days} giorni. A 30 minuti di "
            "orizzonte questo e' meno di 48 osservazioni indipendenti: non "
            "misura niente.")
        return

    report.verdict = "RIUTILIZZABILE (previa rivalidazione)"
    report.reason = (
        f"{fact_rows} righe di fatti di mercato su {days} giorni, simbolo "
        "compatibile. Le colonne di prezzo e volume si possono riallineare "
        "sulla griglia al minuto; ogni confidence storica va ricalcolata "
        "contro l'esito reale prima di essere creduta.")


def scan(roots: list[str], target_symbol: str = "BTCUSDT") -> dict[str, Any]:
    """Il rapporto completo del PASSAGGIO 1."""
    archives = find_archives(roots)
    reports: list[ArchiveReport] = []

    for path in archives:
        kind = _classify(path)
        try:
            size = os.path.getsize(path)
        except OSError:
            size = 0
        rep = ArchiveReport(path=path, kind=kind, size_bytes=size)

        try:
            if kind == "SQLITE":
                _sqlite_report(path, rep)
            elif kind == "GZIP":
                with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
                    with gzip.open(path, "rb") as src:
                        shutil.copyfileobj(src, tmp)
                    tmp_path = tmp.name
                try:
                    _sqlite_report(tmp_path, rep)
                finally:
                    os.unlink(tmp_path)
            elif kind == "TAR":
                with tarfile.open(path, "r:gz") as tar:
                    members = [m.name for m in tar.getmembers() if m.isfile()]
                rep.readable = True
                rep.tables = {"(membri archivio)": len(members)}
                rep.verdict = "DA ESTRARRE"
                rep.reason = (
                    "Archivio tar: contiene " + ", ".join(members[:6]) +
                    ("..." if len(members) > 6 else "") +
                    ". Estrarre e rilanciare la scansione sulla cartella.")
                reports.append(rep)
                continue
            elif kind in ("LOG", "CSV", "JSONL"):
                with open(path, "r", errors="replace") as fh:
                    lines = sum(1 for _ in fh)
                rep.readable = True
                rep.tables = {"(righe)": lines}
            else:
                rep.error = "estensione non gestita"
        except Exception as exc:                                # noqa: BLE001
            rep.error = f"{type(exc).__name__}: {exc}"

        _judge(rep, target_symbol)
        reports.append(rep)

    usable = [r for r in reports if r.verdict.startswith("RIUTILIZZABILE")]
    return {
        "scanned_roots": roots,
        "target_symbol": target_symbol,
        "archives_found": len(reports),
        "usable": len(usable),
        "reports": [r.to_dict() for r in reports],
        "conclusion": _conclusion(reports, usable, target_symbol),
    }


def _conclusion(reports: list[ArchiveReport], usable: list[ArchiveReport],
                symbol: str) -> str:
    if not reports:
        return (
            "Nessun archivio trovato nei percorsi indicati. Non e' un errore "
            "del programma: significa che lo storico non esiste su questa "
            "macchina. La ricostruzione va fatta dagli endpoint storici "
            f"pubblici di Bybit per {symbol}, che e' comunque la fonte "
            "migliore, perche' e' esattamente il mercato da prevedere.")
    if not usable:
        return (
            f"Trovati {len(reports)} archivi, nessuno riutilizzabile per "
            f"{symbol} a 30 minuti. Lo storico utile va costruito da zero "
            "dagli endpoint pubblici di Bybit.")
    # Il totale deve contare SOLO i fatti di mercato. Sommare tutte le tabelle
    # gonfia il numero di ordini di grandezza — le decisioni di un vecchio
    # motore sono centinaia di migliaia, i prezzi che ha registrato qualche
    # centinaio — e il titolo finirebbe per contraddire i verdetti riga per
    # riga, promettendo una miniera dove ci sono duecento righe.
    total = sum(sum(c for t, c in r.tables.items()
                    if t in FACT_TABLES and c > 0) for r in usable)
    span = max((r.time_span.get("days", 0.0) for r in usable), default=0.0)
    # A trenta minuti di orizzonte, un giorno di storia vale 48 osservazioni
    # indipendenti. E' il numero che decide se serve, non le righe.
    independent = int(span * 48)
    verdict = ("Puo' bastare per un primo studio."
               if independent >= 200 else
               "NON basta: serve comunque il backfill da Bybit.")
    return (
        f"{len(usable)} archivi su {len(reports)} contengono fatti di mercato "
        f"compatibili: {total} righe di prezzo/scambi su {span} giorni, cioe' "
        f"circa {independent} osservazioni indipendenti a 30 minuti. {verdict} "
        "Quello che si eredita sono i prezzi, non i giudizi: ogni confidence "
        "storica andrebbe comunque ricalcolata contro l'esito reale.")


def render(result: dict[str, Any]) -> str:
    """Il rapporto in testo, per il terminale."""
    lines = [
        "=" * 72,
        "PASSAGGIO 1 - ANALISI DELLO STORICO ESISTENTE",
        "=" * 72,
        f"Percorsi analizzati : {', '.join(result['scanned_roots'])}",
        f"Simbolo obiettivo   : {result['target_symbol']}",
        f"Archivi trovati     : {result['archives_found']}",
        f"Riutilizzabili      : {result['usable']}",
        "",
    ]
    for rep in result["reports"]:
        lines.append(f"[{rep['verdict']}] {rep['path']}  ({rep['kind']}, "
                     f"{rep['size_bytes']} byte)")
        if rep["tables"]:
            top = sorted(rep["tables"].items(), key=lambda kv: -kv[1])[:8]
            lines.append("    tabelle: " +
                         ", ".join(f"{k}={v}" for k, v in top))
        if rep["time_span"]:
            lines.append(f"    periodo: {rep['time_span'].get('from')} -> "
                         f"{rep['time_span'].get('to')} "
                         f"({rep['time_span'].get('days')} giorni)")
        if rep["symbols"]:
            lines.append("    simboli: " + ", ".join(rep["symbols"][:8]))
        lines.append(f"    motivo : {rep['reason']}")
        lines.append("")
    lines.append("-" * 72)
    lines.append("CONCLUSIONE")
    lines.append(result["conclusion"])
    lines.append("-" * 72)
    return "\n".join(lines)
