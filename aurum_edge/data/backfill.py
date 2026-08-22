"""Ricostruzione dello storico dagli endpoint pubblici di Bybit.

Cosa si puo' ricostruire davvero, e cosa no. La distinzione e' il punto:

RICOSTRUIBILE ALL'INDIETRO (Bybit lo serve come storico)
  * prezzo: open/high/low/close al minuto -> e quindi il prezzo iniziale e il
    prezzo a +5, +10, +15, +20, +30, +45, +60 minuti, cioe' tutte le etichette;
  * volume in base e in quote;
  * volatilita', RSI, MACD, VWAP, EMA, ATR, Bollinger, momentum: sono funzioni
    delle barre, quindi si ricostruiscono esattamente;
  * open interest, sul passo a 5 minuti, per l'archivio che Bybit conserva;
  * funding, otto ore alla volta, con storia lunga;
  * rapporto conti long/short, archivio corto;
  * contesto ETH e SOL, stessa griglia.

NON RICOSTRUIBILE ALL'INDIETRO (nessun endpoint storico pubblico)
  * taker buy/sell e CVD: `recent-trade` da solo l'ultimo migliaio di scambi;
  * squilibrio del libro: il libro e' una fotografia dell'istante;
  * liquidazioni: solo WebSocket, senza storico.

Queste tre restano vuote nello storico e si riempiono in avanti mentre il
collector gira. E' scomodo e va detto: significa che gli edge di flusso non
sono studiabili sui sei mesi passati, ma solo da quando si e' iniziato a
raccogliere. L'alternativa — dedurre il taker flow dal segno della barra — e'
una finzione che alza l'accuratezza in backtest e sparisce in avanti, e questo
programma non la usa.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .. import config
from ..util import timeutil
from ..util.http import FetchError
from .bybit import BybitPublic
from .store import Store

Progress = Callable[[str], None]


def _noop(_: str) -> None:
    pass


@dataclass
class BackfillReport:
    symbol: str
    started_ts: int
    ended_ts: int | None = None
    bars: dict[str, int] = field(default_factory=dict)
    open_interest: int = 0
    funding: int = 0
    account_ratio: int = 0
    span: dict[str, Any] = field(default_factory=dict)
    gaps: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    unavailable: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "started": timeutil.iso(self.started_ts),
            "ended": timeutil.iso(self.ended_ts),
            "bars": self.bars,
            "open_interest_rows": self.open_interest,
            "funding_rows": self.funding,
            "account_ratio_rows": self.account_ratio,
            "span": self.span,
            "gaps": self.gaps,
            "errors": self.errors,
            "unavailable": self.unavailable,
        }


def backfill_klines(client: BybitPublic, store: Store, symbol: str,
                    start_ms: int, end_ms: int, interval: str = "1",
                    progress: Progress = _noop) -> int:
    """Pagina all'indietro nel tempo finche' l'archivio risponde.

    Si pagina **all'indietro** e non in avanti perche' Bybit risponde con le
    ultime `limit` barre precedenti a `end`: partire dalla fine e arretrare
    tocca ogni pagina una volta sola. Andando in avanti servirebbe indovinare
    la larghezza esatta di ogni pagina, e ogni buco di mercato sfaserebbe il
    passo.
    """
    step_ms = 60_000 * int(interval) if interval.isdigit() else 60_000
    cursor = end_ms
    total = 0
    empty_pages = 0

    while cursor > start_ms:
        page_start = max(start_ms, cursor - step_ms * 1000)
        try:
            bars = client.klines(symbol, interval, start_ms=page_start,
                                 end_ms=cursor, limit=1000, closed_only=True)
        except FetchError as exc:
            raise
        if not bars:
            empty_pages += 1
            # Due pagine vuote di fila significano che l'archivio finisce qui.
            if empty_pages >= 2:
                break
            cursor = page_start - 1
            continue
        empty_pages = 0
        total += store.upsert_bars(symbol, bars)
        oldest = bars[0].start_ms
        progress(f"  {symbol} {interval}m: {total} barre, risalito a "
                 f"{timeutil.iso(oldest)}")
        if oldest <= start_ms:
            break
        # Se una pagina non ha fatto arretrare il cursore, si e' toccato il
        # fondo dell'archivio: continuare girerebbe a vuoto.
        if oldest >= cursor:
            break
        cursor = oldest - 1
    return total


def backfill_open_interest(client: BybitPublic, store: Store, symbol: str,
                           start_ms: int, end_ms: int, interval: str = "5min",
                           progress: Progress = _noop) -> int:
    step = {"5min": 300_000, "15min": 900_000, "30min": 1_800_000,
            "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}.get(
                interval, 300_000)
    cursor = end_ms
    total = 0
    empty = 0
    while cursor > start_ms:
        page_start = max(start_ms, cursor - step * 200)
        points = client.open_interest(symbol, interval, start_ms=page_start,
                                      end_ms=cursor, limit=200)
        if not points:
            empty += 1
            if empty >= 2:
                break
            cursor = page_start - 1
            continue
        empty = 0
        total += store.upsert_open_interest(symbol, interval, points)
        oldest = points[0].ts_ms
        progress(f"  {symbol} OI {interval}: {total} punti, risalito a "
                 f"{timeutil.iso(oldest)}")
        if oldest <= start_ms or oldest >= cursor:
            break
        cursor = oldest - 1
    return total


def backfill_funding(client: BybitPublic, store: Store, symbol: str,
                     start_ms: int, end_ms: int,
                     progress: Progress = _noop) -> int:
    cursor = end_ms
    total = 0
    empty = 0
    while cursor > start_ms:
        page_start = max(start_ms, cursor - 8 * timeutil.HOUR_MS * 200)
        points = client.funding_history(symbol, start_ms=page_start,
                                        end_ms=cursor, limit=200)
        if not points:
            empty += 1
            if empty >= 2:
                break
            cursor = page_start - 1
            continue
        empty = 0
        total += store.upsert_funding(symbol, points)
        oldest = points[0].ts_ms
        progress(f"  {symbol} funding: {total} punti, risalito a "
                 f"{timeutil.iso(oldest)}")
        if oldest <= start_ms or oldest >= cursor:
            break
        cursor = oldest - 1
    return total


def find_gaps(store: Store, symbol: str, start_ms: int, end_ms: int,
              step_ms: int = 60_000, max_report: int = 20) -> list[dict[str, Any]]:
    """Buchi nella griglia. Un buco non e' un errore, ma va saputo.

    Bybit non emette una barra quando in quel minuto non c'e' stato uno
    scambio, e i fermi di manutenzione lasciano vuoti piu' larghi. Il costruttore
    di feature non deve attraversarli come se non ci fossero: una finestra a
    cavallo di un buco di due ore non e' una finestra di due ore.
    """
    rows = store.bars(symbol, start_ms=start_ms, end_ms=end_ms)
    gaps: list[dict[str, Any]] = []
    prev: int | None = None
    for row in rows:
        ts = row["ts"]
        if prev is not None and ts - prev > step_ms:
            missing = (ts - prev) // step_ms - 1
            if missing > 0:
                gaps.append({
                    "from": timeutil.iso(prev), "to": timeutil.iso(ts),
                    "from_ms": prev, "to_ms": ts, "missing_bars": missing,
                })
        prev = ts
    gaps.sort(key=lambda g: -g["missing_bars"])
    return gaps[:max_report]


def run(days: int | None = None, store: Store | None = None,
        client: BybitPublic | None = None,
        symbols: list[str] | None = None,
        progress: Progress = _noop) -> BackfillReport:
    """Il backfill completo. Idempotente: rilanciarlo aggiorna, non duplica."""
    days = days or config.BACKFILL_DAYS
    store = store or Store()
    client = client or BybitPublic()
    symbol = config.SYMBOL
    symbols = symbols or [symbol, *config.CONTEXT_SYMBOLS]

    end_ms = timeutil.floor_ms(timeutil.now_ms(), 60_000)
    start_ms = end_ms - days * timeutil.DAY_MS
    report = BackfillReport(symbol=symbol, started_ts=timeutil.now_ms())
    report.unavailable = [
        "taker buy/sell e CVD: nessuno storico pubblico, si accumulano in avanti",
        "squilibrio del book: fotografia dell'istante, non ricostruibile",
        "liquidazioni: solo WebSocket su Bybit v5, nessuno storico",
    ]

    progress(f"Backfill {days} giorni: {timeutil.iso(start_ms)} -> "
             f"{timeutil.iso(end_ms)}")

    for sym in symbols:
        progress(f"[barre] {sym}")
        try:
            report.bars[sym] = backfill_klines(client, store, sym, start_ms,
                                               end_ms, "1", progress)
        except FetchError as exc:
            report.errors.append({"stage": f"klines:{sym}", **exc.to_dict()})
            progress(f"  ERRORE {sym}: {exc}")

    progress("[open interest]")
    try:
        report.open_interest = backfill_open_interest(
            client, store, symbol, start_ms, end_ms, "5min", progress)
    except FetchError as exc:
        report.errors.append({"stage": "open_interest", **exc.to_dict()})

    progress("[funding]")
    try:
        report.funding = backfill_funding(client, store, symbol, start_ms,
                                          end_ms, progress)
    except FetchError as exc:
        report.errors.append({"stage": "funding", **exc.to_dict()})

    progress("[rapporto conti long/short]")
    try:
        points = client.account_ratio(symbol, "5min", limit=500)
        report.account_ratio = store.upsert_account_ratio(symbol, "5min", points)
    except FetchError as exc:
        report.errors.append({"stage": "account_ratio", **exc.to_dict()})

    lo, hi, n = store.bar_span(symbol)
    report.span = {
        "from": timeutil.iso(lo), "to": timeutil.iso(hi), "bars": n,
        "days": round((hi - lo) / 86_400_000, 2) if lo and hi else 0.0,
    }
    if lo and hi:
        report.gaps = find_gaps(store, symbol, lo, hi)
    report.ended_ts = timeutil.now_ms()
    store.set_meta("last_backfill", report.to_dict())
    return report


def render(report: BackfillReport) -> str:
    d = report.to_dict()
    lines = [
        "=" * 72,
        "RICOSTRUZIONE STORICA",
        "=" * 72,
        f"Periodo in archivio : {d['span'].get('from')} -> {d['span'].get('to')}"
        f"  ({d['span'].get('days')} giorni, {d['span'].get('bars')} barre)",
        "Barre per simbolo   : " + ", ".join(
            f"{k}={v}" for k, v in d["bars"].items()) or "nessuna",
        f"Open interest       : {d['open_interest_rows']} punti (5min)",
        f"Funding             : {d['funding_rows']} punti",
        f"Long/short conti    : {d['account_ratio_rows']} punti",
        "",
    ]
    if d["gaps"]:
        lines.append(f"Buchi maggiori nella griglia ({len(d['gaps'])} mostrati):")
        for g in d["gaps"][:5]:
            lines.append(f"  {g['from']} -> {g['to']}  ({g['missing_bars']} barre)")
        lines.append("")
    lines.append("Non ricostruibile all'indietro:")
    for u in d["unavailable"]:
        lines.append(f"  - {u}")
    if d["errors"]:
        lines.append("")
        lines.append("Errori:")
        for e in d["errors"]:
            lines.append(f"  [{e.get('stage')}] {e.get('kind')}: {e.get('detail')}")
    return "\n".join(lines)
