"""Riproduzione di dati registrati come se fossero live.

Il replay e' lo strumento che rende il resto verificabile: senza, ogni
affermazione sul comportamento del motore andrebbe presa sulla fiducia o
aspettata in tempo reale. Il tempo diventa virtuale — l'orologio segue i dati,
non il muro — cosi' un'ora di mercato si ripercorre in secondi e le finestre
da 60 secondi restano esattamente 60 secondi *di dati*.
"""

from __future__ import annotations

import asyncio
import csv
from pathlib import Path
from typing import AsyncIterator

from ..config import MODE_REPLAY
from .base import AdapterCapabilities, MarketDataAdapter, Quote


class VirtualClock:
    """Orologio che avanza con i dati.

    Il motore chiede sempre l'ora a questo oggetto invece che a `time.time()`:
    e' l'unico modo perche' scadenze, latenze e finestre abbiano senso quando
    un'ora di mercato viene ripercorsa in dieci secondi.
    """

    def __init__(self, start_ms: int = 0) -> None:
        self._ts = start_ms

    def now_ms(self) -> int:
        return self._ts

    def set(self, ts: int) -> None:
        # Non torna mai indietro: dati fuori ordine non devono far retrocedere
        # il tempo, o le scadenze gia' calcolate diventerebbero future.
        self._ts = max(self._ts, ts)


class ReplayAdapter(MarketDataAdapter):
    """Rilegge quotazioni da un database AURUM o da un CSV.

    CSV atteso: `ts,mid[,bid,ask]` — `ts` in millisecondi o secondi.
    """

    name = "replay"
    mode = MODE_REPLAY

    def __init__(self, cfg, source: str, speed: float = 50.0,
                 clock: VirtualClock | None = None) -> None:
        super().__init__(cfg)
        self.capabilities = AdapterCapabilities(
            has_bid_ask=False, tick_level=True, typical_interval_ms=100,
            note=f"replay da {source}")
        self.source = source
        self.speed = max(0.0, speed)     # 0 = piu' veloce possibile
        self.clock = clock or VirtualClock()
        self.rows: list[tuple[int, float, float | None, float | None]] = []

    # ------------------------------------------------------------ caricamento
    def load(self) -> int:
        path = Path(self.source)
        if not path.exists():
            raise FileNotFoundError(f"sorgente di replay non trovata: {path}")
        rows = (self._load_db(path) if path.suffix in (".db", ".sqlite", ".sqlite3")
                else self._load_csv(path))
        rows.sort(key=lambda r: r[0])
        self.rows = rows
        if rows:
            self.clock.set(rows[0][0])
            self.capabilities.has_bid_ask = rows[0][2] is not None
        return len(rows)

    def _load_csv(self, path: Path) -> list[tuple[int, float, float | None, float | None]]:
        out = []
        with path.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                try:
                    ts = int(float(row["ts"]))
                    mid = float(row.get("mid") or row.get("price"))
                except (KeyError, TypeError, ValueError):
                    continue
                if ts < 10_000_000_000:      # secondi -> millisecondi
                    ts *= 1000
                bid = float(row["bid"]) if row.get("bid") else None
                ask = float(row["ask"]) if row.get("ask") else None
                out.append((ts, mid, bid, ask))
        return out

    def _load_db(self, path: Path) -> list[tuple[int, float, float | None, float | None]]:
        import sqlite3
        conn = sqlite3.connect(str(path))
        conn.row_factory = sqlite3.Row
        try:
            # Le righe simulate non si riproducono come se fossero mercato.
            cur = conn.execute(
                "SELECT ts, mid, bid, ask FROM market_ticks "
                "WHERE mode != 'SIMULATION' AND mid IS NOT NULL ORDER BY ts ASC")
            return [(int(r["ts"]), float(r["mid"]),
                     r["bid"], r["ask"]) for r in cur]
        finally:
            conn.close()

    # ------------------------------------------------------------- streaming
    async def connect(self) -> bool:
        n = self.load()
        self.health_state.connected = n > 0
        if n == 0:
            self.health_state.last_error = "nessuna quotazione da riprodurre"
        return n > 0

    async def quotes(self) -> AsyncIterator[Quote]:
        previous_ts: int | None = None
        for ts, mid, bid, ask in self.rows:
            if self.speed > 0 and previous_ts is not None:
                delay = (ts - previous_ts) / 1000.0 / self.speed
                if delay > 0:
                    await asyncio.sleep(min(delay, 1.0))
            previous_ts = ts
            self.clock.set(ts)
            q = Quote(ts=ts, received_ts=ts, source=self.name, mode=self.mode,
                      mid=mid, bid=bid, ask=ask, last=mid)
            self.health_state.last_quote_ts = ts
            self.health_state.quotes += 1
            yield q
        self.health_state.connected = False
        self.exhausted = True
