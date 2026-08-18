"""Feed con riserva e controllo di qualita'.

Due responsabilita' che stanno bene insieme:

* **scegliere la sorgente**: prova gli adapter in ordine, tiene la prima che
  funziona, e passa alla successiva quando quella smette di rispondere;
* **giudicare cio' che arriva**: `DataQualityAgent` guarda ogni quotazione e
  decide se il flusso descrive ancora il presente.

Sulla qualita' vale un principio: **blocca solo cio' che e' rotto**. Un feed
morto o timestamp impossibili sono guasti e fermano tutto; uno spread un po'
largo o un aggiornamento lento sono informazioni che pesano nel punteggio, non
divieti. Riempire il sistema di cancelli duri e' il modo piu' rapido di
costruire un motore che non opera mai e non sa dire perche'.
"""

from __future__ import annotations

import asyncio
import statistics
from collections import deque
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

from ..config import MODE_LIVE
from .base import MarketDataAdapter, Quote, now_ms
from .realtime import ADAPTERS


@dataclass
class QualityReport:
    """Il verdetto sul flusso, con il motivo sempre a fianco del numero."""

    quality_score: float = 1.0
    feed_latency_ms: float | None = None
    age_ms: int | None = None
    stale: bool = False
    usable: bool = False
    #: Guasti veri: fermano il motore.
    blocking: list[str] = field(default_factory=list)
    #: Rilievi: pesano, non vietano.
    notes: list[str] = field(default_factory=list)
    duplicates: int = 0
    gaps: int = 0
    outliers: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "quality_score": round(self.quality_score, 3),
            "feed_latency_ms": (round(self.feed_latency_ms, 1)
                                if self.feed_latency_ms is not None else None),
            "age_ms": self.age_ms, "stale": self.stale, "usable": self.usable,
            "blocking": list(self.blocking), "notes": list(self.notes),
            "duplicates": self.duplicates, "gaps": self.gaps,
            "outliers": self.outliers,
        }


class DataQualityAgent:
    """Guarda il flusso e dice se e' utilizzabile, e perche' no."""

    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.last: Quote | None = None
        self._prices: deque[float] = deque(maxlen=200)
        self._intervals: deque[int] = deque(maxlen=200)
        self._latencies: deque[float] = deque(maxlen=200)
        self.duplicates = 0
        self.gaps = 0
        self.outliers = 0
        self.frozen_count = 0

    def observe(self, q: Quote) -> None:
        prev = self.last
        if prev is not None:
            if q.received_ts == prev.received_ts and q.mid == prev.mid:
                self.duplicates += 1
            gap = q.received_ts - prev.received_ts
            if gap > 0:
                self._intervals.append(gap)
                typical = statistics.median(self._intervals) if len(self._intervals) > 8 else gap
                if typical > 0 and gap > 8 * typical:
                    self.gaps += 1
            if q.mid == prev.mid:
                self.frozen_count += 1
            else:
                self.frozen_count = 0
            # Un salto enorme fra due quotazioni consecutive e' quasi sempre un
            # dato sbagliato, non un movimento: si conta e si dichiara.
            if prev.mid > 0:
                move_bps = abs(q.mid - prev.mid) / prev.mid * 10_000.0
                if move_bps > 100.0:
                    self.outliers += 1
        if q.latency_ms is not None:
            self._latencies.append(q.latency_ms)
        self._prices.append(q.mid)
        self.last = q

    def assess(self, now: int | None = None) -> QualityReport:
        cfg = self.cfg
        now = now or now_ms()
        rep = QualityReport(duplicates=self.duplicates, gaps=self.gaps,
                            outliers=self.outliers)
        if self.last is None:
            rep.quality_score = 0.0
            rep.blocking.append("NO_DATA")
            return rep

        rep.age_ms = now - self.last.received_ts
        rep.feed_latency_ms = (statistics.median(self._latencies)
                               if self._latencies else None)

        score = 1.0
        # --- guasti veri -----------------------------------------------------
        if self.last.mid <= 0:
            rep.blocking.append("INVALID_PRICE")
        if self.last.ts and abs(self.last.received_ts - self.last.ts) > 600_000:
            rep.blocking.append("INVALID_TIMESTAMP")
        if rep.age_ms > cfg.max_quote_age_ms:
            rep.stale = True
            rep.blocking.append("DATA_STALE")
        if (rep.feed_latency_ms is not None
                and rep.feed_latency_ms > cfg.max_feed_latency_ms):
            rep.blocking.append("FEED_LATENCY_HIGH")
        # Un prezzo identico per centinaia di aggiornamenti non e' un mercato
        # calmo: e' un feed congelato.
        if self.frozen_count > 200:
            rep.blocking.append("FEED_FROZEN")

        # --- rilievi che pesano ma non vietano -------------------------------
        if rep.age_ms > cfg.max_quote_age_ms / 2:
            score -= 0.15
            rep.notes.append("aggiornamenti radi")
        if rep.feed_latency_ms is None:
            rep.notes.append("latenza non misurabile su questa sorgente")
        elif rep.feed_latency_ms > cfg.max_feed_latency_ms / 2:
            score -= 0.15
            rep.notes.append("latenza elevata")
        if self.duplicates > 0 and len(self._prices) > 20:
            dup_ratio = self.duplicates / max(1, len(self._prices))
            if dup_ratio > 0.3:
                score -= 0.1
                rep.notes.append("molte quotazioni duplicate")
        if self.gaps > 0:
            score -= min(0.2, 0.02 * self.gaps)
            rep.notes.append(f"{self.gaps} interruzioni nel flusso")
        if self.outliers > 0:
            score -= min(0.2, 0.05 * self.outliers)
            rep.notes.append(f"{self.outliers} valori anomali scartati")

        rep.quality_score = max(0.0, min(1.0, score))
        rep.usable = not rep.blocking and rep.quality_score >= 0.5
        return rep


class MarketFeed:
    """Sceglie l'adapter, sorveglia il flusso, pubblica quotazioni pulite."""

    def __init__(self, cfg, adapter: MarketDataAdapter | None = None) -> None:
        self.cfg = cfg
        self.quality = DataQualityAgent(cfg)
        self.adapter: MarketDataAdapter | None = adapter
        self.tried: list[dict[str, Any]] = []
        self.last_quote: Quote | None = None
        self.mode = adapter.mode if adapter else MODE_LIVE

    async def connect(self) -> bool:
        """Prova gli adapter in ordine; il primo che funziona vince."""
        if self.adapter is not None:
            ok = await self.adapter.connect()
            self.mode = self.adapter.mode
            self.tried.append({"name": self.adapter.name, "connected": ok,
                               "error": self.adapter.health_state.last_error})
            return ok
        for name in self.cfg.adapter_names:
            cls = ADAPTERS.get(name)
            if cls is None:
                self.tried.append({"name": name, "connected": False,
                                   "error": "adapter sconosciuto"})
                continue
            candidate = cls(self.cfg)
            try:
                ok = await candidate.connect()
            except Exception as exc:  # noqa: BLE001
                ok = False
                candidate.health_state.last_error = f"{type(exc).__name__}: {exc}"
            self.tried.append({"name": name, "connected": ok,
                               "error": candidate.health_state.last_error})
            if ok:
                self.adapter = candidate
                self.mode = candidate.mode
                return True
        return False

    async def quotes(self) -> AsyncIterator[Quote]:
        if self.adapter is None:
            raise RuntimeError("nessun adapter connesso")
        async for q in self.adapter.quotes():
            self.quality.observe(q)
            self.last_quote = q
            yield q

    def health(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "adapter": self.adapter.health() if self.adapter else None,
            "tried": self.tried,
            "quality": self.quality.assess().to_dict(),
            "last_price": self.last_quote.mid if self.last_quote else None,
            "bid": self.last_quote.bid if self.last_quote else None,
            "ask": self.last_quote.ask if self.last_quote else None,
            "spread_bps": (self.last_quote.spread_bps if self.last_quote else None),
        }
