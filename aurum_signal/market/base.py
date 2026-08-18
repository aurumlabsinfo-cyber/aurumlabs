"""Tipi canonici del mercato e interfaccia degli adapter.

Il motore non deve sapere da dove arrivano i dati. Sa che arrivano `Quote` con
un timestamp, e sa dichiarare quali campi sono **realmente** disponibili: un
adapter che non fornisce bid/ask lo dice, e gli agenti di microstruttura si
astengono invece di lavorare su numeri inventati.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, AsyncIterator


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(slots=True)
class Quote:
    """Una quotazione EUR/USD.

    `ts` e' il timestamp DELLA SORGENTE, `received_ts` quando l'abbiamo vista.
    La differenza e' la latenza del feed, ed e' un dato che il sistema usa: su
    un orizzonte da 60 secondi mezzo secondo di ritardo non e' un dettaglio.
    """

    ts: int
    received_ts: int
    source: str
    mode: str
    mid: float
    bid: float | None = None
    ask: float | None = None
    last: float | None = None

    @property
    def spread(self) -> float | None:
        if self.bid is None or self.ask is None:
            return None
        return self.ask - self.bid

    @property
    def spread_bps(self) -> float | None:
        s = self.spread
        if s is None or not self.mid:
            return None
        return s / self.mid * 10_000.0

    @property
    def latency_ms(self) -> float | None:
        """None quando la sorgente non manda un timestamp proprio.

        `None` significa "non misurabile", non "zero": mostrare 0 ms per un
        feed che non dichiara l'ora e' una bugia comoda.
        """
        if not self.ts:
            return None
        delta = self.received_ts - self.ts
        # Un orologio della sorgente in anticipo produrrebbe latenza negativa:
        # e' un dato sbagliato, non una latenza bassissima.
        return delta if -60_000 < delta < 600_000 else None

    @property
    def microprice(self) -> float | None:
        """Prezzo pesato dal lato del book. Richiede bid e ask reali."""
        if self.bid is None or self.ask is None:
            return None
        return (self.bid + self.ask) / 2.0

    def row(self) -> dict[str, Any]:
        return {
            "ts": self.ts, "received_ts": self.received_ts, "source": self.source,
            "mode": self.mode, "bid": self.bid, "ask": self.ask, "mid": self.mid,
            "last": self.last, "spread": self.spread, "spread_bps": self.spread_bps,
            "latency_ms": self.latency_ms,
        }


@dataclass
class AdapterCapabilities:
    """Cosa questo adapter fornisce DAVVERO.

    Serve a un principio preciso: non inventare un book di livello 2 su un
    feed FX che manda solo il prezzo. Se `has_bid_ask` e' falso, gli agenti
    che dipendono dallo spread si astengono e lo dichiarano.
    """

    has_bid_ask: bool = False
    has_volume: bool = False
    has_depth: bool = False
    tick_level: bool = False           # vero streaming tick, non barre
    typical_interval_ms: int = 1_000
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "has_bid_ask": self.has_bid_ask, "has_volume": self.has_volume,
            "has_depth": self.has_depth, "tick_level": self.tick_level,
            "typical_interval_ms": self.typical_interval_ms, "note": self.note,
        }


@dataclass
class AdapterHealth:
    name: str
    connected: bool = False
    mode: str = "LIVE"
    last_quote_ts: int | None = None
    last_error: str | None = None
    quotes: int = 0
    reconnects: int = 0
    latency_ms: float | None = None

    def age_ms(self) -> int | None:
        if self.last_quote_ts is None:
            return None
        return now_ms() - self.last_quote_ts

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name, "connected": self.connected, "mode": self.mode,
            "last_quote_ts": self.last_quote_ts, "age_ms": self.age_ms(),
            "last_error": self.last_error, "quotes": self.quotes,
            "reconnects": self.reconnects, "latency_ms": self.latency_ms,
        }


class MarketDataAdapter(ABC):
    """Interfaccia comune. Il motore centrale non dipende da un provider."""

    name: str = "base"
    mode: str = "LIVE"
    capabilities: AdapterCapabilities = field(default_factory=AdapterCapabilities)

    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.health_state = AdapterHealth(name=self.name, mode=self.mode)

    @abstractmethod
    async def connect(self) -> bool:
        """Ritorna True solo se la sorgente e' davvero utilizzabile."""

    @abstractmethod
    async def quotes(self) -> AsyncIterator[Quote]:
        """Flusso continuo di quotazioni. Non solleva: registra e riprova."""
        raise NotImplementedError
        yield  # pragma: no cover - rende la firma un generatore asincrono

    async def close(self) -> None:
        return None

    def health(self) -> dict[str, Any]:
        return {**self.health_state.to_dict(),
                "capabilities": self.capabilities.to_dict()}

    # ------------------------------------------------------------- utilita'
    def _make_quote(self, ts: int, mid: float, bid: float | None = None,
                    ask: float | None = None, last: float | None = None) -> Quote:
        q = Quote(ts=ts, received_ts=now_ms(), source=self.name, mode=self.mode,
                  mid=mid, bid=bid, ask=ask, last=last)
        self.health_state.last_quote_ts = q.received_ts
        self.health_state.quotes += 1
        self.health_state.latency_ms = q.latency_ms
        return q
