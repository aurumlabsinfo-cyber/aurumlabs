"""Sorgente SIMULATA. Non e' mercato, e il sistema non lo lascia dimenticare.

Serve a far girare la macchina senza rete e senza chiavi: selftest, sviluppo,
prova del frontend. Ogni riga prodotta e' marcata `mode="SIMULATION"` fino al
database e fino allo schermo, e la ricerca esclude quelle righe: un vantaggio
"dimostrato" su dati generati da un modello non e' un vantaggio.

I parametri riproducono la scala di EUR/USD, non quella di una criptovaluta:
prezzo attorno a 1.085 e sigma di pochi punti base al minuto. Un simulatore
con la volatilita' sbagliata esercita i cancelli su uno strumento che non
esiste, e "funziona in simulazione" non vorrebbe dire niente.
"""

from __future__ import annotations

import asyncio
import math
import random
from typing import AsyncIterator

from ..config import MODE_SIMULATION
from .base import AdapterCapabilities, MarketDataAdapter, Quote, now_ms


class SimulationAdapter(MarketDataAdapter):
    """Cammino con micro-trend alternati, alla scala giusta per EUR/USD."""

    name = "simulation"
    mode = MODE_SIMULATION

    #: Sigma per passo, in punti base, TARATA sul cambio vero: con questi due
    #: numeri la deviazione dei ritorni a 60 secondi misura ~3 bps, che e'
    #: l'ordine di grandezza di EUR/USD. Valori piu' alti (quelli tipici di una
    #: criptovaluta) renderebbero il simulatore inutile: i cancelli sul
    #: movimento atteso passerebbero sempre, e "funziona in simulazione" non
    #: direbbe piu' niente sul comportamento reale.
    VOL_BPS_PER_STEP = 0.05
    DRIFT_MULTIPLIER = 0.25
    #: Mezzo spread tipico, in punti base (~0.5 pip su EUR/USD).
    HALF_SPREAD_BPS = 0.23

    def __init__(self, cfg, seed: int | None = None, interval_ms: int = 100) -> None:
        super().__init__(cfg)
        self.capabilities = AdapterCapabilities(
            has_bid_ask=True, tick_level=True, typical_interval_ms=interval_ms,
            note="DATI GENERATI DA UN MODELLO: non descrivono il mercato reale")
        self.rng = random.Random(seed)
        self.price = 1.08500
        self.drift = 0.0
        self.interval = max(0.01, interval_ms / 1000.0)

    async def connect(self) -> bool:
        self.health_state.connected = True
        self.health_state.last_error = None
        return True

    def step(self, dt: float = 0.1, ts: int | None = None) -> Quote:
        """Un passo del cammino.

        `ts` esplicito rende il simulatore utilizzabile OFFLINE: senza, mille
        passi generati in un ciclo stretto avrebbero tutti lo stesso istante di
        orologio, le finestre temporali resterebbero vuote e ogni feature
        varrebbe None. Un simulatore che si puo' girare solo in tempo reale non
        serve ne' al selftest ne' al backtest.
        """
        self.drift = 0.97 * self.drift + self.rng.gauss(0, 0.35)
        sigma = self.price * (self.VOL_BPS_PER_STEP / 10_000.0) * math.sqrt(dt / 0.1)
        self.price = max(0.5, self.price + self.drift * sigma * self.DRIFT_MULTIPLIER
                         + self.rng.gauss(0, sigma))
        tick = self.cfg.tick_size
        self.price = round(round(self.price / tick) * tick, 8)
        half = max(tick, self.price * self.HALF_SPREAD_BPS / 10_000.0)
        bid = round(round((self.price - half) / tick) * tick, 8)
        ask = round(round((self.price + half) / tick) * tick, 8)
        stamp = ts if ts is not None else now_ms()
        q = Quote(ts=stamp, received_ts=stamp, source=self.name, mode=self.mode,
                  mid=self.price, bid=bid, ask=ask, last=self.price)
        self.health_state.last_quote_ts = stamp
        self.health_state.quotes += 1
        return q

    async def quotes(self) -> AsyncIterator[Quote]:
        while True:
            yield self.step(self.interval)
            await asyncio.sleep(self.interval)
