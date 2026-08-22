"""Dati finti, per provare il codice — mai per provare un vantaggio.

Serve una distinzione netta, perche' confonderla e' il modo piu' rapido di
mentire con un backtest:

* i dati di questo file provano che **il codice gira**: che le serie si
  allineano, che le etichette si attaccano, che il modello converge, che la
  dashboard risponde;
* non provano niente sul mercato. Sono un cammino casuale con un po' di
  struttura addosso.

Anzi, l'uso piu' importante e' l'opposto: il cammino casuale **puro** e' il
banco di prova della validazione. Se il motore di ricerca trova un vantaggio
dentro un cammino casuale, il motore e' rotto, e questo si puo' verificare
automaticamente. E' il test piu' severo del progetto.
"""

from __future__ import annotations

import math
import random
from typing import Any

from ..data.bybit import FundingPoint, Kline, OpenInterestPoint
from ..util import timeutil


def random_walk_klines(*, n: int = 6000, start_price: float = 60_000.0,
                       start_ms: int | None = None, seed: int = 7,
                       sigma_bps: float = 6.0, drift_bps: float = 0.0,
                       vol_clustering: bool = True) -> list[Kline]:
    """Cammino casuale con volatilita' a grappoli e volumi a code grasse.

    Il raggruppamento della volatilita' c'e' perche' senza di esso ogni
    indicatore di regime resterebbe piatto e i test non toccherebbero mai i
    rami che contano. La direzione, invece, resta **imprevedibile per
    costruzione**: e' esattamente cio' che si vuole verificare.
    """
    rng = random.Random(seed)
    start_ms = start_ms or (timeutil.floor_ms(timeutil.now_ms(), 60_000)
                            - n * 60_000)
    out: list[Kline] = []
    price = start_price
    vol_state = 1.0
    for i in range(n):
        if vol_clustering:
            # Processo autoregressivo sul logaritmo della volatilita'.
            vol_state = math.exp(0.97 * math.log(vol_state) +
                                 rng.gauss(0.0, 0.08))
            vol_state = min(max(vol_state, 0.35), 3.0)
        step = rng.gauss(drift_bps, sigma_bps * vol_state) / 10_000.0
        open_ = price
        close = max(1.0, price * (1.0 + step))
        wick = abs(rng.gauss(0.0, sigma_bps * vol_state * 0.6)) / 10_000.0
        high = max(open_, close) * (1.0 + wick)
        low = min(open_, close) * (1.0 - wick)
        base_vol = math.exp(rng.gauss(1.2, 0.7)) * vol_state
        turnover = base_vol * (high + low + close) / 3.0
        out.append(Kline(start_ms + i * 60_000, open_, high, low, close,
                         round(base_vol, 4), round(turnover, 2)))
        price = close
    return out


def klines_with_pattern(*, n: int = 6000, seed: int = 11,
                        edge_strength: float = 0.6) -> list[Kline]:
    """Un cammino casuale con dentro una regolarita' vera, piantata di proposito.

    La regola: dopo tre barre consecutive nello stesso verso accompagnate da
    volume alto, la barra successiva prosegue con probabilita' maggiore di
    meta'. Serve al test opposto rispetto al cammino puro — verificare che il
    motore, quando un vantaggio c'e' davvero, riesca a **trovarlo**. Un
    validatore che non promuove mai niente e' inutile quanto uno che promuove
    tutto.
    """
    rng = random.Random(seed)
    base = random_walk_klines(n=n, seed=seed, vol_clustering=True)
    out: list[Kline] = []
    price = base[0].open
    run = 0
    high_vol = False
    for i, k in enumerate(base):
        step = (k.close / k.open - 1.0)
        if run != 0 and high_vol and rng.random() < 0.5 + edge_strength / 2.0:
            step = abs(step) * (1 if run > 0 else -1) * 1.4
        open_ = price
        close = max(1.0, price * (1.0 + step))
        wick = abs(k.high - k.low) / max(k.close, 1.0) * 0.5
        high = max(open_, close) * (1.0 + wick)
        low = min(open_, close) * (1.0 - wick)
        out.append(Kline(k.start_ms, open_, high, low, close, k.volume,
                         k.volume * close))
        price = close
        if close > open_:
            run = run + 1 if run > 0 else 1
        elif close < open_:
            run = run - 1 if run < 0 else -1
        else:
            run = 0
        run = max(-3, min(3, run))
        high_vol = k.volume > 4.0 and abs(run) >= 3
    return out


def klines_with_horizon_edge(*, n: int = 40_000, seed: int = 23,
                             drift_bps: float = 3.0, sigma_bps: float = 6.0,
                             switch_prob: float = 1 / 300.0) -> list[Kline]:
    """Un cammino con dentro una regolarita' vera **sull'orizzonte giusto**.

    Un regime nascosto (rialzo, ribasso, neutro) cambia raramente e imprime una
    deriva costante. Poiche' dura in media trecento barre, il passato recente lo
    rivela: il rendimento delle ultime ore e' informativo su quello della
    prossima mezz'ora. E' un vantaggio autentico, di quelli che un motore di
    ricerca **deve** trovare.

    Questo e' il gemello del cammino casuale puro, e insieme formano la coppia
    di prove che serve: uno verifica che il motore non inventi vantaggi, l'altro
    che non li perda. Un validatore severo che boccia tutto e' inutile quanto
    uno permissivo che promuove tutto, e senza la seconda prova non si
    distinguono.
    """
    rng = random.Random(seed)
    start_ms = timeutil.floor_ms(timeutil.now_ms(), 60_000) - n * 60_000
    out: list[Kline] = []
    price = 60_000.0
    regime = 0
    vol_state = 1.0
    for i in range(n):
        if rng.random() < switch_prob:
            regime = rng.choice((-1, 0, 1))
        vol_state = min(max(math.exp(0.97 * math.log(vol_state) +
                                     rng.gauss(0.0, 0.08)), 0.4), 2.5)
        step = (rng.gauss(regime * drift_bps, sigma_bps * vol_state)) / 10_000.0
        open_ = price
        close = max(1.0, price * (1.0 + step))
        wick = abs(rng.gauss(0.0, sigma_bps * vol_state * 0.5)) / 10_000.0
        high = max(open_, close) * (1.0 + wick)
        low = min(open_, close) * (1.0 - wick)
        vol = math.exp(rng.gauss(1.2, 0.7)) * vol_state
        out.append(Kline(start_ms + i * 60_000, open_, high, low, close,
                         round(vol, 4), round(vol * close, 2)))
        price = close
    return out


def open_interest_points(klines: list[Kline], *, seed: int = 13
                         ) -> list[OpenInterestPoint]:
    """OI a passo 5 minuti, correlato ai movimenti ma non identico a essi."""
    rng = random.Random(seed)
    out: list[OpenInterestPoint] = []
    oi = 50_000.0
    for i in range(0, len(klines), 5):
        k = klines[i]
        drift = (k.close / k.open - 1.0) * 4_000.0
        oi = max(1_000.0, oi * (1.0 + rng.gauss(drift / 10_000.0, 0.0015)))
        out.append(OpenInterestPoint(k.start_ms, round(oi, 2)))
    return out


def funding_points(klines: list[Kline], *, seed: int = 17) -> list[FundingPoint]:
    """Funding ogni otto ore, con la media leggermente positiva come nella realta'."""
    rng = random.Random(seed)
    out: list[FundingPoint] = []
    if not klines:
        return out
    step = 8 * timeutil.HOUR_MS
    t = timeutil.floor_ms(klines[0].start_ms, step)
    end = klines[-1].start_ms
    while t <= end:
        out.append(FundingPoint(t, round(rng.gauss(0.0001, 0.00008), 8)))
        t += step
    return out


def seed_store(store: Any, *, n: int = 6000, seed: int = 7,
               with_pattern: bool = False, horizon_edge: bool = False,
               symbol: str = "BTCUSDT",
               context: bool = True) -> dict[str, int]:
    """Riempie un archivio con dati finti. Solo per i test."""
    if horizon_edge:
        klines = klines_with_horizon_edge(n=n, seed=seed)
    elif with_pattern:
        klines = klines_with_pattern(n=n, seed=seed)
    else:
        klines = random_walk_klines(n=n, seed=seed)
    counts = {"bars": store.upsert_bars(symbol, klines)}
    counts["oi"] = store.upsert_open_interest(
        symbol, "5min", open_interest_points(klines, seed=seed + 1))
    counts["funding"] = store.upsert_funding(
        symbol, funding_points(klines, seed=seed + 2))
    if context:
        for offset, sym in enumerate(("ETHUSDT", "SOLUSDT"), start=1):
            ctx = random_walk_klines(
                n=n, seed=seed + 100 + offset,
                start_price=3_000.0 / offset,
                start_ms=klines[0].start_ms)
            counts[sym] = store.upsert_bars(sym, ctx)
    return counts
