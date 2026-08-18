"""Buffer circolari e statistiche incrementali.

Il vincolo che decide questo file: il motore valuta piu' volte al secondo, e
ricalcolare l'intero storico a ogni valutazione trasformerebbe una decisione da
millisecondi in una da secondi. Su un orizzonte di 60 secondi, un ritardo di
qualche secondo non e' una lentezza: e' una risposta a una domanda diversa.

Quindi tutto qui dentro e' O(1) o O(log n) per aggiornamento, mai O(n).
"""

from __future__ import annotations

import bisect
import math
from collections import deque
from typing import Iterator


class TimeSeries:
    """Serie (timestamp, valore) con finestra scorrevole e ricerca binaria.

    Due liste parallele invece di una coda di coppie: `bisect` lavora
    direttamente sui timestamp, quindi "il valore di 30 secondi fa" costa
    O(log n) invece di scorrere la finestra.
    """

    __slots__ = ("window_ms", "ts", "values", "_sum", "_sumsq")

    def __init__(self, window_ms: int) -> None:
        self.window_ms = window_ms
        self.ts: list[int] = []
        self.values: list[float] = []
        self._sum = 0.0
        self._sumsq = 0.0

    def append(self, ts: int, value: float) -> None:
        if self.ts and ts < self.ts[-1]:
            return                       # dati fuori ordine: ignorati, non riordinati
        self.ts.append(ts)
        self.values.append(value)
        self._sum += value
        self._sumsq += value * value
        self._trim(ts - self.window_ms)

    def _trim(self, cutoff: int) -> None:
        drop = bisect.bisect_left(self.ts, cutoff)
        if drop <= 0:
            return
        for v in self.values[:drop]:
            self._sum -= v
            self._sumsq -= v * v
        del self.ts[:drop]
        del self.values[:drop]

    def __len__(self) -> int:
        return len(self.ts)

    @property
    def last(self) -> float | None:
        return self.values[-1] if self.values else None

    @property
    def last_ts(self) -> int | None:
        return self.ts[-1] if self.ts else None

    @property
    def span_ms(self) -> int:
        return (self.ts[-1] - self.ts[0]) if len(self.ts) > 1 else 0

    def mean(self) -> float | None:
        return (self._sum / len(self.values)) if self.values else None

    def std(self) -> float | None:
        n = len(self.values)
        if n < 2:
            return None
        var = (self._sumsq - self._sum * self._sum / n) / (n - 1)
        return math.sqrt(max(0.0, var))

    def value_at_or_before(self, ts: int) -> float | None:
        """Il valore piu' recente non successivo a `ts`.

        "Non successivo" e' il punto: e' cio' che rende le feature causali.
        Prendere il campione piu' vicino, anche di un istante nel futuro,
        introdurrebbe informazione che al momento della decisione non c'era.
        """
        if not self.ts:
            return None
        i = bisect.bisect_right(self.ts, ts) - 1
        return self.values[i] if i >= 0 else None

    def slice_since(self, cutoff: int) -> list[float]:
        return self.values[bisect.bisect_left(self.ts, cutoff):]

    def return_bps(self, horizon_ms: int) -> float | None:
        """Ritorno su una finestra, in punti base."""
        if not self.ts:
            return None
        now = self.ts[-1]
        past = self.value_at_or_before(now - horizon_ms)
        current = self.values[-1]
        if past is None or past <= 0 or self.span_ms < horizon_ms * 0.5:
            return None
        return (current - past) / past * 10_000.0

    def realized_vol_bps(self, window_ms: int) -> float | None:
        """Deviazione standard dei ritorni fra campioni consecutivi, in bps."""
        vals = self.slice_since(self.ts[-1] - window_ms) if self.ts else []
        if len(vals) < 3:
            return None
        rets = [math.log(vals[i] / vals[i - 1]) for i in range(1, len(vals))
                if vals[i] > 0 and vals[i - 1] > 0]
        if len(rets) < 2:
            return None
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
        return math.sqrt(var) * 10_000.0

    def windows(self, horizon_ms: int, lookback_ms: int) -> Iterator[tuple[float, float]]:
        """Coppie (inizio, fine) di finestre lunghe un orizzonte.

        Si sovrappongono di proposito: a passo pieno, 3 minuti di storia
        darebbero tre soli campioni su un orizzonte da 60 secondi — troppo
        pochi per leggerci qualcosa. Restano pero' correlate, ed e' per questo
        che gli intervalli di confidenza calcolati su di esse sono ottimisti.
        """
        if not self.ts:
            return
        end = self.ts[-1]
        step = max(horizon_ms // 10, 100)
        t = end - lookback_ms + horizon_ms
        while t <= end:
            a = self.value_at_or_before(t - horizon_ms)
            b = self.value_at_or_before(t)
            if a is not None and b is not None:
                yield (a, b)
            t += step

    def sigma_over(self, horizon_ms: int, lookback_ms: int) -> float | None:
        """Quanto si sposta tipicamente il prezzo in un orizzonte, in bps."""
        if not self.ts or self.span_ms < lookback_ms * 0.8:
            return None
        samples = [(b - a) / a * 10_000.0
                   for a, b in self.windows(horizon_ms, lookback_ms) if a > 0]
        if len(samples) < 5:
            return None
        mean = sum(samples) / len(samples)
        var = sum((s - mean) ** 2 for s in samples) / (len(samples) - 1)
        return math.sqrt(var)

    def zero_move_fraction(self, horizon_ms: int, lookback_ms: int) -> float | None:
        """Quota di finestre che finiscono esattamente dove sono cominciate.

        Su una binaria che non paga il pareggio questo e' un fatto economico
        di primo ordine, e la tolleranza e' zero perche' zero e' la regola con
        cui si decide l'esito: uscita uguale a ingresso e' un pareggio.
        """
        if not self.ts or self.span_ms < lookback_ms * 0.8:
            return None
        total = flat = 0
        for a, b in self.windows(horizon_ms, lookback_ms):
            total += 1
            if a == b:
                flat += 1
        return (flat / total) if total >= 5 else None


class RollingCounter:
    """Conteggio di eventi in una finestra scorrevole."""

    __slots__ = ("window_ms", "events")

    def __init__(self, window_ms: int) -> None:
        self.window_ms = window_ms
        self.events: deque[tuple[int, float]] = deque()

    def add(self, ts: int, weight: float = 1.0) -> None:
        self.events.append((ts, weight))
        cutoff = ts - self.window_ms
        while self.events and self.events[0][0] < cutoff:
            self.events.popleft()

    def count(self) -> int:
        return len(self.events)

    def total(self) -> float:
        return sum(w for _, w in self.events)


class Candle:
    __slots__ = ("ts", "open", "high", "low", "close", "tick_count",
                 "bid_close", "ask_close", "spread_sum", "spread_n")

    def __init__(self, ts: int, price: float) -> None:
        self.ts = ts
        self.open = self.high = self.low = self.close = price
        # Zero, non uno: chi crea la barra chiama subito `update`, e partire da
        # uno conterebbe due volte il primo tick. Su una barra da 5 secondi
        # e' un errore del 20% sul volume mostrato.
        self.tick_count = 0
        self.bid_close: float | None = None
        self.ask_close: float | None = None
        self.spread_sum = 0.0
        self.spread_n = 0

    def update(self, price: float, bid: float | None = None,
               ask: float | None = None) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.tick_count += 1
        if bid is not None:
            self.bid_close = bid
        if ask is not None:
            self.ask_close = ask
        if bid is not None and ask is not None:
            self.spread_sum += (ask - bid)
            self.spread_n += 1

    def to_dict(self) -> dict:
        return {
            "ts": self.ts, "open": self.open, "high": self.high,
            "low": self.low, "close": self.close, "tick_count": self.tick_count,
            "bid_close": self.bid_close, "ask_close": self.ask_close,
            "spread_avg": (self.spread_sum / self.spread_n
                           if self.spread_n else None),
        }


class CandleAggregator:
    """Barre a piu' intervalli, costruite dagli stessi tick che il motore usa.

    Costruirle dagli stessi dati — invece di chiedere le candele a un secondo
    provider — evita che il grafico racconti una storia diversa da quella su
    cui il motore ha deciso.
    """

    def __init__(self, buckets_s: tuple[int, ...], max_bars: int = 600) -> None:
        self.buckets_s = buckets_s
        self.max_bars = max_bars
        self.bars: dict[int, list[Candle]] = {b: [] for b in buckets_s}

    def add(self, ts: int, price: float, bid: float | None = None,
            ask: float | None = None) -> list[tuple[int, Candle]]:
        """Ritorna le barre CHIUSE da questo tick, pronte per il database."""
        closed: list[tuple[int, Candle]] = []
        for bucket in self.buckets_s:
            ms = bucket * 1000
            slot = ts - (ts % ms)
            series = self.bars[bucket]
            if series and series[-1].ts == slot:
                series[-1].update(price, bid, ask)
            else:
                if series:
                    closed.append((bucket, series[-1]))
                series.append(Candle(slot, price))
                series[-1].update(price, bid, ask)
                if len(series) > self.max_bars:
                    del series[0]
        return closed

    def recent(self, bucket_s: int, limit: int = 240) -> list[dict]:
        return [c.to_dict() for c in self.bars.get(bucket_s, [])[-limit:]]
