"""Indicatori classici, tutti causali.

"Causale" ha un significato preciso e verificabile: ogni funzione riceve una
sequenza che finisce alla barra corrente **chiusa** e non guarda oltre. Non
esiste centratura, non esiste media mobile simmetrica, non esiste normalizzazione
sulla media dell'intero campione. Sono i tre modi con cui la conoscenza del
futuro entra in un backtest senza che nessuno se ne accorga.

Ogni funzione restituisce `None` quando non ha abbastanza storia. `None` e' un
valore legittimo che attraversa tutto il sistema fino alla dashboard, dove
diventa "n/d". Riempirlo con uno zero sarebbe peggio che non averlo: uno zero
in un RSI significa ipervenduto estremo, non "non lo so".
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

from ..util.numeric import mean, quantile, stdev


@dataclass(frozen=True)
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float

    @property
    def typical(self) -> float:
        return (self.high + self.low + self.close) / 3.0

    @property
    def range(self) -> float:
        return self.high - self.low


def sma(values: Sequence[float], period: int) -> float | None:
    if period <= 0 or len(values) < period:
        return None
    window = values[-period:]
    return sum(window) / period


def ema(values: Sequence[float], period: int) -> float | None:
    """EMA seminata sulla SMA dei primi `period` valori.

    Seminare sul primo valore singolo, come fanno molte implementazioni veloci,
    lascia una coda di transitorio lunga quanto il periodo. Su una finestra
    corta quella coda e' meta' del segnale.
    """
    if period <= 0 or len(values) < period:
        return None
    k = 2.0 / (period + 1.0)
    out = sum(values[:period]) / period
    for v in values[period:]:
        out = v * k + out * (1.0 - k)
    return out


def ema_series(values: Sequence[float], period: int) -> list[float | None]:
    if period <= 0 or len(values) < period:
        return [None] * len(values)
    k = 2.0 / (period + 1.0)
    out: list[float | None] = [None] * (period - 1)
    cur = sum(values[:period]) / period
    out.append(cur)
    for v in values[period:]:
        cur = v * k + cur * (1.0 - k)
        out.append(cur)
    return out


def rsi(values: Sequence[float], period: int = 14) -> float | None:
    """RSI di Wilder. 50 quando non c'e' stata alcuna perdita e alcun guadagno."""
    if len(values) < period + 1:
        return None
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = values[i] - values[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    avg_gain, avg_loss = gains / period, losses / period
    for i in range(period + 1, len(values)):
        d = values[i] - values[i - 1]
        avg_gain = (avg_gain * (period - 1) + max(d, 0.0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-d, 0.0)) / period
    if avg_loss <= 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def macd(values: Sequence[float], fast: int = 12, slow: int = 26,
         signal: int = 9) -> tuple[float, float, float] | None:
    """(macd, segnale, istogramma). L'istogramma e' quello che porta informazione."""
    if len(values) < slow + signal:
        return None
    fast_s = ema_series(values, fast)
    slow_s = ema_series(values, slow)
    line = [f - s for f, s in zip(fast_s, slow_s)
            if f is not None and s is not None]
    if len(line) < signal:
        return None
    sig = ema(line, signal)
    if sig is None:
        return None
    return (line[-1], sig, line[-1] - sig)


def vwap(bars: Sequence[Bar]) -> float | None:
    """VWAP sulla finestra data. Il denominatore e' il volume, non il numero di barre."""
    notional = sum(b.turnover if b.turnover else b.typical * b.volume
                   for b in bars)
    volume = sum(b.volume for b in bars)
    if volume <= 0 or notional <= 0:
        return None
    return notional / volume


def vwap_bands(bars: Sequence[Bar]) -> tuple[float, float] | None:
    """(vwap, deviazione ponderata). La banda dice quanto e' "lontano" lontano."""
    v = vwap(bars)
    if v is None:
        return None
    volume = sum(b.volume for b in bars)
    if volume <= 0:
        return None
    var = sum(b.volume * (b.typical - v) ** 2 for b in bars) / volume
    return (v, math.sqrt(max(var, 0.0)))


def true_range(prev_close: float, bar: Bar) -> float:
    return max(bar.high - bar.low,
               abs(bar.high - prev_close),
               abs(bar.low - prev_close))


def atr(bars: Sequence[Bar], period: int = 14) -> float | None:
    """ATR di Wilder. In unita' di prezzo: per confrontare regimi va diviso dal prezzo."""
    if len(bars) < period + 1:
        return None
    trs = [true_range(bars[i - 1].close, bars[i]) for i in range(1, len(bars))]
    if len(trs) < period:
        return None
    cur = sum(trs[:period]) / period
    for tr in trs[period:]:
        cur = (cur * (period - 1) + tr) / period
    return cur


def bollinger(values: Sequence[float], period: int = 20,
              k: float = 2.0) -> tuple[float, float, float, float, float] | None:
    """(media, alta, bassa, z, larghezza%).

    La larghezza normalizzata sulla media e' quella che serve: identifica la
    compressione, che e' l'unico stato da cui un breakout ha senso.
    """
    if len(values) < period:
        return None
    window = values[-period:]
    m = sum(window) / period
    s = stdev(window)
    if s is None or m == 0:
        return None
    upper, lower = m + k * s, m - k * s
    z = (values[-1] - m) / s if s > 0 else 0.0
    width_pct = (upper - lower) / abs(m) * 100.0
    return (m, upper, lower, z, width_pct)


def realized_vol_bps(closes: Sequence[float], window: int) -> float | None:
    """Volatilita' realizzata sui rendimenti logaritmici, in bps per barra.

    E' la grandezza con cui si scala tutto il resto: la banda FLAT, il target
    di prezzo, l'invalidazione. Un movimento di 20 bps e' enorme in un'ora
    tranquilla e invisibile in un'ora di dati macro.
    """
    if len(closes) < window + 1:
        return None
    rets = []
    for i in range(len(closes) - window, len(closes)):
        if closes[i - 1] <= 0 or closes[i] <= 0:
            continue
        rets.append(math.log(closes[i] / closes[i - 1]))
    if len(rets) < 3:
        return None
    s = stdev(rets)
    return None if s is None else s * 10_000.0


def momentum_bps(closes: Sequence[float], lookback: int) -> float | None:
    if len(closes) < lookback + 1 or closes[-1 - lookback] <= 0:
        return None
    return (closes[-1] / closes[-1 - lookback] - 1.0) * 10_000.0


def rolling_zscore(values: Sequence[float], window: int) -> float | None:
    if len(values) < window + 1:
        return None
    hist = values[-window - 1:-1]
    m, s = mean(hist), stdev(hist)
    if m is None or s is None or s <= 0:
        return None
    return (values[-1] - m) / s


def rolling_percentile(values: Sequence[float], window: int) -> float | None:
    """Posizione dell'ultimo valore nella sua storia recente, in [0, 1]."""
    if len(values) < window + 1:
        return None
    hist = values[-window - 1:-1]
    last = values[-1]
    below = sum(1 for v in hist if v < last)
    equal = sum(1 for v in hist if v == last)
    return (below + 0.5 * equal) / len(hist)


def compression_ratio(values: Sequence[float], short: int,
                      long: int) -> float | None:
    """Volatilita' recente diviso volatilita' di riferimento.

    Sotto 1 il mercato si sta stringendo; molto sotto 1 e' compressione, lo
    stato che precede piu' spesso un'espansione. Da solo non dice la direzione,
    e non deve pretendere di dirla.
    """
    if long <= short or len(values) < long + 1:
        return None
    s = realized_vol_bps(values, short)
    l = realized_vol_bps(values, long)
    if s is None or l is None or l <= 0:
        return None
    return s / l


def swing_levels(bars: Sequence[Bar], lookback: int = 60) -> tuple[float, float] | None:
    """(minimo, massimo) della finestra. Servono per l'invalidazione."""
    window = bars[-lookback:] if len(bars) >= lookback else list(bars)
    if len(window) < 5:
        return None
    return (min(b.low for b in window), max(b.high for b in window))


def volume_profile(bars: Sequence[Bar], window: int = 60) -> dict[str, float | None]:
    """Il volume recente contro il suo passato, e la sua concentrazione."""
    if len(bars) < window + 1:
        return {"ratio": None, "percentile": None, "burst": None}
    vols = [b.volume for b in bars[-window - 1:]]
    last = vols[-1]
    hist = vols[:-1]
    m = mean(hist)
    med = quantile(hist, 0.5)
    below = sum(1 for v in hist if v < last)
    return {
        "ratio": (last / m) if m and m > 0 else None,
        "percentile": below / len(hist) if hist else None,
        # Il burst si misura sulla mediana, non sulla media: la media di una
        # distribuzione a code grasse e' gia' un'anomalia.
        "burst": (last / med) if med and med > 0 else None,
    }
