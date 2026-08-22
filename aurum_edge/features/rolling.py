"""Serie scorrevoli calcolate in una passata sola.

Ricalcolare una finestra di 240 barre a ogni riga di un archivio di sei mesi
significa qualche centinaio di milioni di operazioni in Python: minuti di
attesa a ogni esperimento, che nella pratica vuol dire meno esperimenti. Qui
ogni serie si calcola in una passata mantenendo lo stato.

Ogni funzione restituisce una lista **della stessa lunghezza dell'ingresso**,
con `None` dove la finestra non e' ancora piena. Allineamento garantito:
l'indice `i` dell'uscita corrisponde sempre all'indice `i` dell'ingresso, e mai
a `i+1`. Uno scivolamento di un solo passo in avanti e' esattamente uno sguardo
sul futuro di un minuto, e su un orizzonte di trenta e' abbastanza per
inventare un vantaggio.
"""

from __future__ import annotations

import bisect
import math
from typing import Sequence


def rolling_mean(values: Sequence[float | None], window: int) -> list[float | None]:
    """Media scorrevole su somme correnti, saltando i buchi.

    Il conteggio tiene solo i valori finiti: una finestra con due buchi su
    sessanta e' ancora una media su cinquantotto, non una media sbagliata su
    sessanta.
    """
    out: list[float | None] = []
    total = 0.0
    count = 0
    for i, v in enumerate(values):
        if v is not None and math.isfinite(v):
            total += v
            count += 1
        if i >= window:
            old = values[i - window]
            if old is not None and math.isfinite(old):
                total -= old
                count -= 1
        out.append(total / count if count > 0 else None)
    return out


def rolling_std(values: Sequence[float | None], window: int) -> list[float | None]:
    out: list[float | None] = []
    total = 0.0
    total_sq = 0.0
    count = 0
    for i, v in enumerate(values):
        if v is not None and math.isfinite(v):
            total += v
            total_sq += v * v
            count += 1
        if i >= window:
            old = values[i - window]
            if old is not None and math.isfinite(old):
                total -= old
                total_sq -= old * old
                count -= 1
        if count >= 2:
            var = (total_sq - total * total / count) / (count - 1)
            out.append(math.sqrt(var) if var > 0 else 0.0)
        else:
            out.append(None)
    return out


def rolling_percentile(values: Sequence[float | None],
                       window: int) -> list[float | None]:
    """Posizione del valore corrente nella finestra PRECEDENTE, in [0, 1].

    La finestra esclude il valore stesso: includerlo lo farebbe competere con
    se' stesso e comprimerebbe gli estremi proprio dove servono, cioe' quando
    un volume e' il piu' alto degli ultimi sessanta minuti.
    """
    out: list[float | None] = []
    ordered: list[float] = []
    history: list[float | None] = []
    for i, v in enumerate(values):
        if len(ordered) >= 5 and v is not None and math.isfinite(v):
            below = bisect.bisect_left(ordered, v)
            equal = bisect.bisect_right(ordered, v) - below
            out.append((below + 0.5 * equal) / len(ordered))
        else:
            out.append(None)
        # Solo ora il valore entra nella storia: da qui in poi e' passato.
        if v is not None and math.isfinite(v):
            bisect.insort(ordered, v)
            history.append(v)
        else:
            history.append(None)
        if len(history) > window:
            old = history.pop(0)
            if old is not None:
                idx = bisect.bisect_left(ordered, old)
                if idx < len(ordered) and ordered[idx] == old:
                    ordered.pop(idx)
    return out


def rolling_median(values: Sequence[float | None],
                   window: int) -> list[float | None]:
    out: list[float | None] = []
    ordered: list[float] = []
    history: list[float | None] = []
    for v in values:
        if len(ordered) >= 3:
            n = len(ordered)
            mid = n // 2
            out.append(ordered[mid] if n % 2
                       else (ordered[mid - 1] + ordered[mid]) / 2.0)
        else:
            out.append(None)
        if v is not None and math.isfinite(v):
            bisect.insort(ordered, v)
            history.append(v)
        else:
            history.append(None)
        if len(history) > window:
            old = history.pop(0)
            if old is not None:
                idx = bisect.bisect_left(ordered, old)
                if idx < len(ordered) and ordered[idx] == old:
                    ordered.pop(idx)
    return out


def rolling_zscore(values: Sequence[float | None], window: int,
                   exclude_self: bool = True) -> list[float | None]:
    """Z-score contro la finestra precedente."""
    shifted: list[float | None] = ([None] + list(values[:-1])
                                   if exclude_self else list(values))
    means = rolling_mean(shifted, window)
    stds = rolling_std(shifted, window)
    out: list[float | None] = []
    for v, m, s in zip(values, means, stds):
        if v is None or m is None or s is None or s <= 0:
            out.append(None)
        else:
            out.append((v - m) / s)
    return out


def rsi_series(closes: Sequence[float], period: int = 14) -> list[float | None]:
    """RSI di Wilder in una passata."""
    n = len(closes)
    out: list[float | None] = [None] * n
    if n < period + 1:
        return out
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    avg_gain, avg_loss = gains / period, losses / period
    out[period] = _rsi_from(avg_gain, avg_loss)
    for i in range(period + 1, n):
        d = closes[i] - closes[i - 1]
        avg_gain = (avg_gain * (period - 1) + max(d, 0.0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-d, 0.0)) / period
        out[i] = _rsi_from(avg_gain, avg_loss)
    return out


def _rsi_from(avg_gain: float, avg_loss: float) -> float:
    if avg_loss <= 0:
        return 100.0 if avg_gain > 0 else 50.0
    return 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))


def ema_series(values: Sequence[float], period: int) -> list[float | None]:
    n = len(values)
    out: list[float | None] = [None] * n
    if period <= 0 or n < period:
        return out
    k = 2.0 / (period + 1.0)
    cur = sum(values[:period]) / period
    out[period - 1] = cur
    for i in range(period, n):
        cur = values[i] * k + cur * (1.0 - k)
        out[i] = cur
    return out


def atr_series(highs: Sequence[float], lows: Sequence[float],
               closes: Sequence[float], period: int = 14) -> list[float | None]:
    n = len(closes)
    out: list[float | None] = [None] * n
    if n < period + 1:
        return out
    trs = [0.0] * n
    for i in range(1, n):
        trs[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]),
                     abs(lows[i] - closes[i - 1]))
    cur = sum(trs[1:period + 1]) / period
    out[period] = cur
    for i in range(period + 1, n):
        cur = (cur * (period - 1) + trs[i]) / period
        out[i] = cur
    return out


def realized_vol_series(closes: Sequence[float], window: int) -> list[float | None]:
    """Deviazione standard dei rendimenti logaritmici, in bps per barra."""
    n = len(closes)
    rets: list[float | None] = [None] * n
    for i in range(1, n):
        if closes[i] > 0 and closes[i - 1] > 0:
            rets[i] = math.log(closes[i] / closes[i - 1]) * 10_000.0
    return rolling_std(rets, window)


def rolling_vwap(turnovers: Sequence[float], volumes: Sequence[float],
                 window: int) -> list[float | None]:
    n = len(volumes)
    out: list[float | None] = [None] * n
    tot_n = tot_v = 0.0
    for i in range(n):
        tot_n += turnovers[i]
        tot_v += volumes[i]
        if i >= window:
            tot_n -= turnovers[i - window]
            tot_v -= volumes[i - window]
        out[i] = (tot_n / tot_v) if tot_v > 0 and tot_n > 0 else None
    return out


def rolling_extreme(values: Sequence[float], window: int,
                    mode: str = "max") -> list[float | None]:
    """Massimo o minimo scorrevole con deque monotona: O(n) invece di O(n*w)."""
    from collections import deque

    n = len(values)
    out: list[float | None] = [None] * n
    dq: deque[int] = deque()
    want_max = mode == "max"
    for i in range(n):
        while dq and dq[0] <= i - window:
            dq.popleft()
        while dq and ((values[dq[-1]] <= values[i]) if want_max
                      else (values[dq[-1]] >= values[i])):
            dq.pop()
        dq.append(i)
        if i >= window - 1:
            out[i] = values[dq[0]]
    return out


def rolling_slope(values: Sequence[float | None], window: int) -> list[float | None]:
    """Pendenza per barra di una regressione sull'indice, in una passata.

    I termini in x sono costanti (l'indice e' regolare), quindi bastano le
    somme correnti di y e di i*y per ottenere la pendenza senza rifare la
    regressione a ogni passo.
    """
    n = len(values)
    out: list[float | None] = [None] * n
    if window < 3:
        return out
    sum_y = 0.0
    count = 0
    buf: list[float | None] = []
    for i, v in enumerate(values):
        buf.append(v)
        if v is not None and math.isfinite(v):
            sum_y += v
            count += 1
        if len(buf) > window:
            old = buf.pop(0)
            if old is not None and math.isfinite(old):
                sum_y -= old
                count -= 1
        if count >= 3:
            # Ricalcolo locale sulla finestra corrente: il costo e' lineare in
            # `window` ma solo quando la finestra e' piena di valori validi.
            pts = [(j, y) for j, y in enumerate(buf)
                   if y is not None and math.isfinite(y)]
            m = len(pts)
            mx = sum(p[0] for p in pts) / m
            my = sum(p[1] for p in pts) / m
            num = sum((p[0] - mx) * (p[1] - my) for p in pts)
            den = sum((p[0] - mx) ** 2 for p in pts)
            out[i] = num / den if den > 0 else None
    return out


def align_step_series(target_ts: Sequence[int], src_ts: Sequence[int],
                      src_values: Sequence[float],
                      max_age_ms: int | None = None) -> list[float | None]:
    """Porta una serie a passo grosso sulla griglia fine, all'indietro.

    Il valore assegnato a `t` e' l'ultimo osservato **a `t` o prima**. Mai
    quello successivo: interpolare fra il punto prima e quello dopo e' il modo
    piu' comune di far entrare il futuro in una feature di open interest o di
    funding, e produce serie bellissime e inutilizzabili.
    """
    out: list[float | None] = []
    j = 0
    last_val: float | None = None
    last_ts: int | None = None
    for t in target_ts:
        while j < len(src_ts) and src_ts[j] <= t:
            last_val = src_values[j]
            last_ts = src_ts[j]
            j += 1
        if last_val is None or (max_age_ms is not None and last_ts is not None
                                and t - last_ts > max_age_ms):
            out.append(None)
        else:
            out.append(last_val)
    return out
