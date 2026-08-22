"""Statistica minima, scritta a mano e senza numpy.

Il pacchetto gira ovunque ci sia un `python3`, senza installare niente. Il
prezzo e' che queste funzioni vanno scritte; il vantaggio e' che non c'e' una
versione di libreria che cambia comportamento fra due macchine.

Ogni funzione ignora i valori non finiti invece di propagare NaN: nei dati di
mercato un buco e' normale (un endpoint che non risponde, una barra senza
scambi) e far collassare l'intera riga per un buco e' un modo veloce di
buttare via meta' del campione.
"""

from __future__ import annotations

import math
from typing import Iterable, Sequence


def finite(values: Iterable[float | None]) -> list[float]:
    out: list[float] = []
    for v in values:
        if v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if math.isfinite(f):
            out.append(f)
    return out


def mean(values: Sequence[float | None]) -> float | None:
    vals = finite(values)
    return sum(vals) / len(vals) if vals else None


def stdev(values: Sequence[float | None], sample: bool = True) -> float | None:
    vals = finite(values)
    n = len(vals)
    if n < (2 if sample else 1):
        return None
    m = sum(vals) / n
    var = sum((v - m) ** 2 for v in vals) / (n - 1 if sample else n)
    return math.sqrt(max(var, 0.0))


def median(values: Sequence[float | None]) -> float | None:
    vals = sorted(finite(values))
    n = len(vals)
    if n == 0:
        return None
    mid = n // 2
    return vals[mid] if n % 2 else (vals[mid - 1] + vals[mid]) / 2.0


def quantile(values: Sequence[float | None], q: float) -> float | None:
    """Quantile con interpolazione lineare, come `numpy.quantile` di default."""
    vals = sorted(finite(values))
    n = len(vals)
    if n == 0:
        return None
    if n == 1:
        return vals[0]
    q = min(max(q, 0.0), 1.0)
    pos = q * (n - 1)
    lo = int(math.floor(pos))
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return vals[lo] * (1 - frac) + vals[hi] * frac


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def safe_div(num: float | None, den: float | None,
             default: float | None = None) -> float | None:
    if num is None or den is None:
        return default
    try:
        if den == 0 or not math.isfinite(den) or not math.isfinite(num):
            return default
        return num / den
    except (TypeError, ValueError):
        return default


def pct_change(new: float | None, old: float | None) -> float | None:
    """Variazione relativa. Restituisce una frazione, non una percentuale."""
    if new is None or old is None or old == 0 or not math.isfinite(old):
        return None
    if not math.isfinite(new):
        return None
    return (new - old) / abs(old)


def bps(new: float | None, old: float | None) -> float | None:
    """Variazione in punti base. L'unita' in cui si ragiona su BTC intraday."""
    ch = pct_change(new, old)
    return None if ch is None else ch * 10_000.0


def zscore(value: float | None, values: Sequence[float | None]) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    m = mean(values)
    s = stdev(values)
    if m is None or s is None or s <= 0:
        return None
    return (value - m) / s


def percentile_rank(value: float | None,
                    values: Sequence[float | None]) -> float | None:
    """Posizione di `value` nella distribuzione, in [0, 1].

    Piu' robusto dello z-score quando la distribuzione ha code grasse, che nei
    volumi e nell'open interest e' la regola, non l'eccezione.
    """
    if value is None or not math.isfinite(value):
        return None
    vals = finite(values)
    if not vals:
        return None
    below = sum(1 for v in vals if v < value)
    equal = sum(1 for v in vals if v == value)
    return (below + 0.5 * equal) / len(vals)


def correlation(xs: Sequence[float | None], ys: Sequence[float | None]) -> float | None:
    """Pearson sui soli indici dove entrambi i valori sono finiti."""
    pairs = [
        (float(x), float(y))
        for x, y in zip(xs, ys)
        if x is not None and y is not None
        and math.isfinite(float(x)) and math.isfinite(float(y))
    ]
    if len(pairs) < 3:
        return None
    n = len(pairs)
    mx = sum(p[0] for p in pairs) / n
    my = sum(p[1] for p in pairs) / n
    num = sum((p[0] - mx) * (p[1] - my) for p in pairs)
    dx = math.sqrt(sum((p[0] - mx) ** 2 for p in pairs))
    dy = math.sqrt(sum((p[1] - my) ** 2 for p in pairs))
    if dx <= 0 or dy <= 0:
        return None
    return num / (dx * dy)


def linreg_slope(values: Sequence[float | None]) -> float | None:
    """Pendenza per passo di una regressione sull'indice.

    Serve per l'accelerazione: la pendenza dell'open interest dice se sta
    salendo, la pendenza della pendenza dice se sta accelerando.
    """
    vals = [(i, v) for i, v in enumerate(values)
            if v is not None and math.isfinite(float(v))]
    n = len(vals)
    if n < 3:
        return None
    mx = sum(p[0] for p in vals) / n
    my = sum(p[1] for p in vals) / n
    num = sum((p[0] - mx) * (p[1] - my) for p in vals)
    den = sum((p[0] - mx) ** 2 for p in vals)
    return None if den <= 0 else num / den


def sigmoid(x: float) -> float:
    """Logistica stabile agli estremi (niente overflow su exp)."""
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def softmax(scores: Sequence[float]) -> list[float]:
    if not scores:
        return []
    m = max(scores)
    exps = [math.exp(s - m) for s in scores]
    total = sum(exps)
    if total <= 0:
        return [1.0 / len(scores)] * len(scores)
    return [e / total for e in exps]


def wilson_interval(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Intervallo di Wilson.

    Con 20 casi e 15 successi, l'intervallo normale dice 0.56-0.94 e quello di
    Wilson 0.54-0.88: il secondo e' quello che non promuove edge inesistenti.
    """
    if n <= 0:
        return (0.0, 1.0)
    p = successes / n
    d = 1.0 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, centre - half), min(1.0, centre + half))


def binomial_tail(successes: int, n: int, p0: float) -> float:
    """P(X >= successes) con X ~ Binomiale(n, p0). Calcolata in log per stabilita'."""
    if n <= 0:
        return 1.0
    successes = max(0, min(n, successes))
    if p0 <= 0:
        return 1.0 if successes == 0 else 0.0
    if p0 >= 1:
        return 1.0
    log_p, log_q = math.log(p0), math.log(1.0 - p0)
    total = 0.0
    for k in range(successes, n + 1):
        log_c = math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1)
        total += math.exp(log_c + k * log_p + (n - k) * log_q)
    return clamp(total, 0.0, 1.0)


def benjamini_hochberg(p_values: Sequence[float],
                       alpha: float = 0.10) -> tuple[list[bool], list[float]]:
    """Controllo della quota di falsi positivi fra i candidati sopravvissuti.

    Il motore prova decine di pattern sugli stessi dati. Senza correzione,
    "significativo a 0.05" significa solo "ho provato abbastanza volte".
    """
    n = len(p_values)
    if n == 0:
        return [], []
    order = sorted(range(n), key=lambda i: p_values[i])
    adjusted = [1.0] * n
    prev = 1.0
    for rank in range(n, 0, -1):
        i = order[rank - 1]
        adj = min(prev, p_values[i] * n / rank)
        adjusted[i] = adj
        prev = adj
    return [a <= alpha for a in adjusted], adjusted
