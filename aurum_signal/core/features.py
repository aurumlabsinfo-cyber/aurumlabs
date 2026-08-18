"""Vettore di feature causali, calcolato in modo incrementale.

**Causale** significa che ogni valore usa solo informazione disponibile al
proprio timestamp o prima. Non e' un dettaglio stilistico: e' la differenza
fra un modello che predice e uno che ricorda. Se una sola feature guarda anche
un istante nel futuro, l'accuratezza fuori campione diventa un numero senza
significato — e sembrera' ottima proprio mentre e' inutile.

Ogni feature che non e' calcolabile vale `None`, mai zero. Zero e' un valore:
"nessun movimento" e "non lo so" sono cose diverse, e confonderle insegna al
modello una regolarita' che non esiste.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any

from .buffers import CandleAggregator, RollingCounter, TimeSeries

#: Orizzonti di ritorno richiesti, in secondi.
RETURN_HORIZONS_S = (1, 3, 5, 10, 15, 30, 60)
#: Intervalli delle candele mostrate e usate dal contesto.
CANDLE_BUCKETS_S = (5, 15, 30, 60, 180, 300, 900)
#: Finestre dell'order flow.
FLOW_WINDOWS_S = (1, 3, 5, 10, 15, 30)

#: Colonne che sono metadati, non predittori. Il prezzo assoluto in
#: particolare NON deve entrare in un modello: imparerebbe il livello di
#: quella settimana invece della dinamica del mercato.
NON_PREDICTIVE = frozenset({
    "ts", "mid", "bid", "ask", "last", "mode", "source",
    "second_of_minute", "minute", "hour", "day_of_week",
})


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    pos = q * (len(s) - 1)
    lo = int(math.floor(pos))
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def _entropy(values: list[float], bins: int = 8) -> float | None:
    """Entropia dei ritorni: quanto e' imprevedibile la serie recente."""
    if len(values) < bins * 3:
        return None
    lo, hi = min(values), max(values)
    if hi <= lo:
        return 0.0
    counts = [0] * bins
    for v in values:
        idx = min(bins - 1, int((v - lo) / (hi - lo) * bins))
        counts[idx] += 1
    n = len(values)
    ent = -sum((c / n) * math.log(c / n) for c in counts if c > 0)
    return ent / math.log(bins)          # normalizzata su [0,1]


@dataclass
class FeatureVector:
    ts: int
    mode: str
    values: dict[str, Any] = field(default_factory=dict)

    def get(self, name: str, default: Any = None) -> Any:
        v = self.values.get(name, default)
        return default if v is None else v

    def has(self, *names: str) -> bool:
        return all(self.values.get(n) is not None for n in names)

    def predictive(self) -> dict[str, float]:
        """Solo le colonne che un modello puo' legittimamente usare."""
        return {k: float(v) for k, v in self.values.items()
                if k not in NON_PREDICTIVE
                and isinstance(v, (int, float)) and not isinstance(v, bool)
                and math.isfinite(float(v))}


class FeatureEngine:
    """Trasforma il flusso di quotazioni in un vettore causale.

    Tiene i buffer, li aggiorna a ogni quotazione in tempo costante, e produce
    il vettore su richiesta del loop di decisione — non a ogni tick, perche'
    valutare piu' spesso di quanto il mercato cambi e' solo lavoro sprecato.
    """

    def __init__(self, cfg) -> None:
        self.cfg = cfg
        longest = max(RETURN_HORIZONS_S) * 1000
        self.mid = TimeSeries(max(900_000, longest * 8))
        self.spread = TimeSeries(300_000)
        self.candles = CandleAggregator(CANDLE_BUCKETS_S)
        #: Tick al rialzo e al ribasso: e' l'unico "order flow" che un feed FX
        #: gratuito consente davvero di misurare.
        self.up_ticks = RollingCounter(60_000)
        self.down_ticks = RollingCounter(60_000)
        self.quotes = RollingCounter(60_000)
        self.computed = 0
        self.last_vector: FeatureVector | None = None
        self._last_mid: float | None = None
        self._has_bid_ask = False

    # ------------------------------------------------------------ ingestione
    def observe(self, quote) -> list[tuple[int, Any]]:
        """Aggiorna i buffer. Ritorna le candele chiuse da questo tick."""
        ts = quote.received_ts
        self.mid.append(ts, quote.mid)
        self.quotes.add(ts)
        if quote.spread_bps is not None:
            self.spread.append(ts, quote.spread_bps)
            self._has_bid_ask = True
        if self._last_mid is not None:
            if quote.mid > self._last_mid:
                self.up_ticks.add(ts)
            elif quote.mid < self._last_mid:
                self.down_ticks.add(ts)
        self._last_mid = quote.mid
        return self.candles.add(ts, quote.mid, quote.bid, quote.ask)

    # ------------------------------------------------------------- calcolo
    def compute(self, ts: int, mode: str) -> FeatureVector | None:
        if len(self.mid) < 10:
            return None
        f: dict[str, Any] = {"ts": ts, "mid": self.mid.last}

        # --- prezzo -------------------------------------------------------
        for h in RETURN_HORIZONS_S:
            f[f"return_{h}s_bps"] = self.mid.return_bps(h * 1000)
        r5, r15, r30 = f["return_5s_bps"], f["return_15s_bps"], f["return_30s_bps"]
        # Momentum: quanto il movimento recente e' coerente con quello medio.
        f["momentum"] = ((r5 + r15) / 2.0) if (r5 is not None and r15 is not None) else None
        # Accelerazione: il movimento recente sta crescendo o esaurendosi?
        f["acceleration"] = ((r5 - r15 / 3.0)
                             if (r5 is not None and r15 is not None) else None)
        f["slope_30s"] = (r30 / 30.0) if r30 is not None else None
        mean30 = self.mid.value_at_or_before(ts - 30_000)
        mu = self.mid.mean()
        f["distance_from_mean_bps"] = (
            (self.mid.last - mu) / mu * 10_000.0 if mu else None)

        # --- volatilita' ---------------------------------------------------
        vol5 = self.mid.realized_vol_bps(5_000)
        vol30 = self.mid.realized_vol_bps(30_000)
        vol300 = self.mid.realized_vol_bps(300_000)
        f["realized_vol_5s_bps"] = vol5
        f["realized_vol_30s_bps"] = vol30
        f["realized_vol_300s_bps"] = vol300
        f["vol_acceleration"] = ((vol5 - vol30)
                                 if (vol5 is not None and vol30 is not None) else None)
        f["vol_ratio"] = (vol5 / vol30) if (vol5 and vol30) else None
        recent = self.mid.slice_since(ts - 60_000)
        if len(recent) >= 5:
            lo, hi = min(recent), max(recent)
            f["range_60s_bps"] = (hi - lo) / lo * 10_000.0 if lo else None
        else:
            f["range_60s_bps"] = None
        # Percentile della volatilita': "alta" ha senso solo rispetto a se stessa.
        vols = [v for v in (self.mid.realized_vol_bps(w)
                            for w in (5_000, 15_000, 30_000, 60_000, 300_000))
                if v is not None]
        f["vol_percentile"] = (
            sum(1 for v in vols if v <= (vol5 or 0)) / len(vols) if vols else None)
        f["vol_compression"] = (
            1.0 if (vol5 is not None and vol300 and vol5 < vol300 * 0.6) else 0.0)
        f["vol_expansion"] = (
            1.0 if (vol5 is not None and vol300 and vol5 > vol300 * 1.6) else 0.0)

        # --- l'orizzonte, che e' cio' che conta davvero --------------------
        horizon_ms = self.cfg.horizon_ms
        lookback = max(horizon_ms * 5, 300_000)
        sigma_h = self.mid.sigma_over(horizon_ms, lookback)
        f["sigma_horizon_bps"] = sigma_h
        f["zero_move_fraction"] = self.mid.zero_move_fraction(horizon_ms, lookback)
        f["expected_move_ticks"] = (
            sigma_h / 10_000.0 * self.mid.last / self.cfg.tick_size
            if (sigma_h and self.cfg.tick_size > 0) else None)

        # --- microstruttura, SOLO se il feed la fornisce --------------------
        f["bid_ask_available"] = 1.0 if self._has_bid_ask else 0.0
        f["spread_bps"] = self.spread.last if self._has_bid_ask else None
        f["spread_change_bps"] = (self.spread.return_bps(10_000)
                                  if self._has_bid_ask and len(self.spread) > 5 else None)
        spread_mean = self.spread.mean() if self._has_bid_ask else None
        f["spread_vs_average"] = (
            (self.spread.last / spread_mean) if (spread_mean and self.spread.last) else None)
        # Il rumore di fondo: sotto questo, un movimento non e' distinguibile
        # da un aggiustamento del book.
        tick_bps = (self.cfg.tick_size / self.mid.last * 10_000.0
                    if self.mid.last else None)
        f["tick_bps"] = tick_bps
        f["noise_floor_bps"] = (
            (f["spread_bps"] or 0.0) + (tick_bps or 0.0) if tick_bps else None)
        f["move_over_noise"] = (
            sigma_h / f["noise_floor_bps"]
            if (sigma_h and f.get("noise_floor_bps")) else None)

        # --- flusso dei tick (l'unico "order flow" davvero disponibile) -----
        for w in FLOW_WINDOWS_S:
            ups = sum(1 for t, _ in self.up_ticks.events if t >= ts - w * 1000)
            downs = sum(1 for t, _ in self.down_ticks.events if t >= ts - w * 1000)
            total = ups + downs
            f[f"tick_imbalance_{w}s"] = ((ups - downs) / total) if total >= 3 else None
        f["quote_velocity_1s"] = sum(
            1 for t, _ in self.quotes.events if t >= ts - 1_000)
        f["quote_velocity_10s"] = sum(
            1 for t, _ in self.quotes.events if t >= ts - 10_000) / 10.0

        # --- statistica ----------------------------------------------------
        rets = self._returns(ts, 60_000)
        f["zscore_60s"] = self._zscore()
        f["autocorr_1"] = self._autocorr(rets, 1)
        f["entropy_60s"] = _entropy(rets)
        f["skew_60s"] = self._moment(rets, 3)
        f["kurtosis_60s"] = self._moment(rets, 4)
        f["trend_strength"] = self._trend_strength()
        f["mean_reversion_strength"] = (
            -f["autocorr_1"] if f["autocorr_1"] is not None else None)

        # --- tempo ---------------------------------------------------------
        lt = time.gmtime(ts / 1000.0)
        f["second_of_minute"] = float(lt.tm_sec)
        f["minute"] = float(lt.tm_min)
        f["hour"] = float(lt.tm_hour)
        f["day_of_week"] = float(lt.tm_wday)
        # Le sessioni contano su EUR/USD: la sovrapposizione Londra-New York e'
        # il momento di massima liquidita' della giornata.
        hour = lt.tm_hour
        f["session_london"] = 1.0 if 7 <= hour < 16 else 0.0
        f["session_newyork"] = 1.0 if 12 <= hour < 21 else 0.0
        f["session_overlap"] = 1.0 if 12 <= hour < 16 else 0.0
        f["session_asia"] = 1.0 if (hour >= 23 or hour < 7) else 0.0
        # Secondi al prossimo minuto: le entrate sono allineate al minuto e la
        # posizione dentro il minuto e' un'informazione reale, non un artificio.
        f["seconds_to_next_minute"] = float(60 - lt.tm_sec)

        vec = FeatureVector(ts=ts, mode=mode, values=f)
        self.last_vector = vec
        self.computed += 1
        return vec

    # ------------------------------------------------------------ supporto
    def _returns(self, ts: int, window_ms: int) -> list[float]:
        vals = self.mid.slice_since(ts - window_ms)
        return [math.log(vals[i] / vals[i - 1]) * 10_000.0
                for i in range(1, len(vals))
                if vals[i] > 0 and vals[i - 1] > 0]

    def _zscore(self) -> float | None:
        mu, sd = self.mid.mean(), self.mid.std()
        if mu is None or not sd:
            return None
        return (self.mid.last - mu) / sd

    @staticmethod
    def _autocorr(rets: list[float], lag: int) -> float | None:
        if len(rets) < lag + 8:
            return None
        mean = sum(rets) / len(rets)
        num = sum((rets[i] - mean) * (rets[i - lag] - mean)
                  for i in range(lag, len(rets)))
        den = sum((r - mean) ** 2 for r in rets)
        return (num / den) if den > 0 else None

    @staticmethod
    def _moment(rets: list[float], order: int) -> float | None:
        n = len(rets)
        if n < 12:
            return None
        mean = sum(rets) / n
        var = sum((r - mean) ** 2 for r in rets) / n
        if var <= 0:
            return None
        sd = math.sqrt(var)
        m = sum(((r - mean) / sd) ** order for r in rets) / n
        return m - 3.0 if order == 4 else m       # curtosi in eccesso

    def _trend_strength(self) -> float | None:
        """R² di una retta sui prezzi recenti: quanto il movimento e' ordinato.

        Distingue un movimento direzionale da un'oscillazione della stessa
        ampiezza — e per una binaria a 60 secondi sono situazioni opposte.
        """
        vals = self.mid.slice_since((self.mid.last_ts or 0) - 60_000)
        n = len(vals)
        if n < 12:
            return None
        xs = list(range(n))
        mx = sum(xs) / n
        my = sum(vals) / n
        sxy = sum((x - mx) * (y - my) for x, y in zip(xs, vals))
        sxx = sum((x - mx) ** 2 for x in xs)
        syy = sum((y - my) ** 2 for y in vals)
        if sxx <= 0 or syy <= 0:
            return None
        r = sxy / math.sqrt(sxx * syy)
        return r * abs(r)                 # segno = direzione, modulo = ordine
