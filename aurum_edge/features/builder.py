"""Il costruttore di feature. Un solo percorso per storico e per live.

Questo file ha una regola che vale piu' di tutto il resto: **la riga che il
modello vede in addestramento e la riga che vede in produzione sono prodotte
dalla stessa funzione**. Non da due funzioni "equivalenti": dalla stessa. Il
percorso live e' il percorso storico fermato all'ultimo indice.

Il motivo e' che la differenza fra addestramento e produzione non si manifesta
come un errore: si manifesta come un modello che funzionava benissimo ieri. Se
lo storico normalizza il volume su una finestra di sessanta barre e il live lo
normalizza su quello che ha in memoria, il modello riceve due grandezze diverse
con lo stesso nome, e nessun test se ne accorge.

Struttura:

* `Frame` carica e allinea tutte le serie su una griglia al minuto e precalcola
  gli indicatori in una passata;
* `Frame.row(i)` costruisce il vettore causale all'indice `i`, guardando solo
  `0..i`;
* `build_dataset` attacca le etichette guardando `i+1..i+orizzonte`, ed e'
  l'unica funzione di questo file che tocca il futuro.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Sequence

from .. import config
from ..forecast import regime as regime_mod
from ..util import timeutil
from ..util.numeric import correlation, safe_div
from ..data.store import Store
from . import rolling
from .dataset import Dataset
from .indicators import Bar
from .labels import Outcome, band_bps, horizon_sigma_bps, outcome_from_bars

# --------------------------------------------------------------------------
# Finestre, in barre da un minuto. Cambiare questi numeri cambia il
# significato delle feature: sono parte del contratto con il modello salvato.
# --------------------------------------------------------------------------
W_SHORT, W_MED, W_LONG = 15, 60, 240
EMA_FAST, EMA_MID, EMA_SLOW = 9, 21, 55
RSI_PERIOD = 14
ATR_PERIOD = 14
BB_PERIOD = 20
VWAP_WINDOW = 60
VOL_HIST = 240


def _bar_from_row(row: Any) -> Bar:
    return Bar(ts=row["ts"], open=row["open"], high=row["high"],
               low=row["low"], close=row["close"],
               volume=row["volume"] or 0.0, turnover=row["turnover"] or 0.0)


@dataclass
class Frame:
    """Serie allineate al minuto piu' gli indicatori precalcolati."""

    symbol: str
    bars: list[Bar]
    ts: list[int] = field(default_factory=list)
    closes: list[float] = field(default_factory=list)
    series: dict[str, list[float | None]] = field(default_factory=dict)
    availability: dict[str, float] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.bars)

    # -------------------------------------------------------------- accesso
    def s(self, name: str, i: int) -> float | None:
        col = self.series.get(name)
        if col is None or i < 0 or i >= len(col):
            return None
        v = col[i]
        return v if v is not None and math.isfinite(v) else None

    # --------------------------------------------------------------- la riga
    def row(self, i: int) -> dict[str, float | None]:
        """Il vettore causale all'indice i. Legge solo indici <= i."""
        close = self.closes[i]
        out: dict[str, float | None] = {}

        # ----------------------------------------------------- price action
        for name, back in (("ret_1m", 1), ("ret_5m", 5), ("ret_15m", 15),
                           ("ret_30m", 30), ("ret_60m", 60)):
            j = i - back
            out[name] = (((close / self.closes[j]) - 1.0) * 10_000.0
                         if j >= 0 and self.closes[j] > 0 else None)

        out["rsi_14"] = self.s("rsi_14", i)
        out["rsi_14_dev"] = (None if out["rsi_14"] is None
                             else out["rsi_14"] - 50.0)
        out["macd_hist_bps"] = self.s("macd_hist_bps", i)
        out["macd_hist_slope"] = self.s("macd_hist_slope", i)

        ema_f, ema_m, ema_s = (self.s("ema_fast", i), self.s("ema_mid", i),
                               self.s("ema_slow", i))
        out["ema_fast_mid_bps"] = (((ema_f - ema_m) / close * 10_000.0)
                                   if ema_f and ema_m and close > 0 else None)
        out["ema_mid_slow_bps"] = (((ema_m - ema_s) / close * 10_000.0)
                                   if ema_m and ema_s and close > 0 else None)
        out["price_vs_ema_slow_bps"] = (((close - ema_s) / close * 10_000.0)
                                        if ema_s and close > 0 else None)

        vw = self.s("vwap_60", i)
        out["price_vs_vwap_bps"] = (((close - vw) / close * 10_000.0)
                                    if vw and close > 0 else None)
        out["vwap_dist_sigma"] = self.s("vwap_dist_sigma", i)

        out["bb_z"] = self.s("bb_z", i)
        out["bb_width_pct"] = self.s("bb_width_pct", i)
        out["bb_width_pctile"] = self.s("bb_width_pctile", i)

        atr = self.s("atr_14", i)
        out["atr_bps"] = (atr / close * 10_000.0) if atr and close > 0 else None
        out["atr_pctile"] = self.s("atr_pctile", i)

        out["rv_15_bps"] = self.s("rv_15", i)
        out["rv_60_bps"] = self.s("rv_60", i)
        out["rv_pctile"] = self.s("rv_pctile", i)
        out["compression"] = self.s("compression", i)

        hi = self.s("high_60", i)
        lo = self.s("low_60", i)
        out["range_position"] = (safe_div(close - lo, hi - lo)
                                 if hi is not None and lo is not None else None)
        out["dist_high_60_bps"] = (((close - hi) / close * 10_000.0)
                                   if hi and close > 0 else None)
        out["dist_low_60_bps"] = (((close - lo) / close * 10_000.0)
                                  if lo and close > 0 else None)
        out["streak"] = self.s("streak", i)

        # ------------------------------------------------------------ volume
        out["vol_ratio_60"] = self.s("vol_ratio_60", i)
        out["vol_burst"] = self.s("vol_burst", i)
        out["vol_pctile"] = self.s("vol_pctile", i)
        out["vol_slope_15"] = self.s("vol_slope_15", i)
        out["turnover_pctile"] = self.s("turnover_pctile", i)

        # ------------------------------------------------------- order flow
        out["taker_imb_5m"] = self.s("taker_imb_5m", i)
        out["taker_imb_15m"] = self.s("taker_imb_15m", i)
        out["cvd_slope_15"] = self.s("cvd_slope_15", i)
        out["cvd_z_60"] = self.s("cvd_z_60", i)
        out["cvd_price_div"] = self.s("cvd_price_div", i)
        out["trades_pctile"] = self.s("trades_pctile", i)

        # ------------------------------------------------------- order book
        out["book_imbalance"] = self.s("book_imbalance", i)
        out["book_imbalance_top"] = self.s("book_imbalance_top", i)
        out["spread_bps"] = self.s("spread_bps", i)

        # ---------------------------------------------------- open interest
        out["oi_chg_15m_pct"] = self.s("oi_chg_15m_pct", i)
        out["oi_chg_60m_pct"] = self.s("oi_chg_60m_pct", i)
        out["oi_accel"] = self.s("oi_accel", i)
        out["oi_pctile"] = self.s("oi_pctile", i)
        # L'interazione OI-prezzo e' l'informazione vera dell'open interest.
        # Il segno del prodotto distingue posizioni nuove che spingono
        # (concordi) da chiusure che sgonfiano (discordi).
        oi_chg, ret15 = out["oi_chg_15m_pct"], out["ret_15m"]
        out["oi_price_agree"] = (
            None if oi_chg is None or ret15 is None
            else (1.0 if (oi_chg > 0 and ret15 > 0) or (oi_chg < 0 and ret15 < 0)
                  else -1.0))
        out["oi_price_impulse"] = (None if oi_chg is None or ret15 is None
                                   else oi_chg * ret15)

        # ------------------------------------------------------ derivatives
        out["funding_rate"] = self.s("funding", i)
        out["funding_bps"] = (None if out["funding_rate"] is None
                              else out["funding_rate"] * 10_000.0)
        out["funding_z"] = self.s("funding_z", i)
        out["basis_bps"] = self.s("basis_bps", i)
        out["mins_to_funding"] = self.s("mins_to_funding", i)
        out["ls_ratio"] = self.s("ls_ratio", i)
        out["ls_z"] = self.s("ls_z", i)

        # ------------------------------------------------------ cross market
        out["eth_ret_15m"] = self.s("eth_ret_15m", i)
        out["sol_ret_15m"] = self.s("sol_ret_15m", i)
        out["eth_lead_5m"] = self.s("eth_lead_5m", i)
        out["sol_lead_5m"] = self.s("sol_lead_5m", i)
        out["eth_corr_60"] = self.s("eth_corr_60", i)
        out["breadth_ratio"] = self.s("breadth_ratio", i)

        # ------------------------------------------------------------ tempo
        ts = self.ts[i]
        hour = timeutil.hour_of_day(ts)
        out["hour_sin"] = math.sin(2 * math.pi * hour / 24.0)
        out["hour_cos"] = math.cos(2 * math.pi * hour / 24.0)
        out["is_weekend"] = 1.0 if timeutil.day_of_week(ts) >= 5 else 0.0
        session = timeutil.session_of(ts)
        for s in ("ASIA", "LONDON", "NEWYORK", "OVERLAP"):
            out[f"session_{s.lower()}"] = 1.0 if session == s else 0.0

        # ------------------------------------------------------------- news
        out["news_impact_1h"] = self.s("news_impact_1h", i)
        out["news_direction_1h"] = self.s("news_direction_1h", i)

        return out

    def regime_at(self, i: int) -> regime_mod.Regime:
        return regime_mod.classify(
            vwap_dist_sigma=self.s("vwap_dist_sigma", i),
            ema_spread_bps=self._ema_spread(i),
            vol_percentile=self.s("rv_pctile", i),
            compression=self.s("compression", i))

    def _ema_spread(self, i: int) -> float | None:
        ema_f, ema_s = self.s("ema_fast", i), self.s("ema_slow", i)
        close = self.closes[i]
        if not ema_f or not ema_s or close <= 0:
            return None
        return (ema_f - ema_s) / close * 10_000.0


# --------------------------------------------------------------------------
# Costruzione del frame
# --------------------------------------------------------------------------
def build_frame(store: Store, symbol: str | None = None, *,
                start_ms: int | None = None, end_ms: int | None = None,
                with_live: bool = True) -> Frame:
    """Carica l'archivio e precalcola tutte le serie in una passata."""
    symbol = symbol or config.SYMBOL
    rows = store.bars(symbol, start_ms=start_ms, end_ms=end_ms)
    bars = [_bar_from_row(r) for r in rows]
    frame = Frame(symbol=symbol, bars=bars)
    if not bars:
        return frame

    frame.ts = [b.ts for b in bars]
    frame.closes = [b.close for b in bars]
    closes = frame.closes
    highs = [b.high for b in bars]
    lows = [b.low for b in bars]
    volumes = [b.volume for b in bars]
    turnovers = [b.turnover if b.turnover else b.typical * b.volume
                 for b in bars]
    n = len(bars)
    S = frame.series

    # ------------------------------------------------------------- momentum
    S["rsi_14"] = rolling.rsi_series(closes, RSI_PERIOD)
    S["ema_fast"] = rolling.ema_series(closes, EMA_FAST)
    S["ema_mid"] = rolling.ema_series(closes, EMA_MID)
    S["ema_slow"] = rolling.ema_series(closes, EMA_SLOW)

    ema12 = rolling.ema_series(closes, 12)
    ema26 = rolling.ema_series(closes, 26)
    macd_line: list[float | None] = [
        (f - s) if f is not None and s is not None else None
        for f, s in zip(ema12, ema26)]
    macd_dense = [v for v in macd_line if v is not None]
    signal_dense = rolling.ema_series(macd_dense, 9) if macd_dense else []
    signal: list[float | None] = [None] * n
    k = 0
    for i, v in enumerate(macd_line):
        if v is None:
            continue
        signal[i] = signal_dense[k] if k < len(signal_dense) else None
        k += 1
    S["macd_hist_bps"] = [
        ((m - s) / c * 10_000.0)
        if m is not None and s is not None and c > 0 else None
        for m, s, c in zip(macd_line, signal, closes)]
    S["macd_hist_slope"] = rolling.rolling_slope(S["macd_hist_bps"], 5)

    # ----------------------------------------------------------- volatilita'
    S["atr_14"] = rolling.atr_series(highs, lows, closes, ATR_PERIOD)
    atr_bps: list[float | None] = [
        (a / c * 10_000.0) if a is not None and c > 0 else None
        for a, c in zip(S["atr_14"], closes)]
    S["atr_pctile"] = rolling.rolling_percentile(atr_bps, VOL_HIST)
    S["rv_15"] = rolling.realized_vol_series(closes, W_SHORT)
    S["rv_60"] = rolling.realized_vol_series(closes, W_MED)
    S["rv_pctile"] = rolling.rolling_percentile(S["rv_60"], VOL_HIST)
    S["compression"] = [
        (s / l) if s is not None and l is not None and l > 0 else None
        for s, l in zip(S["rv_15"], S["rv_60"])]

    # ------------------------------------------------------------ bollinger
    bb_mid = rolling.rolling_mean(closes, BB_PERIOD)
    bb_std = rolling.rolling_std(closes, BB_PERIOD)
    S["bb_z"] = [
        ((c - m) / s) if m is not None and s is not None and s > 0 else None
        for c, m, s in zip(closes, bb_mid, bb_std)]
    S["bb_width_pct"] = [
        (4.0 * s / m * 100.0) if m and s is not None and m > 0 else None
        for m, s in zip(bb_mid, bb_std)]
    S["bb_width_pctile"] = rolling.rolling_percentile(S["bb_width_pct"], VOL_HIST)

    # ----------------------------------------------------------------- vwap
    S["vwap_60"] = rolling.rolling_vwap(turnovers, volumes, VWAP_WINDOW)
    # La deviazione ponderata attorno alla VWAP: la distanza in dollari non
    # dice niente, la distanza in sigma dice tutto.
    dev_sq: list[float | None] = []
    for i in range(n):
        v = S["vwap_60"][i]
        dev_sq.append((bars[i].typical - v) ** 2 if v is not None else None)
    mean_dev = rolling.rolling_mean(dev_sq, VWAP_WINDOW)
    S["vwap_dist_sigma"] = []
    for i in range(n):
        v, md = S["vwap_60"][i], mean_dev[i]
        if v is None or md is None or md <= 0:
            S["vwap_dist_sigma"].append(None)
        else:
            S["vwap_dist_sigma"].append((closes[i] - v) / math.sqrt(md))

    # -------------------------------------------------------------- estremi
    S["high_60"] = rolling.rolling_extreme(highs, W_MED, "max")
    S["low_60"] = rolling.rolling_extreme(lows, W_MED, "min")

    streak: list[float | None] = [None] * n
    run = 0
    for i in range(1, n):
        if closes[i] > closes[i - 1]:
            run = run + 1 if run > 0 else 1
        elif closes[i] < closes[i - 1]:
            run = run - 1 if run < 0 else -1
        else:
            run = 0
        streak[i] = float(run)
    S["streak"] = streak

    # --------------------------------------------------------------- volume
    vol_mean = rolling.rolling_mean(volumes, W_MED)
    vol_median = rolling.rolling_median(volumes, VOL_HIST)
    S["vol_ratio_60"] = [
        (v / m) if m and m > 0 else None for v, m in zip(volumes, vol_mean)]
    S["vol_burst"] = [
        (v / m) if m and m > 0 else None for v, m in zip(volumes, vol_median)]
    S["vol_pctile"] = rolling.rolling_percentile(volumes, VOL_HIST)
    S["vol_slope_15"] = rolling.rolling_slope(
        [float(v) for v in volumes], W_SHORT)
    S["turnover_pctile"] = rolling.rolling_percentile(turnovers, VOL_HIST)

    # ---------------------------------------------------------------- flusso
    _attach_flow(store, symbol, frame)

    # ---------------------------------------------------------- open interest
    _attach_open_interest(store, symbol, frame)

    # --------------------------------------------------------------- funding
    _attach_funding(store, symbol, frame)

    # --------------------------------------------------------- long/short
    _attach_account_ratio(store, symbol, frame)

    # ---------------------------------------------------------- cross market
    _attach_context(store, frame)

    # ------------------------------------------------------------ live-only
    if with_live:
        _attach_snapshots(store, symbol, frame)
        _attach_news(store, frame)
    else:
        for name in ("book_imbalance", "book_imbalance_top", "spread_bps",
                     "basis_bps", "breadth_ratio", "news_impact_1h",
                     "news_direction_1h"):
            S.setdefault(name, [None] * n)

    # Copertura: quanto di ogni serie esiste davvero. Non e' diagnostica
    # accessoria, e' cio' che decide quali colonne il modello puo' usare.
    for name, col in S.items():
        present = sum(1 for v in col if v is not None and math.isfinite(v))
        frame.availability[name] = round(present / n, 4) if n else 0.0

    return frame


def _attach_flow(store: Store, symbol: str, frame: Frame) -> None:
    """Taker buy/sell e CVD. Vuoti dove il collector non era acceso."""
    n = len(frame)
    S = frame.series
    rows = store.flow(symbol, start_ms=frame.ts[0] - timeutil.HOUR_MS,
                      end_ms=frame.ts[-1])
    by_ts = {r["ts"]: r for r in rows}

    imb: list[float | None] = []
    cvd: list[float | None] = []
    trades: list[float | None] = []
    for t in frame.ts:
        r = by_ts.get(t)
        if r is None:
            imb.append(None)
            cvd.append(None)
            trades.append(None)
            continue
        total = (r["taker_buy"] or 0.0) + (r["taker_sell"] or 0.0)
        imb.append(((r["taker_buy"] - r["taker_sell"]) / total)
                   if total > 0 else None)
        cvd.append(r["cvd"])
        trades.append(float(r["trades"] or 0))

    S["taker_imb_5m"] = rolling.rolling_mean(imb, 5)
    S["taker_imb_15m"] = rolling.rolling_mean(imb, W_SHORT)
    S["cvd_slope_15"] = rolling.rolling_slope(cvd, W_SHORT)
    S["cvd_z_60"] = rolling.rolling_zscore(cvd, W_MED)
    S["trades_pctile"] = rolling.rolling_percentile(trades, VOL_HIST)

    # Divergenza: il CVD sale e il prezzo no (o viceversa). E' il segnale di
    # assorbimento, e ha senso solo come prodotto dei due segni.
    price_slope = rolling.rolling_slope([float(c) for c in frame.closes], W_SHORT)
    div: list[float | None] = []
    for cs, ps in zip(S["cvd_slope_15"], price_slope):
        if cs is None or ps is None:
            div.append(None)
        else:
            div.append(1.0 if (cs > 0) != (ps > 0) else 0.0)
    S["cvd_price_div"] = div
    S.setdefault("book_imbalance", [None] * n)


def _attach_open_interest(store: Store, symbol: str, frame: Frame) -> None:
    """OI a passo 5 minuti riportato al minuto, sempre all'indietro."""
    S = frame.series
    rows = store.open_interest(symbol, "5min",
                               start_ms=frame.ts[0] - 4 * timeutil.HOUR_MS,
                               end_ms=frame.ts[-1])
    src_ts = [r["ts"] for r in rows]
    src_val = [r["value"] for r in rows]
    # Un valore piu' vecchio di venti minuti non e' l'open interest di adesso.
    oi = rolling.align_step_series(frame.ts, src_ts, src_val,
                                   max_age_ms=20 * timeutil.MINUTE_MS)
    S["open_interest"] = oi

    def chg(lag: int) -> list[float | None]:
        out: list[float | None] = []
        for i in range(len(oi)):
            j = i - lag
            if j < 0 or oi[i] is None or oi[j] is None or oi[j] == 0:
                out.append(None)
            else:
                out.append((oi[i] / oi[j] - 1.0) * 100.0)
        return out

    S["oi_chg_15m_pct"] = chg(15)
    S["oi_chg_60m_pct"] = chg(60)
    # Accelerazione: la pendenza della pendenza. L'open interest che sale non
    # dice molto; l'open interest che sale sempre piu' in fretta si'.
    slope = rolling.rolling_slope(oi, 30)
    S["oi_accel"] = rolling.rolling_slope(slope, 15)
    S["oi_pctile"] = rolling.rolling_percentile(oi, VOL_HIST)


def _attach_funding(store: Store, symbol: str, frame: Frame) -> None:
    S = frame.series
    rows = store.funding(symbol, start_ms=frame.ts[0] - 30 * timeutil.DAY_MS,
                         end_ms=frame.ts[-1])
    src_ts = [r["ts"] for r in rows]
    src_val = [r["rate"] for r in rows]
    # Il funding vale otto ore: nove ore di tolleranza coprono il ritardo di
    # pubblicazione senza far sopravvivere un valore di ieri.
    funding = rolling.align_step_series(frame.ts, src_ts, src_val,
                                        max_age_ms=9 * timeutil.HOUR_MS)
    S["funding"] = funding
    # Lo z-score su trenta giorni: il funding assoluto e' quasi inutile, la sua
    # posizione rispetto al proprio recente e' quello che segnala un estremo.
    S["funding_z"] = rolling.rolling_zscore(funding, 30 * 24 * 60)

    mins: list[float | None] = []
    for t in frame.ts:
        nxt = None
        for s in src_ts:
            if s > t:
                nxt = s
                break
        if nxt is None:
            # Il funding di Bybit e' ogni otto ore a 00:00, 08:00, 16:00 UTC.
            hours = timeutil.hour_of_day(t)
            next_h = ((hours // 8) + 1) * 8
            mins.append(float(next_h * 60 - (hours * 60 +
                        (t % timeutil.HOUR_MS) // 60_000)))
        else:
            mins.append((nxt - t) / 60_000.0)
    S["mins_to_funding"] = mins


def _attach_account_ratio(store: Store, symbol: str, frame: Frame) -> None:
    S = frame.series
    rows = store.account_ratio(symbol, "5min",
                               start_ms=frame.ts[0] - timeutil.DAY_MS)
    src_ts = [r["ts"] for r in rows]
    ratios = [safe_div(r["buy_ratio"], r["sell_ratio"]) for r in rows]
    clean_ts = [t for t, v in zip(src_ts, ratios) if v is not None]
    clean_v = [v for v in ratios if v is not None]
    ls = rolling.align_step_series(frame.ts, clean_ts, clean_v,
                                   max_age_ms=30 * timeutil.MINUTE_MS)
    S["ls_ratio"] = ls
    S["ls_z"] = rolling.rolling_zscore(ls, 24 * 60)


def _attach_context(store: Store, frame: Frame) -> None:
    """ETH e SOL sulla stessa griglia, piu' il lead-lag rispetto a BTC."""
    S = frame.series
    n = len(frame)
    btc_ret5 = [None if i < 5 or frame.closes[i - 5] <= 0
                else (frame.closes[i] / frame.closes[i - 5] - 1.0) * 10_000.0
                for i in range(n)]

    for sym, tag in ((config.CONTEXT_SYMBOLS[0], "eth"),
                     (config.CONTEXT_SYMBOLS[1], "sol")):
        rows = store.bars(sym, start_ms=frame.ts[0] - timeutil.HOUR_MS,
                          end_ms=frame.ts[-1])
        by_ts = {r["ts"]: r["close"] for r in rows}
        closes = rolling.align_step_series(
            frame.ts, [r["ts"] for r in rows], [r["close"] for r in rows],
            max_age_ms=10 * timeutil.MINUTE_MS)

        ret15: list[float | None] = []
        ret5: list[float | None] = []
        for i in range(n):
            for lag, dest in ((15, ret15), (5, ret5)):
                j = i - lag
                if j < 0 or closes[i] is None or closes[j] is None or closes[j] <= 0:
                    dest.append(None)
                else:
                    dest.append((closes[i] / closes[j] - 1.0) * 10_000.0)
        S[f"{tag}_ret_15m"] = ret15
        # Lead-lag: quanto l'altro mercato si e' mosso PIU' di BTC nella stessa
        # finestra. Se l'alt guida, questo numero e' positivo prima che BTC segua.
        S[f"{tag}_lead_5m"] = [
            None if a is None or b is None else a - b
            for a, b in zip(ret5, btc_ret5)]
        if tag == "eth":
            corr: list[float | None] = [None] * n
            for i in range(n):
                if i < W_MED:
                    continue
                corr[i] = correlation(
                    [frame.closes[j] for j in range(i - W_MED, i + 1)],
                    [closes[j] for j in range(i - W_MED, i + 1)])
            S["eth_corr_60"] = corr
        _ = by_ts


def _attach_snapshots(store: Store, symbol: str, frame: Frame) -> None:
    """Libro, spread, base e breadth: esistono solo da quando si raccoglie."""
    import json as _json

    S = frame.series
    n = len(frame)
    rows = store.snapshots(symbol, start_ms=frame.ts[0] - timeutil.HOUR_MS,
                           limit=200_000)
    if not rows:
        for name in ("book_imbalance", "book_imbalance_top", "spread_bps",
                     "basis_bps", "breadth_ratio"):
            S[name] = [None] * n
        return

    src_ts = [r["ts"] for r in rows]
    fields = {
        "book_imbalance": [r["book_imbalance"] for r in rows],
        "book_imbalance_top": [r["book_imbalance_top"] for r in rows],
        "spread_bps": [r["spread_bps"] for r in rows],
        "basis_bps": [r["basis_bps"] for r in rows],
    }
    breadth: list[float | None] = []
    for r in rows:
        try:
            payload = _json.loads(r["payload"] or "{}")
            b = payload.get("breadth") or {}
            breadth.append(b.get("breadth_ratio") if b.get("available") else None)
        except (ValueError, TypeError):
            breadth.append(None)
    fields["breadth_ratio"] = breadth

    for name, values in fields.items():
        ts_clean = [t for t, v in zip(src_ts, values) if v is not None]
        v_clean = [v for v in values if v is not None]
        S[name] = rolling.align_step_series(
            frame.ts, ts_clean, v_clean,
            max_age_ms=max(config.STALE_SECONDS * 1000, 2 * timeutil.MINUTE_MS))


def _attach_news(store: Store, frame: Frame) -> None:
    """Impatto e direzione delle news nell'ora precedente ogni barra."""
    S = frame.series
    n = len(frame)
    rows = store.news(since_ms=frame.ts[0] - timeutil.DAY_MS, limit=5000)
    if not rows:
        S["news_impact_1h"] = [None] * n
        S["news_direction_1h"] = [None] * n
        return

    items = sorted(((r["ts"], r["impact"] or 0.0,
                     {"BULL": 1.0, "BEAR": -1.0}.get(r["direction"] or "", 0.0))
                    for r in rows), key=lambda x: x[0])
    impact: list[float | None] = []
    direction: list[float | None] = []
    lo = 0
    hi = 0
    for t in frame.ts:
        window_start = t - timeutil.HOUR_MS
        while hi < len(items) and items[hi][0] <= t:
            hi += 1
        while lo < hi and items[lo][0] < window_start:
            lo += 1
        window = items[lo:hi]
        if not window:
            impact.append(0.0)
            direction.append(0.0)
        else:
            impact.append(min(1.0, sum(w[1] for w in window)))
            weighted = sum(w[1] * w[2] for w in window)
            total = sum(w[1] for w in window)
            direction.append(weighted / total if total > 0 else 0.0)
    S["news_impact_1h"] = impact
    S["news_direction_1h"] = direction


# --------------------------------------------------------------------------
# Dataset
# --------------------------------------------------------------------------
FEATURE_ORDER: list[str] | None = None


def feature_names(frame: Frame) -> list[str]:
    """L'ordine delle colonne. Stabile, perche' i pesi salvati vi si appoggiano."""
    if not len(frame):
        return []
    return sorted(frame.row(min(len(frame) - 1, 300)).keys())


def build_dataset(frame: Frame, *, horizon_min: int | None = None,
                  warmup: int = VOL_HIST + 10,
                  step: int = 1,
                  min_coverage: float = 0.80) -> Dataset:
    """Righe causali + etichette a orizzonte. L'unica funzione che vede il futuro.

    `warmup` scarta l'inizio della serie, dove le finestre lunghe non sono
    ancora piene: quelle righe non sono sbagliate, sono meno informate, e
    mescolarle con le altre significa addestrare su una versione depotenziata
    delle stesse feature.
    """
    horizon_min = horizon_min or config.PRIMARY_HORIZON_MIN
    n = len(frame)
    names = feature_names(frame)
    ds = Dataset(names=names, rows=[], ts=[], labels=[], prices=[],
                 outcomes=[], regimes=[], horizon_min=horizon_min)
    if n <= warmup + horizon_min:
        ds.notes["status"] = "DATI INSUFFICIENTI"
        ds.notes["reason"] = (
            f"{n} barre in archivio: servono almeno {warmup + horizon_min + 1} "
            "per avere una sola riga con le finestre lunghe piene e "
            "un'etichetta completa.")
        return ds

    last = n - horizon_min - 1
    for i in range(warmup, last + 1, step):
        row = frame.row(i)
        # La banda FLAT usa la volatilita' stimata SUL PASSATO, non su tutto il
        # campione: usare la sigma globale farebbe entrare la volatilita'
        # futura nella definizione stessa dell'etichetta.
        sigma = horizon_sigma_bps(frame.s("rv_60", i), horizon_min,
                                  config.BAR_MINUTES)
        band = band_bps(sigma)
        future = frame.bars[i + 1:i + 1 + horizon_min]
        outcome = outcome_from_bars(future, frame.closes[i], horizon_min, band)

        ds.rows.append([row.get(name) for name in names])
        ds.ts.append(frame.ts[i])
        ds.labels.append(outcome.label)
        ds.prices.append(frame.closes[i])
        ds.outcomes.append(outcome)
        ds.regimes.append(regime_mod.bucket(frame.regime_at(i).name))

    ds.notes["status"] = "OK"
    ds.notes["warmup_bars"] = warmup
    ds.notes["step"] = step
    ds.notes["availability"] = frame.availability
    return ds.drop_sparse(min_coverage)


def live_row(frame: Frame) -> tuple[dict[str, float | None], int, float,
                                    regime_mod.Regime] | None:
    """L'ultima riga disponibile: la stessa funzione dello storico, all'ultimo indice."""
    if not len(frame):
        return None
    i = len(frame) - 1
    return (frame.row(i), frame.ts[i], frame.closes[i], frame.regime_at(i))
