"""Il regime di mercato: due assi, sei stati, nessuna pretesa.

Un regime non e' una previsione: e' il contesto in cui una previsione va
giudicata. Serve a una cosa sola, ma decisiva: un pattern che funziona in
espansione di volatilita' e perde in compressione non e' un pattern che
"funziona a volte". E' due pattern diversi, e mescolarli produce una media che
non descrive nessuno dei due.

Gli assi sono due perche' sono i due che cambiano il segno delle cose:

* **tendenza**: il prezzo va da qualche parte, o torna indietro? Misurata sulla
  distanza dalla VWAP in unita' di volatilita', non in dollari.
* **volatilita'**: rispetto a se' stessa nelle ultime ore, non in assoluto.

Il terzo asse, la compressione, non e' uno stato: e' un avvertimento. Segnala
che lo stato attuale probabilmente sta per finire.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

TREND_UP = "TREND_RIALZISTA"
TREND_DOWN = "TREND_RIBASSISTA"
RANGE = "LATERALE"
EXPANSION = "ESPANSIONE"
COMPRESSION = "COMPRESSIONE"
UNKNOWN = "SCONOSCIUTO"


@dataclass(frozen=True)
class Regime:
    name: str
    trend: str
    volatility: str
    compressed: bool
    confidence: float
    detail: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name, "trend": self.trend,
            "volatility": self.volatility, "compressed": self.compressed,
            "confidence": round(self.confidence, 3), "detail": self.detail,
        }


def classify(*, vwap_dist_sigma: float | None, ema_spread_bps: float | None,
             vol_percentile: float | None,
             compression: float | None,
             adx_like: float | None = None) -> Regime:
    """Classifica il regime dai soli valori causali passati.

    `vwap_dist_sigma`: distanza dalla VWAP in deviazioni ponderate.
    `ema_spread_bps`: EMA veloce meno EMA lenta, in bps sul prezzo.
    `vol_percentile`: posizione della volatilita' recente nella sua storia.
    `compression`: volatilita' corta / volatilita' lunga.
    """
    known = sum(1 for v in (vwap_dist_sigma, ema_spread_bps, vol_percentile)
                if v is not None)
    if known < 2:
        return Regime(UNKNOWN, UNKNOWN, UNKNOWN, False, 0.0,
                      {"reason": "dati insufficienti per classificare il regime"})

    # Tendenza: serve accordo fra la posizione rispetto alla VWAP e la
    # separazione delle medie. Uno solo dei due basta a fare rumore.
    trend = RANGE
    strength = 0.0
    if vwap_dist_sigma is not None and ema_spread_bps is not None:
        up = vwap_dist_sigma > 0.5 and ema_spread_bps > 2.0
        down = vwap_dist_sigma < -0.5 and ema_spread_bps < -2.0
        if up:
            trend = TREND_UP
        elif down:
            trend = TREND_DOWN
        strength = min(1.0, (abs(vwap_dist_sigma) / 2.0 +
                             min(abs(ema_spread_bps) / 20.0, 1.0)) / 2.0)
    elif ema_spread_bps is not None:
        trend = (TREND_UP if ema_spread_bps > 5.0
                 else TREND_DOWN if ema_spread_bps < -5.0 else RANGE)
        strength = min(1.0, abs(ema_spread_bps) / 20.0) * 0.6

    volatility = UNKNOWN
    if vol_percentile is not None:
        volatility = (EXPANSION if vol_percentile > 0.70
                      else COMPRESSION if vol_percentile < 0.30
                      else "NORMALE")

    compressed = bool(compression is not None and compression < 0.70)

    if trend == RANGE:
        name = f"{RANGE}/{volatility}"
    else:
        name = f"{trend}/{volatility}"

    confidence = min(1.0, 0.35 + 0.45 * strength +
                     (0.20 if vol_percentile is not None else 0.0))
    return Regime(
        name=name, trend=trend, volatility=volatility, compressed=compressed,
        confidence=confidence,
        detail={
            "vwap_dist_sigma": (round(vwap_dist_sigma, 3)
                                if vwap_dist_sigma is not None else None),
            "ema_spread_bps": (round(ema_spread_bps, 2)
                               if ema_spread_bps is not None else None),
            "vol_percentile": (round(vol_percentile, 3)
                               if vol_percentile is not None else None),
            "compression": (round(compression, 3)
                            if compression is not None else None),
            "adx_like": round(adx_like, 2) if adx_like is not None else None,
        })


def bucket(regime_name: str) -> str:
    """Raggruppa i regimi in poche classi, per le statistiche per regime.

    Con sei regimi e trecento osservazioni indipendenti, alcuni gruppi
    resterebbero a venti casi: si stimerebbe il rumore. Tre gruppi sono il
    massimo che un campione realistico regge.
    """
    if regime_name.startswith(TREND_UP) or regime_name.startswith(TREND_DOWN):
        return "TREND"
    if regime_name.startswith(RANGE):
        return "RANGE"
    return "SCONOSCIUTO"
