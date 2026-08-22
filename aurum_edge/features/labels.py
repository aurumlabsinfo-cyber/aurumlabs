"""Etichette. Il posto in cui si decide che cosa significa "aver ragione".

Tre scelte, e ognuna cambia il risultato piu' di qualunque modello:

**1. Tre classi, non due.** Prevedere "sale o scende" costringe a dire qualcosa
anche quando non succede niente, e il mercato per gran parte del tempo non fa
niente. La terza classe, FLAT, e' cio' che rende WAIT una risposta corretta e
misurabile invece di una rinuncia.

**2. La banda e' dinamica.** Il confine fra "si e' mosso" e "e' rumore" scala
con la volatilita' del momento: 15 bps in un'ora di agosto sono un movimento,
in un'ora di dati macro sono il respiro normale. La banda e'
`max(pavimento, k * sigma_orizzonte)`, con sigma stimata **solo sul passato**.

**3. Si misura anche il percorso, non solo l'arrivo.** MFE e MAE — quanto e'
andato a favore e quanto contro prima di arrivare — sono cio' che distingue una
previsione utile da una che finisce nel posto giusto dopo essere passata dieci
volte dalla parte sbagliata. Il tempo al MFE e' la stima della durata.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Sequence

from .. import config
from .indicators import Bar

LONG, SHORT, FLAT = "LONG", "SHORT", "FLAT"


@dataclass(frozen=True)
class Outcome:
    """Cosa e' successo davvero, fra t e t+orizzonte."""

    horizon_min: int
    entry: float
    exit: float | None
    return_bps: float | None
    band_bps: float
    label: str | None
    mfe_bps: float | None          # massimo movimento favorevole al rialzo
    mae_bps: float | None          # massimo movimento avverso (al ribasso)
    time_to_mfe_min: float | None
    time_to_mae_min: float | None
    bars_seen: int
    complete: bool                 # False se la finestra e' troncata

    def to_dict(self) -> dict[str, Any]:
        return {
            "horizon_min": self.horizon_min, "entry": self.entry,
            "exit": self.exit, "return_bps": self.return_bps,
            "band_bps": self.band_bps, "label": self.label,
            "mfe_bps": self.mfe_bps, "mae_bps": self.mae_bps,
            "time_to_mfe_min": self.time_to_mfe_min,
            "time_to_mae_min": self.time_to_mae_min,
            "bars_seen": self.bars_seen, "complete": self.complete,
        }

    def signed_mfe(self, direction: str) -> float | None:
        """MFE nel verso della previsione: per SHORT il favorevole e' il ribasso."""
        if direction == LONG:
            return self.mfe_bps
        if direction == SHORT:
            return None if self.mae_bps is None else -self.mae_bps
        return None

    def signed_mae(self, direction: str) -> float | None:
        if direction == LONG:
            return self.mae_bps
        if direction == SHORT:
            return None if self.mfe_bps is None else -self.mfe_bps
        return None


def band_bps(sigma_bps: float | None,
             min_bps: float | None = None,
             k: float | None = None) -> float:
    """La semi-larghezza della zona FLAT, in punti base.

    `sigma_bps` e' la volatilita' per barra stimata sul passato; l'orizzonte
    scala con la radice del tempo, come vuole un cammino casuale. Non e' un
    modello del mercato: e' il riferimento rispetto a cui un movimento va
    giudicato "piu' grande del normale".
    """
    floor = config.LABEL_BAND_MIN_BPS if min_bps is None else min_bps
    kk = config.LABEL_BAND_SIGMA_K if k is None else k
    if sigma_bps is None or not math.isfinite(sigma_bps) or sigma_bps <= 0:
        return floor
    return max(floor, kk * sigma_bps)


def horizon_sigma_bps(per_bar_sigma_bps: float | None,
                      horizon_min: int, bar_minutes: int = 1) -> float | None:
    """Volatilita' attesa sull'orizzonte, scalata con la radice del tempo."""
    if per_bar_sigma_bps is None or per_bar_sigma_bps <= 0:
        return None
    bars = max(1.0, horizon_min / max(bar_minutes, 1))
    return per_bar_sigma_bps * math.sqrt(bars)


def outcome_from_bars(future: Sequence[Bar], entry: float, horizon_min: int,
                      band: float) -> Outcome:
    """Costruisce l'esito dalle barre FUTURE. E' l'unico posto che le guarda.

    Concentrare qui ogni lettura del futuro non e' eleganza: e' cio' che rende
    verificabile che nessun'altra parte del sistema lo faccia. Il costruttore di
    feature non importa questo modulo, e un test lo controlla.
    """
    if entry <= 0 or not future:
        return Outcome(horizon_min, entry, None, None, band, None, None, None,
                       None, None, 0, False)

    exit_price = future[-1].close
    ret = (exit_price / entry - 1.0) * 10_000.0

    best = worst = 0.0
    t_best = t_worst = 0.0
    for i, bar in enumerate(future, start=1):
        up = (bar.high / entry - 1.0) * 10_000.0
        down = (bar.low / entry - 1.0) * 10_000.0
        if up > best:
            best, t_best = up, float(i)
        if down < worst:
            worst, t_worst = down, float(i)

    label = LONG if ret > band else SHORT if ret < -band else FLAT
    complete = len(future) >= horizon_min

    return Outcome(
        horizon_min=horizon_min, entry=entry, exit=exit_price,
        return_bps=ret, band_bps=band, label=label,
        mfe_bps=best, mae_bps=worst,
        time_to_mfe_min=t_best if best > 0 else None,
        time_to_mae_min=t_worst if worst < 0 else None,
        bars_seen=len(future), complete=complete,
    )


def label_index(label: str | None) -> int | None:
    return {SHORT: 0, FLAT: 1, LONG: 2}.get(label or "")


def index_label(idx: int) -> str:
    return {0: SHORT, 1: FLAT, 2: LONG}.get(idx, FLAT)


def class_distribution(labels: Sequence[str | None]) -> dict[str, Any]:
    """La distribuzione delle classi e la classe piu' frequente.

    E' il riferimento contro cui misurare qualunque modello: se il 62% delle
    finestre e' FLAT, un modello che dice sempre FLAT e' accurato al 62%, e
    ogni confronto con il 50% e' una consolazione.
    """
    counts = {SHORT: 0, FLAT: 0, LONG: 0}
    total = 0
    for lab in labels:
        if lab in counts:
            counts[lab] += 1
            total += 1
    if total == 0:
        return {"total": 0, "counts": counts, "base_rate": None,
                "majority": None}
    majority = max(counts, key=lambda k: counts[k])
    return {
        "total": total,
        "counts": counts,
        "shares": {k: round(v / total, 4) for k, v in counts.items()},
        "base_rate": round(counts[majority] / total, 4),
        "majority": majority,
        "directional_share": round(
            (counts[LONG] + counts[SHORT]) / total, 4),
    }
