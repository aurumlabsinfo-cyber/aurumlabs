"""La qualita' del segnale: quanto ci si puo' fidare di questa singola previsione.

Non e' la probabilita'. La probabilita' dice quanto e' probabile il rialzo; la
qualita' dice quanto e' affidabile quella probabilita'. Sono due cose diverse e
tenerle separate e' importante: un 72% calcolato su dati vecchi di dieci minuti,
in un regime non riconosciuto, con meta' delle feature mancanti, e' un numero
che non vale il 72%, e va mostrato come tale invece di essere corretto in
silenzio.

Cinque componenti, ognuna in [0, 1], combinate con il **minimo pesato** e non
con la media: una media lascia che una componente eccellente compensi una
disastrosa, e qui non e' cosi'. Dati fermi da mezz'ora non si compensano con un
regime chiarissimo.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .. import config
from ..util.numeric import clamp


@dataclass
class Quality:
    score: float
    components: dict[str, float]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": round(self.score, 3),
            "label": self.label,
            "components": {k: round(v, 3) for k, v in self.components.items()},
            "warnings": self.warnings,
        }

    @property
    def label(self) -> str:
        if self.score >= 0.80:
            return "ALTA"
        if self.score >= 0.60:
            return "MEDIA"
        if self.score >= 0.40:
            return "BASSA"
        return "INSUFFICIENTE"


def assess(*, data_age_seconds: float | None, feature_coverage: float,
           regime_confidence: float, model_available: bool,
           model_holdout_ok: bool, spread_bps: float | None,
           bars_available: int, edge_support: bool) -> Quality:
    """Calcola la qualita' e spiega ogni penalizzazione."""
    components: dict[str, float] = {}
    warnings: list[str] = []

    # 1. Freschezza. Decade linearmente fino alla soglia di obsolescenza.
    if data_age_seconds is None:
        components["freschezza"] = 0.0
        warnings.append("nessun dato live: la previsione si basa solo "
                        "sull'archivio")
    else:
        age_ratio = data_age_seconds / max(config.STALE_SECONDS, 1)
        components["freschezza"] = clamp(1.0 - age_ratio, 0.0, 1.0)
        if age_ratio > 0.5:
            warnings.append(
                f"dati fermi da {data_age_seconds:.0f}s "
                f"(limite {config.STALE_SECONDS}s)")

    # 2. Copertura delle feature.
    components["copertura"] = clamp(feature_coverage, 0.0, 1.0)
    if feature_coverage < 0.75:
        warnings.append(
            f"solo il {feature_coverage:.0%} delle variabili e' disponibile: "
            "flusso ordini, libro o open interest potrebbero mancare")

    # 3. Regime riconosciuto.
    components["regime"] = clamp(regime_confidence, 0.0, 1.0)
    if regime_confidence < 0.45:
        warnings.append("regime di mercato non chiaramente riconoscibile")

    # 4. Il modello. Senza un modello che ha superato l'holdout, la
    #    probabilita' e' un'opinione ben formattata.
    if not model_available:
        components["modello"] = 0.0
        warnings.append("nessun modello addestrato: eseguire la ricerca")
    elif not model_holdout_ok:
        components["modello"] = 0.45
        warnings.append("il modello in uso non ha superato l'holdout fresco: "
                        "le sue probabilita' non sono confermate fuori campione")
    else:
        components["modello"] = 1.0

    # 5. Condizioni di mercato: uno spread anomalo segnala un libro sottile,
    #    dove tutte le misure di flusso diventano rumorose.
    if spread_bps is None:
        components["mercato"] = 0.7
    else:
        components["mercato"] = clamp(1.0 - (spread_bps - 0.5) / 5.0, 0.2, 1.0)
        if spread_bps > 3.0:
            warnings.append(f"spread largo ({spread_bps:.2f} bps): "
                            "liquidita' sottile")

    # 6. Storia sufficiente per gli indicatori lunghi.
    components["storia"] = clamp(bars_available / 500.0, 0.0, 1.0)
    if bars_available < 300:
        warnings.append(f"solo {bars_available} barre in memoria: gli "
                        "indicatori a finestra lunga sono ancora parziali")

    if edge_support:
        components["conferma_edge"] = 1.0
    else:
        components["conferma_edge"] = 0.65

    # Il minimo tira giu' il risultato quando una componente e' rotta; la media
    # evita che una sola imperfezione azzeri tutto. La combinazione e' due terzi
    # minimo, un terzo media.
    values = list(components.values())
    worst = min(values)
    avg = sum(values) / len(values)
    score = 0.65 * worst + 0.35 * avg
    return Quality(score=clamp(score, 0.0, 1.0), components=components,
                   warnings=warnings)
