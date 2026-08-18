"""Il Booster: migliora la SELEZIONE, non aumenta il numero di operazioni.

La domanda a cui deve rispondere e' precisa: una volta che il motore ha
individuato un'opportunita', esistono informazioni di brevissimo termine —
negli ultimi secondi prima dell'entrata — che distinguono davvero un segnale
forte da uno debole?

Tre vincoli che ne definiscono il carattere.

**Non crea operazioni.** Riceve un segnale gia' esistente e lo rafforza, lo
indebolisce o lo veta. Un booster che genera segnali propri sarebbe un secondo
motore decisionale non validato.

**Parte in ombra.** In `shadow` osserva, registra e non tocca niente. Si
salvano sia l'esito reale sia quello che si sarebbe ottenuto seguendolo, e si
confrontano. E' l'unico modo di sapere se serve prima di dipenderne.

**La relazione fra punteggio e probabilita' va misurata.** Sommare "64% + 7%
di booster = 71%" e' inventare: il 7 non ha nessuna unita' di misura in comune
con il 64. Qui il punteggio del booster diventa probabilita' solo attraverso
una calibrazione stimata sui dati.
"""

from __future__ import annotations

import json
import math
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from ..ml.calibration import Calibrator, brier_score, fit_platt, wilson_interval
from .validation import binomial_p_value

BOOST, NEUTRAL, DEBOOST, VETO = "BOOST", "NEUTRAL", "DEBOOST", "VETO"
SHADOW, LIVE, OFF = "shadow", "live", "off"


def _two_proportion_p(wins_a: int, n_a: int, wins_b: int, n_b: int) -> float | None:
    """Probabilita' di vedere questa differenza fra due tassi per solo caso.

    Test z a due code sulla proporzione comune. Non e' esatto come Fisher, ma
    su centinaia di osservazioni la differenza e' trascurabile e il costo e'
    nullo — e serve proprio per evitare che due tassi diversi di due punti su
    campioni piccoli vengano scambiati per una capacita' di selezione.
    """
    if n_a < 2 or n_b < 2:
        return None
    p_a, p_b = wins_a / n_a, wins_b / n_b
    pooled = (wins_a + wins_b) / (n_a + n_b)
    var = pooled * (1 - pooled) * (1 / n_a + 1 / n_b)
    if var <= 0:
        return 1.0
    z = (p_a - p_b) / math.sqrt(var)
    # Due code, via la funzione degli errori complementare.
    return math.erfc(abs(z) / math.sqrt(2.0))


@dataclass
class BoosterResult:
    score: float = 0.0                 # -1 (contrario) .. +1 (a favore)
    action: str = NEUTRAL
    confidence: float = 0.0
    base_probability: float = 0.5
    boosted_probability: float = 0.5
    reasons: list[str] = field(default_factory=list)
    latency_ms: float = 0.0
    applied: bool = False              # in ombra resta sempre False
    booster_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": round(self.score, 4), "action": self.action,
            "confidence": round(self.confidence, 4),
            "base_probability": round(self.base_probability, 4),
            "boosted_probability": round(self.boosted_probability, 4),
            "delta": round(self.boosted_probability - self.base_probability, 4),
            "reasons": list(self.reasons),
            "latency_ms": round(self.latency_ms, 3),
            "applied": self.applied, "booster_id": self.booster_id,
        }


class SignalBooster:
    """Legge gli ultimi secondi e giudica la QUALITA' di un segnale esistente."""

    def __init__(self, cfg, booster_id: str | None = None,
                 calibrator: Calibrator | None = None) -> None:
        self.cfg = cfg
        self.booster_id = booster_id or f"booster_v1_{uuid.uuid4().hex[:6]}"
        self.mode = cfg.booster_mode
        self.calibrator = calibrator
        self.validated = False
        #: (punteggio, esito) per stimare la calibrazione e giudicarne l'utilita'.
        self.observations: list[tuple[float, float, bool]] = []

    # ------------------------------------------------------------- giudizio
    def evaluate(self, signal, decision, history: list[dict]) -> BoosterResult:
        """`history` sono le fotografie da T-30 in poi: e' li' che sta il segnale.

        Il valore corrente conta meno della sua DERIVATA: un flusso a +0.7 che
        arriva da +0.1 e' una cosa diversa da un +0.7 che scende da +0.9.
        """
        t0 = time.perf_counter()
        res = BoosterResult(base_probability=decision.probability,
                            booster_id=self.booster_id)
        reasons: list[str] = []
        parts: list[tuple[float, float]] = []      # (peso, contributo)

        direction = 1.0 if signal.direction == "CALL" else -1.0
        f = decision.features

        # 1. Flusso dei tick nella direzione del segnale.
        flow = f.get("tick_imbalance_5s")
        if flow is not None:
            aligned = flow * direction
            parts.append((1.2, max(-1.0, min(1.0, aligned * 1.5))))
            reasons.append(f"flusso {'a favore' if aligned > 0 else 'contrario'} "
                           f"({aligned:+.2f})")

        # 2. Accelerazione del flusso fra le fotografie: la derivata.
        flows = [h.get("order_flow") for h in history if h.get("order_flow") is not None]
        if len(flows) >= 3:
            trend = (flows[-1] - flows[0]) * direction
            parts.append((1.0, max(-1.0, min(1.0, trend * 2.0))))
            reasons.append(f"flusso {'in rafforzamento' if trend > 0 else 'in cedimento'} "
                           f"({trend:+.2f})")

        # 3. Evoluzione della probabilita': si sta rafforzando o morendo?
        probs = [h.get("probability") for h in history if h.get("probability") is not None]
        if len(probs) >= 3:
            drift = probs[-1] - probs[0]
            signed = drift if direction > 0 else -drift
            parts.append((1.1, max(-1.0, min(1.0, signed * 8.0))))
            reasons.append(f"probabilita' {'in crescita' if signed > 0 else 'in calo'} "
                           f"({signed:+.3f})")

        # 4. Momento della microstruttura.
        accel = f.get("acceleration")
        if accel is not None:
            aligned = accel * direction
            noise = f.get("noise_floor_bps") or 0.3
            parts.append((0.8, max(-1.0, min(1.0, aligned / max(noise, 0.1) / 3.0))))

        # 5. Spread: se si allarga proprio adesso, il momento e' peggiore.
        spread_ratio = f.get("spread_vs_average")
        if spread_ratio is not None:
            if spread_ratio > 1.5:
                parts.append((0.9, -0.6))
                reasons.append(f"spread {spread_ratio:.1f}x la media")
            elif spread_ratio < 0.85:
                parts.append((0.5, +0.3))
                reasons.append("spread compresso")

        # 6. Cambio di regime a ridosso dell'entrata: e' un allarme.
        regimes = [h.get("regime") for h in history if h.get("regime")]
        if len(set(regimes)) > 1:
            parts.append((0.7, -0.5))
            reasons.append(f"regime cambiato durante l'attesa ({'->'.join(dict.fromkeys(regimes))})")

        if not parts:
            res.latency_ms = (time.perf_counter() - t0) * 1000.0
            res.reasons = ["nessuna informazione utile negli ultimi secondi"]
            res.boosted_probability = res.base_probability
            return res

        total_w = sum(w for w, _ in parts)
        res.score = sum(w * c for w, c in parts) / total_w
        res.confidence = min(0.9, total_w / 5.0)
        res.reasons = reasons

        # Punteggio -> probabilita': SOLO attraverso una calibrazione stimata.
        # Senza calibratore il booster non ha titolo per spostare un numero, e
        # infatti non lo sposta.
        if self.calibrator is not None:
            base_logit = math.log(max(1e-6, res.base_probability) /
                                  max(1e-6, 1 - res.base_probability))
            adjusted = base_logit + self.calibrator.a * res.score + self.calibrator.b
            res.boosted_probability = 1.0 / (1.0 + math.exp(
                -max(-35.0, min(35.0, adjusted))))
        else:
            res.boosted_probability = res.base_probability
            res.reasons.append("non calibrato: il punteggio non sposta la probabilita'")

        if res.score <= -0.65 and res.confidence > 0.5:
            res.action = VETO
        elif res.score >= 0.35:
            res.action = BOOST
        elif res.score <= -0.25:
            res.action = DEBOOST
        else:
            res.action = NEUTRAL

        # In ombra il segnale live NON viene toccato, mai.
        res.applied = (self.mode == LIVE and self.validated)
        res.latency_ms = (time.perf_counter() - t0) * 1000.0
        return res

    # ----------------------------------------------------------- misurazione
    def record(self, result: BoosterResult, won: bool) -> None:
        self.observations.append((result.score, result.base_probability, won))
        self.observations = self.observations[-5000:]

    def fit_calibration(self, min_samples: int | None = None) -> Calibrator | None:
        """Stima come il punteggio si traduce in probabilita'.

        Finche' non ci sono abbastanza campioni non si calibra: un calibratore
        stimato su cento osservazioni sposterebbe la probabilita' sulla base
        del rumore, con l'aria di essere fondato sui dati.
        """
        need = min_samples or self.cfg.booster_min_samples
        if len(self.observations) < need:
            return None
        scores = [s for s, _, _ in self.observations]
        outcomes = [1 if w else 0 for _, _, w in self.observations]
        # Platt sui punteggi trattati come logit grezzi.
        probs = [1.0 / (1.0 + math.exp(-max(-35.0, min(35.0, s * 2.0))))
                 for s in scores]
        self.calibrator = fit_platt(probs, outcomes)
        return self.calibrator

    def report(self, breakeven: float) -> dict[str, Any]:
        """Il booster migliora davvero la selezione, oppure no?

        Il confronto e' fra il tasso di vittoria di TUTTI i segnali e quello
        dei soli segnali che il booster avrebbe tenuto. Se il secondo non e'
        piu' alto, il booster non seleziona: filtra a caso.
        """
        obs = self.observations
        if len(obs) < 50:
            return {"status": "DATI INSUFFICIENTI", "samples": len(obs),
                    "needed": 50, "mode": self.mode, "validated": self.validated,
                    "booster_id": self.booster_id}
        base_wins = sum(1 for _, _, w in obs if w)
        base_n = len(obs)
        kept = [(s, p, w) for s, p, w in obs if s > -0.25]
        kept_wins = sum(1 for _, _, w in kept if w)
        kept_n = len(kept)
        base_wr = base_wins / base_n
        kept_wr = (kept_wins / kept_n) if kept_n else None
        lo_k, hi_k = wilson_interval(kept_wins, kept_n) if kept_n else (0, 1)

        # Gli scartati sono il vero termine di paragone: se vincono quanto i
        # tenuti, il booster non sta selezionando, sta dividendo a caso.
        dropped_n = base_n - kept_n
        dropped_wins = base_wins - kept_wins
        dropped_wr = (dropped_wins / dropped_n) if dropped_n else None

        payout = self.cfg.payout
        base_exp = base_wr * payout - (1 - base_wr)
        kept_exp = (kept_wr * payout - (1 - kept_wr)) if kept_wr is not None else None

        # "Migliora" richiede una differenza SIGNIFICATIVA, non una differenza.
        #
        # Prima bastava `kept_wr > base_wr + 0.01`: con dati casuali e un filtro
        # casuale, quel confronto risulta vero circa una volta su tre — il
        # booster si dichiarava utile per costruzione. Qui la differenza fra
        # tenuti e scartati passa per un test a due proporzioni, e serve anche
        # che il booster scarti davvero qualcosa: un filtro che tiene tutto ha
        # per forza lo stesso tasso del totale, e non e' un merito.
        sep_p = (_two_proportion_p(kept_wins, kept_n, dropped_wins, dropped_n)
                 if dropped_n >= 20 and kept_n >= 20 else None)
        selective = dropped_n >= 20 and kept_n >= 20
        improves = bool(
            selective and kept_wr is not None and dropped_wr is not None
            and kept_wr > dropped_wr and sep_p is not None and sep_p < 0.05)

        if not selective:
            verdict = ("Il booster non sta separando abbastanza segnali per "
                       "poter essere giudicato: serve che scarti qualcosa.")
        elif improves:
            verdict = ("Il booster seleziona meglio: i segnali che tiene "
                       f"vincono il {kept_wr:.1%} contro il {dropped_wr:.1%} di "
                       f"quelli che scarta, e la differenza e' significativa "
                       f"(p={sep_p:.3f}).")
        else:
            verdict = ("Il booster NON sta selezionando meglio: la differenza "
                       "fra tenuti e scartati e' compatibile con il caso. "
                       "Tenerlo in ombra.")

        return {
            "status": "OK", "booster_id": self.booster_id, "mode": self.mode,
            "validated": self.validated,
            "samples": base_n,
            "base_win_rate": round(base_wr, 4),
            "kept_signals": kept_n,
            "dropped_signals": dropped_n,
            "dropped_win_rate": round(dropped_wr, 4) if dropped_wr is not None else None,
            "boosted_win_rate": round(kept_wr, 4) if kept_wr is not None else None,
            "boosted_ci95": [round(lo_k, 4), round(hi_k, 4)],
            "base_expectancy": round(base_exp, 4),
            "boosted_expectancy": round(kept_exp, 4) if kept_exp is not None else None,
            "p_value_vs_breakeven": (round(binomial_p_value(kept_wins, kept_n, breakeven), 6)
                                     if kept_n else None),
            "p_value_kept_vs_dropped": round(sep_p, 6) if sep_p is not None else None,
            "improves": improves,
            "verdict": verdict,
            "note": ("In ombra il booster non tocca il segnale live: si misura "
                     "cosa sarebbe successo, non si rischia su una supposizione."),
        }

    def can_promote(self, breakeven: float) -> tuple[bool, list[str]]:
        """I requisiti per uscire dall'ombra. Tutti, non alcuni."""
        rep = self.report(breakeven)
        missing: list[str] = []
        if rep.get("status") != "OK":
            return False, ["campione insufficiente"]
        if rep["samples"] < self.cfg.booster_min_samples:
            missing.append(f"servono {self.cfg.booster_min_samples} campioni, "
                           f"ce ne sono {rep['samples']}")
        if not rep["improves"]:
            missing.append("non migliora la selezione rispetto al totale")
        p = rep.get("p_value_vs_breakeven")
        if p is None or p > 0.05:
            missing.append("il vantaggio non e' significativo contro il pareggio")
        if rep.get("boosted_ci95", [0, 1])[0] <= breakeven:
            missing.append("il limite inferiore dell'intervallo non supera il pareggio")
        if self.calibrator is None:
            missing.append("manca la calibrazione punteggio -> probabilita'")
        return (not missing), missing
