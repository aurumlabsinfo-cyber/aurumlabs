"""Gli agenti: ognuno guarda una fetta del mercato e dice cosa ne pensa.

Convenzione unica, rispettata da tutti:

    direction  -1.0 = PUT forte, 0.0 = neutrale, +1.0 = CALL forte
    confidence  0.0 - 1.0, quanto l'agente crede a quello che dice
    reliability 0.0 - 1.0, quanto ha avuto ragione in passato IN QUESTO REGIME

`confidence` e `reliability` sono cose diverse e vanno tenute separate: un
agente puo' essere molto sicuro e storicamente inaffidabile. Il peso effettivo
nasce dal prodotto delle due, non da una sola.

Un agente che non ha i dati per parlare **si astiene** (`abstain`). Astenersi
non e' un fallimento: e' l'unica risposta onesta quando il feed non fornisce
la microstruttura, e produce un NO_TRADE spiegato invece di un numero inventato.
"""

from __future__ import annotations

import math
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from .features import FeatureVector

CALL, PUT, NEUTRAL = "CALL", "PUT", "NEUTRAL"

# Regimi possibili.
TREND = "TREND"
RANGE = "RANGE"
BREAKOUT = "BREAKOUT"
HIGH_VOLATILITY = "HIGH_VOLATILITY"
LOW_VOLATILITY = "LOW_VOLATILITY"
UNCERTAIN = "UNCERTAIN"
REGIMES = (TREND, RANGE, BREAKOUT, HIGH_VOLATILITY, LOW_VOLATILITY, UNCERTAIN)


def squash(x: float, scale: float = 1.0) -> float:
    """Comprime in (-1, 1) senza saturare bruscamente."""
    return math.tanh(x / scale) if scale else 0.0


@dataclass
class AgentOpinion:
    agent: str
    direction: float = 0.0
    confidence: float = 0.0
    reliability: float = 0.5
    reason: str = ""
    latency_ms: float = 0.0
    abstained: bool = False
    features_used: tuple[str, ...] = ()
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def side(self) -> str:
        if self.abstained or self.direction == 0.0:
            return NEUTRAL
        return CALL if self.direction > 0 else PUT

    @property
    def weight(self) -> float:
        """Quanto conta questa opinione: sicurezza per affidabilita'."""
        return 0.0 if self.abstained else self.confidence * self.reliability

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent": self.agent, "direction": round(self.direction, 4),
            "confidence": round(self.confidence, 4),
            "reliability": round(self.reliability, 4),
            "side": self.side, "reason": self.reason,
            "latency_ms": round(self.latency_ms, 3),
            "abstained": self.abstained, "weight": round(self.weight, 4),
            **({"extra": self.extra} if self.extra else {}),
        }


class Agent(ABC):
    """Base comune. Misura la propria latenza e non solleva mai eccezioni."""

    name: str = "agent"
    base_weight: float = 1.0
    #: Feature indispensabili: se mancano, l'agente si astiene.
    requires: tuple[str, ...] = ()

    def __init__(self, cfg) -> None:
        self.cfg = cfg

    def evaluate(self, f: FeatureVector, regime: str,
                 reliability: float = 0.5) -> AgentOpinion:
        started = time.perf_counter()
        try:
            if self.requires and not f.has(*self.requires):
                missing = [r for r in self.requires if f.values.get(r) is None]
                op = self.abstain(f"dati mancanti: {', '.join(missing)}")
            else:
                op = self._evaluate(f, regime)
        except Exception as exc:  # noqa: BLE001 - un agente rotto non ferma il motore
            op = self.abstain(f"errore interno: {type(exc).__name__}: {exc}")
        op.latency_ms = (time.perf_counter() - started) * 1000.0
        op.reliability = reliability
        return op

    @abstractmethod
    def _evaluate(self, f: FeatureVector, regime: str) -> AgentOpinion:
        ...

    def emit(self, direction: float, confidence: float, reason: str,
             used: tuple[str, ...] = (), extra: dict | None = None) -> AgentOpinion:
        return AgentOpinion(
            agent=self.name,
            direction=max(-1.0, min(1.0, direction)),
            confidence=max(0.0, min(1.0, confidence)),
            reason=reason, features_used=used, extra=extra or {})

    def abstain(self, reason: str) -> AgentOpinion:
        return AgentOpinion(agent=self.name, direction=0.0, confidence=0.0,
                            reason=reason, abstained=True)


# --------------------------------------------------------------------------- #
#  AGENTI DIREZIONALI
# --------------------------------------------------------------------------- #

class TrendAgent(Agent):
    """La direzione del movimento ordinato sui minuti, non sui secondi."""

    name, base_weight = "trend", 1.2
    requires = ("trend_strength",)

    def _evaluate(self, f: FeatureVector, regime: str) -> AgentOpinion:
        strength = f.get("trend_strength", 0.0)
        r30 = f.get("return_30s_bps", 0.0)
        r60 = f.get("return_60s_bps", 0.0)
        # Il trend conta se e' ordinato E se ha ampiezza: un R² alto su un
        # movimento di mezzo punto base non e' una tendenza, e' rumore ordinato.
        noise = f.get("noise_floor_bps") or 0.3
        amplitude = abs(r60) / max(noise, 0.05)
        direction = squash(strength * 3.0, 1.0) * min(1.0, amplitude / 3.0)
        confidence = min(0.9, abs(direction) * 0.9 + 0.05)
        return self.emit(direction, confidence,
                         f"ordine {strength:+.2f}, 60s {r60:+.2f}bps "
                         f"({amplitude:.1f}x il rumore)",
                         ("trend_strength", "return_60s_bps"))


class MomentumAgent(Agent):
    """Il movimento recente sta accelerando o esaurendosi."""

    name, base_weight = "momentum", 1.3
    requires = ("return_5s_bps", "return_15s_bps")

    def _evaluate(self, f: FeatureVector, regime: str) -> AgentOpinion:
        r5 = f.get("return_5s_bps", 0.0)
        r15 = f.get("return_15s_bps", 0.0)
        r30 = f.get("return_30s_bps", 0.0)
        accel = f.get("acceleration", 0.0)
        noise = f.get("noise_floor_bps") or 0.3
        # Coerenza fra scale diverse: se 5s e 15s vanno d'accordo, il movimento
        # ha una struttura; se si contraddicono, e' oscillazione.
        agree = 1.0 if r5 * r15 > 0 else 0.4
        raw = (0.5 * r5 + 0.3 * r15 + 0.2 * r30) / max(noise, 0.05)
        direction = squash(raw, 4.0) * agree
        confidence = min(0.9, abs(direction) * 0.85 + 0.08 * (accel != 0))
        return self.emit(direction, confidence,
                         f"5s {r5:+.2f} / 15s {r15:+.2f}bps, "
                         f"{'coerenti' if agree > 0.5 else 'discordi'}",
                         ("return_5s_bps", "return_15s_bps", "acceleration"))


class MeanReversionAgent(Agent):
    """Quando il prezzo si allontana troppo dalla sua media, tende a tornare.

    Vale in RANGE e diventa dannoso in TREND: e' il regime a decidere quanto
    farlo pesare, non l'agente stesso.
    """

    name, base_weight = "mean_reversion", 1.0
    requires = ("zscore_60s",)

    def _evaluate(self, f: FeatureVector, regime: str) -> AgentOpinion:
        z = f.get("zscore_60s", 0.0)
        dist = f.get("distance_from_mean_bps", 0.0)
        autocorr = f.get("autocorr_1", 0.0)
        # Segno opposto allo scostamento: se e' salito troppo, ci si aspetta PUT.
        direction = -squash(z, 2.0)
        # Se l'autocorrelazione e' positiva il movimento persiste: la mean
        # reversion e' meno credibile e lo dichiara abbassando la confidenza.
        penalty = 0.45 if autocorr > 0.2 else 1.0
        confidence = min(0.85, abs(direction) * 0.8 * penalty)
        return self.emit(direction, confidence,
                         f"z {z:+.2f}, scostamento {dist:+.2f}bps, "
                         f"autocorr {autocorr:+.2f}",
                         ("zscore_60s", "autocorr_1"))


class MicrostructureAgent(Agent):
    """Squilibrio dei tick e qualita' dello spread.

    Su un feed FX gratuito non esiste un book di livello 2: qui si usa cio' che
    c'e' davvero — la direzione dei tick e l'andamento dello spread — e nulla
    viene inventato. Senza bid/ask reali l'agente si astiene.
    """

    name, base_weight = "microstructure", 1.4
    requires = ("tick_imbalance_5s",)

    def _evaluate(self, f: FeatureVector, regime: str) -> AgentOpinion:
        i1 = f.values.get("tick_imbalance_1s")
        i5 = f.get("tick_imbalance_5s", 0.0)
        i15 = f.values.get("tick_imbalance_15s")
        parts = [(0.45, i5)]
        if i1 is not None:
            parts.append((0.25, i1))
        if i15 is not None:
            parts.append((0.30, i15))
        total_w = sum(w for w, _ in parts)
        direction = sum(w * v for w, v in parts) / total_w if total_w else 0.0
        # Lo spread e' un moltiplicatore di fiducia, non una direzione: uno
        # spread in allargamento rende ogni lettura meno affidabile.
        spread_ratio = f.values.get("spread_vs_average")
        quality = 1.0
        note = ""
        if spread_ratio is not None:
            if spread_ratio > 1.6:
                quality = 0.5
                note = f", spread {spread_ratio:.1f}x la media"
            elif spread_ratio < 0.8:
                quality = 1.1
                note = ", spread compresso"
        confidence = min(0.9, abs(direction) * 0.9 * quality)
        return self.emit(direction, confidence,
                         f"squilibrio tick 5s {i5:+.2f}{note}",
                         ("tick_imbalance_5s", "spread_vs_average"),
                         {"spread_quality": round(quality, 3)})


class StatisticalAgent(Agent):
    """Autocorrelazione, entropia e code: la serie e' prevedibile o no?"""

    name, base_weight = "statistical", 0.9
    requires = ("autocorr_1", "entropy_60s")

    def _evaluate(self, f: FeatureVector, regime: str) -> AgentOpinion:
        ac = f.get("autocorr_1", 0.0)
        ent = f.get("entropy_60s", 1.0)
        r5 = f.get("return_5s_bps", 0.0)
        # Autocorrelazione positiva = persistenza: segue il movimento.
        # Negativa = alternanza: lo contraddice.
        direction = squash(ac * 2.0, 1.0) * (1.0 if r5 >= 0 else -1.0) * min(1.0, abs(r5))
        # Entropia alta = serie vicina al rumore: qualunque lettura vale meno.
        confidence = min(0.75, abs(direction) * 0.7 * max(0.15, 1.0 - ent))
        return self.emit(direction, confidence,
                         f"autocorr {ac:+.2f}, entropia {ent:.2f}",
                         ("autocorr_1", "entropy_60s"))


# --------------------------------------------------------------------------- #
#  AGENTI NON DIREZIONALI: dicono se vale la pena, non da che parte
# --------------------------------------------------------------------------- #

class VolatilityAgent(Agent):
    """Il movimento atteso batte il rumore abbastanza da poterci operare?

    Non sceglie un lato. Produce un CONTRIBUTO al punteggio, non un divieto:
    un mercato un po' fermo rende il segnale meno attraente, non impossibile.
    """

    name, base_weight = "volatility", 0.0      # non vota la direzione
    requires = ("sigma_horizon_bps",)

    def _evaluate(self, f: FeatureVector, regime: str) -> AgentOpinion:
        sigma = f.get("sigma_horizon_bps", 0.0)
        noise = f.get("noise_floor_bps") or 0.3
        ratio = sigma / max(noise, 0.05)
        flat = f.values.get("zero_move_fraction")
        # Sotto 1.5x il rumore, un movimento a 60 secondi e' indistinguibile
        # dall'aggiustamento del book. Sopra 3x c'e' spazio per operare.
        score = max(-0.35, min(0.15, (ratio - 2.0) * 0.12))
        if flat is not None and flat > 0.25:
            score -= min(0.25, flat * 0.4)
        return self.emit(0.0, 0.0,
                         f"sigma {sigma:.2f}bps = {ratio:.1f}x il rumore"
                         + (f", {flat:.0%} finestre ferme" if flat else ""),
                         ("sigma_horizon_bps", "zero_move_fraction"),
                         {"score_contribution": round(score, 4),
                          "move_over_noise": round(ratio, 3),
                          "tradable": ratio >= 1.5})


class RiskAgent(Agent):
    """Condizioni che rendono il momento sfavorevole a prescindere dal lato."""

    name, base_weight = "risk", 0.0

    def _evaluate(self, f: FeatureVector, regime: str) -> AgentOpinion:
        score = 0.0
        notes: list[str] = []
        spread_ratio = f.values.get("spread_vs_average")
        if spread_ratio is not None and spread_ratio > 2.0:
            score -= 0.20
            notes.append(f"spread {spread_ratio:.1f}x la media")
        vol_exp = f.get("vol_expansion", 0.0)
        if vol_exp > 0:
            score -= 0.10
            notes.append("volatilita' in espansione improvvisa")
        # Il minuto in cui si entra: nei primi secondi dopo il cambio di minuto
        # il mercato e' spesso piu' agitato per gli ordini programmati.
        sec = f.get("second_of_minute", 30.0)
        if sec < 3 or sec > 57:
            score -= 0.05
            notes.append("a cavallo del minuto")
        if f.get("session_asia", 0.0) > 0:
            score -= 0.08
            notes.append("sessione asiatica: EUR/USD e' meno liquido")
        return self.emit(0.0, 0.0, "; ".join(notes) or "nessun rischio rilevato",
                         (), {"score_contribution": round(score, 4),
                              "warnings": notes})


class DataQualityOpinionAgent(Agent):
    """Porta il verdetto sulla qualita' del feed dentro la decisione."""

    name, base_weight = "data_quality", 0.0

    def _evaluate(self, f: FeatureVector, regime: str) -> AgentOpinion:
        quality = f.get("quality_score", 1.0)
        score = 0.0 if quality >= 0.9 else -min(0.3, (0.9 - quality) * 0.6)
        return self.emit(0.0, 0.0, f"qualita' del feed {quality:.2f}", (),
                         {"score_contribution": round(score, 4),
                          "quality": quality})


class NewsAgent(Agent):
    """Il rischio di un salto dovuto a un dato macro.

    Non inventa notizie: se nessun calendario e' configurato lo dichiara e non
    contribuisce. E' l'unico agente autorizzato a produrre un divieto duro,
    perche' attorno alla pubblicazione di un dato la microstruttura misurata
    nei minuti precedenti smette semplicemente di valere.
    """

    name, base_weight = "news", 0.0

    def _evaluate(self, f: FeatureVector, regime: str) -> AgentOpinion:
        available = f.get("news_available", 0.0)
        if not available:
            return self.emit(0.0, 0.0, "NEWS DATA UNAVAILABLE", (),
                             {"score_contribution": 0.0, "risk": "UNKNOWN",
                              "available": False})
        blackout = f.get("news_in_blackout", 0.0)
        minutes = f.values.get("news_minutes_to_next")
        if blackout:
            return self.emit(0.0, 0.0,
                             f"dato macro ad alto impatto"
                             + (f" fra {minutes:.0f} minuti" if minutes is not None else ""),
                             (), {"score_contribution": -1.0, "risk": "HIGH_IMPACT",
                                  "hard_block": True, "available": True})
        risk = "NORMAL"
        score = 0.0
        if minutes is not None and minutes < 45:
            risk, score = "CAUTION", -0.08
        return self.emit(0.0, 0.0,
                         f"nessun evento imminente"
                         if minutes is None else f"prossimo dato fra {minutes:.0f} min",
                         (), {"score_contribution": score, "risk": risk,
                              "available": True})


def build_agents(cfg) -> list[Agent]:
    """Gli agenti in ordine: prima chi vota la direzione, poi chi la giudica."""
    return [
        TrendAgent(cfg), MomentumAgent(cfg), MeanReversionAgent(cfg),
        MicrostructureAgent(cfg), StatisticalAgent(cfg),
        VolatilityAgent(cfg), RiskAgent(cfg), DataQualityOpinionAgent(cfg),
        NewsAgent(cfg),
    ]
