"""Il motore che trasforma opinioni in CALL, PUT o NO_TRADE.

Due scelte di fondo, entrambe deliberate.

**Pochi cancelli duri, molti contributi.** Un sistema con venti condizioni
`if ... : NO_TRADE` non opera mai e non sa dire quale sia il vincolo che
stringe davvero. Qui i cancelli duri sono solo per cio' che e' rotto — feed
morto, dati incoerenti, dato macro in uscita — e tutto il resto sposta un
punteggio. Il risultato e' che si puo' sempre rispondere alla domanda "quanto
manca per operare, e su cosa".

**La probabilita' e' legata al payout, non alla sensazione.** Con payout 0.80
il pareggio e' 55,56%: una probabilita' del 54% non e' un segnale debole, e'
un segnale perdente. La soglia effettiva nasce sempre dal payout.
"""

from __future__ import annotations

import math
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from .agents import (Agent, AgentOpinion, CALL, NEUTRAL, PUT, UNCERTAIN,
                     build_agents)
from .features import FeatureVector
from .regime import RegimeAgent, RegimeVerdict, ReliabilityTracker

NO_TRADE = "NO_TRADE"

#: Codici di blocco. Sono un'enumerazione chiusa di proposito: servono a
#: raggruppare i NO_TRADE in statistiche, e testo libero non si raggruppa.
BLOCK_DATA_STALE = "DATA_STALE"
BLOCK_FEED_DEAD = "FEED_DEAD"
BLOCK_INVALID = "INVALID_DATA"
BLOCK_WARMUP = "WARMUP"
BLOCK_EDGE_LOW = "EDGE_LOW"
BLOCK_CONFIDENCE_LOW = "CONFIDENCE_LOW"
BLOCK_NEWS = "NEWS_HIGH_IMPACT"
BLOCK_VOLATILITY = "VOLATILITY_POOR"
BLOCK_DISAGREEMENT = "AGENT_DISAGREEMENT"
BLOCK_REGIME = "REGIME_UNCERTAIN"
BLOCK_SPREAD = "SPREAD_HIGH"
BLOCK_MAX_CONCURRENT = "MAX_CONCURRENT"
BLOCK_WALLET = "WALLET_EXHAUSTED"
BLOCK_SAFE_MODE = "SAFE_MODE"

#: Quanto il regime amplifica o smorza ciascun agente. Momentum e mean
#: reversion sono opposti: in tendenza vince il primo, in laterale il secondo.
REGIME_MULTIPLIERS: dict[str, dict[str, float]] = {
    "TREND": {"momentum": 1.35, "trend": 1.30, "mean_reversion": 0.45},
    "RANGE": {"momentum": 0.60, "trend": 0.55, "mean_reversion": 1.35},
    "BREAKOUT": {"momentum": 1.40, "trend": 1.20, "mean_reversion": 0.30,
                 "microstructure": 1.15},
    "HIGH_VOLATILITY": {"microstructure": 0.85, "statistical": 0.80},
    "LOW_VOLATILITY": {"microstructure": 1.10, "mean_reversion": 1.15},
    "UNCERTAIN": {},
}


@dataclass
class Decision:
    """Tutto cio' che serve a ricostruire perche' il motore ha deciso cosi'."""

    decision_id: str
    ts: int
    mode: str
    direction: str = NO_TRADE
    probability: float = 0.5
    confidence: float = 0.0
    edge: float = 0.0
    regime: str = UNCERTAIN
    regime_confidence: float = 0.0
    quality: float = 1.0
    news_risk: str = "UNKNOWN"
    planned_entry_ts: int | None = None
    expiry_ts: int | None = None
    lead_ms: int | None = None
    #: Il primo codice e' il vincolo che stringe: gli altri sono contorno.
    blockers: list[str] = field(default_factory=list)
    reasons_positive: list[str] = field(default_factory=list)
    reasons_negative: list[str] = field(default_factory=list)
    opinions: list[AgentOpinion] = field(default_factory=list)
    ml_probability: float | None = None
    ml_model_id: str | None = None
    booster_score: float | None = None
    booster_action: str | None = None
    latency: dict[str, float] = field(default_factory=dict)
    features: dict[str, Any] = field(default_factory=dict)
    raw_score: float = 0.0
    agreement: float = 0.0

    @property
    def primary_blocker(self) -> str | None:
        return self.blockers[0] if self.blockers else None

    @property
    def is_tradeable(self) -> bool:
        return self.direction in (CALL, PUT) and not self.blockers

    def explain(self) -> dict[str, Any]:
        """La spiegazione leggibile: perche' si', perche' no, conclusione."""
        return {
            "direction": self.direction,
            "probability": round(self.probability, 4),
            "confidence": round(self.confidence, 4),
            "edge": round(self.edge, 4),
            "positive": list(self.reasons_positive),
            "negative": list(self.reasons_negative),
            "blockers": list(self.blockers),
            "regime": self.regime,
            "conclusion": (f"{self.direction} con probabilita' stimata "
                           f"{self.probability:.1%}" if self.is_tradeable
                           else f"NO_TRADE: {self.primary_blocker}"),
        }

    def to_row(self) -> dict[str, Any]:
        import json
        return {
            "decision_id": self.decision_id, "ts": self.ts, "mode": self.mode,
            "planned_entry_ts": self.planned_entry_ts,
            "expiry_ts": self.expiry_ts, "lead_ms": self.lead_ms,
            "direction": self.direction,
            "probability": self.probability, "confidence": self.confidence,
            "edge": self.edge, "regime": self.regime, "quality": self.quality,
            "news_risk": self.news_risk,
            "emitted": 0,
            "primary_blocker": self.primary_blocker,
            "blockers": json.dumps(self.blockers),
            "reasons": json.dumps(self.explain(), default=str),
            "agent_payload": json.dumps([o.to_dict() for o in self.opinions]),
            "ml_probability": self.ml_probability,
            "ml_model_id": self.ml_model_id,
            "booster_score": self.booster_score,
            "booster_action": self.booster_action,
            "latency_payload": json.dumps(self.latency),
            "feature_snapshot": json.dumps(self.features, default=str),
        }


class MetaDecisionEngine:
    """Aggrega gli agenti, il modello e il contesto in una sola decisione."""

    def __init__(self, cfg, model=None,
                 reliability: ReliabilityTracker | None = None) -> None:
        self.cfg = cfg
        self.agents: list[Agent] = build_agents(cfg)
        self.regime_agent = RegimeAgent(cfg)
        self.reliability = reliability or ReliabilityTracker()
        self.model = model
        self.blocker_counts: dict[str, int] = {}
        self.decisions = 0
        self.emitted = 0
        #: Quanto la componente euristica puo' allontanarsi dal 50%.
        #: 0.35 significa: con un consenso totale degli agenti si arriva a
        #: ~0.64, non a 0.88. E' un tetto deliberato, non un difetto di
        #: taratura: senza una calibrazione misurata, una confidenza piu' alta
        #: non ha nulla che la giustifichi.
        self.heuristic_shrink = 0.35
        #: Storia (confidenza dichiarata, esito) per misurare la calibrazione.
        self._calibration: list[tuple[float, bool]] = []

    # ------------------------------------------------------------- decisione
    def decide(self, f: FeatureVector, context: dict[str, Any]) -> Decision:
        """`context` porta cio' che non sta nelle feature: qualita' del feed,
        stato del portafoglio, segnali gia' aperti, modalita' sicura."""
        t0 = time.perf_counter()
        cfg = self.cfg
        d = Decision(decision_id=uuid.uuid4().hex[:16], ts=f.ts, mode=f.mode)
        latency: dict[str, float] = {}

        # --- regime -------------------------------------------------------
        t = time.perf_counter()
        verdict: RegimeVerdict = self.regime_agent.classify(f)
        d.regime, d.regime_confidence = verdict.regime, verdict.confidence
        latency["regime_ms"] = (time.perf_counter() - t) * 1000.0

        # --- agenti -------------------------------------------------------
        t = time.perf_counter()
        opinions: list[AgentOpinion] = []
        for agent in self.agents:
            rel = self.reliability.reliability(agent.name, d.regime)
            opinions.append(agent.evaluate(f, d.regime, reliability=rel))
        d.opinions = opinions
        by_name = {o.agent: o for o in opinions}
        latency["agents_ms"] = (time.perf_counter() - t) * 1000.0

        # --- cancelli duri: SOLO cio' che e' rotto o proibito ---------------
        d.quality = float(context.get("quality_score", 1.0))
        if context.get("safe_mode"):
            d.blockers.append(BLOCK_SAFE_MODE)
        for code in context.get("blocking", []):
            d.blockers.append(code if code in (
                BLOCK_DATA_STALE, BLOCK_FEED_DEAD, BLOCK_INVALID) else BLOCK_INVALID)
        if not context.get("warmed_up", True):
            d.blockers.append(BLOCK_WARMUP)
        news = by_name.get("news")
        d.news_risk = (news.extra.get("risk") if news else "UNKNOWN") or "UNKNOWN"
        if news is not None and news.extra.get("hard_block"):
            d.blockers.append(BLOCK_NEWS)
        if context.get("open_signals", 0) >= cfg.max_concurrent_signals:
            d.blockers.append(BLOCK_MAX_CONCURRENT)
        if not context.get("wallet_can_trade", True):
            d.blockers.append(BLOCK_WALLET)

        # --- aggregazione direzionale --------------------------------------
        t = time.perf_counter()
        score, agreement, used = self._aggregate(opinions, d.regime)
        d.raw_score, d.agreement = score, agreement

        # I contributi non direzionali spostano il punteggio, non lo vietano.
        contribution = 0.0
        for o in opinions:
            c = o.extra.get("score_contribution")
            if isinstance(c, (int, float)) and c > -1.0:
                contribution += float(c)
                if c < -0.001:
                    d.reasons_negative.append(f"{o.agent}: {o.reason}")
        contribution = max(-0.6, min(0.3, contribution))

        # Punteggio -> probabilita'.
        #
        # Qui sta la decisione piu' importante del motore, e va spiegata.
        # Un insieme di euristiche pesate NON produce una probabilita': produce
        # una forza direzionale. Trasformarla in "87% di probabilita'" con una
        # sigmoide ripida e' il modo piu' rapido di costruire un sistema che si
        # dichiara sicuro esattamente quanto e' ignorante — e su un cammino
        # quasi casuale, come EUR/USD a 60 secondi, la probabilita' vera resta
        # attorno al 50% qualunque cosa dicano gli agenti.
        #
        # Percio' la componente euristica viene RISTRETTA verso il 50%, e il
        # restringimento si allenta solo quando esiste una calibrazione
        # misurata che lo autorizza. La confidenza si guadagna dai dati, non si
        # asserisce con una formula.
        directional = 1.0 / (1.0 + math.exp(-2.2 * score))
        # L'accordo modula quanto ci si allontana da 50%: agenti divisi non
        # possono produrre una probabilita' estrema.
        probability = 0.5 + (directional - 0.5) * min(1.0, agreement * 1.15)
        probability = 0.5 + (probability - 0.5) * self.heuristic_shrink
        latency["aggregate_ms"] = (time.perf_counter() - t) * 1000.0

        # --- modello -------------------------------------------------------
        t = time.perf_counter()
        if self.model is not None and getattr(self.model, "ready", False):
            out = self.model.predict(f.predictive())
            if out is not None:
                d.ml_probability = out.get("probability")
                d.ml_model_id = out.get("model_id")
                if d.ml_probability is not None:
                    # Un modello pesa la meta' solo se e' stato CALIBRATO fuori
                    # campione: un modello non calibrato produce numeri
                    # sicuri di se' senza esserne autorizzato dai dati.
                    w = 0.5 if out.get("calibrated") else 0.25
                    probability = (1 - w) * probability + w * d.ml_probability
                    side = CALL if d.ml_probability > 0.5 else PUT
                    d.reasons_positive.append(
                        f"modello {side} {d.ml_probability:.1%}"
                        + ("" if out.get("calibrated") else " (non calibrato)"))
        latency["model_ms"] = (time.perf_counter() - t) * 1000.0

        # Il contributo dei non direzionali si applica sulla DISTANZA dal 50%:
        # un mercato fermo non inverte la direzione, la rende meno affidabile.
        probability = 0.5 + (probability - 0.5) * (1.0 + contribution)
        probability = max(0.01, min(0.99, probability))

        d.probability = probability
        d.confidence = max(probability, 1.0 - probability)
        breakeven = cfg.breakeven_win_rate
        d.edge = d.confidence - breakeven

        # --- soglie: sempre legate al payout --------------------------------
        direction = CALL if probability > 0.5 else PUT
        if d.confidence < cfg.effective_min_probability:
            d.blockers.append(BLOCK_CONFIDENCE_LOW)
        if d.edge < cfg.min_edge:
            d.blockers.append(BLOCK_EDGE_LOW)
        if agreement < 0.25:
            d.blockers.append(BLOCK_DISAGREEMENT)
        if d.regime == UNCERTAIN and d.regime_confidence < 0.2:
            d.blockers.append(BLOCK_REGIME)
        vol = by_name.get("volatility")
        if vol is not None and not vol.extra.get("tradable", True):
            d.blockers.append(BLOCK_VOLATILITY)

        d.direction = direction if not d.blockers else NO_TRADE
        for o in opinions:
            if o.abstained or o.weight <= 0.01:
                continue
            line = f"{o.agent} {o.side} {o.confidence:.0%}: {o.reason}"
            (d.reasons_positive if (o.direction > 0) == (direction == CALL)
             else d.reasons_negative).append(line)

        d.features = dict(f.values)
        latency["decision_ms"] = (time.perf_counter() - t0) * 1000.0
        d.latency = {k: round(v, 3) for k, v in latency.items()}

        self.decisions += 1
        if d.is_tradeable:
            self.emitted += 1
        for b in d.blockers:
            self.blocker_counts[b] = self.blocker_counts.get(b, 0) + 1
        return d

    # ------------------------------------------------------------ supporto
    def _aggregate(self, opinions: list[AgentOpinion],
                   regime: str) -> tuple[float, float, dict[str, float]]:
        """Media pesata delle direzioni, piu' quanto gli agenti concordano.

        L'accordo e' una grandezza a se': dice se la squadra e' compatta,
        mentre il punteggio dice da che parte. Confonderli produrrebbe alta
        confidenza su un consenso inesistente.
        """
        multipliers = REGIME_MULTIPLIERS.get(regime, {})
        num = den = 0.0
        pro = con = 0.0
        used: dict[str, float] = {}
        for agent in self.agents:
            if agent.base_weight <= 0:
                continue
            o = next((x for x in opinions if x.agent == agent.name), None)
            if o is None or o.abstained:
                continue
            w = agent.base_weight * multipliers.get(agent.name, 1.0) * o.weight
            if w <= 0:
                continue
            num += o.direction * w
            den += w
            used[agent.name] = round(w, 4)
            if o.direction > 0:
                pro += w
            elif o.direction < 0:
                con += w
        score = (num / den) if den > 0 else 0.0
        total = pro + con
        agreement = (abs(pro - con) / total) if total > 0 else 0.0
        return score, agreement, used

    # ------------------------------------------------- calibrazione misurata
    def record_outcome(self, probability: float, won: bool) -> None:
        """Registra un esito per poter misurare se la confidenza vale qualcosa."""
        self._calibration.append((float(probability), bool(won)))
        self._calibration = self._calibration[-5000:]
        self._update_shrink()

    def _update_shrink(self) -> None:
        """Allarga o stringe il tetto in base a come si e' comportato finora.

        Se il motore ha dichiarato in media il 62% e ha vinto il 61%, la sua
        confidenza descrive qualcosa e puo' allargarsi. Se ha dichiarato 62% e
        vinto 50%, si stringe. E' l'unico modo onesto di guadagnare fiducia:
        misurandola, non assumendola.
        """
        decided = self._calibration[-2000:]
        if len(decided) < 200:
            return
        stated = sum(p for p, _ in decided) / len(decided)
        realised = sum(1 for _, w in decided if w) / len(decided)
        if stated <= 0.5:
            return
        # Rapporto fra vantaggio realizzato e vantaggio dichiarato.
        ratio = (realised - 0.5) / (stated - 0.5)
        target = max(0.15, min(1.0, self.heuristic_shrink * max(0.2, min(2.0, ratio))))
        # Movimento lento: la calibrazione non deve inseguire il rumore.
        self.heuristic_shrink += 0.1 * (target - self.heuristic_shrink)

    def calibration_state(self) -> dict[str, Any]:
        decided = self._calibration[-2000:]
        if not decided:
            return {"samples": 0, "shrink": round(self.heuristic_shrink, 4),
                    "note": "nessun esito ancora: la confidenza resta ristretta"}
        stated = sum(p for p, _ in decided) / len(decided)
        realised = sum(1 for _, w in decided if w) / len(decided)
        return {
            "samples": len(decided),
            "stated_mean": round(stated, 4),
            "realised_win_rate": round(realised, 4),
            "gap": round(realised - stated, 4),
            "shrink": round(self.heuristic_shrink, 4),
        }

    def stats(self) -> dict[str, Any]:
        total = max(1, self.decisions)
        return {
            "decisions": self.decisions,
            "emitted": self.emitted,
            "emission_rate": round(self.emitted / total, 5),
            "blockers": dict(sorted(self.blocker_counts.items(),
                                    key=lambda kv: -kv[1])),
            "breakeven_win_rate": round(self.cfg.breakeven_win_rate, 4),
            "effective_min_probability": round(
                self.cfg.effective_min_probability, 4),
        }
