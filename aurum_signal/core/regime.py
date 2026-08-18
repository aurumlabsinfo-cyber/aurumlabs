"""Classificazione del regime e affidabilita' storica degli agenti per regime.

Perche' il regime conta: momentum e mean reversion sono strutturalmente
opposti. Un sistema che li pesa allo stesso modo sempre e' garantito di avere
meta' dei suoi agenti che remano contro in ogni istante. E' il regime a
decidere chi puo' parlare a voce alta.

Perche' l'affidabilita' e' per regime: un agente puo' essere eccellente in
tendenza e dannoso in laterale. Una sola media storica nasconde esattamente
l'informazione che serve.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from .agents import (BREAKOUT, HIGH_VOLATILITY, LOW_VOLATILITY, RANGE, REGIMES,
                     TREND, UNCERTAIN)
from .features import FeatureVector


@dataclass
class RegimeVerdict:
    regime: str = UNCERTAIN
    confidence: float = 0.0
    reason: str = ""
    volatility_bps: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"regime": self.regime, "confidence": round(self.confidence, 3),
                "reason": self.reason, "volatility_bps": self.volatility_bps}


class RegimeAgent:
    """Classifica lo stato del mercato con regole leggibili.

    Regole esplicite invece di un modello: qui la trasparenza vale piu'
    dell'accuratezza marginale, perche' il regime entra come moltiplicatore su
    tutto il resto e un errore silenzioso si propagherebbe ovunque.
    """

    name = "regime"

    def __init__(self, cfg) -> None:
        self.cfg = cfg

    def classify(self, f: FeatureVector) -> RegimeVerdict:
        vol5 = f.values.get("realized_vol_5s_bps")
        vol300 = f.values.get("realized_vol_300s_bps")
        sigma = f.values.get("sigma_horizon_bps")
        trend = f.values.get("trend_strength")
        r60 = f.values.get("return_60s_bps")
        noise = f.values.get("noise_floor_bps") or 0.3
        rng60 = f.values.get("range_60s_bps")

        if trend is None or sigma is None:
            return RegimeVerdict(UNCERTAIN, 0.0,
                                 "storia insufficiente per classificare")

        ratio = sigma / max(noise, 0.05)
        vol_ratio = (vol5 / vol300) if (vol5 and vol300) else 1.0

        # Volatilita' fuori scala: domina qualunque altra lettura.
        if vol_ratio > 2.2 and ratio > 3.0:
            return RegimeVerdict(HIGH_VOLATILITY, min(0.9, vol_ratio / 4.0),
                                 f"volatilita' 5s {vol_ratio:.1f}x quella lunga",
                                 sigma)
        if ratio < 1.2:
            return RegimeVerdict(LOW_VOLATILITY, min(0.9, (1.5 - ratio)),
                                 f"movimento atteso solo {ratio:.1f}x il rumore",
                                 sigma)
        # Rottura: il prezzo esce dal proprio intervallo con volatilita' in
        # crescita. E' diverso da una tendenza gia' avviata.
        if (rng60 and r60 is not None and abs(r60) > rng60 * 0.7
                and vol_ratio > 1.4):
            return RegimeVerdict(BREAKOUT, min(0.85, vol_ratio / 2.5),
                                 f"movimento {abs(r60):.1f}bps su un intervallo "
                                 f"di {rng60:.1f}bps", sigma)
        if abs(trend) > 0.25:
            return RegimeVerdict(TREND, min(0.9, abs(trend) * 2.2),
                                 f"movimento ordinato ({trend:+.2f})", sigma)
        if abs(trend) < 0.08:
            return RegimeVerdict(RANGE, min(0.85, 0.9 - abs(trend) * 4),
                                 f"nessuna direzione dominante ({trend:+.2f})",
                                 sigma)
        return RegimeVerdict(UNCERTAIN, 0.3,
                             f"segnali contrastanti (ordine {trend:+.2f})", sigma)


class ReliabilityTracker:
    """Quanto ogni agente ha avuto ragione, per regime.

    Stima bayesiana con un prior: tre vittorie di fila non devono rendere un
    agente improvvisamente dominante. Il prior tiene il peso vicino a 0.5
    finche' il campione non e' abbastanza grande da dire qualcosa.

    Decadimento: le osservazioni vecchie contano meno, perche' il mercato
    cambia e un agente affidabile un mese fa non lo e' necessariamente adesso.
    """

    #: Quanto e' forte il prior, in "osservazioni equivalenti".
    #: Con 40, tre vittorie di fila spostano l'affidabilita' di meno del 10%:
    #: e' l'effetto voluto. Un agente non diventa affidabile perche' ha
    #: indovinato tre volte, e un prior debole e' il modo piu' rapido di
    #: inseguire il rumore con pesi che cambiano di continuo.
    PRIOR_STRENGTH = 40.0
    #: Peso residuo di un'osservazione dopo la successiva.
    DECAY = 0.99
    #: Limiti al peso effettivo: senza, un agente potrebbe azzerarsi o dominare.
    MIN_RELIABILITY = 0.25
    MAX_RELIABILITY = 1.6

    def __init__(self) -> None:
        # (agente, regime) -> [successi pesati, totale pesato]
        self._stats: dict[tuple[str, str], list[float]] = {}

    def record(self, agent: str, regime: str, correct: bool) -> None:
        for key in ((agent, regime), (agent, "ALL")):
            s = self._stats.setdefault(key, [0.0, 0.0])
            s[0] *= self.DECAY
            s[1] *= self.DECAY
            s[0] += 1.0 if correct else 0.0
            s[1] += 1.0


    def hit_rate(self, agent: str, regime: str) -> tuple[float, float]:
        """(tasso stimato, campione efficace) con il prior gia' applicato."""
        wins, total = self._stats.get((agent, regime), (0.0, 0.0))
        if total < 8:                      # troppo poco: usa il dato globale
            wins, total = self._stats.get((agent, "ALL"), (0.0, 0.0))
        rate = (wins + 0.5 * self.PRIOR_STRENGTH) / (total + self.PRIOR_STRENGTH)
        return rate, total

    def reliability(self, agent: str, regime: str) -> float:
        """Moltiplicatore del peso, limitato per evitare instabilita'."""
        rate, samples = self.hit_rate(agent, regime)
        # 0.5 -> 1.0 (neutro). Sopra la meta' guadagna peso, sotto lo perde.
        raw = 1.0 + (rate - 0.5) * 2.4
        return max(self.MIN_RELIABILITY, min(self.MAX_RELIABILITY, raw))

    def snapshot(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for (agent, regime), (wins, total) in sorted(self._stats.items()):
            rate, _ = self.hit_rate(agent, regime)
            out.setdefault(agent, {})[regime] = {
                "hit_rate": round(rate, 4),
                "samples": round(total, 1),
                "reliability": round(self.reliability(agent, regime), 4),
            }
        return out

    def load(self, rows: list[dict]) -> int:
        """Ricostruisce lo stato dagli esiti gia' registrati sul database."""
        n = 0
        for r in rows:
            agent, regime, correct = r.get("agent"), r.get("regime"), r.get("correct")
            if agent and regime is not None and correct is not None:
                self.record(agent, regime or UNCERTAIN, bool(correct))
                n += 1
        return n
