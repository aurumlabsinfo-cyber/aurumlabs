"""Decision engine.

Turns agent opinions into one of three answers: UP, DOWN, or NO TRADE.

NO TRADE is a first-class outcome, not a failure. The engine refuses to trade
whenever the data cannot support a decision - stale feed, desynchronised book,
wide spread, high latency, anomaly, unknown regime, model out of distribution,
or simply not enough edge. Forcing a signal out of noise is the single easiest
way to fool yourself, so the gates run *before* any scoring.
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from typing import Any

from app.agents.base import Agent, AgentContext, AgentOutput, Direction, Regime
from app.agents.catalog import MarketRegimeAgent, build_agents
from app.config import Settings
from app.core.clock import now_ms


@dataclass
class Decision:
    ts: int
    symbol: str
    exchange: str
    direction: Direction
    prob_up: float
    prob_down: float
    prob_neutral: float
    confidence: float  # conditional probability of the chosen direction
    edge: float  # |directional probability - 0.5|
    regime: Regime
    reference_price: float
    trigger_price: float | None
    horizon_s: float
    no_trade_reasons: list[str]
    agents: list[AgentOutput]
    aggregate_score: float
    data_quality: float
    model_id: str | None = None
    model_prob_up: float | None = None
    calibrated: bool = False
    detail: dict[str, Any] = field(default_factory=dict)
    #: Which way the aggregate leaned, before the gates were applied. Survives
    #: even when `direction` is NO_TRADE, so a rejected window is still a
    #: learnable observation rather than a discarded one.
    lean: Direction = Direction.NO_TRADE
    #: Confidence of that lean, likewise ungated.
    lean_confidence: float = 0.0

    @property
    def is_trade(self) -> bool:
        return self.direction in (Direction.UP, Direction.DOWN)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": self.ts,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "direction": self.direction.value,
            "prob_up": round(self.prob_up, 4),
            "prob_down": round(self.prob_down, 4),
            "prob_neutral": round(self.prob_neutral, 4),
            "confidence": round(self.confidence, 4),
            "edge": round(self.edge, 4),
            "regime": self.regime.value,
            "reference_price": self.reference_price,
            "trigger_price": self.trigger_price,
            "horizon_s": self.horizon_s,
            "no_trade_reasons": self.no_trade_reasons,
            "lean": self.lean.value,
            "lean_confidence": round(self.lean_confidence, 4),
            "aggregate_score": round(self.aggregate_score, 4),
            "data_quality": round(self.data_quality, 3),
            "model_id": self.model_id,
            "model_prob_up": self.model_prob_up,
            "calibrated": self.calibrated,
            "agents": [a.to_dict() for a in self.agents],
            "detail": self.detail,
        }


class DecisionEngine:
    def __init__(
        self,
        settings: Settings,
        agents: list[Agent] | None = None,
        model_provider: Any = None,
        performance_provider: Any = None,
    ) -> None:
        self.settings = settings
        self.agents = agents if agents is not None else build_agents()
        self.regime_agent = next(
            (a for a in self.agents if isinstance(a, MarketRegimeAgent)),
            MarketRegimeAgent(),
        )
        #: Optional ML model (see app.ml.inference). None => pure rule ensemble.
        self.model_provider = model_provider
        #: Optional callable returning per-agent historical hit rates.
        self.performance_provider = performance_provider

    # ------------------------------------------------------------------ main
    def decide(
        self,
        feature_vector: dict[str, Any],
        market_state: dict[str, Any],
        health: dict[str, Any],
    ) -> Decision:
        s = self.settings
        f: dict[str, Any] = feature_vector.get("features", {})
        ts = feature_vector.get("ts") or now_ms()
        symbol = feature_vector.get("symbol", s.symbol)
        exchange = feature_vector.get("exchange", "")
        quality = health.get("data_quality", {})
        dq = float(quality.get("score", 0.0))
        book_synced = bool(f.get("book_synced"))
        ref_price = market_state.get("price") or f.get("mid") or 0.0

        ctx = AgentContext(
            ts=ts, symbol=symbol, features=f, data_quality=dq,
            book_synced=book_synced,
        )
        regime, regime_conf, regime_reason = self.regime_agent.classify(ctx)
        ctx.regime = regime

        outputs = [a.evaluate(ctx) for a in self.agents]
        by_name = {o.agent: o for o in outputs}

        # --------------------------------------------------------- hard gates
        reasons: list[str] = []
        if not s.signal_enabled:
            reasons.append("signal engine disabled by configuration")
        if not quality.get("warmup_complete", False):
            reasons.append("engine still warming up")
        if dq < s.min_data_quality:
            reasons.append(f"data quality {dq:.2f} < {s.min_data_quality:.2f}")
        for r in quality.get("reasons", []):
            if r not in reasons:
                reasons.append(r)
        if not book_synced:
            reasons.append("order book not synchronised")
        spread_bps = f.get("spread_bps")
        if spread_bps is None:
            reasons.append("spread unavailable")
        elif spread_bps > s.max_spread_bps:
            reasons.append(
                f"spread {spread_bps:.2f}bps above limit {s.max_spread_bps:.2f}bps"
            )
        latency = f.get("latency_ms")
        if latency is not None and latency > s.max_latency_ms:
            reasons.append(f"latency {latency:.0f}ms above limit {s.max_latency_ms:.0f}ms")
        anomaly = by_name.get("anomaly")
        if anomaly and anomaly.extra.get("anomaly_detected"):
            reasons.append("anomaly: " + anomaly.reason)
        if regime is Regime.UNKNOWN:
            reasons.append("market regime unknown")
        missing = [
            k for k in ("return_1000ms", "volume_imbalance_1s", "depth_imbalance_5")
            if f.get(k) is None
        ]
        if missing:
            reasons.append(f"incomplete features: {', '.join(missing)}")

        # Tie risk. Measured on real BTC at 32% for a 5 second horizon, so this
        # is not a corner case: a binary bet on a market that often ends exactly
        # where it started is mostly a bet on the tie rule, not on direction.
        zero_move = f.get("zero_move_fraction")
        if zero_move is not None and zero_move > s.max_zero_move_fraction:
            reasons.append(
                f"{zero_move:.0%} of recent {s.signal_horizon_s:g}s windows had no "
                f"price change at all (limit {s.max_zero_move_fraction:.0%})"
            )
        expected_ticks = f.get("expected_move_ticks")
        if expected_ticks is not None and expected_ticks < s.min_expected_move_ticks:
            reasons.append(
                f"expected move {expected_ticks:.1f} ticks below the "
                f"{s.min_expected_move_ticks:g}-tick minimum"
            )

        vol_agent = by_name.get("volatility")
        if vol_agent and not vol_agent.extra.get("tradable", False):
            reasons.append("expected move too small to be exploitable")

        # ------------------------------------------------------- aggregation
        agg, weights_used = self._aggregate(outputs, regime)
        p_up, p_down, p_neutral = self._probabilities(agg, outputs)

        model_prob = None
        model_id = None
        calibrated = False
        if self.model_provider is not None and self.model_provider.is_ready():
            model_out = self.model_provider.predict(f)
            if model_out is None:
                reasons.append("model unavailable for these features")
            elif model_out.get("out_of_distribution"):
                reasons.append(
                    "model input out of distribution: "
                    + str(model_out.get("ood_reason", ""))
                )
            else:
                model_prob = float(model_out["prob_up"])
                model_id = model_out.get("model_id")
                calibrated = bool(model_out.get("calibrated", False))
                # Blend: the model gets half the weight of the ensemble unless it
                # has been validated out-of-sample (calibrated=True).
                w = 0.5 if calibrated else 0.3
                directional = p_up / max(p_up + p_down, 1e-9)
                blended = (1 - w) * directional + w * model_prob
                mass = p_up + p_down
                p_up, p_down = blended * mass, (1 - blended) * mass

        directional = p_up / max(p_up + p_down, 1e-9)
        edge = abs(directional - 0.5)
        confidence = max(directional, 1 - directional)
        mass = p_up + p_down

        if mass < s.signal_min_confidence:
            reasons.append(
                f"agent agreement {mass:.2f} below {s.signal_min_confidence:.2f}"
            )
        if edge < s.signal_min_edge:
            reasons.append(f"edge {edge:.3f} below minimum {s.signal_min_edge:.3f}")
        if confidence < s.signal_min_confidence:
            reasons.append(
                f"confidence {confidence:.2f} below {s.signal_min_confidence:.2f}"
            )

        direction = Direction.NO_TRADE
        trigger = None
        if not reasons and ref_price > 0:
            direction = Direction.UP if directional > 0.5 else Direction.DOWN
            trigger = self._trigger_price(direction, ref_price, f)
            if trigger is None:
                reasons.append("cannot size trigger: volatility estimate unavailable")
                direction = Direction.NO_TRADE

        return Decision(
            ts=ts,
            symbol=symbol,
            exchange=exchange,
            direction=direction,
            prob_up=p_up,
            prob_down=p_down,
            prob_neutral=p_neutral,
            confidence=confidence if direction is not Direction.NO_TRADE else 0.0,
            edge=edge,
            regime=regime,
            reference_price=ref_price,
            trigger_price=trigger,
            horizon_s=s.signal_horizon_s,
            no_trade_reasons=reasons,
            agents=outputs,
            aggregate_score=agg,
            data_quality=dq,
            model_id=model_id,
            model_prob_up=model_prob,
            calibrated=calibrated,
            lean=Direction.UP if directional > 0.5 else Direction.DOWN,
            lean_confidence=confidence,
            detail={
                "regime_reason": regime_reason,
                "regime_confidence": round(regime_conf, 3),
                "weights": weights_used,
                "agreement_mass": round(mass, 4),
                "sigma_horizon_bps": f.get("sigma_horizon_bps"),
            },
        )

    # ------------------------------------------------------------- internals
    def _aggregate(
        self, outputs: list[AgentOutput], regime: Regime
    ) -> tuple[float, dict[str, float]]:
        """Confidence-weighted mean of signed agent scores, adjusted by regime.

        Momentum and mean reversion are structurally opposed; the regime decides
        which of the two is allowed to speak loudly.
        """
        regime_multiplier = {
            Regime.TREND_UP: {"momentum": 1.4, "mean_reversion": 0.45},
            Regime.TREND_DOWN: {"momentum": 1.4, "mean_reversion": 0.45},
            Regime.RANGE: {"momentum": 0.6, "mean_reversion": 1.35},
            Regime.BREAKOUT: {"momentum": 1.5, "mean_reversion": 0.3,
                              "order_flow": 1.2},
            Regime.EXHAUSTION: {"momentum": 0.5, "mean_reversion": 1.3},
            Regime.HIGH_VOLATILITY: {"price_action": 0.8, "order_book": 1.1},
            Regime.LOW_VOLATILITY: {"order_book": 1.2, "order_flow": 1.1},
        }.get(regime, {})

        hit_rates: dict[str, float] = {}
        if self.performance_provider is not None:
            try:
                hit_rates = self.performance_provider() or {}
            except Exception:  # noqa: BLE001 - performance data is optional
                hit_rates = {}

        num = 0.0
        den = 0.0
        used: dict[str, float] = {}
        by_name = {o.agent: o for o in outputs}
        for agent in self.agents:
            out = by_name.get(agent.name)
            if out is None or agent.weight <= 0:
                continue
            if out.direction is Direction.NO_TRADE and out.score == 0:
                continue
            w = agent.weight * regime_multiplier.get(agent.name, 1.0)
            # Historical hit rate, when available, nudges the weight within +-40%.
            hr = hit_rates.get(agent.name)
            if hr is not None:
                w *= max(0.6, min(1.4, 1.0 + (hr - 0.5) * 2.0))
            contribution = w * out.confidence
            num += out.score * contribution
            den += contribution
            used[agent.name] = round(w, 3)
        return ((num / den) if den > 0 else 0.0, used)

    def _probabilities(
        self, agg: float, outputs: list[AgentOutput]
    ) -> tuple[float, float, float]:
        """Split probability mass between UP, DOWN and NEUTRAL.

        `mass` is how much of the outcome space the agents are willing to claim
        at all - it is driven by agreement, not by the size of the move. What is
        left over is P(NEUTRAL): "no actionable edge".

        These numbers are *model outputs, not calibrated frequencies*, until the
        calibration report says otherwise. `/statistics/calibration` measures the
        realised frequency for each confidence bucket.
        """
        directional = [
            o for o in outputs
            if o.direction in (Direction.UP, Direction.DOWN) and o.confidence > 0
        ]
        if not directional:
            return (0.0, 0.0, 1.0)

        ups = sum(o.confidence for o in directional if o.direction is Direction.UP)
        downs = sum(o.confidence for o in directional if o.direction is Direction.DOWN)
        total = ups + downs
        agreement = abs(ups - downs) / total if total > 0 else 0.0
        avg_conf = total / len(directional)
        mass = max(0.0, min(0.98, agreement * avg_conf))

        # Logistic on the aggregate score gives the split within the mass.
        p_dir_up = 1.0 / (1.0 + math.exp(-3.0 * agg))
        return (p_dir_up * mass, (1.0 - p_dir_up) * mass, 1.0 - mass)

    def _trigger_price(
        self, direction: Direction, ref_price: float, f: dict[str, Any]
    ) -> float | None:
        """Where price must trade before the clock starts.

        Sized from the volatility actually observed over the signal horizon, so
        the trigger is reachable in a quiet market and not trivially reachable in
        a fast one. Clamped by tick size and a hard bps ceiling.
        """
        s = self.settings
        sigma_bps = f.get("sigma_horizon_bps") or f.get("realized_vol_5s_bps")
        if not sigma_bps or sigma_bps <= 0:
            return None
        offset_bps = min(s.trigger_max_bps, s.trigger_sigma_k * sigma_bps)
        offset = ref_price * offset_bps / 10_000.0
        offset = max(offset, s.trigger_min_ticks * s.tick_size)
        raw = ref_price + offset if direction is Direction.UP else ref_price - offset
        ticks = round(raw / s.tick_size)
        price = round(ticks * s.tick_size, 8)
        # Never emit a trigger already on the wrong side of the reference.
        if direction is Direction.UP and price <= ref_price:
            price = round((ticks + 1) * s.tick_size, 8)
        if direction is Direction.DOWN and price >= ref_price:
            price = round((ticks - 1) * s.tick_size, 8)
        return price


def new_signal_id() -> str:
    return uuid.uuid4().hex[:16]
