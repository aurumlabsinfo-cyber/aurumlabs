"""The eight agents.

Each one looks at a different slice of the microstructure and answers the same
question: over the next few seconds, is price more likely to go UP, DOWN, or is
there nothing worth acting on?

Weights encode the stated priority: order flow and order book first, then
microstructure and price action, with classical indicators as context only.
"""

from __future__ import annotations

from typing import Any

from app.agents.base import Agent, AgentContext, AgentOutput, Direction, Regime, squash


class PriceActionAgent(Agent):
    name = "price_action"
    weight = 1.0

    def _evaluate(self, ctx: AgentContext) -> AgentOutput:
        used = ["return_500ms", "return_1000ms", "return_2000ms", "momentum_consistency"]
        if not ctx.has("return_1000ms", "return_2000ms"):
            return self.abstain(ctx, "insufficient price history")

        r05 = ctx.f("return_500ms", 0.0)
        r1 = ctx.f("return_1000ms", 0.0)
        r2 = ctx.f("return_2000ms", 0.0)
        consistency = ctx.f("momentum_consistency", 0.0)
        vol = ctx.f("realized_vol_5s_bps", 0.0) or 1.0

        # Normalise the move by prevailing volatility: 2bps means very different
        # things in a quiet book and in a violent one.
        norm = (0.5 * r05 + 0.3 * r1 + 0.2 * r2) / max(vol, 0.5)
        score = squash(norm, 1.5) * (0.5 + 0.5 * abs(consistency))
        confidence = min(0.95, abs(score) * 0.9 + 0.1 * abs(consistency))
        return self.emit(
            ctx, score, confidence,
            f"blended short-horizon return {norm:+.2f}σ, consistency {consistency:+.2f}",
            used, min_confidence=0.35,
        )


class OrderBookAgent(Agent):
    name = "order_book"
    weight = 1.6

    def _evaluate(self, ctx: AgentContext) -> AgentOutput:
        if not ctx.book_synced:
            return self.abstain(ctx, "order book not synchronised")
        used = [
            "book_imbalance_l1", "depth_imbalance_5", "depth_imbalance_20",
            "micro_price_dev_bps", "bid_wall_distance_bps", "ask_wall_distance_bps",
        ]
        if not ctx.has("depth_imbalance_5"):
            return self.abstain(ctx, "book depth unavailable")

        # L1 imbalance is dropped when the touch is dust (see the feature
        # engine). Rather than abstain, redistribute its weight onto the depth
        # measures, which stay meaningful when the top of book does not.
        l1_raw = ctx.features.get("book_imbalance_l1")
        d5 = ctx.f("depth_imbalance_5", 0.0)
        d20 = ctx.f("depth_imbalance_20", 0.0)
        micro_dev = ctx.f("micro_price_dev_bps", 0.0)
        spread_bps = ctx.f("spread_bps", 1.0) or 1.0

        # Micro-price deviation is the sharpest of these: it already encodes the
        # size-weighted fair value relative to mid.
        micro_term = 0.2 * squash(micro_dev / max(spread_bps * 0.5, 0.05), 1.0)
        if l1_raw is not None:
            score = 0.35 * l1_raw + 0.3 * d5 + 0.15 * d20 + micro_term
            l1 = l1_raw
        else:
            score = 0.5 * d5 + 0.3 * d20 + micro_term
            l1 = float("nan")

        # Walls in front of price cap the move in that direction.
        bid_wall = ctx.features.get("bid_wall_distance_bps")
        ask_wall = ctx.features.get("ask_wall_distance_bps")
        wall_note = ""
        if ask_wall is not None and ask_wall < 2.0 and score > 0:
            score *= 0.5
            wall_note = f"; ask wall {ask_wall:.1f}bps away"
        if bid_wall is not None and bid_wall < 2.0 and score < 0:
            score *= 0.5
            wall_note = f"; bid wall {bid_wall:.1f}bps away"

        confidence = min(0.95, abs(score) * 1.1)
        l1_text = "L1 dust (ignored)" if l1_raw is None else f"L1 imb {l1:+.2f}"
        return self.emit(
            ctx, score, confidence,
            f"{l1_text}, depth5 {d5:+.2f}, micro dev {micro_dev:+.2f}bps{wall_note}",
            used, min_confidence=0.35,
        )


class OrderFlowAgent(Agent):
    name = "order_flow"
    weight = 1.8

    def _evaluate(self, ctx: AgentContext) -> AgentOutput:
        used = [
            "volume_imbalance_1s", "volume_imbalance_5s", "consecutive_buys",
            "consecutive_sells", "large_buy_notional", "large_sell_notional",
            "trade_intensity_1s",
        ]
        if not ctx.has("volume_imbalance_1s", "volume_imbalance_5s"):
            return self.abstain(ctx, "no trade flow yet")

        vi1 = ctx.f("volume_imbalance_1s", 0.0)
        vi5 = ctx.f("volume_imbalance_5s", 0.0)
        count1 = ctx.f("trade_count_1s", 0.0)
        buys = ctx.f("consecutive_buys", 0.0)
        sells = ctx.f("consecutive_sells", 0.0)
        lb = ctx.f("large_buy_notional", 0.0)
        ls = ctx.f("large_sell_notional", 0.0)

        if count1 < 2:
            return self.abstain(ctx, "trade flow too thin to read")

        streak = squash(buys - sells, 6.0)
        large = squash((lb - ls) / max(lb + ls, 1.0) * 2.0, 1.5) if (lb + ls) > 0 else 0.0
        score = 0.4 * vi1 + 0.3 * vi5 + 0.2 * streak + 0.1 * large
        # Thin flow is unreliable; scale confidence by activity.
        activity = min(1.0, count1 / 8.0)
        confidence = min(0.95, abs(score) * 1.15 * (0.4 + 0.6 * activity))
        return self.emit(
            ctx, score, confidence,
            f"flow imb 1s {vi1:+.2f} / 5s {vi5:+.2f}, streak +{int(buys)}/-{int(sells)}, "
            f"{int(count1)} trades/s",
            used, min_confidence=0.35,
        )


class VolatilityAgent(Agent):
    """Does not pick a side; it says whether *any* side is worth taking."""

    name = "volatility"
    weight = 0.8

    def _evaluate(self, ctx: AgentContext) -> AgentOutput:
        used = ["realized_vol_5s_bps", "realized_vol_30s_bps", "vol_ratio_5s_30s",
                "spread_bps"]
        v5 = ctx.features.get("realized_vol_5s_bps")
        v30 = ctx.features.get("realized_vol_30s_bps")
        if v5 is None or v30 is None:
            return self.abstain(ctx, "volatility not yet estimable")

        spread = ctx.f("spread_bps", 1.0) or 1.0
        ratio = ctx.f("vol_ratio_5s_30s", 1.0) or 1.0
        edge_ratio = v5 / max(spread, 0.05)

        # On a real BTC venue the quoted spread is a single tick, so "beats the
        # spread" is satisfied almost always and gates nothing. What actually
        # decides whether the bet is live is whether price is expected to move
        # at all: measured on real data, 32% of 5-second windows ended exactly
        # where they started.
        #
        # Both limits come from the settings. They used to be literals here,
        # which meant an operator loosening MIN_EXPECTED_MOVE_TICKS still got
        # "expected move too small to be exploitable" forever.
        min_ticks = self.setting("min_expected_move_ticks", 2.0)
        max_flat = self.setting("max_zero_move_fraction", 0.35)
        expected_ticks = ctx.features.get("expected_move_ticks")
        zero_move = ctx.features.get("zero_move_fraction")
        moves_enough = expected_ticks is None or expected_ticks >= min_ticks
        rarely_flat = zero_move is None or zero_move <= max_flat
        tradable = edge_ratio > 0.8 and moves_enough and rarely_flat
        confidence = min(0.9, edge_ratio / 3.0) if tradable else 0.0
        # No direction, ever. This agent answers "is any bet worth taking",
        # and a score here leaked a momentum opinion into the aggregate with
        # this agent's weight behind it - the one thing its docstring says it
        # does not do.
        score = 0.0
        reason = (
            f"vol5s {v5:.2f}bps vs spread {spread:.2f}bps (ratio {edge_ratio:.2f}), "
            f"vol regime {ratio:.2f}x"
        )
        if not moves_enough:
            reason += (
                f"; expected move {expected_ticks:.1f} ticks is below the "
                f"{min_ticks:g}-tick minimum"
            )
        if not rarely_flat:
            reason += (
                f"; {zero_move:.0%} of recent windows had no move at all "
                f"(limit {max_flat:.0%})"
            )
        out = self.emit(
            ctx, score, confidence, reason,
            used + ["expected_move_ticks", "zero_move_fraction"],
            min_confidence=0.99,  # never directional on its own
            extra={
                "tradable": tradable,
                "vol_spread_ratio": edge_ratio,
                "expected_move_ticks": expected_ticks,
                "zero_move_fraction": zero_move,
            },
        )
        # This agent is a gate, not a direction: force NO_TRADE, keep confidence
        # as a "conditions are tradable" score for the decision engine.
        out.direction = Direction.NO_TRADE
        out.confidence = confidence
        return out


class MomentumAgent(Agent):
    name = "momentum"
    weight = 1.1

    def _evaluate(self, ctx: AgentContext) -> AgentOutput:
        used = ["return_2000ms", "return_5000ms", "ema_spread_bps", "acceleration_bps_s2",
                "volume_imbalance_5s"]
        if not ctx.has("return_5000ms"):
            return self.abstain(ctx, "insufficient history for momentum")

        r2 = ctx.f("return_2000ms", 0.0)
        r5 = ctx.f("return_5000ms", 0.0)
        accel = ctx.f("acceleration_bps_s2", 0.0)
        ema_spread = ctx.f("ema_spread_bps", 0.0)
        flow = ctx.f("volume_imbalance_5s", 0.0)
        vol = ctx.f("realized_vol_30s_bps", 0.0) or 1.0

        trend = (0.5 * r5 + 0.5 * r2) / max(vol, 0.5)
        # Momentum only counts when flow confirms it.
        confirmation = 1.0 if trend * flow > 0 else 0.45
        score = squash(trend, 2.0) * confirmation + 0.15 * squash(ema_spread, 3.0)
        score += 0.1 * squash(accel, 3.0)
        confidence = min(0.92, abs(score) * confirmation)
        return self.emit(
            ctx, score, confidence,
            f"trend {trend:+.2f}σ, ema spread {ema_spread:+.2f}bps, "
            f"flow {'confirms' if confirmation > 0.5 else 'diverges'}",
            used, min_confidence=0.4,
        )


class MeanReversionAgent(Agent):
    name = "mean_reversion"
    weight = 1.1

    def _evaluate(self, ctx: AgentContext) -> AgentOutput:
        used = ["bb_z", "vwap_deviation_bps", "return_1000ms", "rsi_14",
                "realized_vol_30s_bps"]
        z = ctx.features.get("bb_z")
        if z is None:
            return self.abstain(ctx, "no band statistics yet")

        vwap_dev = ctx.f("vwap_deviation_bps", 0.0)
        r1 = ctx.f("return_1000ms", 0.0)
        rsi_v = ctx.f("rsi_14", 50.0)
        vol = ctx.f("realized_vol_30s_bps", 0.0) or 1.0

        # Fade only genuine stretches, and only when the impulse is already fading.
        stretch = -squash(z, 1.6)
        rsi_term = -squash((rsi_v - 50.0) / 20.0, 1.0) * 0.5
        vwap_term = -squash(vwap_dev / max(vol, 0.5), 2.0) * 0.5
        exhausting = 1.0 if (r1 * z) < 0 else 0.5
        score = (0.55 * stretch + 0.25 * rsi_term + 0.20 * vwap_term) * exhausting
        confidence = min(0.9, abs(score) * (0.9 if abs(z) > 1.5 else 0.5))
        return self.emit(
            ctx, score, confidence,
            f"band z {z:+.2f}, vwap dev {vwap_dev:+.2f}bps, rsi {rsi_v:.0f}"
            f"{', impulse fading' if exhausting > 0.5 else ''}",
            used, min_confidence=0.4,
        )


class MarketRegimeAgent(Agent):
    """Classifies the state of the market. Direction is advisory only."""

    name = "market_regime"
    weight = 0.6

    def classify(self, ctx: AgentContext) -> tuple[Regime, float, str]:
        v5 = ctx.features.get("realized_vol_5s_bps")
        v30 = ctx.features.get("realized_vol_30s_bps")
        r5 = ctx.features.get("return_5000ms")
        consistency = ctx.features.get("momentum_consistency")
        if v30 is None or r5 is None:
            return (Regime.UNKNOWN, 0.0, "not enough history to classify")

        ratio = (v5 / v30) if (v5 and v30 and v30 > 0) else 1.0
        trend_strength = abs(r5) / max(v30, 0.5)
        cons = abs(consistency or 0.0)

        if ratio > 2.2 and trend_strength > 1.5:
            return (Regime.BREAKOUT, min(0.9, ratio / 3.0),
                    f"vol burst {ratio:.1f}x with {trend_strength:.1f}σ move")
        if ratio > 1.8:
            return (Regime.HIGH_VOLATILITY, min(0.85, ratio / 3.0),
                    f"short-horizon vol {ratio:.1f}x the 30s baseline")
        if ratio < 0.45:
            return (Regime.LOW_VOLATILITY, min(0.8, 1.0 - ratio),
                    f"vol compression {ratio:.2f}x")
        if trend_strength > 1.2 and cons > 0.6:
            reg = Regime.TREND_UP if r5 > 0 else Regime.TREND_DOWN
            return (reg, min(0.9, trend_strength / 2.5),
                    f"directional {trend_strength:.1f}σ, consistency {cons:.2f}")
        if trend_strength > 1.2 and cons < 0.3:
            return (Regime.EXHAUSTION, 0.55,
                    f"large move {trend_strength:.1f}σ but horizons disagree")
        return (Regime.RANGE, 0.6, f"no dominant direction ({trend_strength:.1f}σ)")

    def _evaluate(self, ctx: AgentContext) -> AgentOutput:
        regime, conf, reason = self.classify(ctx)
        bias = {
            Regime.TREND_UP: 0.4,
            Regime.TREND_DOWN: -0.4,
        }.get(regime, 0.0)
        out = self.emit(
            ctx, bias, conf, f"{regime.value}: {reason}",
            ["realized_vol_5s_bps", "realized_vol_30s_bps", "return_5000ms",
             "momentum_consistency"],
            min_confidence=0.6,
            extra={"regime": regime.value},
        )
        if regime in (Regime.UNKNOWN,):
            out.direction = Direction.NO_TRADE
        return out


class AnomalyDetector(Agent):
    """Never gives a direction. It can only veto.

    Findings are split into HARD and SOFT. A hard one describes a broken or
    dangerous market picture and vetoes on its own. A soft one is a warning
    sign that is perfectly common on a real venue - a momentarily one-sided
    book, a burst of prints - and only vetoes once several stack up
    (ANOMALY_MAX_SEVERITY). Treating every soft finding as a veto is why the
    engine could sit at NO TRADE through an entirely normal session.
    """

    name = "anomaly"
    weight = 0.0

    #: Anomalies that veto on their own, matched by prefix.
    HARD_PREFIXES = (
        "invalid spread", "abnormal spread", "price spike",
        "order book desynchronised", "data quality", "feed latency",
    )

    @classmethod
    def _is_hard(cls, anomaly: str) -> bool:
        return anomaly.startswith(cls.HARD_PREFIXES)

    def _evaluate(self, ctx: AgentContext) -> AgentOutput:
        anomalies: list[str] = []
        f = ctx.features

        spread = f.get("spread_bps")
        vol30 = f.get("realized_vol_30s_bps")
        vol5 = f.get("realized_vol_5s_bps")
        r100 = f.get("return_100ms")
        depth_b = f.get("depth_notional_bid_20")
        depth_a = f.get("depth_notional_ask_20")
        removal_b = f.get("liquidity_removal_bid")
        removal_a = f.get("liquidity_removal_ask")
        intensity = f.get("trade_intensity_1s")
        intensity_30 = f.get("trade_intensity_30s")
        latency = f.get("latency_ms")

        if spread is None or spread <= 0:
            anomalies.append("invalid spread")
        elif vol30 and spread > max(8.0, 6.0 * vol30):
            anomalies.append(f"abnormal spread {spread:.2f}bps")

        if r100 is not None and vol5 and abs(r100) > 8.0 * max(vol5, 0.5):
            anomalies.append(f"price spike {r100:+.1f}bps in 100ms")

        if removal_b is not None and removal_b > 0.6:
            anomalies.append("bid liquidity disappeared")
        if removal_a is not None and removal_a > 0.6:
            anomalies.append("ask liquidity disappeared")
        if depth_b is not None and depth_a is not None:
            total = depth_b + depth_a
            if total > 0 and min(depth_b, depth_a) / total < 0.08:
                anomalies.append("one-sided book")

        if intensity is not None and intensity_30 and intensity_30 > 0.5:
            if intensity > 12.0 * intensity_30:
                anomalies.append(f"volume burst {intensity / intensity_30:.0f}x")

        if latency is not None and latency > 1000:
            anomalies.append(f"feed latency {latency:.0f}ms")
        if not ctx.book_synced:
            anomalies.append("order book desynchronised")
        if ctx.data_quality < 0.5:
            anomalies.append(f"data quality {ctx.data_quality:.2f}")

        severity = min(1.0, len(anomalies) / 3.0)
        hard = [a for a in anomalies if self._is_hard(a)]
        return AgentOutput(
            agent=self.name,
            direction=Direction.NO_TRADE,
            confidence=severity,
            score=0.0,
            reason="; ".join(anomalies) if anomalies else "no anomaly detected",
            features_used=["spread_bps", "return_100ms", "liquidity_removal_bid",
                           "liquidity_removal_ask", "trade_intensity_1s", "latency_ms"],
            timestamp=ctx.ts,
            data_quality=ctx.data_quality,
            extra={
                "anomalies": anomalies,
                "anomaly_detected": bool(anomalies),
                "hard_anomalies": hard,
                "severity": round(severity, 3),
            },
        )


def build_agents(settings: Any = None) -> list[Agent]:
    return [
        PriceActionAgent(settings),
        OrderBookAgent(settings),
        OrderFlowAgent(settings),
        VolatilityAgent(settings),
        MomentumAgent(settings),
        MeanReversionAgent(settings),
        MarketRegimeAgent(settings),
        AnomalyDetector(settings),
    ]


AGENT_NAMES = [
    "price_action", "order_book", "order_flow", "volatility", "momentum",
    "mean_reversion", "market_regime", "anomaly",
]


def outputs_to_dict(outputs: list[AgentOutput]) -> dict[str, Any]:
    return {o.agent: o.to_dict() for o in outputs}
