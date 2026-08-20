"""BLOCK 2 - DECIDE.  One core, one answer: LONG, SHORT or NO TRADE.

Every decision - including every NO TRADE - carries the numbers that produced
it: quality, probability, residual move expected, cost, margin, leverage,
notional, target, worst case and the time it is allowed to live.  Nothing about
a rejection is implicit, so ``diagnose`` can always say *why*.

Sequence, and the order matters:

  data quality -> portfolio limits -> model -> sizing -> economics -> answer

A trade is only produced when the expected residual move pays for its own costs
with margin to spare *and* the expectancy is positive.  Reaching a target profit
is never a reason to raise leverage.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..config import Config
from ..scan.scanner import Opportunity
from ..scan.snapshot import MarketSnapshot
from ..util.rolling import clamp
from .model import Model
from .risk import AccountState, RiskEngine, Sizing

NO_TRADE = "NO_TRADE"


@dataclass
class Decision:
    ts_ms: float
    symbol: str
    action: str                      # LONG | SHORT | NO_TRADE
    side: str                        # candidate side, set even when NO_TRADE
    quality: float
    probability: float
    expected_move_bps: float
    required_move_bps: float
    target_move_bps: float
    expected_cost_eur: float
    margin_eur: float
    leverage: float
    notional_eur: float
    qty: float
    target_eur: float
    stop_bps: float
    max_loss_eur: float
    max_hold_s: float
    expectancy_eur: float
    reason: str
    reasons: list[str]
    model_version: str
    snapshot: MarketSnapshot
    features: dict[str, float]
    shadow: bool = False
    scan_score: float = 0.0
    scan_components: dict[str, float] = field(default_factory=dict)
    est_slippage_bps: float = 0.0

    @property
    def is_trade(self) -> bool:
        return self.action in ("LONG", "SHORT")

    def entry_price_reference(self) -> float:
        return self.snapshot.mid

    def target_price(self) -> float:
        move = self.target_move_bps / 10_000.0
        return (
            self.snapshot.mid * (1.0 + move)
            if self.side == "LONG"
            else self.snapshot.mid * (1.0 - move)
        )

    def stop_price(self) -> float:
        move = self.stop_bps / 10_000.0
        return (
            self.snapshot.mid * (1.0 - move)
            if self.side == "LONG"
            else self.snapshot.mid * (1.0 + move)
        )

    def to_row(self, snapshot_id: int | None) -> dict[str, Any]:
        import json

        return {
            "ts_ms": self.ts_ms,
            "symbol": self.symbol,
            "action": self.action,
            "side": self.side,
            "quality": self.quality,
            "probability": self.probability,
            "expected_move_bps": self.expected_move_bps,
            "expected_cost_eur": self.expected_cost_eur,
            "margin_eur": self.margin_eur,
            "leverage": self.leverage,
            "notional_eur": self.notional_eur,
            "target_eur": self.target_eur,
            "max_loss_eur": self.max_loss_eur,
            "max_hold_s": self.max_hold_s,
            "expectancy_eur": self.expectancy_eur,
            "reason": self.reason,
            "reasons_json": json.dumps(self.reasons),
            "features_json": json.dumps(
                {k: round(v, 6) for k, v in self.features.items()}
            ),
            "snapshot_id": snapshot_id,
            "model_version": self.model_version,
            "shadow": 1 if self.shadow else 0,
            "executed": 0,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts_ms": self.ts_ms,
            "symbol": self.symbol,
            "action": self.action,
            "side": self.side,
            "quality": round(self.quality, 4),
            "probability": round(self.probability, 4),
            "expected_move_bps": round(self.expected_move_bps, 3),
            "required_move_bps": round(self.required_move_bps, 3),
            "target_move_bps": round(self.target_move_bps, 3),
            "expected_cost_eur": round(self.expected_cost_eur, 4),
            "est_slippage_bps": round(self.est_slippage_bps, 3),
            "margin_eur": round(self.margin_eur, 2),
            "leverage": round(self.leverage, 2),
            "notional_eur": round(self.notional_eur, 2),
            "qty": self.qty,
            "target_eur": round(self.target_eur, 3),
            "stop_bps": round(self.stop_bps, 2),
            "max_loss_eur": round(self.max_loss_eur, 3),
            "max_hold_s": round(self.max_hold_s, 1),
            "expectancy_eur": round(self.expectancy_eur, 4),
            "reason": self.reason,
            "reasons": self.reasons,
            "model_version": self.model_version,
            "shadow": self.shadow,
            "scan_score": round(self.scan_score, 4),
            "mid": self.snapshot.mid,
            "quality_reasons": list(self.snapshot.quality_reasons),
        }


class DecisionCore:
    def __init__(self, cfg: Config, risk: RiskEngine) -> None:
        self.cfg = cfg
        self.risk = risk
        self.evaluated = 0
        self.trades_proposed = 0

    # ------------------------------------------------------------------ main
    def evaluate(
        self,
        opportunity: Opportunity,
        model: Model,
        account: AccountState,
        now_mono: float,
        *,
        shadow: bool = False,
        slippage_hint_bps: float | None = None,
    ) -> Decision:
        self.evaluated += 1
        snap = opportunity.snapshot
        side = opportunity.side
        cfg = self.cfg.decide
        features = snap.features(side)
        probability = model.probability(features)

        q_scan = clamp(0.5 + opportunity.score, 0.0, 1.0)
        quality = clamp(0.55 * q_scan + 0.45 * probability, 0.0, 1.0)

        reasons: list[str] = []
        blockers: list[str] = []

        # 1. the data itself ------------------------------------------------
        if snap.quality == "BAD":
            blockers.append("market data not usable: " + "; ".join(snap.quality_reasons))
        elif not snap.tradable:
            blockers.append("market data degraded: " + "; ".join(snap.quality_reasons))

        # 2. portfolio and account -----------------------------------------
        if not shadow:
            blockers.extend(self.risk.portfolio_blocks(account, snap.symbol, now_mono))

        # 3. the model ------------------------------------------------------
        if probability < cfg.min_probability:
            blockers.append(
                f"probability {probability:.3f} < {cfg.min_probability:.3f} threshold"
            )
        if quality < cfg.min_quality:
            blockers.append(f"opportunity quality {quality:.3f} < {cfg.min_quality:.3f}")

        expected_move_bps = model.expected_move_bps(snap, probability)

        # 4. sizing ---------------------------------------------------------
        # Slippage depends on size and size depends on slippage: one pass with a
        # nominal ticket, one pass with the real one.
        nominal_notional_usd = (
            (cfg.margin_min_eur + cfg.margin_max_eur) / 2.0
            * cfg.leverage_default
            / max(self.cfg.eur_per_usdt, 1e-9)
        )
        slip_bps = (
            slippage_hint_bps
            if slippage_hint_bps is not None
            else snap.slippage_bps_for(side, nominal_notional_usd)
        )
        # A round trip costs both fees and slippage in and out; the risk engine
        # needs that number to keep the true worst case inside the loss cap.
        fees_bps = cfg.taker_fee_rate * 2.0 * 10_000.0
        estimated_cost_bps = fees_bps + 2.0 * (slip_bps + cfg.slippage_safety_bps)
        sizing = self.risk.size(
            account=account,
            quality=quality,
            volatility_bps=snap.volatility_bps,
            spread_bps=snap.spread_bps,
            slippage_bps=slip_bps,
            cost_bps=estimated_cost_bps,
            price=snap.ask if side == "LONG" else snap.bid,
            qty_step=snap.qty_step,
            min_qty=snap.min_qty,
            min_notional_usd=snap.min_notional_usd,
            max_leverage=snap.max_leverage,
        )
        reasons.extend(sizing.reasons)
        if not sizing.ok:
            blockers.extend(sizing.reasons or ["cannot size a position"])
            return self._no_trade(
                snap, side, quality, probability, expected_move_bps, blockers, reasons,
                model, features, opportunity, shadow, slip_bps,
            )

        notional_usd = sizing.notional_eur / max(self.cfg.eur_per_usdt, 1e-9)
        slip_entry_bps = snap.slippage_bps_for(side, notional_usd) + cfg.slippage_safety_bps
        exit_side = "SHORT" if side == "LONG" else "LONG"
        slip_exit_bps = snap.slippage_bps_for(exit_side, notional_usd) + cfg.slippage_safety_bps

        # 5. economics ------------------------------------------------------
        fees_eur = sizing.notional_eur * cfg.taker_fee_rate * 2.0
        slippage_eur = sizing.notional_eur * (slip_entry_bps + slip_exit_bps) / 10_000.0
        cost_eur = fees_eur + slippage_eur

        breakeven_bps = cost_eur / sizing.notional_eur * 10_000.0
        required_bps = breakeven_bps * cfg.cost_safety_multiple
        target_for_reference_bps = (
            (cfg.target_net_eur + cost_eur) / sizing.notional_eur * 10_000.0
        )
        # The €2 reference is a target, not an obligation: if the move on offer is
        # smaller, take the smaller one; if it is larger, the trailing exit lets it run.
        target_move_bps = max(
            min(target_for_reference_bps, expected_move_bps * 0.80),
            breakeven_bps * 1.20,
        )
        target_eur = sizing.notional_eur * target_move_bps / 10_000.0 - cost_eur

        if expected_move_bps < required_bps:
            blockers.append(
                f"expected move {expected_move_bps:.2f}bps < {required_bps:.2f}bps needed "
                f"to clear costs ({cost_eur:.3f} EUR = {breakeven_bps:.2f}bps x "
                f"{cfg.cost_safety_multiple:.2f} safety)"
            )

        loss_eur = sizing.max_loss_eur + cost_eur
        if loss_eur > cfg.max_loss_per_trade_eur * 1.25:
            blockers.append(
                f"worst case {loss_eur:.2f} EUR exceeds the {cfg.max_loss_per_trade_eur:.2f} EUR cap"
            )
        expectancy = probability * target_eur - (1.0 - probability) * loss_eur
        # The floor is an absolute amount at full size, so it has to follow the
        # ticket down: otherwise "half size" quietly becomes "no trades at all".
        min_expectancy = cfg.min_expectancy_eur * clamp(self.risk.size_multiplier, 0.1, 1.0)
        if expectancy < min_expectancy:
            blockers.append(
                f"expectancy {expectancy:.3f} EUR < {min_expectancy:.3f} EUR minimum "
                f"(p={probability:.3f}, win={target_eur:.2f}, loss={loss_eur:.2f})"
            )

        # 6. how long it may live ------------------------------------------
        # enough time for the expected move at the current pace, capped hard
        pace_bps_per_s = max(abs(snap.ret_5s_bps) / 5.0, snap.volatility_bps / 3.0, 0.05)
        max_hold_s = clamp(target_move_bps / pace_bps_per_s * 3.0, 10.0, cfg.max_hold_s)

        if blockers:
            return self._no_trade(
                snap, side, quality, probability, expected_move_bps, blockers, reasons,
                model, features, opportunity, shadow, slip_entry_bps,
                sizing=sizing, cost_eur=cost_eur, required_bps=required_bps,
                target_move_bps=target_move_bps, target_eur=target_eur,
                expectancy=expectancy, max_hold_s=max_hold_s, loss_eur=loss_eur,
            )

        self.trades_proposed += 1
        reasons.insert(
            0,
            f"{side} {snap.symbol}: move already running "
            f"({snap.ret_1s_bps:+.1f}bps/1s, {snap.ret_5s_bps:+.1f}bps/5s), "
            f"flow agrees (OFI5s {snap.ofi_5s:+.2f}, aggression {snap.aggression_5s:+.2f}, "
            f"imbalance {snap.imbalance_top:+.2f}), volume x{snap.volume_acceleration:.2f}; "
            f"p={probability:.3f}, expected {expected_move_bps:.1f}bps vs {required_bps:.1f}bps needed",
        )
        return Decision(
            ts_ms=snap.ts_local_ms,
            symbol=snap.symbol,
            action=side,
            side=side,
            quality=quality,
            probability=probability,
            expected_move_bps=expected_move_bps,
            required_move_bps=required_bps,
            target_move_bps=target_move_bps,
            expected_cost_eur=cost_eur,
            margin_eur=sizing.margin_eur,
            leverage=sizing.leverage,
            notional_eur=sizing.notional_eur,
            qty=sizing.qty,
            target_eur=target_eur,
            stop_bps=sizing.stop_bps,
            max_loss_eur=loss_eur,
            max_hold_s=max_hold_s,
            expectancy_eur=expectancy,
            reason=reasons[0],
            reasons=reasons,
            model_version=model.version,
            snapshot=snap,
            features=features,
            shadow=shadow,
            scan_score=opportunity.score,
            scan_components=opportunity.components,
            est_slippage_bps=slip_entry_bps,
        )

    # ------------------------------------------------------------------ helpers
    def _no_trade(
        self,
        snap: MarketSnapshot,
        side: str,
        quality: float,
        probability: float,
        expected_move_bps: float,
        blockers: list[str],
        reasons: list[str],
        model: Model,
        features: dict[str, float],
        opportunity: Opportunity,
        shadow: bool,
        slip_bps: float,
        *,
        sizing: Sizing | None = None,
        cost_eur: float = 0.0,
        required_bps: float = 0.0,
        target_move_bps: float = 0.0,
        target_eur: float = 0.0,
        expectancy: float = 0.0,
        max_hold_s: float = 0.0,
        loss_eur: float = 0.0,
    ) -> Decision:
        return Decision(
            ts_ms=snap.ts_local_ms,
            symbol=snap.symbol,
            action=NO_TRADE,
            side=side,
            quality=quality,
            probability=probability,
            expected_move_bps=expected_move_bps,
            required_move_bps=required_bps,
            target_move_bps=target_move_bps,
            expected_cost_eur=cost_eur,
            margin_eur=sizing.margin_eur if sizing else 0.0,
            leverage=sizing.leverage if sizing else 0.0,
            notional_eur=sizing.notional_eur if sizing else 0.0,
            qty=sizing.qty if sizing else 0.0,
            target_eur=target_eur,
            stop_bps=sizing.stop_bps if sizing else 0.0,
            max_loss_eur=loss_eur,
            max_hold_s=max_hold_s,
            expectancy_eur=expectancy,
            reason=blockers[0] if blockers else "no reason recorded",
            reasons=blockers + reasons,
            model_version=model.version,
            snapshot=snap,
            features=features,
            shadow=shadow,
            scan_score=opportunity.score,
            scan_components=opportunity.components,
            est_slippage_bps=slip_bps,
        )
