"""Capital, leverage and the limits that bound a single trade.

The rules this engine enforces, in the order they bind:

1. the account keeps a **reserve** - total capital is never fully committed;
2. a position uses roughly €40-60 of margin when equity and risk allow it;
3. leverage starts at 10x and is *reduced* when volatility demands it, never
   raised to reach a profit target;
4. a trade that cannot be sized inside ``max_loss_per_trade`` is simply not
   taken.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any

from ..config import Config
from ..util.rolling import clamp


@dataclass
class AccountState:
    """What the account actually is, as last confirmed by Bybit (or by paper)."""

    source: str = "paper"                       # paper | bybit
    equity_eur: float = 0.0
    available_eur: float = 0.0
    used_margin_eur: float = 0.0
    exposure_eur: float = 0.0
    unrealized_eur: float = 0.0
    realized_today_eur: float = 0.0
    open_positions: int = 0
    symbols_open: set[str] = field(default_factory=set)
    last_update_ms: float = 0.0
    confirmed: bool = False                     # False until reconciled with Bybit

    def free_after_reserve(self, reserve_fraction: float) -> float:
        reserve = self.equity_eur * reserve_fraction
        return max(self.available_eur - reserve, 0.0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "equity_eur": round(self.equity_eur, 4),
            "available_eur": round(self.available_eur, 4),
            "used_margin_eur": round(self.used_margin_eur, 4),
            "exposure_eur": round(self.exposure_eur, 4),
            "unrealized_eur": round(self.unrealized_eur, 4),
            "realized_today_eur": round(self.realized_today_eur, 4),
            "open_positions": self.open_positions,
            "symbols_open": sorted(self.symbols_open),
            "last_update_ms": self.last_update_ms,
            "confirmed": self.confirmed,
        }


@dataclass
class Sizing:
    ok: bool
    margin_eur: float = 0.0
    leverage: float = 0.0
    notional_eur: float = 0.0
    qty: float = 0.0
    stop_bps: float = 0.0
    max_loss_eur: float = 0.0
    reasons: list[str] = field(default_factory=list)


class RiskEngine:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.kill_switch: bool = False
        self.kill_reason: str = ""
        self.day_key: str = time.strftime("%Y-%m-%d")
        self.realized_today_eur: float = 0.0
        self.cooldowns: dict[str, float] = {}

    # ---------------------------------------------------------------- switches
    def engage_kill_switch(self, reason: str) -> None:
        self.kill_switch = True
        self.kill_reason = reason

    def release_kill_switch(self) -> None:
        self.kill_switch = False
        self.kill_reason = ""

    def kill_switch_active(self) -> tuple[bool, str]:
        path = self.cfg.execute.kill_switch_file
        if path and os.path.exists(path):
            return True, f"kill switch file present: {path}"
        if self.kill_switch:
            return True, self.kill_reason or "kill switch engaged"
        return False, ""

    def roll_day(self) -> None:
        today = time.strftime("%Y-%m-%d")
        if today != self.day_key:
            self.day_key = today
            self.realized_today_eur = 0.0

    def record_realized(self, net_eur: float, symbol: str, now_mono: float) -> None:
        self.roll_day()
        self.realized_today_eur += net_eur
        cooldown = (
            self.cfg.decide.cooldown_after_loss_s
            if net_eur <= 0
            else self.cfg.decide.cooldown_after_exit_s
        )
        self.cooldowns[symbol] = now_mono + cooldown

    def cooldown_remaining(self, symbol: str, now_mono: float) -> float:
        until = self.cooldowns.get(symbol, 0.0)
        return max(until - now_mono, 0.0)

    # ---------------------------------------------------------------- gates
    def portfolio_blocks(self, account: AccountState, symbol: str, now_mono: float) -> list[str]:
        """Reasons this account cannot take *this* symbol right now."""
        cfg = self.cfg.decide
        blocks: list[str] = []
        killed, reason = self.kill_switch_active()
        if killed:
            blocks.append(f"kill switch: {reason}")
        if not account.confirmed:
            blocks.append("account state not confirmed by Bybit yet")
        if account.open_positions >= cfg.max_concurrent_positions:
            blocks.append(
                f"max concurrent positions reached ({account.open_positions}/"
                f"{cfg.max_concurrent_positions})"
            )
        if symbol in account.symbols_open:
            blocks.append(f"already holding {symbol}")
        remaining = self.cooldown_remaining(symbol, now_mono)
        if remaining > 0:
            blocks.append(f"cooldown on {symbol}: {remaining:.0f}s left")
        self.roll_day()
        if self.realized_today_eur <= -abs(cfg.daily_max_loss_eur):
            blocks.append(
                f"daily loss limit hit ({self.realized_today_eur:.2f} EUR <= "
                f"-{cfg.daily_max_loss_eur:.2f})"
            )
        free = account.free_after_reserve(cfg.reserve_fraction)
        if free < cfg.margin_min_eur:
            blocks.append(
                f"free capital after {cfg.reserve_fraction:.0%} reserve is "
                f"{free:.2f} EUR < {cfg.margin_min_eur:.2f} EUR minimum margin"
            )
        if account.equity_eur > 0:
            exposure_ratio = account.exposure_eur / account.equity_eur
            if exposure_ratio >= cfg.max_total_exposure_fraction:
                blocks.append(
                    f"exposure {exposure_ratio:.2f}x equity >= "
                    f"{cfg.max_total_exposure_fraction:.2f}x cap"
                )
        return blocks

    # ---------------------------------------------------------------- sizing
    def size(
        self,
        *,
        account: AccountState,
        quality: float,
        volatility_bps: float,
        spread_bps: float,
        slippage_bps: float,
        cost_bps: float,
        price: float,
        qty_step: float,
        min_qty: float,
        min_notional_usd: float,
        max_leverage: float,
    ) -> Sizing:
        cfg = self.cfg.decide
        reasons: list[str] = []

        free = account.free_after_reserve(cfg.reserve_fraction)
        # Quality scales the margin inside the configured band, nothing else does.
        band = cfg.margin_max_eur - cfg.margin_min_eur
        margin = cfg.margin_min_eur + band * clamp((quality - cfg.min_quality) / 0.35, 0.0, 1.0)
        margin = min(margin, free, cfg.margin_max_eur)
        if margin < cfg.margin_min_eur:
            return Sizing(False, reasons=[
                f"margin available {margin:.2f} EUR < minimum {cfg.margin_min_eur:.2f} EUR"
            ])

        leverage = min(cfg.leverage_default, max_leverage, cfg.leverage_max)

        # The stop must sit outside the noise: spread plus a real slippage round trip.
        noise_floor_bps = 2.0 * spread_bps + 2.0 * max(slippage_bps, 0.0) + 1.0
        stop_bps = max(cfg.stop_vol_multiple * max(volatility_bps, 0.5), noise_floor_bps)

        # The worst case is the stop *plus* the round trip it costs to get there,
        # so both go into the budget.  Shrink until it fits: leverage first, then
        # margin.  Never the other way round.
        max_loss = cfg.max_loss_per_trade_eur
        worst_case_bps = stop_bps + max(cost_bps, 0.0)
        notional = margin * leverage
        loss_at_stop = notional * worst_case_bps / 10_000.0
        if loss_at_stop > max_loss:
            wanted_notional = max_loss / (worst_case_bps / 10_000.0)
            new_leverage = wanted_notional / margin
            if new_leverage < cfg.leverage_min:
                new_leverage = cfg.leverage_min
                margin = wanted_notional / new_leverage
                reasons.append(
                    f"margin cut to {margin:.2f} EUR so a {stop_bps:.1f}bps stop plus "
                    f"{cost_bps:.1f}bps of costs stays within {max_loss:.2f} EUR"
                )
                if margin < cfg.margin_min_eur:
                    # the failure leads: an informational note must never be
                    # mistaken for the reason a trade was refused
                    return Sizing(False, reasons=[
                        f"volatility too high: a {stop_bps:.1f}bps stop plus {cost_bps:.1f}bps "
                        f"of costs cannot fit {cfg.margin_min_eur:.2f} EUR margin inside a "
                        f"{max_loss:.2f} EUR loss cap"
                    ] + reasons)
            else:
                reasons.append(
                    f"leverage reduced {leverage:.1f}x -> {new_leverage:.1f}x so a "
                    f"{stop_bps:.1f}bps stop plus costs stays within {max_loss:.2f} EUR"
                )
            leverage = new_leverage
            notional = margin * leverage
            loss_at_stop = notional * worst_case_bps / 10_000.0

        notional_usdt = notional / max(self.cfg.eur_per_usdt, 1e-9)
        qty = notional_usdt / price if price > 0 else 0.0
        if qty_step > 0:
            steps = int(qty / qty_step)
            qty = steps * qty_step
        if qty <= 0 or qty < min_qty:
            return Sizing(False, reasons=[
                f"size {qty:g} below exchange minimum {min_qty:g} for {notional:.2f} EUR notional"
            ] + reasons)
        # re-derive the true notional after rounding, so every number downstream is exact
        notional_usdt = qty * price
        notional = notional_usdt * self.cfg.eur_per_usdt
        if notional_usdt < min_notional_usd:
            return Sizing(False, reasons=[
                f"notional {notional_usdt:.2f} USDT below exchange minimum {min_notional_usd:.2f}"
            ] + reasons)
        margin = notional / leverage

        return Sizing(
            ok=True,
            margin_eur=margin,
            leverage=leverage,
            notional_eur=notional,
            qty=qty,
            stop_bps=stop_bps,
            max_loss_eur=notional * stop_bps / 10_000.0,
            reasons=reasons,
        )

    def to_dict(self) -> dict[str, Any]:
        killed, reason = self.kill_switch_active()
        return {
            "kill_switch": killed,
            "kill_reason": reason,
            "realized_today_eur": round(self.realized_today_eur, 4),
            "daily_max_loss_eur": self.cfg.decide.daily_max_loss_eur,
            "day": self.day_key,
            "cooldowns": {
                symbol: round(until, 1) for symbol, until in self.cooldowns.items()
            },
        }
