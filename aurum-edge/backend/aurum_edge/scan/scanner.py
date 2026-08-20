"""The scanner: turn the whole universe into a live ranking, fast.

The score here is deliberately cheap and model-free - it is a *shortlist*, not a
decision.  Its job is to take a few hundred symbols and hand the Decision Core
the handful worth thinking hard about, several times a second.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable

from ..util.rolling import clamp
from .snapshot import MarketSnapshot


@dataclass
class Opportunity:
    symbol: str
    side: str                       # LONG | SHORT
    score: float
    snapshot: MarketSnapshot
    components: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "score": round(self.score, 4),
            "components": {k: round(v, 4) for k, v in self.components.items()},
            "snapshot": self.snapshot.compact(),
        }


@dataclass
class ScanResult:
    ts_ms: float
    ranked: list[Opportunity]
    considered: int
    skipped: Counter = field(default_factory=Counter)

    def top(self, n: int) -> list[Opportunity]:
        return self.ranked[:n]

    def to_dict(self, limit: int = 12) -> dict[str, Any]:
        return {
            "ts_ms": self.ts_ms,
            "considered": self.considered,
            "ranked": [o.to_dict() for o in self.ranked[:limit]],
            "skipped": dict(self.skipped.most_common(12)),
        }


class Scanner:
    """Momentum + order-flow continuation shortlist."""

    def __init__(self, min_score: float = 0.35) -> None:
        self.min_score = min_score
        self.last_result: ScanResult | None = None

    def score_side(self, snap: MarketSnapshot, side: str) -> tuple[float, dict[str, float]]:
        sign = 1.0 if side == "LONG" else -1.0
        vol = max(snap.volatility_bps, 0.5)

        # A move must already be under way: normalise by the symbol's own noise.
        momentum = (
            0.45 * clamp(sign * snap.ret_1s_bps / vol, -3.0, 3.0)
            + 0.35 * clamp(sign * snap.ret_3s_bps / vol, -3.0, 3.0)
            + 0.20 * clamp(sign * snap.ret_5s_bps / vol, -3.0, 3.0)
        ) / 3.0
        # ... and the flow must still be pushing it.
        flow = (
            0.40 * clamp(sign * snap.ofi_5s, -1.0, 1.0)
            + 0.25 * clamp(sign * snap.ofi_1s, -1.0, 1.0)
            + 0.20 * clamp(sign * snap.aggression_5s, -1.0, 1.0)
            + 0.15 * clamp(sign * snap.imbalance_top, -1.0, 1.0)
        )
        participation = clamp((snap.volume_acceleration - 1.0) / 2.0, -0.5, 1.0)
        micro = clamp(sign * snap.microprice_edge_bps / max(snap.spread_bps, 0.1), -1.0, 1.0)
        # Cost drag: a wide spread against small volatility kills a scalp.
        cost_drag = clamp(snap.spread_bps / vol, 0.0, 3.0)

        score = (
            0.34 * momentum
            + 0.30 * flow
            + 0.14 * participation
            + 0.12 * micro
            - 0.20 * cost_drag
        )
        components = {
            "momentum": momentum,
            "flow": flow,
            "participation": participation,
            "micro": micro,
            "cost_drag": cost_drag,
        }
        return score, components

    def scan(self, snapshots: Iterable[MarketSnapshot], ts_ms: float) -> ScanResult:
        ranked: list[Opportunity] = []
        skipped: Counter = Counter()
        considered = 0

        for snap in snapshots:
            considered += 1
            if snap.quality == "BAD":
                skipped[_first_reason(snap, "data quality BAD")] += 1
                continue
            best: Opportunity | None = None
            for side in ("LONG", "SHORT"):
                score, components = self.score_side(snap, side)
                if best is None or score > best.score:
                    best = Opportunity(snap.symbol, side, score, snap, components)
            if best is None:
                continue
            if best.score < self.min_score:
                skipped["score below shortlist threshold"] += 1
                continue
            if not snap.tradable:
                skipped[_first_reason(snap, "not tradable")] += 1
                # still ranked: the dashboard should see it, the executor will not take it
            ranked.append(best)

        ranked.sort(key=lambda o: o.score, reverse=True)
        result = ScanResult(ts_ms=ts_ms, ranked=ranked, considered=considered, skipped=skipped)
        self.last_result = result
        return result


def _first_reason(snap: MarketSnapshot, fallback: str) -> str:
    return snap.quality_reasons[0] if snap.quality_reasons else fallback
