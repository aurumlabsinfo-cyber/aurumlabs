"""Cross-market engine: 90 directional relationships, 11 lead-lag horizons.

For ten markets there are 90 ordered pairs, and each is examined at every lag in
``cross_market.lead_lag_ms``.  That is 990 correlations per refresh, which is
only affordable because the feature engine samples every symbol onto the *same*
cadence grid: a lag of 750 ms is exactly three positions on that grid for every
symbol, so a whole lag's 10x10 matrix is one matrix multiplication.

Three numbers are reported per ordered pair:

``correlation``   contemporaneous, the baseline "these move together".
``best lag``      the lag whose correlation is strongest, with its t-statistic.
                  A high correlation over 40 samples is not evidence; the
                  t-statistic is what separates the two.
``net edge``      what the relationship would have paid *after costs* if traded
                  mechanically: the mean forward move of the follower,
                  conditional on the leader having moved, minus a round trip.

The last one is the only one that matters for trading, and it is usually
negative.  That is the expected result, not a bug: correlation at 250 ms is
abundant and almost none of it survives the spread.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from ..config import Config
from ..execution.cost_model import CostModel
from ..features.engine import FeatureEngine
from ..logging_setup import get_logger

log = get_logger("cross_market")


@dataclass
class PairRelation:
    leader: str
    follower: str
    correlation: float = 0.0
    best_lag_ms: int = 0
    best_correlation: float = 0.0
    t_stat: float = 0.0
    samples: int = 0
    predictive_score: float = 0.0
    conditional_edge_bps: float = 0.0
    conditional_net_edge_bps: float = 0.0
    conditional_samples: int = 0
    cost_bps: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "leader": self.leader,
            "follower": self.follower,
            "correlation": round(self.correlation, 4),
            "best_lag_ms": self.best_lag_ms,
            "best_correlation": round(self.best_correlation, 4),
            "t_stat": round(self.t_stat, 3),
            "samples": self.samples,
            "predictive_score": round(self.predictive_score, 4),
            "conditional_edge_bps": round(self.conditional_edge_bps, 4),
            "conditional_net_edge_bps": round(self.conditional_net_edge_bps, 4),
            "conditional_samples": self.conditional_samples,
            "cost_bps": round(self.cost_bps, 4),
        }


@dataclass
class CrossMarketState:
    symbols: list[str] = field(default_factory=list)
    lags_ms: list[int] = field(default_factory=list)
    relations: dict[tuple[str, str], PairRelation] = field(default_factory=dict)
    updated_ms: int = 0
    samples: int = 0
    refreshes: int = 0
    #: Correlation matrix at lag 0, for the UI heatmap.
    correlation_matrix: list[list[float]] = field(default_factory=list)


class CrossMarketEngine:
    def __init__(self, config: Config, features: FeatureEngine, cost_model: CostModel) -> None:
        self.config = config
        self.features = features
        self.costs = cost_model
        self.symbols = list(features.history)
        self.cadence_ms = config.features.cadence_ms
        self.lags_ms = [lag for lag in config.cross_market.lead_lag_ms if lag >= self.cadence_ms]
        self.state = CrossMarketState(symbols=self.symbols, lags_ms=self.lags_ms)
        self._task: asyncio.Task[None] | None = None
        self._running = False

    # -------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="cross-market")

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.config.cross_market.refresh_s)
            try:
                # The matrix work is numpy-bound and would otherwise stall the
                # event loop for tens of milliseconds while the feed is running.
                await asyncio.to_thread(self.refresh)
            except Exception:
                log.exception("cross-market refresh failed")

    # ---------------------------------------------------------------- compute

    def _aligned_matrices(self) -> tuple[list[str], np.ndarray, np.ndarray] | None:
        """Return ``(symbols, returns, mids)`` aligned on the cadence grid.

        Symbols with too little history are excluded rather than padded: a
        padded series would manufacture correlation out of zeros.
        """
        window = int(self.config.cross_market.window_s * 1000 / self.cadence_ms)
        usable = [s for s in self.symbols if len(self.features.history[s]) >= self.config.cross_market.min_samples]
        if len(usable) < 2:
            return None
        length = min(min(len(self.features.history[s]) for s in usable), window)
        if length < self.config.cross_market.min_samples:
            return None
        returns = np.empty((len(usable), length), dtype=np.float64)
        mids = np.empty((len(usable), length), dtype=np.float64)
        for row, symbol in enumerate(usable):
            history = self.features.history[symbol]
            returns[row] = np.fromiter(history.ret, dtype=np.float64, count=len(history.ret))[-length:]
            mids[row] = np.fromiter(history.mid, dtype=np.float64, count=len(history.mid))[-length:]
        return usable, returns, mids

    def refresh(self) -> CrossMarketState:
        prepared = self._aligned_matrices()
        if prepared is None:
            return self.state
        symbols, returns, mids = prepared
        length = returns.shape[1]

        centred = returns - returns.mean(axis=1, keepdims=True)
        norms = np.sqrt((centred**2).sum(axis=1))
        norms[norms == 0] = 1e-12
        unit = centred / norms[:, None]
        contemporaneous = unit @ unit.T

        relations: dict[tuple[str, str], PairRelation] = {}
        for i, leader in enumerate(symbols):
            for j, follower in enumerate(symbols):
                if i == j:
                    continue
                relations[(leader, follower)] = PairRelation(
                    leader=leader,
                    follower=follower,
                    correlation=float(contemporaneous[i, j]),
                    samples=length,
                )

        # One matmul per lag gives every ordered pair's lagged correlation.
        for lag_ms in self.lags_ms:
            steps = max(1, round(lag_ms / self.cadence_ms))
            if length - steps < self.config.cross_market.min_samples:
                continue
            lead_slice = returns[:, : length - steps]
            follow_slice = returns[:, steps:]
            lead_unit = _unit_rows(lead_slice)
            follow_unit = _unit_rows(follow_slice)
            correlations = lead_unit @ follow_unit.T
            effective = length - steps
            for i, leader in enumerate(symbols):
                for j, follower in enumerate(symbols):
                    if i == j:
                        continue
                    value = float(correlations[i, j])
                    relation = relations[(leader, follower)]
                    if abs(value) > abs(relation.best_correlation):
                        relation.best_correlation = value
                        relation.best_lag_ms = lag_ms
                        relation.t_stat = _t_stat(value, effective)
                        # A correlation is only interesting in proportion to the
                        # evidence behind it; |r| alone ranks a fluke first.
                        relation.predictive_score = abs(value) * min(
                            1.0, math.sqrt(effective / 1000.0)
                        )

        for relation in relations.values():
            self._conditional_edge(relation, symbols, returns, mids)

        self.state.relations = relations
        self.state.symbols = symbols
        self.state.samples = length
        self.state.refreshes += 1
        self.state.updated_ms = int(np.datetime64("now").astype("datetime64[ms]").astype(np.int64))
        self.state.correlation_matrix = [[round(float(v), 4) for v in row] for row in contemporaneous]
        return self.state

    def _conditional_edge(
        self, relation: PairRelation, symbols: list[str], returns: np.ndarray, mids: np.ndarray
    ) -> None:
        """What the relationship pays after costs when the leader actually moves.

        Conditioning matters: the unconditional mean forward return is zero by
        construction, so a lead-lag relationship is only tradable if the
        follower's move is *larger* when the leader has moved than the round
        trip costs.
        """
        if not relation.best_lag_ms:
            return
        i = symbols.index(relation.leader)
        j = symbols.index(relation.follower)
        steps = max(1, round(relation.best_lag_ms / self.cadence_ms))
        length = returns.shape[1]
        if length - steps < self.config.cross_market.min_samples:
            return

        leader_returns = returns[i, : length - steps]
        threshold = float(np.std(leader_returns))
        if threshold <= 0:
            return
        triggered = np.abs(leader_returns) >= threshold
        if not triggered.any():
            return

        start_mid = mids[j, : length - steps][triggered]
        end_mid = mids[j, steps:][triggered]
        with np.errstate(divide="ignore", invalid="ignore"):
            forward_bps = np.where(start_mid > 0, (end_mid - start_mid) / start_mid * 10_000.0, 0.0)
        direction = np.sign(leader_returns[triggered]) * np.sign(relation.best_correlation or 1.0)
        edges = forward_bps * direction

        spread_bps = self._typical_spread(relation.follower)
        cost = self.costs.round_trip_bps(spread_bps)
        relation.conditional_edge_bps = float(np.mean(edges))
        relation.conditional_samples = int(triggered.sum())
        relation.cost_bps = cost
        relation.conditional_net_edge_bps = relation.conditional_edge_bps - cost

    def _typical_spread(self, symbol: str) -> float:
        snapshot = self.features.snapshot(symbol)
        if snapshot and snapshot.spread_bps:
            return snapshot.spread_bps
        return 1.0

    # ----------------------------------------------------------------- access

    def relation(self, leader: str, follower: str) -> PairRelation | None:
        return self.state.relations.get((leader, follower))

    def ranked(self, limit: int = 20, *, by: str = "predictive_score") -> list[dict[str, Any]]:
        rows = [r.to_dict() for r in self.state.relations.values()]
        rows.sort(key=lambda r: abs(r.get(by, 0.0) or 0.0), reverse=True)
        return rows[:limit]

    def ranked_relations(self, limit: int = 20) -> list[PairRelation]:
        """The relations themselves, strongest first — what the cross-crypto
        agent proposes from."""
        relations = sorted(
            self.state.relations.values(), key=lambda r: r.predictive_score, reverse=True
        )
        return relations[:limit]

    def tradable_relations(self, min_net_edge_bps: float = 0.0) -> list[PairRelation]:
        return [
            relation
            for relation in self.state.relations.values()
            if relation.conditional_net_edge_bps > min_net_edge_bps
            and relation.conditional_samples >= self.config.cross_market.min_samples // 4
        ]

    def matrix(self) -> dict[str, Any]:
        """The 10x10 view the frontend renders."""
        symbols = self.state.symbols
        cells: list[list[dict[str, Any] | None]] = []
        for leader in symbols:
            row: list[dict[str, Any] | None] = []
            for follower in symbols:
                if leader == follower:
                    row.append(None)
                else:
                    relation = self.state.relations.get((leader, follower))
                    row.append(relation.to_dict() if relation else None)
            cells.append(row)
        return {
            "symbols": symbols,
            "lags_ms": self.lags_ms,
            "samples": self.state.samples,
            "updated_ms": self.state.updated_ms,
            "refreshes": self.state.refreshes,
            "correlation": self.state.correlation_matrix,
            "cells": cells,
        }

    def stats(self) -> dict[str, Any]:
        relations = list(self.state.relations.values())
        positive = [r for r in relations if r.conditional_net_edge_bps > 0]
        return {
            "pairs": len(relations),
            "lags": len(self.lags_ms),
            "samples": self.state.samples,
            "refreshes": self.state.refreshes,
            "pairs_with_positive_net_edge": len(positive),
            "best": max((r.to_dict() for r in relations), key=lambda r: r["conditional_net_edge_bps"], default=None),
        }


def _unit_rows(matrix: np.ndarray) -> np.ndarray:
    centred = matrix - matrix.mean(axis=1, keepdims=True)
    norms = np.sqrt((centred**2).sum(axis=1))
    norms[norms == 0] = 1e-12
    return centred / norms[:, None]


def _t_stat(r: float, n: int) -> float:
    if n <= 2:
        return 0.0
    denominator = 1.0 - r * r
    if denominator <= 1e-12:
        return math.copysign(99.0, r)
    return r * math.sqrt((n - 2) / denominator)
