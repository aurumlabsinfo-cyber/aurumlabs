"""Research memory.

Without this, an autonomous search rediscovers its own failures forever.  Each
cycle the agents propose from the same feature space; nothing stops them from
proposing the hypothesis that was killed an hour ago, and nothing would tell
them it had been.  Worse, every re-test is another draw from the multiple-testing
budget, so a search with no memory does not merely waste time — it manufactures
false positives.

What is remembered:

``fingerprint``   the idea's identity, percentile-based so near-identical
                  parameter choices collide instead of counting as new ideas.
``outcome``       REJECTED, PROMOTED, DECAYED, INCONCLUSIVE.
``tests``         how many times it has been tried.  Feeds the multiple-testing
                  penalty in the validation lab.
``retest_after``  when it may be tried again.  A rejection is not always
                  permanent — an edge can be regime-dependent — but it earns a
                  cooldown, and each further rejection lengthens it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..domain import now_ms
from ..logging_setup import get_logger
from ..storage.repositories import ResearchRepository
from .hypotheses import Hypothesis

log = get_logger("research.memory")


class Outcome:
    REJECTED = "REJECTED"
    PROMOTED = "PROMOTED"
    DECAYED = "DECAYED"
    INCONCLUSIVE = "INCONCLUSIVE"


@dataclass(slots=True)
class MemoryVerdict:
    known: bool
    blocked: bool
    reason: str = ""
    outcome: str = ""
    tests: int = 0
    net_edge_bps: float = 0.0


class ResearchMemory:
    def __init__(self, repo: ResearchRepository, *, retest_cooldown_h: float = 6.0) -> None:
        self.repo = repo
        self.retest_cooldown_h = retest_cooldown_h
        self._cache: dict[str, dict[str, Any]] = {}
        self.hits = 0
        self.blocks = 0
        self.records = 0

    def load(self) -> int:
        """Warm the cache from the database.  Memory survives restarts — that is
        most of its value."""
        self._cache = {row["fingerprint"]: row for row in self.repo.all_memory()}
        log.info("research memory loaded", extra={"entries": len(self._cache)})
        return len(self._cache)

    # ------------------------------------------------------------- querying

    def check(self, hypothesis: Hypothesis, *, at_ms: int | None = None) -> MemoryVerdict:
        stamp = at_ms if at_ms is not None else now_ms()
        row = self._cache.get(hypothesis.fingerprint)
        if row is None:
            return MemoryVerdict(known=False, blocked=False)

        self.hits += 1
        outcome = row.get("outcome", "")
        tests = int(row.get("tests") or 0)
        edge = float(row.get("net_edge_bps") or 0.0)
        retest_after = int(row.get("retest_after_ms") or 0)

        if outcome == Outcome.PROMOTED:
            return MemoryVerdict(
                known=True, blocked=True, outcome=outcome, tests=tests, net_edge_bps=edge,
                reason="already promoted; a duplicate would double-count the same edge",
            )
        if retest_after and stamp < retest_after:
            self.blocks += 1
            hours = (retest_after - stamp) / 3_600_000
            return MemoryVerdict(
                known=True, blocked=True, outcome=outcome, tests=tests, net_edge_bps=edge,
                reason=(
                    f"{outcome} after {tests} test(s) (net {edge:+.2f} bps); "
                    f"retest allowed in {hours:.1f} h"
                ),
            )
        return MemoryVerdict(
            known=True, blocked=False, outcome=outcome, tests=tests, net_edge_bps=edge,
            reason=f"previously {outcome} ({tests} test(s)), cooldown expired",
        )

    def test_count(self, fingerprint: str) -> int:
        row = self._cache.get(fingerprint)
        return int(row.get("tests") or 0) if row else 0

    def total_tests(self) -> int:
        """Every test ever run, which is the multiple-testing denominator."""
        return sum(int(row.get("tests") or 0) for row in self._cache.values())

    # -------------------------------------------------------------- writing

    def record(
        self,
        hypothesis: Hypothesis,
        outcome: str,
        *,
        reason: str = "",
        net_edge_bps: float = 0.0,
        samples: int = 0,
        at_ms: int | None = None,
    ) -> None:
        stamp = at_ms if at_ms is not None else now_ms()
        fingerprint = hypothesis.fingerprint
        existing = self._cache.get(fingerprint)
        tests = int(existing.get("tests") or 0) + 1 if existing else 1

        # Each rejection doubles the wait before the same idea may be retried.
        # A search that keeps returning to a dead end backs off from it instead
        # of burning the testing budget on it.
        cooldown_h = self.retest_cooldown_h * (2 ** min(5, tests - 1))
        retest_after = stamp + int(cooldown_h * 3_600_000) if outcome != Outcome.PROMOTED else 0

        row = {
            "fingerprint": fingerprint,
            "agent": hypothesis.agent,
            "family": hypothesis.family,
            "signal_symbol": hypothesis.signal_symbol,
            "execution_symbol": hypothesis.execution_symbol,
            "horizon_ms": hypothesis.horizon_ms,
            "outcome": outcome,
            "reason": reason,
            "net_edge_bps": net_edge_bps,
            "samples": samples,
            "tests": 1,  # the repository accumulates this
            "first_seen_ms": int(existing.get("first_seen_ms") or stamp) if existing else stamp,
            "last_seen_ms": stamp,
            "retest_after_ms": retest_after,
            "conditions": [c.to_dict() for c in hypothesis.conditions],
        }
        self.repo.upsert_memory(row)
        cached = dict(row)
        cached["tests"] = tests
        self._cache[fingerprint] = cached
        self.records += 1

    # --------------------------------------------------------------- report

    def counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self._cache.values():
            outcome = row.get("outcome", "UNKNOWN")
            counts[outcome] = counts.get(outcome, 0) + 1
        return counts

    def worst(self, limit: int = 10) -> list[dict[str, Any]]:
        rows = sorted(self._cache.values(), key=lambda r: float(r.get("net_edge_bps") or 0.0))
        return [
            {
                "fingerprint": r["fingerprint"],
                "agent": r.get("agent"),
                "outcome": r.get("outcome"),
                "net_edge_bps": round(float(r.get("net_edge_bps") or 0.0), 3),
                "tests": int(r.get("tests") or 0),
                "reason": r.get("reason", ""),
            }
            for r in rows[:limit]
        ]

    def stats(self) -> dict[str, Any]:
        return {
            "entries": len(self._cache),
            "outcomes": self.counts(),
            "total_tests": self.total_tests(),
            "lookups": self.hits,
            "blocked": self.blocks,
            "records_written": self.records,
            "retest_cooldown_h": self.retest_cooldown_h,
        }
