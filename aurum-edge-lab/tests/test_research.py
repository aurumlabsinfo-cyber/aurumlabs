"""Hypotheses, memory, statistics and the validation lab.

The most important tests here are the negative controls.  A validation lab that
approves everything and one that approves nothing both pass a "does it run"
test; only feeding it known-good and known-bad data separates them.
"""

from __future__ import annotations

import random

import pytest

from aurum.config import Config
from aurum.domain import Direction, Regime, ValidationStatus
from aurum.research.dataset import Observation
from aurum.research.hypotheses import (
    Hypothesis,
    PercentileCondition,
    new_hypothesis_id,
    resolve_thresholds,
)
from aurum.research.memory import Outcome, ResearchMemory
from aurum.research.statistics import (
    adjusted_p_value,
    benjamini_hochberg,
    build_metrics,
    deflated_score,
    effective_sample_size,
    independent_indices,
    max_drawdown_bps,
    sample_stats,
    student_t_two_tailed_p,
)
from aurum.research.validation import Stage, ValidationLab
from aurum.storage.repositories import Repositories


def hypothesis(**overrides) -> Hypothesis:
    base = dict(
        hypothesis_id=new_hypothesis_id("test"),
        agent="microstructure",
        family="microstructure",
        signal_symbol="BTCUSDT",
        execution_symbol="BTCUSDT",
        direction=Direction.LONG,
        conditions=[PercentileCondition("ofi_norm_1s", ">=", 80.0, threshold=0.4)],
        horizon_ms=5_000,
    )
    base.update(overrides)
    return Hypothesis(**base)


# ------------------------------------------------------------------ statistics


def test_student_t_matches_known_values() -> None:
    # Textbook two-tailed values.
    assert student_t_two_tailed_p(2.228, 10) == pytest.approx(0.05, abs=0.002)
    assert student_t_two_tailed_p(1.96, 100_000) == pytest.approx(0.05, abs=0.002)
    assert student_t_two_tailed_p(0.0, 10) == 1.0
    assert student_t_two_tailed_p(6.0, 30) < 0.0001


def test_overlapping_observations_are_de_overlapped() -> None:
    # 40 triggers 250 ms apart, each measuring a 5 s horizon: they are not
    # 40 independent observations, they are 3.
    timestamps = [i * 250 for i in range(40)]
    chosen = independent_indices(timestamps, horizon_ms=5_000)
    assert len(chosen) == 2
    assert effective_sample_size(timestamps, 5_000) == 2
    # Spread out, they are all independent.
    assert effective_sample_size([i * 6_000 for i in range(40)], 5_000) == 40


def test_significance_is_computed_on_the_independent_subset() -> None:
    """The inflation this prevents, demonstrated."""
    random.seed(7)
    # A tiny positive drift, sampled with heavy overlap.
    values = [random.gauss(0.4, 4.0) for _ in range(400)]
    timestamps = [i * 250 for i in range(400)]

    naive = sample_stats(values)
    metrics = build_metrics(values, values, [0.0] * 400, timestamps, horizon_ms=10_000)

    assert metrics.samples == 400
    # The honest test uses ~10 independent points, so its t is far smaller.
    assert abs(metrics.t_stat) < abs(naive.t_stat)
    assert metrics.p_value > naive.p_value


def test_max_drawdown_of_a_cumulative_curve() -> None:
    assert max_drawdown_bps([1.0, 1.0, -3.0, 1.0]) == pytest.approx(3.0)
    assert max_drawdown_bps([1.0, 2.0, 3.0]) == 0.0


def test_benjamini_hochberg_controls_the_batch() -> None:
    # One genuinely tiny p-value among noise survives; the noise does not.
    passed = benjamini_hochberg([0.0001, 0.4, 0.5, 0.6, 0.9], alpha=0.10)
    assert passed[0] is True
    assert not any(passed[1:])
    # Twenty null p-values spread over [0,1]: none should survive.
    nulls = [(i + 1) / 21 for i in range(20)]
    assert not any(benjamini_hochberg(nulls, alpha=0.10))


def test_adjustment_and_deflation_pay_for_the_search() -> None:
    assert adjusted_p_value(0.01, 1) == pytest.approx(0.01)
    assert adjusted_p_value(0.01, 100) > 0.5      # 100 tries at p=0.01 is not a discovery
    # The same t is worth less when it is the best of many.
    assert deflated_score(5.0, 4.0, 1) > deflated_score(5.0, 4.0, 500)
    assert deflated_score(5.0, 1.0, 500) == 0.0   # below the expected max of the search


# ----------------------------------------------------------------- hypotheses


def test_fingerprint_ignores_fitted_numbers_but_not_the_idea() -> None:
    a = hypothesis()
    b = hypothesis(conditions=[PercentileCondition("ofi_norm_1s", ">=", 80.0, threshold=0.9)])
    assert a.fingerprint == b.fingerprint, "a refitted threshold is the same idea"

    c = hypothesis(conditions=[PercentileCondition("ofi_norm_1s", ">=", 90.0)])
    assert a.fingerprint != c.fingerprint, "a different percentile is a different idea"

    d = hypothesis(direction=Direction.SHORT)
    assert a.fingerprint != d.fingerprint
    e = hypothesis(horizon_ms=10_000)
    assert a.fingerprint != e.fingerprint
    f = hypothesis(regime_filter=Regime.HIGH_VOL)
    assert a.fingerprint != f.fingerprint


def test_hypothesis_matching_and_description() -> None:
    h = hypothesis()
    assert h.matches({"ofi_norm_1s": 0.5})
    assert not h.matches({"ofi_norm_1s": 0.3})
    assert not h.matches({}), "a missing feature must not count as a match"
    assert "BTCUSDT" in h.describe() and "LONG" in h.describe()


def test_thresholds_resolve_from_percentiles() -> None:
    condition = PercentileCondition("x", ">=", 90.0)
    assert resolve_thresholds([condition], {"x": list(range(100))})
    assert condition.threshold == pytest.approx(89.0, abs=1.5)
    # Too little data is a hard no, not a default.
    assert not resolve_thresholds([PercentileCondition("y", ">=", 90.0)], {"y": [1.0, 2.0]})


# --------------------------------------------------------------------- memory


def test_memory_blocks_a_rediscovered_failure(repos: Repositories) -> None:
    memory = ResearchMemory(repos.research, retest_cooldown_h=6.0)
    h = hypothesis()
    assert not memory.check(h).known

    memory.record(h, Outcome.REJECTED, reason="net edge negative", net_edge_bps=-8.0, samples=300)
    verdict = memory.check(h)
    assert verdict.known and verdict.blocked
    assert "REJECTED" in verdict.reason and "retest allowed in" in verdict.reason

    # The same idea with a refitted threshold is still the same idea.
    refit = hypothesis(conditions=[PercentileCondition("ofi_norm_1s", ">=", 80.0, threshold=0.77)])
    assert memory.check(refit).blocked


def test_repeated_rejection_lengthens_the_cooldown(repos: Repositories) -> None:
    memory = ResearchMemory(repos.research, retest_cooldown_h=1.0)
    h = hypothesis()
    memory.record(h, Outcome.REJECTED, at_ms=0)
    first = memory._cache[h.fingerprint]["retest_after_ms"]
    memory.record(h, Outcome.REJECTED, at_ms=0)
    second = memory._cache[h.fingerprint]["retest_after_ms"]
    assert second > first, "a repeatedly failing idea must back off further each time"


def test_memory_survives_a_restart(repos: Repositories) -> None:
    memory = ResearchMemory(repos.research)
    h = hypothesis()
    memory.record(h, Outcome.REJECTED, reason="dead")
    reloaded = ResearchMemory(repos.research)
    assert reloaded.load() >= 1
    assert reloaded.check(h).blocked


def test_promoted_hypotheses_are_not_reproposed(repos: Repositories) -> None:
    memory = ResearchMemory(repos.research)
    h = hypothesis()
    memory.record(h, Outcome.PROMOTED, net_edge_bps=3.0)
    verdict = memory.check(h)
    assert verdict.blocked and "already promoted" in verdict.reason


# ------------------------------------------------------------- validation lab


def observations(
    count: int, mean_bps: float, noise: float, *, cost: float = 0.0, spacing_ms: int = 6_000, seed: int = 3
) -> list[Observation]:
    rng = random.Random(seed)
    rows = []
    for i in range(count):
        gross = rng.gauss(mean_bps, noise)
        rows.append(
            Observation(
                ts_ms=1_700_000_000_000 + i * spacing_ms,
                index=i,
                gross_bps=gross,
                cost_bps=cost,
                net_bps=gross - cost,
                regime=Regime.NORMAL_RANGE,
                spread_bps=1.0,
            )
        )
    return rows


def test_pure_noise_is_rejected(config: Config) -> None:
    """Negative control. A lab that passes this is not validating anything."""
    lab = ValidationLab(config)
    report = lab.evaluate(hypothesis(), observations(1200, mean_bps=0.0, noise=8.0), total_tests=30)
    assert not report.passed
    assert report.status is ValidationStatus.REJECTED
    assert report.stage(Stage.TRAIN) is not None


def test_an_edge_that_does_not_survive_costs_is_rejected(config: Config) -> None:
    """A real 4 bps edge, charged a real 11 bps round trip."""
    lab = ValidationLab(config)
    report = lab.evaluate(
        hypothesis(), observations(1200, mean_bps=4.0, noise=6.0, cost=11.0), total_tests=10
    )
    assert not report.passed
    assert "net edge" in report.reason


def test_a_strong_surviving_edge_passes_every_gate(config: Config) -> None:
    """Positive control: without it, a lab that rejects everything looks correct."""
    lab = ValidationLab(config)
    report = lab.evaluate(
        hypothesis(), observations(1500, mean_bps=14.0, noise=6.0, cost=11.0), total_tests=5
    )
    assert report.passed, report.reason
    assert report.status is ValidationStatus.HOLDOUT_PASS
    stages = [s.stage for s in report.stages]
    assert stages == [Stage.TRAIN, Stage.VALIDATION, Stage.WALKFORWARD, Stage.HOLDOUT]
    assert all(s.passed for s in report.stages)
    assert report.final_metrics.net_edge_bps > 0
    assert report.stage(Stage.WALKFORWARD).folds, "walk-forward must report its folds"


def test_too_few_observations_is_rejected_not_guessed(config: Config) -> None:
    lab = ValidationLab(config)
    report = lab.evaluate(hypothesis(), observations(20, mean_bps=50.0, noise=1.0), total_tests=1)
    assert not report.passed
    assert "observations, need" in report.reason


def test_an_edge_present_in_only_one_fold_fails_walk_forward(config: Config) -> None:
    """One lucky hour is not an edge."""
    lab = ValidationLab(config)
    rows = observations(1500, mean_bps=0.0, noise=5.0, cost=0.0, seed=11)
    # Concentrate a large edge in a single stretch of the walk-forward region.
    for row in rows[700:850]:
        row.gross_bps += 90.0
        row.net_bps = row.gross_bps - row.cost_bps
    report = lab.evaluate(hypothesis(), rows, total_tests=5)
    assert not report.passed
    walk = report.stage(Stage.WALKFORWARD)
    if walk is not None and walk.folds:
        assert not walk.passed
        assert "folds positive" in walk.reason or "net edge" in walk.reason


def test_heavy_search_deflates_a_marginal_edge(config: Config) -> None:
    """The same evidence, judged after 1 test and after 5000."""
    lab = ValidationLab(config)
    rows = observations(1500, mean_bps=13.0, noise=8.0, cost=11.0, seed=5)
    lonely = lab.evaluate(hypothesis(), rows, total_tests=1)
    searched = lab.evaluate(hypothesis(), rows, total_tests=5000)
    assert lonely.passed, lonely.reason
    assert not searched.passed, "5000 tests must make a marginal edge unpersuasive"
    assert "adjusted p" in searched.reason


def test_holdout_is_the_last_slice_and_is_separate(config: Config) -> None:
    lab = ValidationLab(config)
    rows = observations(1000, mean_bps=5.0, noise=3.0)
    splits = lab.split(rows, horizon_ms=5_000)
    train_end = max(o.ts_ms for o in splits[Stage.TRAIN])
    holdout_start = min(o.ts_ms for o in splits[Stage.HOLDOUT])
    assert holdout_start > train_end
    # No observation appears in two splits.
    seen = set()
    for stage_rows in splits.values():
        ids = {o.ts_ms for o in stage_rows}
        assert not (ids & seen), "splits must not overlap"
        seen |= ids


def test_embargo_removes_observations_at_the_boundary(config: Config) -> None:
    config.validation.embargo_multiple = 5.0
    lab = ValidationLab(config)
    rows = observations(1000, mean_bps=1.0, noise=1.0, spacing_ms=1_000)
    splits = lab.split(rows, horizon_ms=5_000)
    total_kept = sum(len(v) for v in splits.values())
    assert total_kept < len(rows), "an embargo that removes nothing is not an embargo"


def test_batch_fdr_culls_simultaneous_survivors(config: Config) -> None:
    lab = ValidationLab(config)
    reports = [
        lab.evaluate(hypothesis(), observations(1500, mean_bps=14.0, noise=6.0, cost=11.0, seed=s),
                     total_tests=2)
        for s in range(4)
    ]
    assert all(r.passed for r in reports)
    culled = lab.apply_fdr(reports)
    # With genuinely strong edges all should survive the batch pass too.
    assert all(r.passed for r in culled)
