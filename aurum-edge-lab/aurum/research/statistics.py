"""Statistics for edge validation.

Three things here decide whether the whole research pipeline means anything.

**Overlapping observations are not independent.**  With a 250 ms sampling grid
and a 5-second horizon, twenty consecutive triggers share nineteen twentieths of
their outcome window.  Treating them as 20 independent samples inflates the
t-statistic by roughly √20 and turns noise into a discovery.  Every significance
test here therefore runs on a *non-overlapping* subset, and both counts are
reported so the difference is visible rather than assumed away.

**The p-value needs a real distribution.**  A normal approximation is fine at
n=1000 and badly wrong at n=30, which is exactly the sample size a rare signal
produces.  Student's t is computed properly, via the regularized incomplete beta
function, with no SciPy dependency.

**Testing many hypotheses guarantees false positives.**  At α=0.05, one in twenty
worthless hypotheses passes.  A system proposing thirty per cycle finds "edges"
every cycle forever.  Benjamini-Hochberg controls the false discovery rate
against the number of tests research memory has actually counted — not against
the number in this batch, which would reset the clock every cycle.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

from ..domain import MetricSet

# ---------------------------------------------------------------------------
# Distributions
# ---------------------------------------------------------------------------

_MAX_ITERATIONS = 300
_EPSILON = 3.0e-12


def _log_beta(a: float, b: float) -> float:
    return math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b)


def _beta_continued_fraction(a: float, b: float, x: float) -> float:
    """Lentz's algorithm for the continued fraction of the incomplete beta."""
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < 1e-30:
        d = 1e-30
    d = 1.0 / d
    h = d
    for m in range(1, _MAX_ITERATIONS + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-30:
            d = 1e-30
        c = 1.0 + aa / c
        if abs(c) < 1e-30:
            c = 1e-30
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-30:
            d = 1e-30
        c = 1.0 + aa / c
        if abs(c) < 1e-30:
            c = 1e-30
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < _EPSILON:
            break
    return h


def regularized_incomplete_beta(a: float, b: float, x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    front = math.exp(a * math.log(x) + b * math.log(1.0 - x) - _log_beta(a, b))
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _beta_continued_fraction(a, b, x) / a
    return 1.0 - math.exp(
        b * math.log(1.0 - x) + a * math.log(x) - _log_beta(b, a)
    ) * _beta_continued_fraction(b, a, 1.0 - x) / b


def student_t_two_tailed_p(t: float, df: float) -> float:
    """Two-tailed p-value for a t statistic.  1.0 when there is no evidence."""
    if df <= 0 or not math.isfinite(t):
        return 1.0
    t = abs(t)
    if t == 0.0:
        return 1.0
    x = df / (df + t * t)
    return max(0.0, min(1.0, regularized_incomplete_beta(df / 2.0, 0.5, x)))


def student_t_critical(df: float, confidence: float = 0.95) -> float:
    """Two-sided critical value, found by bisection on the p-value.

    Bisection rather than a table: the tables stop at df=120 and the sample
    sizes here move continuously.
    """
    if df <= 0:
        return 0.0
    target = 1.0 - confidence
    low, high = 0.0, 100.0
    for _ in range(80):
        mid = (low + high) / 2.0
        if student_t_two_tailed_p(mid, df) > target:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0


# ---------------------------------------------------------------------------
# Sample independence
# ---------------------------------------------------------------------------


def independent_indices(timestamps: Sequence[int], horizon_ms: int) -> list[int]:
    """Greedily select observations whose outcome windows do not overlap.

    This is the difference between a hypothesis that looks significant and one
    that is.  Two triggers 250 ms apart measuring a 5-second horizon are almost
    the same observation; counting both is counting the same evidence twice.
    """
    if horizon_ms <= 0:
        return list(range(len(timestamps)))
    chosen: list[int] = []
    next_free = -1
    for index, stamp in enumerate(timestamps):
        if stamp >= next_free:
            chosen.append(index)
            next_free = stamp + horizon_ms
    return chosen


def effective_sample_size(timestamps: Sequence[int], horizon_ms: int) -> int:
    return len(independent_indices(timestamps, horizon_ms))


# ---------------------------------------------------------------------------
# Descriptive metrics
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class SampleStats:
    n: int
    mean: float
    stdev: float
    t_stat: float
    p_value: float
    ci_low: float
    ci_high: float


def sample_stats(values: Sequence[float], confidence: float = 0.95) -> SampleStats:
    n = len(values)
    if n < 2:
        mean = values[0] if n else 0.0
        return SampleStats(n=n, mean=mean, stdev=0.0, t_stat=0.0, p_value=1.0, ci_low=mean, ci_high=mean)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / (n - 1)
    stdev = math.sqrt(variance)
    if stdev <= 0:
        # Every observation identical: real but degenerate. No dispersion means
        # no uncertainty to report, and a t-statistic would be infinite.
        return SampleStats(n=n, mean=mean, stdev=0.0, t_stat=0.0, p_value=1.0, ci_low=mean, ci_high=mean)
    standard_error = stdev / math.sqrt(n)
    t_stat = mean / standard_error
    p_value = student_t_two_tailed_p(t_stat, n - 1)
    margin = student_t_critical(n - 1, confidence) * standard_error
    return SampleStats(
        n=n, mean=mean, stdev=stdev, t_stat=t_stat, p_value=p_value,
        ci_low=mean - margin, ci_high=mean + margin,
    )


def max_drawdown_bps(values: Sequence[float]) -> float:
    """Deepest peak-to-trough decline of the cumulative net return."""
    peak = 0.0
    cumulative = 0.0
    worst = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        worst = max(worst, peak - cumulative)
    return worst


def percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(pct / 100.0 * (len(ordered) - 1)))))
    return ordered[index]


def build_metrics(
    gross: Sequence[float],
    net: Sequence[float],
    costs: Sequence[float],
    timestamps: Sequence[int],
    horizon_ms: int,
    *,
    confidence: float = 0.95,
) -> MetricSet:
    """The blueprint's minimum metrics, computed on net returns.

    ``t_stat`` and ``p_value`` come from the non-overlapping subset; everything
    descriptive uses every observation, because a win rate over the full set is
    a fair description even when it is not independent evidence.
    """
    metrics = MetricSet()
    if not net:
        return metrics

    metrics.samples = len(net)
    wins = [v for v in net if v > 0]
    losses = [v for v in net if v <= 0]
    metrics.wins = len(wins)
    metrics.losses = len(losses)
    metrics.gross_edge_bps = sum(gross) / len(gross) if gross else 0.0
    metrics.net_edge_bps = sum(net) / len(net)
    metrics.cost_bps = sum(costs) / len(costs) if costs else 0.0
    metrics.avg_win_bps = sum(wins) / len(wins) if wins else 0.0
    metrics.avg_loss_bps = sum(losses) / len(losses) if losses else 0.0
    total_win = sum(wins)
    total_loss = -sum(losses)
    metrics.profit_factor = (total_win / total_loss) if total_loss > 0 else (
        float("inf") if total_win > 0 else 0.0
    )
    metrics.max_drawdown_bps = max_drawdown_bps(net)
    metrics.tail_loss_bps = percentile(net, 5.0)

    independent = independent_indices(timestamps, horizon_ms) if timestamps else list(range(len(net)))
    independent_net = [net[i] for i in independent]
    stats = sample_stats(independent_net, confidence)
    metrics.stdev_bps = stats.stdev
    metrics.t_stat = stats.t_stat
    metrics.p_value = stats.p_value
    metrics.ci_low_bps = stats.ci_low
    metrics.ci_high_bps = stats.ci_high

    if timestamps and len(timestamps) > 1:
        span_hours = (timestamps[-1] - timestamps[0]) / 3_600_000.0
        metrics.events_per_hour = len(net) / span_hours if span_hours > 0 else 0.0
    return metrics


# ---------------------------------------------------------------------------
# Multiple testing
# ---------------------------------------------------------------------------


def benjamini_hochberg(p_values: Sequence[float], alpha: float = 0.10) -> list[bool]:
    """Which p-values survive FDR control at ``alpha``."""
    n = len(p_values)
    if n == 0:
        return []
    indexed = sorted(enumerate(p_values), key=lambda pair: pair[1])
    passed = [False] * n
    largest_k = -1
    for rank, (_, p) in enumerate(indexed, start=1):
        if p <= alpha * rank / n:
            largest_k = rank
    for rank, (original_index, _) in enumerate(indexed, start=1):
        if rank <= largest_k:
            passed[original_index] = True
    return passed


def adjusted_p_value(p_value: float, total_tests: int) -> float:
    """Šidák adjustment for the number of tests already spent.

    Used where a single hypothesis has to be judged on its own — a batch FDR
    needs a batch, and research memory's running test count is the honest
    denominator when there is only one candidate in front of you.
    """
    tests = max(1, total_tests)
    if p_value >= 1.0:
        return 1.0
    return min(1.0, 1.0 - (1.0 - p_value) ** tests)


def deflated_score(net_edge_bps: float, t_stat: float, total_tests: int) -> float:
    """Rank score that pays for the size of the search that produced it.

    A t-statistic of 3 from one test is evidence; the same t from five hundred
    tests is the best of five hundred coin flips.  The expected maximum of n
    standard normals grows like √(2 ln n), so that is what gets subtracted.
    """
    tests = max(1, total_tests)
    expected_max_t = math.sqrt(2.0 * math.log(tests)) if tests > 1 else 0.0
    deflation = max(0.0, abs(t_stat) - expected_max_t)
    if abs(t_stat) <= 0:
        return 0.0
    return net_edge_bps * (deflation / abs(t_stat))
