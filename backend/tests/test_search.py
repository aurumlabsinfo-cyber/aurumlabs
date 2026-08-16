"""Strategy search: does it refuse to crown a winner that is not there?

The decisive test is `test_pure_noise_yields_no_edge`. A search over ~50
candidates will always produce a best-of-50 that looks good; a search worth
trusting is one that recognises its own best candidate as noise.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.ml.dataset import Dataset
from app.ml.search import (
    build_candidates,
    rotation_reality_check,
    search,
)


def make_dataset(
    n: int = 40_000,
    signal_strength: float = 0.0,
    seed: int = 5,
    interval_ms: int = 100,
) -> Dataset:
    """Feature frame shaped like the real one, with a controllable edge.

    `signal_strength` is how strongly `volume_imbalance_1s` predicts the label.
    Zero means the labels are pure noise.
    """
    rng = np.random.default_rng(seed)
    ts = pd.Series(np.arange(n, dtype=np.int64) * interval_ms + 1_700_000_000_000)

    flow1 = rng.normal(0, 0.45, n).clip(-1, 1)
    flow5 = 0.6 * flow1 + rng.normal(0, 0.35, n)
    book = rng.normal(0, 0.4, n).clip(-1, 1)

    prob_up = 0.5 + signal_strength * flow1
    y = pd.Series((rng.random(n) < prob_up).astype(int))

    X = pd.DataFrame(
        {
            "volume_imbalance_1s": flow1,
            "volume_imbalance_5s": flow5,
            "book_imbalance_l1": book,
            "depth_imbalance_5": book * 0.8 + rng.normal(0, 0.2, n),
            "micro_price_dev_bps": rng.normal(0, 0.6, n),
            "return_500ms": rng.normal(0, 1.2, n),
            "return_1000ms": rng.normal(0, 1.6, n),
            "return_2000ms": rng.normal(0, 2.2, n),
            "realized_vol_30s_bps": np.abs(rng.normal(1.5, 0.3, n)) + 0.5,
            "vol_ratio_5s_30s": np.abs(rng.normal(1.0, 0.3, n)),
            "bb_z": rng.normal(0, 1.0, n),
            "vwap_deviation_bps": rng.normal(0, 1.5, n),
        }
    )
    entry = pd.Series(np.full(n, 100_000.0))
    exit_ = entry + np.where(y == 1, 1.0, -1.0)
    return Dataset(X=X, y=y, ts=ts, entry=entry, exit=exit_, horizon_s=5.0)


# ------------------------------------------------------------------- grid
def test_the_grid_is_broad_but_not_absurd():
    candidates = build_candidates()
    # Wide enough to be a real search, small enough that the correction does
    # not swallow a genuine effect.
    assert 30 < len(candidates) < 200
    assert len({c.name for c in candidates}) == len(candidates)
    assert {c.family for c in candidates} >= {
        "order_flow", "book_imbalance_l1", "momentum", "mean_reversion_bb",
    }


def test_families_can_be_narrowed():
    only_flow = build_candidates(families=["order_flow"])
    assert only_flow
    assert {c.family for c in only_flow} == {"order_flow"}


def test_a_higher_threshold_takes_fewer_trades():
    ds = make_dataset(5_000)
    low = next(c for c in build_candidates() if c.name == "order_flow(window=1s,th=0.2)")
    high = next(c for c in build_candidates() if c.name == "order_flow(window=1s,th=0.65)")
    assert low.signals(ds.X)[0].sum() > high.signals(ds.X)[0].sum()


# ------------------------------------------------------------ reality check
def test_rotation_null_is_centred_near_a_coin_flip():
    """With no relationship, the per-candidate null must sit around 0.5."""
    rng = np.random.default_rng(3)
    n = 20_000
    y = np.where(rng.random(n) < 0.5, 1.0, -1.0)
    pred = np.where(rng.random(n) < 0.5, 1.0, -1.0)
    out = rotation_reality_check([pred], y, horizon_s=5.0, rows_per_second=10.0)
    assert out["ran"] is True
    assert 0.45 < out["null_max_mean"] < 0.55


def test_the_null_maximum_rises_with_the_number_of_candidates():
    """This is the whole point: more candidates, higher bar."""
    rng = np.random.default_rng(4)
    n = 20_000
    y = np.where(rng.random(n) < 0.5, 1.0, -1.0)
    few = [np.where(rng.random(n) < 0.5, 1.0, -1.0) for _ in range(2)]
    many = [np.where(rng.random(n) < 0.5, 1.0, -1.0) for _ in range(60)]
    a = rotation_reality_check(few, y, 5.0, 10.0)
    b = rotation_reality_check(many, y, 5.0, 10.0)
    assert b["null_max_p95"] > a["null_max_p95"]


def test_reality_check_declines_on_a_short_sample():
    rng = np.random.default_rng(6)
    y = np.where(rng.random(300) < 0.5, 1.0, -1.0)
    pred = np.where(rng.random(300) < 0.5, 1.0, -1.0)
    out = rotation_reality_check([pred], y, horizon_s=5.0, rows_per_second=10.0)
    assert out["ran"] is False
    assert "rotations" in out["reason"] or "guard" in out["reason"]


# ------------------------------------------------------------------ search
def test_pure_noise_yields_no_edge():
    """A search over ~50 candidates on noise must NOT declare a winner."""
    report = search(make_dataset(40_000, signal_strength=0.0), n_splits=4)
    assert report["status"] == "COMPLETE"
    assert report["candidates_tested"] > 30
    # The best candidate will look good - that is exactly the trap.
    assert report["best"]["win_rate"] > 0.5
    # And the verdict must still be no.
    assert report["verdict"] in ("NO EDGE", "INCONCLUSIVE")
    assert "NESSUN EDGE ROBUSTO IDENTIFICATO" in report["conclusion"]
    assert report["reality_check"]["significant_after_correction"] is False


def test_a_planted_edge_is_found():
    report = search(make_dataset(40_000, signal_strength=0.16), n_splits=4)
    assert report["status"] == "COMPLETE"
    assert report["reality_check"]["significant_after_correction"] is True
    assert report["verdict"] in ("CANDIDATE EDGE", "PROMISING")
    # The edge was planted in volume_imbalance_1s, so the winner must be a
    # flow-based family. Which one wins depends on the trade-count/accuracy
    # trade-off, since ranking is by the CI lower bound.
    assert "flow" in report["best"]["family"]
    assert report["best"]["win_rate"] > 0.52


def test_a_real_edge_below_break_even_is_not_called_tradable():
    """Predicting correctly and making money are different questions."""
    report = search(
        make_dataset(40_000, signal_strength=0.16), payout=0.8, n_splits=4
    )
    assert report["break_even_win_rate"] == round(1 / 1.8, 5)
    if report["best"]["win_rate"] <= report["break_even_win_rate"]:
        assert report["verdict"] == "STATISTICAL EDGE, NOT PROFITABLE"
        assert "still loses money" in " ".join(report["notes"])


def test_candidates_are_ranked_by_worst_case_not_point_estimate():
    report = search(make_dataset(30_000, signal_strength=0.1), n_splits=4)
    board = report["leaderboard"]
    lower_bounds = [r["ci95"][0] for r in board]
    assert lower_bounds == sorted(lower_bounds, reverse=True)


def test_thin_candidates_are_excluded_from_the_ranking():
    report = search(make_dataset(30_000), n_splits=4, min_trades=5_000)
    for row in report["leaderboard"]:
        assert row["trades"] >= 5_000
    excluded = [r for r in report["all_results"] if not r["eligible"] and r["trades"]]
    assert excluded, "expected some candidate to fall below the trade floor"
    assert "need" in excluded[0]["reason"]


def test_search_refuses_a_dataset_too_small_to_split():
    report = search(make_dataset(200), n_splits=4)
    assert report["status"] == "INSUFFICIENT_DATA"
    assert "not enough" in report["conclusion"]


def test_no_eligible_candidate_is_reported_honestly():
    report = search(make_dataset(30_000), n_splits=4, min_trades=10_000_000)
    assert report["status"] == "NO_ELIGIBLE_CANDIDATE"
    assert "NESSUN EDGE ROBUSTO IDENTIFICATO" in report["conclusion"]


def test_report_states_how_many_candidates_were_tested():
    """The reader must be able to see the search breadth without digging."""
    report = search(make_dataset(30_000), n_splits=4)
    assert str(report["candidates_tested"]) in " ".join(report["notes"])
