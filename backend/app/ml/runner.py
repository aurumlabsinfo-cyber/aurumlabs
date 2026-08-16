"""Backtest orchestration: horizons x models x strategies -> one honest report.

Run order:

1. build a causal dataset for each horizon from **recorded** data;
2. run leakage checks (structural + shuffled-label control);
3. walk-forward validate every model with a purge gap;
4. evaluate the rule-based strategies on the same folds;
5. classify the edge, refusing to pick a winner on in-sample profit alone;
6. Monte Carlo the resulting outcome sequence.

If there is not enough data, the report says so and stops. It never
extrapolates a verdict from a handful of rows.
"""

from __future__ import annotations

from typing import Any, Sequence


from app.config import Settings
from app.core.clock import now_ms
from app.core.logging_conf import get_logger
from app.db.repository import record_model_version
from app.ml import montecarlo
from app.ml.dataset import Dataset, build_dataset, impute, label_balance, load_raw
from app.ml.inference import save_model
from app.ml.models import DEFAULT_MODELS, available, build
from app.ml.strategies import evaluate_all
from app.ml.validation import (
    EDGE_PROVEN,
    classify_edge,
    leakage_checks,
    run_walk_forward,
    walk_forward_splits,
)

log = get_logger(__name__)

DEFAULT_HORIZONS = (1.0, 2.0, 3.0, 5.0, 10.0, 15.0, 30.0)


async def run_backtest(
    settings: Settings,
    horizons: Sequence[float] = DEFAULT_HORIZONS,
    models: Sequence[str] | None = None,
    n_splits: int = 5,
    include_synthetic: bool = False,
    save_best: bool = False,
    min_rows: int | None = None,
) -> dict[str, Any]:
    models = list(models or [m for m in DEFAULT_MODELS if m in available()])
    min_rows = min_rows if min_rows is not None else settings.ml_min_samples

    features_df, ticks_df = await load_raw(
        settings.symbol, include_synthetic=include_synthetic
    )
    report: dict[str, Any] = {
        "generated_at": now_ms(),
        "symbol": settings.symbol,
        "primary_horizon_s": settings.signal_horizon_s,
        "payout": settings.binary_payout,
        "payout_status": (
            "KNOWN" if settings.binary_payout is not None else "PAYOUT UNKNOWN"
        ),
        "include_synthetic": include_synthetic,
        "models_tested": models,
        "raw_feature_rows": int(len(features_df)),
        "raw_tick_rows": int(len(ticks_df)),
        "horizons": {},
        "warnings": [],
    }
    if include_synthetic:
        report["warnings"].append(
            "SYNTHETIC DATA INCLUDED - this report describes the simulator, not "
            "the market. It cannot establish an edge."
        )

    if len(features_df) < 200 or len(ticks_df) < 200:
        report["status"] = "INSUFFICIENT_DATA"
        report["conclusion"] = (
            f"Only {len(features_df)} feature rows and {len(ticks_df)} ticks are "
            "recorded. Run the engine against the live feed to collect data "
            "before backtesting. No edge can be evaluated on this."
        )
        return report

    for h in horizons:
        report["horizons"][f"{h:g}s"] = await _run_one_horizon(
            settings, features_df, ticks_df, h, models, n_splits, min_rows,
            save_best=save_best,
        )

    report["comparison"] = _compare_horizons(report["horizons"])
    report["status"] = "COMPLETE"
    report["conclusion"] = _overall_conclusion(report)
    return report


async def _run_one_horizon(
    settings: Settings,
    features_df,
    ticks_df,
    horizon_s: float,
    models: Sequence[str],
    n_splits: int,
    min_rows: int,
    save_best: bool = False,
) -> dict[str, Any]:
    ds = build_dataset(
        features_df, ticks_df, horizon_s=horizon_s,
        min_data_quality=0.0, require_book_synced=True,
    )
    out: dict[str, Any] = {
        "horizon_s": horizon_s,
        "dataset": ds.describe(),
        "label_balance": label_balance(ds.y) if len(ds) else None,
    }
    if len(ds) < 300:
        out["status"] = "INSUFFICIENT_DATA"
        out["conclusion"] = (
            f"{len(ds)} labelled rows at this horizon - not enough to test anything."
        )
        return out

    out["leakage_checks"] = leakage_checks(ds)
    out["splits"] = [
        {
            "fold": s.fold,
            "train_rows": int(len(s.train_idx)),
            "test_rows": int(len(s.test_idx)),
            "purge_gap_ms": s.test_start_ts - s.train_end_ts,
        }
        for s in walk_forward_splits(
            ds.ts.to_numpy(), n_splits=n_splits, horizon_s=horizon_s,
            embargo_s=settings.ml_embargo_s,
        )
    ]

    results: dict[str, Any] = {}
    edges: dict[str, Any] = {}
    for name in models:
        try:
            r = run_walk_forward(
                ds, name, n_splits=n_splits, embargo_s=settings.ml_embargo_s,
                payout=settings.binary_payout,
            )
        except Exception as exc:  # noqa: BLE001
            results[name] = {"model": name, "error": f"{type(exc).__name__}: {exc}"}
            continue
        results[name] = r.to_dict()
        edges[name] = classify_edge(
            r, out["leakage_checks"], settings.binary_payout, min_rows=min_rows
        )
    out["models"] = results
    out["edge_by_model"] = edges
    out["strategies"] = evaluate_all(
        ds, payout=settings.binary_payout, n_splits=n_splits
    )

    best_name, best_edge = _select_best(results, edges)
    out["best_model"] = best_name
    out["edge"] = best_edge
    out["selection_note"] = (
        "Selection is by out-of-sample accuracy lower confidence bound and fold "
        "consistency - never by highest historical profit."
    )

    if best_name and results.get(best_name, {}).get("out_of_sample"):
        oos = results[best_name]["out_of_sample"]
        n = int(oos.get("n") or 0)
        acc = oos.get("accuracy")
        if acc and n >= 50:
            out["monte_carlo"] = montecarlo.simulate(
                win_rate=float(acc), n_trades=min(n, 1000),
                payout=settings.binary_payout, starting_bankroll=20.0,
            )

    if save_best and best_name and best_edge.get("classification") in (
        EDGE_PROVEN, "PROMISING"
    ):
        out["saved_model"] = await _fit_and_save(settings, ds, best_name, out)
    return out


def _fit_with_calibration(
    settings: Settings, ds: Dataset, model_name: str, X
) -> tuple[Any, bool, dict[str, Any]]:
    """Fit the model, calibrated when there is enough data to do it honestly.

    An uncalibrated model is blended into the live ensemble at 0.3 weight
    instead of 0.5, and `calibrated` was hard-coded False - so no amount of
    retraining could ever promote a model. Calibration here is fitted on a
    held-out TAIL of the series, separated from the training rows by one
    horizon plus the embargo, exactly like the walk-forward folds. Calibrating
    on the training rows would produce a confident-looking model that has
    learned its own residuals.

    Returns (fitted estimator, calibrated?, note).
    """
    import numpy as np

    y = ds.y.to_numpy()
    ts = ds.ts.to_numpy()
    n = len(y)
    if not settings.calibrate_models or n < 2000:
        model = build(model_name)
        model.fit(X, y)
        return model, False, {"calibration": "skipped: not enough rows"}

    cut = int(n * 0.8)
    gap_ms = int((ds.horizon_s + settings.ml_embargo_s) * 1000)
    cal_start = int(np.searchsorted(ts, ts[cut - 1] + gap_ms, "left"))
    cal_rows = n - cal_start
    train_y = y[:cut]
    cal_y = y[cal_start:]
    # Isotonic regression needs both classes and a real sample to be worth
    # anything; below that, an uncalibrated model is the honest answer.
    if cal_rows < 500 or len(set(cal_y.tolist())) < 2 or len(set(train_y.tolist())) < 2:
        model = build(model_name)
        model.fit(X, y)
        return model, False, {
            "calibration": f"skipped: only {cal_rows} usable hold-out rows"
        }

    from sklearn.calibration import CalibratedClassifierCV

    base = build(model_name)
    base.fit(X.iloc[:cut], train_y)
    calibrated = CalibratedClassifierCV(base, cv="prefit", method="isotonic")
    calibrated.fit(X.iloc[cal_start:], cal_y)
    return calibrated, True, {
        "calibration": "isotonic on a purged hold-out tail",
        "train_rows": cut,
        "calibration_rows": cal_rows,
        "purge_gap_ms": gap_ms,
    }


async def _fit_and_save(
    settings: Settings, ds: Dataset, model_name: str, horizon_report: dict
) -> dict[str, Any]:
    """Refit on all available data and persist, with training-distribution stats."""
    X = impute(ds.X)
    model, calibrated, calibration_note = _fit_with_calibration(
        settings, ds, model_name, X
    )
    stats = {
        col: {
            "mean": float(X[col].mean()),
            "std": float(X[col].std() or 0.0),
            "min": float(X[col].min()),
            "max": float(X[col].max()),
        }
        for col in X.columns
    }
    model_id = f"{model_name}_h{ds.horizon_s:g}s_{now_ms()}"
    path = save_model(
        model_dir=settings.model_dir,
        model_id=model_id,
        model=model,
        feature_names=list(X.columns),
        feature_stats=stats,
        horizon_s=ds.horizon_s,
        calibrated=calibrated,
        metadata={
            "edge": horizon_report.get("edge"),
            "symbol": settings.symbol,
            "rows": len(ds),
            **calibration_note,
        },
    )

    # Record the version so an activated model can always be traced back to the
    # data window, the validation it passed and the verdict it was given.
    wf = (horizon_report.get("models") or {}).get(model_name) or {}
    ts_values = ds.ts.to_numpy()
    recorded = True
    try:
        await record_model_version(
            {
                "model_id": model_id,
                "ts": now_ms(),
                "algorithm": model_name,
                "horizon_s": ds.horizon_s,
                "symbol": settings.symbol,
                "feature_names": list(X.columns),
                "train_start_ts": int(ts_values[0]),
                "train_end_ts": int(ts_values[-1]),
                "test_start_ts": None,
                "test_end_ts": None,
                "n_train": len(ds),
                "n_test": int((wf.get("out_of_sample") or {}).get("n") or 0) or None,
                "metrics": {
                    "out_of_sample": wf.get("out_of_sample"),
                    "in_sample": wf.get("in_sample"),
                    "selective_thresholds": wf.get("selective_thresholds"),
                },
                "walkforward": wf.get("folds"),
                "edge_classification": (
                    horizon_report.get("edge") or {}
                ).get("classification"),
                "leakage_checks": horizon_report.get("leakage_checks"),
                "params": {"feature_count": len(X.columns)},
                "artifact_path": path,
                "is_active": False,
                "data_source": "LIVE",
            }
        )
    except Exception as exc:  # noqa: BLE001 - the artifact on disk is the primary
        recorded = False
        log.warning("model_version.record_failed", model_id=model_id, error=str(exc))

    return {
        "model_id": model_id,
        "path": path,
        "rows": len(ds),
        "calibrated": calibrated,
        **calibration_note,
        "recorded_in_db": recorded,
    }


def _select_best(
    results: dict[str, Any], edges: dict[str, Any]
) -> tuple[str | None, dict[str, Any]]:
    """Pick by robustness, not by profit.

    Ranking key: lower bound of the out-of-sample accuracy CI, then fold
    consistency. A model whose edge could be zero ranks below one whose worst
    case is still positive, even if its point estimate is lower.
    """
    best: tuple[float, float, str] | None = None
    for name, r in results.items():
        oos = (r or {}).get("out_of_sample") or {}
        ci = oos.get("accuracy_ci95")
        if not ci:
            continue
        folds = (r or {}).get("folds") or []
        consistency = (
            sum(1 for f in folds if (f.get("accuracy") or 0) > 0.5) / len(folds)
            if folds else 0.0
        )
        key = (float(ci[0]), consistency, name)
        if best is None or key > best:
            best = key
    if best is None:
        return (None, {
            "classification": "INCONCLUSIVE",
            "notes": ["no model produced out-of-sample results"],
            "conclusion": "NESSUN EDGE ROBUSTO IDENTIFICATO / NO ROBUST EDGE IDENTIFIED",
        })
    name = best[2]
    return (name, edges.get(name, {}))


def _compare_horizons(horizons: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for label, h in horizons.items():
        edge = h.get("edge") or {}
        oos = edge.get("out_of_sample") or {}
        rows.append(
            {
                "horizon": label,
                "rows": (h.get("dataset") or {}).get("rows"),
                "best_model": h.get("best_model"),
                "oos_accuracy": oos.get("accuracy"),
                "ci95": oos.get("accuracy_ci95"),
                "p_value": oos.get("p_value_vs_coinflip"),
                "classification": edge.get("classification"),
            }
        )
    ranked = sorted(
        [r for r in rows if r.get("ci95")],
        key=lambda r: r["ci95"][0], reverse=True,
    )
    return {
        "table": rows,
        "ranked_by_worst_case_accuracy": [r["horizon"] for r in ranked],
        "note": (
            "The 5s horizon is the target, but whether it carries an edge is an "
            "empirical question answered by this table - not an assumption."
        ),
    }


def _overall_conclusion(report: dict[str, Any]) -> str:
    primary = f"{report['primary_horizon_s']:g}s"
    h = report["horizons"].get(primary) or {}
    edge = h.get("edge") or {}
    classification = edge.get("classification")
    if not classification:
        return (
            f"No verdict at the {primary} horizon: "
            f"{h.get('conclusion', 'insufficient data')}"
        )
    if classification == EDGE_PROVEN:
        return (
            f"{primary}: {classification}. "
            + " ".join(edge.get("notes", []))
            + " This is a statistical result on recorded data, not a promise of "
            "future profit; it must be re-validated on new data before use."
        )
    return (
        f"{primary}: {classification}. "
        + " ".join(edge.get("notes", []))
        + " NESSUN EDGE ROBUSTO IDENTIFICATO al momento / no robust edge "
        "identified at this horizon."
    )


def dataset_readiness(feature_rows: int, tick_rows: int, min_rows: int) -> dict[str, Any]:
    return {
        "feature_rows": feature_rows,
        "tick_rows": tick_rows,
        "min_rows_for_verdict": min_rows,
        "ready": feature_rows >= min_rows,
        "estimated_minutes_recorded": round(feature_rows / 600, 1),
        "note": (
            "At the default 100ms feature cadence, 5,000 rows is roughly 8 "
            "minutes of market. That is enough to exercise the pipeline and far "
            "too little to establish an edge - target hours to days."
        ),
    }
