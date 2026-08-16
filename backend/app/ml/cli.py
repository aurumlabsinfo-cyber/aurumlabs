"""Offline research CLI.

    python -m app.ml.cli status
    python -m app.ml.cli backtest --horizons 1,2,3,5,10,15,30
    python -m app.ml.cli backtest --horizons 5 --models xgboost,lightgbm --save-best
    python -m app.ml.cli strategies --horizon 5
    python -m app.ml.cli shadow
    python -m app.ml.cli montecarlo --payout 0.8

Everything reads the same recorded data the live engine writes. Nothing here
touches an exchange.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from app.config import get_settings
from app.core.logging_conf import configure_logging
from app.db.engine import create_schema, dispose_engine, init_engine
from app.db.repository import table_counts
from app.ml import montecarlo
from app.ml.dataset import build_dataset, horizons_from_string, load_raw
from app.ml.runner import DEFAULT_HORIZONS, dataset_readiness, run_backtest
from app.ml.strategies import evaluate_all


def _print(payload: Any) -> None:
    print(json.dumps(payload, indent=2, default=str))


async def cmd_status(args: argparse.Namespace) -> int:
    settings = get_settings()
    counts = await table_counts()
    _print(
        {
            "symbol": settings.symbol,
            "tables": counts,
            "readiness": dataset_readiness(
                counts.get("features", 0), counts.get("market_ticks", 0),
                settings.ml_min_samples,
            ),
            "payout": settings.binary_payout,
            "payout_status": (
                "KNOWN" if settings.binary_payout is not None else "PAYOUT UNKNOWN"
            ),
        }
    )
    return 0


async def cmd_backtest(args: argparse.Namespace) -> int:
    settings = get_settings()
    horizons = (
        horizons_from_string(args.horizons) if args.horizons else list(DEFAULT_HORIZONS)
    )
    models = [m.strip() for m in args.models.split(",")] if args.models else None
    report = await run_backtest(
        settings,
        horizons=horizons,
        models=models,
        n_splits=args.splits,
        include_synthetic=args.include_synthetic,
        save_best=args.save_best,
    )
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(report, fh, indent=2, default=str)
        print(f"report written to {args.out}")
    if args.summary:
        _print(
            {
                "status": report["status"],
                "conclusion": report.get("conclusion"),
                "comparison": report.get("comparison", {}).get("table"),
                "warnings": report.get("warnings"),
            }
        )
    else:
        _print(report)
    return 0


async def cmd_strategies(args: argparse.Namespace) -> int:
    settings = get_settings()
    features, ticks = await load_raw(
        settings.symbol, include_synthetic=args.include_synthetic
    )
    ds = build_dataset(features, ticks, horizon_s=args.horizon)
    if len(ds) < 300:
        _print(
            {
                "error": f"only {len(ds)} labelled rows - collect more data first",
                "rows": len(ds),
            }
        )
        return 1
    _print(
        {
            "dataset": ds.describe(),
            "strategies": evaluate_all(ds, payout=settings.binary_payout),
            "note": (
                "Rule strategies are stateless, so these ARE their out-of-sample "
                "numbers. Compare the models against them, not against zero."
            ),
        }
    )
    return 0


async def cmd_import(args: argparse.Namespace) -> int:
    """Download real historical market data and replay it into the database."""
    from datetime import datetime

    from app.ml.importer import import_days

    settings = get_settings()
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = (
        datetime.strptime(args.end, "%Y-%m-%d").date()
        if args.end
        else start + __import__("datetime").timedelta(days=args.days - 1)
    )
    symbol = args.symbol or settings.symbol

    print(f"Importing {symbol} from {start} to {end} (public Binance archive)")
    print("This is REAL market data, replayed through the live feature engine.\n")

    def progress(day, stats) -> None:
        print(
            f"  {day}: {stats.book_rows:,} quotes, {stats.trade_rows:,} trades, "
            f"{stats.feature_rows:,} feature rows"
        )

    stats = await import_days(
        settings, symbol, start, end,
        cache_dir=args.cache, persist=not args.dry_run, progress=progress,
    )
    print()
    _print(stats.summary())
    return 0 if stats.days else 1


async def cmd_search(args: argparse.Namespace) -> int:
    """Search the strategy space, correcting for the breadth of the search."""
    from app.ml.dataset import build_dataset, load_raw
    from app.ml.search import search

    settings = get_settings()
    features, ticks = await load_raw(
        settings.symbol, include_synthetic=args.include_synthetic
    )
    if features.empty:
        _print({
            "error": "no data. Run `python -m app.ml.cli import` first, or let "
                     "the live engine record for a few hours.",
        })
        return 1

    ds = build_dataset(features, ticks, horizon_s=args.horizon)
    report = search(
        ds,
        payout=args.payout if args.payout is not None else settings.binary_payout,
        n_splits=args.splits,
        embargo_s=settings.ml_embargo_s,
        families=args.families.split(",") if args.families else None,
        min_trades=args.min_trades,
    )
    report["dataset"] = ds.describe()
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(report, fh, indent=2, default=str)
        print(f"report written to {args.out}\n")
    if args.summary:
        _print({
            "verdict": report.get("verdict"),
            "conclusion": report.get("conclusion"),
            "notes": report.get("notes"),
            "best": report.get("best"),
            "reality_check": report.get("reality_check"),
            "candidates_tested": report.get("candidates_tested"),
            "dataset": report["dataset"],
        })
    else:
        _print(report)
    return 0


async def cmd_shadow(args: argparse.Namespace) -> int:
    """Score every evaluated window, including the ones no signal came from."""
    from app.ml.shadow import evaluate, load_shadow

    settings = get_settings()
    shadow, ticks = await load_shadow(
        settings.symbol, include_synthetic=args.include_synthetic
    )
    if shadow.empty:
        _print({
            "error": "no shadow rows recorded. SHADOW_DECISIONS_ENABLED must be "
                     "true and the engine must have been running.",
        })
        return 1
    _print(evaluate(shadow, ticks))
    return 0


async def cmd_burst(args: argparse.Namespace) -> int:
    """Replay AURUM BURST-15 - sessions, cooldown, stop-loss - on recorded data."""
    from app.ml.burst_backtest import run_from_db

    settings = get_settings()
    if args.n5 is not None:
        settings.burst_n5_min = args.n5
    if args.r10 is not None:
        settings.burst_r10_min_bps = args.r10
    if args.no_ofi:
        settings.burst_require_ofi_agree = False
    report = await run_from_db(
        settings, include_synthetic=args.include_synthetic, payout=args.payout
    )
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(report, fh, indent=2, default=str)
        print(f"report written to {args.out}\n")
    if args.summary:
        _print({k: v for k, v in report.items() if k != "sessions"})
    else:
        _print(report)
    return 0


async def cmd_burst_grid(args: argparse.Namespace) -> int:
    """Sweep the trigger thresholds, the way the original script's `grid` did."""
    from app.ml.burst_backtest import simulate
    from app.ml.dataset import load_raw

    settings = get_settings()
    features, ticks = await load_raw(
        settings.symbol, include_synthetic=args.include_synthetic
    )
    if features.empty:
        _print({"error": "no recorded data. Import or record first."})
        return 1

    print(f"{'n5':>5}{'|r10|':>8}{'ofi':>5}{'trades':>8}{'win':>8}"
          f"{'EV':>9}{'PnL':>9}{'sessions':>10}")
    for n5 in (10, 20, 40, 60, 100):
        for r10 in (0.2, 0.5, 1.0, 2.0):
            for agree in (True, False):
                settings.burst_n5_min = n5
                settings.burst_r10_min_bps = r10
                settings.burst_require_ofi_agree = agree
                rep = simulate(features, ticks, settings, payout=args.payout)
                if rep.get("trades", 0) < args.min_trades:
                    continue
                print(
                    f"{n5:5d}{r10:8.1f}{'yes' if agree else 'no':>5}"
                    f"{rep['trades']:8d}{rep['win_rate_decided'] or 0:8.3f}"
                    f"{rep['ev_per_trade_units']:+9.4f}{rep['pnl_units']:+9.1f}"
                    f"{rep['sessions']['count']:10d}"
                )
    print(
        "\nA grid is a search: the best row here is the maximum of many random "
        "numbers unless it survives `app.ml.cli search`, which corrects for "
        "exactly that."
    )
    return 0


async def cmd_montecarlo(args: argparse.Namespace) -> int:
    from app.db.repository import fetch_all_paper_trades

    settings = get_settings()
    trades = await fetch_all_paper_trades(include_synthetic=args.include_synthetic)
    payout = args.payout if args.payout is not None else settings.binary_payout
    _print(
        montecarlo.from_trades(
            [t.get("result") or "" for t in trades],
            payout=payout,
            n_simulations=args.simulations,
            starting_bankroll=args.bankroll,
        )
    )
    return 0


COMMANDS = {
    "status": cmd_status,
    "import": cmd_import,
    "search": cmd_search,
    "backtest": cmd_backtest,
    "strategies": cmd_strategies,
    "montecarlo": cmd_montecarlo,
    "shadow": cmd_shadow,
    "burst": cmd_burst,
    "burst-grid": cmd_burst_grid,
}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="app.ml.cli", description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="how much data is recorded, and is it enough")

    b = sub.add_parser("backtest", help="walk-forward validation across horizons")
    b.add_argument("--horizons", help="comma separated seconds, e.g. 1,2,3,5,10")
    b.add_argument("--models", help="comma separated model names")
    b.add_argument("--splits", type=int, default=5)
    b.add_argument("--include-synthetic", action="store_true",
                   help="TAINTS the report: simulator rows are not market data")
    b.add_argument("--save-best", action="store_true")
    b.add_argument("--out", help="write the full JSON report to this path")
    b.add_argument("--summary", action="store_true", help="print only the verdict")

    s = sub.add_parser("strategies", help="rule-strategy comparison")
    s.add_argument("--horizon", type=float, default=5.0)
    s.add_argument("--include-synthetic", action="store_true")

    i = sub.add_parser(
        "import", help="download real historical data from the public Binance archive"
    )
    i.add_argument("--symbol", help="defaults to SYMBOL from the environment")
    i.add_argument("--start", required=True, help="first day, YYYY-MM-DD")
    i.add_argument("--end", help="last day, YYYY-MM-DD (defaults to --days from start)")
    i.add_argument("--days", type=int, default=1)
    i.add_argument("--cache", default="./data/binance-archive",
                   help="where to keep the downloaded zips")
    i.add_argument("--dry-run", action="store_true",
                   help="parse and validate without writing to the database")

    se = sub.add_parser(
        "search",
        help="search the strategy space with a multiple-testing correction",
    )
    se.add_argument("--horizon", type=float, default=5.0)
    se.add_argument("--splits", type=int, default=5)
    se.add_argument("--payout", type=float, default=None,
                    help="broker payout, e.g. 0.8; without it only the "
                         "statistical edge is judged, not profitability")
    se.add_argument("--families", help="comma separated, to narrow the search")
    se.add_argument("--min-trades", type=int, default=200)
    se.add_argument("--include-synthetic", action="store_true")
    se.add_argument("--out", help="write the full JSON report here")
    se.add_argument("--summary", action="store_true", help="print only the verdict")

    sh = sub.add_parser(
        "shadow",
        help="hit rate over EVERY evaluated window, emitted or gated out",
    )
    sh.add_argument("--include-synthetic", action="store_true")

    bu = sub.add_parser(
        "burst", help="replay AURUM BURST-15 (sessions and all) on recorded data"
    )
    bu.add_argument("--n5", type=int, help="override BURST_N5_MIN")
    bu.add_argument("--r10", type=float, help="override BURST_R10_MIN_BPS")
    bu.add_argument("--no-ofi", action="store_true",
                    help="do not require order flow to agree with the move")
    bu.add_argument("--payout", type=float, default=None)
    bu.add_argument("--include-synthetic", action="store_true")
    bu.add_argument("--out", help="write the full JSON report here")
    bu.add_argument("--summary", action="store_true")

    bg = sub.add_parser(
        "burst-grid", help="sweep the BURST-15 thresholds over recorded data"
    )
    bg.add_argument("--payout", type=float, default=None)
    bg.add_argument("--min-trades", type=int, default=50)
    bg.add_argument("--include-synthetic", action="store_true")

    m = sub.add_parser("montecarlo", help="risk analysis of recorded paper trades")
    m.add_argument("--payout", type=float, default=None)
    m.add_argument("--simulations", type=int, default=10_000)
    m.add_argument("--bankroll", type=float, default=20.0)
    m.add_argument("--include-synthetic", action="store_true")
    return p


async def _main(argv: list[str]) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)
    init_engine(settings)
    try:
        await create_schema()
        return await COMMANDS[args.command](args)
    finally:
        await dispose_engine()


def main() -> None:
    sys.exit(asyncio.run(_main(sys.argv[1:])))


if __name__ == "__main__":
    main()
