#!/usr/bin/env python3
"""AURUM EDGE LAB — command line entry point.

    python3 main.py run          start the engine and the API
    python3 main.py diagnose     one-shot health and configuration report
    python3 main.py selftest     verify every subsystem against known answers
    python3 main.py export       snapshot config, database and research
    python3 main.py config       print the effective configuration

Paper trading only.  There is no live-order path in this program.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import socket
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from aurum.config import Config, ConfigError, load_config  # noqa: E402
from aurum.logging_setup import get_logger, setup_logging  # noqa: E402

log = get_logger("main")

BANNER = r"""
   _   _   _ ___ _   _ __  __   ___ ___   ___ ___   _      _   ___
  /_\ | | | | _ \ | | |  \/  | | __|   \ / __| __| | |    /_\ | _ )
 / _ \| |_| |   / |_| | |\/| | | _|| |) | (_ | _|  | |__ / _ \| _ \
/_/ \_\\___/|_|_\\___/|_|  |_| |___|___/ \___|___| |____/_/ \_\___/
     autonomous research engine — crypto perpetual futures
                      PAPER TRADING ONLY
"""


def load(args: argparse.Namespace) -> Config:
    overrides: dict[str, Any] = {}
    if getattr(args, "feed", None):
        overrides.setdefault("market", {})["feed"] = args.feed
    if getattr(args, "replay", None):
        overrides.setdefault("market", {})["replay_path"] = str(args.replay)
        overrides["market"]["feed"] = "replay"
    if getattr(args, "port", None):
        overrides.setdefault("api", {})["port"] = args.port
    if getattr(args, "host", None):
        overrides.setdefault("api", {})["host"] = args.host
    if getattr(args, "data_dir", None):
        overrides.setdefault("app", {})["data_dir"] = str(args.data_dir)
    if getattr(args, "log_level", None):
        overrides.setdefault("app", {})["log_level"] = args.log_level
    if getattr(args, "no_research", False):
        overrides.setdefault("research", {})["enabled"] = False
    return load_config(args.config, overrides=overrides or None)


def free_port(host: str, port: int, attempts: int = 20) -> int:
    """Return ``port`` if free, otherwise the next free one.

    Reported loudly by the caller.  A silent port change is how you end up
    reading a dashboard served by yesterday's process.
    """
    for candidate in range(port, port + attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind((host, candidate))
                return candidate
            except OSError:
                continue
    raise RuntimeError(f"no free port in {port}..{port + attempts}")


# --------------------------------------------------------------------- run


def command_run(args: argparse.Namespace) -> int:
    import uvicorn

    from aurum.api.app import create_app

    config = load(args)
    setup_logging(config.app.log_level, config.app.log_json, force=True)
    print(BANNER)

    port = free_port(config.api.host, config.api.port)
    if port != config.api.port:
        print(f"  port {config.api.port} is busy; serving on {port} instead")
        config.api.port = port

    if config.market.feed != "live":
        print("  !! REPLAY FEED — this is not live market data. /health reports feed.live=false")
    print(f"  venue      {config.market.venue}")
    print(f"  symbols    {len(config.market.symbols)}: {', '.join(config.market.symbol_names)}")
    print(f"  wallet     {config.wallet.starting_balance_eur:.2f} {config.wallet.currency} per cycle")
    print(f"  research   {'enabled' if config.research.enabled else 'DISABLED'}")
    print(f"  database   {config.db_url}")
    print(f"  api        http://{config.api.host}:{config.api.port}")
    print(f"  health     curl -s http://{config.api.host}:{config.api.port}/health | python3 -m json.tool")
    print()

    app = create_app(config)
    uvicorn.run(
        app,
        host=config.api.host,
        port=config.api.port,
        log_level=config.app.log_level.lower(),
        access_log=False,
    )
    return 0


# ---------------------------------------------------------------- diagnose


def command_diagnose(args: argparse.Namespace) -> int:
    """A one-shot report: can this machine run the engine, and is it configured?"""
    from aurum.adapters import build_feed
    from aurum.storage.schema import TABLES
    from aurum.storage.sqlite_db import open_database

    try:
        config = load(args)
    except ConfigError as exc:
        print(json.dumps({"ok": False, "stage": "config", "error": str(exc)}, indent=2))
        return 1
    setup_logging("WARNING", force=True)

    report: dict[str, Any] = {
        "ok": True,
        "app": {"name": config.app.name, "version": config.app.version, "env": config.app.env},
        "python": sys.version.split()[0],
        "config_path": str(args.config or "config.yaml"),
        "problems": [],
    }

    report["market"] = {
        "venue": config.market.venue,
        "feed": config.market.feed,
        "live": config.market.feed == "live",
        "symbols": config.market.symbol_names,
        "rest_base": config.market.rest_base,
        "ws_base": config.market.ws_base,
        "streams": config.market.streams,
    }
    report["wallet"] = {
        "starting_balance": config.wallet.starting_balance_eur,
        "currency": config.wallet.currency,
        "risk_per_trade_pct": config.risk.risk_per_trade_pct,
    }
    report["costs"] = {
        "version": config.costs.version,
        "taker_fee_bps": config.costs.taker_fee_bps,
        "round_trip_at_1bps_spread": round(
            1.0
            + 2 * config.costs.taker_fee_bps
            + 2 * config.costs.extra_slippage_bps
            + 2 * (config.costs.latency_ms / 100.0 * config.costs.latency_penalty_bps_per_100ms),
            4,
        ),
    }

    # Database
    try:
        db = open_database(config.db_url)
        db.connect()
        db.create_schema()
        report["database"] = {
            "url": config.db_url,
            "journal_mode": db.query("PRAGMA journal_mode")[0]["journal_mode"],
            "tables": len(TABLES),
            "counts": db.table_counts(),
        }
        db.close()
    except Exception as exc:  # noqa: BLE001
        report["ok"] = False
        report["database"] = {"error": f"{type(exc).__name__}: {exc}"}
        report["problems"].append("database could not be opened")

    # Venue reachability. This is the check that matters most on a fresh box:
    # if the endpoints do not answer, nothing downstream will work and the
    # reason should be visible here rather than as silence at runtime.
    async def check_feed() -> dict[str, Any]:
        feed = build_feed(config)
        try:
            return await feed.verify_endpoints()
        finally:
            await feed.stop()

    try:
        report["endpoints"] = asyncio.run(check_feed())
        if not report["endpoints"].get("ok"):
            report["ok"] = False
            report["problems"].append(
                "market endpoints did not verify: "
                + str(report["endpoints"].get("error", "unknown reason"))
            )
    except Exception as exc:  # noqa: BLE001
        report["ok"] = False
        report["endpoints"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        report["problems"].append("endpoint verification raised")

    report["ports"] = {
        "configured": config.api.port,
        "free": free_port(config.api.host, config.api.port),
    }
    if report["ports"]["free"] != config.api.port:
        report["problems"].append(
            f"port {config.api.port} is in use; the engine would serve on {report['ports']['free']}"
        )

    print(json.dumps(report, indent=2, default=str))
    if report["problems"]:
        print("\nProblems:", file=sys.stderr)
        for problem in report["problems"]:
            print(f"  - {problem}", file=sys.stderr)
    return 0 if report["ok"] else 1


# ---------------------------------------------------------------- selftest


def command_selftest(args: argparse.Namespace) -> int:
    from aurum.diagnostics.selftest import run_selftest

    setup_logging("ERROR", force=True)
    try:
        config = load(args)
    except ConfigError:
        config = None
    report = run_selftest(config)
    if args.json:
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print(report.render())
    return 0 if report.passed else 1


# ------------------------------------------------------------------ export


def command_export(args: argparse.Namespace) -> int:
    from aurum.diagnostics.export import export_snapshot
    from aurum.storage.repositories import Repositories
    from aurum.storage.sqlite_db import open_database

    config = load(args)
    setup_logging(config.app.log_level, force=True)
    db = open_database(config.db_url)
    db.connect()
    db.create_schema()
    try:
        manifest = export_snapshot(
            config,
            db,
            Repositories(db),
            destination=Path(args.out) if args.out else None,
            include_raw=args.include_raw,
            limit=args.limit,
        )
    finally:
        db.close()
    print(json.dumps(manifest, indent=2, default=str))
    print(f"\nExported to {manifest['path']}")
    return 0


# ------------------------------------------------------------------ config


def command_config(args: argparse.Namespace) -> int:
    from aurum.config import settable_report

    config = load(args)
    print(
        json.dumps(
            {
                "version": config.app.version,
                "config": config.public_dict(),
                "settable": settable_report(config),
            },
            indent=2,
            default=str,
        )
    )
    return 0


# --------------------------------------------------------------------- cli


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", type=Path, default=None, help="path to config.yaml")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--log-level", default=None, choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="start the engine and the API")
    run.add_argument("--host", default=None)
    run.add_argument("--port", type=int, default=None)
    run.add_argument("--feed", choices=["live", "replay"], default=None)
    run.add_argument("--replay", type=Path, default=None, help="replay file (implies --feed replay)")
    run.add_argument("--no-research", action="store_true", help="run the feed without research cycles")
    run.set_defaults(func=command_run)

    diagnose = subparsers.add_parser("diagnose", help="one-shot health and configuration report")
    diagnose.add_argument("--feed", choices=["live", "replay"], default=None)
    diagnose.add_argument("--replay", type=Path, default=None)
    diagnose.set_defaults(func=command_diagnose)

    selftest = subparsers.add_parser("selftest", help="verify every subsystem")
    selftest.add_argument("--json", action="store_true")
    selftest.set_defaults(func=command_selftest)

    export = subparsers.add_parser("export", help="snapshot without stopping the engine")
    export.add_argument("--out", default=None, help="destination directory")
    export.add_argument("--include-raw", action="store_true", help="include raw market events")
    export.add_argument("--limit", type=int, default=5000)
    export.set_defaults(func=command_export)

    config_cmd = subparsers.add_parser("config", help="print the effective configuration")
    config_cmd.set_defaults(func=command_config)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
