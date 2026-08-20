"""Command line: run, selftest, test-bybit, diagnose, reconcile, db, learn, stats."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

from . import __version__
from .api.server import ApiServer
from .config import Config, load_config, load_dotenv
from .engine import Engine
from .scan.bybit_rest import BybitError, BybitRest, BybitTransportError
from .scan.bybit_ws import WsConnection
from .storage.db import Database, SchemaError
from .storage.repo import Repo
from .util.clock import Clock
from .util.logging_setup import get_logger, setup_logging

log = get_logger("cli")

GREEN, RED, YELLOW, DIM, BOLD, RESET = (
    "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[1m", "\033[0m"
)


def _tick(ok: bool) -> str:
    return f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"


def _banner(cfg: Config) -> None:
    mode_colour = RED if cfg.is_live else GREEN
    print(f"{BOLD}AURUM EDGE {__version__}{RESET}  "
          f"mode={mode_colour}{cfg.mode.upper()}{RESET}  "
          f"venue=Bybit V5 ({'testnet' if cfg.bybit.testnet else 'mainnet'})")
    print(f"{DIM}database: {cfg.db_path}{RESET}")


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

async def cmd_run(cfg: Config, args: argparse.Namespace) -> int:
    _banner(cfg)
    if cfg.is_live:
        if not cfg.bybit.has_credentials:
            print(f"{RED}LIVE mode needs BYBIT_API_KEY and BYBIT_API_SECRET{RESET}")
            return 2
        if not args.i_understand_live:
            print(
                f"{RED}LIVE mode must be enabled explicitly.{RESET}\n"
                f"Re-run with --i-understand-live to trade real money.\n"
                f"Kill switch: POST /api/control/kill, or create the file named by "
                f"AURUM_KILL_SWITCH_FILE."
            )
            return 2

    if args.simulate:
        if cfg.is_live:
            print(f"{RED}--simulate cannot be combined with LIVE mode.{RESET}")
            return 2
        print(
            f"\n{YELLOW}{'=' * 74}{RESET}\n"
            f"{YELLOW}  SIMULATED FEED - this is NOT Bybit data.{RESET}\n"
            f"  Every snapshot is tagged source=fake, the dashboard shows FEED: FAKE,\n"
            f"  and LIVE mode will refuse to start on it.  Use it to exercise the\n"
            f"  stack, never to judge an edge.\n"
            f"{YELLOW}{'=' * 74}{RESET}\n"
        )

    from .simulator import SimulatedMarketCore

    clock = Clock()
    engine = (
        Engine(cfg, clock, market=SimulatedMarketCore(cfg, clock, symbols=args.symbols))
        if args.simulate
        else Engine(cfg)
    )
    api = ApiServer(cfg, engine)
    stop = asyncio.Event()

    def _request_stop(*_: Any) -> None:
        print("\nstopping...")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:  # pragma: no cover - windows
            signal.signal(sig, _request_stop)

    try:
        try:
            await engine.start()
        except (BybitTransportError, BybitError) as exc:
            # Refusing to start is correct: the alternative is trading on data
            # that is not the exchange's.
            print(f"\n{RED}cannot start: Bybit is not reachable.{RESET}\n  {exc}\n")
            print(f"{DIM}Run 'python -m aurum_edge test-bybit' for a full connectivity "
                  f"report, or 'python -m aurum_edge run --simulate' to exercise the "
                  f"stack on a clearly-labelled synthetic feed.{RESET}")
            await engine.stop(flatten=False)
            return 1
        await api.start()
        print(f"{GREEN}running{RESET} - dashboard API on port {api.bound_port}, "
              f"run_id={engine.db.run_id}")
        print(f"{DIM}dashboard: cd ../frontend && python3 serve.py "
              f"--api http://127.0.0.1:{api.bound_port}{RESET}")
        await stop.wait()
    finally:
        await api.stop()
        await engine.stop()
    return 0


# ---------------------------------------------------------------------------
# selftest - full pipeline, offline, deterministic
# ---------------------------------------------------------------------------

async def cmd_selftest(cfg: Config, args: argparse.Namespace) -> int:
    """Prove the whole pipeline end to end without touching the network."""
    from .execute.broker import PaperBroker
    from .simulator import SimulatedMarketCore

    duration = args.seconds
    db_path = args.db or str(
        Path(os.environ.get("TMPDIR", "/tmp")) / f"aurum_selftest_{int(time.time())}.sqlite3"
    )
    cfg = _replace(cfg, mode="paper", db_path=db_path)
    print(f"{BOLD}AURUM EDGE selftest{RESET}  ({duration}s, simulated feed, PAPER)")
    print(f"{DIM}database: {db_path}{RESET}\n")

    clock = Clock()
    market = SimulatedMarketCore(cfg, clock, symbols=args.symbols, seed=args.seed)
    engine = Engine(
        cfg, clock, market=market, broker=PaperBroker(cfg, clock), db=Database(db_path)
    )
    checks: list[tuple[str, bool, str]] = []
    try:
        await engine.start()
        deadline = time.monotonic() + duration
        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)

        state = engine.state()
        repo = engine.repo

        snapshots = engine.last_snapshots
        checks.append((
            "market core produces synchronised snapshots",
            len(snapshots) >= args.symbols,
            f"{len(snapshots)} symbols with a live snapshot",
        ))
        books_ok = sum(1 for s in snapshots.values() if s.book_state == "OK")
        checks.append((
            "order books are in sync",
            books_ok >= args.symbols,
            f"{books_ok}/{len(snapshots)} books OK",
        ))
        fresh = [s for s in snapshots.values() if s.book_age_ms < 2_000]
        checks.append((
            "data is live, not stale",
            len(fresh) >= args.symbols,
            f"{len(fresh)} books updated within 2s",
        ))
        scan = engine.scanner.last_result
        checks.append((
            "scanner ranks opportunities",
            bool(scan and scan.considered > 0),
            f"considered {scan.considered if scan else 0}, shortlisted "
            f"{len(scan.ranked) if scan else 0}",
        ))
        decisions = repo.recent_decisions(limit=500)
        checks.append((
            "decisions are recorded",
            len(decisions) > 0,
            f"{len(decisions)} decisions stored",
        ))
        no_trade = [d for d in decisions if d["action"] == "NO_TRADE"]
        explained = [d for d in no_trade if d["reason"]]
        checks.append((
            "every NO TRADE has a reason",
            len(no_trade) == len(explained),
            f"{len(explained)}/{len(no_trade)} explained",
        ))
        trades = repo.trades(limit=500)
        checks.append((
            "PAPER opens and closes positions",
            len(trades) >= args.min_trades,
            f"{len(trades)} closed trades, {engine.execution.trades_opened} opened",
        ))
        worst = 0.0
        for trade in trades:
            residual = abs(
                trade["gross_pnl_eur"] - trade["slippage_eur"] - trade["fees_eur"]
                - trade["net_pnl_eur"]
            )
            worst = max(worst, residual)
        checks.append((
            "costs and P&L reconcile exactly",
            worst < 1e-9,
            f"largest residual {worst:.2e} EUR (net = gross - slippage - fees)",
        ))
        executions = repo.db.query("SELECT COUNT(*) AS n FROM executions")[0]["n"]
        orders = repo.db.query("SELECT COUNT(*) AS n FROM orders")[0]["n"]
        checks.append((
            "orders and executions are stored separately",
            executions > 0 and orders > 0,
            f"{orders} orders, {executions} executions (an ACK is not a fill)",
        ))
        checks.append((
            "database wrote every table without failures",
            repo.db.write_failures == 0 and repo.db.pending_write_failures() == 0,
            f"write failures: {repo.db.write_failures}",
        ))
        diagnosis = engine.diagnose()
        checks.append((
            "diagnose always explains the current state",
            bool(diagnosis["gate_reasons"] or diagnosis["top_candidates"] or
                 diagnosis["scan_skipped"] or diagnosis["reject_summary"]),
            f"trading_allowed={diagnosis['trading_allowed']}, "
            f"{len(diagnosis['top_candidates'])} candidates explained",
        ))
        checks.append((
            "state payload is complete and serialisable",
            _state_complete(state),
            f"{len(json.dumps(state, default=str))} bytes",
        ))
        checks.append((
            "the simulated feed is labelled as simulated",
            state["feed_source"] == "fake" and not state["feed_is_real"],
            f"feed_source={state['feed_source']}",
        ))
        labelled = repo.labelled_decision_count()
        checks.append((
            "decisions get outcome labels for learning",
            labelled > 0,
            f"{labelled} decisions labelled with what the market did next",
        ))
    finally:
        await engine.stop(flatten=True)

    print()
    ok = True
    for name, passed, detail in checks:
        ok = ok and passed
        print(f"  [{_tick(passed)}] {name:<48} {DIM}{detail}{RESET}")
    stats = Repo(Database(db_path, run_id=engine.db.run_id).open()).stats()
    print(
        f"\n{BOLD}result{RESET}: {stats.trades} trades, net {stats.net_pnl_eur:+.3f} EUR, "
        f"fees {stats.fees_eur:.3f}, slippage {stats.slippage_eur:.3f}, "
        f"WR {stats.win_rate:.1%}, expectancy {stats.expectancy_eur:+.3f} EUR/trade"
    )
    print(f"{DIM}(simulated market - these numbers prove the plumbing, not an edge){RESET}")
    print(f"\n{_tick(ok)} selftest {'passed' if ok else 'FAILED'}")
    return 0 if ok else 1


def _state_complete(state: dict[str, Any]) -> bool:
    required = (
        "mode", "feed_source", "health", "market", "account", "positions", "trades",
        "stats", "opportunities", "no_trade_reasons", "model", "risk", "execution",
    )
    if not all(key in state for key in required):
        return False
    stats = state["stats"]
    return all(
        key in stats
        for key in (
            "win_rate", "avg_win_eur", "avg_loss_eur", "expectancy_eur", "gross_pnl_eur",
            "fees_eur", "slippage_eur", "net_pnl_eur", "trades_per_hour",
        )
    )


# ---------------------------------------------------------------------------
# test-bybit - the live connectivity proof
# ---------------------------------------------------------------------------

async def cmd_test_bybit(cfg: Config, args: argparse.Namespace) -> int:
    """Talk to the real Bybit and report exactly what worked."""
    _banner(cfg)
    print(f"\n{BOLD}Bybit V5 connectivity{RESET}\n")
    results: list[tuple[str, bool, str]] = []
    rest = BybitRest(cfg.bybit)
    universe: list[str] = []

    # 1. REST reachability and clock skew
    try:
        started = time.perf_counter()
        server = await rest.server_time()
        elapsed = (time.perf_counter() - started) * 1000.0
        server_ms = float(server.get("timeNano", 0)) / 1e6 or float(server["timeSecond"]) * 1000
        skew = abs(time.time() * 1000.0 - server_ms)
        results.append((
            "REST /v5/market/time",
            skew < 5_000,
            f"round trip {elapsed:.0f}ms, clock skew {skew:.0f}ms",
        ))
    except (BybitError, BybitTransportError) as exc:
        results.append(("REST /v5/market/time", False, str(exc)))
        _print_results(results)
        print(f"\n{RED}Bybit is not reachable from this machine.{RESET}")
        print(f"{DIM}Check outbound HTTPS to {cfg.bybit.rest_base} "
              f"(firewall, proxy, or an egress policy).{RESET}")
        await rest.close()
        return 1

    # 2. instruments + tickers
    try:
        instruments = await rest.instruments()
        tickers = await rest.tickers()
        perps = [
            i for i in instruments
            if i.get("quoteCoin") == "USDT"
            and i.get("contractType") == "LinearPerpetual"
            and i.get("status") == "Trading"
        ]
        results.append((
            "REST instruments-info + tickers",
            len(perps) > 50 and len(tickers) > 50,
            f"{len(perps)} tradable USDT perpetuals, {len(tickers)} tickers",
        ))
        by_turnover = sorted(
            tickers, key=lambda t: float(t.get("turnover24h", 0) or 0), reverse=True
        )
        universe = [
            t["symbol"] for t in by_turnover
            if t["symbol"] in {i["symbol"] for i in perps}
        ][:5]
        results.append((
            "liquidity filter selects a universe",
            len(universe) >= 3,
            f"top by turnover: {', '.join(universe)}",
        ))
    except (BybitError, BybitTransportError) as exc:
        results.append(("REST instruments-info + tickers", False, str(exc)))

    # 3. public websocket: book, trades, tickers
    if universe:
        received: dict[str, int] = {"orderbook": 0, "publicTrade": 0, "tickers": 0}
        first_book: dict[str, Any] = {}
        latencies: list[float] = []

        def on_message(msg: dict[str, Any]) -> None:
            topic = msg.get("topic", "")
            head = topic.split(".")[0]
            if head in received:
                received[head] += 1
            if head == "orderbook" and not first_book:
                first_book.update(msg)
            ts = float(msg.get("ts", 0) or 0)
            if ts:
                latencies.append(time.time() * 1000.0 - ts)

        conn = WsConnection(
            name="test-public",
            url=cfg.bybit.ws_public,
            on_message=on_message,
            ping_interval_s=20.0,
            stale_feed_ms=15_000.0,
        )
        topics = [f"orderbook.50.{s}" for s in universe[:3]]
        topics += [f"publicTrade.{s}" for s in universe[:3]]
        topics += [f"tickers.{s}" for s in universe[:3]]
        await conn.start(topics)
        live = await conn.wait_live(20.0)
        await asyncio.sleep(args.seconds)
        health = conn.health()
        await conn.stop()

        results.append((
            "public websocket connects and subscribes",
            live and health["messages"] > 0,
            f"{health['messages']} messages on {health['topics']} topics",
        ))
        results.append((
            "order book stream is flowing",
            received["orderbook"] > 5,
            f"{received['orderbook']} book messages, first type="
            f"{first_book.get('type', 'n/a')}",
        ))
        results.append((
            "public trades are flowing",
            received["publicTrade"] > 0,
            f"{received['publicTrade']} trade messages",
        ))
        results.append((
            "tickers (open interest, funding) are flowing",
            received["tickers"] > 0,
            f"{received['tickers']} ticker messages",
        ))
        if latencies:
            latencies.sort()
            p50 = latencies[len(latencies) // 2]
            p95 = latencies[int(len(latencies) * 0.95)]
            results.append((
                "feed latency is workable",
                p95 < cfg.scan.max_latency_ms,
                f"p50 {p50:.0f}ms, p95 {p95:.0f}ms (limit {cfg.scan.max_latency_ms:.0f}ms)",
            ))

    # 4. private side
    if cfg.bybit.has_credentials:
        try:
            wallet = await rest.wallet_balance()
            equity = wallet.get("totalEquity", "?")
            results.append((
                "REST private: wallet balance",
                bool(wallet),
                f"account {cfg.bybit.account_type}, total equity {equity} USDT",
            ))
        except (BybitError, BybitTransportError) as exc:
            results.append(("REST private: wallet balance", False, str(exc)))
        try:
            positions = await rest.positions()
            orders = await rest.open_orders()
            results.append((
                "REST private: positions and open orders",
                True,
                f"{len(positions)} positions, {len(orders)} open orders",
            ))
        except (BybitError, BybitTransportError) as exc:
            results.append(("REST private: positions and open orders", False, str(exc)))

        authed = {"ok": False}

        def on_private(msg: dict[str, Any]) -> None:
            authed["ok"] = True

        private = WsConnection(
            name="test-private",
            url=cfg.bybit.ws_private,
            on_message=on_private,
            api_key=cfg.bybit.api_key,
            api_secret=cfg.bybit.api_secret,
            private=True,
            stale_feed_ms=float("inf"),
        )
        await private.start(["wallet", "order", "execution", "position"])
        live = await private.wait_live(20.0)
        health = private.health()
        await private.stop()
        results.append((
            "private websocket authenticates and subscribes",
            live,
            f"state={health['state']}, topics={health['topics']}"
            + (f", error={health['last_error']}" if health["last_error"] else ""),
        ))
    else:
        results.append((
            "private stream (skipped)",
            True,
            "no API key configured - public data only, PAPER mode is fine",
        ))

    await rest.close()
    _print_results(results)
    ok = all(passed for _, passed, _ in results)
    print(f"\n{_tick(ok)} Bybit connectivity {'verified' if ok else 'INCOMPLETE'}")
    return 0 if ok else 1


def _print_results(results: list[tuple[str, bool, str]]) -> None:
    for name, passed, detail in results:
        print(f"  [{_tick(passed)}] {name:<46} {DIM}{detail}{RESET}")


# ---------------------------------------------------------------------------
# diagnose / reconcile / db / learn / stats
# ---------------------------------------------------------------------------

async def cmd_diagnose(cfg: Config, args: argparse.Namespace) -> int:
    """Ask the running backend why it is not trading."""
    import aiohttp

    url = args.url or f"http://127.0.0.1:{cfg.api.port}"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(f"{url}/api/diagnose") as resp:
                payload = await resp.json()
    except Exception as exc:  # noqa: BLE001
        print(f"{RED}no running backend at {url}{RESET} ({exc})")
        print(f"{DIM}start one with: python -m aurum_edge run{RESET}")
        return 1

    print(f"{BOLD}AURUM EDGE diagnose{RESET}  mode={payload['mode']} "
          f"feed={payload['feed_source']} state={payload['system_state']}")
    allowed = payload["trading_allowed"]
    print(f"\ntrading allowed: {GREEN if allowed else RED}{allowed}{RESET}")
    for reason in payload["gate_reasons"]:
        print(f"  {RED}-{RESET} {reason}")
    for reason in payload["portfolio_blocks"]:
        print(f"  {YELLOW}-{RESET} {reason}")
    print(f"\nuniverse {payload['symbols_in_universe']} symbols, "
          f"{payload['symbols_with_snapshot']} with snapshots, "
          f"{payload['tradable_symbols']} tradable, "
          f"{payload['shortlisted']} shortlisted")
    if payload["scan_skipped"]:
        print(f"\n{BOLD}why symbols were skipped{RESET}")
        for reason, count in payload["scan_skipped"].items():
            print(f"  {count:>6}  {reason}")
    if payload["reject_summary"]:
        print(f"\n{BOLD}why candidates were rejected (last 15 min){RESET}")
        for row in payload["reject_summary"]:
            print(f"  {row['count']:>6}  {row['reason']}")
    if payload["top_candidates"]:
        print(f"\n{BOLD}best candidates right now{RESET}")
        for candidate in payload["top_candidates"]:
            colour = GREEN if candidate["action"] != "NO_TRADE" else DIM
            print(f"  {colour}{candidate['symbol']:<14} {candidate['side']:<5} "
                  f"{candidate['action']:<9}{RESET} p={candidate['probability']:.3f} "
                  f"q={candidate['quality']:.3f} "
                  f"move {candidate['expected_move_bps']:.1f}bps vs "
                  f"{candidate['required_move_bps']:.1f}bps needed, "
                  f"E={candidate['expectancy_eur']:+.3f} EUR")
            for reason in candidate["reasons"][:3]:
                print(f"       {DIM}{reason}{RESET}")
    print(f"\n{BOLD}components{RESET}")
    for component in payload["components"]:
        colour = {"OK": GREEN, "DEGRADED": YELLOW, "DOWN": RED}[component["state"]]
        print(f"  {colour}{component['state']:<9}{RESET} {component['name']:<14} "
              f"{DIM}{component['detail']}{RESET}")
    return 0


async def cmd_reconcile(cfg: Config, args: argparse.Namespace) -> int:
    """One-shot reconciliation against Bybit (or the paper venue)."""
    _banner(cfg)
    from .execute.broker import PaperBroker

    clock = Clock()
    db = Database(cfg.db_path).open()
    repo = Repo(db)
    rest = BybitRest(cfg.bybit)
    from .decide.risk import RiskEngine
    from .execute.execution_core import ExecutionCore
    from .execute.reconcile import Reconciler

    if cfg.is_live:
        from .execute.bybit_broker import BybitBroker

        broker = BybitBroker(cfg, clock, rest)
    else:
        broker = PaperBroker(cfg, clock)
    execution = ExecutionCore(cfg, clock, broker, repo, RiskEngine(cfg))
    reconciler = Reconciler(cfg, clock, execution, repo, rest=rest if cfg.is_live else None)
    result = await reconciler.reconcile("cli")
    print(json.dumps(result.to_dict(), indent=2, default=str))
    await rest.close()
    db.close()
    return 0 if result.ok else 1


def cmd_db(cfg: Config, args: argparse.Namespace) -> int:
    print(f"{BOLD}database{RESET} {cfg.db_path}")
    try:
        db = Database(cfg.db_path).open()
    except SchemaError as exc:
        print(f"{RED}{exc}{RESET}")
        return 1
    print(f"  schema version : {db.schema_version()}")
    print(f"  migrations     : {sorted(db.applied_versions())}")
    print(f"  write failures : {db.pending_write_failures()}")
    for table in (
        "runs", "snapshots", "decisions", "orders", "executions", "positions",
        "trades", "equity", "model_versions", "health_events", "reconciliations",
    ):
        count = db.query_one(f"SELECT COUNT(*) AS n FROM {table}")
        print(f"  {table:<15}: {count['n']}")
    runs = db.query("SELECT run_id, mode, feed_source, started_at FROM runs ORDER BY started_at DESC LIMIT 5")
    if runs:
        print(f"\n{BOLD}recent runs{RESET}")
        for row in runs:
            when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row["started_at"]))
            print(f"  {when}  {row['mode']:<6} {row['feed_source']:<7} {row['run_id']}")
    db.close()
    return 0


def cmd_stats(cfg: Config, args: argparse.Namespace) -> int:
    db = Database(cfg.db_path).open()
    repo = Repo(db)
    rows = repo.trades(run_id="*", limit=100_000)
    from .storage.repo import compute_stats

    stats = compute_stats(rows)
    print(f"{BOLD}AURUM EDGE - all runs{RESET}  ({cfg.db_path})")
    for key, value in stats.to_dict().items():
        print(f"  {key:<20}: {value}")
    by_symbol = repo.symbol_stats()[:15]
    if by_symbol:
        print(f"\n{BOLD}by symbol{RESET}")
        for row in by_symbol:
            wr = row["wins"] / row["trades"] if row["trades"] else 0.0
            print(f"  {row['symbol']:<14} {row['trades']:>4} trades  "
                  f"WR {wr:>6.1%}  net {row['net_eur']:+8.3f} EUR  "
                  f"slip {row['slippage_bps_mean']:.2f}bps")
    db.close()
    return 0


async def cmd_learn(cfg: Config, args: argparse.Namespace) -> int:
    from .decide.model import Model, champion_v1
    from .learn.pipeline import LearningPipeline

    db = Database(cfg.db_path).open()
    repo = Repo(db)
    row = repo.champion()
    champion = Model.from_row(row) if row else champion_v1()
    pipeline = LearningPipeline(cfg, repo)
    report = pipeline.run(champion)
    print(json.dumps(report.to_dict(), indent=2, default=str))
    promoted, detail = pipeline.maybe_promote(champion)
    print(f"\npromotion: {detail}")
    db.close()
    return 0 if report.status in ("shadow", "rejected", "skipped") else 1


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

def _replace(cfg: Config, **changes: Any) -> Config:
    from dataclasses import replace

    return replace(cfg, **changes)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aurum-edge",
        description="AURUM EDGE 1.0 - Bybit V5 trading system (SCAN / DECIDE / EXECUTE+LEARN)",
    )
    parser.add_argument("--env", default=".env", help="path to a .env file")
    parser.add_argument("--log-level", default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run the trading core and the API")
    run.add_argument("--i-understand-live", action="store_true",
                     help="required to start in LIVE mode")
    run.add_argument("--simulate", action="store_true",
                     help="run on a synthetic feed labelled source=fake (PAPER only)")
    run.add_argument("--symbols", type=int, default=12,
                     help="number of synthetic symbols when --simulate is used")

    selftest = sub.add_parser(
        "selftest", help="prove the full pipeline offline on a simulated feed"
    )
    selftest.add_argument("--seconds", type=float, default=75.0)
    selftest.add_argument("--symbols", type=int, default=12)
    selftest.add_argument("--seed", type=int, default=7)
    selftest.add_argument("--min-trades", type=int, default=1)
    selftest.add_argument("--db", default=None)

    test_bybit = sub.add_parser("test-bybit", help="verify the real Bybit connection")
    test_bybit.add_argument("--seconds", type=float, default=8.0,
                            help="how long to observe the public stream")

    diagnose = sub.add_parser("diagnose", help="explain why the backend is not trading")
    diagnose.add_argument("--url", default=None)

    sub.add_parser("reconcile", help="reconcile local state with Bybit")
    sub.add_parser("db", help="inspect the canonical database")
    sub.add_parser("stats", help="performance across every run")
    sub.add_parser("learn", help="run the champion/challenger pipeline once")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    load_dotenv(args.env)
    cfg = load_config()
    setup_logging(args.log_level or cfg.log_level, cfg.log_json)

    handlers = {
        "run": cmd_run,
        "selftest": cmd_selftest,
        "test-bybit": cmd_test_bybit,
        "diagnose": cmd_diagnose,
        "reconcile": cmd_reconcile,
        "learn": cmd_learn,
    }
    sync_handlers = {"db": cmd_db, "stats": cmd_stats}

    try:
        if args.command in sync_handlers:
            return sync_handlers[args.command](cfg, args)
        return asyncio.run(handlers[args.command](cfg, args))
    except KeyboardInterrupt:
        return 130
    except SchemaError as exc:
        print(f"{RED}{exc}{RESET}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
