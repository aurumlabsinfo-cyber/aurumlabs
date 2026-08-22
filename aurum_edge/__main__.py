"""Riga di comando di AURUM EDGE DISCOVERY.

    python3 -m aurum_edge <comando> [opzioni]

Comandi:

    analizza     PASSAGGIO 1: cerca e giudica lo storico esistente
    backfill     ricostruisce lo storico dagli endpoint pubblici di Bybit
    collect      avvia la raccolta live (resta in esecuzione)
    ricerca      un giro di ricerca, oppure in ciclo con --loop
    previsione   stampa la previsione corrente
    serve        avvia tutto: raccolta, ricerca e dashboard
    stato        cosa c'e' nell'archivio e come sta il sistema
    selftest     prove offline, senza rete
    edges        elenco degli edge e dei loro stati

Nessun comando puo' piazzare un ordine. Non esiste il codice per farlo.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from typing import Any

from . import __version__, config
from .data.store import Store
from .util import timeutil


def _log(msg: str) -> None:
    print(msg, flush=True)


def _store(args: Any) -> Store:
    return Store(getattr(args, "db", None) or config.DB_PATH)


# --------------------------------------------------------------------------
# analizza
# --------------------------------------------------------------------------
def cmd_analizza(args: Any) -> int:
    from .data import legacy

    roots = args.paths or [os.getcwd(), os.path.expanduser("~")]
    roots = [r for r in dict.fromkeys(roots) if os.path.exists(r)]
    result = legacy.scan(roots, config.SYMBOL)
    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(legacy.render(result))
    return 0


# --------------------------------------------------------------------------
# backfill
# --------------------------------------------------------------------------
def cmd_backfill(args: Any) -> int:
    from .data import backfill
    from .util.http import FetchError

    store = _store(args)
    try:
        report = backfill.run(days=args.days, store=store, progress=_log)
    except FetchError as exc:
        _log(f"\nBLOCCATO: {exc.detail}")
        _log("L'host di Bybit non e' raggiungibile da questa rete. Il "
             "programma non tenta strade alternative di proposito: dati di "
             "mercato presi da un'altra fonte non sono gli stessi dati.")
        return 2
    print()
    print(backfill.render(report))
    return 0 if not report.errors else 1


# --------------------------------------------------------------------------
# collect
# --------------------------------------------------------------------------
def cmd_collect(args: Any) -> int:
    from .data.collector import Collector, run_forever

    store = _store(args)
    if args.once:
        collector = Collector(store=store, log=_log)
        try:
            result = collector.poll_once()
        except Exception as exc:                                # noqa: BLE001
            _log(f"ERRORE: {type(exc).__name__}: {exc}")
            return 2
        print(json.dumps(result, indent=2, default=str))
        return 0
    run_forever(store=store, log=_log)
    return 0


# --------------------------------------------------------------------------
# ricerca
# --------------------------------------------------------------------------
def cmd_ricerca(args: Any) -> int:
    from .research.engine import ResearchEngine, run_forever

    store = _store(args)
    if args.loop:
        run_forever(store=store, log=_log)
        return 0

    engine = ResearchEngine(store=store, log=_log)
    report = engine.run(max_candidates=args.max_candidates)
    engine.update_shadow_states()
    data = report.to_dict()
    if args.json:
        print(json.dumps(data, indent=2, default=str))
        return 0

    print()
    print("=" * 72)
    print(f"RICERCA {report.run_id} — {report.status}")
    print("=" * 72)
    ds = data.get("dataset") or {}
    print(f"Righe            : {ds.get('rows')} "
          f"({(ds.get('span') or {}).get('days')} giorni)")
    classes = ds.get("classes") or {}
    print(f"Classi           : {classes.get('shares')}")
    print(f"Fuga di dati     : {(data.get('leakage') or {}).get('status')}")
    power = data.get("power") or {}
    print(f"Potenza          : {power.get('n')} osservazioni indipendenti, "
          f"vantaggio rilevabile {power.get('detectable_lift')}")
    model = data.get("model") or {}
    print(f"Modello          : {model.get('status')}")
    for reason in (model.get("verdict") or {}).get("reasons", [])[:4]:
        print(f"                   - {reason}")
    edges = data.get("edges") or {}
    print(f"Edge             : {edges.get('candidates')} provati, "
          f"{edges.get('passed_thresholds')} oltre le soglie, "
          f"{edges.get('survived_fdr')} dopo la correzione")
    for c in (edges.get("confirmed") or [])[:6]:
        print(f"                   [{c['state']}] {c['label']}")
    if data.get("errors"):
        print("Errori           :")
        for e in data["errors"]:
            print(f"                   {e}")
    print()
    print("CONCLUSIONE")
    print(data.get("conclusion"))
    return 0


# --------------------------------------------------------------------------
# previsione
# --------------------------------------------------------------------------
def cmd_previsione(args: Any) -> int:
    from .forecast.engine import ForecastEngine, score_pending

    store = _store(args)
    score_pending(store)
    forecast = ForecastEngine(store).forecast(persist=not args.dry_run)
    data = forecast.to_dict()
    if args.json:
        print(json.dumps(data, indent=2, default=str))
        return 0

    p = data["probabilities"]
    print()
    print(f"  {data['symbol']} PERPETUAL — orizzonte {data['horizon_min']} minuti")
    print("  " + "=" * 62)
    print()
    print(f"      {data['verdict']}")
    print()
    print(f"  LONG {p['LONG']:.1%}   SHORT {p['SHORT']:.1%}   FLAT {p['FLAT']:.1%}")
    print(f"  Prezzo            : {data['price']:,.2f} "
          f"({(data.get('diagnostics') or {}).get('price_source')})")
    if data["target_low"] is not None:
        print(f"  Target range      : {data['target_low']:,.2f} – "
              f"{data['target_high']:,.2f}")
        print(f"  Movimento atteso  : {data['expected_move_pct']:+.2f}%")
    print(f"  Durata prevista   : {data['expected_duration']}")
    if data["invalidation"] is not None:
        print(f"  Invalidazione     : {data['invalidation']:,.2f}")
    q = data["quality"]
    print(f"  Qualita' segnale  : {q['label']} ({q['score']})")
    print(f"  Regime            : {data['regime'].get('name')}")
    if data["edge"]:
        print(f"  Edge attivo       : {data['edge']['label']}")
    print()
    if data["blockers"]:
        print("  Perche' non c'e' una direzione:")
        for b in data["blockers"]:
            print(f"    - {b}")
        print()
    if q["warnings"]:
        print("  Avvertimenti:")
        for w in q["warnings"]:
            print(f"    - {w}")
        print()
    print("  Motivazioni principali:")
    for r in data["reasons"][:6]:
        print(f"    - {r['text']}  ({r['detail']})")
    print()
    return 0


# --------------------------------------------------------------------------
# serve
# --------------------------------------------------------------------------
def cmd_serve(args: Any) -> int:
    from .data.collector import Collector
    from .forecast.engine import score_pending
    from .research.engine import ResearchEngine
    from .web.server import serve

    store = _store(args)
    collector = None
    if not args.no_collector:
        collector = Collector(store=store, log=_log)
        collector.start()
        _log(f"[serve] collector avviato (passo {config.POLL_SECONDS}s)")
    else:
        _log("[serve] collector disattivato: la dashboard usera' solo "
             "l'archivio")

    stop = threading.Event()

    def research_loop() -> None:
        engine = ResearchEngine(store=Store(store.path), log=_log)
        # Un attimo di respiro all'avvio: la prima previsione deve poter
        # apparire subito, senza aspettare un giro di ricerca completo.
        stop.wait(20)
        while not stop.is_set():
            try:
                engine.run()
                engine.update_shadow_states()
                score_pending(engine.store)
            except Exception as exc:                            # noqa: BLE001
                _log(f"[ricerca] errore: {type(exc).__name__}: {exc}")
            stop.wait(config.RESEARCH_INTERVAL_SECONDS)

    if not args.no_research:
        threading.Thread(target=research_loop, name="aurum-research",
                         daemon=True).start()
        _log(f"[serve] ricerca attiva (un giro ogni "
             f"{config.RESEARCH_INTERVAL_SECONDS}s)")

    try:
        serve(host=args.host, port=args.port, store=store,
              collector=collector, log=_log)
    finally:
        stop.set()
        if collector:
            collector.stop()
    return 0


# --------------------------------------------------------------------------
# stato
# --------------------------------------------------------------------------
def cmd_stato(args: Any) -> int:
    from .research import lifecycle

    store = _store(args)
    coverage = store.coverage()
    snapshot = store.latest_snapshot(config.SYMBOL)
    champion = store.model_by_role("CHAMPION")
    run = store.latest_research_run()
    counts: dict[str, int] = {}
    for row in store.edges():
        counts[row["state"]] = counts.get(row["state"], 0) + 1

    if args.json:
        print(json.dumps({
            "coverage": coverage, "edges": counts,
            "champion": champion["model_id"] if champion else None,
            "last_research": run["run_id"] if run else None,
        }, indent=2, default=str))
        return 0

    print()
    print("=" * 72)
    print(f"AURUM EDGE DISCOVERY {__version__} — stato")
    print("=" * 72)
    print(f"Archivio        : {store.path}")
    print(f"Simbolo         : {coverage['symbol']}")
    print(f"Barre           : {coverage['bars']} "
          f"({coverage['days']} giorni)")
    print(f"Da              : {timeutil.iso(coverage['bars_from'])}")
    print(f"A               : {timeutil.iso(coverage['bars_to'])}")
    print("Tabelle         :")
    for name, count in coverage["tables"].items():
        print(f"                  {name:<20} {count}")
    print(f"Previsioni giudicate: {coverage['forecasts_scored']}")
    if snapshot:
        age = (timeutil.now_ms() - snapshot["ts"]) / 1000.0
        print(f"Ultimo dato live: {timeutil.iso(snapshot['ts'])} "
              f"({age:.0f}s fa)" +
              ("" if age <= config.STALE_SECONDS else "  [NON FRESCO]"))
    else:
        print("Ultimo dato live: mai (il collector non ha mai scritto)")
    print(f"Modello campione: {champion['model_id'] if champion else 'nessuno'}")
    print(f"Ultima ricerca  : "
          f"{timeutil.iso(run['started_ts']) if run else 'mai'} "
          f"({run['status'] if run else '-'})")
    print("Edge per stato  :")
    if counts:
        for state in lifecycle.ORDER:
            if state in counts:
                print(f"                  {state:<18} {counts[state]}")
    else:
        print("                  nessun edge registrato")
    print()
    print(f"Esecuzione ordini: {'ATTIVA' if config.EXECUTION_ENABLED else 'DISATTIVATA (per costruzione)'}")
    print()
    return 0


# --------------------------------------------------------------------------
# edges
# --------------------------------------------------------------------------
def cmd_edges(args: Any) -> int:
    from .research import lifecycle

    store = _store(args)
    rows = store.edges([args.state] if args.state else None)
    if args.json:
        print(json.dumps(
            [lifecycle.EdgeRecord.from_row(r).to_dict() for r in rows],
            indent=2, default=str))
        return 0
    if not rows:
        print("Nessun edge registrato. Eseguire `ricerca`.")
        return 0
    for row in rows:
        record = lifecycle.EdgeRecord.from_row(row)
        wf = (record.metrics or {}).get("walk_forward") or {}
        d = wf.get("directional") or {}
        print(f"[{record.state:<15}] {record.label}")
        print(f"   famiglia={record.family}  "
              f"accuratezza={d.get('accuracy')}  n={d.get('n')}  "
              f"auc={wf.get('auc_directional')}")
        if record.reject_reason:
            print(f"   rifiutato: {record.reject_reason}")
        print()
    return 0


# --------------------------------------------------------------------------
# selftest
# --------------------------------------------------------------------------
def cmd_selftest(args: Any) -> int:
    from .tests.selftest import run_all

    return run_all(verbose=not args.quiet, quick=args.quick)


# --------------------------------------------------------------------------
# parser
# --------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="aurum_edge",
        description=("AURUM EDGE DISCOVERY — previsione manuale a 30 minuti "
                     "su BTCUSDT Perpetual. Nessuna esecuzione di ordini."),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    p.add_argument("--version", action="version", version=__version__)
    p.add_argument("--db", help=f"percorso dell'archivio (default {config.DB_PATH})")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("analizza", help="PASSAGGIO 1: analizza lo storico esistente")
    s.add_argument("paths", nargs="*", help="cartelle da analizzare")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_analizza)

    s = sub.add_parser("backfill", help="ricostruisce lo storico da Bybit")
    s.add_argument("--days", type=int, default=config.BACKFILL_DAYS)
    s.set_defaults(func=cmd_backfill)

    s = sub.add_parser("collect", help="raccolta live dei dati pubblici")
    s.add_argument("--once", action="store_true", help="un solo giro e termina")
    s.set_defaults(func=cmd_collect)

    s = sub.add_parser("ricerca", help="un giro di ricerca")
    s.add_argument("--loop", action="store_true")
    s.add_argument("--max-candidates", type=int, default=None)
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_ricerca)

    s = sub.add_parser("previsione", help="la previsione corrente")
    s.add_argument("--json", action="store_true")
    s.add_argument("--dry-run", action="store_true",
                   help="non salvare la previsione nell'archivio")
    s.set_defaults(func=cmd_previsione)

    s = sub.add_parser("serve", help="raccolta + ricerca + dashboard")
    s.add_argument("--host", default=config.HTTP_HOST)
    s.add_argument("--port", type=int, default=config.HTTP_PORT)
    s.add_argument("--no-collector", action="store_true")
    s.add_argument("--no-research", action="store_true")
    s.set_defaults(func=cmd_serve)

    s = sub.add_parser("stato", help="stato dell'archivio e del sistema")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_stato)

    s = sub.add_parser("edges", help="elenco degli edge")
    s.add_argument("--state", help="filtra per stato")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_edges)

    s = sub.add_parser("selftest", help="prove offline, senza rete")
    s.add_argument("--quiet", action="store_true")
    s.add_argument("--quick", action="store_true",
                   help="salta le prove lente di validazione statistica")
    s.set_defaults(func=cmd_selftest)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\ninterrotto")
        return 130


if __name__ == "__main__":
    sys.exit(main())
