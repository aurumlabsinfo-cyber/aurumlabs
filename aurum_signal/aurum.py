#!/usr/bin/env python3
"""AURUM SIGNAL ENGINE M60 — riga di comando.

    python3 aurum.py selftest      verifica il sistema su se stesso, senza rete
    python3 aurum.py run           motore live + dashboard
    python3 aurum.py diagnose      perche' non arrivano segnali
    python3 aurum.py status        stato in una schermata
    python3 aurum.py replay        riproduce dati registrati come se fossero live
    python3 aurum.py backtest      simula il ciclo completo su dati registrati
    python3 aurum.py research      un giro di ricerca su cio' che ha raccolto
    python3 aurum.py export        esporta i dati in CSV/JSON

Il sistema **non esegue operazioni**: analizza, prevede, notifica e studia.
L'operazione la esegue una persona.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
from pathlib import Path

# Deve funzionare in tre modi: `python3 aurum.py` dentro la cartella,
# `python3 -m aurum_signal.aurum` da fuori, e `from aurum_signal import aurum`.
# Lanciato come script il pacchetto non e' definito e gli import relativi
# fallirebbero: qui lo si definisce, cosi' il resto del file usa un solo stile.
if __package__ in (None, ""):  # pragma: no cover - percorso a script
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    import aurum_signal  # noqa: F401 - registra il pacchetto
    __package__ = "aurum_signal"

from .config import APP_NAME, MODE_SIMULATION, VERSION, Config
from .core.engine import AurumEngine
from .market.fallback import MarketFeed
from .market.replay import ReplayAdapter
from .market.simulation import SimulationAdapter
from .storage.database import Database


def _banner(cfg: Config, mode: str) -> None:
    print(f"\n{APP_NAME} {VERSION}")
    print(f"  strumento     : {cfg.symbol}")
    print(f"  scadenza      : {cfg.horizon_seconds}s")
    print(f"  anticipo      : >= {cfg.signal_lead_seconds}s "
          f"(congelato negli ultimi {cfg.signal_freeze_seconds}s)")
    print(f"  payout        : {cfg.payout:.0%}  ->  pareggio "
          f"{cfg.breakeven_win_rate:.2%}")
    print(f"  soglia utile  : {cfg.effective_min_probability:.2%} "
          f"(pareggio + margine {cfg.min_edge:.0%})")
    print(f"  portafoglio   : {cfg.virtual_capital:.0f} virtuali, "
          f"{cfg.virtual_stake:.0f} a operazione")
    print(f"  modalita'     : {mode}")
    print(f"  ORDINI        : NESSUNO. Il sistema notifica, tu esegui.\n")


def _build_engine(cfg: Config, args) -> AurumEngine:
    """Sceglie la sorgente in base al comando, senza toccare il motore."""
    if getattr(args, "simulate", False):
        adapter = SimulationAdapter(cfg, seed=getattr(args, "seed", None))
        return AurumEngine(cfg, MarketFeed(cfg, adapter))
    if getattr(args, "replay_source", None):
        adapter = ReplayAdapter(cfg, args.replay_source,
                                speed=getattr(args, "speed", 50.0))
        return AurumEngine(cfg, MarketFeed(cfg, adapter))
    return AurumEngine(cfg)


# --------------------------------------------------------------------------- #
#  COMANDI
# --------------------------------------------------------------------------- #

def cmd_run(cfg: Config, args) -> int:
    engine = _build_engine(cfg, args)
    _banner(cfg, engine.feed.adapter.mode if engine.feed.adapter else "LIVE")

    server = None
    if cfg.http_port:
        from .dashboard.server import (DashboardServer,  # import tardivo
                                       transport_state)
        server = DashboardServer(cfg, engine)
        server.start()
        print(f"  dashboard     : http://{cfg.http_host}:{server.port}")
        t = transport_state()
        if not t["realtime"]:
            print(f"  aggiornamenti : a interrogazione — {t['reason']}")
            print(f"                  per il tempo reale: {t['fix']}")
        print()

    try:
        asyncio.run(engine.run(max_seconds=getattr(args, "seconds", None)))
    except KeyboardInterrupt:
        print("\ninterrotto.")
    except RuntimeError as exc:
        print(f"\nERRORE: {exc}\n")
        print("Suggerimenti:")
        print("  * senza chiavi API prova:  python3 aurum.py run --simulate")
        print("  * per usare dati veri metti TWELVEDATA_API_KEY o "
              "FINNHUB_API_KEY in .env")
        return 1
    finally:
        engine.stop()
        if server:
            server.stop()
        engine.db.stop()
    return 0


def cmd_status(cfg: Config, args) -> int:
    db = Database(cfg.database_path)
    counts = db.counts()
    print(f"\n{APP_NAME} {VERSION} — stato del database\n")
    print(f"  file: {os.path.abspath(cfg.database_path)}")
    for table, n in counts.items():
        if n > 0:
            print(f"    {table:22s} {n:>10,}")
    rows = db.query("SELECT result, COUNT(*) n FROM signals "
                    "WHERE result IS NOT NULL GROUP BY result")
    if rows:
        print("\n  esiti registrati:")
        for r in rows:
            print(f"    {r['result']:10s} {r['n']:>6}")
    cycles = db.query("SELECT * FROM wallet_cycles ORDER BY cycle_id DESC LIMIT 5")
    if cycles:
        print("\n  ultimi cicli:")
        for c in cycles:
            end = c["ending_balance"]
            print(f"    #{c['cycle_id']:<3} {c['trades'] or 0:>4} operazioni  "
                  f"saldo {end if end is not None else '—'}  "
                  f"{c['reason_closed'] or 'in corso'}")
    db.stop()
    return 0


def cmd_diagnose(cfg: Config, args) -> int:
    """Perche' non arrivano segnali, in italiano."""
    import urllib.request
    url = f"http://{cfg.http_host}:{cfg.http_port}/diagnostics"
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            d = json.loads(r.read())
    except Exception as exc:  # noqa: BLE001 - e' un diagnostico
        print(f"Non riesco a parlare con il motore su {url}")
        print(f"  {type(exc).__name__}: {exc}\n")
        print("  Il motore e' in esecuzione in un altro terminale?")
        print("  Se usi una porta diversa: --http-port 8123")
        return 1

    print(f"\n{APP_NAME} — diagnosi\n")
    feed = d.get("feed", {})
    q = feed.get("quality", {})
    print(f"  modalita'         : {d.get('mode')}")
    print(f"  sorgente          : {(feed.get('adapter') or {}).get('name')} "
          f"({'connessa' if (feed.get('adapter') or {}).get('connected') else 'NON CONNESSA'})")
    print(f"  eta' ultimo tick  : {d.get('last_tick_age_ms')} ms")
    print(f"  qualita' dati     : {q.get('quality_score')} "
          f"(latenza {q.get('feed_latency_ms')} ms)")
    if q.get("blocking"):
        print(f"  BLOCCHI DURI      : {', '.join(q['blocking'])}")
    for note in q.get("notes", []):
        print(f"                      nota: {note}")
    if d.get("safe_mode"):
        print(f"  MODALITA' SICURA  : {d.get('safe_reason')}")

    dec = d.get("decisions", {})
    print(f"\n  decisioni valutate: {dec.get('decisions')}")
    print(f"  segnali emessi    : {dec.get('emitted')} "
          f"({(dec.get('emission_rate') or 0):.2%})")
    print(f"  pareggio richiesto: {dec.get('breakeven_win_rate')}")
    print(f"  soglia effettiva  : {dec.get('effective_min_probability')}")
    print("\n  Cosa blocca, in ordine:")
    for code, n in (d.get("top_blockers") or [])[:8]:
        share = n / max(1, dec.get("decisions", 1))
        print(f"    {share:6.1%}  {code}  ({n}x)")

    cal = d.get("calibration", {})
    if cal.get("samples"):
        print(f"\n  calibrazione: dichiarato {cal.get('stated_mean')}, "
              f"realizzato {cal.get('realised_win_rate')} "
              f"(scarto {cal.get('gap')}) su {cal['samples']} esiti")
    else:
        print(f"\n  calibrazione: nessun esito ancora "
              f"(confidenza ristretta a {cal.get('shrink')})")

    lat = d.get("latency", {})
    print(f"\n  latenza pipeline p50: {lat.get('pipeline_p50_ms')} ms "
          f"(limite {lat.get('budget_ms')} ms) "
          f"-> {'ok' if lat.get('within_budget') else 'OLTRE IL LIMITE'}")

    print("\n  ADVICE")
    top = (d.get("top_blockers") or [[None]])[0][0]
    advice = {
        "WARMUP": "Sta ancora raccogliendo storia. Serve qualche minuto.",
        "CONFIDENCE_LOW": ("La confidenza resta sotto la soglia. E' il caso piu' "
                           "comune ed e' spesso corretto: senza calibrazione "
                           "misurata il motore si tiene stretto di proposito."),
        "EDGE_LOW": ("Il vantaggio stimato non supera il pareggio del payout. "
                     "Con payout 0.80 servono piu' del 55,6% di vittorie."),
        "AGENT_DISAGREEMENT": "Gli agenti non concordano: e' informazione, non un guasto.",
        "DATA_STALE": "Il feed non aggiorna. Controlla rete e chiavi API.",
        "MAX_CONCURRENT": "C'e' gia' un segnale aperto. Normale.",
        "VOLATILITY_POOR": ("Il movimento atteso non batte il rumore: su EUR/USD "
                            "a 60 secondi capita spesso, ed e' un fatto del "
                            "mercato, non una soglia da abbassare."),
        "SAFE_MODE": "Modalita' sicura: i dati non sono affidabili.",
    }.get(top, "Nessun cancello dominante: il motore ha appena iniziato.")
    print(f"    {advice}")
    print("\n  Prima di allentare una soglia guarda /blockers: dice se le "
          "decisioni\n  scartate da quel filtro AVREBBERO vinto.")
    return 0


def cmd_replay(cfg: Config, args) -> int:
    if not args.source:
        print("Serve --source: un file .db di AURUM oppure un CSV ts,mid")
        return 2
    args.replay_source = args.source
    return cmd_run(cfg, args)


def cmd_backtest(cfg: Config, args) -> int:
    """Simula il ciclo completo su dati registrati, senza scorciatoie."""
    from .research.validation import WalkForwardValidator
    db = Database(cfg.database_path)
    engine = AurumEngine(cfg, MarketFeed(cfg, SimulationAdapter(cfg)), db)
    ds = engine.build_dataset(include_simulation=args.include_simulation)
    if len(ds) < 100:
        print(f"Dati insufficienti: {len(ds)} righe utilizzabili.")
        print(f"  {ds.notes}")
        db.stop()
        return 1
    from .ml.model import PureLogistic, fit_estimator, predict_proba
    v = WalkForwardValidator(cfg)
    rep = v.run(ds,
                lambda d: fit_estimator(PureLogistic(epochs=16), d.rows, d.y),
                lambda m, d: predict_proba(m, d.rows))
    print(json.dumps(rep, indent=2, ensure_ascii=False))
    db.stop()
    return 0


def cmd_research(cfg: Config, args) -> int:
    from .research.engine import ResearchEngine
    db = Database(cfg.database_path)
    engine = AurumEngine(cfg, MarketFeed(cfg, SimulationAdapter(cfg)), db)
    ds = engine.build_dataset(include_simulation=args.include_simulation)
    print(f"Dataset: {len(ds)} righe, {len(ds.names)} feature. {ds.notes}\n")
    rep = engine.research.run_once(ds)
    print(json.dumps(rep, indent=2, ensure_ascii=False, default=str)[:12000])
    db.stop()
    return 0


def cmd_export(cfg: Config, args) -> int:
    db = Database(cfg.database_path)
    out_dir = Path(args.out or "export")
    out_dir.mkdir(parents=True, exist_ok=True)
    tables = ["signals", "decisions", "wallet_ledger", "wallet_cycles",
              "shadow_decisions", "setups", "research_experiments"]
    for t in tables:
        rows = db.query(f"SELECT * FROM {t}")
        if not rows:
            continue
        path = out_dir / f"{t}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
        print(f"  {path}  ({len(rows)} righe)")
    db.stop()
    return 0


def cmd_selftest(cfg: Config, args) -> int:
    from .tests.selftest import run_selftest
    return run_selftest(cfg, verbose=not args.quiet)


COMMANDS = {
    "run": cmd_run, "status": cmd_status, "diagnose": cmd_diagnose,
    "replay": cmd_replay, "backtest": cmd_backtest, "research": cmd_research,
    "export": cmd_export, "selftest": cmd_selftest,
}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="aurum.py",
        description=f"{APP_NAME} {VERSION} — segnali EUR/USD a 60 secondi, "
                    "esecuzione manuale. Il sistema non invia ordini.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""esempi:
  aurum.py selftest                     verifica tutto, senza rete
  aurum.py run --simulate               prova senza chiavi API (dati SIMULATI)
  aurum.py run                          live, serve una chiave in .env
  aurum.py diagnose                     perche' non arrivano segnali
  aurum.py replay --source aurum_m60.db --speed 100
  aurum.py research                     un giro di ricerca sui dati raccolti
""")
    p.add_argument("command", choices=sorted(COMMANDS))
    p.add_argument("--simulate", action="store_true",
                   help="usa la sorgente SIMULATA (dati generati, non mercato)")
    p.add_argument("--source", help="file per il replay (.db o .csv)")
    p.add_argument("--speed", type=float, default=50.0,
                   help="velocita' del replay (0 = piu' veloce possibile)")
    p.add_argument("--seconds", type=float,
                   help="ferma il motore dopo N secondi")
    p.add_argument("--seed", type=int, help="seme della simulazione")
    p.add_argument("--db", help="percorso del database")
    p.add_argument("--http-port", type=int, help="porta della dashboard (0 = nessuna)")
    p.add_argument("--payout", type=float, help="payout del broker, es. 0.8")
    p.add_argument("--stake", type=float, help="puntata virtuale")
    p.add_argument("--capital", type=float, help="capitale virtuale del ciclo")
    p.add_argument("--lead", type=int, help="anticipo del segnale in secondi")
    p.add_argument("--include-simulation", action="store_true",
                   help="INQUINA la ricerca: include le righe simulate")
    p.add_argument("--out", help="cartella di esportazione")
    p.add_argument("--quiet", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    overrides = {}
    if args.db:
        overrides["database_path"] = args.db
    if args.http_port is not None:
        overrides["http_port"] = args.http_port
    if args.payout is not None:
        overrides["payout"] = args.payout
    if args.stake is not None:
        overrides["virtual_stake"] = args.stake
    if args.capital is not None:
        overrides["virtual_capital"] = args.capital
    if args.lead is not None:
        overrides["signal_lead_seconds"] = args.lead
    try:
        cfg = Config.load(**overrides)
    except ValueError as exc:
        print(f"\n{exc}\n")
        return 2
    return COMMANDS[args.command](cfg, args)


if __name__ == "__main__":
    sys.exit(main())
