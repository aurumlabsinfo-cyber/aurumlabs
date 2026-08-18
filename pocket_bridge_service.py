#!/usr/bin/env python3
"""Bridge Pocket Option -> AURUM. SOLA LETTURA.

Perche' esiste un processo separato invece di mettere tutto nel motore.

Il motore e' un file solo, senza dipendenze, e non deve mai vedere le tue
credenziali. Questo bridge invece la sessione la vede - e' il suo lavoro - ma
non fa nient'altro: legge saldo, valuta e stato della connessione e li espone
su una singola pagina locale. Tenendoli separati:

  * il motore resta senza dipendenze e senza segreti;
  * se il bridge cade, il motore continua a girare e lo dichiara "dato vecchio";
  * la sessione sta in UN file, con i permessi giusti, e non entra mai in un
    log, in una risposta HTTP o in un database.

-----------------------------------------------------------------------------
NESSUN ORDINE VIENE MAI INVIATO.

In questo file non esiste una chiamata di acquisto, vendita o piazzamento
ordine. Il collegamento e' di sola lettura. Se un giorno vorrai operare per
davvero, quella e' una decisione tua da prendere con un altro programma, non
una riga da aggiungere qui di nascosto.
-----------------------------------------------------------------------------

Uso:

    pip install pocketoptionapi-async
    python3 pocket_bridge_service.py --auth ~/.aurum/pocket.auth

Poi si lancia il motore puntandolo qui:

    python3 aurum_binary_eurusd_m60.py run --payout 0.8 \\
        --pocket-bridge http://127.0.0.1:8010
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import stat
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from pocketoptionapi_async import AsyncPocketOptionClient
except Exception as exc:  # pragma: no cover - dipendenza esterna
    print("Manca la libreria del broker.", file=sys.stderr)
    print("  pip install pocketoptionapi-async", file=sys.stderr)
    print(f"  ({type(exc).__name__}: {exc})", file=sys.stderr)
    raise SystemExit(2)


VERSION = "1.0.0"

#: Stato condiviso fra il thread del broker e il server HTTP. Il server non
#: tocca mai la rete: legge solo questo, quindi non puo' bloccarsi.
STATE: dict[str, object] = {
    "connected": False,
    "balance": None,
    "currency": None,
    "payout": None,
    "asset": None,
    "mode": None,
    "account_type": None,
    "last_error": None,
    "last_fetch_ts": None,
    "reads": 0,
    "orders_sent": 0,          # resta zero: non esiste un percorso d'ordine
    "execution_supported": False,
}
LOCK = threading.Lock()


def now_ms() -> int:
    return int(time.time() * 1000)


def set_state(**fields) -> None:
    with LOCK:
        STATE.update(fields)


def read_auth(path: str) -> tuple[str, bool]:
    """Legge la sessione e ne controlla la FORMA, mai il contenuto.

    Ritorna anche se e' una sessione demo, perche' e' l'unica cosa del token
    che vale la pena mostrare a schermo.
    """
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        raise SystemExit(
            f"File di sessione non trovato: {path}\n"
            "Crealo cosi':\n"
            "  mkdir -p ~/.aurum && chmod 700 ~/.aurum\n"
            "  nano ~/.aurum/pocket.auth      # incolla la stringa 42[\"auth\",...]\n"
            "  chmod 600 ~/.aurum/pocket.auth"
        )
    mode = stat.S_IMODE(os.stat(path).st_mode)
    if mode & 0o077:
        print(f"ATTENZIONE: {path} e' leggibile da altri utenti (permessi "
              f"{oct(mode)}). Correggi con: chmod 600 {path}", file=sys.stderr)
    with open(path, encoding="utf-8") as fh:
        auth = fh.read().strip()
    if not auth:
        raise SystemExit(f"{path} e' vuoto.")
    if not auth.startswith('42["auth"'):
        raise SystemExit(
            f"{path} non ha il formato atteso.\n"
            'Deve cominciare con 42["auth" - e\' la stringa che il browser '
            "invia all'apertura della sessione."
        )
    is_demo = '"isDemo":1' in auth
    return auth, is_demo


async def broker_loop(auth: str, is_demo: bool, asset: str, poll_s: float) -> None:
    """L'unico posto che parla con il broker. Legge, e basta."""
    client = None
    balance_countdown = 0
    while True:
        try:
            if client is None:
                client = AsyncPocketOptionClient(
                    auth,
                    is_demo=is_demo,
                    enable_logging=False,      # la sessione non finisce nei log
                )
                ok = await client.connect()
                if not ok:
                    raise RuntimeError("connect() ha restituito False")
                set_state(connected=True, last_error=None,
                          mode="demo" if is_demo else "live",
                          account_type="demo" if is_demo else "live",
                          asset=asset)

            balance_countdown -= 1
            if balance_countdown <= 0:
                balance_countdown = 4
                result = await client.get_balance()
                balance = getattr(result, "balance", None)
                currency = getattr(result, "currency", None)
                if isinstance(result, dict):
                    balance = balance if balance is not None else result.get("balance")
                    currency = currency if currency is not None else result.get("currency")
                if balance is not None:
                    set_state(balance=float(balance), currency=currency)

            # Il payout dell'asset, se la libreria lo espone. Non si inventa:
            # senza, il motore mostra "—" e il portafoglio resta spento finche'
            # non passi tu --payout.
            payout = None
            for name in ("get_payout", "payout", "get_payouts"):
                fn = getattr(client, name, None)
                if fn is None:
                    continue
                try:
                    value = fn(asset) if name != "payout" else fn
                    if asyncio.iscoroutine(value):
                        value = await value
                    if isinstance(value, dict):
                        value = value.get(asset)
                    if isinstance(value, (int, float)):
                        payout = float(value)
                        if payout > 1.5:       # arriva in percentuale
                            payout /= 100.0
                        break
                except Exception:              # noqa: BLE001 - e' facoltativo
                    continue
            if payout is not None:
                set_state(payout=payout)

            with LOCK:
                STATE["reads"] = int(STATE["reads"]) + 1
            set_state(connected=True, last_error=None, last_fetch_ts=now_ms())

        except Exception as exc:               # noqa: BLE001 - si riprova sempre
            set_state(connected=False,
                      last_error=f"{type(exc).__name__}: {exc}")
            if client is not None:
                try:
                    await client.disconnect()
                except Exception:              # noqa: BLE001
                    pass
                client = None
            await asyncio.sleep(5.0)
            continue

        await asyncio.sleep(max(1.0, poll_s))


class Handler(BaseHTTPRequestHandler):
    """Serve solo memoria: non tocca la rete, quindi non blocca mai il motore."""

    def log_message(self, *args) -> None:      # niente log di richieste
        pass

    def do_GET(self) -> None:                  # noqa: N802
        path = self.path.split("?")[0].rstrip("/") or "/"
        if path not in ("/status", "/"):
            self.send_error(404, "solo /status")
            return
        with LOCK:
            body = dict(STATE)
        ts = body.get("last_fetch_ts")
        body["age_s"] = round((now_ms() - int(ts)) / 1000.0, 1) if ts else None
        body["version"] = VERSION
        body["note"] = ("bridge di SOLA LETTURA: nessun ordine viene mai "
                        "inviato da questo processo")
        raw = json.dumps(body, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="pocket_bridge_service.py",
        description="Bridge Pocket Option -> AURUM, SOLA LETTURA. "
                    "Nessun ordine viene mai inviato.")
    p.add_argument("--auth", default="~/.aurum/pocket.auth",
                   help="file con la stringa di sessione del broker")
    p.add_argument("--asset", default="EURUSD",
                   help="asset di cui leggere il payout")
    p.add_argument("--host", default="127.0.0.1",
                   help="ascolta solo in locale: non esporre questo servizio")
    p.add_argument("--port", type=int, default=8010)
    p.add_argument("--poll", type=float, default=3.0,
                   help="secondi fra una lettura e l'altra")
    args = p.parse_args(argv)

    auth, is_demo = read_auth(args.auth)
    set_state(asset=args.asset, mode="demo" if is_demo else "live",
              account_type="demo" if is_demo else "live")

    print(f"POCKET BRIDGE {VERSION}")
    print(f"  sessione   : {os.path.expanduser(args.auth)}")
    print(f"  conto      : {'DEMO' if is_demo else 'LIVE'}")
    print(f"  asset      : {args.asset}")
    print(f"  in ascolto : http://{args.host}:{args.port}/status")
    print("  ORDINI     : NON ESISTONO IN QUESTO PROGRAMMA")
    print()

    def run_broker() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(broker_loop(auth, is_demo, args.asset,
                                                args.poll))
        finally:
            loop.close()

    threading.Thread(target=run_broker, name="broker", daemon=True).start()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.daemon_threads = True
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nbridge fermato. Nessun ordine e' mai stato inviato.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
