"""Dashboard: FastAPI quando c'e', server della libreria standard altrimenti.

Perche' entrambi: il progetto deve installarsi e mostrare qualcosa anche senza
`pip install`, ma FastAPI porta WebSocket e validazione che vale la pena avere
quando e' disponibile. La superficie delle rotte e' identica nei due casi, cosi'
il frontend non sa quale delle due sta parlando.
"""

from __future__ import annotations

import asyncio
import json
import math
import threading
from pathlib import Path
from typing import Any

from ..config import VERSION

try:                                            # pragma: no cover - opzionale
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse, JSONResponse
    import uvicorn
    FASTAPI = True
except Exception:                               # pragma: no cover - opzionale
    FASTAPI = False

#: uvicorn non parla WebSocket da solo: gli serve `websockets` (o `wsproto`).
#: Senza, l'aggiornamento di protocollo su /ws viene respinto con un 404 e il
#: browser ritenta all'infinito. E' un caso frequente — `pip install uvicorn`
#: senza `[standard]` lo produce — quindi va rilevato e DETTO, non subito: la
#: pagina funziona lo stesso interrogando il motore, solo un po' meno viva.
try:                                            # pragma: no cover - opzionale
    import websockets  # noqa: F401
    WEBSOCKETS = True
except Exception:                               # pragma: no cover - opzionale
    try:
        import wsproto  # noqa: F401
        WEBSOCKETS = True
    except Exception:
        WEBSOCKETS = False


def transport_state() -> dict[str, Any]:
    """Come arrivano gli aggiornamenti alla pagina, e perche'."""
    if not FASTAPI:
        return {"realtime": False, "transport": "polling",
                "reason": "fastapi/uvicorn non installati: server della "
                          "libreria standard, stesse rotte senza WebSocket",
                "fix": "pip install 'uvicorn[standard]' fastapi"}
    if not WEBSOCKETS:
        return {"realtime": False, "transport": "polling",
                "reason": "uvicorn senza backend WebSocket: /ws risponde 404",
                "fix": "pip install 'uvicorn[standard]'  (oppure websockets)"}
    return {"realtime": True, "transport": "websocket", "reason": "", "fix": ""}


INDEX_HTML = (Path(__file__).parent / "frontend" / "index.html").read_text(
    encoding="utf-8") if (Path(__file__).parent / "frontend" / "index.html").exists() else ""


def json_safe(value: Any) -> Any:
    """Rende un risultato serializzabile senza far cadere la rotta.

    JSON non ammette `Infinity` ne' `NaN`: un solo valore non finito in fondo a
    una struttura fa fallire l'intera risposta con un 500. E' successo davvero
    — un profit factor infinito (una vittoria, nessuna perdita) faceva
    rispondere 500 a `/wallet`, cioe' proprio nel caso piu' banale.

    La causa si corregge dove nasce; questa e' la rete di sicurezza: un numero
    che non si puo' rappresentare diventa `null`, e il resto della pagina
    continua a funzionare invece di sparire.
    """
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    return value


def _routes(cfg, engine) -> dict[str, Any]:
    """Le funzioni dietro ogni rotta, indipendenti dal server usato."""

    def market() -> dict:
        q = engine.last_quote
        health = engine.feed.health()
        return {
            "symbol": cfg.symbol, "mode": engine.mode,
            "price": q.mid if q else None,
            "bid": q.bid if q else None, "ask": q.ask if q else None,
            "spread": q.spread if q else None,
            "spread_bps": q.spread_bps if q else None,
            "last_update": q.received_ts if q else None,
            "feed_latency_ms": (health.get("quality") or {}).get("feed_latency_ms"),
            "age_ms": (engine.now() - q.received_ts) if q else None,
            "quality": health.get("quality"),
            "price_decimals": cfg.price_decimals,
            "server_ts": engine.now(),
        }

    def candles(interval: str = "1m", limit: int = 240) -> dict:
        buckets = {"5s": 5, "15s": 15, "30s": 30, "1m": 60, "3m": 180,
                   "5m": 300, "15m": 900}
        secs = buckets.get(interval)
        if secs is None:
            return {"error": "intervallo sconosciuto",
                    "available": list(buckets)}
        bars = engine.features.candles.recent(secs, limit)
        markers = [
            {"ts": s.entry_ts, "state": s.state, "direction": s.direction,
             "price": s.entry_price, "expiry_ts": s.expiry_ts,
             "expiry_price": s.expiry_price, "result": s.result,
             "probability": s.probability}
            for s in (list(engine.signals.history[-40:])
                      + list(engine.signals.active.values()))
        ]
        return {"interval": interval, "bucket_s": secs, "count": len(bars),
                "candles": bars, "markers": markers, "mode": engine.mode,
                "server_ts": engine.now(),
                "note": ("Barre costruite dagli stessi tick su cui il motore "
                         "decide: il grafico non racconta un'altra storia.")}

    def current_signal() -> dict:
        now = engine.now()
        active = list(engine.signals.active.values())
        sig = active[0] if active else engine.signals.last_signal
        d = engine.last_decision
        out: dict[str, Any] = {
            "server_ts": now,
            "mode": engine.mode,
            "next_entry_ts": engine.signals.next_entry_ts(now),
            "status": "NO_TRADE",
            "decision": d.explain() if d else None,
            "regime": d.regime if d else None,
            "news_risk": d.news_risk if d else None,
            "safe_mode": engine.safe_mode,
        }
        if sig is not None and sig.is_open():
            out.update({"status": sig.state, "signal": sig.to_dict(now),
                        "seconds_to_entry": round(sig.seconds_to_entry(now), 1)})
        elif d is not None and not d.is_tradeable:
            out["status"] = "WATCH" if d.confidence > 0.5 else "NO_TRADE"
            out["blockers"] = d.blockers
        return out

    def signal_history(limit: int = 120) -> list[dict]:
        """Lo storico dal DATABASE, non dalla memoria del processo.

        La lista in memoria si azzera a ogni riavvio: dopo un `systemctl
        restart` lo storico spariva dallo schermo mentre il saldo restava
        quello di prima, perche' il portafoglio si ricostruisce dal registro e
        lo storico no. Due verita' diverse sulla stessa schermata sono peggio
        di un dato mancante — il database e' l'unica verita', e questa rotta
        legge da li'.

        Se la lettura fallisce si ripiega sulla memoria: uno storico parziale
        vale piu' di una tabella vuota.
        """
        try:
            rows = engine.db.query(
                "SELECT * FROM signals WHERE state IN ('EXPIRED','CANCELLED') "
                "ORDER BY entry_ts DESC LIMIT ?", (limit,))
            if rows:
                return rows
        except Exception:                    # noqa: BLE001 - mai far cadere la pagina
            pass
        return [s.to_dict() for s in engine.signals.history[-limit:]][::-1]

    def agents() -> dict:
        d = engine.last_decision
        if d is None:
            return {"agents": [], "note": "nessuna decisione ancora"}
        return {
            "regime": d.regime,
            "agreement": round(d.agreement, 4),
            "agents": [
                {**o.to_dict(),
                 "reliability_by_regime": engine.reliability.hit_rate(
                     o.agent, d.regime)[0]}
                for o in d.opinions],
            "reliability": engine.reliability.snapshot(),
            "shrink": round(engine.decisions.heuristic_shrink, 4),
            "note": ("Il peso effettivo e' confidenza x affidabilita' storica "
                     "IN QUESTO REGIME: sono grandezze diverse."),
        }

    def statistics() -> dict:
        rows = engine.db.query(
            "SELECT result, direction, confidence, probability, regime, pnl "
            "FROM signals WHERE result IS NOT NULL")
        decided = [r for r in rows if r["result"] in ("WIN", "LOSS")]
        wins = sum(1 for r in decided if r["result"] == "WIN")
        n = len(decided)
        from ..ml.calibration import (calibration_verdict, reliability_curve,
                                      wilson_interval)
        from ..research.validation import binomial_p_value
        lo, hi = wilson_interval(wins, n) if n else (0.0, 1.0)
        be = cfg.breakeven_win_rate
        probs = [r["probability"] for r in decided]
        outs = [1 if r["result"] == "WIN" else 0 for r in decided]
        # L'esito e' "la direzione indicata era giusta": si allinea la
        # probabilita' alla direzione per poter valutare la calibrazione.
        aligned = [p if d == "CALL" else 1 - p
                   for p, d in zip(probs, [r["direction"] for r in decided])]
        curve = reliability_curve(aligned, outs) if n >= 20 else []
        by_regime: dict[str, list[int]] = {}
        for r in decided:
            g = by_regime.setdefault(r["regime"] or "?", [0, 0])
            g[1] += 1
            g[0] += 1 if r["result"] == "WIN" else 0
        gross_win = sum(r["pnl"] for r in rows if (r["pnl"] or 0) > 0)
        gross_loss = -sum(r["pnl"] for r in rows if (r["pnl"] or 0) < 0)
        wr = (wins / n) if n else None
        return {
            "signals": len(rows), "decided": n, "wins": wins,
            "losses": n - wins,
            "draws": sum(1 for r in rows if r["result"] == "DRAW"),
            "win_rate": round(wr, 4) if wr is not None else None,
            "breakeven_win_rate": round(be, 4),
            "edge_over_breakeven": round(wr - be, 4) if wr is not None else None,
            "ci95": [round(lo, 4), round(hi, 4)],
            "p_value_vs_breakeven": (round(binomial_p_value(wins, n, be), 5)
                                     if n else None),
            "beats_breakeven": bool(n and lo > be),
            "expectancy": (round(wr * cfg.payout - (1 - wr), 4)
                           if wr is not None else None),
            "profit_factor": (round(gross_win / gross_loss, 3)
                              if gross_loss > 0 else None),
            "call_win_rate": _side_rate(decided, "CALL"),
            "put_win_rate": _side_rate(decided, "PUT"),
            "by_regime": {k: {"samples": v[1],
                              "win_rate": round(v[0] / v[1], 4)}
                          for k, v in sorted(by_regime.items()) if v[1] >= 3},
            "calibration": curve,
            "calibration_verdict": calibration_verdict(curve, be),
            "signals_per_hour": _per_hour(engine),
            "note": ("Un tasso di vittoria non e' un vantaggio finche' il "
                     "limite INFERIORE dell'intervallo non supera il pareggio."),
        }

    def _side_rate(decided: list[dict], side: str) -> float | None:
        sel = [r for r in decided if r["direction"] == side]
        if not sel:
            return None
        return round(sum(1 for r in sel if r["result"] == "WIN") / len(sel), 4)

    def _per_hour(eng) -> float | None:
        up = (eng.now() - eng.started_ts) / 3_600_000.0
        return round(eng.signals.counters["created"] / up, 2) if up > 0.01 else None

    def blockers() -> dict:
        stats = engine.decisions.stats()
        shadow = engine.research.shadow.report()
        by_reason = {b["blocker"]: b for b in (shadow.get("blockers") or [])}
        out = []
        for code, count in stats["blockers"].items():
            s = by_reason.get(code, {})
            out.append({
                "blocker": code, "count": count,
                "share": round(count / max(1, stats["decisions"]), 4),
                "shadow_win_rate": s.get("shadow_win_rate"),
                "shadow_samples": s.get("blocked"),
                "shadow_expectancy": s.get("expectancy"),
                "verdict": s.get("verdict", "nessun esito ombra ancora"),
            })
        return {"decisions": stats["decisions"], "blockers": out,
                "emitted": stats["emitted"],
                "emission_rate": stats["emission_rate"],
                "note": ("Un filtro va giudicato su cosa ha scartato: se le "
                         "decisioni bloccate avrebbero vinto, sta costando.")}

    def orderflow() -> dict:
        d = engine.last_decision
        f = d.features if d else {}
        windows = {}
        for w in (1, 3, 5, 10, 15, 30):
            windows[f"{w}s"] = f.get(f"tick_imbalance_{w}s")
        available = engine.feed.adapter.capabilities.has_bid_ask if engine.feed.adapter else False
        return {
            "available": bool(available),
            "windows": windows,
            "score": f.get("tick_imbalance_5s"),
            "quote_velocity_1s": f.get("quote_velocity_1s"),
            "spread_bps": f.get("spread_bps"),
            "spread_vs_average": f.get("spread_vs_average"),
            "note": ("Su un feed FX gratuito non esiste un book di livello 2: "
                     "qui c'e' solo cio' che il provider fornisce davvero — "
                     "direzione dei tick e spread. Nulla viene inventato."),
        }

    def preview() -> dict:
        active = list(engine.signals.active.values())
        sig = active[0] if active else engine.signals.last_signal
        if sig is None:
            return {"snapshots": {}, "note": "nessun segnale in corso"}
        return {"signal_id": sig.signal_id, "direction": sig.direction,
                "snapshots": sig.previews,
                "evolution": [u.to_dict() for u in sig.updates],
                "note": ("Le fotografie da T-30 servono a capire come si e' "
                         "sviluppato un segnale, non solo com'e' finito.")}

    return {
        "/health": lambda: {**engine.health(), "dashboard": transport_state()},
        "/diagnostics": lambda: engine.diagnostics(),
        "/market": market,
        "/candles": candles,
        "/signal": current_signal,
        "/signals": lambda: {"active": [s.to_dict(engine.now())
                                        for s in engine.signals.active.values()],
                             "history": signal_history(),
                             "stats": engine.signals.stats()},
        "/agents": agents,
        "/wallet": lambda: {**engine.wallet.status(),
                            "equity_curve": engine.wallet.equity_curve()},
        "/cycles": lambda: {"cycles": engine.wallet.cycle_history()},
        "/statistics": statistics,
        "/latency": lambda: engine.latency.report(),
        "/blockers": blockers,
        "/booster": lambda: engine.booster.report(cfg.breakeven_win_rate),
        "/orderflow": orderflow,
        "/preview": preview,
        "/research": lambda: engine.research.status(),
        "/models": lambda: {"active": None, "versions":
                            engine.db.query("SELECT model_id, ts, algorithm, "
                                            "n_train FROM model_versions "
                                            "ORDER BY ts DESC LIMIT 20")},
        "/strategy": lambda: {"champion": engine.research.champion_id,
                              "shrink": engine.decisions.heuristic_shrink,
                              "policy": engine.research.status()["policy"]},
        "/config": lambda: cfg.to_dict(),
    }


def build_app(cfg, engine):
    """Costruisce l'app FastAPI. Usata anche dal selftest per le rotte."""
    if not FASTAPI:
        raise RuntimeError("FastAPI non installato")
    app = FastAPI(title="AURUM SIGNAL ENGINE M60", version=VERSION,
                  docs_url="/docs")
    routes = _routes(cfg, engine)

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return INDEX_HTML

    def _register(path: str, fn):
        if path == "/candles":
            @app.get(path)
            def _candles(interval: str = "1m", limit: int = 240):
                return JSONResponse(json_safe(fn(interval, limit)))
        else:
            @app.get(path, name=path.strip("/").replace("/", "_") or "root")
            def _handler(_fn=fn):
                return JSONResponse(json_safe(_fn()))

    for path, fn in routes.items():
        _register(path, fn)

    @app.post("/config")
    async def update_config(payload: dict) -> dict:
        """Modifica i pochi valori che ha senso cambiare da schermo.

        Ogni valore viene validato: un payout a zero o una puntata piu' grande
        del capitale non sono configurazioni aggressive, sono stati che
        renderebbero insensato ogni numero prodotto dopo.
        """
        allowed = {"virtual_stake": (0.01, cfg.virtual_capital),
                   "payout": (0.05, 5.0),
                   "signal_lead_seconds": (0, 300),
                   "audio_enabled": None, "telegram_enabled": None,
                   "research_enabled": None, "booster_enabled": None}
        applied, rejected = {}, {}
        for key, value in (payload or {}).items():
            if key not in allowed:
                rejected[key] = "non modificabile"
                continue
            bounds = allowed[key]
            if bounds is None:
                setattr(cfg, key, bool(value))
                applied[key] = bool(value)
                continue
            try:
                v = float(value)
            except (TypeError, ValueError):
                rejected[key] = "non numerico"
                continue
            lo, hi = bounds
            if not lo <= v <= hi:
                rejected[key] = f"fuori intervallo {lo}-{hi}"
                continue
            setattr(cfg, key, int(v) if key.endswith("_seconds") else v)
            applied[key] = getattr(cfg, key)
        try:
            cfg.validate()
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
        return {"ok": not rejected, "applied": applied, "rejected": rejected,
                "config": cfg.to_dict()}

    @app.websocket("/ws")
    async def ws(socket: WebSocket) -> None:
        """Aggiornamenti in tempo reale, senza che il browser interroghi."""
        await socket.accept()
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue(maxsize=200)

        def on_event(event: str, payload: dict) -> None:
            try:
                loop.call_soon_threadsafe(queue.put_nowait,
                                          {"event": event, "data": payload})
            except Exception:  # noqa: BLE001 - coda piena: si perde un evento
                pass

        engine.subscribe(on_event)
        try:
            await socket.send_json({"event": "hello",
                                    "data": {"version": VERSION,
                                             "mode": engine.mode,
                                             "symbol": cfg.symbol}})
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=1.0)
                    await socket.send_json(msg)
                except asyncio.TimeoutError:
                    await socket.send_json({"event": "tick",
                                            "data": routes["/signal"]()})
        except WebSocketDisconnect:
            pass
        finally:
            if on_event in engine._subscribers:
                engine._subscribers.remove(on_event)

    return app


class DashboardServer:
    """Avvia il server su un thread: il motore non deve aspettarlo."""

    def __init__(self, cfg, engine) -> None:
        self.cfg = cfg
        self.engine = engine
        self.port = cfg.http_port
        self._thread: threading.Thread | None = None
        self._server = None

    def start(self) -> None:
        if FASTAPI:
            self._start_fastapi()
        else:
            self._start_stdlib()

    def _start_fastapi(self) -> None:
        app = build_app(self.cfg, self.engine)
        config = uvicorn.Config(app, host=self.cfg.http_host, port=self.port,
                                log_level="warning", access_log=False)
        self._server = uvicorn.Server(config)

        def run() -> None:
            asyncio.run(self._server.serve())

        self._thread = threading.Thread(target=run, name="dashboard", daemon=True)
        self._thread.start()

    def _start_stdlib(self) -> None:
        """Riserva senza dipendenze: stessa superficie, senza WebSocket."""
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from urllib.parse import parse_qs, urlsplit

        routes = _routes(self.cfg, self.engine)

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):  # noqa: N802
                parts = urlsplit(self.path)
                path = parts.path.rstrip("/") or "/"
                if path == "/":
                    body = INDEX_HTML.encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                fn = routes.get(path)
                if fn is None:
                    payload, status = {"error": "rotta sconosciuta",
                                       "available": sorted(routes)}, 404
                else:
                    try:
                        q = {k: v[0] for k, v in parse_qs(parts.query).items()}
                        payload = (fn(q.get("interval", "1m"),
                                      int(q.get("limit", 240)))
                                   if path == "/candles" else fn())
                        status = 200
                    except Exception as exc:  # noqa: BLE001
                        payload, status = {"error": f"{type(exc).__name__}: {exc}"}, 500
                body = json.dumps(json_safe(payload), default=str).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        for port in range(self.cfg.http_port, self.cfg.http_port + 10):
            try:
                self._server = ThreadingHTTPServer((self.cfg.http_host, port),
                                                   Handler)
                self.port = port
                break
            except OSError:
                continue
        if self._server is None:
            return
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        name="dashboard", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is None:
            return
        if FASTAPI:
            self._server.should_exit = True
        else:
            self._server.shutdown()
