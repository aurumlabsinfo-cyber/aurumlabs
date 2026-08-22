"""Server HTTP: una dashboard e una manciata di endpoint JSON.

Su `http.server` della libreria standard, senza framework. Il carico e' una
persona che guarda una pagina: qualunque cosa in piu' sarebbe una dipendenza da
installare e aggiornare in cambio di niente.

Tutti gli endpoint sono in **sola lettura**. Non esiste una rotta POST, PUT o
DELETE in questo file, e il gestore risponde 405 a qualunque metodo diverso da
GET. Un pannello che non puo' scrivere non puo' nemmeno essere convinto a
piazzare un ordine da una pagina aperta per sbaglio.

Rotte:

    GET /                      la dashboard
    GET /api/forecast          la previsione corrente
    GET /api/research          stato della ricerca ed edge
    GET /api/status            salute di dati, collector e archivio
    GET /api/history           previsioni passate, con esito quando c'e'
    GET /api/edges             elenco completo degli edge e dei loro stati
    GET /api/news              flusso di notizie recente
    GET /healthz               vivo o no
"""

from __future__ import annotations

import json
import os
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from .. import config
from ..data.store import Store
from ..forecast.engine import ForecastEngine, score_pending
from ..news import feeds as news_feeds
from ..research import lifecycle
from ..util import timeutil

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


class AppState:
    """Stato condiviso fra le richieste: archivio, motore, cache."""

    def __init__(self, store: Store | None = None,
                 collector: Any | None = None) -> None:
        self.store = store or Store()
        self.engine = ForecastEngine(self.store)
        self.collector = collector
        self.started_ts = timeutil.now_ms()
        self._lock = threading.Lock()
        self._cache: dict[str, Any] = {}
        self._cache_ts: dict[str, float] = {}

    def cached(self, key: str, ttl: float, producer) -> Any:
        """Una cache minuscola: la previsione non va ricalcolata a ogni F5."""
        now = time.monotonic()
        with self._lock:
            if key in self._cache and now - self._cache_ts.get(key, 0) < ttl:
                return self._cache[key]
        value = producer()
        with self._lock:
            self._cache[key] = value
            self._cache_ts[key] = now
        return value

    def invalidate(self, key: str | None = None) -> None:
        with self._lock:
            if key is None:
                self._cache.clear()
                self._cache_ts.clear()
            else:
                self._cache.pop(key, None)
                self._cache_ts.pop(key, None)

    # ------------------------------------------------------------- payload
    def forecast_payload(self) -> dict[str, Any]:
        def build() -> dict[str, Any]:
            score_pending(self.store)
            return self.engine.forecast().to_dict()
        return self.cached("forecast", 15.0, build)

    def research_payload(self) -> dict[str, Any]:
        def build() -> dict[str, Any]:
            run = self.store.latest_research_run()
            report: dict[str, Any] = {}
            if run is not None and run["report"]:
                try:
                    report = json.loads(run["report"])
                except (ValueError, TypeError):
                    report = {}

            by_state: dict[str, list[dict[str, Any]]] = {}
            for row in self.store.edges():
                record = lifecycle.EdgeRecord.from_row(row)
                by_state.setdefault(record.state, []).append(record.to_dict())

            champion = self.store.model_by_role("CHAMPION")
            challenger = self.store.model_by_role("CHALLENGER")
            coverage = self.store.coverage()
            scored = self._live_performance()

            validated = by_state.get(lifecycle.VALIDATED, [])
            return {
                "headline": ("NESSUN EDGE VALIDATO" if not validated
                             else f"{len(validated)} EDGE VALIDATI"),
                "counts": {state: len(items) for state, items in by_state.items()},
                "states_order": list(lifecycle.ORDER),
                "edges": by_state,
                "last_run": {
                    "run_id": run["run_id"] if run else None,
                    "started": timeutil.iso(run["started_ts"]) if run else None,
                    "ended": timeutil.iso(run["ended_ts"]) if run else None,
                    "status": run["status"] if run else "MAI ESEGUITA",
                    "rows": run["rows"] if run else None,
                    "candidates": run["candidates"] if run else None,
                },
                "report": {
                    "conclusion": report.get("conclusion"),
                    "dataset": report.get("dataset"),
                    "power": report.get("power"),
                    "leakage": report.get("leakage"),
                    "catalogue": report.get("catalogue"),
                    "edges": {k: v for k, v in (report.get("edges") or {}).items()
                              if k != "confirmed"},
                    "model": _model_summary(report.get("model") or {}),
                },
                "champion": _model_row(champion),
                "challenger": _model_row(challenger),
                "coverage": coverage,
                "live_performance": scored,
            }
        return self.cached("research", 60.0, build)

    def _live_performance(self) -> dict[str, Any]:
        """Come stanno andando le previsioni gia' emesse e gia' scadute.

        E' il numero meno lusinghiero e il piu' importante: misura il sistema
        su previsioni scritte prima di conoscerne l'esito, che e' l'unica prova
        che non si puo' truccare guardando indietro.
        """
        from ..util.numeric import wilson_interval

        rows = self.store.forecasts(limit=1000, scored_only=True)
        directional = [r for r in rows if r["verdict"] in ("LONG", "SHORT")]
        resolved = [r for r in directional if r["outcome_correct"] is not None]
        waits = sum(1 for r in rows if r["verdict"] == "WAIT")
        if not resolved:
            return {
                "available": False, "scored": len(rows), "waits": waits,
                "directional": len(directional),
                "note": ("Nessuna previsione direzionale ancora scaduta e "
                         "risolta. E' lo stato normale all'avvio, e resta tale "
                         "finche' il sistema dice WAIT."),
            }
        hits = sum(1 for r in resolved if r["outcome_correct"])
        lo, hi = wilson_interval(hits, len(resolved))
        return {
            "available": True,
            "scored": len(rows),
            "waits": waits,
            "wait_share": round(waits / len(rows), 3) if rows else None,
            "directional": len(directional),
            "resolved": len(resolved),
            "correct": hits,
            "accuracy": round(hits / len(resolved), 4),
            "ci95": [round(lo, 4), round(hi, 4)],
            "note": ("Previsioni scritte prima dell'esito e giudicate dopo. "
                     "Nessuna e' stata rimossa."),
        }

    def status_payload(self) -> dict[str, Any]:
        coverage = self.store.coverage()
        snapshot = self.store.latest_snapshot(config.SYMBOL)
        age = None
        if snapshot is not None:
            age = round((timeutil.now_ms() - snapshot["ts"]) / 1000.0, 1)
        collector = (self.collector.health.to_dict() if self.collector
                     else {"running": False,
                           "note": "collector non avviato in questo processo"})
        return {
            "symbol": config.SYMBOL,
            "server_started": timeutil.iso(self.started_ts),
            "uptime_seconds": round(
                (timeutil.now_ms() - self.started_ts) / 1000.0, 1),
            "database": {"path": self.store.path, **coverage},
            "live": {
                "last_snapshot": timeutil.iso(snapshot["ts"]) if snapshot else None,
                "age_seconds": age,
                "fresh": bool(age is not None and age <= config.STALE_SECONDS),
                "last_price": snapshot["last_price"] if snapshot else None,
            },
            "collector": collector,
            "config": {
                "horizon_min": config.PRIMARY_HORIZON_MIN,
                "poll_seconds": config.POLL_SECONDS,
                "stale_seconds": config.STALE_SECONDS,
                "min_prob": config.MIN_DIRECTIONAL_PROB,
                "min_margin": config.MIN_PROB_MARGIN,
                "min_quality": config.MIN_SIGNAL_QUALITY,
                "execution_enabled": config.EXECUTION_ENABLED,
            },
            "safety": {
                "execution_enabled": config.EXECUTION_ENABLED,
                "note": ("Questo programma non apre ne' chiude ordini, non si "
                         "collega a un wallet, non usa capitale ne' leva. Usa "
                         "solo endpoint pubblici in lettura."),
            },
        }

    def history_payload(self, limit: int = 100) -> dict[str, Any]:
        rows = self.store.forecasts(limit=limit)
        return {
            "count": len(rows),
            "items": [{
                "at": timeutil.iso(r["ts"]),
                "verdict": r["verdict"],
                "p_long": r["p_long"], "p_short": r["p_short"],
                "p_flat": r["p_flat"],
                "price": r["price"],
                "quality": r["quality"], "regime": r["regime"],
                "outcome_label": r["outcome_label"],
                "outcome_return_bps": r["outcome_return_bps"],
                "outcome_correct": r["outcome_correct"],
                "resolved": r["outcome_ts"] is not None,
            } for r in rows],
        }

    def edges_payload(self) -> dict[str, Any]:
        out: list[dict[str, Any]] = []
        for row in self.store.edges():
            record = lifecycle.EdgeRecord.from_row(row)
            data = record.to_dict()
            obs = self.store.edge_observations(record.edge_id)
            live = [o for o in obs if o["is_live"]]
            resolved = [o for o in live if o["correct"] is not None]
            data["observations"] = {
                "total": len(obs), "live": len(live), "resolved": len(resolved),
                "correct": sum(1 for o in resolved if o["correct"]),
            }
            out.append(data)
        return {"count": len(out), "edges": out}

    def news_payload(self) -> dict[str, Any]:
        return self.cached("news", 120.0,
                           lambda: news_feeds.summarise(self.store))


def _model_row(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    try:
        metrics = json.loads(row["metrics"] or "{}")
    except (ValueError, TypeError):
        metrics = {}
    holdout = metrics.get("holdout") or {}
    wf = metrics.get("walk_forward") or {}
    return {
        "model_id": row["model_id"],
        "created": timeutil.iso(row["created_ts"]),
        "role": row["role"], "kind": row["kind"],
        "horizon_min": row["horizon_min"], "rows": row["rows"],
        "walk_forward": wf,
        "holdout": {
            "balanced_accuracy": holdout.get("balanced_accuracy"),
            "auc_directional": holdout.get("auc_directional"),
            "directional": holdout.get("directional"),
            "calibration_ece": holdout.get("calibration"),
            "by_regime": holdout.get("by_regime"),
        },
    }


def _model_summary(model: dict[str, Any]) -> dict[str, Any]:
    wf = (model.get("walk_forward") or {}).get("overall") or {}
    return {
        "status": model.get("status"),
        "holdout_confirms": model.get("holdout_confirms"),
        "walk_forward": {
            "independent": (model.get("walk_forward") or {})
                           .get("independent_samples"),
            "balanced_accuracy": wf.get("balanced_accuracy"),
            "auc_directional": wf.get("auc_directional"),
            "directional": wf.get("directional"),
            "base_rate": wf.get("directional_base_rate"),
            "calibration": (wf.get("calibration") or {}).get("ece"),
            "consistency": (model.get("walk_forward") or {})
                           .get("fold_consistency"),
        },
        "holdout": model.get("holdout"),
        "verdict": model.get("verdict"),
    }


class Handler(BaseHTTPRequestHandler):
    """Solo GET. Ogni altro metodo riceve 405 e non tocca l'archivio."""

    protocol_version = "HTTP/1.1"
    state: AppState = None                                      # type: ignore
    server_version = "AurumEdge/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        if os.environ.get("AURUM_HTTP_LOG"):
            super().log_message(fmt, *args)

    # -------------------------------------------------------------- metodi
    def do_GET(self) -> None:                                   # noqa: N802
        path = urlparse(self.path).path.rstrip("/") or "/"
        try:
            if path == "/":
                return self._send_file("index.html", "text/html; charset=utf-8")
            if path == "/healthz":
                return self._send_json({"ok": True,
                                        "at": timeutil.iso(timeutil.now_ms())})
            if path == "/api/forecast":
                return self._send_json(self.state.forecast_payload())
            if path == "/api/research":
                return self._send_json(self.state.research_payload())
            if path == "/api/status":
                return self._send_json(self.state.status_payload())
            if path == "/api/history":
                return self._send_json(self.state.history_payload())
            if path == "/api/edges":
                return self._send_json(self.state.edges_payload())
            if path == "/api/news":
                return self._send_json(self.state.news_payload())
            if path.startswith("/static/"):
                return self._send_file(os.path.basename(path),
                                       _mime(path))
            self._send_json({"error": "rotta sconosciuta", "path": path}, 404)
        except BrokenPipeError:
            pass
        except Exception as exc:                                # noqa: BLE001
            self._send_json({
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(limit=6).splitlines()[-4:],
            }, 500)

    def do_POST(self) -> None:                                  # noqa: N802
        self._send_json({
            "error": "questo server e' in sola lettura",
            "note": ("AURUM EDGE DISCOVERY non accetta comandi: non apre "
                     "ordini e non ha nulla da scrivere su richiesta."),
        }, 405)

    do_PUT = do_POST
    do_DELETE = do_POST
    do_PATCH = do_POST

    # ------------------------------------------------------------- risposte
    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, default=str, ensure_ascii=False,
                          separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, name: str, mime: str) -> None:
        # Nessun percorso relativo: si serve solo cio' che sta in `static/`.
        safe = os.path.basename(name)
        full = os.path.join(STATIC_DIR, safe)
        if not os.path.isfile(full):
            return self._send_json({"error": "file non trovato",
                                    "file": safe}, 404)
        with open(full, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def _mime(path: str) -> str:
    if path.endswith(".css"):
        return "text/css; charset=utf-8"
    if path.endswith(".js"):
        return "application/javascript; charset=utf-8"
    if path.endswith(".html"):
        return "text/html; charset=utf-8"
    return "application/octet-stream"


def build_server(host: str | None = None, port: int | None = None,
                 store: Store | None = None,
                 collector: Any | None = None) -> tuple[ThreadingHTTPServer, AppState]:
    state = AppState(store=store, collector=collector)
    handler = type("BoundHandler", (Handler,), {"state": state})
    server = ThreadingHTTPServer((host or config.HTTP_HOST,
                                  port or config.HTTP_PORT), handler)
    server.daemon_threads = True
    return server, state


def serve(host: str | None = None, port: int | None = None,
          store: Store | None = None, collector: Any | None = None,
          log=print) -> None:
    server, _ = build_server(host, port, store, collector)
    h, p = server.server_address[0], server.server_address[1]
    log(f"[web] dashboard su http://{h}:{p}/")
    log("[web] sola lettura: nessuna rotta puo' aprire o chiudere un ordine")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("[web] arresto")
    finally:
        server.server_close()
