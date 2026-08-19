"""Snapshot export that does not stop the engine.

Two properties make this safe to run against a live system:

* **Reads only.**  Every query goes through the read connection.  Under WAL a
  reader sees a consistent snapshot without blocking the writer, so ingestion
  continues at full rate while the export runs.
* **Bounded.**  Raw market events are excluded by default — they are the largest
  table by orders of magnitude and are reconstructible from the retention
  window.  Pass ``include_raw`` when the point of the export *is* the raw data.

The output is a directory of JSON files plus a manifest, so a failed export
leaves a partial directory that is obviously partial rather than a truncated
archive that looks whole.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from ..config import Config
from ..logging_setup import get_logger
from ..storage.base import Database
from ..storage.repositories import Repositories

log = get_logger("diagnostics.export")


def export_snapshot(
    config: Config,
    db: Database,
    repos: Repositories,
    *,
    destination: Path | None = None,
    include_raw: bool = False,
    runtime_state: dict[str, Any] | None = None,
    limit: int = 5000,
) -> dict[str, Any]:
    started = time.monotonic()
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    target = (destination or config.export_dir) / f"aurum-export-{stamp}"
    target.mkdir(parents=True, exist_ok=True)

    files: dict[str, int] = {}

    def write(name: str, payload: Any) -> None:
        path = target / f"{name}.json"
        text = json.dumps(payload, indent=2, default=str)
        path.write_text(text, encoding="utf-8")
        files[name] = len(text)

    write("config", config.public_dict())
    write("cycles", repos.wallet.list_cycles(limit=500))
    write("postmortems", repos.wallet.postmortems(limit=200))
    write("wallet_ledger", repos.wallet.list_ledger(limit=limit))
    write("trades", repos.execution.list_trades(limit=limit))
    write("signals", repos.execution.list_signals(limit=limit))
    write("positions", repos.execution.restore_open_positions())
    write("hypotheses", repos.research.list_hypotheses(limit=limit))
    write("experiments", repos.research.list_experiments(limit=limit))
    write("research_memory", repos.research.all_memory(limit=limit))
    write("agent_events", repos.research.recent_agent_events(limit=limit))
    write("strategies", repos.strategies.list_strategies(limit=500))
    write("model_registry", repos.strategies.list_models(limit=200))
    write("system_events", repos.system.recent(limit=limit))

    if runtime_state is not None:
        write("runtime_state", runtime_state)

    if include_raw:
        write(
            "market_events",
            db.query("SELECT * FROM market_events ORDER BY ts_ms DESC LIMIT ?", (limit,)),
        )
        write(
            "orderbook_snapshots",
            db.query("SELECT * FROM orderbook_snapshots ORDER BY ts_ms DESC LIMIT ?", (limit,)),
        )
        write("features", db.query("SELECT * FROM features ORDER BY ts_ms DESC LIMIT ?", (limit,)))

    manifest = {
        "exported_at": stamp,
        "app": {"name": config.app.name, "version": config.app.version, "env": config.app.env},
        "engine_stopped": False,
        "include_raw": include_raw,
        "row_limit": limit,
        "table_counts": db.table_counts(),
        "files": files,
        "duration_s": round(time.monotonic() - started, 3),
        "path": str(target),
    }
    (target / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    log.info("export complete", extra={"path": str(target), "files": len(files)})
    return manifest
