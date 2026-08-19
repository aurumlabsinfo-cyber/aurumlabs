"""Deterministic file replay.

This exists so the pipeline above the adapter — books, features, research,
execution — can be exercised end to end without a venue, and so a recorded
incident can be replayed until it is understood.

It is not a market simulator and it is not reachable in production:
``app.env=production`` refuses ``market.feed=replay`` at config-validation time
(see :mod:`aurum.config`), every event it emits carries ``source="replay"``, and
``/health`` reports ``feed.live=false`` for the whole run.  Nothing downstream
has to remember to check — the marker travels with the data.

File format: JSON Lines.  An optional header line ``{"aurum_replay": 1, ...}``
may lead, followed by records::

    {"symbol": "BTCUSDT", "kind": "depth",    "ts_ms": 1, "payload": {...}}
    {"symbol": "BTCUSDT", "kind": "snapshot", "ts_ms": 0, "payload": {"lastUpdateId": 10,
                                                                      "bids": [[p, q]],
                                                                      "asks": [[p, q]]}}
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

from ..domain import EventKind, FeedState, MarketEvent
from ..logging_setup import get_logger
from .base import DepthSnapshot, MarketFeed

log = get_logger("adapters.replay")


class ReplayFeed(MarketFeed):
    kind = "replay"

    def __init__(
        self,
        symbols: list[str],
        path: str | Path,
        *,
        speed: float = 0.0,
        loop: bool = False,
    ) -> None:
        super().__init__(symbols)
        self.path = Path(path)
        #: 0 = as fast as possible; 1.0 = original wall-clock pacing.
        self.speed = speed
        self.loop = loop
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._records: list[dict[str, Any]] = []
        self._snapshots: dict[str, list[dict[str, Any]]] = {}
        self.header: dict[str, Any] = {}
        self.cursor_ts_ms = 0
        self.finished = asyncio.Event()

    # ------------------------------------------------------------------ load

    def load(self) -> int:
        if not self.path.exists():
            raise FileNotFoundError(f"replay file not found: {self.path}")
        records: list[dict[str, Any]] = []
        with self.path.open("r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except ValueError as exc:
                    raise ValueError(f"{self.path}:{line_no}: not valid JSON ({exc})") from exc
                if "aurum_replay" in record:
                    self.header = record
                    continue
                if record.get("kind") == "snapshot":
                    self._snapshots.setdefault(record["symbol"], []).append(record)
                    continue
                records.append(record)
        records.sort(key=lambda r: r.get("ts_ms", 0))
        self._records = records
        for snaps in self._snapshots.values():
            snaps.sort(key=lambda r: r.get("ts_ms", 0))
        self.stats.subscriptions = len({r["symbol"] for r in records})
        self.stats.endpoint = str(self.path)
        log.info(
            "replay loaded",
            extra={"records": len(records), "snapshots": sum(len(v) for v in self._snapshots.values())},
        )
        return len(records)

    # -------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if self._running:
            return
        if not self._records:
            self.load()
        self._running = True
        self.finished.clear()
        self._set_state(FeedState.LIVE, "replay")
        self._task = asyncio.create_task(self._replay_loop(), name="replay-feed")

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        self._set_state(FeedState.DISCONNECTED, "stopped")

    async def _replay_loop(self) -> None:
        while self._running:
            previous_ts: int | None = None
            for record in self._records:
                if not self._running:
                    return
                ts_ms = int(record.get("ts_ms", 0))
                if self.speed > 0 and previous_ts is not None:
                    gap = (ts_ms - previous_ts) / 1000.0 / self.speed
                    if gap > 0:
                        await asyncio.sleep(min(gap, 5.0))
                elif previous_ts is not None:
                    # Even at full speed, yield so consumers get scheduled.
                    await asyncio.sleep(0)
                previous_ts = ts_ms
                self.cursor_ts_ms = ts_ms
                event = self._to_event(record)
                if event is not None:
                    self._emit(event)
            if not self.loop:
                break
        self.finished.set()
        self._set_state(FeedState.DISCONNECTED, "replay finished")

    def _to_event(self, record: dict[str, Any]) -> MarketEvent | None:
        try:
            kind = EventKind(record["kind"])
        except (KeyError, ValueError):
            return None
        ts_ms = int(record.get("ts_ms", 0))
        return MarketEvent(
            symbol=str(record["symbol"]).upper(),
            kind=kind,
            ts_ms=ts_ms,
            recv_ms=int(record.get("recv_ms", ts_ms)),
            payload=dict(record.get("payload", {})),
            source="replay",
        )

    # ------------------------------------------------------------------ REST

    async def fetch_depth_snapshot(self, symbol: str, limit: int = 1000) -> DepthSnapshot:
        snaps = self._snapshots.get(symbol.upper())
        if not snaps:
            raise LookupError(f"replay file has no snapshot for {symbol}")
        chosen = snaps[0]
        for snap in snaps:
            if snap.get("ts_ms", 0) <= self.cursor_ts_ms:
                chosen = snap
            else:
                break
        payload = chosen["payload"]
        now = int(time.time() * 1000)
        return DepthSnapshot(
            symbol=symbol.upper(),
            last_update_id=int(payload["lastUpdateId"]),
            ts_ms=int(chosen.get("ts_ms", now)),
            recv_ms=now,
            bids=[(float(p), float(q)) for p, q in payload.get("bids", ())],
            asks=[(float(p), float(q)) for p, q in payload.get("asks", ())],
        )

    async def sync_time(self) -> int | None:
        return None

    async def verify_endpoints(self) -> dict[str, Any]:
        return {
            "checked": True,
            "ok": self.path.exists(),
            "mode": "replay",
            "live": False,
            "path": str(self.path),
            "records": len(self._records),
            "warning": "REPLAY FEED — this is not live market data.",
        }
