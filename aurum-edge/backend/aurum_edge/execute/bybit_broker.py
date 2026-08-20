"""The live Bybit broker: private websocket in, REST out.

Order placement is REST because that is the reliable path; *confirmation* is
always the private stream.  ``submit()`` returning an ack proves only that Bybit
accepted the request - :class:`ExecutionCore` waits for the ``execution`` topic
before a position is considered open.

``orderLinkId`` is deterministic, so a resubmission after a reconnect is refused
by Bybit as a duplicate instead of opening a second position.  That refusal is
treated as success, because the first request is the one that counts.
"""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from ..config import Config
from ..scan.bybit_rest import BybitError, BybitRest, BybitTransportError
from ..scan.bybit_ws import WsConnection
from ..util.clock import Clock
from ..util.logging_setup import get_logger
from .broker import ExecutionEvent, ExecutionHandler, OrderAck, OrderIntent

log = get_logger("execute.bybit")

# Bybit answers with these when an orderLinkId has already been used.
DUPLICATE_CODES = {110072, 110079, 10005}
DUPLICATE_HINTS = ("orderlinkid", "duplicate")


class BybitBroker:
    name = "bybit"

    def __init__(
        self,
        cfg: Config,
        clock: Clock,
        rest: BybitRest,
        ws_url: str | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.cfg = cfg
        self.clock = clock
        self.rest = rest
        self.ws_url = ws_url or cfg.bybit.ws_private
        self._session = session
        self._handler: ExecutionHandler | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

        self.conn = WsConnection(
            name="private",
            url=self.ws_url,
            on_message=self._on_message,
            api_key=cfg.bybit.api_key,
            api_secret=cfg.bybit.api_secret,
            private=True,
            ping_interval_s=cfg.scan.ping_interval_s,
            ping_timeout_s=cfg.scan.ping_timeout_s,
            stale_feed_ms=float("inf"),   # a quiet account is normal, not stale
            session=session,
            on_reconnect=self._on_reconnect,
        )

        self.wallet: dict[str, Any] = {}
        self.positions: dict[str, dict[str, Any]] = {}
        self.orders: dict[str, dict[str, Any]] = {}
        self.leverage_set: dict[str, float] = {}
        self.submitted = 0
        self.rejected = 0
        self.filled = 0
        self.last_error: str = ""
        self.on_reconnect_hook = None
        self.on_order_update = None

    # ---------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        if not self.cfg.bybit.has_credentials:
            raise RuntimeError("live broker needs BYBIT_API_KEY and BYBIT_API_SECRET")
        self._loop = asyncio.get_running_loop()
        await self.conn.start(["execution", "order", "position", "wallet"])
        if not await self.conn.wait_live(20.0):
            raise RuntimeError("private websocket did not become live within 20s")

    async def stop(self) -> None:
        await self.conn.stop()

    def set_execution_handler(self, handler: ExecutionHandler) -> None:
        self._handler = handler

    async def _on_reconnect(self, name: str) -> None:
        log.warning("private stream reconnected - account state must be reconciled")
        if self.on_reconnect_hook is not None:
            await self.on_reconnect_hook(name)

    # ---------------------------------------------------------------- stream in
    def _on_message(self, msg: dict[str, Any]) -> None:
        topic = msg.get("topic", "")
        rows = msg.get("data") or []
        if topic == "execution":
            for row in rows:
                self._on_execution(row)
        elif topic == "wallet":
            for row in rows:
                self._on_wallet(row)
        elif topic == "position":
            for row in rows:
                self._on_position(row)
        elif topic == "order":
            for row in rows:
                self._on_order(row)

    def _on_execution(self, row: dict[str, Any]) -> None:
        if row.get("category") not in (None, self.cfg.bybit.category):
            return
        if str(row.get("execType", "Trade")) != "Trade":
            return  # funding, settlement: not a fill of ours
        try:
            event = ExecutionEvent(
                exec_id=str(row["execId"]),
                order_link_id=str(row.get("orderLinkId") or ""),
                symbol=str(row["symbol"]),
                side=str(row["side"]),
                price=float(row["execPrice"]),
                qty=float(row["execQty"]),
                fee=float(row.get("execFee", 0) or 0),
                ts_ms=float(row.get("execTime", self.clock.now_ms())),
                is_maker=bool(row.get("isMaker", False)),
                exchange_order_id=str(row.get("orderId") or ""),
                raw=row,
            )
        except (KeyError, TypeError, ValueError) as exc:
            log.error("malformed execution row: %s (%s)", row, exc)
            return
        self.filled += 1
        if self._handler is not None and self._loop is not None:
            # the stream callback is synchronous; hand the fill to the loop
            self._loop.create_task(self._handler(event))

    def _on_wallet(self, row: dict[str, Any]) -> None:
        self.wallet = row

    def _on_position(self, row: dict[str, Any]) -> None:
        symbol = row.get("symbol")
        if not symbol:
            return
        try:
            size = float(row.get("size", 0) or 0)
        except (TypeError, ValueError):
            size = 0.0
        if size <= 0:
            self.positions.pop(symbol, None)
        else:
            self.positions[symbol] = row

    def _on_order(self, row: dict[str, Any]) -> None:
        link = row.get("orderLinkId")
        if link:
            self.orders[str(link)] = row
            if self.on_order_update is not None:
                self.on_order_update(row)

    # ---------------------------------------------------------------- orders out
    async def ensure_leverage(self, symbol: str, leverage: float) -> None:
        if abs(self.leverage_set.get(symbol, 0.0) - leverage) < 1e-9:
            return
        try:
            await self.rest.set_leverage(symbol, leverage)
            self.leverage_set[symbol] = leverage
        except (BybitError, BybitTransportError) as exc:
            log.warning("could not set leverage %sx on %s: %s", leverage, symbol, exc)

    async def submit(self, intent: OrderIntent) -> OrderAck:
        self.submitted += 1
        if not intent.reduce_only:
            await self.ensure_leverage(intent.symbol, intent.leverage)
        try:
            result = await self.rest.place_order(
                symbol=intent.symbol,
                side=intent.side,
                qty=intent.qty_str,
                order_link_id=intent.order_link_id,
                order_type=intent.order_type,
                price=str(intent.price) if intent.price is not None else None,
                reduce_only=intent.reduce_only,
                time_in_force="IOC" if intent.order_type == "Market" else "GTC",
            )
        except BybitError as exc:
            text = f"{exc.ret_code} {exc.ret_msg}".lower()
            if exc.ret_code in DUPLICATE_CODES or any(h in text for h in DUPLICATE_HINTS):
                # The first attempt is already live: this is the idempotency
                # guard doing its job, not a failure.
                log.warning("duplicate orderLinkId %s - first submission stands",
                            intent.order_link_id)
                return OrderAck(
                    intent.order_link_id, True, self.clock.now_ms(),
                    reject_reason="duplicate orderLinkId (idempotent)",
                    raw={"retCode": exc.ret_code, "retMsg": exc.ret_msg},
                )
            self.rejected += 1
            self.last_error = str(exc)
            return OrderAck(
                intent.order_link_id, False, self.clock.now_ms(),
                reject_reason=f"{exc.ret_code}: {exc.ret_msg}",
            )
        except BybitTransportError as exc:
            # Unknown outcome: the order may or may not exist.  Never retry blind -
            # reconciliation decides, and the deterministic id makes it safe.
            self.rejected += 1
            self.last_error = str(exc)
            return OrderAck(
                intent.order_link_id, False, self.clock.now_ms(),
                reject_reason=f"transport error, outcome unknown: {exc}",
            )

        return OrderAck(
            order_link_id=intent.order_link_id,
            accepted=True,
            ts_ms=self.clock.now_ms(),
            exchange_order_id=str(result.get("orderId") or ""),
            raw=result,
        )

    # ---------------------------------------------------------------- account
    def account(self) -> dict[str, Any]:
        eur = self.cfg.eur_per_usdt
        equity = _f(self.wallet.get("totalEquity"))
        available = _f(self.wallet.get("totalAvailableBalance"))
        used = max(equity - available, 0.0)
        return {
            "source": "bybit",
            "equity_eur": equity * eur,
            "available_eur": available * eur,
            "used_margin_eur": used * eur,
            "realized_eur": 0.0,
            "fees_eur": 0.0,
            "positions": {
                symbol: {
                    "side": "LONG" if row.get("side") == "Buy" else "SHORT",
                    "qty": _f(row.get("size")),
                    "entry_price": _f(row.get("entryPrice") or row.get("avgPrice")),
                    "margin_eur": _f(row.get("positionIM")) * eur,
                    "leverage": _f(row.get("leverage")) or self.cfg.decide.leverage_default,
                    "unrealized_eur": _f(row.get("unrealisedPnl")) * eur,
                }
                for symbol, row in self.positions.items()
            },
            "wallet_raw": self.wallet,
        }

    def health(self) -> dict[str, Any]:
        health = self.conn.health()
        return {
            "broker": "bybit",
            "connected": self.conn.is_live,
            "ws": health,
            "submitted": self.submitted,
            "filled": self.filled,
            "rejected": self.rejected,
            "has_wallet": bool(self.wallet),
            "open_positions": len(self.positions),
            "last_error": self.last_error,
        }


def _f(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
