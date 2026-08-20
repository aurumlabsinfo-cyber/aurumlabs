"""A controllable stand-in for Bybit V5 (REST + websocket).

It is *not* a market simulator - it is a wire-level double, so the real client
code (signing, subscribe batching, ping/pong, book sequencing, private auth,
order placement) is what gets tested.  Tests drive it explicitly:

    await fake.push_book("BTCUSDT", bid=100, ask=100.1)     # normal update
    await fake.push_book("BTCUSDT", ..., skip_sequence=True) # a gap
    await fake.drop_sockets()                                # a reconnect
    fake.silence = True                                      # a stale feed
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from typing import Any

from aiohttp import WSMsgType, web


class FakeBybit:
    def __init__(self, api_key: str = "testkey", api_secret: str = "testsecret") -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.app = web.Application()
        self.runner: web.AppRunner | None = None
        self.port = 0

        self.public_sockets: set[web.WebSocketResponse] = set()
        self.private_sockets: set[web.WebSocketResponse] = set()
        self.subscriptions: dict[int, set[str]] = {}
        self.silence = False
        self.reject_subscribe = False
        self.reject_auth = False
        self.answer_ping = True

        self.update_ids: dict[str, int] = {}
        self.orders: list[dict[str, Any]] = []
        self.executions: list[dict[str, Any]] = []
        self.positions: list[dict[str, Any]] = []
        self.wallet = {
            "accountType": "UNIFIED",
            "totalEquity": "1000",
            "totalAvailableBalance": "1000",
            "coin": [{"coin": "USDT", "equity": "1000", "walletBalance": "1000"}],
        }
        self.order_error: tuple[int, str] | None = None
        self.auto_fill = True
        self.signed_requests: list[dict[str, str]] = []
        self.subscribe_calls: list[list[str]] = []
        self.symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

        self._setup()

    # ---------------------------------------------------------------- routes
    def _setup(self) -> None:
        r = self.app.router
        r.add_get("/v5/market/time", self.market_time)
        r.add_get("/v5/market/instruments-info", self.instruments)
        r.add_get("/v5/market/tickers", self.tickers)
        r.add_get("/v5/market/orderbook", self.orderbook)
        r.add_get("/v5/account/wallet-balance", self.wallet_balance)
        r.add_get("/v5/account/fee-rate", self.fee_rate)
        r.add_get("/v5/position/list", self.position_list)
        r.add_get("/v5/order/realtime", self.open_orders)
        r.add_get("/v5/execution/list", self.execution_list)
        r.add_post("/v5/order/create", self.order_create)
        r.add_post("/v5/order/cancel", self.order_cancel)
        r.add_post("/v5/order/cancel-all", self.order_cancel_all)
        r.add_post("/v5/position/set-leverage", self.set_leverage)
        r.add_get("/v5/public/linear", self.public_ws)
        r.add_get("/v5/private", self.private_ws)

    async def start(self) -> "FakeBybit":
        self.runner = web.AppRunner(self.app, access_log=None)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
        return self

    async def stop(self) -> None:
        for socket in list(self.public_sockets | self.private_sockets):
            await socket.close()
        if self.runner:
            await self.runner.cleanup()

    @property
    def rest_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def ws_public_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/v5/public/linear"

    @property
    def ws_private_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/v5/private"

    # ---------------------------------------------------------------- REST
    @staticmethod
    def _ok(result: Any) -> web.Response:
        return web.json_response({"retCode": 0, "retMsg": "OK", "result": result,
                                  "time": int(time.time() * 1000)})

    def _record_signature(self, request: web.Request) -> bool:
        headers = {k: v for k, v in request.headers.items() if k.startswith("X-BAPI")}
        self.signed_requests.append(headers)
        return "X-BAPI-SIGN" in headers

    def verify_signature(self, headers: dict[str, str], payload: str) -> bool:
        expected = hmac.new(
            self.api_secret.encode(),
            (headers["X-BAPI-TIMESTAMP"] + headers["X-BAPI-API-KEY"]
             + headers["X-BAPI-RECV-WINDOW"] + payload).encode(),
            hashlib.sha256,
        ).hexdigest()
        return expected == headers["X-BAPI-SIGN"]

    async def market_time(self, request: web.Request) -> web.Response:
        now = time.time()
        return self._ok({"timeSecond": str(int(now)), "timeNano": str(int(now * 1e9))})

    async def instruments(self, request: web.Request) -> web.Response:
        return self._ok({
            "category": "linear",
            "list": [
                {
                    "symbol": symbol,
                    "contractType": "LinearPerpetual",
                    "status": "Trading",
                    "quoteCoin": "USDT",
                    "settleCoin": "USDT",
                    "priceFilter": {"tickSize": "0.01"},
                    "lotSizeFilter": {
                        "qtyStep": "0.001", "minOrderQty": "0.001", "minNotionalValue": "5",
                    },
                    "leverageFilter": {"maxLeverage": "25"},
                }
                for symbol in self.symbols
            ],
            "nextPageCursor": "",
        })

    async def tickers(self, request: web.Request) -> web.Response:
        return self._ok({
            "category": "linear",
            "list": [
                {
                    "symbol": symbol,
                    "lastPrice": "100",
                    "turnover24h": str(500_000_000 - index * 1_000_000),
                    "volume24h": "1000000",
                    "openInterest": "12345",
                    "fundingRate": "0.0001",
                    "bid1Price": "99.99", "ask1Price": "100.01",
                }
                for index, symbol in enumerate(self.symbols)
            ],
        })

    async def orderbook(self, request: web.Request) -> web.Response:
        return self._ok({
            "s": request.query.get("symbol"), "b": [["99.99", "10"]], "a": [["100.01", "10"]],
            "u": 1, "seq": 1,
        })

    async def wallet_balance(self, request: web.Request) -> web.Response:
        self._record_signature(request)
        return self._ok({"list": [self.wallet]})

    async def fee_rate(self, request: web.Request) -> web.Response:
        self._record_signature(request)
        return self._ok({"list": [{"symbol": "BTCUSDT", "takerFeeRate": "0.00055",
                                   "makerFeeRate": "0.0002"}]})

    async def position_list(self, request: web.Request) -> web.Response:
        self._record_signature(request)
        return self._ok({"list": self.positions})

    async def open_orders(self, request: web.Request) -> web.Response:
        self._record_signature(request)
        return self._ok({"list": [o for o in self.orders if o.get("orderStatus") == "New"]})

    async def execution_list(self, request: web.Request) -> web.Response:
        self._record_signature(request)
        return self._ok({"list": self.executions})

    async def order_create(self, request: web.Request) -> web.Response:
        self._record_signature(request)
        body = await request.json()
        if self.order_error:
            code, message = self.order_error
            return web.json_response({"retCode": code, "retMsg": message, "result": {}})
        link_id = body.get("orderLinkId", "")
        if any(o["orderLinkId"] == link_id for o in self.orders):
            return web.json_response({
                "retCode": 110072, "retMsg": "orderLinkId exists", "result": {},
            })
        order_id = f"fake-order-{len(self.orders) + 1}"
        order = {
            "orderId": order_id,
            "orderLinkId": link_id,
            "symbol": body["symbol"],
            "side": body["side"],
            "qty": body["qty"],
            "orderStatus": "New",
            "cumExecQty": "0",
            "avgPrice": "",
            "updatedTime": str(int(time.time() * 1000)),
        }
        self.orders.append(order)
        if self.auto_fill:
            asyncio.create_task(self._fill(order, float(body["qty"])))
        return self._ok({"orderId": order_id, "orderLinkId": link_id})

    async def order_cancel(self, request: web.Request) -> web.Response:
        self._record_signature(request)
        body = await request.json()
        for order in self.orders:
            if order["orderLinkId"] == body.get("orderLinkId"):
                order["orderStatus"] = "Cancelled"
        return self._ok({"orderLinkId": body.get("orderLinkId")})

    async def order_cancel_all(self, request: web.Request) -> web.Response:
        self._record_signature(request)
        for order in self.orders:
            order["orderStatus"] = "Cancelled"
        return self._ok({"list": []})

    async def set_leverage(self, request: web.Request) -> web.Response:
        self._record_signature(request)
        return self._ok({})

    async def _fill(self, order: dict[str, Any], qty: float, price: float = 100.0) -> None:
        await asyncio.sleep(0.02)
        order["orderStatus"] = "Filled"
        order["cumExecQty"] = str(qty)
        order["avgPrice"] = str(price)
        await self.push_execution(
            symbol=order["symbol"], side=order["side"], qty=qty, price=price,
            order_link_id=order["orderLinkId"], order_id=order["orderId"],
        )

    # ---------------------------------------------------------------- websocket
    async def public_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.public_sockets.add(ws)
        self.subscriptions[id(ws)] = set()
        try:
            await self._ws_loop(ws, private=False)
        finally:
            self.public_sockets.discard(ws)
            self.subscriptions.pop(id(ws), None)
        return ws

    async def private_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.private_sockets.add(ws)
        self.subscriptions[id(ws)] = set()
        try:
            await self._ws_loop(ws, private=True)
        finally:
            self.private_sockets.discard(ws)
            self.subscriptions.pop(id(ws), None)
        return ws

    async def _ws_loop(self, ws: web.WebSocketResponse, private: bool) -> None:
        async for msg in ws:
            if msg.type is not WSMsgType.TEXT:
                continue
            payload = json.loads(msg.data)
            op = payload.get("op")
            if op == "ping":
                if self.answer_ping:
                    await ws.send_json({"op": "pong", "success": True, "ret_msg": "pong"})
            elif op == "auth":
                key, expires, signature = payload["args"]
                expected = hmac.new(
                    self.api_secret.encode(), f"GET/realtime{expires}".encode(), hashlib.sha256
                ).hexdigest()
                ok = (not self.reject_auth) and key == self.api_key and signature == expected
                await ws.send_json({"op": "auth", "success": ok,
                                    "ret_msg": "" if ok else "auth failed"})
            elif op == "subscribe":
                args = payload.get("args", [])
                self.subscribe_calls.append(args)
                if self.reject_subscribe:
                    await ws.send_json({"op": "subscribe", "success": False,
                                        "ret_msg": "subscription rejected"})
                else:
                    self.subscriptions[id(ws)] |= set(args)
                    await ws.send_json({"op": "subscribe", "success": True, "ret_msg": "",
                                        "conn_id": str(id(ws))})
            elif op == "unsubscribe":
                self.subscriptions[id(ws)] -= set(payload.get("args", []))
                await ws.send_json({"op": "unsubscribe", "success": True, "ret_msg": ""})

    def topics(self) -> set[str]:
        out: set[str] = set()
        for topics in self.subscriptions.values():
            out |= topics
        return out

    async def _broadcast(self, sockets: set[web.WebSocketResponse], message: dict[str, Any]) -> None:
        if self.silence:
            return
        topic = message.get("topic", "")
        for ws in list(sockets):
            if ws.closed:
                continue
            if topic and topic not in self.subscriptions.get(id(ws), set()):
                continue
            await ws.send_str(json.dumps(message))

    async def drop_sockets(self) -> None:
        """Force every client to reconnect."""
        for ws in list(self.public_sockets | self.private_sockets):
            await ws.close(code=1001, message=b"server restart")
        await asyncio.sleep(0.05)

    # ---------------------------------------------------------------- pushes
    async def push_book(
        self,
        symbol: str,
        bid: float = 99.99,
        ask: float = 100.01,
        bid_size: float = 10.0,
        ask_size: float = 10.0,
        levels: int = 5,
        snapshot: bool = False,
        skip_sequence: bool = False,
        depth: str = "orderbook.50",
    ) -> None:
        update_id = self.update_ids.get(symbol, 0) + (2 if skip_sequence else 1)
        self.update_ids[symbol] = update_id
        bids = [[f"{bid - i * 0.01:.2f}", f"{bid_size:.3f}"] for i in range(levels)]
        asks = [[f"{ask + i * 0.01:.2f}", f"{ask_size:.3f}"] for i in range(levels)]
        await self._broadcast(self.public_sockets, {
            "topic": f"{depth}.{symbol}",
            "type": "snapshot" if snapshot else "delta",
            "ts": int(time.time() * 1000),
            "cts": int(time.time() * 1000),
            "data": {"s": symbol, "b": bids, "a": asks, "u": update_id, "seq": update_id * 2},
        })

    async def push_trade(
        self, symbol: str, price: float = 100.0, size: float = 1.0, side: str = "Buy"
    ) -> None:
        await self._broadcast(self.public_sockets, {
            "topic": f"publicTrade.{symbol}",
            "type": "snapshot",
            "ts": int(time.time() * 1000),
            "data": [{
                "T": int(time.time() * 1000), "s": symbol, "S": side,
                "v": str(size), "p": str(price), "L": "PlusTick", "i": "x", "BT": False,
            }],
        })

    async def push_ticker(self, symbol: str, **fields: Any) -> None:
        data = {"symbol": symbol, "lastPrice": "100", "openInterest": "12345",
                "turnover24h": "500000000", "fundingRate": "0.0001"}
        data.update({k: str(v) for k, v in fields.items()})
        await self._broadcast(self.public_sockets, {
            "topic": f"tickers.{symbol}", "type": "delta",
            "ts": int(time.time() * 1000), "data": data,
        })

    async def push_execution(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        order_link_id: str,
        order_id: str = "fake-order",
        fee: float | None = None,
        exec_id: str | None = None,
    ) -> None:
        exec_id = exec_id or f"exec-{len(self.executions) + 1}"
        row = {
            "category": "linear", "symbol": symbol, "side": side,
            "execId": exec_id, "execPrice": str(price), "execQty": str(qty),
            "execFee": str(fee if fee is not None else qty * price * 0.00055),
            "execType": "Trade", "execTime": str(int(time.time() * 1000)),
            "isMaker": False, "orderId": order_id, "orderLinkId": order_link_id,
        }
        self.executions.append(row)
        await self._broadcast(self.private_sockets, {
            "topic": "execution", "id": exec_id,
            "creationTime": int(time.time() * 1000), "data": [row],
        })

    async def push_wallet(self, equity: float, available: float) -> None:
        self.wallet = {
            "accountType": "UNIFIED",
            "totalEquity": str(equity),
            "totalAvailableBalance": str(available),
            "coin": [{"coin": "USDT", "equity": str(equity), "walletBalance": str(equity)}],
        }
        await self._broadcast(self.private_sockets, {
            "topic": "wallet", "creationTime": int(time.time() * 1000), "data": [self.wallet],
        })

    async def push_position(
        self, symbol: str, side: str, size: float, entry: float, leverage: float = 10.0
    ) -> None:
        row = {
            "symbol": symbol, "side": side, "size": str(size), "entryPrice": str(entry),
            "leverage": str(leverage), "positionValue": str(size * entry),
            "unrealisedPnl": "0", "positionIM": str(size * entry / leverage),
            "createdTime": str(int(time.time() * 1000)),
            "updatedTime": str(int(time.time() * 1000)),
        }
        self.positions = [p for p in self.positions if p["symbol"] != symbol]
        if size > 0:
            self.positions.append(row)
        await self._broadcast(self.private_sockets, {
            "topic": "position", "creationTime": int(time.time() * 1000), "data": [row],
        })
