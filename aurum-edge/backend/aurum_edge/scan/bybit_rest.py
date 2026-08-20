"""Bybit V5 REST client.

REST is used only where a stream cannot answer: instrument discovery, the
liquidity pre-filter, order placement, and the reconciliation reads (wallet,
open orders, executions, positions).  Everything that can arrive on a websocket
arrives on a websocket.

Signing follows the V5 rule:
``sign = HMAC_SHA256(secret, timestamp + api_key + recv_window + (query|body))``.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from typing import Any, Mapping
from urllib.parse import urlencode

import aiohttp

from ..config import BybitConfig
from ..util.logging_setup import get_logger

log = get_logger("bybit.rest")


class BybitError(RuntimeError):
    """Bybit answered with a non-zero retCode."""

    def __init__(self, ret_code: int, ret_msg: str, endpoint: str) -> None:
        super().__init__(f"{endpoint}: retCode={ret_code} retMsg={ret_msg}")
        self.ret_code = ret_code
        self.ret_msg = ret_msg
        self.endpoint = endpoint


class BybitTransportError(RuntimeError):
    """The request never reached Bybit (network, DNS, proxy, timeout)."""


class BybitRest:
    def __init__(
        self,
        cfg: BybitConfig,
        session: aiohttp.ClientSession | None = None,
        base_url: str | None = None,
        timeout_s: float = 12.0,
    ) -> None:
        self.cfg = cfg
        self.base_url = (base_url or cfg.rest_base).rstrip("/")
        self._session = session
        self._own_session = session is None
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self.last_latency_ms: float = 0.0
        self.request_count = 0

    async def __aenter__(self) -> "BybitRest":
        await self.ensure_session()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
            self._own_session = True
        return self._session

    async def close(self) -> None:
        if self._own_session and self._session and not self._session.closed:
            await self._session.close()

    # ---------------------------------------------------------------- signing
    def _headers(self, payload: str) -> dict[str, str]:
        ts = str(int(time.time() * 1000))
        recv = str(self.cfg.recv_window_ms)
        to_sign = ts + self.cfg.api_key + recv + payload
        signature = hmac.new(
            self.cfg.api_secret.encode(), to_sign.encode(), hashlib.sha256
        ).hexdigest()
        return {
            "X-BAPI-API-KEY": self.cfg.api_key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv,
            "X-BAPI-SIGN": signature,
            "Content-Type": "application/json",
        }

    # ---------------------------------------------------------------- transport
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        signed: bool = False,
        retries: int = 3,
    ) -> dict[str, Any]:
        session = await self.ensure_session()
        params = {k: v for k, v in (params or {}).items() if v is not None}
        url = f"{self.base_url}{endpoint}"
        delay = 0.4
        last_exc: Exception | None = None

        for attempt in range(retries):
            started = time.perf_counter()
            try:
                if method == "GET":
                    query = urlencode(params)
                    headers = self._headers(query) if signed else {}
                    async with session.get(
                        f"{url}?{query}" if query else url, headers=headers
                    ) as resp:
                        text = await resp.text()
                        status = resp.status
                else:
                    body = json.dumps(params, separators=(",", ":")) if params else "{}"
                    headers = self._headers(body) if signed else {"Content-Type": "application/json"}
                    async with session.post(url, data=body, headers=headers) as resp:
                        text = await resp.text()
                        status = resp.status
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                log.warning("REST %s %s transport error (%s/%s): %s",
                            method, endpoint, attempt + 1, retries, exc)
                if attempt + 1 < retries:
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
                raise BybitTransportError(f"{method} {endpoint}: {exc}") from exc

            self.last_latency_ms = (time.perf_counter() - started) * 1000.0
            self.request_count += 1

            if status >= 500 or status == 429:
                last_exc = BybitTransportError(f"{method} {endpoint}: HTTP {status}")
                if attempt + 1 < retries:
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
                raise last_exc
            if status != 200:
                raise BybitTransportError(f"{method} {endpoint}: HTTP {status}: {text[:300]}")

            try:
                data = json.loads(text)
            except json.JSONDecodeError as exc:
                raise BybitTransportError(f"{method} {endpoint}: bad JSON: {text[:200]}") from exc

            ret_code = int(data.get("retCode", -1))
            if ret_code != 0:
                raise BybitError(ret_code, str(data.get("retMsg", "")), endpoint)
            return data

        raise BybitTransportError(str(last_exc))  # pragma: no cover - loop always returns

    # ---------------------------------------------------------------- public
    async def server_time(self) -> dict[str, Any]:
        data = await self._request("GET", "/v5/market/time")
        return data["result"]

    async def instruments(self) -> list[dict[str, Any]]:
        """All instruments of the configured category, following the cursor."""
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        for _ in range(20):  # hard stop, Bybit pages 1000 at a time
            params = {"category": self.cfg.category, "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            data = await self._request("GET", "/v5/market/instruments-info", params)
            result = data["result"]
            out.extend(result.get("list", []))
            cursor = result.get("nextPageCursor") or None
            if not cursor:
                break
        return out

    async def tickers(self) -> list[dict[str, Any]]:
        data = await self._request("GET", "/v5/market/tickers", {"category": self.cfg.category})
        return data["result"].get("list", [])

    async def orderbook(self, symbol: str, limit: int = 50) -> dict[str, Any]:
        data = await self._request(
            "GET",
            "/v5/market/orderbook",
            {"category": self.cfg.category, "symbol": symbol, "limit": limit},
        )
        return data["result"]

    # ---------------------------------------------------------------- private
    async def wallet_balance(self) -> dict[str, Any]:
        data = await self._request(
            "GET",
            "/v5/account/wallet-balance",
            {"accountType": self.cfg.account_type},
            signed=True,
        )
        rows = data["result"].get("list", [])
        return rows[0] if rows else {}

    async def positions(self) -> list[dict[str, Any]]:
        data = await self._request(
            "GET",
            "/v5/position/list",
            {"category": self.cfg.category, "settleCoin": self.cfg.quote_coin, "limit": 200},
            signed=True,
        )
        return data["result"].get("list", [])

    async def open_orders(self) -> list[dict[str, Any]]:
        data = await self._request(
            "GET",
            "/v5/order/realtime",
            {"category": self.cfg.category, "settleCoin": self.cfg.quote_coin, "limit": 50},
            signed=True,
        )
        return data["result"].get("list", [])

    async def executions(self, limit: int = 100, start_ms: int | None = None) -> list[dict[str, Any]]:
        data = await self._request(
            "GET",
            "/v5/execution/list",
            {
                "category": self.cfg.category,
                "limit": limit,
                "startTime": start_ms,
            },
            signed=True,
        )
        return data["result"].get("list", [])

    async def fee_rates(self, symbol: str | None = None) -> list[dict[str, Any]]:
        data = await self._request(
            "GET",
            "/v5/account/fee-rate",
            {"category": self.cfg.category, "symbol": symbol},
            signed=True,
        )
        return data["result"].get("list", [])

    async def place_order(
        self,
        symbol: str,
        side: str,
        qty: str,
        order_link_id: str,
        order_type: str = "Market",
        price: str | None = None,
        reduce_only: bool = False,
        time_in_force: str = "IOC",
        position_idx: int = 0,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "category": self.cfg.category,
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": qty,
            "orderLinkId": order_link_id,
            "timeInForce": time_in_force,
            "positionIdx": position_idx,
        }
        if price is not None:
            params["price"] = price
        if reduce_only:
            params["reduceOnly"] = True
        data = await self._request("POST", "/v5/order/create", params, signed=True, retries=1)
        return data["result"]

    async def cancel_order(self, symbol: str, order_link_id: str) -> dict[str, Any]:
        data = await self._request(
            "POST",
            "/v5/order/cancel",
            {"category": self.cfg.category, "symbol": symbol, "orderLinkId": order_link_id},
            signed=True,
            retries=1,
        )
        return data["result"]

    async def cancel_all(self) -> dict[str, Any]:
        data = await self._request(
            "POST",
            "/v5/order/cancel-all",
            {"category": self.cfg.category, "settleCoin": self.cfg.quote_coin},
            signed=True,
            retries=1,
        )
        return data["result"]

    async def set_leverage(self, symbol: str, leverage: float) -> None:
        try:
            await self._request(
                "POST",
                "/v5/position/set-leverage",
                {
                    "category": self.cfg.category,
                    "symbol": symbol,
                    "buyLeverage": str(leverage),
                    "sellLeverage": str(leverage),
                },
                signed=True,
                retries=1,
            )
        except BybitError as exc:
            # 110043 = leverage not modified: already the value we want.
            if exc.ret_code != 110043:
                raise
