"""Adapter live per EUR/USD, con sorgenti gratuite o freemium.

Una nota che vale piu' del codice: **il tick reale FX gratuito non esiste.**
I livelli gratuiti danno quotazioni al secondo nel migliore dei casi, spesso
al minuto, e quasi mai bid/ask veri. Ogni adapter dichiara cosa fornisce
davvero tramite `capabilities`, e il motore ne tiene conto invece di far finta
di avere una microstruttura che non ha.

Chi vuole vero order flow FX deve pagare un feed istituzionale: non c'e' un
modo gratuito, e inventarne uno sarebbe peggio che dichiararlo.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, AsyncIterator

from .base import AdapterCapabilities, MarketDataAdapter, Quote, now_ms


async def _fetch_json(url: str, timeout: float = 8.0) -> Any:
    """GET JSON senza bloccare il loop asincrono."""
    def _blocking() -> Any:
        req = urllib.request.Request(url, headers={"User-Agent": "aurum-m60"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}")
            return json.loads(resp.read().decode("utf-8"))
    return await asyncio.to_thread(_blocking)


class TwelveDataAdapter(MarketDataAdapter):
    """Twelve Data: quotazione EUR/USD via REST, livello gratuito.

    Il livello gratuito non da' bid/ask sul cambio: `has_bid_ask=False`, e gli
    agenti di spread si astengono di conseguenza.
    """

    name = "twelvedata"

    def __init__(self, cfg) -> None:
        super().__init__(cfg)
        self.capabilities = AdapterCapabilities(
            has_bid_ask=False, tick_level=False,
            typical_interval_ms=int(cfg.quote_poll_seconds * 1000),
            note="livello gratuito: prezzo senza bid/ask")
        self.api_key = cfg.twelvedata_api_key

    async def connect(self) -> bool:
        if not self.api_key:
            self.health_state.last_error = "TWELVEDATA_API_KEY mancante"
            return False
        try:
            data = await self._quote_once()
        except Exception as exc:  # noqa: BLE001
            self.health_state.last_error = f"{type(exc).__name__}: {exc}"
            return False
        self.health_state.connected = data is not None
        self.health_state.last_error = None if data else "risposta senza prezzo"
        return data is not None

    async def _quote_once(self) -> Quote | None:
        params = urllib.parse.urlencode({
            "symbol": self.cfg.symbol, "apikey": self.api_key})
        data = await _fetch_json(f"https://api.twelvedata.com/price?{params}")
        if not isinstance(data, dict) or "price" not in data:
            raise RuntimeError(str(data)[:160])
        price = float(data["price"])
        if price <= 0:
            raise RuntimeError("prezzo non positivo")
        # Twelve Data /price non manda un timestamp: la latenza NON e'
        # misurabile e viene dichiarata tale (ts=0 -> latency_ms None).
        return self._make_quote(ts=0, mid=price, last=price)

    async def quotes(self) -> AsyncIterator[Quote]:
        interval = max(0.5, self.cfg.quote_poll_seconds)
        while True:
            try:
                q = await self._quote_once()
                if q is not None:
                    self.health_state.connected = True
                    self.health_state.last_error = None
                    yield q
            except Exception as exc:  # noqa: BLE001 - si riprova, non si muore
                self.health_state.connected = False
                self.health_state.last_error = f"{type(exc).__name__}: {exc}"
                self.health_state.reconnects += 1
                await asyncio.sleep(min(30.0, interval * 4))
                continue
            await asyncio.sleep(interval)


class FinnhubAdapter(MarketDataAdapter):
    """Finnhub: quotazione forex, livello gratuito.

    Manda un timestamp della sorgente, quindi qui la latenza del feed e'
    davvero misurabile — cosa che su Twelve Data /price non e'.
    """

    name = "finnhub"

    def __init__(self, cfg) -> None:
        super().__init__(cfg)
        self.capabilities = AdapterCapabilities(
            has_bid_ask=False, tick_level=False,
            typical_interval_ms=int(cfg.quote_poll_seconds * 1000),
            note="livello gratuito: prezzo con timestamp, senza bid/ask")
        self.api_key = cfg.finnhub_api_key

    async def connect(self) -> bool:
        if not self.api_key:
            self.health_state.last_error = "FINNHUB_API_KEY mancante"
            return False
        try:
            q = await self._quote_once()
        except Exception as exc:  # noqa: BLE001
            self.health_state.last_error = f"{type(exc).__name__}: {exc}"
            return False
        self.health_state.connected = q is not None
        return q is not None

    async def _quote_once(self) -> Quote | None:
        sym = "OANDA:" + self.cfg.symbol.replace("/", "_")
        params = urllib.parse.urlencode({"symbol": sym, "token": self.api_key})
        data = await _fetch_json(f"https://finnhub.io/api/v1/quote?{params}")
        if not isinstance(data, dict):
            raise RuntimeError(str(data)[:160])
        price = data.get("c")
        if not price:
            raise RuntimeError(f"risposta senza prezzo: {str(data)[:120]}")
        ts = int(float(data.get("t") or 0) * 1000)
        return self._make_quote(ts=ts, mid=float(price), last=float(price))

    async def quotes(self) -> AsyncIterator[Quote]:
        interval = max(0.5, self.cfg.quote_poll_seconds)
        while True:
            try:
                q = await self._quote_once()
                if q is not None:
                    self.health_state.connected = True
                    self.health_state.last_error = None
                    yield q
            except Exception as exc:  # noqa: BLE001
                self.health_state.connected = False
                self.health_state.last_error = f"{type(exc).__name__}: {exc}"
                self.health_state.reconnects += 1
                await asyncio.sleep(min(30.0, interval * 4))
                continue
            await asyncio.sleep(interval)


class ExchangeRateAdapter(MarketDataAdapter):
    """open.er-api.com: gratuito e senza chiave, ma LENTO.

    Aggiorna nell'ordine dei minuti: e' inutilizzabile come feed operativo per
    un orizzonte da 60 secondi, e serve solo come riserva d'emergenza e come
    riferimento per accorgersi se le altre fonti divergono. L'adapter lo
    dichiara e il motore rifiuta di emettere segnali quando questa e' l'unica
    sorgente viva.
    """

    name = "exchangerate"
    slow_reference_only = True

    def __init__(self, cfg) -> None:
        super().__init__(cfg)
        self.capabilities = AdapterCapabilities(
            has_bid_ask=False, tick_level=False, typical_interval_ms=60_000,
            note="gratuito senza chiave, aggiornamento lento: solo riferimento")

    async def connect(self) -> bool:
        try:
            q = await self._quote_once()
        except Exception as exc:  # noqa: BLE001
            self.health_state.last_error = f"{type(exc).__name__}: {exc}"
            return False
        self.health_state.connected = q is not None
        return q is not None

    async def _quote_once(self) -> Quote | None:
        base, _, quote_ccy = self.cfg.symbol.partition("/")
        data = await _fetch_json(f"https://open.er-api.com/v6/latest/{base}")
        rate = (data.get("rates") or {}).get(quote_ccy)
        if not rate:
            raise RuntimeError("nessun cambio nella risposta")
        ts = int(float(data.get("time_last_update_unix") or 0) * 1000)
        return self._make_quote(ts=ts, mid=float(rate), last=float(rate))

    async def quotes(self) -> AsyncIterator[Quote]:
        while True:
            try:
                q = await self._quote_once()
                if q is not None:
                    self.health_state.connected = True
                    self.health_state.last_error = None
                    yield q
            except Exception as exc:  # noqa: BLE001
                self.health_state.connected = False
                self.health_state.last_error = f"{type(exc).__name__}: {exc}"
                self.health_state.reconnects += 1
            await asyncio.sleep(30.0)


ADAPTERS: dict[str, type[MarketDataAdapter]] = {
    "twelvedata": TwelveDataAdapter,
    "finnhub": FinnhubAdapter,
    "exchangerate": ExchangeRateAdapter,
}
