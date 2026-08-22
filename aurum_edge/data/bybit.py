"""Bybit v5, endpoint pubblici di mercato. Sola lettura.

Ogni metodo qui dentro corrisponde a un endpoint sotto `/v5/market/`, che e' la
famiglia che Bybit serve **senza autenticazione**. Non esiste in questo file
una chiave, una firma HMAC, un header `X-BAPI-SIGN`, ne' un endpoint sotto
`/v5/order/`, `/v5/position/` o `/v5/account/`. Non e' una dimenticanza: e' il
motivo per cui questo programma non puo' toccare denaro.

Note sul formato di Bybit che e' facile sbagliare:

* le kline tornano **dalla piu' recente alla piu' vecchia**, e ogni riga e' una
  lista di stringhe. Qui vengono invertite e convertite subito, una volta sola.
* l'ultima kline e' la barra **in formazione**: usarla come dato chiuso e' una
  forma sottile di sguardo sul futuro, perche' quel valore cambiera' ancora.
  `closed_only=True` la scarta, ed e' il default ovunque serva.
* `openInterest` nei ticker e' in contratti; `openInterestValue` in USD. Sono
  due serie diverse e confonderle rende insensate le variazioni.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .. import config
from ..util import timeutil
from ..util.http import FetchError, get_json


# --------------------------------------------------------------------------
# Tipi
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class Kline:
    """Una barra chiusa. `start_ms` e' l'apertura, non la chiusura."""

    start_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float        # in base (BTC)
    turnover: float      # in quote (USDT)

    @property
    def typical(self) -> float:
        return (self.high + self.low + self.close) / 3.0


@dataclass(frozen=True)
class Trade:
    ts_ms: int
    price: float
    size: float
    side: str            # "Buy" | "Sell": e' il lato dell'AGGRESSORE (taker)

    @property
    def notional(self) -> float:
        return self.price * self.size


@dataclass(frozen=True)
class OrderBook:
    ts_ms: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]

    @property
    def best_bid(self) -> float | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0][0] if self.asks else None

    @property
    def mid(self) -> float | None:
        b, a = self.best_bid, self.best_ask
        return (b + a) / 2.0 if b and a else None

    @property
    def spread_bps(self) -> float | None:
        b, a, m = self.best_bid, self.best_ask, self.mid
        if not (b and a and m):
            return None
        return (a - b) / m * 10_000.0

    def imbalance(self, depth: int = 25) -> float | None:
        """(bid - ask) / (bid + ask) sulla profondita' richiesta, in [-1, 1].

        Sulla profondita', non solo sul primo livello: il primo livello e' il
        piu' facile da falsificare e il piu' volatile.
        """
        bid = sum(q for _, q in self.bids[:depth])
        ask = sum(q for _, q in self.asks[:depth])
        total = bid + ask
        return None if total <= 0 else (bid - ask) / total

    def notional_imbalance(self, depth: int = 25) -> float | None:
        bid = sum(p * q for p, q in self.bids[:depth])
        ask = sum(p * q for p, q in self.asks[:depth])
        total = bid + ask
        return None if total <= 0 else (bid - ask) / total


@dataclass(frozen=True)
class Ticker:
    symbol: str
    ts_ms: int
    last_price: float | None
    mark_price: float | None
    index_price: float | None
    bid1: float | None
    ask1: float | None
    bid1_size: float | None
    ask1_size: float | None
    volume_24h: float | None
    turnover_24h: float | None
    open_interest: float | None          # contratti
    open_interest_value: float | None    # USD
    funding_rate: float | None
    next_funding_ms: int | None
    price_24h_pcnt: float | None
    high_24h: float | None
    low_24h: float | None

    @property
    def basis_bps(self) -> float | None:
        """Scostamento del mark dall'index. Il premio del perpetual, in bps."""
        if self.mark_price is None or self.index_price in (None, 0):
            return None
        return (self.mark_price - self.index_price) / self.index_price * 10_000.0


@dataclass(frozen=True)
class OpenInterestPoint:
    ts_ms: int
    open_interest: float


@dataclass(frozen=True)
class FundingPoint:
    ts_ms: int
    rate: float


@dataclass(frozen=True)
class AccountRatioPoint:
    ts_ms: int
    buy_ratio: float
    sell_ratio: float


# --------------------------------------------------------------------------
# Client
# --------------------------------------------------------------------------
def _f(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if f == f and abs(f) != float("inf") else None


def _i(value: Any) -> int | None:
    f = _f(value)
    return None if f is None else int(f)


class BybitPublic:
    """Il client. Costruirlo non apre connessioni e non richiede segreti."""

    def __init__(self, base_url: str | None = None,
                 category: str | None = None) -> None:
        self.base = (base_url or config.BYBIT_REST).rstrip("/")
        self.category = category or config.CATEGORY

    # ---------------------------------------------------------------- basso
    def _call(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        payload = get_json(f"{self.base}{path}", params)
        if not isinstance(payload, dict):
            raise FetchError("API", "risposta non e' un oggetto JSON", path)
        code = payload.get("retCode")
        if code not in (0, None):
            raise FetchError("API", f"retCode={code} retMsg={payload.get('retMsg')}",
                             path)
        result = payload.get("result")
        return result if isinstance(result, dict) else {}

    # ---------------------------------------------------------------- kline
    def klines(self, symbol: str, interval: str = "1", *,
               start_ms: int | None = None, end_ms: int | None = None,
               limit: int = 1000, closed_only: bool = True) -> list[Kline]:
        """Barre in ordine cronologico crescente.

        `interval` e' la stringa di Bybit: "1", "3", "5", "15", "60", "D".
        """
        result = self._call("/v5/market/kline", {
            "category": self.category, "symbol": symbol, "interval": interval,
            "start": start_ms, "end": end_ms, "limit": min(max(limit, 1), 1000),
        })
        rows = result.get("list") or []
        out: list[Kline] = []
        for r in rows:
            if len(r) < 7:
                continue
            ts = _i(r[0])
            o, h, l, c = _f(r[1]), _f(r[2]), _f(r[3]), _f(r[4])
            v, t = _f(r[5]), _f(r[6])
            if ts is None or None in (o, h, l, c):
                continue
            out.append(Kline(ts, o, h, l, c, v or 0.0, t or 0.0))
        out.sort(key=lambda k: k.start_ms)
        if closed_only and out:
            # La barra corrente non e' chiusa: il suo `close` cambiera' ancora.
            step_ms = _interval_ms(interval)
            cutoff = timeutil.now_ms() - step_ms
            out = [k for k in out if k.start_ms <= cutoff]
        return out

    def mark_klines(self, symbol: str, interval: str = "1", *,
                    start_ms: int | None = None, end_ms: int | None = None,
                    limit: int = 1000) -> list[Kline]:
        result = self._call("/v5/market/mark-price-kline", {
            "category": self.category, "symbol": symbol, "interval": interval,
            "start": start_ms, "end": end_ms, "limit": min(max(limit, 1), 1000),
        })
        return _klines_from_ohlc(result.get("list") or [])

    def index_klines(self, symbol: str, interval: str = "1", *,
                     start_ms: int | None = None, end_ms: int | None = None,
                     limit: int = 1000) -> list[Kline]:
        result = self._call("/v5/market/index-price-kline", {
            "category": self.category, "symbol": symbol, "interval": interval,
            "start": start_ms, "end": end_ms, "limit": min(max(limit, 1), 1000),
        })
        return _klines_from_ohlc(result.get("list") or [])

    # --------------------------------------------------------------- ticker
    def ticker(self, symbol: str) -> Ticker:
        result = self._call("/v5/market/tickers",
                            {"category": self.category, "symbol": symbol})
        rows = result.get("list") or []
        if not rows:
            raise FetchError("API", f"nessun ticker per {symbol}", "/v5/market/tickers")
        return _ticker_from(rows[0], symbol)

    def tickers(self, symbols: Iterable[str] | None = None) -> dict[str, Ticker]:
        """Tutti i ticker della categoria in una sola chiamata.

        Per la market breadth serve un paniere: dieci chiamate separate
        costerebbero dieci volte tanto e arriverebbero in dieci istanti diversi,
        rendendo la "sincronia" che si vuole misurare un artefatto.
        """
        result = self._call("/v5/market/tickers", {"category": self.category})
        wanted = set(symbols) if symbols else None
        out: dict[str, Ticker] = {}
        for row in result.get("list") or []:
            sym = row.get("symbol")
            if not sym or (wanted and sym not in wanted):
                continue
            out[sym] = _ticker_from(row, sym)
        return out

    # --------------------------------------------------------------- trades
    def recent_trades(self, symbol: str, limit: int = 1000) -> list[Trade]:
        """Gli scambi recenti. `side` e' il lato del taker: e' cio' che serve.

        Da qui nascono taker buy/sell e il CVD. Bybit non offre uno storico
        profondo di questo endpoint: il CVD storico va costruito accumulandolo
        in avanti, ed e' esattamente cio' che fa il collector.
        """
        result = self._call("/v5/market/recent-trade", {
            "category": self.category, "symbol": symbol,
            "limit": min(max(limit, 1), 1000),
        })
        out: list[Trade] = []
        for row in result.get("list") or []:
            ts, price, size = _i(row.get("time")), _f(row.get("price")), _f(row.get("size"))
            side = row.get("side") or ""
            if ts is None or price is None or size is None or side not in ("Buy", "Sell"):
                continue
            out.append(Trade(ts, price, size, side))
        out.sort(key=lambda t: t.ts_ms)
        return out

    # ------------------------------------------------------------ orderbook
    def orderbook(self, symbol: str, limit: int = 200) -> OrderBook:
        result = self._call("/v5/market/orderbook", {
            "category": self.category, "symbol": symbol,
            "limit": min(max(limit, 1), 500),
        })
        bids = [(p, q) for p, q in
                ((_f(r[0]), _f(r[1])) for r in result.get("b") or [] if len(r) >= 2)
                if p is not None and q is not None]
        asks = [(p, q) for p, q in
                ((_f(r[0]), _f(r[1])) for r in result.get("a") or [] if len(r) >= 2)
                if p is not None and q is not None]
        bids.sort(key=lambda x: -x[0])
        asks.sort(key=lambda x: x[0])
        return OrderBook(_i(result.get("ts")) or timeutil.now_ms(), bids, asks)

    # -------------------------------------------------------- open interest
    def open_interest(self, symbol: str, interval: str = "5min", *,
                      start_ms: int | None = None, end_ms: int | None = None,
                      limit: int = 200) -> list[OpenInterestPoint]:
        """Storico dell'open interest. `interval`: 5min, 15min, 30min, 1h, 4h, 1d.

        Bybit conserva questa serie per un periodo limitato (tipicamente pochi
        mesi sui passi fini). Chi chiede un backfill piu' lungo dell'archivio
        ottiene meno righe: il codice non finge il contrario, lascia il buco.
        """
        result = self._call("/v5/market/open-interest", {
            "category": self.category, "symbol": symbol,
            "intervalTime": interval, "startTime": start_ms, "endTime": end_ms,
            "limit": min(max(limit, 1), 200),
        })
        out: list[OpenInterestPoint] = []
        for row in result.get("list") or []:
            ts, oi = _i(row.get("timestamp")), _f(row.get("openInterest"))
            if ts is not None and oi is not None:
                out.append(OpenInterestPoint(ts, oi))
        out.sort(key=lambda p: p.ts_ms)
        return out

    # -------------------------------------------------------------- funding
    def funding_history(self, symbol: str, *, start_ms: int | None = None,
                        end_ms: int | None = None,
                        limit: int = 200) -> list[FundingPoint]:
        result = self._call("/v5/market/funding/history", {
            "category": self.category, "symbol": symbol,
            "startTime": start_ms, "endTime": end_ms,
            "limit": min(max(limit, 1), 200),
        })
        out: list[FundingPoint] = []
        for row in result.get("list") or []:
            ts = _i(row.get("fundingRateTimestamp"))
            rate = _f(row.get("fundingRate"))
            if ts is not None and rate is not None:
                out.append(FundingPoint(ts, rate))
        out.sort(key=lambda p: p.ts_ms)
        return out

    # ------------------------------------------------------ long/short data
    def account_ratio(self, symbol: str, period: str = "5min",
                      limit: int = 50) -> list[AccountRatioPoint]:
        """Rapporto conti long/short. Sentimento, non posizione.

        Misura la quota di **conti** posizionati in un verso, non il capitale.
        Migliaia di conti piccolissimi da un lato e pochi grandi dall'altro
        danno un rapporto che dice il contrario di quello che si crede.
        """
        result = self._call("/v5/market/account-ratio", {
            "category": self.category, "symbol": symbol,
            "period": period, "limit": min(max(limit, 1), 500),
        })
        out: list[AccountRatioPoint] = []
        for row in result.get("list") or []:
            ts = _i(row.get("timestamp"))
            buy, sell = _f(row.get("buyRatio")), _f(row.get("sellRatio"))
            if ts is not None and buy is not None and sell is not None:
                out.append(AccountRatioPoint(ts, buy, sell))
        out.sort(key=lambda p: p.ts_ms)
        return out

    # --------------------------------------------------------- liquidazioni
    def liquidations_supported(self) -> bool:
        """Bybit v5 pubblica le liquidazioni solo via WebSocket (`allLiquidation`).

        Non esiste un endpoint REST pubblico, e nemmeno uno storico. Il campo
        resta quindi vuoto salvo che un collector WebSocket lo riempia, e la
        dashboard lo dichiara "non disponibile" invece di mostrare uno zero che
        sembrerebbe "nessuna liquidazione".
        """
        return False

    def server_time_ms(self) -> int:
        payload = get_json(f"{self.base}/v5/market/time")
        result = payload.get("result") if isinstance(payload, dict) else None
        if isinstance(result, dict):
            nano = _i(result.get("timeNano"))
            if nano:
                return nano // 1_000_000
            sec = _i(result.get("timeSecond"))
            if sec:
                return sec * 1000
        return _i(payload.get("time")) or timeutil.now_ms()


# --------------------------------------------------------------------------
# Aiutanti di modulo
# --------------------------------------------------------------------------
_INTERVAL_MS = {
    "1": 60_000, "3": 180_000, "5": 300_000, "15": 900_000, "30": 1_800_000,
    "60": 3_600_000, "120": 7_200_000, "240": 14_400_000, "360": 21_600_000,
    "720": 43_200_000, "D": 86_400_000, "W": 604_800_000,
}


def _interval_ms(interval: str) -> int:
    return _INTERVAL_MS.get(str(interval), 60_000)


def _klines_from_ohlc(rows: list[list[Any]]) -> list[Kline]:
    """Mark e index kline hanno 5 colonne: niente volume, niente turnover."""
    out: list[Kline] = []
    for r in rows:
        if len(r) < 5:
            continue
        ts = _i(r[0])
        o, h, l, c = _f(r[1]), _f(r[2]), _f(r[3]), _f(r[4])
        if ts is None or None in (o, h, l, c):
            continue
        out.append(Kline(ts, o, h, l, c, 0.0, 0.0))
    out.sort(key=lambda k: k.start_ms)
    return out


def _ticker_from(row: dict[str, Any], symbol: str) -> Ticker:
    next_funding = _i(row.get("nextFundingTime"))
    return Ticker(
        symbol=symbol,
        ts_ms=timeutil.now_ms(),
        last_price=_f(row.get("lastPrice")),
        mark_price=_f(row.get("markPrice")),
        index_price=_f(row.get("indexPrice")),
        bid1=_f(row.get("bid1Price")),
        ask1=_f(row.get("ask1Price")),
        bid1_size=_f(row.get("bid1Size")),
        ask1_size=_f(row.get("ask1Size")),
        volume_24h=_f(row.get("volume24h")),
        turnover_24h=_f(row.get("turnover24h")),
        open_interest=_f(row.get("openInterest")),
        open_interest_value=_f(row.get("openInterestValue")),
        funding_rate=_f(row.get("fundingRate")),
        next_funding_ms=next_funding if next_funding else None,
        price_24h_pcnt=_f(row.get("price24hPcnt")),
        high_24h=_f(row.get("highPrice24h")),
        low_24h=_f(row.get("lowPrice24h")),
    )
