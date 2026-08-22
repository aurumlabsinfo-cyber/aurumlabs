"""Il collector: interroga Bybit in lettura e scrive nell'archivio.

Perche' polling REST e non WebSocket. Su un orizzonte di trenta minuti, la
differenza fra sapere una cosa adesso e saperla fra venti secondi e' irrilevante:
non stiamo inseguendo un riempimento, stiamo stimando dove sara' il prezzo fra
mezz'ora. Il polling, in cambio, non ha riconnessioni da gestire, non perde
messaggi in silenzio e si diagnostica con un `curl`. E' la scelta noiosa, ed e'
quella giusta qui.

Cosa fa a ogni giro:

* un solo `/tickers` per tutta la categoria: BTC, ETH, SOL e il paniere della
  market breadth arrivano nello stesso istante, cosi' la loro "sincronia" e'
  una misura e non un artefatto di tre chiamate sfalsate;
* il libro a 200 livelli, per lo squilibrio;
* gli scambi recenti, deduplicati, per taker buy/sell e CVD;
* le barre al minuto, per tenere la griglia storica attaccata al presente;
* l'open interest recente e le news, ma piu' di rado.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .. import config
from ..util import timeutil
from ..util.http import FetchError
from .bybit import BybitPublic, Trade
from .store import Store

Log = Callable[[str], None]


@dataclass
class CollectorHealth:
    """Lo stato del collector, cosi' come la dashboard deve raccontarlo."""

    started_ts: int = 0
    last_ok_ts: int | None = None
    last_error: dict[str, Any] | None = None
    polls: int = 0
    failures: int = 0
    consecutive_failures: int = 0
    trades_seen: int = 0
    blocked: bool = False
    blocked_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        age = None
        if self.last_ok_ts:
            age = round((timeutil.now_ms() - self.last_ok_ts) / 1000.0, 1)
        return {
            "started": timeutil.iso(self.started_ts),
            "last_ok": timeutil.iso(self.last_ok_ts),
            "age_seconds": age,
            "fresh": bool(age is not None and age <= config.STALE_SECONDS),
            "polls": self.polls,
            "failures": self.failures,
            "consecutive_failures": self.consecutive_failures,
            "trades_seen": self.trades_seen,
            "blocked": self.blocked,
            "blocked_reason": self.blocked_reason,
            "last_error": self.last_error,
        }


class Collector:
    """Un ciclo di raccolta. Si avvia, si ferma, e dice sempre come sta."""

    def __init__(self, store: Store | None = None,
                 client: BybitPublic | None = None,
                 log: Log = lambda _: None) -> None:
        self.store = store or Store()
        self.client = client or BybitPublic()
        self.log = log
        self.health = CollectorHealth()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_trade_ts = 0
        self._seen_trades: set[tuple[int, float, float, str]] = set()
        self._last_bars_ts = 0.0
        self._last_oi_ts = 0.0
        self._last_news_ts = 0.0
        self._last_ratio_ts = 0.0

    # ------------------------------------------------------------ ciclo vita
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self.health.started_ts = timeutil.now_ms()
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="aurum-collector",
                                        daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=timeout)

    def _loop(self) -> None:
        while not self._stop.is_set():
            began = time.monotonic()
            try:
                self.poll_once()
            except FetchError as exc:
                self._register_failure(exc)
            except Exception as exc:                            # noqa: BLE001
                self._register_failure(FetchError("INTERNO", f"{type(exc).__name__}: {exc}"))
            elapsed = time.monotonic() - began
            self._stop.wait(max(1.0, config.POLL_SECONDS - elapsed))

    def _register_failure(self, exc: FetchError) -> None:
        self.health.failures += 1
        self.health.consecutive_failures += 1
        self.health.last_error = exc.to_dict()
        if exc.kind == "BLOCKED":
            self.health.blocked = True
            self.health.blocked_reason = exc.detail
        self.log(f"[collector] errore {exc.kind}: {exc.detail}")

    # ------------------------------------------------------------- un giro
    def poll_once(self) -> dict[str, Any]:
        """Un giro completo. Sollevata un'eccezione, il giro non e' andato."""
        symbol = config.SYMBOL
        now = timeutil.now_ms()
        out: dict[str, Any] = {"ts": now}

        wanted = {symbol, *config.CONTEXT_SYMBOLS, *config.BREADTH_SYMBOLS}
        tickers = self.client.tickers(wanted)
        main = tickers.get(symbol)
        if main is None:
            raise FetchError("API", f"{symbol} assente dal listino ticker")

        book = self.client.orderbook(symbol, limit=200)
        trades = self.client.recent_trades(symbol, limit=1000)
        new_trades = self._dedupe(trades)
        self._write_flow(symbol, new_trades)

        breadth = _breadth(tickers)
        payload = {
            "breadth": breadth,
            "context": {
                s: {
                    "last": t.last_price,
                    "pcnt24h": t.price_24h_pcnt,
                    "oi": t.open_interest,
                    "funding": t.funding_rate,
                }
                for s, t in tickers.items() if s in config.CONTEXT_SYMBOLS
            },
            "book_depth_levels": min(len(book.bids), len(book.asks)),
            "liquidations": {
                "available": False,
                "reason": ("Bybit v5 pubblica le liquidazioni solo via "
                           "WebSocket, senza storico REST."),
            },
        }

        self.store.add_snapshot({
            "ts": now,
            "symbol": symbol,
            "last_price": main.last_price,
            "mark_price": main.mark_price,
            "index_price": main.index_price,
            "basis_bps": main.basis_bps,
            "bid1": book.best_bid if book.best_bid else main.bid1,
            "ask1": book.best_ask if book.best_ask else main.ask1,
            "spread_bps": book.spread_bps,
            "book_imbalance": book.imbalance(25),
            "book_imbalance_top": book.imbalance(5),
            "book_notional_imb": book.notional_imbalance(25),
            "volume_24h": main.volume_24h,
            "turnover_24h": main.turnover_24h,
            "open_interest": main.open_interest,
            "open_interest_value": main.open_interest_value,
            "funding_rate": main.funding_rate,
            "next_funding_ms": main.next_funding_ms,
            "payload": json.dumps(payload, separators=(",", ":")),
        })

        wall = time.monotonic()
        # Le barre: ogni minuto basta e avanza.
        if wall - self._last_bars_ts > 60:
            self._last_bars_ts = wall
            for sym in (symbol, *config.CONTEXT_SYMBOLS):
                try:
                    bars = self.client.klines(sym, "1", limit=120,
                                              closed_only=True)
                    out[f"bars_{sym}"] = self.store.upsert_bars(sym, bars)
                except FetchError as exc:
                    self.log(f"[collector] barre {sym}: {exc.detail}")

        # Open interest: il passo minimo dell'endpoint e' 5 minuti.
        if wall - self._last_oi_ts > 300:
            self._last_oi_ts = wall
            try:
                pts = self.client.open_interest(symbol, "5min", limit=200)
                out["oi_rows"] = self.store.upsert_open_interest(symbol, "5min", pts)
            except FetchError as exc:
                self.log(f"[collector] open interest: {exc.detail}")

        if wall - self._last_ratio_ts > 900:
            self._last_ratio_ts = wall
            try:
                pts = self.client.account_ratio(symbol, "5min", limit=200)
                out["ratio_rows"] = self.store.upsert_account_ratio(
                    symbol, "5min", pts)
            except FetchError as exc:
                self.log(f"[collector] long/short: {exc.detail}")

        if wall - self._last_news_ts > config.NEWS_REFRESH_SECONDS:
            self._last_news_ts = wall
            try:
                from ..news.feeds import refresh_news
                out["news"] = refresh_news(self.store)
            except Exception as exc:                            # noqa: BLE001
                self.log(f"[collector] news: {type(exc).__name__}: {exc}")

        self.health.polls += 1
        self.health.last_ok_ts = now
        self.health.consecutive_failures = 0
        self.health.blocked = False
        self.health.blocked_reason = None
        out["trades_new"] = len(new_trades)
        return out

    # --------------------------------------------------------------- flusso
    def _dedupe(self, trades: list[Trade]) -> list[Trade]:
        """Gli scambi gia' visti al giro precedente non vanno contati due volte.

        `recent-trade` restituisce sempre l'ultimo migliaio: fra un giro e il
        successivo la sovrapposizione e' quasi totale. Contarla vorrebbe dire
        gonfiare il CVD di un fattore pari al numero di giri, che e' il tipo di
        errore che poi si scambia per un segnale fortissimo.
        """
        fresh: list[Trade] = []
        for t in trades:
            if t.ts_ms < self._last_trade_ts - 5_000:
                continue
            key = (t.ts_ms, t.price, t.size, t.side)
            if key in self._seen_trades:
                continue
            self._seen_trades.add(key)
            fresh.append(t)
        if trades:
            self._last_trade_ts = max(self._last_trade_ts, trades[-1].ts_ms)
        if len(self._seen_trades) > 20_000:
            cutoff = self._last_trade_ts - 120_000
            self._seen_trades = {k for k in self._seen_trades if k[0] >= cutoff}
        self.health.trades_seen += len(fresh)
        return fresh

    def _write_flow(self, symbol: str, trades: list[Trade]) -> None:
        if not trades:
            return
        buckets: dict[int, dict[str, float]] = {}
        for t in trades:
            minute = timeutil.floor_ms(t.ts_ms, 60_000)
            b = buckets.setdefault(minute, {"buy": 0.0, "sell": 0.0,
                                            "buy_n": 0.0, "sell_n": 0.0,
                                            "n": 0.0})
            if t.side == "Buy":
                b["buy"] += t.size
                b["buy_n"] += t.notional
            else:
                b["sell"] += t.size
                b["sell_n"] += t.notional
            b["n"] += 1
        for minute in sorted(buckets):
            b = buckets[minute]
            self.store.add_flow(symbol, minute, taker_buy=b["buy"],
                                taker_sell=b["sell"], buy_notional=b["buy_n"],
                                sell_notional=b["sell_n"], trades=int(b["n"]))


def _breadth(tickers: dict[str, Any]) -> dict[str, Any]:
    """Market breadth: quanti del paniere salgono, e quanto si muovono insieme.

    Una salita di BTC mentre nove monete su dieci scendono non e' la stessa
    cosa di una salita con tutto il paniere dietro. La prima e' spesso una
    copertura o un flusso singolo; la seconda e' un movimento di mercato.
    """
    rows = [(s, t) for s, t in tickers.items() if s in config.BREADTH_SYMBOLS]
    changes = [t.price_24h_pcnt for _, t in rows if t.price_24h_pcnt is not None]
    if not changes:
        return {"available": False}
    up = sum(1 for c in changes if c > 0)
    return {
        "available": True,
        "symbols": len(changes),
        "advancing": up,
        "declining": len(changes) - up,
        "breadth_ratio": round(up / len(changes), 3),
        "median_change_pct": round(sorted(changes)[len(changes) // 2] * 100, 3),
    }


def run_forever(store: Store | None = None, log: Log = print) -> None:
    """Avvio da riga di comando. Ctrl-C ferma."""
    collector = Collector(store=store, log=log)
    log(f"[collector] avvio, passo {config.POLL_SECONDS}s, simbolo {config.SYMBOL}")
    collector.start()
    try:
        while True:
            time.sleep(30)
            h = collector.health.to_dict()
            log(f"[collector] giri={h['polls']} errori={h['failures']} "
                f"eta'={h['age_seconds']}s scambi={h['trades_seen']}")
            if h["blocked"]:
                log(f"[collector] BLOCCATO: {h['blocked_reason']}")
    except KeyboardInterrupt:
        log("[collector] arresto")
        collector.stop()
