"""Import real historical market data from Binance's public archive.

Until now the only way to get research data was to run the engine live and wait.
That makes any serious study take days. Binance publishes its own historical
market data, free and without credentials, at ``data.binance.vision``; this
module downloads it, replays it through the **same** `FeatureEngine` the live
system uses, and writes the result to the database.

Because the features are produced by the identical code path, a strategy
validated on imported data is validated on the same definitions it will see
live. That property is the entire point of replaying rather than recomputing.

What the archive does and does not contain
------------------------------------------
* ``bookTicker`` - best bid/ask with sizes. Gives L1 imbalance, spread,
  micro-price. **It does not contain full depth**, so every depth feature
  (`depth_imbalance_*`, walls, liquidity removal) stays `None` and any agent or
  strategy that needs them correctly abstains. This is stated in the import
  summary rather than papered over.
* ``aggTrades`` - aggregated trades with the maker flag, which is what the
  order-flow features need. These come through complete.

Rows are written with ``source = REPLAY`` and ``is_synthetic = false``: this is
real market data, not a simulation, and the research tooling includes it.
"""

from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterator

import httpx

from app.config import Settings
from app.core.bus import EventBus
from app.core.logging_conf import get_logger
from app.db import repository as repo
from app.db.repository import BatchWriter
from app.features.engine import FeatureEngine
from app.marketdata.orderbook import LocalOrderBook
from app.marketdata.types import BookTicker, DataSource, MarketTick, Trade

log = get_logger(__name__)

ARCHIVE_BASE = "https://data.binance.vision/data/spot/daily"

#: Column layouts as published by Binance. Files may or may not carry a header
#: row depending on their vintage, so the parser sniffs for one.
BOOK_TICKER_COLUMNS = [
    "update_id", "best_bid_price", "best_bid_qty", "best_ask_price",
    "best_ask_qty", "transaction_time", "event_time",
]
AGG_TRADE_COLUMNS = [
    "agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id",
    "transact_time", "is_buyer_maker", "is_best_match",
]


class ImportError_(RuntimeError):
    """Raised when the archive does not look the way we expect."""


@dataclass
class ImportStats:
    days: list[str] = field(default_factory=list)
    book_rows: int = 0
    trade_rows: int = 0
    feature_rows: int = 0
    skipped_days: list[dict[str, str]] = field(default_factory=list)
    first_ts: int | None = None
    last_ts: int | None = None
    bytes_downloaded: int = 0

    def summary(self) -> dict[str, Any]:
        minutes = (
            (self.last_ts - self.first_ts) / 60_000
            if self.first_ts and self.last_ts else 0
        )
        return {
            "days_imported": self.days,
            "days_skipped": self.skipped_days,
            "book_ticker_rows": self.book_rows,
            "trade_rows": self.trade_rows,
            "feature_rows": self.feature_rows,
            "market_minutes": round(minutes, 1),
            "first_ts": self.first_ts,
            "last_ts": self.last_ts,
            "megabytes_downloaded": round(self.bytes_downloaded / 1_048_576, 1),
            "source": "REPLAY",
            "depth_features": (
                "UNAVAILABLE - the public archive carries top-of-book only, so "
                "depth/wall/liquidity-removal features are null and agents that "
                "need them abstain."
            ),
        }


# ------------------------------------------------------------------ download
def archive_url(symbol: str, kind: str, day: date) -> str:
    name = f"{symbol.upper()}-{kind}-{day.isoformat()}.zip"
    return f"{ARCHIVE_BASE}/{kind}/{symbol.upper()}/{name}"


async def fetch_zip(
    client: httpx.AsyncClient, url: str, cache_dir: Path | None
) -> bytes | None:
    """Download (or read from cache) one archive file. None means 404."""
    if cache_dir:
        cached = cache_dir / url.rsplit("/", 1)[-1]
        if cached.exists():
            return cached.read_bytes()

    resp = await client.get(url)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    payload = resp.content
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / url.rsplit("/", 1)[-1]).write_bytes(payload)
    return payload


def read_csv_rows(payload: bytes, expected: list[str]) -> Iterator[list[str]]:
    """Yield CSV rows from a zipped archive file, skipping any header."""
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        names = zf.namelist()
        if not names:
            raise ImportError_("archive is empty")
        with zf.open(names[0]) as fh:
            text = io.TextIOWrapper(fh, encoding="utf-8")
            reader = csv.reader(text)
            first = next(reader, None)
            if first is None:
                return
            # A header row is text where we expect numbers.
            is_header = any(c in first for c in expected[:2])
            if not is_header:
                _validate_width(first, expected)
                yield first
            for row in reader:
                if row:
                    yield row


def _validate_width(row: list[str], expected: list[str]) -> None:
    if len(row) < len(expected):
        raise ImportError_(
            f"expected at least {len(expected)} columns {expected}, got "
            f"{len(row)}: {row[:8]}. The archive layout may have changed - "
            "refusing to guess rather than corrupt the research dataset."
        )


# -------------------------------------------------------------- replay market
class ReplayMarket:
    """The minimal `MarketDataEngine` surface the FeatureEngine depends on.

    `book.synced` is True whenever a quote is present, because the top of book
    genuinely is synchronised - it comes from the venue's own bookTicker feed.
    What is missing is *depth*, and the depth features are `None` to say so.
    """

    def __init__(self, symbol: str, exchange: str = "binance_spot") -> None:
        self.symbol = symbol
        self.primary_name = exchange
        self.last_tick: MarketTick | None = None
        self.last_trade: Trade | None = None
        self.book = LocalOrderBook(exchange, symbol)
        self.derivatives = None
        self.recent_liquidations: list = []
        self.adapters: dict = {}
        self.errors: list = []
        self.source = DataSource.REPLAY
        self.is_synthetic = False

    def record_error(self, component: str, message: str) -> None:
        self.errors.append({"component": component, "message": message})

    def data_quality(self) -> dict[str, Any]:
        # Replay has no live-feed concerns (staleness, latency, warmup); the
        # only real question is whether a quote exists at this instant.
        ok = self.last_tick is not None
        return {
            "score": 1.0 if ok else 0.0,
            "ok": ok,
            "reasons": [] if ok else ["no quote yet"],
            "warmup_complete": ok,
        }


# ------------------------------------------------------------------- import
async def import_days(
    settings: Settings,
    symbol: str,
    start: date,
    end: date,
    cache_dir: str | None = "./data/binance-archive",
    persist: bool = True,
    progress: Any = None,
) -> ImportStats:
    """Download, replay and store one or more days of real market data."""
    stats = ImportStats()
    cache = Path(cache_dir) if cache_dir else None

    bus = EventBus(maxsize=64)
    market = ReplayMarket(symbol)
    features = FeatureEngine(settings, bus, market)  # type: ignore[arg-type]
    writer = BatchWriter(settings) if persist else None

    async with httpx.AsyncClient(
        timeout=120.0, follow_redirects=True,
        headers={"User-Agent": "btc-5s-quant-engine/1.0"},
    ) as client:
        day = start
        while day <= end:
            try:
                await _import_one_day(
                    settings, client, symbol, day, cache, market, features,
                    writer, stats, progress,
                )
            except ImportError_ as exc:
                stats.skipped_days.append({"day": day.isoformat(), "reason": str(exc)})
                log.warning("import.day_failed", day=day.isoformat(), error=str(exc))
            except httpx.HTTPError as exc:
                stats.skipped_days.append(
                    {"day": day.isoformat(), "reason": f"download failed: {exc}"}
                )
            day += timedelta(days=1)

    if writer:
        await writer.flush()
    return stats


async def _import_one_day(
    settings: Settings,
    client: httpx.AsyncClient,
    symbol: str,
    day: date,
    cache: Path | None,
    market: ReplayMarket,
    features: FeatureEngine,
    writer: BatchWriter | None,
    stats: ImportStats,
    progress: Any,
) -> None:
    book_zip = await fetch_zip(client, archive_url(symbol, "bookTicker", day), cache)
    if book_zip is None:
        raise ImportError_(
            "no bookTicker archive for this date (too recent, or the symbol "
            "does not publish it)"
        )
    trade_zip = await fetch_zip(client, archive_url(symbol, "aggTrades", day), cache)
    stats.bytes_downloaded += len(book_zip) + (len(trade_zip) or 0 if trade_zip else 0)

    quotes = list(_parse_book_ticker(book_zip, symbol))
    if not quotes:
        raise ImportError_("bookTicker archive parsed to zero rows")
    trades = list(_parse_agg_trades(trade_zip, symbol)) if trade_zip else []

    _sanity_check(quotes, trades, day)

    merged = _merge_by_time(quotes, trades)
    interval = settings.feature_interval_ms
    next_feature_ts = 0

    for item in merged:
        if isinstance(item, BookTicker):
            tick = _tick_from_quote(item, market)
            market.last_tick = tick
            market.book.synced = True
            market.book.desync_reason = None
            features.ingest_tick(tick)
            stats.book_rows += 1
            if writer:
                writer.add(repo.MarketTickRow, _tick_row(tick))
        else:
            market.last_trade = item
            features.ingest_trade(item)
            stats.trade_rows += 1
            if writer:
                writer.add(repo.TradeRow, _trade_row(item))

        ts = item.server_ts
        stats.first_ts = stats.first_ts or ts
        stats.last_ts = ts

        # Emit a feature vector on the same cadence the live engine uses.
        if ts >= next_feature_ts and market.last_tick is not None:
            fv = features.compute(ts=ts)
            if fv is not None:
                fv["source"] = DataSource.REPLAY.value
                fv["is_synthetic"] = False
                stats.feature_rows += 1
                if writer:
                    writer.add(repo.FeatureRow, _feature_row(fv))
            next_feature_ts = ts + interval

        if writer and writer.pending > settings.db_batch_max_rows * 4:
            await writer.flush()

    if writer:
        await writer.flush()
    stats.days.append(day.isoformat())
    if progress:
        progress(day, stats)


def _parse_book_ticker(payload: bytes, symbol: str) -> Iterator[BookTicker]:
    for row in read_csv_rows(payload, BOOK_TICKER_COLUMNS):
        _validate_width(row, BOOK_TICKER_COLUMNS)
        try:
            # Older files omit event_time; fall back to transaction_time.
            ts = int(float(row[6])) if len(row) > 6 and row[6] else int(float(row[5]))
            yield BookTicker(
                exchange="binance_spot",
                symbol=symbol,
                bid_price=float(row[1]),
                bid_qty=float(row[2]),
                ask_price=float(row[3]),
                ask_qty=float(row[4]),
                exchange_ts=ts,
                server_ts=ts,
                update_id=int(float(row[0])),
                source=DataSource.REPLAY,
            )
        except (ValueError, IndexError) as exc:
            raise ImportError_(f"malformed bookTicker row {row[:8]}: {exc}") from exc


def _parse_agg_trades(payload: bytes, symbol: str) -> Iterator[Trade]:
    for row in read_csv_rows(payload, AGG_TRADE_COLUMNS):
        _validate_width(row, AGG_TRADE_COLUMNS)
        try:
            ts = int(float(row[5]))
            yield Trade(
                exchange="binance_spot",
                symbol=symbol,
                trade_id=int(float(row[0])),
                price=float(row[1]),
                quantity=float(row[2]),
                is_buyer_maker=str(row[6]).strip().lower() in ("true", "1"),
                exchange_ts=ts,
                server_ts=ts,
                source=DataSource.REPLAY,
            )
        except (ValueError, IndexError) as exc:
            raise ImportError_(f"malformed aggTrades row {row[:8]}: {exc}") from exc


def _sanity_check(
    quotes: list[BookTicker], trades: list[Trade], day: date
) -> None:
    """Refuse data that cannot be what it claims to be.

    A silently mis-parsed column would poison every downstream study, so the
    import fails loudly instead.
    """
    sample = quotes[: min(1000, len(quotes))]
    crossed = sum(1 for q in sample if q.bid_price >= q.ask_price)
    if crossed > len(sample) * 0.01:
        raise ImportError_(
            f"{crossed}/{len(sample)} sampled quotes are crossed (bid >= ask) - "
            "the column layout is probably not what we assumed"
        )
    if any(q.bid_price <= 0 or q.ask_qty < 0 for q in sample):
        raise ImportError_("non-positive prices or negative sizes in bookTicker")

    day_start = int(
        __import__("datetime").datetime(
            day.year, day.month, day.day,
            tzinfo=__import__("datetime").timezone.utc,
        ).timestamp() * 1000
    )
    day_end = day_start + 86_400_000
    outside = sum(1 for q in sample if not (day_start <= q.server_ts < day_end + 60_000))
    if outside > len(sample) * 0.01:
        raise ImportError_(
            f"{outside}/{len(sample)} timestamps fall outside {day} - the "
            "timestamp column is probably in the wrong unit or position"
        )
    if trades:
        tsample = trades[: min(1000, len(trades))]
        if any(t.price <= 0 or t.quantity <= 0 for t in tsample):
            raise ImportError_("non-positive price or quantity in aggTrades")


def _merge_by_time(
    quotes: list[BookTicker], trades: list[Trade]
) -> Iterator[BookTicker | Trade]:
    """Interleave both streams in timestamp order, quotes first on ties.

    Quotes win ties so a trade is always evaluated against a book state that
    already exists - the same ordering the live engine sees.
    """
    i = j = 0
    while i < len(quotes) and j < len(trades):
        if quotes[i].server_ts <= trades[j].server_ts:
            yield quotes[i]
            i += 1
        else:
            yield trades[j]
            j += 1
    while i < len(quotes):
        yield quotes[i]
        i += 1
    while j < len(trades):
        yield trades[j]
        j += 1


def _tick_from_quote(q: BookTicker, market: ReplayMarket) -> MarketTick:
    return MarketTick(
        exchange=q.exchange,
        symbol=q.symbol,
        ts=q.server_ts,
        exchange_ts=q.exchange_ts,
        bid_price=q.bid_price,
        bid_qty=q.bid_qty,
        ask_price=q.ask_price,
        ask_qty=q.ask_qty,
        mid=q.mid,
        micro_price=q.micro_price,
        spread=q.spread,
        spread_bps=q.spread_bps,
        last_price=market.last_trade.price if market.last_trade else None,
        # Replay carries no network latency: the venue timestamp is the only
        # clock there is. Reporting 0 is honest; inventing one would not be.
        latency_ms=0,
        book_synced=True,
        source=DataSource.REPLAY,
    )


def _tick_row(tick: MarketTick) -> dict:
    return {
        "ts": tick.ts, "exchange_ts": tick.exchange_ts, "latency_ms": 0,
        "exchange": tick.exchange, "symbol": tick.symbol,
        "bid_price": tick.bid_price, "bid_qty": tick.bid_qty,
        "ask_price": tick.ask_price, "ask_qty": tick.ask_qty, "mid": tick.mid,
        "micro_price": tick.micro_price, "spread": tick.spread,
        "spread_bps": tick.spread_bps, "last_price": tick.last_price,
        "book_synced": True, "source": DataSource.REPLAY.value,
        "is_synthetic": False,
    }


def _trade_row(t: Trade) -> dict:
    return {
        "ts": t.server_ts, "exchange_ts": t.exchange_ts, "latency_ms": 0,
        "exchange": t.exchange, "symbol": t.symbol, "trade_id": t.trade_id,
        "price": t.price, "quantity": t.quantity, "notional": t.notional,
        "is_buyer_maker": t.is_buyer_maker, "aggressor": t.aggressor.value,
        "source": DataSource.REPLAY.value, "is_synthetic": False,
    }


def _feature_row(fv: dict) -> dict:
    import math

    payload = {
        k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
        for k, v in fv["features"].items()
    }
    f = fv["features"]
    return {
        "ts": fv["ts"], "exchange": fv["exchange"], "symbol": fv["symbol"],
        "mid": f.get("mid") or 0.0, "micro_price": f.get("micro_price") or 0.0,
        "spread_bps": f.get("spread_bps") or 0.0,
        "book_synced": bool(f.get("book_synced")),
        "data_quality": f.get("data_quality") or 0.0,
        "regime": None, "payload": payload,
        "source": DataSource.REPLAY.value, "is_synthetic": False,
    }


async def purge_replay() -> dict[str, int]:
    """Remove imported rows, leaving live-recorded data untouched."""
    from sqlalchemy import delete

    from app.db.engine import session_scope

    deleted: dict[str, int] = {}
    async with session_scope() as s:
        for table in (repo.MarketTickRow, repo.TradeRow, repo.FeatureRow):
            res = await s.execute(delete(table).where(table.source == "REPLAY"))
            deleted[table.__tablename__] = res.rowcount or 0
    return deleted
