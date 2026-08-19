#!/usr/bin/env python3
"""Generate a replay file for offline development and testing.

READ THIS BEFORE TRUSTING ANY NUMBER PRODUCED FROM ITS OUTPUT.

The data this writes is **synthetic**.  It is not a market, it is not a
forecast, and an edge discovered in it is a property of the generator, not of
crypto.  It exists for exactly two purposes:

1.  Exercising the pipeline end to end — books, sequencing, features, research,
    validation, execution, wallet, API — without a venue connection.
2.  Providing a deterministic input so a regression in any of those layers shows
    up as a changed number rather than as "the market was different today".

This file lives outside the ``aurum`` package on purpose.  Nothing in the
runtime imports it, and ``app.env=production`` refuses to read a replay feed at
all, so there is no path by which generated data can reach a production run.

The generator does plant one real structure: BTCUSDT leads the alts by a
configurable lag with a configurable beta.  That is what lets the cross-crypto
agent and the validation lab be tested for *both* outcomes — finding a
relationship that is genuinely there, and rejecting the ones that are not.

    python3 tools/make_replay.py --minutes 20 --out data/replay.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import random
from dataclasses import dataclass, field
from pathlib import Path

TICK = {
    "BTCUSDT": 0.1, "ETHUSDT": 0.01, "BNBUSDT": 0.01, "SOLUSDT": 0.01, "XRPUSDT": 0.0001,
    "DOGEUSDT": 0.00001, "ADAUSDT": 0.0001, "LINKUSDT": 0.001, "AVAXUSDT": 0.001, "TRXUSDT": 0.00001,
}
START_PRICE = {
    "BTCUSDT": 61000.0, "ETHUSDT": 2400.0, "BNBUSDT": 560.0, "SOLUSDT": 145.0, "XRPUSDT": 0.52,
    "DOGEUSDT": 0.115, "ADAUSDT": 0.38, "LINKUSDT": 11.5, "AVAXUSDT": 22.0, "TRXUSDT": 0.125,
}
LOT = {
    "BTCUSDT": 0.001, "ETHUSDT": 0.01, "BNBUSDT": 0.01, "SOLUSDT": 0.1, "XRPUSDT": 1.0,
    "DOGEUSDT": 10.0, "ADAUSDT": 1.0, "LINKUSDT": 0.1, "AVAXUSDT": 0.1, "TRXUSDT": 10.0,
}

#: Followers copy BTC's return from ``lag_ms`` ago, scaled by ``beta``.
FOLLOWERS = {
    "ETHUSDT":  (250, 0.55), "BNBUSDT": (500, 0.35), "SOLUSDT": (750, 0.60),
    "XRPUSDT":  (1000, 0.30), "DOGEUSDT": (500, 0.75), "ADAUSDT": (2000, 0.25),
    "LINKUSDT": (750, 0.45), "AVAXUSDT": (500, 0.55), "TRXUSDT": (3000, 0.15),
}


@dataclass
class SymbolSim:
    symbol: str
    price: float
    tick: float
    lot: float
    vol: float
    update_id: int = 1000
    spread_mult: float = 1.0
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    returns: list[tuple[int, float]] = field(default_factory=list)
    last_snapshot_id: int = 0

    def round_price(self, price: float) -> float:
        return round(round(price / self.tick) * self.tick, 10)

    def seed_book(self, rng: random.Random, levels: int = 25) -> None:
        self.bids.clear()
        self.asks.clear()
        mid = self.price
        half = max(self.tick, self.round_price(mid * rng.uniform(0.4, 1.2) / 10_000))
        for i in range(levels):
            bid = self.round_price(mid - half - i * self.tick)
            ask = self.round_price(mid + half + i * self.tick)
            size = self.lot * rng.uniform(5, 60) * (1.0 + i * 0.15)
            self.bids[bid] = round(size, 6)
            self.asks[ask] = round(self.lot * rng.uniform(5, 60) * (1.0 + i * 0.15), 6)

    def reprice(self, rng: random.Random, levels: int = 25) -> list[tuple[str, float, float]]:
        """Move the book toward the current mid and jiggle a few resting sizes.

        Incremental on purpose: a real diff stream touches a handful of levels
        per event, and regenerating all fifty every 100 ms would produce a file
        an order of magnitude larger that also teaches the liquidity features
        nothing — every level would look like it was added and removed at once.
        """
        changes: list[tuple[str, float, float]] = []
        mid = self.price
        half = max(self.tick, self.round_price(mid * self.spread_mult / 10_000))

        wanted_best_bid = self.round_price(mid - half)
        wanted_best_ask = self.round_price(mid + half)

        # Retire levels that the move left on the wrong side, and open new ones
        # at the new inside price.
        for price in [p for p in self.bids if p >= wanted_best_ask]:
            changes.append(("b", price, 0.0))
            del self.bids[price]
        for price in [p for p in self.asks if p <= wanted_best_bid]:
            changes.append(("a", price, 0.0))
            del self.asks[price]

        for i in range(levels):
            bid = self.round_price(wanted_best_bid - i * self.tick)
            if bid not in self.bids:
                size = round(self.lot * rng.uniform(5, 60) * (1.0 + i * 0.15), 6)
                self.bids[bid] = size
                changes.append(("b", bid, size))
            ask = self.round_price(wanted_best_ask + i * self.tick)
            if ask not in self.asks:
                size = round(self.lot * rng.uniform(5, 60) * (1.0 + i * 0.15), 6)
                self.asks[ask] = size
                changes.append(("a", ask, size))

        # Trim the tails so the book does not grow without bound.
        for price in sorted(self.bids, reverse=True)[levels:]:
            changes.append(("b", price, 0.0))
            del self.bids[price]
        for price in sorted(self.asks)[levels:]:
            changes.append(("a", price, 0.0))
            del self.asks[price]

        # A couple of resting sizes churn each step: this is what order-flow and
        # liquidity features actually read.
        for _ in range(rng.randint(0, 3)):
            side = "b" if rng.random() < 0.5 else "a"
            book = self.bids if side == "b" else self.asks
            if not book:
                continue
            price = rng.choice(sorted(book, reverse=(side == "b"))[:8])
            size = round(max(self.lot, book[price] * rng.uniform(0.4, 1.8)), 6)
            book[price] = size
            changes.append((side, price, size))

        self.spread_mult = min(2.5, max(0.35, self.spread_mult * rng.uniform(0.94, 1.06)))
        return changes

    def snapshot_payload(self, depth: int = 25) -> dict:
        bids = sorted(self.bids.items(), key=lambda kv: -kv[0])[:depth]
        asks = sorted(self.asks.items(), key=lambda kv: kv[0])[:depth]
        return {
            "lastUpdateId": self.update_id,
            "bids": [[p, q] for p, q in bids],
            "asks": [[p, q] for p, q in asks],
        }


def generate(
    out: Path,
    *,
    minutes: float,
    seed: int,
    symbols: list[str],
    step_ms: int = 100,
    snapshot_every_s: float = 60.0,
) -> dict:
    rng = random.Random(seed)
    sims: dict[str, SymbolSim] = {}
    for symbol in symbols:
        sims[symbol] = SymbolSim(
            symbol=symbol,
            price=START_PRICE.get(symbol, 100.0),
            tick=TICK.get(symbol, 0.01),
            lot=LOT.get(symbol, 1.0),
            vol=rng.uniform(0.00006, 0.00016),
        )
        sims[symbol].seed_book(rng)

    start_ms = 1_760_000_000_000  # fixed epoch so replays are byte-identical
    total_steps = int(minutes * 60 * 1000 / step_ms)
    leader = "BTCUSDT" if "BTCUSDT" in sims else symbols[0]

    counts = {"depth": 0, "trade": 0, "book_ticker": 0, "mark_price": 0, "snapshot": 0}
    out.parent.mkdir(parents=True, exist_ok=True)

    with out.open("w", encoding="utf-8") as fh:
        fh.write(
            json.dumps(
                {
                    "aurum_replay": 1,
                    "synthetic": True,
                    "warning": "SYNTHETIC DATA — not a market. Any edge found here is an artifact.",
                    "seed": seed,
                    "symbols": symbols,
                    "step_ms": step_ms,
                    "planted_structure": {
                        "leader": leader,
                        "followers": {k: {"lag_ms": v[0], "beta": v[1]} for k, v in FOLLOWERS.items()
                                      if k in sims},
                    },
                }
            )
            + "\n"
        )

        def write(record: dict) -> None:
            fh.write(json.dumps(record) + "\n")

        for symbol, sim in sims.items():
            write({"symbol": symbol, "kind": "snapshot", "ts_ms": start_ms - 1,
                   "payload": sim.snapshot_payload()})
            sim.last_snapshot_id = sim.update_id
            counts["snapshot"] += 1

        burst_until = 0
        for step in range(total_steps):
            ts = start_ms + step * step_ms

            # --- leader path: random walk with occasional volatility bursts ---
            lead = sims[leader]
            if step > 0 and rng.random() < 0.002:
                burst_until = ts + rng.randint(3_000, 12_000)
            scale = 3.0 if ts < burst_until else 1.0
            drift = rng.gauss(0.0, lead.vol * scale)
            lead.price = lead.round_price(lead.price * (1.0 + drift))
            lead.returns.append((ts, drift))

            # --- followers: lagged copy of the leader plus their own noise ---
            for symbol, sim in sims.items():
                if symbol == leader:
                    continue
                lag_ms, beta = FOLLOWERS.get(symbol, (0, 0.0))
                copied = 0.0
                if lag_ms:
                    target = ts - lag_ms
                    for rts, rval in reversed(lead.returns):
                        if rts <= target:
                            copied = rval if rts > target - step_ms else 0.0
                            break
                own = rng.gauss(0.0, sim.vol * scale)
                move = beta * copied + own
                sim.price = sim.round_price(sim.price * (1.0 + move))
                sim.returns.append((ts, move))

            # Keep the leader's return history bounded to the deepest lag.
            if len(lead.returns) > 400:
                del lead.returns[:200]

            # --- emit per-symbol events ---
            for symbol, sim in sims.items():
                changes = sim.reprice(rng, levels=25)
                if changes:
                    first = sim.update_id + 1
                    prev = sim.update_id
                    sim.update_id += max(1, len(changes) // 4)
                    bids = [[p, q] for side, p, q in changes if side == "b"]
                    asks = [[p, q] for side, p, q in changes if side == "a"]
                    write({
                        "symbol": symbol, "kind": "depth", "ts_ms": ts,
                        "payload": {"U": first, "u": sim.update_id, "pu": prev, "b": bids, "a": asks},
                    })
                    counts["depth"] += 1

                best_bid = max(sim.bids) if sim.bids else sim.price
                best_ask = min(sim.asks) if sim.asks else sim.price
                write({
                    "symbol": symbol, "kind": "book_ticker", "ts_ms": ts,
                    "payload": {"bid": best_bid, "bid_qty": sim.bids.get(best_bid, sim.lot),
                                "ask": best_ask, "ask_qty": sim.asks.get(best_ask, sim.lot),
                                "update_id": sim.update_id},
                })
                counts["book_ticker"] += 1

                # Trades: arrival rate and aggressor side both follow the move,
                # which is what gives order-flow features something to measure.
                recent = sim.returns[-1][1] if sim.returns else 0.0
                intensity = 0.35 + min(0.5, abs(recent) / (sim.vol * 4.0)) * 0.5
                if rng.random() < intensity:
                    p_buy = 0.5 + max(-0.35, min(0.35, recent / (sim.vol * 6.0)))
                    aggressor = "BUY" if rng.random() < p_buy else "SELL"
                    price = best_ask if aggressor == "BUY" else best_bid
                    qty = round(sim.lot * rng.uniform(1, 40), 6)
                    write({
                        "symbol": symbol, "kind": "trade", "ts_ms": ts,
                        "payload": {"price": price, "qty": qty, "aggressor": aggressor,
                                    "trade_id": step * 100 + len(counts)},
                    })
                    counts["trade"] += 1

                if step % 10 == 0:
                    write({
                        "symbol": symbol, "kind": "mark_price", "ts_ms": ts,
                        "payload": {
                            "mark_price": sim.round_price(sim.price * (1.0 + rng.gauss(0, 0.00002))),
                            "index_price": sim.round_price(sim.price * (1.0 + rng.gauss(0, 0.00003))),
                            "settlement_price": sim.price,
                            "funding_rate": round(rng.gauss(0.0001, 0.00005), 8),
                            "next_funding_ms": ts + 3_600_000,
                        },
                    })
                    counts["mark_price"] += 1

                if step_ms * step % int(snapshot_every_s * 1000) == 0 and step > 0:
                    write({"symbol": symbol, "kind": "snapshot", "ts_ms": ts,
                           "payload": sim.snapshot_payload()})
                    counts["snapshot"] += 1

    return {
        "path": str(out),
        "records": sum(counts.values()),
        "counts": counts,
        "minutes": minutes,
        "symbols": len(symbols),
        "size_mb": round(out.stat().st_size / (1024 * 1024), 2),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, default=Path("data/replay.jsonl"))
    parser.add_argument("--minutes", type=float, default=20.0)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--step-ms", type=int, default=100)
    parser.add_argument("--symbols", default=",".join(START_PRICE))
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    report = generate(args.out, minutes=args.minutes, seed=args.seed, symbols=symbols, step_ms=args.step_ms)
    print(json.dumps(report, indent=2))
    print("\nSYNTHETIC DATA. Any edge found in this file is an artifact of the generator.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
