# Architecture

## Data flow

```
        EXCHANGE (public feeds)
               │
     ┌─────────┴──────────┐
     │  WebSocket   REST  │   adapters: binance_spot, binance_futures,
     │  (stream)  (sync)  │             coinbase, synthetic
     └─────────┬──────────┘
               ▼
      MARKET DATA ENGINE          canonical types, latency + clock skew,
               │                  health, data-quality scoring
               ▼
      LOCAL ORDER BOOK            snapshot + diffs, sequence validation,
               │                  gap detection, automatic resync
               ▼
       FEATURE ENGINE             ~60 causal features every 100 ms
               │
      ┌────────┴────────┐
      ▼                 ▼
   DATABASE        MULTI-AGENT ENGINE       8 agents
  (batched)               │
                          ▼
                  DECISION ENGINE           gates → P(UP)/P(DOWN)/P(NEUTRAL)
                          │                 → NO TRADE or a trigger price
                          ▼
                  SIGNAL LIFECYCLE          WAITING → TRIGGERED → ACTIVE
                          │                 → EXPIRED → WIN/LOSS
                          ▼
                  PAPER TRADING + STATS
                          │
                          ▼
                  WEBSOCKET FAN-OUT → BROWSER
```

Everything is `asyncio`. One process, cooperative tasks, no threads in the hot
path.

## Why an event bus

The WebSocket reader must never block on a slow consumer — not the database,
not a browser socket. `app/core/bus.py` gives every subscriber a bounded queue
and **drops the oldest message** when a consumer falls behind, counting the
drop. A stalled dashboard tab degrades that tab, not the engine.

Consequence: the persistence layer and the UI can lag; the order book and the
signal engine cannot.

## Module map

| Path | Responsibility |
|---|---|
| `app/config.py` | All settings, from environment only |
| `app/core/bus.py` | Bounded pub/sub with drop-oldest back-pressure |
| `app/core/clock.py` | Epoch-ms helpers; exchange vs server timestamps |
| `app/marketdata/base.py` | `ExchangeAdapter` ABC + reconnect loop |
| `app/marketdata/binance.py` | Spot and USD-M futures public feeds |
| `app/marketdata/coinbase.py` | Second venue, proving the abstraction |
| `app/marketdata/synthetic.py` | Offline simulator (flagged everywhere) |
| `app/marketdata/orderbook.py` | Sequence-validated local book |
| `app/marketdata/engine.py` | Orchestration, health, data quality |
| `app/features/rolling.py` | Ring buffers, time-indexed lookup, bars |
| `app/features/engine.py` | The causal feature vector |
| `app/agents/catalog.py` | The eight agents |
| `app/signals/decision.py` | Gates, probability blending, trigger sizing |
| `app/signals/lifecycle.py` | Trigger detection, countdown, settlement |
| `app/signals/statistics.py` | Win rate, EV, break-even, calibration |
| `app/ml/*` | Dataset, walk-forward, leakage, strategies, verdicts |
| `app/db/*` | SQLAlchemy models, batched writer, queries |
| `app/api/*` | REST routes, WebSocket streams, security middleware |
| `app/services/container.py` | Composition root |

## The local order book

Binance's documented diff-depth procedure, generalised:

1. buffer incoming diffs;
2. fetch the REST snapshot, note `lastUpdateId`;
3. drop buffered diffs with `u <= lastUpdateId`;
4. the first applied diff must satisfy `U <= lastUpdateId + 1 <= u`;
5. each later diff must be contiguous (`U == prev_u + 1`, or `pu == prev_u`
   where the venue supplies it);
6. any violation → mark desynced, refetch the snapshot.

Extra guard: if applying a diff ever produces a **crossed** book
(`best_bid >= best_ask`), the state is wrong by definition, so the book
desyncs rather than publishing it. This caught a genuine bug during
development.

While `synced == false`, `data_quality` is 0 and the decision engine emits NO
TRADE. There is no "best effort" mode.

## Causality

The feature engine only ever reads data with a timestamp at or before the
vector's own timestamp:

* `TimeSeries.value_ago()` returns `None` when the history does not reach back
  far enough — it never extrapolates;
* indicators run on **closed** bars;
* unavailable values stay `None` all the way into JSONB, so a model can tell
  "no data" apart from "zero".

Labels are attached later, offline, in `app/ml/dataset.py`. That separation is
what makes the recorded `features` table usable for honest supervised learning.

## Timestamps

Three clocks, all recorded:

| Field | Meaning |
|---|---|
| `exchange_ts` | the venue's own timestamp on the message |
| `server_ts` | when this process received it |
| `latency_ms` | `server_ts - exchange_ts` |

`latency_ms` includes any clock skew between venue and host, so the engine
independently estimates that skew from the venue's REST time endpoint
(round-trip corrected) and reports it per adapter in `/health`.

## Signal lifecycle, precisely

```
create        status=WAITING     triggered_at=null   expires_at=null
              trigger_price = ref ± k·σ(horizon), tick-rounded

on each tick  if (UP and price >= trigger) or (DOWN and price <= trigger):
                  triggered_at = server now
                  expires_at   = triggered_at + horizon_s·1000
                  entry_price  = the price actually observed at the touch
                  status: TRIGGERED → ACTIVE

at expiry     expiry_price = current mid
              WIN  if the move matched the direction
              LOSS if it went the other way
              TIE  if the price is exactly unchanged

no touch      status=CANCELLED after SIGNAL_WAIT_TIMEOUT_S — not a loss
```

The monitor loop runs at 20 ms independently of the browser. The client
receives `triggered_at`, `expires_at` and `server_ts` on every frame, tracks its
offset from the server clock, and renders the remaining time. It never decides
a state transition.

## Database schema

Twelve tables: `market_ticks`, `trades`, `order_book_snapshots`,
`order_book_updates`, `features`, `signals`, `paper_trades`,
`agent_predictions`, `model_versions`, `system_events`, `errors`,
`performance_metrics`.

Conventions:

* `ts` is epoch milliseconds (BIGINT, indexed) with a `created_at TIMESTAMPTZ`
  mirror for human queries;
* every table that can hold non-live data carries `source` and `is_synthetic`,
  and the research tooling filters on them;
* the full feature vector lives in a JSONB `payload`, so adding a feature does
  not require a migration;
* `order_book_updates` stores the raw sequenced diffs — including the ones that
  failed validation, flagged `gap_detected` — so book research can replay
  exactly what the engine saw. It is very high volume, so it is opt-in via
  `PERSIST_BOOK_UPDATES`;
* `performance_metrics` snapshots paper-trading performance on a timer, giving
  a history of how the measured edge evolves rather than only its latest value;
* `model_versions` records every saved model with its data window, validation
  metrics and edge verdict, so an activated model can always be traced back;
* writes are batched (default 500 ms) — the feed never waits on the database,
  and a failed flush re-queues its rows instead of dropping them.

## Adding an exchange

1. implement `ExchangeAdapter` (`_stream_once`, `fetch_depth_snapshot`,
   `fetch_server_time`), translating to the canonical types;
2. declare its `capabilities` honestly — advertise `DEPTH_DIFF` only if the
   venue really publishes sequenced diffs on a public channel;
3. register it in `app/marketdata/registry.py`;
4. add it to `EXCHANGES`. The first entry is the price authority; the rest are
   auxiliary (e.g. `binance_futures` for funding, open interest and
   liquidations).

Nothing downstream changes.
