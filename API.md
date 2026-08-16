# API

Base URL `http://localhost:8000`. Interactive docs at `/docs`.

All timestamps are epoch **milliseconds**. Every response that describes market
state carries `is_synthetic` and `source`.

Read endpoints are public — the underlying market data is public. Endpoints
that change engine behaviour require the header `X-API-Key: $ADMIN_API_KEY`.
When `ADMIN_API_KEY` is unset those endpoints return `503` and stay disabled.

Rate limit: `RATE_LIMIT_REQUESTS` per `RATE_LIMIT_WINDOW_S` per client IP
(default 120/60s), `429` with `Retry-After` when exceeded. `/health*` is exempt.

---

## System

### `GET /health`
Full component health: `websocket`, `api`, `database`, `market_data`,
`order_book`, `model`, `latency`, `error_rate`, plus market counters, latency
percentiles, order-book stats, per-adapter connection state and the
data-quality breakdown.

```jsonc
{
  "status": "HEALTHY",
  "source": "LIVE",
  "is_synthetic": false,
  "synthetic_warning": null,
  "components": { "order_book": { "status": "UP", "detail": { "synced": true, "resyncs": 0 } } },
  "market": {
    "feed_age_ms": 84,
    "tick_latency_ms": { "p50": 41, "p95": 96, "max": 210 },
    "data_quality": { "score": 1.0, "ok": true, "reasons": [], "warmup_complete": true }
  }
}
```

### `GET /health/live` · `GET /health/ready`
Liveness (always cheap) and readiness (has data, quality above zero).

### `GET /settings`
Runtime-mutable settings and static configuration. **Never** returns secrets,
the database URL or the admin key.

### `PATCH /settings` 🔒
```json
{ "key": "signal_min_confidence", "value": 0.65 }
```
Only keys in the allow-list are accepted (`signal_*`, `trigger_*`,
`max_spread_bps`, `max_latency_ms`, `min_data_quality`, `binary_payout`,
`paper_stake`). Anything else returns `400`.

### `POST /admin/purge-synthetic` 🔒
Deletes every simulator-produced row. Live rows are untouched.

---

## Market data

### `GET /market`
Consolidated top of book: `price` (mid), `bid`/`ask` with sizes, `spread`,
`spread_bps`, `micro_price`, `last_price`, `latency_ms`, `book_synced`,
derivatives (funding, open interest, mark/index) when a futures feed is
configured, and recent liquidations.

### `GET /orderbook?levels=20`
Live local book plus its integrity statistics:

```jsonc
{
  "synced": true,
  "desync_reason": null,
  "last_update_id": 74839201,
  "bids": [[104515.8, 1.23]],
  "asks": [[104516.3, 0.87]],
  "stats": { "applied_updates": 48213, "gaps_detected": 1, "resyncs": 1, "dropped_stale": 902 }
}
```

`levels` is bounded 1–200; out-of-range returns `422`.

### `GET /trades?limit=50`
Recent trades with `side` (the aggressor), `notional` and per-trade latency.

### `GET /features?history=0`
The latest causal feature vector; `history=N` also returns the last N persisted
vectors. Values that cannot be computed yet are `null`, never `0`.

### `GET /candles?interval=1m&limit=300`

OHLC bars aggregated in PostgreSQL from the recorded mid-price ticks.
`interval` is one of `1s 5s 15s 1m 5m 10m 1h`; `limit` is 10–1500 buckets.
`time` is in **seconds** (what lightweight-charts expects), not milliseconds.

The dashboard uses this for chart history and then folds live WebSocket ticks
onto the same bucket boundaries, so the newest bar continues the series rather
than starting a duplicate one. A bar only covers time this engine was actually
running — gaps are real downtime, not missing data.

### `GET /exchanges`
Available adapters, which are configured, which is primary, and each one's
declared capabilities.

---

## Analysis

### `GET /agents`
All eight agents for the most recent evaluation, each with `direction`,
`confidence`, `score`, `reason`, `features_used`, `timestamp`, `data_quality`;
plus the regime, `prob_up` / `prob_down` / `prob_neutral` (they sum to 1) and
the current `no_trade_reasons`.

### `GET /signals?limit=50`
Live active signals, recently settled ones, engine counters, the last decision
and the persisted signal rows.

### `GET /signals/current`
The single current signal plus market state — the endpoint the card would use
if you were not on the WebSocket.

---

## Paper trading

### `GET /paper-trading?limit=100&result=WIN`
Recorded paper trades. `mode` states plainly that no order is ever sent.

### `GET /statistics?include_synthetic=false&window_hours=24`
Everything: totals, CALL/PUT/NO TRADE counts, wins/losses/ties, win rate with a
95% Wilson interval and a binomial p-value, break-even win rate, expected
value, profit factor, max drawdown, streaks, average confidence, and
breakdowns by regime, direction, confidence bucket, volatility bucket and hour.

With no payout configured, `pnl_units`, `expected_value_per_trade` and
`break_even_win_rate` are `null` and `payout_status` is `"PAYOUT UNKNOWN"`.

### `GET /shadow?include_synthetic=false`

Accuracy of the engine's lean on **every evaluated window**, not only the ones
that became signals — including the ones a gate rejected, split by which gate
rejected them.

`signals` is a biased sample: it is the set of moments the engine already
liked. This endpoint is how you tell whether a gate is discarding noise or
discarding information. Compare `emitted` against `blocked`; `gate_value`
states the difference and warns that overlapping windows make the intervals
narrower than the truth.

A win rate here is the accuracy of the lean, **not a tradable result**: no
trigger had to be reached and no payout is applied.

### `GET /retrain` · `POST /retrain` 🔒

Status of the periodic walk-forward retraining loop, and a way to force one
cycle now. Forcing the schedule does not force the outcome: a model is
activated only when its edge classifies as PROVEN or PROMISING, and a run
concluding NO ROBUST EDGE leaves the live engine exactly as it was.

Disabled by default (`AUTO_RETRAIN_ENABLED=false`) because it replaces the
live model without a human reading the report first.

### `GET /statistics/calibration`
Per-confidence-bucket observed win rate, calibration error, Brier score and
expected calibration error. This is how you find out whether "80%" means 80%.

### `GET /statistics/montecarlo?simulations=10000&bankroll_units=20`
Bootstrap distributions of final P&L, max drawdown, longest losing streak,
probability of loss, risk of ruin and the Kelly fraction. Without a payout only
the streak and win-count distributions are returned.

---

## Research

### `GET /backtest`
Whether a run is in progress, how much data is recorded, whether that is enough
for a verdict, and the last completed report.

### `POST /backtest` 🔒
```json
{
  "horizons": [1, 2, 3, 5, 10, 15, 30],
  "models": ["logistic_regression", "xgboost", "lightgbm"],
  "n_splits": 5,
  "include_synthetic": false,
  "save_best": false
}
```
Runs asynchronously; poll `GET /backtest`. `409` if one is already running.

### `GET /models`
Available algorithms, the active model (with its out-of-distribution
threshold), and every recorded model version with its metrics and edge
classification.

### `POST /models/activate` 🔒 · `POST /models/deactivate` 🔒
Load or unload a trained model. With no model active, decisions come from the
rule ensemble alone.

---

## WebSocket streams

| Endpoint | Payload |
|---|---|
| `/ws/market` | `tick` and `trade` frames |
| `/ws/orderbook` | `orderbook` snapshots, throttled to 4/s |
| `/ws/signals` | `snapshot` then every lifecycle event |
| `/ws/dashboard` | everything the UI needs on one socket |

Every frame carries `server_ts`; a `heartbeat` frame arrives every few seconds
so clients can keep their clock offset fresh even when the market is quiet.

Signal events, in lifecycle order:

```
signal_created → trigger_hit → trade_active → trade_expired → signal_settled
                                          (or signal_cancelled)
```

Example `trade_active` payload:

```jsonc
{
  "type": "signal",
  "server_ts": 1786620000123,
  "data": {
    "event": "trade_active",
    "signal": {
      "signal_id": "9f2c…", "direction": "DOWN", "status": "ACTIVE",
      "trigger_price": 104515.8, "entry_price": 104515.8,
      "triggered_at": 1786620000100, "expires_at": 1786620005100,
      "horizon_s": 5.0, "confidence": 0.82, "remaining_ms": 4977
    }
  }
}
```

`remaining_ms` is authoritative **at `server_ts`**. Clients should recompute it
as `expires_at - (Date.now() + clockOffset)` rather than trusting a local timer.

Connections are capped at `WS_MAX_CONNECTIONS` (close code `1013` beyond that).
Slow consumers lose their oldest queued frames rather than stalling the engine.

---

## Errors

| Code | Meaning |
|---|---|
| `422` | Parameter failed validation |
| `401` | Missing or wrong `X-API-Key` |
| `503` | Admin endpoints disabled (`ADMIN_API_KEY` unset) |
| `409` | A backtest is already running |
| `429` | Rate limit exceeded |
| `500` | Unhandled error — logged server-side, no stack trace in the response |
