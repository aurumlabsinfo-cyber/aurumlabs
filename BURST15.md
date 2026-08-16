# AURUM BURST-15

A 15-minute operating window over a 5-second horizon.

```bash
SIGNAL_STRATEGY=burst15
```

---

## The idea

It does not try to predict where price will be in fifteen minutes. With this
data that is not predictable, and pretending otherwise is how a system ends up
confidently wrong. Instead it opens a fifteen-minute **window** and, inside it,
takes only **bursts of tape** on a five-second horizon.

The window is the risk container. The burst is the trade.

## The trigger

Evaluated on every feature vector (10/s by default):

| Condition | Setting | Default |
|---|---|---|
| trades in the last 5s | `BURST_N5_MIN` | 40 |
| \|10-second return\| in bps | `BURST_R10_MIN_BPS` | 0.5 |
| order flow agrees with the move | `BURST_REQUIRE_OFI_AGREE` | true |

Direction is the **sign of the 10-second return** — this is a momentum rule,
not a fade. Order flow is measured as notional imbalance over 5 seconds
(`ofi_notional_5s`), so one 10 BTC print outweighs two hundred dust prints;
requiring it to point the same way as the move is what separates a burst from
a wick.

## The entry is time-based

This is the one structural difference from the ensemble path, and it matters:

* **ensemble** — the signal names a trigger *price*, and the countdown starts
  only when the market trades there. If price never comes, the signal expires
  unfilled.
* **burst15** — the signal enters `BURST_ENTRY_DELAY_MS` (default 1000ms) after
  the trigger, at whatever the market is then. Expiry is entry + horizon.

Nothing waits for a level, because the whole premise is that the move is
already happening.

## The session

Not optional, and not decoration. A session closes early on any of these:

| Rule | Setting | Default |
|---|---|---|
| window length | `BURST_SESSION_S` | 900s |
| gap between entries | `BURST_COOLDOWN_MS` | 6000 |
| trades per session | `BURST_MAX_TRADES_SESSION` | 40 |
| session stop-loss | `BURST_STOP_LOSS_UNITS` | -6 units |
| session take-profit | `BURST_TAKE_PROFIT_UNITS` | +15 units |

A session that stops out **stays** closed until its window would have ended
anyway. Reopening immediately would turn a stop-loss into a suggestion. With
`BURST_AUTO_RESTART=true` the next window opens on its own after that.

Only one trade is open at a time: with a 5-second horizon and a 6-second
cooldown, overlapping entries would double the position without any rule ever
saying so.

P&L is in stake units on **paper trades only**. `BINARY_PAYOUT` is used when
set; when it is not, the session's own stop-loss arithmetic falls back to
`BURST_ASSUMED_PAYOUT` and every response says `payout_is_assumed: true`. The
reported monetary P&L elsewhere keeps saying `PAYOUT UNKNOWN` rather than
inventing a number.

## Guards

The trigger is never even evaluated when the data cannot support it:
`BURST_MAX_STALENESS_MS` (feed age) and `BURST_MIN_DATA_QUALITY`. A stale feed
produces NO TRADE, not a guess.

---

## Running it

### Live (paper)

```bash
SIGNAL_STRATEGY=burst15 docker compose up -d --build

curl localhost:8000/burst/session      # window state, P&L, trades
curl localhost:8000/diagnostics        # what is blocking, ranked
curl localhost:8000/signals/current    # the current signal
```

The window can be driven by hand (admin key required):

```bash
curl -XPOST localhost:8000/burst/session/start -H "X-API-Key: $ADMIN_API_KEY"
curl -XPOST localhost:8000/burst/session/stop  -H "X-API-Key: $ADMIN_API_KEY"
```

### Offline, on recorded data

```bash
# the whole strategy - sessions, cooldown, stop-loss - replayed
docker compose exec backend python -m app.ml.cli burst --summary

# the threshold sweep the original script called `grid`
docker compose exec backend python -m app.ml.cli burst-grid

# the entry rule alone, on purged walk-forward folds, next to every other rule
docker compose exec backend python -m app.ml.cli strategies --horizon 5
```

`burst` resolves entries and expiries from the recorded tick series. A row
whose future is not recorded is dropped rather than forward-filled, so the
report never contains a fill the data cannot support.

Get real data first — the archive importer downloads Binance's own published
`bookTicker` and `aggTrades` and replays them through this same feature engine:

```bash
docker compose exec backend python -m app.ml.cli import --start 2026-08-01 --days 7
```

### Before you believe a good number

`burst-grid` tries 40 parameter combinations. The best of 40 looks impressive
on pure noise — that is what a search does. Run it through the multiple-testing
correction before drawing any conclusion:

```bash
docker compose exec backend python -m app.ml.cli search --horizon 5 --payout 0.8 --summary
```

See [STRATEGY.md](STRATEGY.md) for what that correction does and why an
uncorrected grid result is worth nothing.

---

## Where it lives in the code

| File | Role |
|---|---|
| `strategies/aurum_burst15.py` | the original standalone script, unchanged |
| `backend/app/signals/burst.py` | the live strategy: trigger, session, guards |
| `backend/app/ml/burst_backtest.py` | offline replay, sessions included |
| `backend/app/ml/strategies.py::burst15` | the entry rule as a research candidate |
| `backend/app/features/engine.py` | `return_10000ms`, `ofi_notional_5s`, `trade_count_5s` |
| `backend/tests/test_burst.py` | the rules above, pinned |

The live path and the research path read the same three features by name
(`N5_FEATURE`, `R10_FEATURE`, `OFI_FEATURE` in `burst.py`) so a change to one
cannot silently diverge from the other.

## What this does not claim

BURST-15 is a rule, not a model. It does not produce a calibrated probability,
and the `confidence` it reports is a measure of how far past its thresholds the
trigger fired — useful for ranking and for the calibration report, not a
frequency. Whether it has an edge on your data is an empirical question, and
the tooling above exists to answer it honestly, including with "no".
