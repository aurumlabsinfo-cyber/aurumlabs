# BTC 5-Second Quant Engine

Live BTC market data → local order book → microstructure features → multi-agent
analysis → **paper-only** 5-second directional signals with a backend-verified
trigger and countdown.

**PAPER TRADING ONLY.** No code path in this repository can place an order on
any venue. There is no private API key, no signing, no order endpoint.

---

## What it actually claims

| Claim | Status |
|---|---|
| Real-time public market data (WebSocket + REST) | Implemented |
| Sequence-validated local order book with auto-resync | Implemented, unit-tested |
| ~60 causal microstructure features at 10 Hz | Implemented |
| 8 agents + decision engine with NO TRADE gates | Implemented, unit-tested |
| Backend-verified trigger, backend-driven countdown | Implemented, verified in-browser |
| Paper trading, statistics, calibration, Monte Carlo | Implemented |
| Walk-forward validation, leakage checks, edge classification | Implemented, unit-tested |
| **A proven 5-second edge in BTC** | **NOT CLAIMED. Not established.** |

The last row is the point. The system is built to *test* whether a 5-second
edge exists on your data and to return `NESSUN EDGE ROBUSTO IDENTIFICATO` when
it does not. It ships with no pre-baked model and no backtest results.

> **Verification status of this build:** the container it was developed in has
> no route to any exchange (Binance, Coinbase, Kraken and Bybit are all blocked
> by egress policy), so the full pipeline was verified end-to-end against the
> built-in **synthetic** feed and PostgreSQL, not against a live venue. The
> Binance/Coinbase adapters are written to the documented public APIs but have
> not been run against the real endpoints. Point it at a real feed and check
> `/health` first (see [Verifying it is really live](#verifying-it-is-really-live)).

---

## Un solo file, in locale

Se vuoi solo farlo girare sulla tua macchina senza Docker, senza PostgreSQL e
senza `pip install`, tutto il motore sta anche in un file solo:

```bash
python3 aurum_engine.py selftest        # verifica il file su se stesso
python3 aurum_engine.py check           # il venue e' raggiungibile?
python3 aurum_engine.py run --strategy burst15 --payout 0.8
open http://localhost:8000              # dashboard
```

Solo libreria standard di Python (3.9+). Il database e' **SQLite**
(`aurum.db`), il client WebSocket e il server HTTP sono scritti dentro il file,
e l'apprendimento e' una regressione logistica implementata a mano — stessa
pipeline: dataset causale, walk-forward con purga, classificazione dell'edge,
attivazione solo se il verdetto regge.

| Comando | Cosa fa |
|---|---|
| `run` | live su Binance/Coinbase, oppure `--source sim` (simulatore) o `--source csv` (replay) |
| `check` | DNS, REST e WebSocket, con il motivo esatto se qualcosa non passa |
| `stats` | statistiche del paper trading, calibrazione, Monte Carlo |
| `backtest` | walk-forward su cio' che ha registrato |
| `burst` / `burst-grid` | replay di BURST-15, sessioni incluse, e la griglia di soglie |
| `shadow` | anche le finestre che NON sono state tradate |
| `wallet` | saldo, cicli, regole di puntata e registro di cassa |
| `selftest` | 11 verifiche interne, nessuna rete richiesta |

La dashboard su `:8000` e' servita dallo stesso file: **portafoglio** con saldo,
puntata, curva del capitale e ciclo in corso; grafico a candele sempre in vista
(5s / 15s / 1m / 5m / 10m / 30m) con le operazioni disegnate sopra; il segnale
corrente con il countdown; la diagnostica dei cancelli; cosa sta analizzando il
motore in questo momento (gli otto agenti oppure le tre condizioni di BURST-15,
piu' la microstruttura); la sessione, le statistiche, l'apprendimento, la salute
e lo storico completo delle operazioni con puntata, esito in euro e saldo.

### Il portafoglio

```bash
python3 aurum_engine.py run --payout 0.85 \
    --capital 500 --stake-amount 10        # 500 EUR, 10 EUR a operazione
python3 aurum_engine.py wallet             # saldo, cicli, registro di cassa
```

Il conto parte da `--capital` (500 EUR di default) e rischia `--stake-amount`
per operazione (10 di default; con `--stake-mode percent --stake-percent 2`
punta invece una quota del saldo, quindi composta). Quando il saldo non copre
piu' una puntata il **ciclo e' bruciato**: il motore si ferma, ri-studia tutto
quello che ha registrato con lo stesso walk-forward di sempre, attiva un modello
solo se il verdetto regge, e riapre un ciclo nuovo con il capitale iniziale.

Ogni movimento e' una riga nel registro (`wallet_ledger`), quindi il saldo e'
ricostruibile e verificabile - non un contatore in memoria. **Senza `--payout`
il portafoglio resta spento**: senza il payout del broker un saldo in denaro non
e' definito, e il motore lo dichiara invece di inventarlo.

E va detto: ricominciare dopo un azzeramento **non recupera** il capitale
bruciato. Quello che i cicli misurano e' quanti ne servono e quanto durano.

Cosa resta solo nel progetto completo: la ricerca di strategie con correzione
per test multipli, l'importatore dell'archivio storico Binance, i modelli ad
alberi e il frontend Next.js.

---

## Quick start

```bash
./setup.sh --start            # generates .env with fresh secrets, then starts everything
open http://localhost:3000    # dashboard
open http://localhost:8000/docs   # API
```

That is the whole setup. `setup.sh` generates the database password and your
admin key, points the engine at the live public Binance feed, and checks that
the endpoints are actually reachable from your machine.

Manual equivalent, if you prefer:

```bash
cp .env.example .env          # set POSTGRES_PASSWORD, optionally ADMIN_API_KEY
docker compose up -d --build
```

Local development without Docker: see [INSTALLATION.md](INSTALLATION.md).

### There is no exchange API key to add

This is worth stating plainly, because it is the usual assumption:

* **No Binance API key is needed, used, or accepted.** Market data comes from
  public endpoints (`api.binance.com`, `stream.binance.com`) that require no
  credential. There is no signing code and no private endpoint anywhere in the
  repository.
* The engine **cannot place an order**, so there is nothing for a key to
  authorise. Paper trading is the only mode that exists.
* The two secrets `setup.sh` generates are yours and local:
  `POSTGRES_PASSWORD` for your database, and `ADMIN_API_KEY` which guards
  `/settings`, `/backtest` and `/models` on your own instance.

The only value you may want to add by hand is `BINARY_PAYOUT` — your broker's
payout, e.g. `0.8`. Leave it empty and the system reports `PAYOUT UNKNOWN` and
refuses to state a monetary P&L rather than guessing one.

If Binance is blocked where you are, `setup.sh` says so during the check. The
engine will start, receive nothing, and correctly emit NO TRADE — it will not
pretend to have data. Point `EXCHANGES` at `coinbase` in that case, or run the
backend behind a network that can reach the venue.

---

## How to read the signal

The main screen shows one card:

```
BTC/USDT
$104.520,20

        ↓  GIÙ

TRIGGER      DURATA     CONFIDENCE     ENTRY
$104.515,80   5 SEC        82%           —

ASPETTA CHE BTC TOCCHI  $104.515,80
Il countdown parte SOLO al trigger
```

* **GIÙ / SU** — predicted direction over the horizon.
* **TRIGGER** — the price that must trade before anything starts. It sits
  *below* the current price for a DOWN signal and *above* it for an UP signal,
  and is sized from the volatility observed over the horizon.
* **The countdown does not run yet.** Status is `IN ATTESA` (WAITING).

When the live price touches the trigger, the **backend** flips the state and
stamps `triggered_at` / `expires_at`:

```
STATO: ATTIVO
TRIGGER RAGGIUNTO · COUNTDOWN
        ( 5 → 4 → 3 → 2 → 1 )
```

then `SCADUTO` and finally `WIN` / `LOSS` / `PARI`, decided by comparing the
expiry price to the entry price. The browser never decides any of this: it
renders server timestamps corrected by a measured clock offset.

Full lifecycle:

```
ANALYSIS → SIGNAL CREATED → WAITING FOR TRIGGER → TRIGGER HIT
        → ACTIVE (5s countdown) → EXPIRED → WIN / LOSS
```

If the trigger is never reached inside the wait window, the signal is
`CANCELLED` — not counted as a win or a loss.

### NO TRADE is a result

The engine refuses to signal when the data cannot support a decision: order
book desynchronised, spread too wide, latency too high, feed stale, anomaly
detected, regime unknown, features incomplete, model out of distribution, or
simply not enough edge. The card then shows **NO TRADE** with the reasons.

If it shows NO TRADE for longer than you expect, do not start turning
thresholds down. Ask the engine:

```bash
curl localhost:8000/diagnostics | jq '.verdict, .blocking_gates'
```

That ranks every gate by how often it actually fired and leads with the
verdict — most often `NO MARKET DATA`, which is a connectivity problem no
threshold can fix. The dashboard shows the same ranking under the NO TRADE
card.

---

## Two strategies

`SIGNAL_STRATEGY` selects which decision path produces signals:

* **`ensemble`** (default) — the eight agents, their gates, and a trigger price
  the market must come to before the countdown starts.
* **`burst15`** — **AURUM BURST-15**: a 15-minute operating window that only
  takes bursts of tape on a 5-second horizon, entering at the market one second
  after the trigger rather than waiting for a level, with a session stop-loss
  and take-profit that close the window early. See [BURST15.md](BURST15.md).

---

## Modes

* **SIMPLE** (`/`) — the card, the live chart, signal history, a stats summary.
* **PRO** (`/pro`) — order book, order flow tape, all 8 agents with their
  reasoning, P(UP)/P(DOWN)/P(NEUTRAL), the full feature read-out, system health
  and paper-trading statistics.

### Desktop notifications, without a browser tab

```bash
pip install websockets
python3 desktop/notifier.py --sound
```

Holds one WebSocket open and fires native OS notifications (macOS, Linux,
Windows 10+) on every signal transition. See [desktop/README.md](desktop/README.md).

A held-open socket is used rather than periodic polling for a simple reason: a
5-second call has to arrive inside those 5 seconds. Sampling once a minute would
mean predicting a window that closed 55 seconds earlier.

---

## Verifying it is really live

Nothing below reads a cached or fabricated value.

```bash
# 1. Feed, book, latency, per-component health
curl -s localhost:8000/health | jq '{source, is_synthetic, components, market}'

# 2. Two-sided quote moving in real time (run twice, compare)
curl -s localhost:8000/market | jq '{price, bid, ask, spread_bps, latency_ms, ts}'

# 3. The order book is genuinely synchronised (not a REST snapshot on a loop)
curl -s localhost:8000/orderbook | jq '{synced, last_update_id, stats}'

# 4. Raw WebSocket frames
npx wscat -c ws://localhost:8000/ws/market
```

Signs it is live and healthy: `is_synthetic: false`, `source: "LIVE"`,
`orderbook.synced: true`, `applied_updates` climbing, `last_update_id` climbing,
`latency_ms` a plausible tens-of-milliseconds, `feed_age_ms` under a second.

If `EXCHANGES=synthetic`, every response carries `is_synthetic: true`, every
database row is flagged, and a permanent orange banner sits at the top of the
dashboard. That mode is for offline development only.

---

## Verifying the trigger and the countdown

```bash
# Watch the lifecycle events as the backend emits them
npx wscat -c ws://localhost:8000/ws/signals
```

You will see, in order: `signal_created` (status `WAITING`, `triggered_at:
null`, `expires_at: null`) → `trigger_hit` → `trade_active` (now
`triggered_at` and `expires_at` are set, exactly `horizon_s` apart) →
`trade_expired` → `signal_settled` with the result.

The invariant that matters: **`expires_at` does not exist until the price
touches the trigger.** The automated tests assert exactly this
(`backend/tests/test_lifecycle.py`).

---

## Verifying paper trading

```bash
curl -s localhost:8000/paper-trading | jq '.trades[0]'
curl -s localhost:8000/statistics | jq '.overall'
curl -s localhost:8000/statistics/calibration | jq
curl -s "localhost:8000/statistics/montecarlo?simulations=10000" | jq
```

Every signal is written to `paper_trades` with its trigger, entry, expiry,
confidence, regime, features and agent panel. Statistics report a Wilson
confidence interval and a binomial p-value next to every win rate, and flag
insufficient samples instead of celebrating them.

**On payout:** with `BINARY_PAYOUT` unset the system reports `PAYOUT UNKNOWN`
and refuses to state a monetary P&L, because a win rate above 50% does **not**
imply profit. Break-even is `1 / (1 + payout)` — at an 80% payout you need
55.6% just to break even.

---

## Looking for a strategy on real data

You do not have to wait days for the live engine to record. Binance publishes
its own historical market data for free, and the importer replays it through
the same feature engine the live system uses:

```bash
cd backend
python -m app.ml.cli import --symbol BTCUSDT --start 2026-08-01 --days 7
python -m app.ml.cli search --horizon 5 --payout 0.8 --summary
python -m app.ml.cli backtest --horizons 1,2,3,5,10,15,30 --summary
python -m app.ml.cli montecarlo --payout 0.8
```

`search` enumerates ~50 concrete strategies and then does the part that is
usually skipped: it tests the winner against the distribution of *the best of
50 under noise*, using a circular rotation test. Searching 50 candidates and
keeping the best finds a "winner" on pure noise essentially every time; without
that correction the number you get back is meaningless.

Read **[STRATEGY.md](STRATEGY.md)** before trusting any result — it shows what
the output looks like on data with no edge at all, and why the best candidate
there still hits 51.6%.

See [BACKTEST.md](BACKTEST.md) for the walk-forward methodology and
[MODEL.md](MODEL.md) for the feature set, agents and decision engine.

---

## Documentation

| File | Contents |
|---|---|
| [INSTALLATION.md](INSTALLATION.md) | Local setup, database, running the tests |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Data flow, modules, threading model, schema |
| [API.md](API.md) | Every REST route and WebSocket stream |
| [STRATEGY.md](STRATEGY.md) | Importing real data, the strategy search, multiple-testing correction |
| [BACKTEST.md](BACKTEST.md) | Walk-forward, purging, leakage checks, verdicts |
| [MODEL.md](MODEL.md) | Features, agents, decision engine, trigger sizing |
| [SECURITY.md](SECURITY.md) | Secrets, rate limiting, threat model |
| [DEPLOYMENT.md](DEPLOYMENT.md) | Docker, scaling, retention, monitoring |
| [BURST15.md](BURST15.md) | The AURUM BURST-15 session strategy, live and offline |
| [ANALISI.md](ANALISI.md) | 🇮🇹 Perché non arrivavano segnali, perché non imparava, cosa è cambiato |
| [aurum_engine.py](aurum_engine.py) | Lo stesso motore in un unico file, SQLite, zero dipendenze |

---

## Tests

Backend (271 tests):

```bash
cd backend
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/btcquant_test \
  python -m pytest -q
```

Order book state machine, feature causality, agents, NO-TRADE gating, the
trigger/countdown lifecycle, statistics and break-even logic, walk-forward
splitting, leakage detection, edge classification, reconnection, back-pressure,
database failure handling, persistence of every table, the REST API and the
WebSocket streams, the BURST-15 trigger and session rules, and that the
configured proxy actually reaches the adapters.

Frontend (67 tests):

```bash
npm test
```

WebSocket message folding, clock-offset estimation, the countdown arithmetic
(including the rule that no countdown exists before the trigger), trigger
progress, formatting, and the signal card rendering every lifecycle state.

---

## Absolute rules this codebase follows

1. No invented data. Missing values are `null`, never zero-filled.
2. No invented backtest results. No pre-trained model ships with the repo.
3. No claim that a strategy works without out-of-sample validation.
4. No future data in any feature. Labels come from the future, features never.
5. No random shuffling of time-ordered data. Chronological, purged splits only.
6. No real orders. Ever.
7. No API key required for market data, and none in the code or the frontend.
8. NO TRADE is a valid, first-class decision.
9. The trigger is verified by the backend against the live feed.
10. The countdown starts only when the price reaches the trigger.
11. The system is allowed to conclude: **NESSUN EDGE ROBUSTO IDENTIFICATO.**

Priority order, when they conflict: **data quality → statistical validity →
robustness → risk control → performance.**

---

## Disclaimer

Research and education. Not financial advice. Nothing here is a prediction of
future prices. Sub-10-second directional prediction in crypto is close to the
hardest regime there is: spreads, fees and broker payouts routinely exceed the
entire edge, and a system that looks profitable on a few hours of recorded
ticks is usually measuring its own overfitting. Treat every number this system
produces as a hypothesis to be attacked, not a result to be trusted.
