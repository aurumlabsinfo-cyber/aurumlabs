# AURUM EDGE LAB

Autonomous research engine for crypto perpetual futures. Ten markets, live public
market data, a €100 virtual wallet, and a research loop whose job is to **falsify**
trading hypotheses rather than to produce trades.

**Default venue: Bybit V5 linear perpetuals.** Binance USD-M is equally
supported, as are both testnets — one line in `config.yaml`.

**Paper trading only, and no API keys.** This system reads *public* market data.
It never asks for a key, never accepts one, and has no signing code or order
endpoint to use one with. If you have Bybit API keys, keep them where they are:
nothing here will read them. The selftest asserts this against both adapters.

> The objective is not "many trades". The objective is **validated net edge — or
> an explicit NO EDGE**. A run that ends with zero trades and a clear reason is a
> successful run.

---

## Verification status of this build

Read this before trusting any number the system produces.

| Claim | Status |
|---|---|
| Bybit V5 and Binance USD-M adapters behind one interface | Implemented, unit-tested |
| Order book with venue sequence validation and deterministic resync | Implemented, unit-tested |
| ~90 causal microstructure features at 4 Hz, no lookahead | Implemented, tested for causality |
| 10×10 cross-market matrix, 11 lead-lag horizons, cost-aware | Implemented, verified against a planted relationship |
| Five research agents with real inputs, ranking, state, memory and metrics | Implemented |
| Validation lab: purged walk-forward, embargo, isolated holdout, FDR control | Implemented, tested in both directions |
| €100 wallet, ledger, risk gates, PaperBroker with book-walk fills | Implemented, tested |
| Conditional reset: post-mortem then eligibility, never automatic | Implemented, tested |
| REST + WebSocket API, ten-view frontend | Implemented, tested against a live runtime |
| **Verified against a real venue feed (either one)** | **NO — see below** |
| **A profitable edge in crypto perpetuals** | **NOT CLAIMED. Not established.** |

The container this was developed in has **no route to either venue**.
`api.bybit.com`, `stream.bybit.com`, `bybit-exchange.github.io`,
`fapi.binance.com` and `developers.binance.com` are all refused by the egress
policy with `403 Forbidden` on CONNECT. So the pipeline was verified end to end
against the **replay** feed and a synthetic file, not against a venue.

What that means concretely:

* Both adapters are written to the documented public streams and REST paths, and
  their wire-format handling is unit-tested against those documented shapes —
  but neither has **ever opened a socket to a venue**.
* Endpoint routing is configuration, not constants, and is **verified at
  startup**. Point it at the real venue and `main.py diagnose` tells you within
  seconds whether the endpoints answer and whether all ten symbols are listed as
  tradable perpetuals.
* Because the docs were unreachable at build time, the configured URLs could not
  be re-checked against current official documentation. **Check them.** They are
  a table in `aurum/venues.py`, overridable per field in `config.yaml`.

```bash
python3 main.py diagnose   # the first thing to run on a machine with real network
```

Anything it reports under `endpoints` is the truth about your network, not mine.

---

## Quick start

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

python3 main.py selftest              # 31 checks, no network needed
python3 main.py diagnose              # can this machine reach the venue?
python3 main.py run                   # engine + API on http://127.0.0.1:8002

cd frontend && npm install && npm run dev    # dashboard on http://localhost:3000
```

Without a venue connection you can still exercise the entire system:

```bash
python3 tools/make_replay.py --minutes 25 --out data/replay.jsonl
python3 main.py run --replay data/replay.jsonl --replay-speed 6
```

The generated file is **synthetic**. Any edge found in it is an artifact of the
generator, `/health` reports `market_feed.live = false` for the whole run, every
event carries `source="replay"`, the banner says so on every page of the UI, and
`app.env=production` refuses to load a replay feed at all.

### Operational commands

```bash
python3 main.py run [--replay FILE] [--replay-speed N] [--port 8002] [--no-research]
python3 main.py diagnose        # config, database, endpoints, ports
python3 main.py selftest        # every subsystem against known answers
python3 main.py export          # snapshot without stopping ingestion
python3 main.py config          # the effective configuration

curl -s http://127.0.0.1:8002/health      | python3 -m json.tool
curl -s http://127.0.0.1:8002/wallet      | python3 -m json.tool
curl -s http://127.0.0.1:8002/research    | python3 -m json.tool
curl -s http://127.0.0.1:8002/diagnostics | python3 -m json.tool
```

---

## Exploration mode — trading with no edge, on purpose

Research can tell you a hypothesis does not survive its costs. It cannot tell
you whether the code that would have traded it works, because that code never
runs. A system whose execution path has never executed has an **untested
execution path**, and the first real signal is the worst possible time to find
that out.

Exploration mode opens paper positions on a timer, with no validated edge, to
exercise that path.

```bash
AURUM_EXPLORATION=true AURUM_EXPLORATION_TRADES_PER_DAY=250 python3 main.py run
```

It is off by default and it is **not a strategy**:

* **The direction is a seeded coin flip**, deliberately. Anything cleverer —
  momentum, order-flow sign, the best-scoring rejected hypothesis — would make
  the P&L look like a claim about the market, and it would be a claim the
  validation lab has already rejected. A coin flip cannot be mistaken for an
  edge, which is what makes the measurement readable: what comes back is the
  cost of trading and nothing else.
* **Everything is tagged `exploration`** — in the signal, in the `signals`
  table, and in every API response — so validated performance figures can never
  absorb these trades.
* **Research does not learn from them.** They create no hypotheses and never
  enter research memory.
* `/health` reports `status: EXPLORATION` rather than `NO_VALIDATED_EDGE`,
  because a turning-over wallet and an idle one must not look alike.

It waives exactly **one** gate — edge versus costs. The drawdown and daily-loss
breakers, data quality, spread and crossed-book checks, position and exposure
caps, cooldown, sizing, liquidity and margin all still apply. Those protect
against damage, not against trading without an edge.

### What it costs, measured

85 exploration trades over 25 minutes of replayed market:

| | |
|---|---|
| average gross move | −3.08 bps |
| average round-trip cost | **10.75 bps** |
| average net | **−12.08 bps** |
| total | −3.03 EUR on a 100 EUR wallet |
| strategies promoted | 0 |

That is the point, not a defect: **the loss is the cost**. If forced trading
were profitable the research gates would have found the edge and promoted a
champion.

### Sizing it so it survives the day

Exploration burns the wallet at a predictable rate:

```
loss/day  ≈  trades_per_day × notional × round_trip_bps / 10000
notional  ≈  equity × exploration.risk_per_trade_pct / 100 ÷ (stop_bps / 10000)
```

At the shipped `risk_per_trade_pct: 0.10` that is roughly **−0.036 EUR per
trade**, so 250 trades/day costs about 9% of a 100 EUR wallet per day — which
trips `risk.daily_loss_limit_pct: 10.0` in about a day. That breaker firing is
correct behaviour, not a fault. To run for longer, size down:

```bash
AURUM_SET__exploration__risk_per_trade_pct=0.02   # ~2.5%/day instead of ~9%
```

`risk.min_notional_eur: 5.0` is the floor — below about `0.011` every entry is
refused as `NOTIONAL_TOO_SMALL`.

Two settings interact and it is worth knowing which binds first: the achievable
rate is capped by `risk.max_concurrent_positions ÷ exploration.hold_s`. At the
defaults that is 3 ÷ 120 s ≈ 2160/day, comfortably above 250. Raise `hold_s` or
lower the position cap far enough and entries start returning `MAX_POSITIONS`
instead — visible in `/health` under `exploration.last_rejection`.

### Run it in its own database

```bash
AURUM_DATA_DIR=./data/exploration AURUM_EXPLORATION=true python3 main.py run
```

Exploration P&L reaches the same 100 EUR cycle wallet, so a long forced run can
end a cycle on costs alone and hand the post-mortem a failure caused by nothing
but turnover. Keeping it in a separate data directory leaves the clean research
history and its cycle arithmetic untouched.

---

## What it actually does

```
Bybit V5 / Binance USD-M public streams
        │
        ▼
   adapter ──► bounded queue ──► order books (sequence-validated, auto-resync)
                                        │
                                        ▼
                              data-quality gate ──────────┐
                                        │                 │  blocks execution
                                        ▼                 │  independently of
                              feature engine (250 ms)     │  strategy confidence
                                        │                 │
                    ┌───────────────────┼─────────────┐   │
                    ▼                   ▼             ▼   │
             cross-market         5 research    regime     │
             matrix (90 pairs)    agents        engine     │
                    └───────────────────┬─────────────┘    │
                                        ▼                  │
                             Research Director             │
                                        │                  │
                                        ▼                  │
                             Validation Lab                │
                    train → validation → walk-forward      │
                          → holdout → live shadow          │
                                        │                  │
                                        ▼                  │
                             strategy lifecycle            │
                    RESEARCH→CANDIDATE→CHALLENGER→         │
                    SHADOW→CHAMPION                        │
                                        │                  │
                                        ▼                  ▼
                              risk manager ◄───────────────┘
                                        │
                                        ▼
                              PaperBroker (€100 wallet)
                                        │
                                        ▼
                    cycle manager → post-mortem → conditional reset
```

Only a **CHAMPION** reaches the wallet. Everything else is measured and recorded.

---

## The parts that decide whether any of this means anything

Most of this system is plumbing. Four things determine whether its output is
knowledge or noise, and they are worth reading the code for.

### 1. Overlapping observations are not independent evidence

Sampling every 250 ms and measuring a 5-second horizon means twenty consecutive
triggers share nineteen twentieths of their outcome window. Counting them as
twenty independent samples inflates the t-statistic by roughly √20 and turns
noise into a discovery. Every significance test runs on a **non-overlapping
subset**, and both counts are reported so the difference is visible.

```
40 triggers, 250 ms apart, 5 s horizon  →  2 independent observations
```

`aurum/research/statistics.py:independent_indices`

### 2. Costs are charged before anything is called an edge

A round trip at a 1 bps spread costs **11.4 bps**: two taker fees (9.0), the
spread (1.0), slippage (1.0) and a latency penalty (0.4). At these horizons that
is larger than almost every signal anyone finds. Costs are charged per
observation from the spread actually quoted at that entry, not averaged at the
end.

The cross-market engine demonstrates the point on every run: it reliably finds
BTC leading the alts with *t*-statistics of 6–8, and reports **zero** pairs with
a positive net edge, because +2.7 bps gross does not survive an 11.2 bps round
trip. That is the correct answer, not a failure.

### 3. Searching hard guarantees false positives

Thirty hypotheses per cycle at α=0.05 finds an "edge" every cycle forever. Three
defences, in order:

* **Research memory** keys ideas by a *percentile* fingerprint, so refitting a
  threshold is recognised as the same idea and cannot be re-tested for free.
  Each rejection doubles the cooldown before a retest.
* **Per-hypothesis adjustment** against the running total of tests spent. The
  same evidence that convinces after 1 test does not after 5000.
* **Benjamini-Hochberg** across simultaneous survivors, which catches what the
  per-hypothesis adjustment cannot: many hypotheses passing together by chance.

### 4. The holdout is used once

Agents rank features on the **discovery slice only** — enforced in
`ResearchAgent.discovery_view`, not requested politely. A holdout consulted during
hypothesis creation is not a holdout; it is training data with a reassuring name.

---

## The €100 cycle, and why it does not reset

Each cycle starts at exactly €100.00. When it fails:

1. Block new entries.
2. Freeze the champion — retired with its evidence, never deleted.
3. Run the post-mortem agent.
4. Classify causes and update research memory.
5. Generate or reactivate challengers **only if justified**.
6. Re-run validation and shadow gates.
7. Open a new cycle at €100 **only once an eligible strategy exists**.

Step 7 is the one worth guarding. A system that reopens at €100 the moment it
goes broke will lose €100 an hour forever and produce a dashboard full of
activity. This one sits in `AWAITING_EDGE` with a written reason until research
has actually produced something that passed every gate.

The post-mortem attributes causes from the cycle's own trades — edge decay,
execution cost, slippage, regime shift, concentration, sizing, calibration — and
refuses to attribute anything from fewer than eight trades, because below that
any attribution is storytelling. **No recommendation it can produce ever
suggests lowering a gate.**

---

## Where to look

| Question | File |
|---|---|
| What can be configured, and what is bounded? | `aurum/config.py` |
| How is the book kept correct? | `aurum/market/orderbook.py` |
| When may a symbol be traded? | `aurum/market/quality.py` |
| What are the features, and are they causal? | `aurum/features/engine.py` |
| What does a trade cost? | `aurum/execution/cost_model.py` |
| How is a hypothesis identified? | `aurum/research/hypotheses.py` |
| Why is a p-value trustworthy here? | `aurum/research/statistics.py` |
| What must a hypothesis survive? | `aurum/research/validation.py` |
| What stops the search repeating itself? | `aurum/research/memory.py` |
| What may reach the wallet? | `aurum/strategies/lifecycle.py` |
| Why did nothing trade? | `aurum/diagnostics/collector.py` |
| What happens when a cycle fails? | `aurum/wallet/postmortem.py` |

---

## API

All endpoints in the blueprint's contract are served and tested against a live
runtime (`tests/test_api.py`).

```
GET /health          GET /market            GET /markets
GET /market/{symbol} GET /orderbook/{symbol} GET /features/{symbol}
GET /agents          GET /research          GET /hypotheses
GET /strategies      GET /champion          GET /challengers
GET /signals         GET /positions         GET /trades
GET /wallet          GET /cycles            GET /statistics
GET /data-quality    GET /diagnostics       GET /config
GET /cross-market    GET /trades/{id}       POST /config/settings
WS  /ws/live
```

Two conventions the frontend depends on:

* **Absence is explained.** `/champion` with no champion returns the *reason*
  there is none. A UI that renders a dash because the API said nothing cannot
  distinguish a healthy idle system from a broken one.
* **`/diagnostics` explains every gate**, including the ones at zero, with counts,
  percentages, what each means and what to do about it.

---

## Frontend

Ten views: Dashboard, Markets, Cross Market, Research, Strategies, Paper Trading,
Wallet & Cycles, Agents, Diagnostics, Settings.

```bash
cd frontend
npm install
npm run dev          # http://localhost:3000
NEXT_PUBLIC_API_BASE=http://host:8002 npm run build
```

Next.js 16 with the App Router, Turbopack by default, no UI framework and no chart
library — the charts are inline SVG. `turbopack.root` is pinned to the frontend
directory because this project sits inside a repository with its own Next app at
the top level.

The **Paper Trading** view carries the *WHY THIS TRADE* panel: the exact feature
values at the moment of the decision, each entry condition with the value it
actually held, the full cost breakdown, and predicted versus realised edge.

---

## Testing

```bash
pytest -q               # 139 tests
python3 main.py selftest # 31 subsystem checks, no network
cd frontend && npx tsc --noEmit && npm run build
```

The tests worth reading are the ones with known answers in both directions.
`tests/test_research.py` feeds the validation lab pure noise (rejected), a real
4 bps edge against an 11 bps round trip (rejected), and a 14 bps edge (passes
every gate). Without the last one, a lab that rejects everything would look
correct.

---

## What this is not

* It is **not** a trading system. There is no order path and no key.
* It has **not** been run against a live venue. See the verification status above.
* It makes **no claim** that an edge exists in crypto perpetual futures. It is
  built to test that question and to answer "no" when the answer is no.
* The synthetic replay generator is a **test fixture**, not a market. It lives
  outside the `aurum` package, nothing in the runtime imports it, and production
  refuses to read its output.

---

## Venues

```yaml
market:
  venue: bybit_linear     # bybit_linear_testnet | binance_usdm | binance_usdm_testnet
  rest_base: ""           # empty = use the venue's defaults
  ws_base: ""
```

Leaving the endpoint fields empty is deliberate: they are filled from the
selected venue's table in `aurum/venues.py`. That is what stops the mistake this
system most needs to prevent — switching `venue` while the old venue's hosts sit
in the config, producing a 404 you have to go hunting for. Set any field
explicitly and it wins.

Bybit's public API differs from Binance's in four ways that each needed real
handling rather than a rename, and each is asserted in `tests/test_bybit.py`:

| | Binance USD-M | Bybit V5 |
|---|---|---|
| Order-book snapshot | pulled over REST, spliced onto the diff stream | pushed on the socket as `type: "snapshot"` |
| Sequencing | three fields, `U` / `u` / `pu` | one counter `u`, +1 per delta (`u == 1` means restart) |
| Heartbeat | protocol ping frames, answered by the library | application-level `{"op":"ping"}` we must send |
| Trade side | `m` = "buyer is the maker", must be inverted | `S` = the taker's side, stated directly |

The last one is the dangerous one: getting it backwards inverts every order-flow
feature while leaving all of them plausible. A test asserts the two venues agree
on the aggressor for the same economic event.

One more Bybit quirk it handles: for linear contracts the `tickers` topic is a
*delta* stream, so a push carries only what changed. Reading one as complete
would blank mark price and funding every time the best bid alone moved.

The WebSocket book depth is chosen automatically as the smallest Bybit offers
that covers `depth_levels` — depth 50 pushes every 20 ms while depth 200 pushes
every 100 ms, so taking the deeper book "to be safe" would make it five times
staler for no benefit.

**No API keys.** Neither adapter has a private endpoint, a signing routine or an
order path, and the selftest greps both for `api_key`, `hmac`, `X-BAPI-SIGN` and
order URLs on every run.

---

## Configuration

Everything is in `config.yaml`. Environment overrides accept either separator:

```bash
AURUM_SET__api__port=8010 python3 main.py run     # works in any shell
env 'AURUM_SET__api.port=8010' python3 main.py run
AURUM_FEED=replay AURUM_LOG_LEVEL=DEBUG python3 main.py run
```

Only the values listed under `settable` in `/config` may be changed at runtime,
and each carries a hard server-side bound. The Settings page can move
`risk_per_trade_pct` between 0.10 and 2.00; it cannot move
`wallet.starting_balance_eur` at all.

### Two assumptions stated explicitly

* **EUR/USDT.** The wallet is EUR; the contracts are quoted in USDT. The
  conversion is `fx.usdt_per_eur` (default 1.08), applied to every notional and
  reported by `/health` and `/config` so it is never silently applied. Wire it to
  a real rate before treating the P&L as EUR.
* **Fees.** `costs.taker_fee_bps: 4.5` is the standard Binance USD-M taker rate.
  Your tier may differ, and the difference matters more than most of the research
  — the cost model version is recorded on every trade so changing it invalidates
  the research measured under it rather than silently reinterpreting it.
