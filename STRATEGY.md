# Finding a strategy on real data

This is the runbook for the actual question: **is there a tradable 5-second
edge in BTC, and which rule captures it?**

The honest answer, before you start: probably not, and the machinery below
exists to tell you that quickly rather than let you discover it slowly with
money. Read [the trap](#the-trap-this-is-built-to-avoid) first.

---

## The three commands

```bash
# 1. Get real historical data (free, public, no API key)
docker compose exec backend python -m app.ml.cli import \
    --symbol BTCUSDT --start 2026-08-01 --days 7

# 2. Search the strategy space, corrected for the breadth of the search
docker compose exec backend python -m app.ml.cli search \
    --horizon 5 --payout 0.8 --summary

# 3. Cross-check with the model-based study
docker compose exec backend python -m app.ml.cli backtest \
    --horizons 1,2,3,5,10,15,30 --summary
```

Step 1 takes a few minutes per day of data. Step 2 takes seconds.

---

## Step 1 — real data, at scale

`import` downloads Binance's own published market data from
`data.binance.vision` and **replays it through the same feature engine the live
system uses**. That last point is what makes the result meaningful: a rule
validated on imported data was validated on the identical feature definitions
it will meet live.

| Archive stream | Gives you |
|---|---|
| `bookTicker` | best bid/ask and sizes → L1 imbalance, spread, micro-price |
| `aggTrades` | every trade with its maker flag → the whole order-flow family |

**What the archive does not contain: full order-book depth.** So
`depth_imbalance_*`, walls and liquidity-removal features stay `null`, and any
rule or agent needing them abstains rather than inventing a value. If you want
depth research you must record it live with `PERSIST_BOOK_UPDATES=true`.

Rows land with `source = REPLAY`, `is_synthetic = false`. It is real market
data, so the research tooling includes it. Remove it later with:

```python
from app.ml.importer import purge_replay
```

The importer validates aggressively — crossed quotes, timestamps outside the
requested day, wrong column counts, seconds where milliseconds belong — and
**aborts loudly** rather than silently mis-parsing. A quietly shifted column
would poison every study you run afterwards.

```bash
# Parse and validate without writing anything:
python -m app.ml.cli import --start 2026-08-01 --dry-run
```

How much to import: **days, not hours.** Five-second dynamics differ sharply
between Asian and US sessions and between calm and violent days. A week is a
reasonable first pass; a month is better.

---

## Step 2 — the search, and why it corrects itself

`search` enumerates ~50 concrete strategies — parameter grids over order flow,
L1 book imbalance, micro-price deviation, momentum, mean reversion, and a few
combinations — and scores every one on the **same purged walk-forward
out-of-sample folds**.

Then it does the part that almost everything else skips.

### The trap this is built to avoid

Test 50 strategies on pure noise. The best of the 50 will show maybe 51.5%
accuracy, a tidy confidence interval, and a p-value under 0.05. It is a mirage
— you did not find an edge, you found the maximum of 50 random numbers. With
enough candidates this happens essentially every time.

The fix is to test the winner against the distribution of *the best of 50*,
not against a coin flip.

### How the correction works

A **circular rotation test** (White's Reality Check):

1. rotate the label series by an offset `k` against the features — this
   destroys any real relationship while preserving the labels' own
   autocorrelation exactly;
2. recompute every candidate's win rate at that rotation;
3. take the **maximum across candidates** — one draw from the null of "best of
   the whole search";
4. repeat over thousands of rotations to build the null distribution;
5. the family-wise p-value is how often that null maximum reaches the observed
   best.

Rotations smaller than the label's own memory are excluded: at those offsets
the rotated labels still overlap the originals, so they are not a valid null.
This matters a great deal here — 5-second labels sampled every 100 ms overlap
heavily, and a naive i.i.d. bootstrap would understate the null and hand back a
spurious "edge".

The whole cross-correlation across every rotation is one FFT per candidate,
which is what makes an exhaustive test affordable.

### What the output looks like

Real output from a search over 49 candidates on 60,000 rows of **pure noise**:

```
VERDICT: NO EDGE

  49 candidates were tested; the winner must therefore beat the distribution
  of the best-of-49, not a coin flip.

  ranked first by worst case: micro_price_dev(th_bps=0.2) at 0.5014 over 35216
  trades. The correction is applied to the most flattering candidate instead
  (mean_reversion_bb(z=2.5) at 0.5156), which is the harder test; a
  random-alignment search of the same breadth reaches 0.5560 5% of the time
  (family-wise p = 0.8466).

  The best candidate is inside what the search itself produces from noise.
```

Note the numbers: the best candidate hit **51.6%**, which uncorrected would
look like a discovery. The null says a search this wide reaches **55.6%** on
noise one time in twenty. Hence p = 0.85 and the correct verdict, *no edge*.

That is the single most valuable output this tool produces.

### Ranking

Candidates are ranked by the **lower bound of the win-rate confidence
interval**, never by the point estimate. A strategy whose edge might be zero
ranks below one whose worst case is still positive. Candidates taking fewer
than `--min-trades` (default 200) out-of-sample trades are excluded outright —
otherwise the search always crowns some ultra-selective variant that went 9-1
by luck.

The multiple-testing correction is applied to the *most flattering* candidate,
which is usually not the one ranked first. That is deliberate and conservative:
it makes passing harder.

---

## Step 3 — the verdicts

| Verdict | Meaning |
|---|---|
| `NO EDGE` | The best candidate is inside what the search produces from noise. **Expected outcome.** |
| `INCONCLUSIVE` | Not enough data to build a valid null. Import more. |
| `STATISTICAL EDGE, NOT PROFITABLE` | It really predicts — and still loses money after the payout. |
| `PROMISING` | Survives correction, but the interval still touches 0.5. |
| `CANDIDATE EDGE` | Survives correction with a bounded effect. **Still a hypothesis.** |

### Even `CANDIDATE EDGE` is not a green light

The same folds were used to rank the candidates, so the winner has been
selected on the data it is being judged on. The real test is out-of-time:

```bash
# import a period you have never looked at
python -m app.ml.cli import --start 2026-09-01 --days 5
python -m app.ml.cli search --horizon 5 --payout 0.8 --summary
```

If the same family and roughly the same parameters win again, on data chosen
after the fact, you have something worth paper-trading live. If it evaporates —
which is the common outcome — it was overfitting and you just saved yourself
the tuition.

---

## Profitability is a separate question from accuracy

```
break-even win rate = 1 / (1 + payout)
```

| Payout | Break-even |
|---|---|
| 0.70 | 58.8% |
| 0.80 | 55.6% |
| 0.90 | 52.6% |

A statistically real 53% edge **loses money** at an 80% payout. This is why
`--payout` matters: without it the search judges only the statistical edge and
says so. With it, a real-but-unprofitable result gets its own verdict rather
than being reported as a win.

And these are paper fills at the observed price. A real broker adds spread,
slippage and rejections, all of which come out of the same thin margin.

---

## What real BTC data actually looks like

Measured on **Crypto.com BTC_USDT, 2026-08-14 07:48 UTC** — 50 one-minute
candles and 150 consecutive trade prints, pulled live. These numbers changed
several defaults in this repository, so they are recorded here with their
provenance rather than left as folklore.

| Measurement | Value | Consequence |
|---|---|---|
| Quoted spread | 1 tick = **0.0016 bps** | The old `MAX_SPREAD_BPS=3.0` gate was ~1900x looser than the market and could never fire. Now 1.0. |
| **5s windows with zero net price change** | **32%** | A binary bet at this horizon is substantially a bet on the tie rule. New NO-TRADE gate. |
| 5s windows with no trades at all | 18% | No order-flow information exists in nearly a fifth of windows. |
| Median 1-minute range | 2.07 bps | Two of fifty minutes moved a **single tick** in the whole minute. |
| Trade arrivals | 1.56/s average, 28% of seconds have any trade, busiest second had **44** | Flow is violently bursty, not Poisson. Gaps up to 14s. |
| Top-of-book sizes | bid $10,583 vs ask **$96** | `book_imbalance_l1` reads +0.98 from a dust order. Now suppressed below a minimum notional. |
| Depth-5 imbalance, same instant | +0.146 | Depth stays informative where L1 does not. |
| Trade size | median $499, 35% of prints under $100 | Most prints are dust; "large trade" thresholds must be percentile-based, as they are. |
| Venue volume | median $17k/minute, 36% of minutes under $10k | This venue is thin. See the warning below. |

### The finding that matters most

**32% of 5-second windows ended exactly where they started.**

That single number reframes the whole problem. If a broker treats an unchanged
price as a loss, then roughly a third of trades are lost before direction is
even considered, and the break-even win rate on the *directional* trades rises
correspondingly. If it refunds them, a third of your capital is dead time. Either
way, the horizon is fighting the tick size, not just the noise.

The engine now measures this continuously as `zero_move_fraction` and refuses
to trade when it exceeds `MAX_ZERO_MOVE_FRACTION` (default 0.35). It also
requires the expected move to be worth at least `MIN_EXPECTED_MOVE_TICKS`
(default 2). The old check — "does the expected move clear the spread?" — was
inert on real data, because a one-tick spread is cleared by almost anything.

### A warning about venue choice

The instrument measured above trades roughly $83M/day. Binance BTCUSDT trades
one to two orders of magnitude more. On a thin venue, minutes pass with $25 of
volume and the price simply does not move — which is precisely the condition
under which a 5-second directional bet cannot work. **Use the deepest venue you
can reach as the primary feed**; treat thin venues as cross-checks only.

---

## Where to look first

If anything works at this horizon, order flow is the likeliest place: it is the
only family that measures *actual executed aggression* rather than a derived
statistic. The engine already weights it highest (1.8) for that reason.

Narrow the search when you want a cleaner correction — fewer candidates means a
lower bar to clear:

```bash
python -m app.ml.cli search --families order_flow,order_flow_confirmed,flow_plus_book
```

The classical indicators (RSI, Bollinger, VWAP) are in the grid as controls. If
one of *those* wins at 5 seconds, be suspicious rather than pleased: it is far
more likely to be an artefact of the search than a genuine effect.

---

## What none of this can tell you

* That an edge found on August data survives September.
* That you can execute at these prices.
* That the payout you were quoted is the payout you get.
* That a live paper-trading result matching the backtest means it is real —
  it means it has not been falsified *yet*.

Treat every verdict as a hypothesis to attack, never as a result to trust.
