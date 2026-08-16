# Backtesting and edge evaluation

The goal is **not** to find a strategy that made money on recorded data. It is
to determine whether a statistically robust edge exists at a five second
horizon — and to say so plainly when it does not.

## Running it

```bash
cd backend
python -m app.ml.cli status                                  # enough data?
python -m app.ml.cli backtest --horizons 1,2,3,5,10,15,30 --summary
python -m app.ml.cli backtest --horizons 5 --models xgboost,lightgbm --out report.json
python -m app.ml.cli strategies --horizon 5
python -m app.ml.cli montecarlo --payout 0.8
```

Or `POST /backtest` with the admin key and poll `GET /backtest`.

## How much data is enough

`ML_MIN_SAMPLES` (default 5,000 out-of-sample rows) is the floor below which
the tool returns `INCONCLUSIVE` rather than a verdict. At the default 100 ms
feature cadence 5,000 rows is about eight minutes of market — enough to
exercise the machinery, nowhere near enough to conclude anything.

Realistically: hours for a first look, days across different sessions before
any verdict deserves attention. Five-second BTC dynamics differ sharply between
Asian and US hours, and between quiet and volatile days.

## Label construction

```
features at t   →   label = sign( mid(t + h) − mid(t) )
```

* `mid(t)` is the value the feature engine itself recorded at `t`, which is
  already causal;
* `mid(t + h)` is resolved from the tick table using the **first tick at or
  after** `t + h`;
* if no tick exists within `label_tolerance_ms` (default 750 ms) the row is
  **dropped**, never forward-filled — a forward-filled label is a fabricated
  one;
* exact ties are dropped and reported separately as `tie_fraction`;
* rows recorded while the order book was desynchronised are excluded.

## Validation: walk-forward with purging

No random shuffling, anywhere. Splits are strictly chronological with a purge
gap between train and test of `horizon + ML_EMBARGO_S` (default 5s + 30s):

```
|............ train ............|== purge ==|.... test ....|
                                 ^ horizon + embargo
```

The purge exists because a training row's *label window* extends `h` seconds
into the future. Without it, the last rows of the training set are labelled
using prices that fall inside the test period — a subtle, extremely common
leak that inflates results.

Each fold reports its own accuracy, AUC, log loss, Brier score, confidence
interval and in-sample accuracy for comparison. `folds_above_50` measures
consistency: a model that wins on one fold and loses on four has not found
anything.

## Leakage checks

Run before any verdict; a failure forces `FAILED`.

| Check | What it catches |
|---|---|
| `timestamps_monotonic` | rows out of chronological order |
| `duplicate_timestamps` | the same instant on both sides of a split |
| `label_uses_future_only` | entry == exit rows surviving the tie filter |
| `no_future_named_features` | a column named `future_*`, `next_*`, `target_*` |
| `shuffled_label_control` | **the important one**: retrain on randomly permuted labels; if the model still beats chance out of sample, information is leaking |
| `single_feature_separation` | any lone feature with univariate AUC > 0.9 — at a 5s horizon that is a leak, not alpha |

## Models compared

`logistic_regression`, `random_forest`, `gradient_boosting`, `xgboost`,
`lightgbm` — all deliberately shallow and strongly regularised. A deep network
trained on a few hours of ticks memorises the session, so a neural network is
only justified once the tree ensembles show a stable out-of-sample edge first.

## Rule strategies as the control group

`momentum`, `mean_reversion`, `order_book_imbalance`, `order_flow`, `breakout`,
`volatility_expansion`, `liquidation_pressure`, `ensemble`.

These are stateless functions of the feature row — nothing is fitted, so their
out-of-sample number *is* their number. They are evaluated on exactly the same
folds as the models. A model that cannot beat "trade with the order-flow
imbalance" has learned nothing worth deploying. Strategies whose inputs are
missing (e.g. liquidations without a futures feed) decline to trade rather than
inventing a view.

## Selection

Ranked by the **lower bound of the out-of-sample accuracy confidence
interval**, then by fold consistency — never by highest historical profit. A
model whose edge might be zero ranks below one whose worst case is still
positive, even if its point estimate is lower.

## Verdicts

| Verdict | Conditions |
|---|---|
| `PROVEN EDGE` | leakage checks pass, ≥ `ML_MIN_SAMPLES` OOS rows, CI lower bound > 0.5, p < 0.01, ≥ 75% of folds above chance, in-sample/out-of-sample gap ≤ 0.06, and (if payout known) beats break-even |
| `PROMISING` | above chance but inconsistent across folds, weakly significant, or fails break-even |
| `INCONCLUSIVE` | not enough data, or the confidence interval contains 0.5 |
| `OVERFIT` | large in-sample/out-of-sample gap with chance inside the OOS interval |
| `FAILED` | leakage detected, or significantly *below* chance |

Anything other than `PROVEN` / `PROMISING` reports:

> **NESSUN EDGE ROBUSTO IDENTIFICATO / NO ROBUST EDGE IDENTIFIED**

That is a legitimate, expected outcome. Sub-10-second directional prediction is
close to the hardest regime in the market, and the honest answer most of the
time is "no".

## Profitability ≠ accuracy

For a binary option:

```
break-even win rate = 1 / (1 + payout)
```

| Payout | Break-even |
|---|---|
| 0.70 | 58.8% |
| 0.80 | 55.6% |
| 0.90 | 52.6% |
| 1.00 | 50.0% |

So a *statistically real* 53% edge still loses money at an 80% payout. With
`BINARY_PAYOUT` unset the report says `PAYOUT UNKNOWN` and evaluates the
statistical edge only — it will not state a P&L it cannot compute.

`selective_thresholds` in each model report shows accuracy when the model is
only allowed to act above a confidence level, with the resulting coverage. That
is the number that matters for a system whose most common answer is NO TRADE.

## Monte Carlo

Bootstraps the outcome sequence to produce distributions of final P&L, maximum
drawdown, longest losing streak, probability of loss, risk of ruin and the
Kelly fraction. A 55% win rate will still hand you an eight-loss streak often
enough to matter; this is where you find that out before it happens live.

## What this can never tell you

* That the edge will persist. It is a statement about recorded data.
* That you can execute at these prices. Paper entries assume a fill at the
  observed price; a real broker adds spread, slippage and rejections.
* That the payout you were quoted is the payout you will receive.
* That an edge found on one week of data survives the next.

Re-validate on fresh data before believing anything, and treat every result as
a hypothesis to attack rather than a conclusion to trust.
