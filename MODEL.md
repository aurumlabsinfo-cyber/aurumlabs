# Model: features, agents, decision engine

Priority order, as designed: **order flow → order book → microstructure →
price action → classical indicators**. The classical indicators are context and
control variables, not the signal.

## Features

Computed every `FEATURE_INTERVAL_MS` (default 100 ms). Every value is causal:
it uses only information available at or before the vector's timestamp. Values
that cannot be computed yet are `null`, never `0`.

### Price action
`return_100ms`, `return_250ms`, `return_500ms`, `return_1000ms`,
`return_2000ms`, `return_3000ms`, `return_5000ms` (all in basis points),
`velocity_bps_s`, `acceleration_bps_s2`, `momentum_5s`,
`momentum_consistency` (+1 when every horizon agrees on direction, −1 when they
fully disagree).

### Order book
`mid`, `micro_price`, `micro_price_dev_bps`, `spread`, `spread_bps`,
`relative_spread`, `book_imbalance_l1`, `bid_qty_l1`, `ask_qty_l1`,
`depth_imbalance_5`, `depth_imbalance_20`, `depth_notional_bid_20`,
`depth_notional_ask_20`, `liquidity_concentration_bid/ask`,
`bid_wall_size`, `ask_wall_size`, `bid_wall_distance_bps`,
`ask_wall_distance_bps`, `depth_within_5bps_bid/ask`,
`liquidity_removal_bid/ask`, `book_levels_bid/ask`.

The **micro-price** is the size-weighted mid: it leans toward the side with
less resting size, and is usually a better one-second-ahead estimate of fair
value than the mid. A **wall** is a level materially larger than the local
average (`BOOK_WALL_MULTIPLE`).

### Order flow (windows: 1s, 5s, 30s)
`buy_volume_*`, `sell_volume_*`, `volume_imbalance_*`, `buy_sell_ratio_*`,
`trade_intensity_*`, `avg_trade_size_*`, `trade_count_*`,
`aggressive_buy_notional_*`, `aggressive_sell_notional_*`,
`consecutive_buys`, `consecutive_sells`, `large_trade_count`,
`large_buy_notional`, `large_sell_notional`,
`large_trade_threshold_notional` (the rolling 95th percentile).

Aggressor side comes from the venue's maker flag: buyer-is-maker means the
**seller** was the aggressor.

### Volatility
`realized_vol_1s_bps`, `realized_vol_5s_bps`, `realized_vol_30s_bps`,
`vol_ratio_5s_30s`, `vol_acceleration`, `sigma_horizon_bps`.

`sigma_horizon_bps` is the standard deviation of returns measured **over the
signal horizon itself** — how far price typically travels in 5 seconds right
now. It sizes the trigger.

### Classical indicators (secondary)
`ema_9`, `ema_21`, `ema_spread_bps`, `rsi_14`, `vwap_60s`,
`vwap_deviation_bps`, `bb_mid/upper/lower/z`, `atr_14`. Computed on closed
1-second bars.

### Derivatives (only with a futures adapter)
`funding_rate`, `open_interest`, `mark_index_spread_bps`,
`liq_buy_notional_5s`, `liq_sell_notional_5s`, `liq_imbalance_5s`,
`liq_count_30s`. All `null` without the feed — never zero-filled, so a model
cannot mistake "no feed" for "no liquidations".

---

## The eight agents

Each returns `direction`, `confidence`, `score` (signed, −1..+1), `reason`,
`features_used`, `timestamp`, `data_quality`.

| Agent | Weight | Reads |
|---|---|---|
| `price_action` | 1.0 | short-horizon returns normalised by volatility, cross-horizon consistency |
| `order_book` | 1.6 | L1 and depth imbalance, micro-price deviation, walls capping the move |
| `order_flow` | 1.8 | aggressive volume imbalance, streaks, large prints, activity-scaled confidence |
| `volatility` | 0.8 | **gate, not a direction**: is the expected move bigger than the spread? |
| `momentum` | 1.1 | trend normalised by volatility, only counted when flow confirms it |
| `mean_reversion` | 1.1 | band z-score, VWAP deviation, RSI — fades stretches only when the impulse is already fading |
| `market_regime` | 0.6 | classifies the state; direction is advisory |
| `anomaly` | 0.0 | **veto only**: it can never produce a signal |

Agents abstain rather than guess: `order_book` abstains when the book is
desynchronised, `order_flow` abstains when fewer than two trades hit in the
last second, and every agent abstains when its inputs are missing.

### Regimes
`TREND_UP`, `TREND_DOWN`, `RANGE`, `BREAKOUT`, `HIGH_VOLATILITY`,
`LOW_VOLATILITY`, `EXHAUSTION`, `UNKNOWN`. `UNKNOWN` forces NO TRADE.

The regime re-weights the ensemble: momentum is amplified and mean reversion
suppressed in trends and breakouts, and the reverse in ranges and exhaustion.
Those two agents are structurally opposed, so the regime decides which one gets
to speak loudly.

### Anomalies
Price spikes (100 ms move > 8σ of 5s volatility), abnormal spread, bid or ask
liquidity disappearing (> 60% of near depth gone in 1s), one-sided book,
volume bursts (> 12× the 30s baseline), feed latency > 1s, desynchronised book,
data quality < 0.5. **Any anomaly ⇒ NO TRADE.**

---

## Decision engine

### Hard gates, evaluated before any scoring
Signal engine disabled · still warming up · data quality below
`MIN_DATA_QUALITY` · order book desynchronised · spread above
`MAX_SPREAD_BPS` · latency above `MAX_LATENCY_MS` · anomaly detected · regime
`UNKNOWN` · required features missing · expected move does not clear the spread.

Any one of them ⇒ NO TRADE, with the reason recorded and shown in the UI.

### Aggregation
Confidence-weighted mean of the signed agent scores, with regime multipliers
and — once at least 20 settled signals exist — a per-agent historical hit-rate
adjustment bounded to ±40%.

### Probabilities

```
mass    = agreement × average confidence        (how much the agents will claim)
p_dir   = logistic(3 × aggregate_score)         (how the claim splits)

P(UP)      = p_dir × mass
P(DOWN)    = (1 − p_dir) × mass
P(NEUTRAL) = 1 − mass
```

`P(NEUTRAL)` is not "the price will not move" — it is "no actionable edge".
The three always sum to 1.

Reported `confidence` is the *conditional* probability of the chosen direction,
`P(UP) / (P(UP) + P(DOWN))`, which is exactly the quantity that
`/statistics/calibration` measures against realised frequency.

**These are model outputs, not calibrated frequencies**, until the calibration
report says otherwise. Check it before believing "82%".

### Model blending
When a trained model is active and in-distribution, its probability is blended
with the ensemble at 30% weight (50% once validated out of sample). If the
current feature vector sits outside the training distribution (more than three
features beyond 6σ of their training mean, or too many missing), the model is
skipped and the engine emits NO TRADE.

### Trigger sizing

```
offset_bps = min(TRIGGER_MAX_BPS, TRIGGER_SIGMA_K × sigma_horizon_bps)
offset     = max(offset, TRIGGER_MIN_TICKS × TICK_SIZE)

UP   → trigger = reference + offset     (price must rise to it)
DOWN → trigger = reference − offset     (price must fall to it)
```

Rounded to `TICK_SIZE` and forced onto the correct side of the reference. This
makes the trigger reachable in a quiet market and not trivially reachable in a
fast one — and it is why the signal is an *entry condition*, not just a
direction.

---

## Trained models

Nothing ships pre-trained. Train from your own recorded data:

```bash
python -m app.ml.cli backtest --horizons 5 --save-best
curl -X POST localhost:8000/models/activate \
     -H "X-API-Key: $ADMIN_API_KEY" \
     -H 'Content-Type: application/json' \
     -d '{"model_id":"xgboost_h5s_1786620000000"}'
```

A model is only saved when its verdict is `PROVEN EDGE` or `PROMISING`. Each
artifact stores the model, its feature names, the training-distribution
statistics used for the out-of-distribution guard, the horizon and the edge
report. Deactivate with `POST /models/deactivate` — decisions then come from
the rule ensemble alone.

---

## Known limitations

* Confidence is uncalibrated until you have enough settled trades to check it.
* Agent weights are priors chosen by hand, refined only by observed hit rates.
* Paper entries assume a fill at the observed trigger price; a real broker adds
  spread, slippage and rejections.
* The regime classifier uses thresholds, not a fitted model.
* No cross-venue arbitrage or lead-lag signal, though the adapter layer makes
  adding one straightforward.
