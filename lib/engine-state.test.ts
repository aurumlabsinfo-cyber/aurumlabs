import { describe, expect, it } from "vitest";
import {
  INITIAL_STATE,
  MAX_TRADES,
  countdownFor,
  distanceToTrigger,
  reduceMessage,
  triggerProgress,
  waitRemainingFor,
} from "./engine-state";
import type { LiveSignal } from "./types";

const T0 = 1_700_000_000_000;

function signal(over: Partial<LiveSignal> = {}): LiveSignal {
  return {
    signal_id: "abc123",
    symbol: "BTCUSDT",
    exchange: "binance_spot",
    direction: "DOWN",
    status: "WAITING",
    reference_price: 104520.2,
    trigger_price: 104515.8,
    horizon_s: 5,
    confidence: 0.82,
    prob_up: 0.15,
    prob_down: 0.75,
    prob_neutral: 0.1,
    edge: 0.3,
    regime: "TREND_DOWN",
    created_at: T0,
    expires_wait_at: T0 + 30_000,
    triggered_at: null,
    expires_at: null,
    settled_at: null,
    entry_price: null,
    expiry_price: null,
    result: null,
    pnl_units: null,
    data_quality: 1,
    model_id: null,
    is_synthetic: false,
    server_ts: T0,
    remaining_ms: null,
    ...over,
  };
}

describe("reduceMessage", () => {
  it("ignores frames it does not know", () => {
    const next = reduceMessage(
      INITIAL_STATE,
      { type: "something_new", server_ts: T0 },
      T0,
    );
    expect(next.tick).toBeNull();
    expect(next.signal).toBeNull();
  });

  it("estimates the clock offset from the first frame", () => {
    // Server is 500 ms ahead of this browser.
    const next = reduceMessage(INITIAL_STATE, { server_ts: T0 + 500 }, T0);
    expect(next.clockOffsetMs).toBe(500);
  });

  it("smooths later offset samples instead of jumping", () => {
    const first = reduceMessage(INITIAL_STATE, { server_ts: T0 + 500 }, T0);
    // A single delayed frame must not yank the countdown around.
    const second = reduceMessage(first, { server_ts: T0 + 1500 }, T0);
    expect(second.clockOffsetMs).toBeGreaterThan(500);
    expect(second.clockOffsetMs).toBeLessThan(700);
  });

  it("caps the trade tape", () => {
    let state = INITIAL_STATE;
    for (let i = 0; i < MAX_TRADES + 20; i++) {
      state = reduceMessage(
        state,
        {
          type: "trade",
          server_ts: T0 + i,
          data: { ts: T0 + i, price: 100 + i, quantity: 1, notional: 100, side: "BUY", trade_id: i },
        },
        T0,
      );
    }
    expect(state.trades).toHaveLength(MAX_TRADES);
    expect(state.trades[0].trade_id).toBe(MAX_TRADES + 19); // newest first
  });

  it("tracks the current signal and records lifecycle events", () => {
    const created = reduceMessage(
      INITIAL_STATE,
      {
        type: "signal",
        server_ts: T0,
        data: { event: "signal_created", signal: signal() },
      },
      T0,
    );
    expect(created.signal?.status).toBe("WAITING");
    expect(created.lastEvent?.event).toBe("signal_created");
    expect(created.history).toHaveLength(0);
  });

  it("moves a settled signal into history exactly once", () => {
    const settled = signal({ status: "WIN", result: "WIN" });
    let state = reduceMessage(
      INITIAL_STATE,
      { type: "signal", server_ts: T0, data: { event: "signal_settled", signal: settled } },
      T0,
    );
    // A duplicate settle frame must not create a second row.
    state = reduceMessage(
      state,
      { type: "signal", server_ts: T0, data: { event: "signal_settled", signal: settled } },
      T0,
    );
    expect(state.history).toHaveLength(1);
    expect(state.history[0].result).toBe("WIN");
  });

  it("does not put an in-flight signal into history", () => {
    const state = reduceMessage(
      INITIAL_STATE,
      {
        type: "signal",
        server_ts: T0,
        data: { event: "trade_active", signal: signal({ status: "ACTIVE" }) },
      },
      T0,
    );
    expect(state.history).toHaveLength(0);
    expect(state.signal?.status).toBe("ACTIVE");
  });

  it("takes the active signal from a snapshot, falling back to the last settled", () => {
    const active = signal({ status: "ACTIVE", signal_id: "live" });
    const old = signal({ status: "WIN", signal_id: "old", result: "WIN" });

    const withActive = reduceMessage(
      INITIAL_STATE,
      {
        type: "signal_snapshot",
        server_ts: T0,
        data: { active: [active], last_settled: [old] },
      },
      T0,
    );
    expect(withActive.signal?.signal_id).toBe("live");

    const idle = reduceMessage(
      INITIAL_STATE,
      {
        type: "signal_snapshot",
        server_ts: T0,
        data: { active: [], last_settled: [old] },
      },
      T0,
    );
    expect(idle.signal?.signal_id).toBe("old");
  });
});

describe("countdownFor", () => {
  it("returns nothing while the signal waits for its trigger", () => {
    // THE core rule: no countdown before the trigger is hit.
    const reading = countdownFor(signal({ status: "WAITING" }), T0 + 2000);
    expect(reading.remainingMs).toBeNull();
    expect(reading.secondsLeft).toBeNull();
    expect(reading.running).toBe(false);
  });

  it("counts down from the server expiry once active", () => {
    const active = signal({
      status: "ACTIVE",
      triggered_at: T0,
      expires_at: T0 + 5000,
      entry_price: 104515.8,
    });
    expect(countdownFor(active, T0).secondsLeft).toBe(5);
    expect(countdownFor(active, T0 + 1200).secondsLeft).toBe(4);
    expect(countdownFor(active, T0 + 4100).secondsLeft).toBe(1);
    expect(countdownFor(active, T0 + 4999).remainingMs).toBe(1);
  });

  it("never goes negative after expiry", () => {
    const active = signal({ status: "ACTIVE", expires_at: T0 + 5000 });
    const reading = countdownFor(active, T0 + 9000);
    expect(reading.remainingMs).toBe(0);
    expect(reading.running).toBe(false);
  });

  it("uses the server clock, so a skewed browser does not shorten the trade", () => {
    const active = signal({ status: "ACTIVE", expires_at: T0 + 5000 });
    const browserIsThreeSecondsBehind = T0 - 3000;
    const offset = 3000; // measured from server_ts frames
    expect(
      countdownFor(active, browserIsThreeSecondsBehind + offset).secondsLeft,
    ).toBe(5);
  });

  it("shows nothing once the signal has settled", () => {
    const won = signal({ status: "WIN", result: "WIN", expires_at: T0 + 5000 });
    expect(countdownFor(won, T0 + 1000).remainingMs).toBeNull();
  });

  it("shows nothing when there is no signal at all", () => {
    expect(countdownFor(null, T0).running).toBe(false);
  });
});

describe("waitRemainingFor", () => {
  it("counts the wait window down while WAITING", () => {
    expect(waitRemainingFor(signal(), T0 + 10_000)).toBe(20_000);
  });

  it("is null once the signal is no longer waiting", () => {
    expect(waitRemainingFor(signal({ status: "ACTIVE" }), T0)).toBeNull();
  });

  it("floors at zero rather than going negative", () => {
    expect(waitRemainingFor(signal(), T0 + 99_000)).toBe(0);
  });
});

describe("triggerProgress", () => {
  it("is 0 at the reference price and 100 at the trigger", () => {
    const s = signal(); // DOWN: 104520.2 -> 104515.8
    expect(triggerProgress(s, 104520.2)).toBeCloseTo(0, 5);
    expect(triggerProgress(s, 104515.8)).toBeCloseTo(100, 5);
  });

  it("clamps past the trigger", () => {
    expect(triggerProgress(signal(), 104000)).toBe(100);
  });

  it("is 0 when price moves away from the trigger", () => {
    // DOWN signal, price rising: no progress at all.
    expect(triggerProgress(signal(), 104600)).toBe(0);
  });

  it("works for UP signals too", () => {
    const up = signal({
      direction: "UP",
      reference_price: 104520.2,
      trigger_price: 104528.4,
    });
    expect(triggerProgress(up, 104524.3)).toBeCloseTo(50, 0);
    expect(triggerProgress(up, 104600)).toBe(100);
    expect(triggerProgress(up, 104400)).toBe(0);
  });

  it("is null without a price", () => {
    expect(triggerProgress(signal(), null)).toBeNull();
  });
});

describe("distanceToTrigger", () => {
  it("reports what is left to cover", () => {
    expect(distanceToTrigger(signal(), 104520.2)).toBeCloseTo(4.4, 5);
  });

  it("is zero once the trigger is reached or passed", () => {
    expect(distanceToTrigger(signal(), 104515.8)).toBe(0);
    expect(distanceToTrigger(signal(), 104000)).toBe(0);
  });

  it("respects direction", () => {
    const up = signal({
      direction: "UP",
      reference_price: 104520.2,
      trigger_price: 104528.4,
    });
    expect(distanceToTrigger(up, 104520.2)).toBeCloseTo(8.2, 5);
    expect(distanceToTrigger(up, 104530)).toBe(0);
  });
});
