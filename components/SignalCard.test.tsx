/**
 * The card is the product. These tests pin the behaviour the whole system
 * exists to deliver: no countdown before the trigger, a countdown after it,
 * and an honest NO TRADE when the engine declines.
 */

import { act, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { EngineContext, type EngineContextValue } from "@/lib/engine";
import { INITIAL_STATE } from "@/lib/engine-state";
import { SignalCard } from "./SignalCard";
import type { LiveSignal, Tick } from "@/lib/types";

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

function tick(price = 104520.2): Tick {
  return {
    ts: T0,
    exchange_ts: T0,
    latency_ms: 30,
    price,
    bid: price - 0.2,
    bid_qty: 1,
    ask: price + 0.2,
    ask_qty: 1,
    spread: 0.4,
    spread_bps: 0.04,
    micro_price: price,
    last_price: price,
    book_synced: true,
    is_synthetic: false,
  };
}

function renderCard(over: Partial<EngineContextValue> = {}) {
  const value: EngineContextValue = {
    ...INITIAL_STATE,
    connected: true,
    connecting: false,
    tick: tick(),
    hello: {
      symbol: "BTCUSDT",
      horizon_s: 5,
      is_synthetic: false,
      source: "LIVE",
      payout: null,
      payout_status: "PAYOUT UNKNOWN",
    },
    serverNow: () => Date.now(),
    ...over,
  };
  return render(
    <EngineContext.Provider value={value}>
      <SignalCard />
    </EngineContext.Provider>,
  );
}

afterEach(() => {
  vi.useRealTimers();
});

describe("SignalCard", () => {
  it("shows the price and the pair", () => {
    renderCard();
    expect(screen.getByText("BTC/USDT")).toBeDefined();
    expect(screen.getByText(/104\.520,20/)).toBeDefined();
  });

  it("shows NO TRADE with its reasons when the engine declines", () => {
    renderCard({
      signal: null,
      agents: {
        ts: T0,
        agents: [],
        regime: "UNKNOWN",
        prob_up: 0,
        prob_down: 0,
        prob_neutral: 1,
        no_trade_reasons: ["order book not synchronised", "spread too wide"],
      },
    });
    expect(screen.getByText("NO TRADE")).toBeDefined();
    expect(screen.getByText(/order book not synchronised/)).toBeDefined();
    expect(screen.getByText(/spread too wide/)).toBeDefined();
  });

  it("shows direction, trigger, duration and confidence while WAITING", () => {
    renderCard({ signal: signal() });
    expect(screen.getByText("GIÙ")).toBeDefined();
    // Shown twice on purpose: in the trigger cell and in the "wait for" block.
    expect(screen.getAllByText("$104.515,80")).toHaveLength(2);
    expect(screen.getByText("5 SEC")).toBeDefined();
    expect(screen.getByText("82%")).toBeDefined();
    expect(screen.getByText("IN ATTESA")).toBeDefined();
  });

  it("does NOT show a countdown before the trigger is hit", () => {
    renderCard({ signal: signal() });
    // The instruction is present...
    expect(
      screen.getByText(/Il countdown parte SOLO al trigger/),
    ).toBeDefined();
    // ...and no countdown ring is rendered.
    expect(screen.queryByText(/trigger raggiunto/i)).toBeNull();
    expect(screen.queryByText("5", { selector: "div" })).toBeNull();
  });

  it("tells the user what price to wait for", () => {
    renderCard({ signal: signal() });
    expect(screen.getByText(/Aspetta che BTC tocchi/i)).toBeDefined();
    // 104520.20 -> 104515.80 is 4.40 away
    expect(screen.getByText(/mancano \$4,40/)).toBeDefined();
  });

  it("shows the countdown once the backend reports ACTIVE", () => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
    renderCard({
      signal: signal({
        status: "ACTIVE",
        triggered_at: T0,
        expires_at: T0 + 5000,
        entry_price: 104515.8,
      }),
      tick: tick(104515.8),
    });
    expect(screen.getByText(/trigger raggiunto/i)).toBeDefined();
    expect(screen.getByText("5")).toBeDefined();
    expect(screen.getByText("ATTIVO")).toBeDefined();
  });

  it("counts down as server time advances", () => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
    renderCard({
      signal: signal({
        status: "ACTIVE",
        triggered_at: T0,
        expires_at: T0 + 5000,
        entry_price: 104515.8,
      }),
    });
    expect(screen.getByText("5")).toBeDefined();
    act(() => {
      vi.advanceTimersByTime(2000);
    });
    expect(screen.getByText("3")).toBeDefined();
    act(() => {
      vi.advanceTimersByTime(2000);
    });
    expect(screen.getByText("1")).toBeDefined();
  });

  it("shows the entry price once triggered", () => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
    renderCard({
      signal: signal({
        status: "ACTIVE",
        triggered_at: T0,
        expires_at: T0 + 5000,
        entry_price: 104515.8,
      }),
    });
    expect(screen.getAllByText("$104.515,80").length).toBeGreaterThan(1);
  });

  it("shows the result with entry and expiry once settled", () => {
    renderCard({
      signal: signal({
        status: "WIN",
        result: "WIN",
        triggered_at: T0,
        expires_at: T0 + 5000,
        settled_at: T0 + 5000,
        entry_price: 104515.8,
        expiry_price: 104509.2,
      }),
    });
    // Appears as both the status line and the result badge.
    expect(screen.getAllByText("WIN").length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText(/104\.509,20/)).toBeDefined();
    expect(screen.queryByText(/trigger raggiunto/i)).toBeNull();
  });

  it("renders an UP signal with the trigger above the price", () => {
    renderCard({
      signal: signal({
        direction: "UP",
        reference_price: 104520.2,
        trigger_price: 104528.4,
        confidence: 0.86,
      }),
    });
    expect(screen.getByText("SU")).toBeDefined();
    expect(screen.getAllByText("$104.528,40")).toHaveLength(2);
    expect(screen.getByText("86%")).toBeDefined();
  });

  it("says so when the connection drops instead of showing stale state", () => {
    renderCard({ connected: false, signal: signal() });
    expect(screen.getByText("CONNESSIONE PERSA")).toBeDefined();
  });
});
