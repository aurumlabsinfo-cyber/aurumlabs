/**
 * Pure state logic for the engine WebSocket.
 *
 * Kept free of React so the message handling and the countdown arithmetic can
 * be tested directly — these are the two places where a bug would silently
 * misreport a live trade.
 */

import type {
  AgentsMessage,
  FeatureVector,
  Health,
  Hello,
  LiveSignal,
  OrderBookSnapshot,
  SignalEvent,
  Tick,
  TradePrint,
} from "./types";

export interface EngineState {
  connected: boolean;
  connecting: boolean;
  reconnects: number;
  lastError: string | null;
  /** serverNow - clientNow, in ms. Added to Date.now() for server time. */
  clockOffsetMs: number;
  hello: Hello | null;
  tick: Tick | null;
  trades: TradePrint[];
  signal: LiveSignal | null;
  history: LiveSignal[];
  agents: AgentsMessage | null;
  health: Health | null;
  orderbook: OrderBookSnapshot | null;
  features: FeatureVector | null;
  lastEvent: { event: string; ts: number; signal: LiveSignal } | null;
}

export const INITIAL_STATE: EngineState = {
  connected: false,
  connecting: true,
  reconnects: 0,
  lastError: null,
  clockOffsetMs: 0,
  hello: null,
  tick: null,
  trades: [],
  signal: null,
  history: [],
  agents: null,
  health: null,
  orderbook: null,
  features: null,
  lastEvent: null,
};

export const MAX_TRADES = 60;
export const MAX_HISTORY = 50;

const TERMINAL_STATUSES = new Set([
  "WIN",
  "LOSS",
  "TIE",
  "CANCELLED",
  "EXPIRED",
]);

/**
 * Fold one server frame into the state.
 *
 * `clientNow` is passed in rather than read from the clock so the offset
 * estimation is deterministic under test.
 */
export function reduceMessage(
  prev: EngineState,
  msg: Record<string, unknown>,
  clientNow: number,
): EngineState {
  const serverTs = Number(msg.server_ts ?? 0);
  const next: EngineState = { ...prev };

  if (serverTs > 0) {
    // Smooth the offset: a single sample also contains one-way network delay.
    const sample = serverTs - clientNow;
    next.clockOffsetMs =
      prev.clockOffsetMs === 0 ? sample : prev.clockOffsetMs * 0.9 + sample * 0.1;
  }

  switch (msg.type) {
    case "hello":
      next.hello = msg.data as Hello;
      break;
    case "tick":
      next.tick = msg.data as Tick;
      break;
    case "trade":
      next.trades = [msg.data as TradePrint, ...prev.trades].slice(0, MAX_TRADES);
      break;
    case "agents":
      next.agents = msg.data as AgentsMessage;
      break;
    case "state": {
      const data = msg.data as {
        health: Health;
        orderbook: OrderBookSnapshot;
        features: FeatureVector | null;
      };
      next.health = data.health;
      next.orderbook = data.orderbook;
      next.features = data.features;
      break;
    }
    case "orderbook":
      next.orderbook = msg.data as OrderBookSnapshot;
      break;
    case "signal_snapshot": {
      const data = msg.data as {
        active: LiveSignal[];
        last_settled: LiveSignal[];
      };
      next.signal = data.active?.[0] ?? data.last_settled?.[0] ?? null;
      next.history = data.last_settled ?? [];
      break;
    }
    case "signal": {
      const event = msg.data as SignalEvent;
      next.signal = event.signal;
      next.lastEvent = {
        event: event.event,
        ts: serverTs,
        signal: event.signal,
      };
      if (
        event.event === "signal_settled" &&
        TERMINAL_STATUSES.has(event.signal.status)
      ) {
        next.history = [
          event.signal,
          ...prev.history.filter((s) => s.signal_id !== event.signal.signal_id),
        ].slice(0, MAX_HISTORY);
      }
      break;
    }
    default:
      break; // heartbeat, and anything the server adds later
  }
  return next;
}

export interface CountdownReading {
  remainingMs: number | null;
  secondsLeft: number | null;
  running: boolean;
}

export const NO_COUNTDOWN: CountdownReading = {
  remainingMs: null,
  secondsLeft: null,
  running: false,
};

/**
 * Time left on a signal, from the SERVER's expiry timestamp.
 *
 * Returns `NO_COUNTDOWN` while the signal is still waiting for its trigger:
 * before the trigger is hit there is no countdown to show, and inventing one
 * would misrepresent what the backend is doing.
 *
 * `now` must already be corrected by the clock offset.
 */
export function countdownFor(
  signal: LiveSignal | null,
  now: number,
): CountdownReading {
  if (!signal) return NO_COUNTDOWN;
  if (signal.status !== "ACTIVE" && signal.status !== "TRIGGERED") {
    return NO_COUNTDOWN;
  }
  if (!signal.expires_at) return NO_COUNTDOWN;

  const remainingMs = Math.max(0, signal.expires_at - now);
  return {
    remainingMs,
    secondsLeft: Math.ceil(remainingMs / 1000),
    running: remainingMs > 0,
  };
}

/** Time left in the wait window, before the trigger is reached. */
export function waitRemainingFor(
  signal: LiveSignal | null,
  now: number,
): number | null {
  if (!signal || signal.status !== "WAITING") return null;
  return Math.max(0, signal.expires_wait_at - now);
}

/**
 * Progress from the reference price toward the trigger, as 0..100.
 *
 * Uses signed distance: an overshoot past the trigger stays at 100 rather than
 * wrapping back toward 0, and a move in the wrong direction reads 0.
 */
export function triggerProgress(
  signal: LiveSignal | null,
  price: number | null,
): number | null {
  if (!signal || price === null) return null;
  const total = signal.trigger_price - signal.reference_price;
  if (total === 0) return null;
  const travelled = price - signal.reference_price;
  return Math.max(0, Math.min(100, (travelled / total) * 100));
}

/** Price distance still to cover before the trigger; 0 once it is reached. */
export function distanceToTrigger(
  signal: LiveSignal | null,
  price: number | null,
): number | null {
  if (!signal || price === null) return null;
  const remaining =
    signal.direction === "UP"
      ? signal.trigger_price - price
      : price - signal.trigger_price;
  return Math.max(0, remaining);
}
