"use client";

/**
 * Single WebSocket connection to the backend, shared by the whole UI.
 *
 * Two things matter here:
 *
 * 1. **Reconnection.** The socket reconnects with exponential backoff and the
 *    UI shows the real connection state rather than a frozen last value.
 * 2. **Clock synchronisation.** Every frame carries `server_ts`. We track the
 *    offset between the server clock and this browser's clock so the 5 second
 *    countdown can be derived from the backend's `triggered_at`/`expires_at`
 *    instead of a local `setInterval` that drifts.
 *
 * The state folding and the countdown arithmetic live in `engine-state.ts` as
 * pure functions; this file only owns the socket and the React plumbing.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import {
  countdownFor,
  INITIAL_STATE,
  reduceMessage,
  waitRemainingFor,
  type CountdownReading,
  type EngineState,
} from "./engine-state";
import type { LiveSignal } from "./types";

export type { EngineState } from "./engine-state";

const WS_BASE =
  process.env.NEXT_PUBLIC_WS_URL ??
  (typeof window !== "undefined"
    ? `${window.location.protocol === "https:" ? "wss" : "ws"}://${window.location.hostname}:8000`
    : "ws://localhost:8000");

export interface EngineContextValue extends EngineState {
  serverNow: () => number;
}

/** Exported so tests can render components against a fixed engine state. */
export const EngineContext = createContext<EngineContextValue | null>(null);

export function EngineProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<EngineState>(INITIAL_STATE);
  const socketRef = useRef<WebSocket | null>(null);
  const attemptRef = useRef(0);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const closedRef = useRef(false);

  const applyMessage = useCallback((msg: Record<string, unknown>) => {
    const clientNow = Date.now();
    setState((prev) => reduceMessage(prev, msg, clientNow));
  }, []);

  const connect = useCallback(() => {
    if (closedRef.current) return;
    setState((s) => ({ ...s, connecting: true }));
    let ws: WebSocket;
    try {
      ws = new WebSocket(`${WS_BASE}/ws/dashboard`);
    } catch (err) {
      setState((s) => ({ ...s, connecting: false, lastError: String(err) }));
      scheduleReconnect();
      return;
    }
    socketRef.current = ws;

    ws.onopen = () => {
      attemptRef.current = 0;
      setState((s) => ({
        ...s,
        connected: true,
        connecting: false,
        lastError: null,
      }));
    };
    ws.onmessage = (event) => {
      try {
        applyMessage(JSON.parse(event.data));
      } catch {
        /* a malformed frame must not take down the UI */
      }
    };
    ws.onerror = () => {
      setState((s) => ({ ...s, lastError: "websocket error" }));
    };
    ws.onclose = () => {
      setState((s) => ({ ...s, connected: false, connecting: false }));
      scheduleReconnect();
    };

    function scheduleReconnect() {
      if (closedRef.current) return;
      attemptRef.current += 1;
      const delay = Math.min(1000 * 2 ** (attemptRef.current - 1), 15000);
      const jittered = delay * (0.5 + Math.random());
      setState((s) => ({ ...s, reconnects: attemptRef.current }));
      timerRef.current = setTimeout(connect, jittered);
    }
  }, [applyMessage]);

  useEffect(() => {
    closedRef.current = false;
    connect();
    return () => {
      closedRef.current = true;
      if (timerRef.current) clearTimeout(timerRef.current);
      socketRef.current?.close();
    };
  }, [connect]);

  const value = useMemo<EngineContextValue>(
    () => ({
      ...state,
      serverNow: () => Date.now() + state.clockOffsetMs,
    }),
    [state],
  );

  return (
    <EngineContext.Provider value={value}>{children}</EngineContext.Provider>
  );
}

export function useEngine(): EngineContextValue {
  const ctx = useContext(EngineContext);
  if (!ctx) throw new Error("useEngine must be used inside <EngineProvider>");
  return ctx;
}

/**
 * Milliseconds left on a signal, derived from the SERVER's expiry timestamp.
 * Null while the signal is still waiting for its trigger - the countdown does
 * not exist before the trigger is hit.
 */
export function useCountdown(
  signal: LiveSignal | null,
  offsetMs: number,
): CountdownReading {
  const running =
    !!signal &&
    (signal.status === "ACTIVE" || signal.status === "TRIGGERED") &&
    !!signal.expires_at;
  const now = useClock(running, 50);
  // `now === 0` means the clock store has not been subscribed yet (one frame at
  // mount). Render nothing rather than a number derived from a missing clock.
  if (now === 0) return countdownFor(null, 0);
  return countdownFor(signal, now + offsetMs);
}

/** Countdown of the *wait* window, before the trigger is reached. */
export function useWaitCountdown(
  signal: LiveSignal | null,
  offsetMs: number,
): number | null {
  const running = !!signal && signal.status === "WAITING";
  const now = useClock(running, 200);
  if (now === 0) return null;
  return waitRemainingFor(signal, now + offsetMs);
}

/**
 * The system clock as an external store.
 *
 * The clock is genuinely external mutable state, so it is read through
 * `useSyncExternalStore` rather than mirrored into React state. That matters
 * here for one concrete reason: the snapshot is refreshed the moment the timer
 * subscribes, so the first frame after a trigger shows the real remaining time
 * instead of a value captured before the signal existed.
 *
 * Returns 0 until the store is subscribed; callers treat that as "no reading
 * yet" rather than as a timestamp.
 */
function useClock(active: boolean, intervalMs: number): number {
  const nowRef = useRef(0);

  const subscribe = useCallback(
    (onChange: () => void) => {
      nowRef.current = Date.now();
      if (!active) return () => {};
      const id = setInterval(() => {
        nowRef.current = Date.now();
        onChange();
      }, intervalMs);
      return () => clearInterval(id);
    },
    [active, intervalMs],
  );

  const getSnapshot = useCallback(() => nowRef.current, []);
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
