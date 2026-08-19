/**
 * The live WebSocket connection, shared by every page.
 *
 * One socket for the whole app, held in a context, because ten pages each
 * opening their own would multiply the server's broadcast work by ten for no
 * benefit — the payload is identical.
 *
 * Connection state is exposed rather than hidden. A dashboard that silently
 * shows the last state it received is how you end up staring at numbers from
 * twenty minutes ago; every page shows the connection pill, and stale data is
 * labelled as stale.
 */

"use client";

import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

import { API_BASE } from "./api";
import type { LiveState } from "./types";

export type ConnectionState = "connecting" | "open" | "closed" | "error";

interface LiveContextValue {
  state: LiveState | null;
  connection: ConnectionState;
  lastMessageAt: number | null;
  /** Milliseconds since the last frame, or null before the first one. */
  ageMs: number | null;
  attempts: number;
}

const LiveContext = createContext<LiveContextValue>({
  state: null,
  connection: "connecting",
  lastMessageAt: null,
  ageMs: null,
  attempts: 0,
});

function wsUrl(): string {
  const base = API_BASE.replace(/^http/, "ws");
  return `${base}/ws/live`;
}

export function LiveProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<LiveState | null>(null);
  const [connection, setConnection] = useState<ConnectionState>("connecting");
  const [lastMessageAt, setLastMessageAt] = useState<number | null>(null);
  const [attempts, setAttempts] = useState(0);
  const [now, setNow] = useState(() => Date.now());
  const socketRef = useRef<WebSocket | null>(null);
  const closedByUs = useRef(false);

  useEffect(() => {
    const ticker = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(ticker);
  }, []);

  useEffect(() => {
    closedByUs.current = false;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let attempt = 0;

    const connect = () => {
      setConnection("connecting");
      const socket = new WebSocket(wsUrl());
      socketRef.current = socket;

      socket.onopen = () => {
        attempt = 0;
        setAttempts(0);
        setConnection("open");
        // The server does not need anything from us, but sending keeps the
        // receive loop on the server side awake and lets it notice a dead peer.
        socket.send("hello");
      };

      socket.onmessage = (event) => {
        try {
          setState(JSON.parse(event.data) as LiveState);
          setLastMessageAt(Date.now());
        } catch {
          /* a malformed frame is dropped; the next one is 500 ms away */
        }
      };

      socket.onerror = () => setConnection("error");

      socket.onclose = () => {
        socketRef.current = null;
        if (closedByUs.current) return;
        setConnection("closed");
        attempt += 1;
        setAttempts(attempt);
        // Exponential backoff, capped: a backend that is down should not be
        // hammered by every open tab.
        const delay = Math.min(15_000, 500 * 2 ** Math.min(attempt, 5));
        retry = setTimeout(connect, delay);
      };
    };

    connect();
    return () => {
      closedByUs.current = true;
      if (retry) clearTimeout(retry);
      socketRef.current?.close();
      socketRef.current = null;
    };
  }, []);

  const value = useMemo<LiveContextValue>(
    () => ({
      state,
      connection,
      lastMessageAt,
      ageMs: lastMessageAt === null ? null : now - lastMessageAt,
      attempts,
    }),
    [state, connection, lastMessageAt, now, attempts],
  );

  return <LiveContext.Provider value={value}>{children}</LiveContext.Provider>;
}

export function useLive(): LiveContextValue {
  return useContext(LiveContext);
}
