/**
 * Backend access.
 *
 * Two rules the whole UI depends on:
 *
 * 1. A failed request is a *state*, not an exception to swallow. `useApi`
 *    returns `{ data, error, loading }` and every panel renders the error,
 *    because a dashboard that shows an empty table when the backend is down
 *    is indistinguishable from one showing a healthy idle system.
 * 2. Nothing here invents a value. There are no defaults standing in for
 *    missing numbers — `null` reaches the component, and the component says
 *    so.
 */

"use client";

import { useCallback, useEffect, useRef, useState } from "react";

export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8002";

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly path: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export async function apiGet<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    signal,
    headers: { Accept: "application/json" },
    cache: "no-store",
  });
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = await response.json();
      detail = typeof body?.detail === "string" ? body.detail : JSON.stringify(body?.detail ?? body);
    } catch {
      /* the body was not JSON; the status text is what we have */
    }
    throw new ApiError(detail, response.status, path);
  }
  return (await response.json()) as T;
}

export async function apiPost<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(body),
  });
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const detail =
      payload && typeof payload.detail === "object"
        ? Object.values(payload.detail).join("; ")
        : (payload?.detail ?? response.statusText);
    throw new ApiError(String(detail), response.status, path);
  }
  return payload as T;
}

export interface ApiState<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  refresh: () => void;
}

/** Fetch on mount and on an interval, with the request cancelled on unmount. */
export function useApi<T>(path: string | null, intervalMs = 0): ApiState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(path !== null);
  const [nonce, setNonce] = useState(0);
  const mounted = useRef(true);

  const refresh = useCallback(() => setNonce((n) => n + 1), []);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  useEffect(() => {
    if (!path) {
      setLoading(false);
      return;
    }
    const controller = new AbortController();
    let timer: ReturnType<typeof setInterval> | undefined;

    const load = async () => {
      try {
        const payload = await apiGet<T>(path, controller.signal);
        if (!mounted.current) return;
        setData(payload);
        setError(null);
      } catch (exc) {
        if (controller.signal.aborted || !mounted.current) return;
        setError(exc instanceof Error ? exc.message : String(exc));
      } finally {
        if (mounted.current) setLoading(false);
      }
    };

    void load();
    if (intervalMs > 0) timer = setInterval(load, intervalMs);
    return () => {
      controller.abort();
      if (timer) clearInterval(timer);
    };
  }, [path, intervalMs, nonce]);

  return { data, error, loading, refresh };
}
