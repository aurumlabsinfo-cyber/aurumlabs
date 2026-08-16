/**
 * Thin REST client.
 *
 * Only public read endpoints are called from the browser. Administrative
 * endpoints require `X-API-Key`, and that key lives in the backend
 * environment - it is never shipped to the client.
 */

export const API_BASE =
  process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function get<T>(path: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    signal,
    headers: { Accept: "application/json" },
    cache: "no-store",
  });
  if (!res.ok) {
    throw new ApiError(`${path} failed: ${res.status}`, res.status);
  }
  return (await res.json()) as T;
}

export type Candle = {
  time: number; // seconds
  open: number;
  high: number;
  low: number;
  close: number;
  ticks: number;
};

export type CandleResponse = {
  interval: string;
  bucket_seconds: number;
  count: number;
  candles: Candle[];
};

export const api = {
  health: (signal?: AbortSignal) => get<Record<string, unknown>>("/health", signal),
  candles: (interval: string, limit = 300, signal?: AbortSignal) =>
    get<CandleResponse>(`/candles?interval=${interval}&limit=${limit}`, signal),
  market: (signal?: AbortSignal) => get<Record<string, unknown>>("/market", signal),
  statistics: (signal?: AbortSignal) =>
    get<Record<string, unknown>>("/statistics", signal),
  calibration: (signal?: AbortSignal) =>
    get<Record<string, unknown>>("/statistics/calibration", signal),
  paperTrades: (limit = 50, signal?: AbortSignal) =>
    get<Record<string, unknown>>(`/paper-trading?limit=${limit}`, signal),
  backtest: (signal?: AbortSignal) =>
    get<Record<string, unknown>>("/backtest", signal),
  models: (signal?: AbortSignal) => get<Record<string, unknown>>("/models", signal),
};

/** Poll a REST endpoint on an interval, with cleanup. */
export function poll<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
  onData: (data: T) => void,
  intervalMs: number,
  onError?: (err: unknown) => void,
): () => void {
  let stopped = false;
  let timer: ReturnType<typeof setTimeout>;
  const controller = new AbortController();

  const run = async () => {
    try {
      const data = await fetcher(controller.signal);
      if (!stopped) onData(data);
    } catch (err) {
      if (!stopped && !(err instanceof DOMException && err.name === "AbortError")) {
        onError?.(err);
      }
    }
    if (!stopped) timer = setTimeout(run, intervalMs);
  };
  run();

  return () => {
    stopped = true;
    controller.abort();
    clearTimeout(timer);
  };
}
