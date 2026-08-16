"use client";

import { useEngine } from "@/lib/engine";
import { formatNumber } from "@/lib/format";

const STATUS_TONE: Record<string, string> = {
  UP: "text-up",
  DOWN: "text-down",
  DEGRADED: "text-warn",
  DISABLED: "text-muted",
};

const LABELS: Record<string, string> = {
  websocket: "WEBSOCKET",
  api: "API",
  database: "DATABASE",
  market_data: "MARKET DATA",
  order_book: "ORDER BOOK",
  model: "MODEL",
  latency: "LATENCY",
  error_rate: "ERROR RATE",
};

export function HealthPanel() {
  const { health, connected, tick } = useEngine();
  const components = health?.components ?? {};
  const latency = health?.market?.tick_latency_ms ?? {};
  const quality = health?.market?.data_quality;

  return (
    <section className="panel overflow-hidden">
      <header className="flex items-center justify-between border-b border-[var(--border)] px-3 py-2">
        <h2 className="label">System health</h2>
        <span
          className={`text-[11px] ${health?.status === "HEALTHY" ? "text-up" : "text-warn"}`}
        >
          {health?.status ?? (connected ? "…" : "OFFLINE")}
        </span>
      </header>

      <div className="grid grid-cols-2 gap-px bg-[var(--border)] sm:grid-cols-4">
        {Object.entries(LABELS).map(([key, label]) => {
          const status = components[key]?.status ?? "DOWN";
          return (
            <div key={key} className="bg-[var(--surface)] px-3 py-2">
              <div className="label">{label}</div>
              <div
                className={`mt-0.5 text-xs font-bold ${STATUS_TONE[status] ?? "text-muted"}`}
              >
                {status}
              </div>
            </div>
          );
        })}
      </div>

      <div className="tnum grid grid-cols-2 gap-px border-t border-[var(--border)] bg-[var(--border)] text-[11px] sm:grid-cols-4">
        <Metric label="Latency p50" value={`${formatNumber(latency.p50, 0)} ms`} />
        <Metric label="Latency p95" value={`${formatNumber(latency.p95, 0)} ms`} />
        <Metric
          label="Spread"
          value={tick ? `${formatNumber(tick.spread_bps, 2)} bps` : "—"}
        />
        <Metric
          label="Data quality"
          value={quality ? `${(quality.score * 100).toFixed(0)}%` : "—"}
        />
      </div>

      {quality && quality.reasons.length > 0 && (
        <ul className="border-t border-[var(--border)] px-3 py-2 text-[11px] text-muted">
          {quality.reasons.map((r) => (
            <li key={r}>· {r}</li>
          ))}
        </ul>
      )}
    </section>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-[var(--surface)] px-3 py-2">
      <div className="label">{label}</div>
      <div className="mt-0.5 font-semibold">{value}</div>
    </div>
  );
}
