"use client";

import { useEngine } from "@/lib/engine";
import { agentLabel, formatPercent } from "@/lib/format";
import type { AgentOutput } from "@/lib/types";

const ORDER = [
  "price_action",
  "order_book",
  "order_flow",
  "volatility",
  "momentum",
  "mean_reversion",
  "market_regime",
  "anomaly",
];

function tone(a: AgentOutput): string {
  if (a.direction === "UP") return "text-up";
  if (a.direction === "DOWN") return "text-down";
  return "text-muted";
}

export function AgentsPanel() {
  const { agents } = useEngine();
  const list = agents?.agents ?? [];
  const sorted = [...list].sort(
    (a, b) => ORDER.indexOf(a.agent) - ORDER.indexOf(b.agent),
  );

  return (
    <section className="panel overflow-hidden">
      <header className="flex items-center justify-between border-b border-[var(--border)] px-3 py-2">
        <h2 className="label">Agenti</h2>
        <span className="text-[11px] text-muted">
          regime: {agents?.regime ?? "—"}
        </span>
      </header>

      <div className="grid grid-cols-1 gap-px bg-[var(--border)] sm:grid-cols-2">
        {sorted.length === 0 && (
          <p className="col-span-full bg-[var(--surface)] px-3 py-6 text-center text-xs text-muted">
            In attesa della prima valutazione…
          </p>
        )}
        {sorted.map((a) => (
          <div key={a.agent} className="bg-[var(--surface)] px-3 py-2">
            <div className="flex items-center justify-between gap-2">
              <span className="text-[11px] font-semibold tracking-wide">
                {agentLabel(a.agent)}
              </span>
              <span className={`text-[11px] font-bold ${tone(a)}`}>
                {a.direction === "NO_TRADE" ? "NO TRADE" : a.direction}
              </span>
            </div>
            <div className="mt-1 flex items-center gap-2">
              <div className="h-1 flex-1 overflow-hidden rounded bg-surface-2">
                <div
                  className={
                    a.score > 0
                      ? "h-full bg-up"
                      : a.score < 0
                        ? "h-full bg-down"
                        : "h-full bg-neutral"
                  }
                  style={{ width: `${Math.abs(a.score) * 100}%` }}
                />
              </div>
              <span className="tnum w-10 text-right text-[11px] text-muted">
                {formatPercent(a.confidence)}
              </span>
            </div>
            <p className="mt-1 line-clamp-2 text-[11px] text-muted">{a.reason}</p>
          </div>
        ))}
      </div>

      {agents && (
        <footer className="grid grid-cols-3 gap-px border-t border-[var(--border)] bg-[var(--border)] text-center">
          <Prob label="P(UP)" value={agents.prob_up} className="text-up" />
          <Prob label="P(DOWN)" value={agents.prob_down} className="text-down" />
          <Prob
            label="P(NEUTRAL)"
            value={agents.prob_neutral}
            className="text-muted"
          />
        </footer>
      )}
    </section>
  );
}

function Prob({
  label,
  value,
  className,
}: {
  label: string;
  value: number;
  className: string;
}) {
  return (
    <div className="bg-[var(--surface)] px-2 py-2">
      <div className="label">{label}</div>
      <div className={`tnum mt-0.5 text-lg font-bold ${className}`}>
        {formatPercent(value, 1)}
      </div>
    </div>
  );
}
