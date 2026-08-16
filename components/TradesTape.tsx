"use client";

import { useEngine } from "@/lib/engine";
import { formatPrice, formatQty, formatTime } from "@/lib/format";

export function TradesTape({ limit = 18 }: { limit?: number }) {
  const { trades, features } = useEngine();
  const f = features?.features ?? {};
  const imbalance = typeof f.volume_imbalance_1s === "number" ? f.volume_imbalance_1s : null;

  return (
    <section className="panel overflow-hidden">
      <header className="flex items-center justify-between border-b border-[var(--border)] px-3 py-2">
        <h2 className="label">Order flow</h2>
        {imbalance !== null && (
          <span
            className={`tnum text-[11px] ${imbalance > 0 ? "text-up" : imbalance < 0 ? "text-down" : "text-muted"}`}
          >
            imbalance 1s {imbalance > 0 ? "+" : ""}
            {imbalance.toFixed(2)}
          </span>
        )}
      </header>
      <div className="tnum max-h-72 overflow-y-auto text-[11px]">
        {trades.length === 0 ? (
          <p className="px-3 py-6 text-center text-muted">
            Nessun trade ricevuto.
          </p>
        ) : (
          trades.slice(0, limit).map((t) => (
            <div
              key={`${t.trade_id}-${t.ts}`}
              className="flex items-center justify-between border-b border-[var(--border)]/50 px-3 py-1 last:border-0"
            >
              <span className="text-muted">{formatTime(t.ts)}</span>
              <span className={t.side === "BUY" ? "text-up" : "text-down"}>
                {formatPrice(t.price)}
              </span>
              <span className="text-muted">{formatQty(t.quantity, 4)}</span>
              <span
                className={`w-9 text-right ${t.side === "BUY" ? "text-up" : "text-down"}`}
              >
                {t.side}
              </span>
            </div>
          ))
        )}
      </div>
    </section>
  );
}
