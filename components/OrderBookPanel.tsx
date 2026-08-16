"use client";

import { useEngine } from "@/lib/engine";
import { formatPrice, formatQty } from "@/lib/format";

export function OrderBookPanel({ levels = 10 }: { levels?: number }) {
  const { orderbook } = useEngine();
  const bids = orderbook?.bids?.slice(0, levels) ?? [];
  const asks = orderbook?.asks?.slice(0, levels) ?? [];
  const max = Math.max(
    ...bids.map(([, q]) => q),
    ...asks.map(([, q]) => q),
    0.0001,
  );

  return (
    <section className="panel overflow-hidden">
      <header className="flex items-center justify-between border-b border-[var(--border)] px-3 py-2">
        <h2 className="label">Order book</h2>
        <span
          className={`text-[11px] ${orderbook?.synced ? "text-up" : "text-down"}`}
        >
          {orderbook?.synced
            ? `SYNCED · id ${orderbook.last_update_id}`
            : `DESYNC${orderbook?.desync_reason ? `: ${orderbook.desync_reason}` : ""}`}
        </span>
      </header>

      {!orderbook ? (
        <p className="px-3 py-6 text-center text-xs text-muted">
          In attesa del book…
        </p>
      ) : (
        <div className="tnum grid grid-cols-2 gap-px bg-[var(--border)] text-[11px]">
          <div className="bg-[var(--surface)] p-2">
            <div className="label mb-1 text-up">Bid</div>
            {bids.map(([price, qty]) => (
              <Row key={price} price={price} qty={qty} max={max} side="bid" />
            ))}
          </div>
          <div className="bg-[var(--surface)] p-2">
            <div className="label mb-1 text-down">Ask</div>
            {asks.map(([price, qty]) => (
              <Row key={price} price={price} qty={qty} max={max} side="ask" />
            ))}
          </div>
        </div>
      )}
      <footer className="border-t border-[var(--border)] px-3 py-1.5 text-[11px] text-muted">
        {orderbook
          ? `${orderbook.bid_levels} livelli bid · ${orderbook.ask_levels} livelli ask`
          : ""}
      </footer>
    </section>
  );
}

function Row({
  price,
  qty,
  max,
  side,
}: {
  price: number;
  qty: number;
  max: number;
  side: "bid" | "ask";
}) {
  const width = `${Math.min(100, (qty / max) * 100)}%`;
  return (
    <div className="relative flex justify-between px-1 py-0.5">
      <span
        className={`absolute inset-y-0 ${side === "bid" ? "right-0 bg-up/10" : "left-0 bg-down/10"}`}
        style={{ width }}
      />
      <span
        className={`relative ${side === "bid" ? "text-up" : "text-down"}`}
      >
        {formatPrice(price)}
      </span>
      <span className="relative text-muted">{formatQty(qty, 3)}</span>
    </div>
  );
}
