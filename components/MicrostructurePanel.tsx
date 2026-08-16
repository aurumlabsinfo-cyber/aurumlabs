"use client";

/** Live feature read-out: the numbers the agents actually consumed. */

import { useEngine } from "@/lib/engine";
import { formatNumber } from "@/lib/format";

type Row = { label: string; key: string; digits?: number; suffix?: string };

const GROUPS: { title: string; rows: Row[] }[] = [
  {
    title: "Order flow",
    rows: [
      { label: "Volume imbalance 1s", key: "volume_imbalance_1s" },
      { label: "Volume imbalance 5s", key: "volume_imbalance_5s" },
      { label: "Trade intensity 1s", key: "trade_intensity_1s", suffix: "/s" },
      { label: "Consecutive buys", key: "consecutive_buys", digits: 0 },
      { label: "Consecutive sells", key: "consecutive_sells", digits: 0 },
      { label: "Large trades 5s", key: "large_trade_count", digits: 0 },
    ],
  },
  {
    title: "Order book",
    rows: [
      { label: "Imbalance L1", key: "book_imbalance_l1" },
      { label: "Depth imbalance 5", key: "depth_imbalance_5" },
      { label: "Depth imbalance 20", key: "depth_imbalance_20" },
      { label: "Micro-price dev", key: "micro_price_dev_bps", suffix: " bps" },
      { label: "Bid wall dist", key: "bid_wall_distance_bps", suffix: " bps" },
      { label: "Ask wall dist", key: "ask_wall_distance_bps", suffix: " bps" },
      { label: "Liquidity removal bid", key: "liquidity_removal_bid" },
      { label: "Liquidity removal ask", key: "liquidity_removal_ask" },
    ],
  },
  {
    title: "Prezzo e volatilità",
    rows: [
      { label: "Return 500ms", key: "return_500ms", suffix: " bps" },
      { label: "Return 1s", key: "return_1000ms", suffix: " bps" },
      { label: "Return 5s", key: "return_5000ms", suffix: " bps" },
      { label: "Acceleration", key: "acceleration_bps_s2" },
      { label: "Realized vol 5s", key: "realized_vol_5s_bps", suffix: " bps" },
      { label: "Realized vol 30s", key: "realized_vol_30s_bps", suffix: " bps" },
      { label: "Vol ratio 5s/30s", key: "vol_ratio_5s_30s" },
      { label: "σ orizzonte", key: "sigma_horizon_bps", suffix: " bps" },
    ],
  },
  {
    title: "Indicatori (secondari)",
    rows: [
      { label: "RSI 14", key: "rsi_14", digits: 1 },
      { label: "EMA spread", key: "ema_spread_bps", suffix: " bps" },
      { label: "VWAP dev", key: "vwap_deviation_bps", suffix: " bps" },
      { label: "Bollinger z", key: "bb_z" },
      { label: "ATR 14", key: "atr_14" },
    ],
  },
  {
    title: "Derivati (solo con feed futures)",
    rows: [
      { label: "Funding rate", key: "funding_rate", digits: 6 },
      { label: "Open interest", key: "open_interest", digits: 1 },
      { label: "Liquidation imbalance 5s", key: "liq_imbalance_5s" },
      { label: "Liquidations 30s", key: "liq_count_30s", digits: 0 },
    ],
  },
];

export function MicrostructurePanel() {
  const { features } = useEngine();
  const f = features?.features ?? {};

  return (
    <section className="panel overflow-hidden">
      <header className="flex items-center justify-between border-b border-[var(--border)] px-3 py-2">
        <h2 className="label">Microstruttura</h2>
        <span className="text-[11px] text-muted">
          {features ? `aggiornato ${new Date(features.ts).toLocaleTimeString("it-IT")}` : "—"}
        </span>
      </header>

      <div className="grid grid-cols-1 gap-px bg-[var(--border)] md:grid-cols-2 xl:grid-cols-3">
        {GROUPS.map((group) => (
          <div key={group.title} className="bg-[var(--surface)] px-3 py-2">
            <div className="label mb-1">{group.title}</div>
            <dl className="tnum space-y-0.5 text-[11px]">
              {group.rows.map((row) => {
                const raw = f[row.key];
                const value =
                  typeof raw === "number"
                    ? `${formatNumber(raw, row.digits ?? 3)}${row.suffix ?? ""}`
                    : "n/d";
                return (
                  <div key={row.key} className="flex justify-between gap-2">
                    <dt className="text-muted">{row.label}</dt>
                    <dd
                      className={
                        typeof raw === "number"
                          ? raw > 0
                            ? "text-up"
                            : raw < 0
                              ? "text-down"
                              : ""
                          : "text-muted"
                      }
                    >
                      {value}
                    </dd>
                  </div>
                );
              })}
            </dl>
          </div>
        ))}
      </div>
      <footer className="border-t border-[var(--border)] px-3 py-1.5 text-[11px] text-muted">
        &ldquo;n/d&rdquo; significa che il dato non è disponibile (feed assente o
        storico insufficiente) — non zero.
      </footer>
    </section>
  );
}
