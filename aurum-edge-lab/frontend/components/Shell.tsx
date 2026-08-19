/** Navigation and the always-visible system banner. */

"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";

import { useLive } from "@/lib/live";
import { duration, eur } from "@/lib/format";
import { Pill } from "./ui";

const NAV = [
  { href: "/", label: "Dashboard" },
  { href: "/markets", label: "Markets" },
  { href: "/cross-market", label: "Cross Market" },
  { href: "/research", label: "Research" },
  { href: "/strategies", label: "Strategies" },
  { href: "/paper-trading", label: "Paper Trading" },
  { href: "/wallet", label: "Wallet & Cycles" },
  { href: "/agents", label: "Agents" },
  { href: "/diagnostics", label: "Diagnostics" },
  { href: "/settings", label: "Settings" },
];

const STATUS_KIND: Record<string, "ok" | "warn" | "bad" | "info" | "neutral"> = {
  OK: "ok",
  WARMING_UP: "info",
  NO_VALIDATED_EDGE: "warn",
  AWAITING_EDGE: "warn",
  NO_FEED: "bad",
  DEGRADED: "bad",
  STOPPED: "bad",
};

export function Shell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const { state, connection, ageMs } = useLive();

  const badges: Record<string, string> = {};
  if (state) {
    badges["/markets"] = `${state.feed.symbols_live}/${state.feed.symbols_configured}`;
    badges["/paper-trading"] = String(state.positions.length);
    badges["/strategies"] = String(
      Object.entries(state.strategy_counts)
        .filter(([key]) => key !== "REJECTED" && key !== "RETIRED")
        .reduce((total, [, value]) => total + value, 0),
    );
    badges["/agents"] = String(state.agents.length);
  }

  return (
    <div className="shell">
      <nav className="sidebar">
        <div className="brand">
          <div className="brand-name">AURUM EDGE LAB</div>
          <div className="brand-sub">paper trading only</div>
        </div>
        {NAV.map((item) => (
          <Link
            key={item.href}
            href={item.href}
            className={`nav-link${pathname === item.href ? " active" : ""}`}
          >
            <span>{item.label}</span>
            {badges[item.href] ? <span className="nav-badge">{badges[item.href]}</span> : null}
          </Link>
        ))}
        <div style={{ padding: "14px 16px 0", borderTop: "1px solid var(--border)", marginTop: 12 }}>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            <Pill
              kind={connection === "open" ? "ok" : connection === "connecting" ? "info" : "bad"}
              dot
            >
              {connection === "open" ? "live" : connection}
            </Pill>
            {state ? (
              <Pill kind={STATUS_KIND[state.status] ?? "neutral"}>{state.status}</Pill>
            ) : null}
            {ageMs !== null && ageMs > 4000 ? (
              <Pill kind="warn">stale {duration(ageMs)}</Pill>
            ) : null}
          </div>
        </div>
      </nav>
      <main className="main">
        <SystemBanner />
        {children}
      </main>
    </div>
  );
}

/**
 * The three things that must never be missed, on every page: the feed is not
 * live, the wallet is not trading, or the engine is unreachable.
 */
function SystemBanner() {
  const { state, connection, attempts } = useLive();

  if (connection !== "open" && !state) {
    return (
      <div className="note bad">
        <strong>No connection to the engine.</strong> Start it with{" "}
        <code>python3 main.py run</code> and check <code>/health</code>. Reconnect attempts:{" "}
        {attempts}.
      </div>
    );
  }
  if (!state) return null;

  return (
    <>
      {!state.feed.live ? (
        <div className="note warn">
          <strong>REPLAY FEED — this is not live market data.</strong> Every number below is derived
          from a recorded or generated file. <code>/health</code> reports{" "}
          <code>market_feed.live = false</code> for the whole run.
        </div>
      ) : null}
      {state.risk.entries_blocked ? (
        <div className="note bad">
          <strong>Entries blocked.</strong> {state.risk.block_reason}
        </div>
      ) : null}
      {state.cycle.blocked_reason ? (
        <div className="note warn">
          <strong>Cycle {state.cycle.cycle?.cycle_id ?? "?"} is not trading.</strong>{" "}
          {state.cycle.blocked_reason}
        </div>
      ) : null}
      {state.status === "NO_VALIDATED_EDGE" ? (
        <div className="note">
          <strong>NO VALIDATED EDGE.</strong> {state.no_edge_reason} — this is a successful state,
          not a fault. The wallet holds {eur(state.wallet.equity)} and nothing is authorised to
          trade it.
        </div>
      ) : null}
    </>
  );
}
