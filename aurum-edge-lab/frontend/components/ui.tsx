/** Shared presentation pieces.  No data fetching happens in here. */

"use client";

import type { ReactNode } from "react";

import { MISSING, bps, num, tone } from "@/lib/format";

export function Panel({
  title,
  note,
  children,
  flush = false,
  actions,
}: {
  title: string;
  note?: ReactNode;
  children: ReactNode;
  flush?: boolean;
  actions?: ReactNode;
}) {
  return (
    <section className="panel">
      <header className="panel-head">
        <h2 className="panel-title">{title}</h2>
        {actions ?? (note ? <span className="panel-note">{note}</span> : null)}
      </header>
      <div className={flush ? "panel-body flush" : "panel-body"}>{children}</div>
    </section>
  );
}

export function Stat({
  label,
  value,
  hint,
  tone: valueTone,
  small = false,
}: {
  label: string;
  value: ReactNode;
  hint?: ReactNode;
  tone?: "up" | "down" | "flat" | "warn";
  small?: boolean;
}) {
  return (
    <div className="stat">
      <span className="stat-label">{label}</span>
      <span className={`stat-value${small ? " sm" : ""}${valueTone ? ` ${valueTone}` : ""}`}>
        {value}
      </span>
      {hint ? <span className="stat-hint">{hint}</span> : null}
    </div>
  );
}

export function Pill({
  children,
  kind = "neutral",
  dot = false,
}: {
  children: ReactNode;
  kind?: "neutral" | "ok" | "bad" | "warn" | "info";
  dot?: boolean;
}) {
  const cls = kind === "neutral" ? "pill" : `pill ${kind}`;
  return (
    <span className={cls}>
      {dot ? <span className="dot" /> : null}
      {children}
    </span>
  );
}

/** Signed basis points, coloured by sign. */
export function Bps({ value, digits = 2 }: { value: number | null | undefined; digits?: number }) {
  return <span className={tone(value)}>{bps(value, digits)}</span>;
}

export function Note({
  kind = "info",
  children,
}: {
  kind?: "info" | "warn" | "bad" | "neutral";
  children: ReactNode;
}) {
  return <div className={kind === "neutral" ? "note" : `note ${kind}`}>{children}</div>;
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="empty">{children}</div>;
}

/**
 * Loading, error and empty are three different states and each says so.
 * Rendering an empty table for all three is the single most common way a
 * dashboard lies about what it knows.
 */
export function Async({
  loading,
  error,
  empty,
  emptyMessage,
  children,
}: {
  loading: boolean;
  error: string | null;
  empty?: boolean;
  emptyMessage?: ReactNode;
  children: ReactNode;
}) {
  if (error) {
    return (
      <Note kind="bad">
        <strong>Request failed.</strong> {error}
      </Note>
    );
  }
  if (loading) return <Empty>Loading…</Empty>;
  if (empty) return <Empty>{emptyMessage ?? "Nothing to show yet."}</Empty>;
  return <>{children}</>;
}

/** A minimal inline sparkline.  No chart library: this is 20 lines of SVG. */
export function Sparkline({
  values,
  width = 220,
  height = 40,
  stroke,
}: {
  values: number[];
  width?: number;
  height?: number;
  stroke?: string;
}) {
  const clean = values.filter((v) => Number.isFinite(v));
  if (clean.length < 2) {
    return (
      <span className="faint" style={{ fontSize: 11.5 }}>
        not enough points yet
      </span>
    );
  }
  const min = Math.min(...clean);
  const max = Math.max(...clean);
  const span = max - min || 1;
  const step = width / (clean.length - 1);
  const points = clean
    .map((value, index) => {
      const x = index * step;
      const y = height - ((value - min) / span) * (height - 4) - 2;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  const rising = clean[clean.length - 1] >= clean[0];
  const colour = stroke ?? (rising ? "var(--up)" : "var(--down)");
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} role="img" aria-label="trend">
      <polyline points={points} fill="none" stroke={colour} strokeWidth="1.5" />
    </svg>
  );
}

/** An equity curve with a baseline at the cycle's starting balance. */
export function EquityChart({
  points,
  baseline,
  width = 720,
  height = 160,
}: {
  points: { ts_ms: number; equity: number }[];
  baseline: number;
  width?: number;
  height?: number;
}) {
  if (points.length < 2) {
    return <Empty>The equity curve needs at least two snapshots.</Empty>;
  }
  const values = points.map((p) => p.equity);
  const min = Math.min(...values, baseline);
  const max = Math.max(...values, baseline);
  const span = max - min || 1;
  const step = width / (points.length - 1);
  const y = (value: number) => height - ((value - min) / span) * (height - 12) - 6;
  const path = points.map((p, i) => `${i === 0 ? "M" : "L"}${(i * step).toFixed(1)},${y(p.equity).toFixed(1)}`).join(" ");
  const last = values[values.length - 1];
  const colour = last >= baseline ? "var(--up)" : "var(--down)";
  return (
    <svg
      width="100%"
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio="none"
      role="img"
      aria-label="equity curve"
    >
      <line
        x1="0"
        x2={width}
        y1={y(baseline)}
        y2={y(baseline)}
        stroke="var(--border-strong)"
        strokeDasharray="4 4"
        strokeWidth="1"
      />
      <path d={path} fill="none" stroke={colour} strokeWidth="1.8" />
      <text x="4" y={Math.max(12, y(baseline) - 5)} fill="var(--text-faint)" fontSize="10">
        start €{num(baseline, 2)}
      </text>
    </svg>
  );
}

/** Renders a number or the missing marker; never a silent zero. */
export function Maybe({ value }: { value: ReactNode | null | undefined }) {
  if (value === null || value === undefined || value === "") {
    return <span className="faint">{MISSING}</span>;
  }
  return <>{value}</>;
}
