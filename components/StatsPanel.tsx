"use client";

/**
 * Paper-trading statistics.
 *
 * Note what is deliberately absent: a P&L number when the payout is unknown.
 * The backend refuses to invent one and this panel says so instead of showing
 * a comforting zero.
 */

import { useEffect, useState } from "react";
import { api, poll } from "@/lib/api";
import { useEngine } from "@/lib/engine";
import { formatNumber, formatPercent } from "@/lib/format";

interface Overall {
  total_signals: number;
  call_up: number;
  put_down: number;
  no_trade: number | null;
  wins: number;
  losses: number;
  ties: number;
  decided: number;
  win_rate: number | null;
  win_rate_ci95: [number, number] | null;
  win_rate_p_value_vs_50: number | null;
  sufficient_sample: boolean;
  statistically_significant: boolean;
  payout_status: string;
  break_even_win_rate: number | null;
  beats_break_even: boolean | null;
  expected_value_per_trade: number | null;
  pnl_units: number | null;
  max_drawdown_units: number | null;
  max_winning_streak: number;
  max_losing_streak: number;
  average_confidence: number | null;
  warnings: string[];
}

export function StatsPanel({ compact = false }: { compact?: boolean }) {
  const [data, setData] = useState<Overall | null>(null);
  const [error, setError] = useState<string | null>(null);
  const { hello, health } = useEngine();
  const synthetic = hello?.is_synthetic ?? health?.is_synthetic ?? false;

  useEffect(
    () =>
      poll(
        (signal) => api.statistics(signal),
        (body) => {
          setData((body as { overall: Overall }).overall);
          setError(null);
        },
        5000,
        (err) => setError(String(err)),
      ),
    [],
  );

  if (error && !data) {
    return (
      <section className="panel px-3 py-4 text-xs text-warn">
        Statistiche non disponibili: {error}
      </section>
    );
  }
  if (!data) {
    return (
      <section className="panel px-3 py-4 text-xs text-muted">
        Caricamento statistiche…
      </section>
    );
  }

  const cells = [
    { label: "Segnali", value: String(data.total_signals) },
    { label: "NO TRADE", value: data.no_trade === null ? "—" : String(data.no_trade) },
    { label: "Win rate", value: formatPercent(data.win_rate ?? undefined, 1) },
    { label: "Win / Loss", value: `${data.wins} / ${data.losses}` },
    {
      label: "P&L",
      value:
        data.pnl_units === null
          ? "PAYOUT UNKNOWN"
          : `${formatNumber(data.pnl_units)} u`,
    },
    {
      label: "Drawdown",
      value:
        data.max_drawdown_units === null
          ? "—"
          : `${formatNumber(data.max_drawdown_units)} u`,
    },
    { label: "Streak +", value: String(data.max_winning_streak) },
    { label: "Streak −", value: String(data.max_losing_streak) },
  ];

  return (
    <section className="panel overflow-hidden">
      <header className="flex items-center justify-between border-b border-[var(--border)] px-3 py-2">
        <h2 className="label">Paper trading</h2>
        <span className="text-[11px] text-muted">
          {data.sufficient_sample
            ? data.statistically_significant
              ? "campione significativo"
              : "campione sufficiente, non significativo"
            : "campione insufficiente"}
        </span>
      </header>

      {synthetic && data.total_signals === 0 && (
        <p className="border-b border-[var(--border)] bg-warn/10 px-3 py-2 text-[11px] text-warn">
          Le statistiche escludono per definizione i trade sintetici, quindi
          questo pannello resta a zero finché il motore gira sul simulatore.
          Collega un feed reale (EXCHANGES=binance_spot) per popolarlo.
        </p>
      )}

      <div className="tnum grid grid-cols-2 gap-px bg-[var(--border)] sm:grid-cols-4">
        {cells.map((c) => (
          <div key={c.label} className="bg-[var(--surface)] px-3 py-2">
            <div className="label">{c.label}</div>
            <div className="mt-0.5 text-sm font-bold">{c.value}</div>
          </div>
        ))}
      </div>

      {!compact && (
        <div className="space-y-1 border-t border-[var(--border)] px-3 py-2 text-[11px] text-muted">
          {data.win_rate_ci95 && (
            <p className="tnum">
              Intervallo di confidenza 95%:{" "}
              {formatPercent(data.win_rate_ci95[0], 1)} –{" "}
              {formatPercent(data.win_rate_ci95[1], 1)}
              {data.win_rate_p_value_vs_50 !== null &&
                ` · p = ${data.win_rate_p_value_vs_50}`}
            </p>
          )}
          <p>
            Break-even win rate:{" "}
            {data.break_even_win_rate === null
              ? "non calcolabile senza payout — 1/(1+payout)"
              : `${formatPercent(data.break_even_win_rate, 2)} ${
                  data.beats_break_even ? "(superato)" : "(NON superato)"
                }`}
          </p>
          {data.warnings.map((w) => (
            <p key={w} className="text-warn">
              ⚠ {w}
            </p>
          ))}
        </div>
      )}
    </section>
  );
}
