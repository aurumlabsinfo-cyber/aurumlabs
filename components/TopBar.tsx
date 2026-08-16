"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";
import { useEngine } from "@/lib/engine";
import { useAlerts } from "@/lib/alerts";

function StatusDot({ ok, warn }: { ok: boolean; warn?: boolean }) {
  const color = ok ? "bg-up" : warn ? "bg-warn" : "bg-down";
  return <span className={`inline-block h-2 w-2 rounded-full ${color}`} />;
}

export function TopBar() {
  const { connected, connecting, reconnects, hello, health } = useEngine();
  const { settings, setSetting } = useAlerts();
  const [open, setOpen] = useState(false);
  const pathname = usePathname();

  const synthetic = hello?.is_synthetic ?? health?.is_synthetic ?? false;
  const quality = health?.market?.data_quality;

  return (
    <header className="sticky top-0 z-40 border-b border-[var(--border)] bg-[var(--background)]/95 backdrop-blur">
      {synthetic && (
        <div className="bg-warn/15 border-b border-warn/40 px-3 py-1.5 text-center text-[11px] font-semibold tracking-wide text-warn">
          ⚠ DATI SINTETICI — simulatore, NON mercato reale. Nessuna statistica
          qui descrive il mercato.
        </div>
      )}
      <div className="mx-auto flex max-w-6xl items-center gap-3 px-3 py-2 sm:px-4">
        <Link href="/" className="flex items-center gap-2 shrink-0">
          <span className="text-sm font-bold tracking-tight">BTC 5S</span>
          <span className="hidden text-[11px] text-muted sm:inline">
            QUANT ENGINE
          </span>
        </Link>

        <nav className="flex items-center gap-1 text-xs">
          <Link
            href="/"
            className={`rounded px-2 py-1 ${pathname === "/" ? "bg-surface-2 text-foreground" : "text-muted hover:text-foreground"}`}
          >
            SIMPLE
          </Link>
          <Link
            href="/pro"
            className={`rounded px-2 py-1 ${pathname === "/pro" ? "bg-surface-2 text-foreground" : "text-muted hover:text-foreground"}`}
          >
            PRO
          </Link>
        </nav>

        <div className="ml-auto flex items-center gap-3 text-[11px] text-muted">
          <span className="hidden items-center gap-1.5 sm:flex">
            <StatusDot ok={connected} warn={connecting} />
            {connected
              ? "LIVE"
              : connecting
                ? "CONNESSIONE…"
                : `RICONNESSIONE (${reconnects})`}
          </span>
          {quality && (
            <span className="hidden items-center gap-1.5 md:flex">
              <StatusDot ok={quality.ok} warn={quality.score > 0} />
              DATA {(quality.score * 100).toFixed(0)}%
            </span>
          )}
          <button
            type="button"
            onClick={() => setOpen((v) => !v)}
            aria-expanded={open}
            className="rounded border border-[var(--border)] px-2 py-1 hover:text-foreground"
          >
            ALERT
          </button>
        </div>
      </div>

      {open && (
        <div className="mx-auto max-w-6xl px-3 pb-3 sm:px-4">
          <div className="panel-2 flex flex-wrap gap-4 p-3 text-xs">
            {(
              [
                ["visual", "Notifica visiva"],
                ["sound", "Suono"],
                ["browser", "Notifica browser"],
              ] as const
            ).map(([key, label]) => (
              <label key={key} className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={settings[key]}
                  onChange={(e) => setSetting(key, e.target.checked)}
                  className="h-3.5 w-3.5 accent-[var(--info)]"
                />
                {label}
              </label>
            ))}
            <span className="text-muted">
              Tutte disattivabili. Il suono è sintetizzato, nessun file esterno.
            </span>
          </div>
        </div>
      )}
    </header>
  );
}
