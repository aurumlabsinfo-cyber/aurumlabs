"use client";

/**
 * The main card. Everything else on the page is secondary to this.
 *
 * Reading order, top to bottom: what the pair is, what the price is, WHICH WAY,
 * at what trigger, for how long, how confident, and what state we are in.
 *
 * The countdown is NOT a local timer that starts when the signal appears. It
 * appears only once the backend reports `TRIGGERED`/`ACTIVE`, and its value is
 * derived from the server's `expires_at` corrected by the measured clock offset.
 */

import { useEngine, useCountdown, useWaitCountdown } from "@/lib/engine";
import { distanceToTrigger, triggerProgress } from "@/lib/engine-state";
import {
  STATUS_LABEL,
  directionArrow,
  directionLabel,
  formatDuration,
  formatHorizon,
  formatPercent,
  formatPrice,
} from "@/lib/format";
import type { Diagnostics, LiveSignal } from "@/lib/types";

function toneFor(signal: LiveSignal | null): {
  color: string;
  border: string;
  bg: string;
} {
  if (!signal || signal.direction === "NO_TRADE") {
    return {
      color: "text-neutral",
      border: "border-[var(--border)]",
      bg: "bg-transparent",
    };
  }
  return signal.direction === "UP"
    ? { color: "text-up", border: "border-up/40", bg: "bg-up/5" }
    : { color: "text-down", border: "border-down/40", bg: "bg-down/5" };
}

function ResultBadge({ signal }: { signal: LiveSignal }) {
  const result = signal.result ?? signal.status;
  const styles: Record<string, string> = {
    WIN: "bg-up/20 text-up border-up/50",
    LOSS: "bg-down/20 text-down border-down/50",
    TIE: "bg-neutral/20 text-muted border-[var(--border)]",
    CANCELLED: "bg-neutral/10 text-muted border-[var(--border)]",
  };
  return (
    <span
      className={`rounded border px-3 py-1 text-sm font-bold tracking-wider ${
        styles[result] ?? styles.CANCELLED
      }`}
    >
      {STATUS_LABEL[result] ?? result}
    </span>
  );
}

export function SignalCard() {
  const { signal, tick, hello, agents, diagnostics, clockOffsetMs, connected } =
    useEngine();
  const { secondsLeft, remainingMs, running } = useCountdown(
    signal,
    clockOffsetMs,
  );
  const waitMs = useWaitCountdown(signal, clockOffsetMs);
  const tone = toneFor(signal);

  const price = tick?.price ?? null;
  const symbol = hello?.symbol ?? "BTC/USDT";
  const horizon = signal?.horizon_s ?? hello?.horizon_s ?? 5;
  const settled = !!signal?.result;
  const isActive = signal?.status === "ACTIVE" || signal?.status === "TRIGGERED";
  // Under a minute the bare second count is the clearest thing to put in the
  // dial; a 15-minute horizon has to read as 14:59, not as 899.
  const countdownText =
    horizon < 60
      ? String(secondsLeft ?? 0)
      : formatDuration(remainingMs ?? 0);

  return (
    <section
      className={`panel relative overflow-hidden border-2 ${tone.border} ${tone.bg} px-5 py-6 sm:px-8 sm:py-8`}
    >
      {/* ---------------------------------------------------------- header */}
      <div className="flex items-baseline justify-between gap-3">
        <div>
          <div className="label">{symbol.replace("USDT", "/USDT")}</div>
          <div className="tnum mt-1 text-3xl font-bold tracking-tight sm:text-5xl">
            ${formatPrice(price)}
          </div>
        </div>
        <div className="text-right">
          <div className="label">Stato</div>
          <div className="mt-1 text-sm font-semibold">
            {connected ? (
              signal ? (
                STATUS_LABEL[signal.status] ?? signal.status
              ) : (
                "IN ANALISI"
              )
            ) : (
              <span className="text-warn">CONNESSIONE PERSA</span>
            )}
          </div>
        </div>
      </div>

      {/* ------------------------------------------------------- direction */}
      {!signal || signal.direction === "NO_TRADE" ? (
        <NoTradeBlock
          reasons={
            agents?.no_trade_reasons ??
            diagnostics?.last_decision_reasons ??
            []
          }
          diagnostics={diagnostics}
        />
      ) : (
        <>
          <div className="mt-6 flex items-center justify-center gap-4">
            <span
              className={`${tone.color} text-7xl leading-none sm:text-8xl`}
              aria-hidden
            >
              {directionArrow(signal.direction)}
            </span>
            <span
              className={`${tone.color} text-6xl font-black leading-none tracking-tight sm:text-8xl`}
            >
              {directionLabel(signal.direction)}
            </span>
          </div>

          {/* ------------------------------------------------------ trigger */}
          <div className="mt-7 grid grid-cols-2 gap-3 sm:grid-cols-4">
            <Cell label="Trigger">
              <span className="tnum text-xl font-bold sm:text-2xl">
                ${formatPrice(signal.trigger_price)}
              </span>
            </Cell>
            <Cell label="Durata">
              <span className="tnum text-xl font-bold sm:text-2xl">
                {formatHorizon(horizon)}
              </span>
            </Cell>
            <Cell label="Confidence">
              <span className="tnum text-xl font-bold sm:text-2xl">
                {formatPercent(signal.confidence)}
              </span>
            </Cell>
            <Cell label="Entry">
              <span className="tnum text-xl font-bold sm:text-2xl">
                {signal.entry_price ? `$${formatPrice(signal.entry_price)}` : "—"}
              </span>
            </Cell>
          </div>

          {/* ---------------------------------------------------- countdown */}
          <div className="mt-7 flex flex-col items-center gap-3">
            {signal.status === "WAITING" && (
              <div className="text-center">
                <div className="label">Aspetta che BTC tocchi</div>
                <div className="tnum mt-1 text-2xl font-bold sm:text-3xl">
                  ${formatPrice(signal.trigger_price)}
                </div>
                <div className="mt-2 text-xs text-muted">
                  Il countdown parte SOLO al trigger
                  {waitMs !== null && (
                    <> · finestra {formatDuration(waitMs)}</>
                  )}
                </div>
                <DistanceBar signal={signal} price={price} />
              </div>
            )}

            {isActive && (
              <div className="relative flex flex-col items-center">
                <div className="label">Trigger raggiunto · countdown</div>
                <div
                  className={`pulse ${tone.color} tnum relative mt-2 flex h-28 w-28 items-center justify-center rounded-full border-4 ${tone.border} font-black ${
                    countdownText.length > 4
                      ? "text-3xl"
                      : countdownText.length > 2
                        ? "text-4xl"
                        : "text-6xl"
                  }`}
                >
                  {countdownText}
                </div>
                <div className="mt-2 h-1 w-40 overflow-hidden rounded bg-surface-2">
                  <div
                    className={
                      signal.direction === "UP" ? "h-full bg-up" : "h-full bg-down"
                    }
                    style={{
                      width: `${Math.max(0, Math.min(100, ((remainingMs ?? 0) / (horizon * 1000)) * 100))}%`,
                      transition: "width 80ms linear",
                    }}
                  />
                </div>
                {!running && (
                  <div className="mt-2 text-xs text-muted">SCADUTO</div>
                )}
              </div>
            )}

            {settled && (
              <div className="flex flex-col items-center gap-2">
                <ResultBadge signal={signal} />
                <div className="tnum text-xs text-muted">
                  entry ${formatPrice(signal.entry_price)} → expiry $
                  {formatPrice(signal.expiry_price)}
                </div>
              </div>
            )}
          </div>
        </>
      )}
    </section>
  );
}

function Cell({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="panel-2 px-3 py-2 text-center">
      <div className="label">{label}</div>
      <div className="mt-1">{children}</div>
    </div>
  );
}

/** How far the price still has to travel before the clock can start. */
function DistanceBar({
  signal,
  price,
}: {
  signal: LiveSignal;
  price: number | null;
}) {
  const progress = triggerProgress(signal, price);
  const remaining = distanceToTrigger(signal, price);
  if (progress === null || remaining === null) return null;
  return (
    <div className="mt-3 w-56">
      <div className="h-1.5 overflow-hidden rounded bg-surface-2">
        <div
          className={signal.direction === "UP" ? "h-full bg-up" : "h-full bg-down"}
          style={{ width: `${progress}%`, transition: "width 150ms linear" }}
        />
      </div>
      <div className="tnum mt-1 text-[11px] text-muted">
        mancano ${formatPrice(remaining)}
      </div>
    </div>
  );
}

function NoTradeBlock({
  reasons,
  diagnostics,
}: {
  reasons: string[];
  diagnostics: Diagnostics | null;
}) {
  // The instantaneous reasons change ten times a second. What an operator
  // actually needs is which gate has been binding, and for how long nothing
  // has come out - so both are shown, the persistent one first.
  const top = diagnostics?.blocking_gates?.slice(0, 3) ?? [];
  return (
    <div className="mt-8 flex flex-col items-center text-center">
      <div className="text-4xl font-black tracking-tight text-neutral sm:text-6xl">
        NO TRADE
      </div>
      <p className="mt-3 max-w-md text-xs text-muted">
        Nessun segnale forzato. Il motore opera solo quando i dati lo
        consentono.
      </p>
      {diagnostics && (
        <p className="mt-2 text-[11px] text-muted">
          {diagnostics.signals_emitted} segnali in{" "}
          {formatDuration(diagnostics.uptime_s * 1000)} ·{" "}
          {diagnostics.decisions_evaluated} finestre valutate
        </p>
      )}
      {top.length > 0 && (
        <div className="mt-4 w-full max-w-md text-left">
          <div className="label mb-1">Cosa blocca, nel tempo</div>
          <ul className="space-y-1 text-[11px] text-muted">
            {top.map((g) => (
              <li
                key={g.gate}
                className="panel-2 flex items-baseline justify-between gap-3 px-2 py-1"
              >
                <span className="truncate">{g.gate}</span>
                <span className="tnum shrink-0 text-warn">
                  {formatPercent(g.share_of_decisions)}
                </span>
              </li>
            ))}
          </ul>
        </div>
      )}
      {reasons.length > 0 && (
        <div className="mt-3 w-full max-w-md text-left">
          <div className="label mb-1">Ultima finestra</div>
          <ul className="space-y-1 text-[11px] text-muted">
            {reasons.slice(0, 5).map((r) => (
              <li key={r} className="panel-2 px-2 py-1">
                · {r}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
