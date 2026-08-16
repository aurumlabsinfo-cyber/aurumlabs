"use client";

import { useEngine } from "@/lib/engine";
import { directionArrow, directionLabel, formatPrice, formatTime } from "@/lib/format";

const RESULT_STYLE: Record<string, string> = {
  WIN: "text-up",
  LOSS: "text-down",
  TIE: "text-muted",
  CANCELLED: "text-muted",
};

export function SignalHistory({ limit = 12 }: { limit?: number }) {
  const { history } = useEngine();
  const rows = history.slice(0, limit);

  return (
    <section className="panel overflow-hidden">
      <header className="flex items-center justify-between border-b border-[var(--border)] px-3 py-2">
        <h2 className="label">Storico segnali</h2>
        <span className="text-[11px] text-muted">{history.length} recenti</span>
      </header>

      {rows.length === 0 ? (
        <p className="px-3 py-6 text-center text-xs text-muted">
          Nessun segnale concluso in questa sessione.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[520px] text-xs">
            <thead className="text-muted">
              <tr className="border-b border-[var(--border)]">
                <Th>Ora</Th>
                <Th>Segnale</Th>
                <Th align="right">Trigger</Th>
                <Th align="right">Entry</Th>
                <Th align="right">Expiry</Th>
                <Th align="right">Risultato</Th>
              </tr>
            </thead>
            <tbody className="tnum">
              {rows.map((s) => (
                <tr
                  key={s.signal_id}
                  className="border-b border-[var(--border)]/60 last:border-0"
                >
                  <Td>{formatTime(s.triggered_at ?? s.created_at)}</Td>
                  <Td>
                    <span
                      className={
                        s.direction === "UP"
                          ? "text-up"
                          : s.direction === "DOWN"
                            ? "text-down"
                            : "text-muted"
                      }
                    >
                      {directionArrow(s.direction)} {directionLabel(s.direction)}
                    </span>
                  </Td>
                  <Td align="right">{formatPrice(s.trigger_price)}</Td>
                  <Td align="right">{formatPrice(s.entry_price)}</Td>
                  <Td align="right">{formatPrice(s.expiry_price)}</Td>
                  <Td align="right">
                    <span
                      className={`font-semibold ${RESULT_STYLE[s.result ?? ""] ?? "text-muted"}`}
                    >
                      {s.result ?? s.status}
                    </span>
                  </Td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function Th({
  children,
  align = "left",
}: {
  children: React.ReactNode;
  align?: "left" | "right";
}) {
  return (
    <th
      className={`px-3 py-2 font-medium ${align === "right" ? "text-right" : "text-left"}`}
    >
      {children}
    </th>
  );
}

function Td({
  children,
  align = "left",
}: {
  children: React.ReactNode;
  align?: "left" | "right";
}) {
  return (
    <td
      className={`px-3 py-1.5 ${align === "right" ? "text-right" : "text-left"}`}
    >
      {children}
    </td>
  );
}
