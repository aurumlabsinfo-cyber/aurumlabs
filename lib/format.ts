/** Display helpers. Prices use the Italian convention: 104.520,20 */

export function formatPrice(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toLocaleString("it-IT", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export function formatQty(value: number | null | undefined, digits = 4): string {
  if (value === null || value === undefined) return "—";
  return value.toLocaleString("it-IT", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  });
}

export function formatPercent(
  value: number | null | undefined,
  digits = 0,
): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return `${(value * 100).toFixed(digits)}%`;
}

export function formatNumber(
  value: number | null | undefined,
  digits = 2,
): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toFixed(digits);
}

export function formatTime(ts: number | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleTimeString("it-IT", { hour12: false });
}

/**
 * Countdown text.
 *
 * Below a minute it stays a bare number of seconds, with a tenth of a second
 * in the final ten - at a 5-second horizon that tenth is most of the
 * information. From a minute up it switches to m:ss, because "847" is not a
 * duration anybody reads at a glance.
 */
export function formatDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return "—";
  const total = Math.max(0, ms) / 1000;
  if (total < 10) return total.toFixed(1);
  if (total < 60) return Math.round(total).toString();
  const rounded = Math.round(total);
  const minutes = Math.floor(rounded / 60);
  const seconds = rounded % 60;
  if (minutes < 60) return `${minutes}:${String(seconds).padStart(2, "0")}`;
  const hours = Math.floor(minutes / 60);
  return `${hours}:${String(minutes % 60).padStart(2, "0")}:${String(
    seconds,
  ).padStart(2, "0")}`;
}

/** "5 SEC" / "15 MIN": the horizon as the card's headline unit. */
export function formatHorizon(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined || Number.isNaN(seconds)) {
    return "—";
  }
  if (seconds < 60) {
    return `${Number.isInteger(seconds) ? seconds : seconds.toFixed(1)} SEC`;
  }
  const minutes = seconds / 60;
  if (minutes < 60) {
    return `${Number.isInteger(minutes) ? minutes : minutes.toFixed(1)} MIN`;
  }
  const hours = minutes / 60;
  return `${Number.isInteger(hours) ? hours : hours.toFixed(1)} H`;
}

export function directionLabel(direction: string): string {
  if (direction === "UP") return "SU";
  if (direction === "DOWN") return "GIÙ";
  return "NO TRADE";
}

export function directionArrow(direction: string): string {
  if (direction === "UP") return "↑";
  if (direction === "DOWN") return "↓";
  return "—";
}

export function agentLabel(name: string): string {
  return name.replace(/_/g, " ").toUpperCase();
}

export const STATUS_LABEL: Record<string, string> = {
  WAITING: "IN ATTESA",
  TRIGGERED: "TRIGGER RAGGIUNTO",
  ACTIVE: "ATTIVO",
  EXPIRED: "SCADUTO",
  WIN: "WIN",
  LOSS: "LOSS",
  TIE: "PARI",
  CANCELLED: "ANNULLATO",
};
