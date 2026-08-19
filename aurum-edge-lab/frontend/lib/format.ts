/**
 * Formatting.
 *
 * `null` and `undefined` render as an em dash, never as `0`. The difference
 * between "the book is empty" and "we have not received a book" is exactly the
 * difference this dashboard exists to show.
 */

export const MISSING = "—";

export function num(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return MISSING;
  return value.toLocaleString("en-GB", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export function price(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return MISSING;
  const digits = value >= 1000 ? 2 : value >= 1 ? 4 : 6;
  return value.toLocaleString("en-GB", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export function eur(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return MISSING;
  return `€${value.toLocaleString("en-GB", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}`;
}

export function signedEur(value: number | null | undefined, digits = 4): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return MISSING;
  const sign = value > 0 ? "+" : value < 0 ? "−" : "";
  return `${sign}€${Math.abs(value).toLocaleString("en-GB", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}`;
}

export function bps(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return MISSING;
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)} bps`;
}

export function pct(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return MISSING;
  return `${value.toFixed(digits)}%`;
}

export function ratio(value: number | null | undefined, digits = 3): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return MISSING;
  return value.toFixed(digits);
}

export function integer(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return MISSING;
  return Math.round(value).toLocaleString("en-GB");
}

export function duration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return MISSING;
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)} s`;
  const minutes = seconds / 60;
  if (minutes < 60) return `${minutes.toFixed(1)} min`;
  const hours = minutes / 60;
  if (hours < 24) return `${hours.toFixed(1)} h`;
  return `${(hours / 24).toFixed(1)} d`;
}

export function clock(ms: number | null | undefined): string {
  if (!ms) return MISSING;
  return new Date(ms).toLocaleTimeString("en-GB", { hour12: false });
}

export function stamp(ms: number | null | undefined): string {
  if (!ms) return MISSING;
  return new Date(ms).toLocaleString("en-GB", { hour12: false });
}

export function ago(ms: number | null | undefined): string {
  if (!ms) return MISSING;
  return `${duration(Date.now() - ms)} ago`;
}

/** Sign class for colouring a value. */
export function tone(value: number | null | undefined): "up" | "down" | "flat" {
  if (value === null || value === undefined || !Number.isFinite(value) || value === 0) return "flat";
  return value > 0 ? "up" : "down";
}

export function shortId(value: string | null | undefined, keep = 8): string {
  if (!value) return MISSING;
  return value.length <= keep + 4 ? value : `${value.slice(0, keep)}…`;
}
