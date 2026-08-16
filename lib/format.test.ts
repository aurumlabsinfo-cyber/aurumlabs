import { describe, expect, it } from "vitest";
import {
  STATUS_LABEL,
  directionArrow,
  directionLabel,
  formatDuration,
  formatHorizon,
  formatNumber,
  formatPercent,
  formatPrice,
  formatQty,
  formatTime,
} from "./format";

describe("formatPrice", () => {
  it("uses the Italian convention", () => {
    expect(formatPrice(104520.2)).toBe("104.520,20");
    expect(formatPrice(1234567.891)).toBe("1.234.567,89");
  });

  it("renders a dash for missing values rather than 0", () => {
    // A price of "0" would be a lie; absence must look like absence.
    expect(formatPrice(null)).toBe("—");
    expect(formatPrice(undefined)).toBe("—");
    expect(formatPrice(NaN)).toBe("—");
  });

  it("keeps a real zero", () => {
    expect(formatPrice(0)).toBe("0,00");
  });
});

describe("formatPercent", () => {
  it("scales and rounds", () => {
    expect(formatPercent(0.82)).toBe("82%");
    expect(formatPercent(0.8234, 1)).toBe("82.3%");
  });

  it("is a dash when unknown", () => {
    expect(formatPercent(null)).toBe("—");
  });
});

describe("formatQty / formatNumber", () => {
  it("trims trailing precision", () => {
    expect(formatQty(1.5)).toBe("1,5");
    expect(formatNumber(1.23456, 2)).toBe("1.23");
  });

  it("is a dash when unknown", () => {
    expect(formatQty(null)).toBe("—");
    expect(formatNumber(undefined)).toBe("—");
  });
});

describe("formatDuration", () => {
  it("shows tenths under ten seconds and whole seconds above", () => {
    expect(formatDuration(4400)).toBe("4.4");
    expect(formatDuration(17_000)).toBe("17");
  });

  it("never goes negative", () => {
    expect(formatDuration(-500)).toBe("0.0");
  });
});

describe("formatTime", () => {
  it("is a dash for a missing timestamp", () => {
    expect(formatTime(null)).toBe("—");
    expect(formatTime(0)).toBe("—");
  });

  it("renders a 24-hour clock", () => {
    expect(formatTime(1_700_000_000_000)).toMatch(/^\d{2}:\d{2}:\d{2}$/);
  });
});

describe("direction labels", () => {
  it("speaks Italian in the UI", () => {
    expect(directionLabel("UP")).toBe("SU");
    expect(directionLabel("DOWN")).toBe("GIÙ");
    expect(directionLabel("NO_TRADE")).toBe("NO TRADE");
  });

  it("pairs each direction with an arrow", () => {
    expect(directionArrow("UP")).toBe("↑");
    expect(directionArrow("DOWN")).toBe("↓");
    expect(directionArrow("NO_TRADE")).toBe("—");
  });
});

describe("STATUS_LABEL", () => {
  it("covers every lifecycle state the backend can emit", () => {
    for (const status of [
      "WAITING",
      "TRIGGERED",
      "ACTIVE",
      "EXPIRED",
      "WIN",
      "LOSS",
      "TIE",
      "CANCELLED",
    ]) {
      expect(STATUS_LABEL[status]).toBeTruthy();
    }
  });
});

// ------------------------------------------------- minute-scale horizons
describe("formatDuration at minute scale", () => {
  it("keeps a tenth of a second in the final ten", () => {
    expect(formatDuration(4300)).toBe("4.3");
  });

  it("drops the decimal between ten seconds and a minute", () => {
    expect(formatDuration(42_000)).toBe("42");
  });

  it("switches to m:ss from a minute up", () => {
    // 899 is not a duration anybody reads at a glance.
    expect(formatDuration(899_000)).toBe("14:59");
    expect(formatDuration(60_000)).toBe("1:00");
    expect(formatDuration(65_000)).toBe("1:05");
  });

  it("adds hours only when there are hours", () => {
    expect(formatDuration(3_600_000)).toBe("1:00:00");
    expect(formatDuration(3_725_000)).toBe("1:02:05");
  });

  it("never renders a negative countdown", () => {
    expect(formatDuration(-5000)).toBe("0.0");
  });
});

describe("formatHorizon", () => {
  it("reads seconds below a minute", () => {
    expect(formatHorizon(5)).toBe("5 SEC");
  });

  it("reads minutes at and above a minute", () => {
    expect(formatHorizon(900)).toBe("15 MIN");
    expect(formatHorizon(60)).toBe("1 MIN");
  });

  it("keeps one decimal for a fractional unit rather than rounding it away", () => {
    expect(formatHorizon(90)).toBe("1.5 MIN");
  });

  it("reads hours at an hour and above", () => {
    expect(formatHorizon(3600)).toBe("1 H");
  });

  it("has no reading without a horizon", () => {
    expect(formatHorizon(null)).toBe("—");
    expect(formatHorizon(undefined)).toBe("—");
  });
});
