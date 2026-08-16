/**
 * Chart bucketing.
 *
 * The browser folds live ticks onto the same bucket boundaries the backend
 * used for history. If the two disagreed, the newest candle would straddle two
 * buckets and the chart would show a bar that never existed.
 */

import { describe, expect, it } from "vitest";
import { TIMEFRAMES, bucketStart } from "./PriceChart";

describe("bucketStart", () => {
  it("floors onto the bucket, in seconds", () => {
    // 12:00:47.900 with a 1-minute bucket belongs to 12:00:00.
    const ts = Date.UTC(2026, 7, 14, 12, 0, 47, 900);
    expect(bucketStart(ts, 60)).toBe(Date.UTC(2026, 7, 14, 12, 0, 0) / 1000);
  });

  it("agrees with the backend formula (ts / bucket) * bucket", () => {
    const ts = 1_723_640_147_900;
    for (const { seconds } of TIMEFRAMES) {
      const bucketMs = seconds * 1000;
      expect(bucketStart(ts, seconds)).toBe(
        Math.floor(ts / bucketMs) * bucketMs / 1000,
      );
    }
  });

  it("puts every tick of one minute into the same bar", () => {
    const base = Date.UTC(2026, 7, 14, 12, 0, 0);
    const buckets = new Set(
      Array.from({ length: 60 }, (_, i) => bucketStart(base + i * 1000, 60)),
    );
    expect(buckets.size).toBe(1);
  });

  it("opens a new bar the instant the bucket rolls over", () => {
    const base = Date.UTC(2026, 7, 14, 12, 0, 0);
    expect(bucketStart(base + 59_999, 60)).not.toBe(bucketStart(base + 60_000, 60));
  });

  it("is exact on a bucket boundary", () => {
    const base = Date.UTC(2026, 7, 14, 12, 0, 0);
    expect(bucketStart(base, 300)).toBe(base / 1000);
  });

  it("offers the timeframes the dashboard advertises", () => {
    expect(TIMEFRAMES.map((t) => t.key)).toEqual(["1s", "1m", "5m", "10m", "1h"]);
  });
});
