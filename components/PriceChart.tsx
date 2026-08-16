"use client";

/**
 * Live BTC chart, fed directly by the WebSocket tick stream.
 *
 * History comes from `/candles`, aggregated in PostgreSQL from the recorded
 * ticks; the newest bar is then folded forward in the browser from the live
 * WebSocket stream, on the same bucket boundaries. Trigger, entry and expiry
 * are drawn as price lines and markers so the signal can be read against the
 * actual path of the market.
 */

import { useEffect, useRef, useState } from "react";
import {
  ColorType,
  CrosshairMode,
  type CandlestickData,
  type IChartApi,
  type IPriceLine,
  type ISeriesApi,
  type SeriesMarker,
  type Time,
  createChart,
} from "lightweight-charts";
import { useEngine } from "@/lib/engine";
import { api } from "@/lib/api";

const UP = "#16d97e";
const DOWN = "#ff4d5e";

/**
 * Selectable timeframes. The bucket size must match the backend's, because
 * live bars are folded in the browser onto the same boundaries the history
 * came back on - otherwise the newest candle would straddle two buckets.
 */
export const TIMEFRAMES = [
  { key: "1s", label: "1s", seconds: 1 },
  { key: "1m", label: "1m", seconds: 60 },
  { key: "5m", label: "5m", seconds: 300 },
  { key: "10m", label: "10m", seconds: 600 },
  { key: "1h", label: "1h", seconds: 3600 },
] as const;

export type TimeframeKey = (typeof TIMEFRAMES)[number]["key"];

/** Floor a millisecond timestamp onto its bucket, in seconds. */
export function bucketStart(tsMs: number, bucketSeconds: number): number {
  const bucketMs = bucketSeconds * 1000;
  return Math.floor(tsMs / bucketMs) * bucketSeconds;
}

export function PriceChart({
  height = 280,
  initialTimeframe = "1m",
}: {
  height?: number;
  initialTimeframe?: TimeframeKey;
}) {
  const [timeframe, setTimeframe] = useState<TimeframeKey>(initialTimeframe);
  // Keyed by timeframe so switching derives "loading" during render instead of
  // setting state from inside the effect, which would cascade a second render.
  const [history, setHistory] = useState<{
    tf: TimeframeKey;
    state: "ready" | "empty" | "error";
  } | null>(null);
  const historyState = history?.tf === timeframe ? history.state : "loading";
  const bucketSeconds =
    TIMEFRAMES.find((t) => t.key === timeframe)?.seconds ?? 60;
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const barRef = useRef<CandlestickData<Time> | null>(null);
  const linesRef = useRef<IPriceLine[]>([]);
  const markersRef = useRef<SeriesMarker<Time>[]>([]);
  const lastSignalRef = useRef<string>("");

  const { tick, signal, lastEvent } = useEngine();

  // -------------------------------------------------------------- mount
  useEffect(() => {
    if (!containerRef.current) return;
    const chart = createChart(containerRef.current, {
      height,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#8b95ab",
        fontSize: 11,
      },
      grid: {
        vertLines: { color: "rgba(33,41,57,0.5)" },
        horzLines: { color: "rgba(33,41,57,0.5)" },
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: "#212939" },
      timeScale: {
        borderColor: "#212939",
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 4,
      },
      handleScale: { axisPressedMouseMove: { time: true, price: false } },
    });
    const series = chart.addCandlestickSeries({
      upColor: UP,
      downColor: DOWN,
      borderUpColor: UP,
      borderDownColor: DOWN,
      wickUpColor: UP,
      wickDownColor: DOWN,
      priceFormat: { type: "price", precision: 2, minMove: 0.01 },
    });
    chartRef.current = chart;
    seriesRef.current = series;

    const resize = () => {
      if (containerRef.current) {
        chart.applyOptions({ width: containerRef.current.clientWidth });
      }
    };
    resize();
    const observer = new ResizeObserver(resize);
    observer.observe(containerRef.current);

    return () => {
      observer.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      barRef.current = null;
      linesRef.current = [];
    };
  }, [height]);

  // ------------------------------------------------------------- history
  // The live stream can only ever show the session the tab has been open for.
  // Anything longer than that has to come from the recorded ticks.
  useEffect(() => {
    const series = seriesRef.current;
    if (!series) return;
    const controller = new AbortController();
    barRef.current = null;

    api
      .candles(timeframe, 400, controller.signal)
      .then((res) => {
        if (controller.signal.aborted) return;
        const current = seriesRef.current;
        if (!current) return;
        const data = res.candles.map((c) => ({
          time: c.time as Time,
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close,
        }));
        current.setData(data);
        // Carry the newest bar forward so the first live tick extends it
        // rather than opening a duplicate bar on the same bucket.
        barRef.current = data.length ? { ...data[data.length - 1] } : null;
        setHistory({ tf: timeframe, state: data.length ? "ready" : "empty" });
      })
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return;
        setHistory({ tf: timeframe, state: "error" });
      });

    return () => controller.abort();
  }, [timeframe]);

  // --------------------------------------------------------- tick stream
  useEffect(() => {
    const series = seriesRef.current;
    if (!series || !tick) return;
    const second = bucketStart(tick.ts, bucketSeconds) as Time;
    const bar = barRef.current;
    // A tick older than the bar we are building belongs to a bucket already
    // closed. lightweight-charts rejects an out-of-order update, so drop it.
    if (bar && (second as number) < (bar.time as number)) return;
    if (!bar || bar.time !== second) {
      barRef.current = {
        time: second,
        open: tick.price,
        high: tick.price,
        low: tick.price,
        close: tick.price,
      };
    } else {
      barRef.current = {
        ...bar,
        high: Math.max(bar.high, tick.price),
        low: Math.min(bar.low, tick.price),
        close: tick.price,
      };
    }
    series.update(barRef.current);
  }, [tick, bucketSeconds]);

  // ------------------------------------------------- trigger / entry lines
  useEffect(() => {
    const series = seriesRef.current;
    if (!series) return;
    for (const line of linesRef.current) series.removePriceLine(line);
    linesRef.current = [];
    if (!signal || signal.direction === "NO_TRADE") return;

    const color = signal.direction === "UP" ? UP : DOWN;
    linesRef.current.push(
      series.createPriceLine({
        price: signal.trigger_price,
        color,
        lineWidth: 2,
        lineStyle: 2,
        axisLabelVisible: true,
        title: "TRIGGER",
      }),
    );
    if (signal.entry_price) {
      linesRef.current.push(
        series.createPriceLine({
          price: signal.entry_price,
          color: "#4d9dff",
          lineWidth: 1,
          lineStyle: 0,
          axisLabelVisible: true,
          title: "ENTRY",
        }),
      );
    }
    if (signal.expiry_price) {
      linesRef.current.push(
        series.createPriceLine({
          price: signal.expiry_price,
          color: "#8b95ab",
          lineWidth: 1,
          lineStyle: 3,
          axisLabelVisible: true,
          title: "EXPIRY",
        }),
      );
    }
  }, [signal]);

  // ------------------------------------------------------------- markers
  useEffect(() => {
    const series = seriesRef.current;
    if (!series || !lastEvent) return;
    const key = `${lastEvent.signal.signal_id}:${lastEvent.event}`;
    if (lastSignalRef.current === key) return;
    lastSignalRef.current = key;

    const s = lastEvent.signal;
    let marker: SeriesMarker<Time> | null = null;
    if (lastEvent.event === "trigger_hit" && s.triggered_at) {
      marker = {
        time: bucketStart(s.triggered_at, bucketSeconds) as Time,
        position: s.direction === "UP" ? "belowBar" : "aboveBar",
        color: s.direction === "UP" ? UP : DOWN,
        shape: s.direction === "UP" ? "arrowUp" : "arrowDown",
        text: "TRIGGER",
      };
    } else if (lastEvent.event === "signal_settled" && s.settled_at && s.result) {
      marker = {
        time: bucketStart(s.settled_at, bucketSeconds) as Time,
        position: "aboveBar",
        color: s.result === "WIN" ? UP : s.result === "LOSS" ? DOWN : "#8b95ab",
        shape: "circle",
        text: s.result,
      };
    }
    if (!marker) return;
    markersRef.current = [...markersRef.current, marker].slice(-40);
    series.setMarkers(markersRef.current);
  }, [lastEvent, bucketSeconds]);

  return (
    <section className="panel overflow-hidden">
      <header className="flex flex-wrap items-center justify-between gap-2 border-b border-[var(--border)] px-3 py-2">
        <h2 className="label">
          Grafico · candele {timeframe}
          {historyState === "loading" && " · carico storico…"}
          {historyState === "empty" && " · nessuno storico registrato"}
          {historyState === "error" && " · storico non disponibile"}
        </h2>
        <div className="flex items-center gap-2">
          <div
            role="group"
            aria-label="Intervallo del grafico"
            className="flex overflow-hidden rounded border border-[var(--border)]"
          >
            {TIMEFRAMES.map((t) => (
              <button
                key={t.key}
                type="button"
                onClick={() => setTimeframe(t.key)}
                aria-pressed={timeframe === t.key}
                className={`px-2 py-0.5 text-[11px] transition-colors ${
                  timeframe === t.key
                    ? "bg-[var(--accent,#4d9dff)] text-black"
                    : "text-muted hover:text-[var(--fg)]"
                }`}
              >
                {t.label}
              </button>
            ))}
          </div>
          <span className="tnum text-[11px] text-muted">
            {tick ? `${tick.price.toFixed(2)}` : "—"}
          </span>
        </div>
      </header>
      <div ref={containerRef} className="w-full" style={{ height }} />
    </section>
  );
}
