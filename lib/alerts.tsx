"use client";

/**
 * Alerting: visual, optional sound, optional browser notification.
 *
 * All three are independently switchable and every one defaults to OFF except
 * the visual flash. Sound is synthesised with WebAudio so there is no asset to
 * fetch, and browser notifications are only requested after an explicit opt-in.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useEngine } from "./engine";

export interface AlertSettings {
  visual: boolean;
  sound: boolean;
  browser: boolean;
}

const DEFAULTS: AlertSettings = { visual: true, sound: false, browser: false };
const STORAGE_KEY = "btcquant.alerts";

const EVENT_COPY: Record<string, { title: string; tone: number }> = {
  signal_created: { title: "NUOVO SEGNALE", tone: 660 },
  trigger_hit: { title: "TRIGGER RAGGIUNTO", tone: 880 },
  trade_active: { title: "TRADE ATTIVO", tone: 990 },
  trade_expired: { title: "TRADE SCADUTO", tone: 440 },
  signal_settled: { title: "RISULTATO", tone: 520 },
  signal_cancelled: { title: "SEGNALE ANNULLATO", tone: 330 },
};

interface AlertsContextValue {
  settings: AlertSettings;
  setSetting: (key: keyof AlertSettings, value: boolean) => void;
  toast: { id: number; title: string; body: string } | null;
}

const AlertsContext = createContext<AlertsContextValue | null>(null);

export function AlertsProvider({ children }: { children: React.ReactNode }) {
  const { lastEvent } = useEngine();
  const [settings, setSettings] = useState<AlertSettings>(DEFAULTS);
  const [toast, setToast] = useState<AlertsContextValue["toast"]>(null);
  const seenRef = useRef<string>("");
  const audioRef = useRef<AudioContext | null>(null);

  useEffect(() => {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) setSettings({ ...DEFAULTS, ...JSON.parse(raw) });
    } catch {
      /* storage unavailable: defaults are fine */
    }
  }, []);

  const setSetting = useCallback(
    (key: keyof AlertSettings, value: boolean) => {
      setSettings((prev) => {
        const next = { ...prev, [key]: value };
        try {
          localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
        } catch {
          /* ignore */
        }
        return next;
      });
      if (key === "browser" && value && "Notification" in window) {
        void Notification.requestPermission();
      }
    },
    [],
  );

  const beep = useCallback((frequency: number) => {
    try {
      audioRef.current ??= new AudioContext();
      const ctx = audioRef.current;
      if (ctx.state === "suspended") void ctx.resume();
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.frequency.value = frequency;
      osc.type = "sine";
      gain.gain.setValueAtTime(0.0001, ctx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.12, ctx.currentTime + 0.01);
      gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 0.18);
      osc.connect(gain).connect(ctx.destination);
      osc.start();
      osc.stop(ctx.currentTime + 0.2);
    } catch {
      /* autoplay policy or no audio device */
    }
  }, []);

  useEffect(() => {
    if (!lastEvent) return;
    const key = `${lastEvent.signal.signal_id}:${lastEvent.event}`;
    if (seenRef.current === key) return;
    seenRef.current = key;

    const copy = EVENT_COPY[lastEvent.event];
    if (!copy) return;
    const sig = lastEvent.signal;
    const body =
      lastEvent.event === "signal_settled"
        ? `${sig.result ?? sig.status} · ${sig.direction}`
        : `${sig.direction === "UP" ? "SU" : "GIÙ"} · trigger ${sig.trigger_price}`;

    if (settings.visual) {
      setToast({ id: Date.now(), title: copy.title, body });
    }
    if (settings.sound) beep(copy.tone);
    if (
      settings.browser &&
      typeof Notification !== "undefined" &&
      Notification.permission === "granted"
    ) {
      try {
        new Notification(copy.title, { body, tag: sig.signal_id });
      } catch {
        /* some browsers require a service worker */
      }
    }
  }, [lastEvent, settings, beep]);

  useEffect(() => {
    if (!toast) return;
    const id = setTimeout(() => setToast(null), 2600);
    return () => clearTimeout(id);
  }, [toast]);

  const value = useMemo(
    () => ({ settings, setSetting, toast }),
    [settings, setSetting, toast],
  );

  return (
    <AlertsContext.Provider value={value}>{children}</AlertsContext.Provider>
  );
}

export function useAlerts(): AlertsContextValue {
  const ctx = useContext(AlertsContext);
  if (!ctx) throw new Error("useAlerts must be used inside <AlertsProvider>");
  return ctx;
}
