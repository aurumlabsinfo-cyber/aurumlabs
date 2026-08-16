"use client";

import { useAlerts } from "@/lib/alerts";

export function Toaster() {
  const { toast } = useAlerts();
  if (!toast) return null;
  return (
    <div
      role="status"
      aria-live="polite"
      className="fixed bottom-4 left-1/2 z-50 -translate-x-1/2 px-3"
    >
      <div className="flash-in panel px-4 py-2 text-center shadow-lg">
        <div className="text-xs font-bold tracking-wider">{toast.title}</div>
        <div className="text-[11px] text-muted">{toast.body}</div>
      </div>
    </div>
  );
}
