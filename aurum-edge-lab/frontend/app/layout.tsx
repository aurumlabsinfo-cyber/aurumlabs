import type { Metadata } from "next";
import type { ReactNode } from "react";

import { Shell } from "@/components/Shell";
import { LiveProvider } from "@/lib/live";
import "./globals.css";

export const metadata: Metadata = {
  title: "AURUM EDGE LAB",
  description:
    "Autonomous research engine for crypto perpetual futures. Paper trading only.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        {/* One WebSocket for the whole app; every page reads the same state. */}
        <LiveProvider>
          <Shell>{children}</Shell>
        </LiveProvider>
      </body>
    </html>
  );
}
