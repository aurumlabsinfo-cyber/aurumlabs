import type { Metadata, Viewport } from "next";
import "./globals.css";
import { EngineProvider } from "@/lib/engine";
import { AlertsProvider } from "@/lib/alerts";
import { TopBar } from "@/components/TopBar";
import { Toaster } from "@/components/Toaster";

export const metadata: Metadata = {
  title: "BTC 5-Second Quant Engine",
  description:
    "Live BTC market data, microstructure analysis and paper-only 5 second signal evaluation.",
};

export const viewport: Viewport = {
  themeColor: "#06070a",
  width: "device-width",
  initialScale: 1,
  maximumScale: 1,
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="it" className="h-full">
      <body className="min-h-full flex flex-col">
        <EngineProvider>
          <AlertsProvider>
            <TopBar />
            <main className="flex-1 w-full mx-auto max-w-6xl px-3 pb-10 sm:px-4">
              {children}
            </main>
            <Toaster />
          </AlertsProvider>
        </EngineProvider>
      </body>
    </html>
  );
}
