import { AgentsPanel } from "@/components/AgentsPanel";
import { HealthPanel } from "@/components/HealthPanel";
import { MicrostructurePanel } from "@/components/MicrostructurePanel";
import { OrderBookPanel } from "@/components/OrderBookPanel";
import { PriceChart } from "@/components/PriceChart";
import { SignalCard } from "@/components/SignalCard";
import { SignalHistory } from "@/components/SignalHistory";
import { StatsPanel } from "@/components/StatsPanel";
import { TradesTape } from "@/components/TradesTape";

/** PRO MODE: the full instrument panel behind the decision. */
export default function ProModePage() {
  return (
    <div className="space-y-4 pt-4">
      <SignalCard />
      <PriceChart height={320} />
      <div className="grid gap-4 lg:grid-cols-2">
        <OrderBookPanel levels={12} />
        <TradesTape />
      </div>
      <AgentsPanel />
      <MicrostructurePanel />
      <HealthPanel />
      <StatsPanel />
      <SignalHistory limit={20} />
    </div>
  );
}
