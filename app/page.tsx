import { SignalCard } from "@/components/SignalCard";
import { PriceChart } from "@/components/PriceChart";
import { SignalHistory } from "@/components/SignalHistory";
import { StatsPanel } from "@/components/StatsPanel";

/**
 * SIMPLE MODE.
 *
 * One job: the signal must be readable in under a second. Everything that is
 * not the direction, the trigger, the duration, the countdown, the status and
 * the confidence lives in PRO mode.
 */
export default function SimpleModePage() {
  return (
    <div className="space-y-4 pt-4">
      <SignalCard />
      <PriceChart height={260} />
      <SignalHistory />
      <StatsPanel compact />
    </div>
  );
}
