/** Shapes mirrored from the backend's WebSocket and REST payloads. */

export type Direction = "UP" | "DOWN" | "NO_TRADE";

export type SignalStatus =
  | "WAITING"
  | "TRIGGERED"
  | "ACTIVE"
  | "EXPIRED"
  | "WIN"
  | "LOSS"
  | "TIE"
  | "CANCELLED";

export interface Tick {
  ts: number;
  exchange_ts: number;
  latency_ms: number;
  price: number;
  bid: number;
  bid_qty: number;
  ask: number;
  ask_qty: number;
  spread: number;
  spread_bps: number;
  micro_price: number;
  last_price: number | null;
  book_synced: boolean;
  is_synthetic: boolean;
}

export interface TradePrint {
  ts: number;
  price: number;
  quantity: number;
  notional: number;
  side: "BUY" | "SELL";
  trade_id: number;
}

export interface LiveSignal {
  signal_id: string;
  symbol: string;
  exchange: string;
  direction: Direction;
  status: SignalStatus;
  reference_price: number;
  trigger_price: number;
  horizon_s: number;
  confidence: number;
  prob_up: number;
  prob_down: number;
  prob_neutral: number;
  edge: number;
  regime: string;
  created_at: number;
  expires_wait_at: number;
  triggered_at: number | null;
  expires_at: number | null;
  settled_at: number | null;
  entry_price: number | null;
  expiry_price: number | null;
  result: string | null;
  pnl_units: number | null;
  data_quality: number;
  model_id: string | null;
  is_synthetic: boolean;
  server_ts: number;
  /** Authoritative at `server_ts`; the client re-derives it from the clock. */
  remaining_ms: number | null;
  wait_remaining_ms?: number | null;
}

export interface AgentOutput {
  agent: string;
  direction: Direction;
  confidence: number;
  score: number;
  reason: string;
  features_used: string[];
  timestamp: number;
  data_quality: number;
  extra?: Record<string, unknown>;
}

export interface AgentsMessage {
  ts: number;
  agents: AgentOutput[];
  regime: string;
  prob_up: number;
  prob_down: number;
  prob_neutral: number;
  no_trade_reasons: string[];
}

export interface OrderBookLevel {
  0: number;
  1: number;
}

export interface OrderBookSnapshot {
  exchange: string;
  symbol: string;
  last_update_id: number;
  synced: boolean;
  desync_reason: string | null;
  bids: [number, number][];
  asks: [number, number][];
  bid_levels: number;
  ask_levels: number;
  ts: number;
}

export interface ComponentHealth {
  status: "UP" | "DOWN" | "DEGRADED" | "DISABLED";
  detail?: unknown;
}

export interface Health {
  status: string;
  uptime_s: number;
  server_ts: number;
  symbol: string;
  source: string;
  is_synthetic: boolean;
  synthetic_warning: string | null;
  components: Record<string, ComponentHealth>;
  market: {
    feed_age_ms: number | null;
    tick_latency_ms: Record<string, number | null>;
    orderbook: Record<string, unknown>;
    data_quality: { score: number; ok: boolean; reasons: string[] };
    counters: Record<string, number>;
    adapters: Record<string, { connected: boolean; reconnects: number }>;
  };
  signals: Record<string, number>;
}

export interface Hello {
  symbol: string;
  horizon_s: number;
  is_synthetic: boolean;
  source: string;
  payout: number | null;
  payout_status: string;
}

export interface FeatureVector {
  ts: number;
  symbol: string;
  source: string;
  is_synthetic: boolean;
  features: Record<string, number | boolean | null>;
}

export interface SignalEvent {
  event: string;
  signal: LiveSignal;
  reason?: string;
}
