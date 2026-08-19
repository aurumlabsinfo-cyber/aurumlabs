/** Shapes the backend actually returns.  Kept narrow: only what the UI reads. */

export interface WalletState {
  cycle_id: number;
  currency: string;
  starting_balance: number;
  balance: number;
  available: number;
  reserved: number;
  equity: number;
  realized_pnl: number;
  unrealized_pnl: number;
  fees_paid: number;
  peak_equity: number;
  drawdown_pct: number;
  return_pct: number;
  trades: number;
  wins: number;
  losses: number;
  win_rate: number;
  daily_pnl_pct?: number;
}

export interface QualityState {
  symbol: string;
  score: number;
  state: string;
  flags: string[];
  tradable: boolean;
  latency_ms: number;
  spread_bps: number | null;
  events_per_min: number;
  sequence_gaps: number;
  resyncs: number;
  book_levels: number;
}

export interface MarketRow {
  symbol: string;
  tier: string;
  role: string;
  mid: number | null;
  microprice: number | null;
  best_bid: number | null;
  best_ask: number | null;
  spread_bps: number | null;
  last_price: number | null;
  mark_price: number | null;
  index_price: number | null;
  funding_rate: number;
  book_state: string;
  trades_buffered: number;
  quality: QualityState | null;
}

export interface MetricSet {
  samples: number;
  wins: number;
  losses: number;
  win_rate: number;
  gross_edge_bps: number;
  net_edge_bps: number;
  cost_bps: number;
  avg_win_bps: number;
  avg_loss_bps: number;
  profit_factor: number;
  max_drawdown_bps: number;
  tail_loss_bps: number;
  stdev_bps: number;
  t_stat: number;
  p_value: number;
  ci_low_bps: number;
  ci_high_bps: number;
  events_per_hour: number;
}

export interface StrategyRow {
  strategy_id: string;
  name: string;
  state: string;
  version: number;
  score: number;
  can_trade: boolean;
  symbol: string;
  signal_symbol: string;
  direction: string;
  horizon_ms: number;
  entry_delay_ms: number;
  description: string;
  hypothesis_id: string;
  agent: string;
  conditions: ConditionRow[];
  validated_metrics: MetricSet;
  shadow_metrics: MetricSet;
  live_metrics: MetricSet;
  shadow_signals: number;
  created_ms: number;
  updated_ms: number;
  promoted_ms: number;
  last_reason: string;
  live_trades: number;
  live_net_pnl_eur: number;
}

export interface ConditionRow {
  feature: string;
  op: string;
  percentile: number;
  threshold: number;
  samples: number;
}

export interface PositionRow {
  position_id: string;
  symbol: string;
  direction: string;
  qty: number;
  entry_ts_ms: number;
  entry_price: number;
  requested_entry_price: number;
  notional_eur: number;
  margin_eur: number;
  mark_price: number;
  unrealized_pnl_eur: number;
  strategy_id: string;
  horizon_ms: number;
  stop_bps: number;
  target_bps: number;
}

export interface TradeRow {
  trade_id: string;
  symbol: string;
  direction: string;
  qty: number;
  entry_ts_ms: number;
  exit_ts_ms: number;
  holding_ms: number;
  requested_entry_price: number;
  entry_price: number;
  requested_exit_price: number;
  exit_price: number;
  gross_pnl_eur: number;
  fees_eur: number;
  net_pnl_eur: number;
  return_bps: number;
  net_return_bps: number;
  entry_slippage_bps: number;
  exit_slippage_bps: number;
  cost_bps: number;
  expected_edge_bps: number;
  exit_reason: string;
  strategy_id: string;
  hypothesis_id: string;
  cycle_id: number;
  regime: string;
  cost_model_version: string;
  features?: Record<string, number>;
}

export interface AgentRow {
  name: string;
  family: string;
  description: string;
  enabled: boolean;
  inputs?: string[];
  horizons_ms?: number[];
  discovery_fraction?: number;
  min_abs_correlation?: number;
  runs?: number;
  last_run_ms?: number;
  note?: string;
  metrics?: {
    runs: number;
    features_read: number;
    candidates_ranked: number;
    proposed: number;
    blocked_by_memory: number;
    dropped_unresolvable: number;
    dropped_weak: number;
    errors: number;
    last_run_ms: number;
    last_error: string;
    last_proposals: string[];
    best_ranks: { feature: string; correlation: number; samples: number; direction: string }[];
  };
}

export interface RejectionRow {
  reason: string;
  count: number;
  percent: number;
  total_since_start: number;
  explanation: string;
  action: string;
}

export interface CycleRow {
  cycle_id: number;
  state: string;
  started_ms: number;
  ended_ms: number | null;
  starting_balance: number;
  final_balance: number | null;
  peak_equity: number;
  max_drawdown_pct: number;
  trades: number;
  wins: number;
  losses: number;
  net_pnl: number;
  champion_strategy_id: string | null;
  end_reason: string;
}

export interface PostMortemRow {
  cycle_id: number;
  created_ms: number;
  verdict: string;
  primary_cause: string;
  causes: { cause: string; weight: number; detail: string }[];
  evidence: Record<string, unknown>;
  recommendations: string[];
  trades_analyzed: number;
  net_pnl: number;
}

export interface CrossMarketCell {
  leader: string;
  follower: string;
  correlation: number;
  best_lag_ms: number;
  best_correlation: number;
  t_stat: number;
  samples: number;
  predictive_score: number;
  conditional_edge_bps: number;
  conditional_net_edge_bps: number;
  conditional_samples: number;
  cost_bps: number;
}

export interface LiveState {
  type: string;
  ts_ms: number | null;
  status: string;
  feed: {
    kind: string;
    live: boolean;
    state: string;
    symbols_live: number;
    symbols_configured: number;
    events_processed: number;
  };
  markets: MarketRow[];
  wallet: WalletState;
  cycle: {
    cycle: CycleRow | null;
    state: string;
    active: boolean;
    blocked_reason: string;
    resets: number;
    postmortems: number;
    last_postmortem: PostMortemRow | null;
    failure_floor_eur: number;
  };
  positions: PositionRow[];
  recent_trades: TradeRow[];
  champion: StrategyRow | null;
  no_edge_reason: string;
  strategy_counts: Record<string, number>;
  research: {
    cycles_run: number;
    hypotheses: number;
    validated: number;
    rejected: number;
    memory_entries: number;
    shadow_pending: number;
  };
  agents: {
    name: string;
    family: string;
    runs: number;
    proposed: number;
    blocked_by_memory: number;
    errors: number;
    last_run_ms: number;
  }[];
  data_quality: {
    symbols: number;
    tradable: number;
    mean_score: number;
    min_score_to_trade: number;
    blocked: Record<string, string[]>;
    worst: { symbol: string; score: number; state: string } | null;
  };
  diagnostics: {
    summary: string;
    accepted: number;
    evaluated: number;
    top_rejections: RejectionRow[];
  };
  risk: {
    entries_blocked: boolean;
    block_reason: string;
    drawdown_pct: number;
    max_drawdown_pct: number;
    daily_pnl_pct: number;
    daily_loss_limit_pct: number;
    risk_per_trade_pct: number;
    max_concurrent_positions: number;
    max_exposure_pct: number;
    rejections: Record<string, number>;
  };
  errors: { ts_ms: number; component: string; error: string }[];
}
