"use client";

import { useState } from "react";

import { Async, Bps, Empty, Note, Panel, Pill, Stat } from "@/components/ui";
import { useApi } from "@/lib/api";
import {
  duration,
  eur,
  integer,
  num,
  pct,
  price,
  signedEur,
  stamp,
  tone,
} from "@/lib/format";
import { useLive } from "@/lib/live";
import type { TradeRow } from "@/lib/types";

interface TradesPayload {
  count: number;
  broker: {
    live_orders_possible: boolean;
    open_positions: number;
    opened: number;
    closed: number;
    wins: number;
    losses: number;
    win_rate: number;
    profit_factor: number;
    net_pnl_eur: number;
    fees_eur: number;
    exposure_eur: number;
    mean_entry_slippage_bps: number;
    exit_reasons: Record<string, number>;
  };
  trades: TradeRow[];
}

interface WhyPayload {
  trade: TradeRow;
  why: {
    hypothesis: { description: string; agent: string; conditions: ConditionSpec[] } | null;
    strategy: { strategy_id: string; state: string; version: number } | null;
    signal: {
      confidence: number;
      expected_edge_bps: number;
      expected_cost_bps: number;
      net_edge_bps: number;
      regime: string;
      ts_ms: number;
    } | null;
    conditions_at_entry: { condition: ConditionSpec; value_at_entry: number | null }[];
    cost_breakdown: {
      entry_slippage_bps: number;
      exit_slippage_bps: number;
      fees_eur: number;
      total_cost_bps: number;
      cost_model_version: string;
    };
    outcome: {
      expected_edge_bps: number;
      realised_return_bps: number;
      net_return_bps: number;
      exit_reason: string;
    };
  };
}

interface ConditionSpec {
  feature: string;
  op: string;
  percentile: number;
  threshold: number;
}

export default function PaperTradingPage() {
  const { state } = useLive();
  const trades = useApi<TradesPayload>("/trades?limit=120", 4000);
  const [selected, setSelected] = useState<string | null>(null);
  const why = useApi<WhyPayload>(selected ? `/trades/${selected}` : null, 0);

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">Paper Trading</h1>
          <p className="page-sub">
            Open positions, closed trades, and the WHY THIS TRADE view: the exact feature values at
            the moment of the decision, the costs charged, and the strategy that asked for it.
          </p>
        </div>
      </div>

      <Note kind="info">
        <strong>Paper only.</strong> No code path in this system can place an order on any venue.
        Fills are simulated against the live book by walking the resting depth, and the requested
        price is stored beside the filled one on every trade.
      </Note>

      <Async loading={trades.loading} error={trades.error}>
        {trades.data ? (
          <div className="grid cols-4">
            <Panel title="Result">
              <Stat
                label="Net P&L"
                value={signedEur(trades.data.broker.net_pnl_eur)}
                tone={tone(trades.data.broker.net_pnl_eur)}
                hint={`${integer(trades.data.broker.closed)} closed · fees ${eur(
                  trades.data.broker.fees_eur,
                  4,
                )}`}
              />
            </Panel>
            <Panel title="Hit rate">
              <Stat
                label="Wins / losses"
                value={`${integer(trades.data.broker.wins)} / ${integer(trades.data.broker.losses)}`}
                hint={`${pct(trades.data.broker.win_rate * 100, 1)} · PF ${num(
                  trades.data.broker.profit_factor,
                  2,
                )}`}
              />
            </Panel>
            <Panel title="Execution quality">
              <Stat
                label="Mean entry slippage"
                value={`${num(trades.data.broker.mean_entry_slippage_bps, 3)} bps`}
                tone={trades.data.broker.mean_entry_slippage_bps > 2 ? "warn" : "flat"}
                hint="filled price versus requested price"
              />
            </Panel>
            <Panel title="Exposure">
              <Stat
                label="Open notional"
                value={eur(trades.data.broker.exposure_eur)}
                hint={`${integer(trades.data.broker.open_positions)} open positions`}
              />
            </Panel>
          </div>
        ) : null}
      </Async>

      <Panel title="Open positions" note={`${state?.positions.length ?? 0} open`} flush>
        {!state || state.positions.length === 0 ? (
          <Empty>No open positions.</Empty>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>Side</th>
                  <th className="num">Qty</th>
                  <th className="num">Requested</th>
                  <th className="num">Filled</th>
                  <th className="num">Mark</th>
                  <th className="num">Notional</th>
                  <th className="num">Margin</th>
                  <th className="num">Unrealised</th>
                  <th className="num">Stop / target</th>
                  <th className="num">Held</th>
                </tr>
              </thead>
              <tbody>
                {state.positions.map((position) => (
                  <tr key={position.position_id}>
                    <td className="mono">{position.symbol}</td>
                    <td className={position.direction === "LONG" ? "up" : "down"}>
                      {position.direction}
                    </td>
                    <td className="num">{num(position.qty, 6)}</td>
                    <td className="num faint">{price(position.requested_entry_price)}</td>
                    <td className="num">{price(position.entry_price)}</td>
                    <td className="num">{price(position.mark_price)}</td>
                    <td className="num">{eur(position.notional_eur)}</td>
                    <td className="num faint">{eur(position.margin_eur)}</td>
                    <td className={`num ${tone(position.unrealized_pnl_eur)}`}>
                      {signedEur(position.unrealized_pnl_eur)}
                    </td>
                    <td className="num faint">
                      {num(position.stop_bps, 1)} / {num(position.target_bps, 1)}
                    </td>
                    <td className="num">
                      {duration(Date.now() - position.entry_ts_ms)} /{" "}
                      {duration(position.horizon_ms)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>

      <Panel
        title="Trade history"
        note={trades.data ? `${integer(trades.data.count)} shown — click a row for WHY THIS TRADE` : ""}
        flush
      >
        <Async
          loading={trades.loading}
          error={trades.error}
          empty={trades.data?.trades.length === 0}
          emptyMessage="No paper trades yet. Nothing trades until a strategy has passed every gate."
        >
          <div className="table-wrap scroll-y">
            <table>
              <thead>
                <tr>
                  <th>Closed</th>
                  <th>Symbol</th>
                  <th>Side</th>
                  <th className="num">Entry</th>
                  <th className="num">Exit</th>
                  <th className="num">Gross bps</th>
                  <th className="num">Cost bps</th>
                  <th className="num">Net bps</th>
                  <th className="num">Net €</th>
                  <th className="num">Held</th>
                  <th>Exit</th>
                  <th>Regime</th>
                </tr>
              </thead>
              <tbody>
                {trades.data?.trades.map((trade) => (
                  <tr
                    key={trade.trade_id}
                    onClick={() => setSelected(trade.trade_id)}
                    style={{
                      cursor: "pointer",
                      background: trade.trade_id === selected ? "var(--bg-hover)" : undefined,
                    }}
                  >
                    <td className="faint mono" style={{ fontSize: 11 }}>
                      {stamp(trade.exit_ts_ms)}
                    </td>
                    <td className="mono">{trade.symbol}</td>
                    <td className={trade.direction === "LONG" ? "up" : "down"}>{trade.direction}</td>
                    <td className="num">{price(trade.entry_price)}</td>
                    <td className="num">{price(trade.exit_price)}</td>
                    <td className="num">
                      <Bps value={trade.return_bps} />
                    </td>
                    <td className="num down">{num(trade.cost_bps, 2)}</td>
                    <td className="num">
                      <Bps value={trade.net_return_bps} />
                    </td>
                    <td className={`num ${tone(trade.net_pnl_eur)}`}>{signedEur(trade.net_pnl_eur)}</td>
                    <td className="num faint">{duration(trade.holding_ms)}</td>
                    <td className="faint">{trade.exit_reason}</td>
                    <td className="faint">{trade.regime}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Async>
      </Panel>

      {selected ? (
        <Async loading={why.loading} error={why.error}>
          {why.data ? <WhyThisTrade payload={why.data} /> : null}
        </Async>
      ) : null}
    </>
  );
}

function WhyThisTrade({ payload }: { payload: WhyPayload }) {
  const { trade, why } = payload;
  const predicted = why.outcome.expected_edge_bps;
  const realised = why.outcome.realised_return_bps;

  return (
    <Panel title="Why this trade" note={trade.trade_id}>
      <div className="grid cols-2">
        <div>
          <div className="stat-label" style={{ marginBottom: 5 }}>
            The claim
          </div>
          <p style={{ marginTop: 0, fontSize: 13 }}>
            {why.hypothesis?.description ?? "The originating hypothesis is no longer on file."}
          </p>
          <div className="chips" style={{ marginBottom: 10 }}>
            {why.hypothesis ? <Pill kind="info">{why.hypothesis.agent}</Pill> : null}
            {why.strategy ? (
              <>
                <Pill>{why.strategy.state}</Pill>
                <Pill>v{why.strategy.version}</Pill>
              </>
            ) : null}
            <Pill>{trade.regime}</Pill>
          </div>

          <div className="stat-label" style={{ marginBottom: 5 }}>
            Conditions at the moment of the decision
          </div>
          {why.conditions_at_entry.length === 0 ? (
            <Empty>No conditions recorded.</Empty>
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Feature</th>
                    <th>Required</th>
                    <th className="num">Threshold</th>
                    <th className="num">Actual</th>
                    <th>Held?</th>
                  </tr>
                </thead>
                <tbody>
                  {why.conditions_at_entry.map((row) => {
                    const value = row.value_at_entry;
                    const held =
                      value === null
                        ? null
                        : row.condition.op.startsWith(">")
                          ? value >= row.condition.threshold
                          : value <= row.condition.threshold;
                    return (
                      <tr key={row.condition.feature + row.condition.op}>
                        <td className="mono">{row.condition.feature}</td>
                        <td>
                          {row.condition.op} p{num(row.condition.percentile, 0)}
                        </td>
                        <td className="num faint">{num(row.condition.threshold, 6)}</td>
                        <td className="num">{value === null ? "—" : num(value, 6)}</td>
                        <td>
                          {held === null ? (
                            <span className="faint">—</span>
                          ) : (
                            <Pill kind={held ? "ok" : "bad"}>{held ? "yes" : "no"}</Pill>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <div>
          <div className="stat-label" style={{ marginBottom: 5 }}>
            What it cost
          </div>
          <dl className="kv">
            <dt>requested entry</dt>
            <dd>{price(trade.requested_entry_price)}</dd>
            <dt>filled entry</dt>
            <dd>{price(trade.entry_price)}</dd>
            <dt>entry slippage</dt>
            <dd className="down">{num(why.cost_breakdown.entry_slippage_bps, 3)} bps</dd>
            <dt>requested exit</dt>
            <dd>{price(trade.requested_exit_price)}</dd>
            <dt>filled exit</dt>
            <dd>{price(trade.exit_price)}</dd>
            <dt>exit slippage</dt>
            <dd className="down">{num(why.cost_breakdown.exit_slippage_bps, 3)} bps</dd>
            <dt>fees</dt>
            <dd className="down">{eur(why.cost_breakdown.fees_eur, 6)}</dd>
            <dt>total cost</dt>
            <dd className="down">{num(why.cost_breakdown.total_cost_bps, 3)} bps</dd>
            <dt>cost model</dt>
            <dd className="faint">{why.cost_breakdown.cost_model_version}</dd>
          </dl>

          <div className="stat-label" style={{ margin: "12px 0 5px" }}>
            Predicted versus realised
          </div>
          <div className="grid cols-3" style={{ gap: 10 }}>
            <Stat label="Expected edge" value={<Bps value={predicted} />} small />
            <Stat label="Realised gross" value={<Bps value={realised} />} small />
            <Stat
              label="Realised net"
              value={<Bps value={why.outcome.net_return_bps} />}
              small
              tone={tone(why.outcome.net_return_bps)}
            />
          </div>
          <p className="faint" style={{ fontSize: 11.5, marginTop: 8, marginBottom: 0 }}>
            {predicted > 0 && realised < predicted * 0.3
              ? "The move was materially smaller than predicted. Repeated across trades this is a calibration failure, and the post-mortem agent reports it as one."
              : predicted > 0 && why.outcome.net_return_bps < 0 && realised > 0
                ? "The direction was right and the costs took it. That is the most common way a real edge loses money at these horizons."
                : "Exited on " + why.outcome.exit_reason + "."}
          </p>
        </div>
      </div>

      {trade.features && Object.keys(trade.features).length ? (
        <details style={{ marginTop: 12 }}>
          <summary className="stat-label" style={{ cursor: "pointer" }}>
            Full feature snapshot at decision time ({Object.keys(trade.features).length} values)
          </summary>
          <div className="table-wrap scroll-y" style={{ marginTop: 8 }}>
            <table>
              <thead>
                <tr>
                  <th>Feature</th>
                  <th className="num">Value</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(trade.features)
                  .sort(([a], [b]) => a.localeCompare(b))
                  .map(([key, value]) => (
                    <tr key={key}>
                      <td className="mono">{key}</td>
                      <td className="num">{num(value, 6)}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        </details>
      ) : null}
    </Panel>
  );
}
