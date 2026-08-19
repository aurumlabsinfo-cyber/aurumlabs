"use client";

import { useState } from "react";

import { Async, Bps, Empty, Note, Panel, Pill, Stat } from "@/components/ui";
import { useApi } from "@/lib/api";
import { ago, duration, integer, num, pct, signedEur, stamp, tone } from "@/lib/format";
import type { MetricSet, StrategyRow } from "@/lib/types";

interface StrategiesPayload {
  counts: Record<string, number>;
  transitions: number;
  strategies: StrategyRow[];
}

interface StrategyDetail {
  strategy: StrategyRow;
  versions: {
    version: number;
    state: string;
    previous_state: string | null;
    reason: string;
    created_ms: number;
    evidence: Record<string, unknown>;
  }[];
  trades: { trade_id: string; net_pnl_eur: number; net_return_bps: number; exit_reason: string; exit_ts_ms: number }[];
}

interface ChampionPayload {
  champion: StrategyRow | null;
  state: string;
  reason?: string;
  shadows_in_evaluation?: number;
  note?: string;
}

const STATE_KIND: Record<string, "ok" | "warn" | "bad" | "info" | "neutral"> = {
  RESEARCH: "neutral",
  CANDIDATE: "info",
  CHALLENGER: "info",
  SHADOW: "warn",
  CHAMPION: "ok",
  DEGRADED: "bad",
  RETIRED: "neutral",
  REJECTED: "bad",
};

const LIFECYCLE = ["RESEARCH", "CANDIDATE", "CHALLENGER", "SHADOW", "CHAMPION"];

export default function StrategiesPage() {
  const list = useApi<StrategiesPayload>("/strategies", 4000);
  const champion = useApi<ChampionPayload>("/champion", 4000);
  const [selected, setSelected] = useState<string | null>(null);
  const detail = useApi<StrategyDetail>(selected ? `/strategies/${selected}` : null, 5000);

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">Strategies</h1>
          <p className="page-sub">
            The lifecycle from RESEARCH to CHAMPION, with the evidence behind every promotion and the
            version history that makes it reversible only through a new version.
          </p>
        </div>
      </div>

      <Async loading={champion.loading} error={champion.error}>
        {champion.data ? (
          champion.data.champion ? (
            <Panel title="Champion" note={champion.data.champion.strategy_id}>
              <StrategySummary strategy={champion.data.champion} />
            </Panel>
          ) : (
            <Note kind="warn">
              <strong>No champion.</strong> {champion.data.reason}
              {champion.data.shadows_in_evaluation ? (
                <>
                  {" "}
                  {champion.data.shadows_in_evaluation} strategy/ies are in live shadow evaluation.
                </>
              ) : null}
              <div className="faint" style={{ marginTop: 6, fontSize: 12 }}>
                {champion.data.note}
              </div>
            </Note>
          )
        ) : null}
      </Async>

      <Async loading={list.loading} error={list.error}>
        {list.data ? (
          <>
            <Panel title="Lifecycle" note={`${integer(list.data.transitions)} transitions recorded`}>
              <div style={{ display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
                {LIFECYCLE.map((state, index) => (
                  <div key={state} style={{ display: "flex", alignItems: "center", gap: 10 }}>
                    <div style={{ textAlign: "center" }}>
                      <div className="stat-value sm mono">{integer(list.data!.counts[state] ?? 0)}</div>
                      <Pill kind={STATE_KIND[state]}>{state}</Pill>
                    </div>
                    {index < LIFECYCLE.length - 1 ? <span className="faint">→</span> : null}
                  </div>
                ))}
                <div style={{ width: 22 }} />
                {["DEGRADED", "RETIRED", "REJECTED"].map((state) => (
                  <div key={state} style={{ textAlign: "center" }}>
                    <div className="stat-value sm mono">{integer(list.data!.counts[state] ?? 0)}</div>
                    <Pill kind={STATE_KIND[state]}>{state}</Pill>
                  </div>
                ))}
              </div>
              <p className="faint" style={{ fontSize: 11.5, marginTop: 12, marginBottom: 0 }}>
                Only a CHAMPION may touch the wallet, and only on paper. SHADOW emits real signals
                against the live feed with no wallet impact — most of what dies there dies because
                the spread at the moment the signal fired was not the spread the backtest averaged.
              </p>
            </Panel>

            <Panel title="All strategies" note="ranked by score" flush>
              {list.data.strategies.length === 0 ? (
                <Empty>
                  No strategy has been created yet. A strategy exists only once a hypothesis has
                  passed every validation gate.
                </Empty>
              ) : (
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>State</th>
                        <th>Agent</th>
                        <th>Symbol</th>
                        <th>Dir</th>
                        <th className="num">Score</th>
                        <th className="num">Validated</th>
                        <th className="num">Shadow</th>
                        <th className="num">Live</th>
                        <th className="num">Trades</th>
                        <th className="num">P&amp;L</th>
                        <th className="num">v</th>
                        <th>Updated</th>
                      </tr>
                    </thead>
                    <tbody>
                      {list.data.strategies.map((strategy) => (
                        <tr
                          key={strategy.strategy_id}
                          onClick={() => setSelected(strategy.strategy_id)}
                          style={{
                            cursor: "pointer",
                            background:
                              strategy.strategy_id === selected ? "var(--bg-hover)" : undefined,
                          }}
                        >
                          <td>
                            <Pill kind={STATE_KIND[strategy.state] ?? "neutral"}>
                              {strategy.state}
                            </Pill>
                          </td>
                          <td className="faint">{strategy.agent}</td>
                          <td className="mono">{strategy.symbol}</td>
                          <td className={strategy.direction === "LONG" ? "up" : "down"}>
                            {strategy.direction}
                          </td>
                          <td className="num">{num(strategy.score, 3)}</td>
                          <td className="num">
                            <Bps value={strategy.validated_metrics.net_edge_bps} />
                          </td>
                          <td className="num">
                            {strategy.shadow_metrics.samples ? (
                              <Bps value={strategy.shadow_metrics.net_edge_bps} />
                            ) : (
                              <span className="faint">{strategy.shadow_signals || "—"}</span>
                            )}
                          </td>
                          <td className="num">
                            {strategy.live_metrics.samples ? (
                              <Bps value={strategy.live_metrics.net_edge_bps} />
                            ) : (
                              <span className="faint">—</span>
                            )}
                          </td>
                          <td className="num">{integer(strategy.live_trades)}</td>
                          <td className={`num ${tone(strategy.live_net_pnl_eur)}`}>
                            {strategy.live_trades ? signedEur(strategy.live_net_pnl_eur) : "—"}
                          </td>
                          <td className="num faint">{strategy.version}</td>
                          <td className="faint mono" style={{ fontSize: 11 }}>
                            {ago(strategy.updated_ms)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Panel>
          </>
        ) : null}
      </Async>

      {selected ? (
        <Async loading={detail.loading} error={detail.error}>
          {detail.data ? (
            <>
              <Panel title="Strategy detail" note={detail.data.strategy.strategy_id}>
                <StrategySummary strategy={detail.data.strategy} />
              </Panel>

              <div className="grid cols-2">
                <Panel title="Version history" note="append-only" flush>
                  <div className="table-wrap scroll-y">
                    <table>
                      <thead>
                        <tr>
                          <th className="num">v</th>
                          <th>Transition</th>
                          <th className="wrap">Reason</th>
                          <th>When</th>
                        </tr>
                      </thead>
                      <tbody>
                        {detail.data.versions.map((version) => (
                          <tr key={version.version}>
                            <td className="num">{version.version}</td>
                            <td>
                              <span className="faint">{version.previous_state ?? "∅"}</span>{" "}
                              <span className="faint">→</span>{" "}
                              <Pill kind={STATE_KIND[version.state] ?? "neutral"}>
                                {version.state}
                              </Pill>
                            </td>
                            <td className="wrap" style={{ fontSize: 11.5 }}>
                              {version.reason}
                            </td>
                            <td className="faint mono" style={{ fontSize: 11 }}>
                              {stamp(version.created_ms)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </Panel>

                <Panel title="Trades from this strategy" flush>
                  {detail.data.trades.length === 0 ? (
                    <Empty>This strategy has not traded.</Empty>
                  ) : (
                    <div className="table-wrap scroll-y">
                      <table>
                        <thead>
                          <tr>
                            <th>Closed</th>
                            <th className="num">Net €</th>
                            <th className="num">Net bps</th>
                            <th>Exit</th>
                          </tr>
                        </thead>
                        <tbody>
                          {detail.data.trades.map((trade) => (
                            <tr key={trade.trade_id}>
                              <td className="faint mono" style={{ fontSize: 11 }}>
                                {stamp(trade.exit_ts_ms)}
                              </td>
                              <td className={`num ${tone(trade.net_pnl_eur)}`}>
                                {signedEur(trade.net_pnl_eur)}
                              </td>
                              <td className="num">
                                <Bps value={trade.net_return_bps} />
                              </td>
                              <td className="faint">{trade.exit_reason}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </Panel>
              </div>
            </>
          ) : null}
        </Async>
      ) : null}
    </>
  );
}

function StrategySummary({ strategy }: { strategy: StrategyRow }) {
  return (
    <div>
      <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
        <Pill kind={STATE_KIND[strategy.state] ?? "neutral"} dot>
          {strategy.state}
        </Pill>
        <Pill kind="info">{strategy.agent}</Pill>
        <Pill>v{strategy.version}</Pill>
        <Pill>{strategy.symbol}</Pill>
        <span className="faint mono" style={{ fontSize: 11.5 }}>
          score {num(strategy.score, 4)}
        </span>
      </div>

      <p style={{ margin: "10px 0 4px", fontSize: 13 }}>{strategy.description}</p>
      {strategy.last_reason ? (
        <p className="faint" style={{ fontSize: 11.5, margin: "0 0 10px" }}>
          last transition: {strategy.last_reason}
        </p>
      ) : null}

      <div className="grid cols-3" style={{ gap: 12 }}>
        <MetricsCard title="Validated (holdout)" metrics={strategy.validated_metrics} />
        <MetricsCard
          title={`Shadow (${integer(strategy.shadow_signals)} signals)`}
          metrics={strategy.shadow_metrics}
        />
        <MetricsCard title={`Live (${integer(strategy.live_trades)} trades)`} metrics={strategy.live_metrics} />
      </div>

      {strategy.conditions.length ? (
        <div style={{ marginTop: 12 }}>
          <div className="stat-label" style={{ marginBottom: 5 }}>
            Entry conditions on {strategy.signal_symbol}
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Feature</th>
                  <th>Test</th>
                  <th className="num">Percentile</th>
                  <th className="num">Threshold</th>
                  <th className="num">Fitted on</th>
                </tr>
              </thead>
              <tbody>
                {strategy.conditions.map((condition) => (
                  <tr key={condition.feature + condition.op}>
                    <td className="mono">{condition.feature}</td>
                    <td>{condition.op}</td>
                    <td className="num">p{num(condition.percentile, 0)}</td>
                    <td className="num">{num(condition.threshold, 6)}</td>
                    <td className="num faint">{integer(condition.samples)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="faint" style={{ fontSize: 11.5, marginTop: 6, marginBottom: 0 }}>
            Thresholds are percentiles of the feature&apos;s own distribution, so the same claim
            survives a change in market conditions and research memory recognises a refit as the
            same idea. Horizon {duration(strategy.horizon_ms)}
            {strategy.entry_delay_ms ? `, entered ${duration(strategy.entry_delay_ms)} after the signal` : ""}.
          </p>
        </div>
      ) : null}
    </div>
  );
}

function MetricsCard({ title, metrics }: { title: string; metrics: MetricSet }) {
  if (!metrics || metrics.samples === 0) {
    return (
      <div>
        <div className="stat-label">{title}</div>
        <div className="faint" style={{ fontSize: 12, marginTop: 6 }}>
          no observations yet
        </div>
      </div>
    );
  }
  return (
    <div>
      <div className="stat-label">{title}</div>
      <dl className="kv" style={{ marginTop: 5 }}>
        <dt>net edge</dt>
        <dd>
          <Bps value={metrics.net_edge_bps} />
        </dd>
        <dt>gross</dt>
        <dd>
          <Bps value={metrics.gross_edge_bps} />
        </dd>
        <dt>cost</dt>
        <dd className="down">{num(metrics.cost_bps, 2)}</dd>
        <dt>samples</dt>
        <dd>{integer(metrics.samples)}</dd>
        <dt>win rate</dt>
        <dd>{pct(metrics.win_rate * 100, 1)}</dd>
        <dt>profit factor</dt>
        <dd>{num(metrics.profit_factor, 3)}</dd>
        <dt>max drawdown</dt>
        <dd className="down">{num(metrics.max_drawdown_bps, 1)} bps</dd>
        <dt>t / p</dt>
        <dd>
          {num(metrics.t_stat, 2)} / {num(metrics.p_value, 4)}
        </dd>
        <dt>95% CI</dt>
        <dd>
          {num(metrics.ci_low_bps, 2)} … {num(metrics.ci_high_bps, 2)}
        </dd>
      </dl>
    </div>
  );
}
