"use client";

import { useState } from "react";

import { Async, Empty, EquityChart, Note, Panel, Pill, Stat } from "@/components/ui";
import { useApi } from "@/lib/api";
import { duration, eur, integer, num, pct, signedEur, stamp, tone } from "@/lib/format";
import type { CycleRow, PostMortemRow, WalletState } from "@/lib/types";

interface WalletPayload {
  wallet: WalletState;
  cycle: {
    cycle: CycleRow | null;
    state: string;
    active: boolean;
    blocked_reason: string;
    resets: number;
    failure_floor_eur: number;
    last_postmortem: PostMortemRow | null;
  };
  ledger: {
    id: number;
    ts_ms: number;
    kind: string;
    amount: number;
    balance_after: number;
    reference: string;
    detail: string;
  }[];
  equity_curve: { ts_ms: number; equity: number; balance: number; drawdown_pct: number }[];
  risk: {
    drawdown_pct: number;
    max_drawdown_pct: number;
    daily_pnl_pct: number;
    daily_loss_limit_pct: number;
    risk_per_trade_pct: number;
    entries_blocked: boolean;
    block_reason: string;
  };
}

interface CyclesPayload {
  current: { cycle: CycleRow | null; state: string; blocked_reason: string };
  cycles: CycleRow[];
  postmortems: PostMortemRow[];
}

export default function WalletPage() {
  const wallet = useApi<WalletPayload>("/wallet", 3000);
  const cycles = useApi<CyclesPayload>("/cycles", 6000);
  const [openPostMortem, setOpenPostMortem] = useState<number | null>(null);

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">Wallet &amp; Cycles</h1>
          <p className="page-sub">
            Each cycle starts at exactly €100.00. A reset is conditional: it happens only after a
            completed post-mortem and only when a strategy has passed live shadow evaluation.
          </p>
        </div>
      </div>

      <Async loading={wallet.loading} error={wallet.error}>
        {wallet.data ? (
          <>
            {wallet.data.cycle.blocked_reason ? (
              <Note kind="warn">
                <strong>Cycle not trading.</strong> {wallet.data.cycle.blocked_reason}
              </Note>
            ) : null}

            <div className="grid cols-4">
              <Panel title="Equity">
                <Stat
                  label={`Cycle ${wallet.data.wallet.cycle_id}`}
                  value={eur(wallet.data.wallet.equity)}
                  tone={tone(wallet.data.wallet.equity - wallet.data.wallet.starting_balance)}
                  hint={`${pct(wallet.data.wallet.return_pct)} on ${eur(
                    wallet.data.wallet.starting_balance,
                  )}`}
                />
              </Panel>
              <Panel title="Drawdown">
                <Stat
                  label="From peak equity"
                  value={pct(wallet.data.wallet.drawdown_pct)}
                  tone={
                    wallet.data.wallet.drawdown_pct >= wallet.data.risk.max_drawdown_pct * 0.7
                      ? "down"
                      : "flat"
                  }
                  hint={`peak ${eur(wallet.data.wallet.peak_equity)} · breaker at ${pct(
                    wallet.data.risk.max_drawdown_pct,
                    0,
                  )}`}
                />
                <div className="bar" style={{ marginTop: 8 }}>
                  <span
                    style={{
                      width: `${Math.min(100, (wallet.data.wallet.drawdown_pct / wallet.data.risk.max_drawdown_pct) * 100)}%`,
                      background:
                        wallet.data.wallet.drawdown_pct >= wallet.data.risk.max_drawdown_pct * 0.7
                          ? "var(--down)"
                          : "var(--info)",
                    }}
                  />
                </div>
              </Panel>
              <Panel title="Trades">
                <Stat
                  label="This cycle"
                  value={integer(wallet.data.wallet.trades)}
                  hint={`${integer(wallet.data.wallet.wins)} won · ${integer(
                    wallet.data.wallet.losses,
                  )} lost · ${pct(wallet.data.wallet.win_rate * 100, 1)}`}
                />
              </Panel>
              <Panel title="Failure floor">
                <Stat
                  label="Cycle ends below"
                  value={eur(wallet.data.cycle.failure_floor_eur)}
                  tone={
                    wallet.data.wallet.equity <= wallet.data.cycle.failure_floor_eur * 1.2
                      ? "warn"
                      : "flat"
                  }
                  hint={`${integer(wallet.data.cycle.resets)} resets so far`}
                />
              </Panel>
            </div>

            <Panel title="Equity curve" note={`cycle ${wallet.data.wallet.cycle_id}`}>
              {wallet.data.equity_curve.length < 2 ? (
                <Empty>Not enough wallet snapshots yet.</Empty>
              ) : (
                <EquityChart
                  points={wallet.data.equity_curve}
                  baseline={wallet.data.wallet.starting_balance}
                />
              )}
            </Panel>

            <div className="grid cols-2">
              <Panel title="Balances" note="reserved margin is not available">
                <dl className="kv">
                  <dt>starting balance</dt>
                  <dd>{eur(wallet.data.wallet.starting_balance)}</dd>
                  <dt>balance (realised)</dt>
                  <dd>{eur(wallet.data.wallet.balance)}</dd>
                  <dt>reserved</dt>
                  <dd>{eur(wallet.data.wallet.reserved)}</dd>
                  <dt>available</dt>
                  <dd>{eur(wallet.data.wallet.available)}</dd>
                  <dt>unrealised</dt>
                  <dd className={tone(wallet.data.wallet.unrealized_pnl)}>
                    {signedEur(wallet.data.wallet.unrealized_pnl)}
                  </dd>
                  <dt>equity</dt>
                  <dd>{eur(wallet.data.wallet.equity)}</dd>
                  <dt>realised P&amp;L</dt>
                  <dd className={tone(wallet.data.wallet.realized_pnl)}>
                    {signedEur(wallet.data.wallet.realized_pnl)}
                  </dd>
                  <dt>fees paid</dt>
                  <dd className="down">{eur(wallet.data.wallet.fees_paid, 6)}</dd>
                  <dt>daily P&amp;L</dt>
                  <dd className={tone(wallet.data.wallet.daily_pnl_pct ?? 0)}>
                    {pct(wallet.data.wallet.daily_pnl_pct ?? 0)}
                  </dd>
                </dl>
              </Panel>

              <Panel title="Ledger" note="every movement, newest first" flush>
                {wallet.data.ledger.length === 0 ? (
                  <Empty>No ledger entries yet.</Empty>
                ) : (
                  <div className="table-wrap scroll-y">
                    <table>
                      <thead>
                        <tr>
                          <th>When</th>
                          <th>Kind</th>
                          <th className="num">Amount</th>
                          <th className="num">Balance after</th>
                          <th>Reference</th>
                        </tr>
                      </thead>
                      <tbody>
                        {wallet.data.ledger.map((entry) => (
                          <tr key={entry.id}>
                            <td className="faint mono" style={{ fontSize: 11 }}>
                              {stamp(entry.ts_ms)}
                            </td>
                            <td>{entry.kind}</td>
                            <td className={`num ${tone(entry.amount)}`}>
                              {signedEur(entry.amount, 6)}
                            </td>
                            <td className="num">{eur(entry.balance_after, 6)}</td>
                            <td className="faint mono" style={{ fontSize: 11 }}>
                              {entry.reference}
                            </td>
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

      <Async loading={cycles.loading} error={cycles.error}>
        {cycles.data ? (
          <>
            <Panel title="Cycle history" note="nothing is deleted on reset" flush>
              {cycles.data.cycles.length === 0 ? (
                <Empty>No cycles recorded.</Empty>
              ) : (
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th className="num">#</th>
                        <th>State</th>
                        <th>Started</th>
                        <th className="num">Duration</th>
                        <th className="num">Start</th>
                        <th className="num">Final</th>
                        <th className="num">Net</th>
                        <th className="num">Max DD</th>
                        <th className="num">Trades</th>
                        <th className="wrap">End reason</th>
                      </tr>
                    </thead>
                    <tbody>
                      {cycles.data.cycles.map((cycle) => (
                        <tr key={cycle.cycle_id}>
                          <td className="num">{cycle.cycle_id}</td>
                          <td>
                            <Pill
                              kind={
                                cycle.state === "ACTIVE"
                                  ? "ok"
                                  : cycle.state === "AWAITING_EDGE"
                                    ? "warn"
                                    : "neutral"
                              }
                            >
                              {cycle.state}
                            </Pill>
                          </td>
                          <td className="faint mono" style={{ fontSize: 11 }}>
                            {stamp(cycle.started_ms)}
                          </td>
                          <td className="num faint">
                            {duration((cycle.ended_ms ?? Date.now()) - cycle.started_ms)}
                          </td>
                          <td className="num">{eur(cycle.starting_balance)}</td>
                          <td className="num">{cycle.final_balance ? eur(cycle.final_balance) : "—"}</td>
                          <td className={`num ${tone(cycle.net_pnl)}`}>{signedEur(cycle.net_pnl)}</td>
                          <td className="num down">{pct(cycle.max_drawdown_pct)}</td>
                          <td className="num">{integer(cycle.trades)}</td>
                          <td className="wrap faint" style={{ fontSize: 11.5 }}>
                            {cycle.end_reason || "—"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Panel>

            <Panel title="Post-mortems" note="run before any reset is considered" flush>
              {cycles.data.postmortems.length === 0 ? (
                <Empty>
                  No post-mortem has run. One runs on cycle failure or champion degradation, and a
                  reset cannot happen before it completes.
                </Empty>
              ) : (
                <div>
                  {cycles.data.postmortems.map((post) => (
                    <div
                      key={`${post.cycle_id}-${post.created_ms}`}
                      style={{ borderBottom: "1px solid var(--border)", padding: "11px 13px" }}
                    >
                      <div
                        style={{
                          display: "flex",
                          gap: 8,
                          alignItems: "center",
                          flexWrap: "wrap",
                          cursor: "pointer",
                        }}
                        onClick={() =>
                          setOpenPostMortem(openPostMortem === post.cycle_id ? null : post.cycle_id)
                        }
                      >
                        <Pill kind={post.verdict === "FAILED" ? "bad" : "warn"}>{post.verdict}</Pill>
                        <strong>Cycle {post.cycle_id}</strong>
                        <Pill kind="info">{post.primary_cause}</Pill>
                        <span className={tone(post.net_pnl)}>{signedEur(post.net_pnl)}</span>
                        <span className="faint" style={{ fontSize: 11.5 }}>
                          {integer(post.trades_analyzed)} trades · {stamp(post.created_ms)}
                        </span>
                        <span className="faint" style={{ marginLeft: "auto" }}>
                          {openPostMortem === post.cycle_id ? "▲" : "▼"}
                        </span>
                      </div>

                      {openPostMortem === post.cycle_id ? (
                        <div style={{ marginTop: 10 }}>
                          <div className="stat-label" style={{ marginBottom: 5 }}>
                            Attributed causes
                          </div>
                          <table>
                            <thead>
                              <tr>
                                <th>Cause</th>
                                <th className="num">Weight</th>
                                <th className="wrap">Evidence</th>
                              </tr>
                            </thead>
                            <tbody>
                              {post.causes.map((cause) => (
                                <tr key={cause.cause}>
                                  <td>{cause.cause}</td>
                                  <td className="num">{num(cause.weight, 3)}</td>
                                  <td className="wrap" style={{ fontSize: 11.5 }}>
                                    {cause.detail}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>

                          <div className="stat-label" style={{ margin: "12px 0 5px" }}>
                            What to research next
                          </div>
                          <ul style={{ margin: 0, paddingLeft: 18, fontSize: 12.5 }}>
                            {post.recommendations.map((rec) => (
                              <li key={rec} style={{ marginBottom: 3 }}>
                                {rec}
                              </li>
                            ))}
                          </ul>
                          <p className="faint" style={{ fontSize: 11.5, marginTop: 8, marginBottom: 0 }}>
                            No recommendation ever proposes lowering a gate to produce trades. That
                            is the one conclusion this system is not permitted to reach.
                          </p>
                        </div>
                      ) : null}
                    </div>
                  ))}
                </div>
              )}
            </Panel>
          </>
        ) : null}
      </Async>
    </>
  );
}
