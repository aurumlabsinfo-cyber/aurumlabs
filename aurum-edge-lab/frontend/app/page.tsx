"use client";

import { Bps, Empty, Maybe, Panel, Pill, Sparkline, Stat } from "@/components/ui";
import { bps, clock, duration, eur, integer, num, pct, price, signedEur, tone } from "@/lib/format";
import { useLive } from "@/lib/live";

export default function DashboardPage() {
  const { state } = useLive();

  if (!state) {
    return (
      <>
        <PageHead />
        <Empty>Waiting for the first state frame from the engine…</Empty>
      </>
    );
  }

  const { wallet, cycle, feed, research, data_quality: quality, diagnostics } = state;

  return (
    <>
      <PageHead />

      <div className="grid cols-4">
        <Panel title="Wallet" note={`cycle ${wallet.cycle_id}`}>
          <Stat
            label="Equity"
            value={eur(wallet.equity)}
            tone={tone(wallet.equity - wallet.starting_balance)}
            hint={
              <>
                started {eur(wallet.starting_balance)} · {pct(wallet.return_pct)} ·{" "}
                <span className={tone(-wallet.drawdown_pct)}>dd {pct(wallet.drawdown_pct)}</span>
              </>
            }
          />
          <div style={{ marginTop: 10 }} className="kv">
            <dt>Available</dt>
            <dd>{eur(wallet.available)}</dd>
            <dt>Reserved</dt>
            <dd>{eur(wallet.reserved)}</dd>
            <dt>Realised</dt>
            <dd className={tone(wallet.realized_pnl)}>{signedEur(wallet.realized_pnl)}</dd>
            <dt>Unrealised</dt>
            <dd className={tone(wallet.unrealized_pnl)}>{signedEur(wallet.unrealized_pnl)}</dd>
            <dt>Fees paid</dt>
            <dd className="down">{eur(wallet.fees_paid, 4)}</dd>
          </div>
        </Panel>

        <Panel title="Cycle" note={cycle.state}>
          <Stat
            label="Cycle number"
            value={cycle.cycle?.cycle_id ?? "—"}
            hint={
              cycle.cycle
                ? `open ${duration(Date.now() - cycle.cycle.started_ms)} · ${integer(
                    cycle.cycle.trades,
                  )} trades`
                : "no cycle open"
            }
          />
          <div style={{ marginTop: 10 }} className="kv">
            <dt>State</dt>
            <dd>
              <Pill kind={cycle.active ? "ok" : "warn"}>{cycle.state}</Pill>
            </dd>
            <dt>Failure floor</dt>
            <dd>{eur(cycle.failure_floor_eur)}</dd>
            <dt>Resets</dt>
            <dd>{integer(cycle.resets)}</dd>
            <dt>Post-mortems</dt>
            <dd>{integer(cycle.postmortems)}</dd>
            <dt>Win / loss</dt>
            <dd>
              {integer(wallet.wins)} / {integer(wallet.losses)}
            </dd>
          </div>
        </Panel>

        <Panel title="Connection" note={feed.state}>
          <Stat
            label="Symbols live"
            value={`${feed.symbols_live} / ${feed.symbols_configured}`}
            tone={feed.symbols_live === feed.symbols_configured ? "up" : "warn"}
            hint={
              <>
                {feed.live ? (
                  <Pill kind="ok" dot>
                    LIVE
                  </Pill>
                ) : (
                  <Pill kind="warn" dot>
                    {feed.kind.toUpperCase()} — NOT LIVE
                  </Pill>
                )}
              </>
            }
          />
          <div style={{ marginTop: 10 }} className="kv">
            <dt>Events processed</dt>
            <dd>{integer(feed.events_processed)}</dd>
            <dt>Data time</dt>
            <dd>{clock(state.ts_ms)}</dd>
            <dt>Quality mean</dt>
            <dd className={quality.mean_score >= quality.min_score_to_trade ? "up" : "warn"}>
              {num(quality.mean_score, 3)}
            </dd>
            <dt>Tradable</dt>
            <dd>
              {quality.tradable} / {quality.symbols}
            </dd>
            <dt>Worst</dt>
            <dd>
              <Maybe value={quality.worst ? `${quality.worst.symbol} ${num(quality.worst.score, 2)}` : null} />
            </dd>
          </div>
        </Panel>

        <Panel title="Research" note={`${research.cycles_run} cycles`}>
          <Stat
            label="Hypotheses tested"
            value={integer(research.hypotheses)}
            hint={
              <>
                {integer(research.validated)} passed · {integer(research.rejected)} rejected
              </>
            }
          />
          <div style={{ marginTop: 10 }} className="kv">
            <dt>Memory entries</dt>
            <dd>{integer(research.memory_entries)}</dd>
            <dt>Shadow pending</dt>
            <dd>{integer(research.shadow_pending)}</dd>
            {Object.entries(state.strategy_counts)
              .filter(([, count]) => count > 0)
              .map(([label, count]) => (
                <ReactFragmentRow key={label} label={label} count={count} />
              ))}
          </div>
        </Panel>
      </div>

      <div className="grid cols-2">
        <Panel
          title="Active champion"
          note={state.champion ? state.champion.strategy_id : "none"}
        >
          {state.champion ? (
            <ChampionCard champion={state.champion} />
          ) : (
            <div>
              <Stat label="Champion" value="NO VALIDATED EDGE" tone="warn" small />
              <p className="stat-hint" style={{ marginTop: 8 }}>
                {state.no_edge_reason}
              </p>
              <p className="faint" style={{ fontSize: 12, marginTop: 8 }}>
                The system does not promote a strategy in order to produce activity. Nothing trades
                until a hypothesis has survived costs, out-of-sample validation and live shadow
                evaluation.
              </p>
            </div>
          )}
        </Panel>

        <Panel title="Why nothing is trading" note="last hour">
          <p style={{ marginTop: 0, marginBottom: 10 }} className="dim">
            {diagnostics.summary}
          </p>
          {diagnostics.top_rejections.length === 0 ? (
            <Empty>No decisions have been evaluated yet.</Empty>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>Gate</th>
                  <th className="num">Count</th>
                  <th className="num">Share</th>
                </tr>
              </thead>
              <tbody>
                {diagnostics.top_rejections.map((row) => (
                  <tr key={row.reason}>
                    <td>
                      <span className={row.reason === "ACCEPTED" ? "up" : ""}>{row.reason}</span>
                    </td>
                    <td className="num">{integer(row.count)}</td>
                    <td className="num">{pct(row.percent, 1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Panel>
      </div>

      <Panel title="Markets" note={`${state.markets.length} perpetual futures`} flush>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Tier</th>
                <th className="num">Mid</th>
                <th className="num">Spread</th>
                <th className="num">Mark dev</th>
                <th className="num">Funding</th>
                <th className="num">Quality</th>
                <th>Book</th>
                <th>Flags</th>
              </tr>
            </thead>
            <tbody>
              {state.markets.map((row) => {
                const markDev =
                  row.mark_price && row.mid ? ((row.mark_price - row.mid) / row.mid) * 10_000 : null;
                return (
                  <tr key={row.symbol}>
                    <td className="mono">{row.symbol}</td>
                    <td className="faint">{row.tier}</td>
                    <td className="num">{price(row.mid)}</td>
                    <td className="num">{row.spread_bps === null ? "—" : num(row.spread_bps, 2)}</td>
                    <td className="num">
                      <Bps value={markDev} />
                    </td>
                    <td className="num">{bps(row.funding_rate * 10_000, 2)}</td>
                    <td className="num">
                      <span className={row.quality?.tradable ? "up" : "warn"}>
                        {num(row.quality?.score ?? null, 2)}
                      </span>
                    </td>
                    <td>
                      <Pill kind={row.book_state === "READY" ? "ok" : "warn"}>{row.book_state}</Pill>
                    </td>
                    <td>
                      {row.quality?.flags.length ? (
                        <span className="chips">
                          {row.quality.flags.map((flag) => (
                            <Pill key={flag} kind="warn">
                              {flag}
                            </Pill>
                          ))}
                        </span>
                      ) : (
                        <span className="faint">—</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Panel>

      <div className="grid cols-2">
        <Panel title="Open positions" note={`${state.positions.length} open`} flush>
          {state.positions.length === 0 ? (
            <Empty>No open positions.</Empty>
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Side</th>
                    <th className="num">Entry</th>
                    <th className="num">Mark</th>
                    <th className="num">Unrealised</th>
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
                      <td className="num">{price(position.entry_price)}</td>
                      <td className="num">{price(position.mark_price)}</td>
                      <td className={`num ${tone(position.unrealized_pnl_eur)}`}>
                        {signedEur(position.unrealized_pnl_eur)}
                      </td>
                      <td className="num">{duration(Date.now() - position.entry_ts_ms)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Panel>

        <Panel title="Agents" note={`${state.agents.length} running`} flush>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Agent</th>
                  <th className="num">Runs</th>
                  <th className="num">Proposed</th>
                  <th className="num">Blocked</th>
                  <th className="num">Errors</th>
                  <th className="num">Last run</th>
                </tr>
              </thead>
              <tbody>
                {state.agents.map((agent) => (
                  <tr key={agent.name}>
                    <td className="mono">{agent.name}</td>
                    <td className="num">{integer(agent.runs)}</td>
                    <td className="num">{integer(agent.proposed)}</td>
                    <td className="num faint">{integer(agent.blocked_by_memory)}</td>
                    <td className={`num ${agent.errors ? "down" : "faint"}`}>
                      {integer(agent.errors)}
                    </td>
                    <td className="num faint">{clock(agent.last_run_ms)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      </div>

      {state.recent_trades.length > 0 ? (
        <Panel title="Recent paper trades" note="net of fees and slippage" flush>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Closed</th>
                  <th>Symbol</th>
                  <th>Side</th>
                  <th className="num">Net €</th>
                  <th className="num">Net bps</th>
                  <th className="num">Cost bps</th>
                  <th>Exit</th>
                </tr>
              </thead>
              <tbody>
                {state.recent_trades.map((trade) => (
                  <tr key={trade.trade_id}>
                    <td className="faint mono">{clock(trade.exit_ts_ms)}</td>
                    <td className="mono">{trade.symbol}</td>
                    <td className={trade.direction === "LONG" ? "up" : "down"}>{trade.direction}</td>
                    <td className={`num ${tone(trade.net_pnl_eur)}`}>{signedEur(trade.net_pnl_eur)}</td>
                    <td className="num">
                      <Bps value={trade.net_return_bps} />
                    </td>
                    <td className="num down">{num(trade.cost_bps, 2)}</td>
                    <td className="faint">{trade.exit_reason}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      ) : null}
    </>
  );
}

function PageHead() {
  return (
    <div className="page-head">
      <div>
        <h1 className="page-title">Dashboard</h1>
        <p className="page-sub">
          Wallet, cycle, feed and research state. The objective is validated net edge — or an
          explicit NO EDGE.
        </p>
      </div>
    </div>
  );
}

function ReactFragmentRow({ label, count }: { label: string; count: number }) {
  return (
    <>
      <dt>{label.toLowerCase()}</dt>
      <dd>{integer(count)}</dd>
    </>
  );
}

function ChampionCard({ champion }: { champion: NonNullable<ReturnType<typeof useLive>["state"]>["champion"] }) {
  if (!champion) return null;
  const validated = champion.validated_metrics;
  const live = champion.live_metrics;
  return (
    <div>
      <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
        <Pill kind="ok" dot>
          CHAMPION
        </Pill>
        <Pill kind="info">{champion.agent}</Pill>
        <Pill>v{champion.version}</Pill>
        <span className="mono faint" style={{ fontSize: 11.5 }}>
          {champion.strategy_id}
        </span>
      </div>
      <p style={{ margin: "10px 0", fontSize: 13 }}>{champion.description}</p>
      <div className="grid cols-3" style={{ gap: 10 }}>
        <Stat
          label="Validated net"
          value={bps(validated.net_edge_bps)}
          tone={tone(validated.net_edge_bps)}
          small
          hint={`${integer(validated.samples)} samples · PF ${num(validated.profit_factor, 2)}`}
        />
        <Stat
          label="Live net"
          value={live.samples ? bps(live.net_edge_bps) : "—"}
          tone={live.samples ? tone(live.net_edge_bps) : "flat"}
          small
          hint={`${integer(champion.live_trades)} trades`}
        />
        <Stat
          label="Live P&L"
          value={signedEur(champion.live_net_pnl_eur)}
          tone={tone(champion.live_net_pnl_eur)}
          small
          hint={`horizon ${duration(champion.horizon_ms)}`}
        />
      </div>
      {validated.samples > 0 ? (
        <div style={{ marginTop: 10 }}>
          <Sparkline
            values={[
              validated.ci_low_bps,
              validated.net_edge_bps,
              validated.ci_high_bps,
            ]}
            width={200}
            height={26}
          />
          <div className="faint" style={{ fontSize: 11.5 }}>
            95% CI {bps(validated.ci_low_bps)} … {bps(validated.ci_high_bps)} · p=
            {num(validated.p_value, 4)}
          </div>
        </div>
      ) : null}
    </div>
  );
}
