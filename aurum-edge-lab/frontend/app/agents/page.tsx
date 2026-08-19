"use client";

import { Async, Empty, Note, Panel, Pill, Stat } from "@/components/ui";
import { useApi } from "@/lib/api";
import { ago, duration, integer, num, pct, stamp } from "@/lib/format";
import type { AgentRow } from "@/lib/types";

interface AgentsPayload {
  count: number;
  agents: AgentRow[];
  recent_events: {
    id: number;
    ts_ms: number;
    agent: string;
    kind: string;
    severity: string;
    message: string;
    detail: Record<string, unknown>;
  }[];
}

export default function AgentsPage() {
  const { data, error, loading } = useApi<AgentsPayload>("/agents", 4000);

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">Agents</h1>
          <p className="page-sub">
            Five research families and the post-mortem agent. Each declares the features it reads,
            counts what it examined, and reports what research memory refused to let it re-propose.
          </p>
        </div>
      </div>

      <Note kind="neutral">
        <strong>No agent here is cosmetic.</strong> Each ranks candidate features against the forward
        return it claims to predict, on the discovery slice only — never on the validation,
        walk-forward or holdout data that will judge the result. The counters below are what makes
        that inspectable rather than asserted.
      </Note>

      <Async loading={loading} error={error} empty={!data}>
        {data ? (
          <>
            <div className="grid cols-2">
              {data.agents.map((agent) => (
                <AgentCard key={agent.name} agent={agent} />
              ))}
            </div>

            <Panel title="Agent event log" note="newest first" flush>
              {data.recent_events.length === 0 ? (
                <Empty>No agent events recorded yet.</Empty>
              ) : (
                <div className="table-wrap scroll-y">
                  <table>
                    <thead>
                      <tr>
                        <th>When</th>
                        <th>Agent</th>
                        <th>Kind</th>
                        <th className="wrap">Message</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.recent_events.map((event) => (
                        <tr key={event.id}>
                          <td className="faint mono" style={{ fontSize: 11 }}>
                            {stamp(event.ts_ms)}
                          </td>
                          <td className="mono">{event.agent}</td>
                          <td>
                            <Pill
                              kind={
                                event.severity === "ERROR"
                                  ? "bad"
                                  : event.kind === "proposed"
                                    ? "ok"
                                    : "neutral"
                              }
                            >
                              {event.kind}
                            </Pill>
                          </td>
                          <td className="wrap" style={{ fontSize: 11.5 }}>
                            {event.message}
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
    </>
  );
}

function AgentCard({ agent }: { agent: AgentRow }) {
  const metrics = agent.metrics;
  return (
    <Panel
      title={agent.name}
      note={
        agent.enabled ? (
          <Pill kind="ok" dot>
            enabled
          </Pill>
        ) : (
          <Pill kind="bad">disabled</Pill>
        )
      }
    >
      <p style={{ marginTop: 0, marginBottom: 10, fontSize: 12.5 }} className="dim">
        {agent.description}
      </p>

      {metrics ? (
        <>
          <div className="grid cols-3" style={{ gap: 10, marginBottom: 10 }}>
            <Stat
              label="Runs"
              value={integer(metrics.runs)}
              small
              hint={metrics.last_run_ms ? ago(metrics.last_run_ms) : "not yet run"}
            />
            <Stat
              label="Proposed"
              value={integer(metrics.proposed)}
              small
              tone={metrics.proposed > 0 ? "up" : "flat"}
              hint={`${integer(metrics.candidates_ranked)} candidates ranked`}
            />
            <Stat
              label="Errors"
              value={integer(metrics.errors)}
              small
              tone={metrics.errors > 0 ? "down" : "flat"}
              hint={metrics.last_error || "none"}
            />
          </div>

          <dl className="kv">
            <dt>features read</dt>
            <dd>{integer(metrics.features_read)}</dd>
            <dt>blocked by memory</dt>
            <dd>{integer(metrics.blocked_by_memory)}</dd>
            <dt>dropped as weak</dt>
            <dd>{integer(metrics.dropped_weak)}</dd>
            <dt>dropped unresolvable</dt>
            <dd>{integer(metrics.dropped_unresolvable)}</dd>
            {agent.horizons_ms ? (
              <>
                <dt>horizons</dt>
                <dd>{agent.horizons_ms.map((h) => duration(h)).join(", ")}</dd>
              </>
            ) : null}
            {agent.discovery_fraction !== undefined ? (
              <>
                <dt>discovery slice</dt>
                <dd>{pct(agent.discovery_fraction * 100, 0)}</dd>
              </>
            ) : null}
            {agent.min_abs_correlation !== undefined ? (
              <>
                <dt>min |correlation|</dt>
                <dd>{num(agent.min_abs_correlation, 3)}</dd>
              </>
            ) : null}
          </dl>

          {agent.inputs?.length ? (
            <div style={{ marginTop: 10 }}>
              <div className="stat-label" style={{ marginBottom: 4 }}>
                Inputs
              </div>
              <div className="chips">
                {agent.inputs.map((input) => (
                  <Pill key={input}>{input}</Pill>
                ))}
              </div>
            </div>
          ) : null}

          {metrics.best_ranks?.length ? (
            <div style={{ marginTop: 10 }}>
              <div className="stat-label" style={{ marginBottom: 4 }}>
                Strongest candidates found
              </div>
              <table>
                <thead>
                  <tr>
                    <th>Feature</th>
                    <th className="num">r</th>
                    <th>Implies</th>
                    <th className="num">n</th>
                  </tr>
                </thead>
                <tbody>
                  {metrics.best_ranks.map((rank) => (
                    <tr key={rank.feature}>
                      <td className="mono">{rank.feature}</td>
                      <td className="num">{num(rank.correlation, 4)}</td>
                      <td className={rank.direction === "LONG" ? "up" : "down"}>{rank.direction}</td>
                      <td className="num faint">{integer(rank.samples)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}

          {metrics.last_proposals?.length ? (
            <div style={{ marginTop: 10 }}>
              <div className="stat-label" style={{ marginBottom: 4 }}>
                Most recent proposals
              </div>
              <ul style={{ margin: 0, paddingLeft: 18, fontSize: 11.5 }} className="dim">
                {metrics.last_proposals.map((proposal, index) => (
                  <li key={index} style={{ marginBottom: 3 }}>
                    {proposal}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
        </>
      ) : (
        <dl className="kv">
          <dt>runs</dt>
          <dd>{integer(agent.runs ?? 0)}</dd>
          <dt>last run</dt>
          <dd>{agent.last_run_ms ? ago(agent.last_run_ms) : "never"}</dd>
          {agent.note ? (
            <>
              <dt>note</dt>
              <dd style={{ fontFamily: "var(--sans)" }}>{agent.note}</dd>
            </>
          ) : null}
        </dl>
      )}
    </Panel>
  );
}
