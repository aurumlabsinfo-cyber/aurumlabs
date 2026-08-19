"use client";

import { useState } from "react";

import { Async, Bps, Empty, Note, Panel, Pill, Stat } from "@/components/ui";
import { useApi } from "@/lib/api";
import { ago, duration, integer, num, pct, shortId, stamp } from "@/lib/format";

interface ResearchPayload {
  director: {
    running: boolean;
    cycles_run: number;
    last_cycle_ms: number;
    cycle_interval_s: number;
    enabled: boolean;
    hypotheses_known: number;
    no_edge_reason: string;
    has_champion: boolean;
    strategy_counts: Record<string, number>;
    memory: {
      entries: number;
      outcomes: Record<string, number>;
      total_tests: number;
      blocked: number;
      retest_cooldown_h: number;
    };
    validation: {
      validated: number;
      rejected: number;
      rejections_by_stage: Record<string, number>;
      gates: string[];
      config: Record<string, number>;
    };
    last_cycle: CycleReport | null;
  };
  hypothesis_status_counts: Record<string, number>;
  memory_worst: {
    fingerprint: string;
    agent: string;
    outcome: string;
    net_edge_bps: number;
    tests: number;
    reason: string;
  }[];
  recent_cycles: CycleReport[];
  recent_experiments: ExperimentRow[];
}

interface CycleReport {
  cycle: number;
  started_ms: number;
  duration_ms: number;
  proposed: number;
  deduplicated: number;
  blocked_by_memory: number;
  evaluated: number;
  rejected: number;
  survived: number;
  promoted: number;
  observations_built: number;
  reasons: Record<string, number>;
  notes: string[];
}

interface ExperimentRow {
  experiment_id: string;
  hypothesis_id: string;
  stage: string;
  passed: boolean | number;
  reason: string;
  samples: number;
  p_value: number;
  adjusted_p_value: number;
  score: number;
  metrics: { net_edge_bps?: number; profit_factor?: number; win_rate?: number };
  created_ms: number;
}

interface HypothesesPayload {
  count: number;
  status_counts: Record<string, number>;
  hypotheses: {
    hypothesis_id: string;
    agent: string;
    family: string;
    signal_symbol: string;
    execution_symbol: string;
    direction: string;
    horizon_ms: number;
    entry_delay_ms: number;
    validation_status: string;
    sample_count: number;
    description: string;
    fingerprint: string;
    created_ms: number;
    conditions: { feature: string; op: string; percentile: number; threshold: number }[];
  }[];
}

const STATUS_KIND: Record<string, "ok" | "warn" | "bad" | "info" | "neutral"> = {
  UNTESTED: "neutral",
  TRAIN_PASS: "info",
  VALIDATION_PASS: "info",
  WALKFORWARD_PASS: "info",
  HOLDOUT_PASS: "ok",
  SHADOW_PASS: "ok",
  REJECTED: "bad",
};

export default function ResearchPage() {
  const research = useApi<ResearchPayload>("/research", 4000);
  const [statusFilter, setStatusFilter] = useState<string>("");
  const hypotheses = useApi<HypothesesPayload>(
    `/hypotheses?limit=120${statusFilter ? `&status=${statusFilter}` : ""}`,
    6000,
  );

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">Research</h1>
          <p className="page-sub">
            Hypotheses proposed, tested and rejected. Every gate a hypothesis failed is recorded with
            the number that failed it, and every failure is remembered so the search does not
            rediscover it.
          </p>
        </div>
      </div>

      <Async loading={research.loading} error={research.error}>
        {research.data ? (
          <>
            <Note kind={research.data.director.has_champion ? "info" : "warn"}>
              <strong>
                {research.data.director.has_champion ? "Champion active." : "No validated edge."}
              </strong>{" "}
              {research.data.director.no_edge_reason}
            </Note>

            <div className="grid cols-4">
              <Panel title="Cycles">
                <Stat
                  label="Research cycles run"
                  value={integer(research.data.director.cycles_run)}
                  hint={
                    research.data.director.last_cycle_ms
                      ? `last ${ago(research.data.director.last_cycle_ms)} · every ${duration(
                          research.data.director.cycle_interval_s * 1000,
                        )}`
                      : "not yet run"
                  }
                />
              </Panel>
              <Panel title="Hypotheses">
                <Stat
                  label="Known to the director"
                  value={integer(research.data.director.hypotheses_known)}
                  hint={`${integer(research.data.director.validation.validated)} passed · ${integer(
                    research.data.director.validation.rejected,
                  )} rejected`}
                />
              </Panel>
              <Panel title="Research memory">
                <Stat
                  label="Remembered ideas"
                  value={integer(research.data.director.memory.entries)}
                  hint={`${integer(
                    research.data.director.memory.total_tests,
                  )} tests spent · ${integer(research.data.director.memory.blocked)} re-proposals blocked`}
                />
              </Panel>
              <Panel title="Multiple testing">
                <Stat
                  label="Tests in the denominator"
                  value={integer(research.data.director.memory.total_tests)}
                  hint={`FDR alpha ${num(research.data.director.validation.config.fdr_alpha, 2)}`}
                />
              </Panel>
            </div>

            <div className="grid cols-2">
              <Panel title="Where hypotheses die" note="rejections by gate">
                {Object.keys(research.data.director.validation.rejections_by_stage).length === 0 ? (
                  <Empty>Nothing has been rejected yet.</Empty>
                ) : (
                  <table>
                    <thead>
                      <tr>
                        <th>Gate</th>
                        <th className="num">Rejected</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(research.data.director.validation.rejections_by_stage)
                        .sort((a, b) => b[1] - a[1])
                        .map(([stage, count]) => (
                          <tr key={stage}>
                            <td>{stage}</td>
                            <td className="num">{integer(count)}</td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                )}
                <p className="faint" style={{ fontSize: 11.5, marginTop: 10, marginBottom: 0 }}>
                  Gates run in order: {research.data.director.validation.gates.join(" → ")}. A
                  hypothesis stops at the first one it fails.
                </p>
              </Panel>

              <Panel title="Validation protocol" note="never re-tuned to produce trades">
                <dl className="kv">
                  {Object.entries(research.data.director.validation.config).map(([key, value]) => (
                    <ConfigRow key={key} name={key} value={value} />
                  ))}
                </dl>
              </Panel>
            </div>

            <Panel title="Recent research cycles" note="most recent last" flush>
              {research.data.recent_cycles.length === 0 ? (
                <Empty>No cycle has completed yet.</Empty>
              ) : (
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th className="num">Cycle</th>
                        <th>Started</th>
                        <th className="num">Took</th>
                        <th className="num">Proposed</th>
                        <th className="num">Deduped</th>
                        <th className="num">Memory-blocked</th>
                        <th className="num">Evaluated</th>
                        <th className="num">Observations</th>
                        <th className="num">Survived</th>
                        <th className="num">Promoted</th>
                        <th className="wrap">Top rejection</th>
                      </tr>
                    </thead>
                    <tbody>
                      {research.data.recent_cycles.map((cycle) => {
                        const top = Object.entries(cycle.reasons).sort((a, b) => b[1] - a[1])[0];
                        return (
                          <tr key={cycle.cycle}>
                            <td className="num">{cycle.cycle}</td>
                            <td className="faint mono">{stamp(cycle.started_ms)}</td>
                            <td className="num faint">{duration(cycle.duration_ms)}</td>
                            <td className="num">{integer(cycle.proposed)}</td>
                            <td className="num faint">{integer(cycle.deduplicated)}</td>
                            <td className="num faint">{integer(cycle.blocked_by_memory)}</td>
                            <td className="num">{integer(cycle.evaluated)}</td>
                            <td className="num faint">{integer(cycle.observations_built)}</td>
                            <td className={`num ${cycle.survived ? "up" : "faint"}`}>
                              {integer(cycle.survived)}
                            </td>
                            <td className={`num ${cycle.promoted ? "up" : "faint"}`}>
                              {integer(cycle.promoted)}
                            </td>
                            <td className="wrap faint" style={{ fontSize: 11.5 }}>
                              {top ? `${top[0]} (${top[1]})` : cycle.notes[0] ?? "—"}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </Panel>

            <Panel
              title="Hypotheses"
              flush
              actions={
                <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
                  <option value="">all statuses</option>
                  {Object.keys(research.data.hypothesis_status_counts).map((status) => (
                    <option key={status} value={status}>
                      {status} ({research.data!.hypothesis_status_counts[status]})
                    </option>
                  ))}
                </select>
              }
            >
              <Async
                loading={hypotheses.loading}
                error={hypotheses.error}
                empty={hypotheses.data?.hypotheses.length === 0}
                emptyMessage="No hypotheses match this filter yet."
              >
                <div className="table-wrap scroll-y">
                  <table>
                    <thead>
                      <tr>
                        <th>Status</th>
                        <th>Agent</th>
                        <th className="wrap">Claim</th>
                        <th className="num">Horizon</th>
                        <th className="num">Delay</th>
                        <th className="num">Samples</th>
                        <th>Created</th>
                      </tr>
                    </thead>
                    <tbody>
                      {hypotheses.data?.hypotheses.map((row) => (
                        <tr key={row.hypothesis_id}>
                          <td>
                            <Pill kind={STATUS_KIND[row.validation_status] ?? "neutral"}>
                              {row.validation_status}
                            </Pill>
                          </td>
                          <td className="faint">{row.agent}</td>
                          <td className="wrap" style={{ fontSize: 12 }}>
                            {row.description}
                          </td>
                          <td className="num">{duration(row.horizon_ms)}</td>
                          <td className="num faint">
                            {row.entry_delay_ms ? duration(row.entry_delay_ms) : "—"}
                          </td>
                          <td className="num">{integer(row.sample_count)}</td>
                          <td className="faint mono" style={{ fontSize: 11 }}>
                            {stamp(row.created_ms)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Async>
            </Panel>

            <div className="grid cols-2">
              <Panel title="Recent experiments" note="one row per gate" flush>
                {research.data.recent_experiments.length === 0 ? (
                  <Empty>No experiments have been run yet.</Empty>
                ) : (
                  <div className="table-wrap scroll-y">
                    <table>
                      <thead>
                        <tr>
                          <th>Gate</th>
                          <th>Result</th>
                          <th className="num">Net</th>
                          <th className="num">p</th>
                          <th className="num">adj p</th>
                          <th className="wrap">Reason</th>
                        </tr>
                      </thead>
                      <tbody>
                        {research.data.recent_experiments.map((row) => (
                          <tr key={row.experiment_id}>
                            <td>{row.stage}</td>
                            <td>
                              <Pill kind={row.passed ? "ok" : "bad"}>
                                {row.passed ? "pass" : "fail"}
                              </Pill>
                            </td>
                            <td className="num">
                              <Bps value={row.metrics?.net_edge_bps ?? null} />
                            </td>
                            <td className="num faint">{num(row.p_value, 4)}</td>
                            <td className="num">{num(row.adjusted_p_value, 4)}</td>
                            <td className="wrap faint" style={{ fontSize: 11.5 }}>
                              {row.reason}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </Panel>

              <Panel title="Research memory" note="ideas that failed, and why" flush>
                {research.data.memory_worst.length === 0 ? (
                  <Empty>Nothing remembered yet.</Empty>
                ) : (
                  <div className="table-wrap scroll-y">
                    <table>
                      <thead>
                        <tr>
                          <th>Agent</th>
                          <th>Outcome</th>
                          <th className="num">Net</th>
                          <th className="num">Tests</th>
                          <th className="wrap">Reason</th>
                        </tr>
                      </thead>
                      <tbody>
                        {research.data.memory_worst.map((row) => (
                          <tr key={row.fingerprint}>
                            <td className="faint">{row.agent}</td>
                            <td>
                              <Pill kind={row.outcome === "PROMOTED" ? "ok" : "bad"}>
                                {row.outcome}
                              </Pill>
                            </td>
                            <td className="num">
                              <Bps value={row.net_edge_bps} />
                            </td>
                            <td className="num">{integer(row.tests)}</td>
                            <td className="wrap faint" style={{ fontSize: 11.5 }}>
                              {row.reason}
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
    </>
  );
}

function ConfigRow({ name, value }: { name: string; value: number }) {
  const label = name.replace(/_/g, " ");
  const rendered = name.endsWith("_frac")
    ? pct(value * 100, 0)
    : name.endsWith("_bps")
      ? `${num(value, 2)} bps`
      : num(value, value < 1 ? 3 : 2);
  return (
    <>
      <dt>{label}</dt>
      <dd>{rendered}</dd>
    </>
  );
}
