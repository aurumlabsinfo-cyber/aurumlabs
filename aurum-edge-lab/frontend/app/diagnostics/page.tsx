"use client";

import { Async, Empty, Note, Panel, Pill, Stat } from "@/components/ui";
import { useApi } from "@/lib/api";
import { integer, num, pct, stamp } from "@/lib/format";
import type { RejectionRow } from "@/lib/types";

interface DiagnosticsPayload {
  summary: string;
  status: string;
  no_edge_reason: string;
  cycle_blocked_reason: string;
  risk_block_reason: string;
  window_s: number;
  evaluated_total: number;
  accepted_total: number;
  shadow_signals_total: number;
  acceptance_rate: number;
  window_evaluated: number;
  window_accepted: number;
  rejections: RejectionRow[];
  top_reason: string | null;
  uptime_s: number;
  persisted_rejections: { rejection: string; n: number }[];
  risk_rejections: Record<string, number>;
  gate_catalogue: { reason: string; explanation: string; action: string }[];
  data_quality: {
    symbols: number;
    tradable: number;
    mean_score: number;
    min_score_to_trade: number;
    blocked: Record<string, string[]>;
  };
  warmup: {
    warmed_up: boolean;
    features_ready: boolean;
    books_ready: boolean;
    min_warmup_s: number;
  };
  recent_errors: { ts_ms: number; component: string; error: string }[];
  system_errors: { ts_ms: number; level: string; component: string; message: string }[];
}

export default function DiagnosticsPage() {
  const { data, error, loading } = useApi<DiagnosticsPayload>("/diagnostics", 3000);

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">Diagnostics</h1>
          <p className="page-sub">
            Why no signals. Every gate that can turn a would-be trade into no trade, with its count,
            its share, what it means, and what to do about it.
          </p>
        </div>
      </div>

      <Async loading={loading} error={error} empty={!data}>
        {data ? (
          <>
            <Note kind={data.window_accepted > 0 ? "info" : "warn"}>
              <strong>{data.summary}</strong>
            </Note>

            <div className="grid cols-4">
              <Panel title="Status">
                <Stat
                  label="Engine"
                  value={data.status}
                  small
                  tone={data.status === "OK" ? "up" : data.status === "WARMING_UP" ? "flat" : "warn"}
                  hint={`up ${num(data.uptime_s / 60, 1)} min`}
                />
              </Panel>
              <Panel title="Decisions">
                <Stat
                  label="Evaluated in window"
                  value={integer(data.window_evaluated)}
                  hint={`${integer(data.evaluated_total)} since start`}
                />
              </Panel>
              <Panel title="Accepted">
                <Stat
                  label="Became a trade"
                  value={integer(data.window_accepted)}
                  tone={data.window_accepted > 0 ? "up" : "flat"}
                  hint={`${pct(data.acceptance_rate * 100, 3)} of all decisions`}
                />
              </Panel>
              <Panel title="Shadow signals">
                <Stat
                  label="Measured, never traded"
                  value={integer(data.shadow_signals_total)}
                  hint="live evaluation without wallet impact"
                />
              </Panel>
            </div>

            {!data.warmup.warmed_up ? (
              <Note kind="info">
                <strong>Still warming up.</strong> Books ready: {data.warmup.books_ready ? "yes" : "no"} ·
                features ready: {data.warmup.features_ready ? "yes" : "no"} · research warmup{" "}
                {num(data.warmup.min_warmup_s, 0)} s. Decisions before warmup are not evidence of
                anything.
              </Note>
            ) : null}

            {data.risk_block_reason ? (
              <Note kind="bad">
                <strong>Entries blocked.</strong> {data.risk_block_reason}
              </Note>
            ) : null}
            {data.cycle_blocked_reason ? (
              <Note kind="warn">
                <strong>Cycle.</strong> {data.cycle_blocked_reason}
              </Note>
            ) : null}
            <Note kind="neutral">
              <strong>Research.</strong> {data.no_edge_reason}
            </Note>

            <Panel
              title="Gates that fired"
              note={`last ${num(data.window_s / 60, 0)} minutes`}
              flush
            >
              {data.rejections.length === 0 ? (
                <Empty>No decisions have been evaluated in this window.</Empty>
              ) : (
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Gate</th>
                        <th className="num">Count</th>
                        <th className="num">Share</th>
                        <th className="num">Since start</th>
                        <th className="wrap">What it means</th>
                        <th className="wrap">What to do</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.rejections.map((row) => (
                        <tr key={row.reason}>
                          <td>
                            <Pill kind={row.reason === "ACCEPTED" ? "ok" : "neutral"}>
                              {row.reason}
                            </Pill>
                          </td>
                          <td className="num">{integer(row.count)}</td>
                          <td className="num">
                            <div style={{ display: "flex", alignItems: "center", gap: 6, justifyContent: "flex-end" }}>
                              <div className="bar" style={{ width: 54 }}>
                                <span
                                  style={{
                                    width: `${row.percent}%`,
                                    background:
                                      row.reason === "ACCEPTED" ? "var(--up)" : "var(--info)",
                                  }}
                                />
                              </div>
                              {pct(row.percent, 1)}
                            </div>
                          </td>
                          <td className="num faint">{integer(row.total_since_start)}</td>
                          <td className="wrap" style={{ fontSize: 11.5 }}>
                            {row.explanation}
                          </td>
                          <td className="wrap faint" style={{ fontSize: 11.5 }}>
                            {row.action}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Panel>

            <div className="grid cols-2">
              <Panel title="Data quality" note="blocks entry regardless of confidence">
                <div className="grid cols-3" style={{ gap: 10 }}>
                  <Stat
                    label="Tradable"
                    value={`${data.data_quality.tradable} / ${data.data_quality.symbols}`}
                    small
                    tone={
                      data.data_quality.tradable === data.data_quality.symbols ? "up" : "warn"
                    }
                  />
                  <Stat label="Mean score" value={num(data.data_quality.mean_score, 3)} small />
                  <Stat
                    label="Minimum"
                    value={num(data.data_quality.min_score_to_trade, 2)}
                    small
                    hint="quality.min_score_to_trade"
                  />
                </div>
                {Object.keys(data.data_quality.blocked).length === 0 ? (
                  <p className="up" style={{ fontSize: 12.5, marginTop: 10, marginBottom: 0 }}>
                    Every symbol passes the data-quality gate.
                  </p>
                ) : (
                  <table style={{ marginTop: 10 }}>
                    <thead>
                      <tr>
                        <th>Symbol</th>
                        <th className="wrap">Blocked because</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(data.data_quality.blocked).map(([symbol, reasons]) => (
                        <tr key={symbol}>
                          <td className="mono">{symbol}</td>
                          <td className="wrap warn" style={{ fontSize: 11.5 }}>
                            {reasons.join("; ")}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </Panel>

              <Panel title="Risk manager rejections" note="cumulative since start">
                {Object.keys(data.risk_rejections).length === 0 ? (
                  <Empty>The risk manager has not refused anything.</Empty>
                ) : (
                  <table>
                    <thead>
                      <tr>
                        <th>Reason</th>
                        <th className="num">Count</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(data.risk_rejections)
                        .sort((a, b) => b[1] - a[1])
                        .map(([reason, count]) => (
                          <tr key={reason}>
                            <td>{reason}</td>
                            <td className="num">{integer(count)}</td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                )}
              </Panel>
            </div>

            {data.recent_errors.length || data.system_errors.length ? (
              <Panel title="Errors" note="most recent first" flush>
                <div className="table-wrap scroll-y">
                  <table>
                    <thead>
                      <tr>
                        <th>When</th>
                        <th>Component</th>
                        <th className="wrap">Error</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.recent_errors.map((err, index) => (
                        <tr key={`runtime-${index}`}>
                          <td className="faint mono" style={{ fontSize: 11 }}>
                            {stamp(err.ts_ms)}
                          </td>
                          <td>{err.component}</td>
                          <td className="wrap down" style={{ fontSize: 11.5 }}>
                            {err.error}
                          </td>
                        </tr>
                      ))}
                      {data.system_errors.map((err, index) => (
                        <tr key={`system-${index}`}>
                          <td className="faint mono" style={{ fontSize: 11 }}>
                            {stamp(err.ts_ms)}
                          </td>
                          <td>{err.component}</td>
                          <td className="wrap down" style={{ fontSize: 11.5 }}>
                            {err.message}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Panel>
            ) : (
              <Panel title="Errors">
                <p className="up" style={{ margin: 0, fontSize: 12.5 }}>
                  No errors recorded.
                </p>
              </Panel>
            )}

            <Panel title="Every gate the system can report" note="including those at zero" flush>
              <div className="table-wrap scroll-y">
                <table>
                  <thead>
                    <tr>
                      <th>Gate</th>
                      <th className="wrap">Meaning</th>
                      <th className="wrap">Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.gate_catalogue.map((gate) => (
                      <tr key={gate.reason}>
                        <td className="mono" style={{ fontSize: 11.5 }}>
                          {gate.reason}
                        </td>
                        <td className="wrap" style={{ fontSize: 11.5 }}>
                          {gate.explanation}
                        </td>
                        <td className="wrap faint" style={{ fontSize: 11.5 }}>
                          {gate.action}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Panel>
          </>
        ) : null}
      </Async>
    </>
  );
}
