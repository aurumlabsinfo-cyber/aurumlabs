"use client";

import { useState } from "react";

import { Async, Bps, Empty, Note, Panel, Pill, Stat } from "@/components/ui";
import { useApi } from "@/lib/api";
import { duration, integer, num } from "@/lib/format";
import type { CrossMarketCell } from "@/lib/types";

interface CrossMarketPayload {
  matrix: {
    symbols: string[];
    lags_ms: number[];
    samples: number;
    refreshes: number;
    correlation: number[][];
    cells: (CrossMarketCell | null)[][];
  };
  ranked: CrossMarketCell[];
  stats: {
    pairs: number;
    lags: number;
    samples: number;
    refreshes: number;
    pairs_with_positive_net_edge: number;
    best: CrossMarketCell | null;
  };
}

type Metric = "predictive_score" | "best_correlation" | "conditional_net_edge_bps" | "t_stat";

const METRICS: { key: Metric; label: string; help: string }[] = [
  {
    key: "predictive_score",
    label: "Predictive score",
    help: "|correlation| weighted by how much evidence is behind it.",
  },
  {
    key: "best_correlation",
    label: "Correlation at best lag",
    help: "The strongest lagged correlation found across all eleven horizons.",
  },
  {
    key: "t_stat",
    label: "t-statistic",
    help: "Whether that correlation is distinguishable from noise at this sample size.",
  },
  {
    key: "conditional_net_edge_bps",
    label: "Net edge after costs",
    help: "What the relationship would have paid, conditional on the leader moving, minus a full round trip. Usually negative — that is the honest answer.",
  },
];

export default function CrossMarketPage() {
  const { data, error, loading } = useApi<CrossMarketPayload>("/cross-market", 5000);
  const [metric, setMetric] = useState<Metric>("predictive_score");
  const [focus, setFocus] = useState<CrossMarketCell | null>(null);

  const active = METRICS.find((m) => m.key === metric)!;

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">Cross Market</h1>
          <p className="page-sub">
            Every ordered pair among the configured markets, examined at eleven lead-lag horizons
            from 100 ms to 60 s. Rows lead, columns follow.
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <select value={metric} onChange={(event) => setMetric(event.target.value as Metric)}>
            {METRICS.map((option) => (
              <option key={option.key} value={option.key}>
                {option.label}
              </option>
            ))}
          </select>
        </div>
      </div>

      <Async loading={loading} error={error} empty={!data} emptyMessage="No cross-market state yet.">
        {data ? (
          <>
            <div className="grid cols-4">
              <Panel title="Pairs">
                <Stat
                  label="Ordered relationships"
                  value={integer(data.stats.pairs)}
                  hint={`${data.stats.lags} lags each`}
                />
              </Panel>
              <Panel title="Samples">
                <Stat
                  label="Aligned observations"
                  value={integer(data.stats.samples)}
                  hint={`${integer(data.stats.refreshes)} refreshes`}
                />
              </Panel>
              <Panel title="Tradable after costs">
                <Stat
                  label="Pairs with positive net edge"
                  value={integer(data.stats.pairs_with_positive_net_edge)}
                  tone={data.stats.pairs_with_positive_net_edge > 0 ? "up" : "flat"}
                  hint={
                    data.stats.pairs_with_positive_net_edge === 0
                      ? "expected: correlation at these horizons rarely survives the spread"
                      : "candidates for the cross-crypto agent"
                  }
                />
              </Panel>
              <Panel title="Strongest">
                {data.stats.best ? (
                  <Stat
                    label={`${data.stats.best.leader} → ${data.stats.best.follower}`}
                    value={<Bps value={data.stats.best.conditional_net_edge_bps} />}
                    small
                    hint={`lag ${duration(data.stats.best.best_lag_ms)} · r=${num(
                      data.stats.best.best_correlation,
                      3,
                    )}`}
                  />
                ) : (
                  <Empty>No relations computed yet.</Empty>
                )}
              </Panel>
            </div>

            <Note kind="neutral">
              <strong>{active.label}.</strong> {active.help}
            </Note>

            <Panel title="Matrix" note="row leads · column follows" flush>
              {data.matrix.symbols.length === 0 ? (
                <Empty>
                  Not enough aligned history yet — the matrix needs the configured minimum sample
                  count on at least two symbols.
                </Empty>
              ) : (
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>leader ↓ / follower →</th>
                        {data.matrix.symbols.map((symbol) => (
                          <th key={symbol} className="num">
                            {symbol.replace("USDT", "")}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {data.matrix.cells.map((row, i) => (
                        <tr key={data.matrix.symbols[i]}>
                          <td className="mono">{data.matrix.symbols[i]}</td>
                          {row.map((cell, j) => (
                            <MatrixCell
                              key={`${i}-${j}`}
                              cell={cell}
                              metric={metric}
                              onClick={() => setFocus(cell)}
                            />
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Panel>

            {focus ? (
              <Panel
                title={`${focus.leader} → ${focus.follower}`}
                note={`best lag ${duration(focus.best_lag_ms)}`}
              >
                <div className="grid cols-4">
                  <Stat
                    label="Contemporaneous r"
                    value={num(focus.correlation, 4)}
                    small
                    hint="same instant"
                  />
                  <Stat
                    label="Best lagged r"
                    value={num(focus.best_correlation, 4)}
                    small
                    hint={`t = ${num(focus.t_stat, 2)} over ${integer(focus.samples)} samples`}
                  />
                  <Stat
                    label="Conditional gross"
                    value={<Bps value={focus.conditional_edge_bps} />}
                    small
                    hint={`${integer(focus.conditional_samples)} triggered observations`}
                  />
                  <Stat
                    label="Net of costs"
                    value={<Bps value={focus.conditional_net_edge_bps} />}
                    small
                    hint={`round trip ${num(focus.cost_bps, 2)} bps`}
                  />
                </div>
                <p className="faint" style={{ fontSize: 12, marginTop: 10, marginBottom: 0 }}>
                  {focus.conditional_net_edge_bps > 0
                    ? "This pair covers its costs on the measured window. It is a candidate for the cross-crypto agent, not a validated edge — it still has to survive out-of-sample validation and live shadow evaluation."
                    : `Gross ${num(focus.conditional_edge_bps, 2)} bps does not cover the ${num(
                        focus.cost_bps,
                        2,
                      )} bps round trip. The relationship is real and not tradable, which is the ordinary outcome at these horizons.`}
                </p>
              </Panel>
            ) : null}

            <Panel title="Strongest relationships" note="ranked by predictive score" flush>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Leader</th>
                      <th>Follower</th>
                      <th className="num">Lag</th>
                      <th className="num">r</th>
                      <th className="num">t</th>
                      <th className="num">Score</th>
                      <th className="num">Gross bps</th>
                      <th className="num">Cost bps</th>
                      <th className="num">Net bps</th>
                      <th className="num">Obs</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.ranked.map((cell) => (
                      <tr
                        key={`${cell.leader}-${cell.follower}`}
                        onClick={() => setFocus(cell)}
                        style={{ cursor: "pointer" }}
                      >
                        <td className="mono">{cell.leader}</td>
                        <td className="mono">{cell.follower}</td>
                        <td className="num">{duration(cell.best_lag_ms)}</td>
                        <td className="num">{num(cell.best_correlation, 3)}</td>
                        <td className="num">{num(cell.t_stat, 1)}</td>
                        <td className="num">{num(cell.predictive_score, 3)}</td>
                        <td className="num">
                          <Bps value={cell.conditional_edge_bps} />
                        </td>
                        <td className="num down">{num(cell.cost_bps, 2)}</td>
                        <td className="num">
                          <Bps value={cell.conditional_net_edge_bps} />
                        </td>
                        <td className="num faint">{integer(cell.conditional_samples)}</td>
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

function MatrixCell({
  cell,
  metric,
  onClick,
}: {
  cell: CrossMarketCell | null;
  metric: Metric;
  onClick: () => void;
}) {
  if (!cell) {
    return (
      <td className="num faint" style={{ background: "var(--bg-raised)" }}>
        ·
      </td>
    );
  }
  const value = cell[metric];
  const scale =
    metric === "conditional_net_edge_bps"
      ? Math.max(-1, Math.min(1, value / 20))
      : metric === "t_stat"
        ? Math.max(-1, Math.min(1, value / 8))
        : Math.max(-1, Math.min(1, value));
  const alpha = Math.min(0.42, Math.abs(scale) * 0.42);
  const colour = scale >= 0 ? `rgba(53,208,127,${alpha})` : `rgba(255,92,92,${alpha})`;
  const digits = metric === "conditional_net_edge_bps" ? 1 : metric === "t_stat" ? 1 : 3;
  return (
    <td
      className="num"
      title={`${cell.leader} → ${cell.follower} · lag ${cell.best_lag_ms} ms · ${cell.conditional_samples} obs`}
      onClick={onClick}
      style={{ background: colour, cursor: "pointer" }}
    >
      {num(value, digits)}
    </td>
  );
}
