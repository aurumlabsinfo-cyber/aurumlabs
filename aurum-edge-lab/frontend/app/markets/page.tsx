"use client";

import { useEffect, useState } from "react";

import { Async, Bps, Empty, Panel, Pill, Sparkline, Stat } from "@/components/ui";
import { useApi } from "@/lib/api";
import { bps, clock, integer, num, price, pct } from "@/lib/format";
import { useLive } from "@/lib/live";

interface SymbolDetail {
  symbol: string;
  book: {
    state: string;
    ready: boolean;
    last_update_id: number;
    levels: { bid: number; ask: number };
    best_bid: number | null;
    best_ask: number | null;
    mid: number | null;
    spread_bps: number | null;
    is_crossed: boolean;
    buffered: number;
    stats: Record<string, number | string>;
  };
  quality: {
    score: number;
    state: string;
    flags: string[];
    latency_ms: number;
    events_per_min: number;
    sequence_gaps: number;
    resyncs: number;
    book_levels: number;
  } | null;
  features: { values: Record<string, number>; regime: string } | null;
  regime: string;
  mark_price: number;
  index_price: number;
  funding_rate: number;
  latency: Record<string, number>;
  recent_trades: { ts_ms: number; price: number; qty: number; aggressor: string }[];
  history: { ts_ms: number[]; mid: number[] };
}

interface BookView {
  bids: [number, number][];
  asks: [number, number][];
  mid: number | null;
  microprice: number | null;
  spread_bps: number | null;
  ready: boolean;
  state: string;
}

export default function MarketsPage() {
  const { state } = useLive();
  const [selected, setSelected] = useState<string | null>(null);

  useEffect(() => {
    if (!selected && state?.markets.length) setSelected(state.markets[0].symbol);
  }, [state, selected]);

  const detail = useApi<SymbolDetail>(selected ? `/market/${selected}` : null, 1500);
  const book = useApi<BookView>(selected ? `/orderbook/${selected}?depth=12` : null, 1000);

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">Markets</h1>
          <p className="page-sub">
            The ten-symbol grid, and a drill-down into one market: book, trades, microstructure
            features and the data-quality verdict that decides whether it may be traded at all.
          </p>
        </div>
      </div>

      <Panel title="Universe" note={`${state?.markets.length ?? 0} symbols`} flush>
        {!state ? (
          <Empty>Waiting for state…</Empty>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>Tier</th>
                  <th>Role</th>
                  <th className="num">Bid</th>
                  <th className="num">Ask</th>
                  <th className="num">Mid</th>
                  <th className="num">Micro</th>
                  <th className="num">Spread bps</th>
                  <th className="num">Funding</th>
                  <th className="num">Quality</th>
                  <th>Feed</th>
                </tr>
              </thead>
              <tbody>
                {state.markets.map((row) => (
                  <tr
                    key={row.symbol}
                    onClick={() => setSelected(row.symbol)}
                    style={{
                      cursor: "pointer",
                      background: row.symbol === selected ? "var(--bg-hover)" : undefined,
                    }}
                  >
                    <td className="mono">{row.symbol}</td>
                    <td className="faint">{row.tier}</td>
                    <td className="faint" style={{ fontSize: 11.5 }}>
                      {row.role}
                    </td>
                    <td className="num">{price(row.best_bid)}</td>
                    <td className="num">{price(row.best_ask)}</td>
                    <td className="num">{price(row.mid)}</td>
                    <td className="num">{price(row.microprice)}</td>
                    <td className="num">{num(row.spread_bps, 2)}</td>
                    <td className="num">{bps(row.funding_rate * 10_000, 2)}</td>
                    <td className={`num ${row.quality?.tradable ? "up" : "warn"}`}>
                      {num(row.quality?.score ?? null, 3)}
                    </td>
                    <td>
                      <Pill kind={row.quality?.state === "LIVE" ? "ok" : "warn"}>
                        {row.quality?.state ?? "—"}
                      </Pill>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>

      {selected ? (
        <>
          <div className="page-head" style={{ marginTop: 6 }}>
            <h2 className="page-title" style={{ fontSize: 16 }}>
              {selected}
            </h2>
          </div>

          <div className="grid cols-3">
            <Panel title="Price" note="last 300 samples">
              <Async loading={detail.loading} error={detail.error}>
                {detail.data ? (
                  <>
                    <Stat
                      label="Mid"
                      value={price(detail.data.book.mid)}
                      hint={`regime ${detail.data.regime}`}
                    />
                    <div style={{ marginTop: 8 }}>
                      <Sparkline values={detail.data.history?.mid ?? []} width={260} height={54} />
                    </div>
                    <dl className="kv" style={{ marginTop: 10 }}>
                      <dt>Mark</dt>
                      <dd>{price(detail.data.mark_price)}</dd>
                      <dt>Index</dt>
                      <dd>{price(detail.data.index_price)}</dd>
                      <dt>Funding</dt>
                      <dd>{bps(detail.data.funding_rate * 10_000, 3)} / 8h</dd>
                    </dl>
                  </>
                ) : null}
              </Async>
            </Panel>

            <Panel title="Data quality" note="blocks trading independently of confidence">
              <Async loading={detail.loading} error={detail.error}>
                {detail.data?.quality ? (
                  <>
                    <Stat
                      label="Score"
                      value={num(detail.data.quality.score, 3)}
                      tone={detail.data.quality.flags.length === 0 ? "up" : "warn"}
                      hint={detail.data.quality.state}
                    />
                    <dl className="kv" style={{ marginTop: 10 }}>
                      <dt>Latency</dt>
                      <dd>{num(detail.data.quality.latency_ms, 1)} ms</dd>
                      <dt>Events / min</dt>
                      <dd>{integer(detail.data.quality.events_per_min)}</dd>
                      <dt>Sequence gaps</dt>
                      <dd className={detail.data.quality.sequence_gaps ? "warn" : ""}>
                        {integer(detail.data.quality.sequence_gaps)}
                      </dd>
                      <dt>Resyncs</dt>
                      <dd>{integer(detail.data.quality.resyncs)}</dd>
                      <dt>Book levels</dt>
                      <dd>{integer(detail.data.quality.book_levels)}</dd>
                    </dl>
                    {detail.data.quality.flags.length ? (
                      <div className="chips" style={{ marginTop: 10 }}>
                        {detail.data.quality.flags.map((flag) => (
                          <Pill key={flag} kind="warn">
                            {flag}
                          </Pill>
                        ))}
                      </div>
                    ) : (
                      <div style={{ marginTop: 10 }}>
                        <Pill kind="ok" dot>
                          tradable
                        </Pill>
                      </div>
                    )}
                  </>
                ) : (
                  <Empty>No quality assessment yet.</Empty>
                )}
              </Async>
            </Panel>

            <Panel title="Book integrity" note="sequence validation">
              <Async loading={detail.loading} error={detail.error}>
                {detail.data ? (
                  <>
                    <Stat
                      label="State"
                      value={detail.data.book.state}
                      tone={detail.data.book.ready ? "up" : "warn"}
                      small
                      hint={`last update id ${integer(detail.data.book.last_update_id)}`}
                    />
                    <dl className="kv" style={{ marginTop: 10 }}>
                      <dt>Levels bid / ask</dt>
                      <dd>
                        {integer(detail.data.book.levels.bid)} / {integer(detail.data.book.levels.ask)}
                      </dd>
                      <dt>Crossed</dt>
                      <dd className={detail.data.book.is_crossed ? "down" : ""}>
                        {detail.data.book.is_crossed ? "YES" : "no"}
                      </dd>
                      <dt>Buffered diffs</dt>
                      <dd>{integer(detail.data.book.buffered)}</dd>
                      <dt>Updates applied</dt>
                      <dd>{integer(Number(detail.data.book.stats.updates_applied))}</dd>
                      <dt>Snapshots</dt>
                      <dd>{integer(Number(detail.data.book.stats.snapshots_applied))}</dd>
                      <dt>Gaps</dt>
                      <dd className={Number(detail.data.book.stats.sequence_gaps) ? "warn" : ""}>
                        {integer(Number(detail.data.book.stats.sequence_gaps))}
                      </dd>
                    </dl>
                    {detail.data.book.stats.last_gap_detail ? (
                      <p className="faint" style={{ fontSize: 11.5, marginTop: 8 }}>
                        {String(detail.data.book.stats.last_gap_detail)}
                      </p>
                    ) : null}
                  </>
                ) : null}
              </Async>
            </Panel>
          </div>

          <div className="grid cols-2">
            <Panel title="Order book" note="top 12 levels" flush>
              <Async loading={book.loading} error={book.error}>
                {book.data?.ready ? (
                  <div className="table-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th className="num">Bid qty</th>
                          <th className="num">Bid</th>
                          <th className="num">Ask</th>
                          <th className="num">Ask qty</th>
                        </tr>
                      </thead>
                      <tbody>
                        {book.data.bids.map((bid, index) => {
                          const ask = book.data!.asks[index];
                          return (
                            <tr key={index}>
                              <td className="num faint">{num(bid?.[1] ?? null, 4)}</td>
                              <td className="num up">{price(bid?.[0] ?? null)}</td>
                              <td className="num down">{price(ask?.[0] ?? null)}</td>
                              <td className="num faint">{num(ask?.[1] ?? null, 4)}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <Empty>Book is {book.data?.state ?? "not ready"}.</Empty>
                )}
              </Async>
            </Panel>

            <Panel title="Microstructure features" note="live snapshot">
              <Async loading={detail.loading} error={detail.error}>
                {detail.data?.features ? (
                  <FeatureGrid values={detail.data.features.values} />
                ) : (
                  <Empty>No feature snapshot yet.</Empty>
                )}
              </Async>
            </Panel>
          </div>

          <Panel title="Trade tape" note="most recent first" flush>
            <Async loading={detail.loading} error={detail.error}>
              {detail.data?.recent_trades?.length ? (
                <div className="table-wrap scroll-y">
                  <table>
                    <thead>
                      <tr>
                        <th>Time</th>
                        <th className="num">Price</th>
                        <th className="num">Qty</th>
                        <th>Aggressor</th>
                      </tr>
                    </thead>
                    <tbody>
                      {[...detail.data.recent_trades].reverse().map((trade, index) => (
                        <tr key={`${trade.ts_ms}-${index}`}>
                          <td className="faint mono">{clock(trade.ts_ms)}</td>
                          <td className="num">{price(trade.price)}</td>
                          <td className="num">{num(trade.qty, 4)}</td>
                          <td className={trade.aggressor === "BUY" ? "up" : "down"}>
                            {trade.aggressor}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <Empty>No trades recorded for this symbol yet.</Empty>
              )}
            </Async>
          </Panel>
        </>
      ) : null}
    </>
  );
}

const FEATURE_GROUPS: [string, string[]][] = [
  ["Price", ["mid", "microprice", "spread_bps", "micro_dev_bps"]],
  ["Returns", ["ret_250ms", "ret_1s", "ret_5s", "ret_30s", "ret_60s"]],
  ["Volatility", ["vol_1s", "vol_10s", "vol_60s", "vol_ratio", "vol_percentile"]],
  ["Book", ["imbalance_1", "imbalance_5", "imbalance_10", "depth_bid_5", "depth_ask_5"]],
  ["Order flow", ["ofi_norm_1s", "ofi_norm_5s", "flow_imbalance_1s", "trades_1s", "buy_vol_1s"]],
  ["Liquidity", ["liq_net_1s", "liq_pressure_1s", "liq_added_bid_1s", "liq_removed_ask_1s"]],
  ["Derivatives", ["mark_dev_bps", "index_dev_bps", "funding_bps_8h", "funding_countdown_s"]],
  ["Meta", ["quality_score", "latency_ms", "book_age_ms", "trend_z"]],
];

function FeatureGrid({ values }: { values: Record<string, number> }) {
  return (
    <div style={{ display: "grid", gap: 10 }}>
      {FEATURE_GROUPS.map(([group, keys]) => {
        const present = keys.filter((key) => key in values);
        if (present.length === 0) return null;
        return (
          <div key={group}>
            <div className="stat-label" style={{ marginBottom: 4 }}>
              {group}
            </div>
            <dl className="kv">
              {present.map((key) => (
                <FeatureRow key={key} name={key} value={values[key]} />
              ))}
            </dl>
          </div>
        );
      })}
    </div>
  );
}

function FeatureRow({ name, value }: { name: string; value: number }) {
  const isBps = name.endsWith("_bps") || name.startsWith("ret_") || name.startsWith("vol_");
  return (
    <>
      <dt>{name}</dt>
      <dd>
        {isBps && !name.startsWith("vol_") ? (
          <Bps value={value} />
        ) : name === "vol_percentile" ? (
          pct(value, 1)
        ) : (
          num(value, Math.abs(value) < 1 ? 5 : 3)
        )}
      </dd>
    </>
  );
}
