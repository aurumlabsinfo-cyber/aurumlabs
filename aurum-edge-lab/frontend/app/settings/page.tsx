"use client";

import { useEffect, useState } from "react";

import { Async, Note, Panel, Pill } from "@/components/ui";
import { apiPost, useApi } from "@/lib/api";
import { num } from "@/lib/format";

interface SettableRow {
  path: string;
  value: number | boolean;
  type: string;
  min: number | null;
  max: number | null;
  note: string;
}

interface ConfigPayload {
  config: Record<string, Record<string, unknown>>;
  settable: SettableRow[];
  symbols: { symbol: string; tier: string; role: string; use: string }[];
  note: string;
}

export default function SettingsPage() {
  const { data, error, loading, refresh } = useApi<ConfigPayload>("/config", 0);
  const [draft, setDraft] = useState<Record<string, string>>({});
  const [status, setStatus] = useState<{ kind: "ok" | "bad"; message: string } | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!data) return;
    setDraft(Object.fromEntries(data.settable.map((row) => [row.path, String(row.value)])));
  }, [data]);

  const dirty =
    data?.settable.filter((row) => draft[row.path] !== undefined && draft[row.path] !== String(row.value)) ??
    [];

  const save = async () => {
    if (!dirty.length) return;
    setSaving(true);
    setStatus(null);
    const payload: Record<string, unknown> = {};
    for (const row of dirty) {
      const raw = draft[row.path];
      payload[row.path] = row.type === "bool" ? raw === "true" : Number(raw);
    }
    try {
      const result = await apiPost<{ applied: Record<string, unknown>; rejected: Record<string, string> }>(
        "/config/settings",
        payload,
      );
      const rejected = Object.entries(result.rejected ?? {});
      if (rejected.length) {
        setStatus({
          kind: "bad",
          message: rejected.map(([path, reason]) => `${path}: ${reason}`).join(" · "),
        });
      } else {
        setStatus({
          kind: "ok",
          message: `Applied ${Object.keys(result.applied).length} setting(s).`,
        });
      }
      refresh();
    } catch (exc) {
      setStatus({ kind: "bad", message: exc instanceof Error ? exc.message : String(exc) });
    } finally {
      setSaving(false);
    }
  };

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="page-title">Settings</h1>
          <p className="page-sub">
            Safe, non-structural controls only. Every value is validated and hard-bounded on the
            server; a value outside its range is refused, never clamped.
          </p>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button onClick={() => data && setDraft(Object.fromEntries(data.settable.map((r) => [r.path, String(r.value)])))} disabled={!dirty.length}>
            Reset
          </button>
          <button className="primary" onClick={save} disabled={!dirty.length || saving}>
            {saving ? "Applying…" : `Apply ${dirty.length || ""}`}
          </button>
        </div>
      </div>

      {status ? (
        <Note kind={status.kind === "ok" ? "info" : "bad"}>
          <strong>{status.kind === "ok" ? "Saved." : "Refused."}</strong> {status.message}
        </Note>
      ) : null}

      <Async loading={loading} error={error} empty={!data}>
        {data ? (
          <>
            <Note kind="neutral">{data.note}</Note>

            <Panel title="Runtime settings" note="bounded server-side" flush>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Setting</th>
                      <th className="wrap">What it does</th>
                      <th className="num">Current</th>
                      <th className="num">Range</th>
                      <th>New value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.settable.map((row) => {
                      const changed = draft[row.path] !== undefined && draft[row.path] !== String(row.value);
                      return (
                        <tr key={row.path}>
                          <td className="mono" style={{ fontSize: 11.5 }}>
                            {row.path}
                          </td>
                          <td className="wrap" style={{ fontSize: 11.5 }}>
                            {row.note}
                          </td>
                          <td className="num">
                            {row.type === "bool" ? String(row.value) : num(Number(row.value), 2)}
                          </td>
                          <td className="num faint">
                            {row.min === null || row.max === null
                              ? "—"
                              : `${num(row.min, 2)} … ${num(row.max, 2)}`}
                          </td>
                          <td>
                            {row.type === "bool" ? (
                              <select
                                value={draft[row.path] ?? String(row.value)}
                                onChange={(e) =>
                                  setDraft({ ...draft, [row.path]: e.target.value })
                                }
                              >
                                <option value="true">true</option>
                                <option value="false">false</option>
                              </select>
                            ) : (
                              <input
                                type="number"
                                step="0.01"
                                min={row.min ?? undefined}
                                max={row.max ?? undefined}
                                value={draft[row.path] ?? String(row.value)}
                                onChange={(e) => setDraft({ ...draft, [row.path]: e.target.value })}
                                style={{
                                  width: 110,
                                  borderColor: changed ? "var(--accent)" : undefined,
                                }}
                              />
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </Panel>

            <Panel title="Universe" note="structural — requires a restart with a new config file" flush>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Symbol</th>
                      <th>Tier</th>
                      <th>Role</th>
                      <th className="wrap">Research use</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.symbols.map((symbol) => (
                      <tr key={symbol.symbol}>
                        <td className="mono">{symbol.symbol}</td>
                        <td>
                          <Pill kind={symbol.tier === "CORE" ? "info" : "neutral"}>{symbol.tier}</Pill>
                        </td>
                        <td className="faint">{symbol.role}</td>
                        <td className="wrap faint" style={{ fontSize: 11.5 }}>
                          {symbol.use}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Panel>

            <Panel title="Effective configuration" note="read-only">
              <details>
                <summary className="stat-label" style={{ cursor: "pointer" }}>
                  Show the full configuration as served by /config
                </summary>
                <pre
                  className="mono"
                  style={{
                    marginTop: 10,
                    fontSize: 11,
                    background: "var(--bg-raised)",
                    padding: 12,
                    borderRadius: 4,
                    overflowX: "auto",
                    maxHeight: 420,
                  }}
                >
                  {JSON.stringify(data.config, null, 2)}
                </pre>
              </details>
            </Panel>
          </>
        ) : null}
      </Async>
    </>
  );
}
