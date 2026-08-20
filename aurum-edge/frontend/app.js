/*
 * AURUM EDGE dashboard.
 *
 * This file is the whole frontend: no build step, no framework, no data of its
 * own.  It renders exactly what /api/state returns and nothing else - it never
 * recomputes P&L, never caches a number across updates, and greys itself out
 * the moment the backend stops answering, so an old screen can never be read as
 * a live one.
 *
 * REQUIRED_STATE_PATHS is the contract with the backend: the test-suite reads
 * this array out of this file and asserts every path exists in the payload the
 * engine produces.  Adding a field here without adding it to the backend fails
 * the build.
 */

const REQUIRED_STATE_PATHS = [
  "mode",
  "feed_source",
  "feed_is_real",
  "version",
  "run_id",
  "db_path",
  "uptime_s",
  "cycle_ms",
  "health.system_state",
  "health.trading_allowed",
  "health.blocking_reasons",
  "health.components",
  "market.connections",
  "market.connections_live",
  "market.connections_total",
  "market.symbols",
  "market.focus_size",
  "market.books_ok",
  "market.latency_ms_p50",
  "market.latency_ms_p95",
  "account.equity_eur",
  "account.available_eur",
  "account.used_margin_eur",
  "account.exposure_eur",
  "account.unrealized_eur",
  "account.open_positions",
  "positions",
  "opportunities.ranked",
  "no_trade_reasons",
  "reject_summary",
  "trades",
  "stats.trades",
  "stats.win_rate",
  "stats.avg_win_eur",
  "stats.avg_loss_eur",
  "stats.expectancy_eur",
  "stats.gross_pnl_eur",
  "stats.fees_eur",
  "stats.slippage_eur",
  "stats.net_pnl_eur",
  "stats.trades_per_hour",
  "stats.max_drawdown_eur",
  "risk.kill_switch",
  "model.champion",
  "model.shadow",
  "model.learning",
  "model.events",
  "execution.entries_blocked",
];

const state = { data: null, receivedAt: 0, socket: null, endpoint: null, polling: null };

function defaultEndpoint() {
  const fromQuery = new URLSearchParams(location.search).get("api");
  if (fromQuery) return fromQuery.replace(/\/$/, "");
  const stored = localStorage.getItem("aurum.endpoint");
  if (stored) return stored;
  const host = location.hostname || "127.0.0.1";
  return `http://${host}:8100`;
}

function fmt(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return Number(value).toFixed(digits);
}
function money(value, digits = 2) {
  if (value === null || value === undefined) return "-";
  const n = Number(value);
  return `${n >= 0 ? "" : "-"}€${Math.abs(n).toFixed(digits)}`;
}
function pct(value) {
  if (value === null || value === undefined) return "-";
  return `${(Number(value) * 100).toFixed(1)}%`;
}
function signClass(value) {
  if (value === null || value === undefined) return "neutral";
  return Number(value) > 0 ? "pos" : Number(value) < 0 ? "neg" : "neutral";
}
function timeOf(ms) {
  if (!ms) return "-";
  return new Date(Number(ms)).toLocaleTimeString();
}
function el(id) { return document.getElementById(id); }

function setBadge(id, text, cls) {
  const node = el(id);
  node.textContent = text;
  node.className = `badge ${cls || ""}`;
}

/* ------------------------------------------------------------------ render */

function render(data) {
  if (!data) return;
  el("version").textContent = data.version;

  const live = data.mode === "LIVE";
  setBadge("mode-badge", data.mode, live ? "live" : "ok");
  setBadge(
    "feed-badge",
    `FEED: ${String(data.feed_source).toUpperCase()}`,
    data.feed_is_real ? "ok" : "warn"
  );
  const st = data.health.system_state;
  setBadge(
    "state-badge",
    st,
    st === "LIVE_READY" ? (data.health.trading_allowed ? "ok" : "warn") : "bad"
  );

  renderKpis(data);
  renderHealth(data);
  renderPositions(data);
  renderOpportunities(data);
  renderReasons(data);
  renderTrades(data);
  renderModel(data);
  renderMarket(data);

  el("footer-run").textContent = `run ${data.run_id}`;
  el("footer-db").textContent = `db ${data.db_path}`;
}

function renderKpis(d) {
  const s = d.stats;
  const a = d.account;
  const kpis = [
    ["equity", money(a.equity_eur), `free ${money(a.available_eur)}`, "neutral"],
    ["used / exposure", money(a.used_margin_eur), `exposure ${money(a.exposure_eur)}`, "neutral"],
    ["net P&L", money(s.net_pnl_eur), `gross ${money(s.gross_pnl_eur)}`, signClass(s.net_pnl_eur)],
    ["fees", money(s.fees_eur), `slippage ${money(s.slippage_eur)}`, "neutral"],
    ["trades", String(s.trades), `${fmt(s.trades_per_hour, 1)} / hour`, "neutral"],
    ["win rate", pct(s.win_rate), `${s.wins ?? 0}W / ${s.losses ?? 0}L`, "neutral"],
    ["avg win / loss", money(s.avg_win_eur), `loss ${money(s.avg_loss_eur)}`, signClass(s.avg_win_eur)],
    ["expectancy", money(s.expectancy_eur, 3), "per trade, net", signClass(s.expectancy_eur)],
    ["drawdown", money(s.max_drawdown_eur), "peak to trough", "neutral"],
    ["unrealized", money(a.unrealized_eur), `${a.open_positions} open`, signClass(a.unrealized_eur)],
    ["symbols", String(d.market.symbols), `focus ${d.market.focus_size}, books ${d.market.books_ok}`, "neutral"],
    ["latency", `${fmt(d.market.latency_ms_p50, 0)}ms`, `p95 ${fmt(d.market.latency_ms_p95, 0)}ms`, "neutral"],
    ["cycle", `${fmt(d.cycle_ms, 1)}ms`, `${d.cycles} cycles`, "neutral"],
    ["uptime", `${fmt(d.uptime_s / 60, 1)}m`, `mode ${d.mode}`, "neutral"],
  ];
  el("kpis").innerHTML = kpis
    .map(
      ([label, value, sub, cls]) =>
        `<div class="kpi"><div class="label">${label}</div>` +
        `<div class="value ${cls}">${value}</div><div class="sub">${sub}</div></div>`
    )
    .join("");
}

function renderHealth(d) {
  const health = d.health;
  el("components").innerHTML = health.components
    .map(
      (c) =>
        `<div class="component ${c.state.toLowerCase()}">` +
        `<div class="name">${c.name} <span class="${
          c.state === "OK" ? "pos" : c.state === "DEGRADED" ? "neutral" : "neg"
        }">${c.state}</span></div>` +
        `<div class="detail">${escapeHtml(c.detail)}</div></div>`
    )
    .join("");
  el("health-summary").textContent = health.trading_allowed
    ? "trading allowed"
    : "new entries blocked";

  const blocking = el("blocking");
  const reasons = health.blocking_reasons || [];
  if (reasons.length || d.execution.entries_blocked || d.risk.kill_switch) {
    const extra = [];
    if (d.risk.kill_switch) extra.push(`kill switch: ${d.risk.kill_reason}`);
    if (d.execution.entries_blocked) extra.push(d.execution.entries_blocked_reason);
    blocking.classList.remove("hidden");
    blocking.innerHTML =
      "<strong>new entries are blocked because:</strong><ul>" +
      [...reasons, ...extra].map((r) => `<li>${escapeHtml(r)}</li>`).join("") +
      "</ul>";
  } else {
    blocking.classList.add("hidden");
  }
}

function renderPositions(d) {
  const rows = d.positions || [];
  el("positions-count").textContent = `${rows.length} open`;
  fillTable(
    "positions",
    rows,
    (p) => `
      <td>${p.symbol}</td>
      <td class="${p.side === "LONG" ? "pos" : "neg"}">${p.side}</td>
      <td>${p.qty}</td>
      <td>${fmt(p.entry_price, 6)}</td>
      <td>${p.mark_price === null ? "-" : fmt(p.mark_price, 6)}</td>
      <td>${fmt(p.leverage, 1)}x</td>
      <td>${money(p.margin_eur)}</td>
      <td class="${signClass(p.unrealized_eur)}">${money(p.unrealized_eur, 3)}</td>
      <td>${money(p.target_eur, 2)}</td>
      <td>${fmt(p.age_s, 1)}s</td>
      <td>${p.state}</td>`,
    "no open positions"
  );
}

function renderOpportunities(d) {
  const rows = (d.opportunities && d.opportunities.ranked) || [];
  el("opps-count").textContent = `${rows.length} of ${
    (d.opportunities && d.opportunities.considered) || 0
  } considered`;
  fillTable(
    "opportunities",
    rows,
    (o) => {
      const s = o.snapshot;
      return `
      <td>${o.symbol}</td>
      <td class="${o.side === "LONG" ? "pos" : "neg"}">${o.side}</td>
      <td>${fmt(o.score, 3)}</td>
      <td class="${signClass(s.ret_1s_bps)}">${fmt(s.ret_1s_bps, 1)}</td>
      <td class="${signClass(s.ret_5s_bps)}">${fmt(s.ret_5s_bps, 1)}</td>
      <td class="${signClass(s.ofi_5s)}">${fmt(s.ofi_5s, 2)}</td>
      <td class="${signClass(s.imbalance_top)}">${fmt(s.imbalance_top, 2)}</td>
      <td class="${signClass(s.aggression_5s)}">${fmt(s.aggression_5s, 2)}</td>
      <td>${fmt(s.volume_accel, 2)}</td>
      <td>${fmt(s.spread_bps, 2)}</td>
      <td class="${s.quality === "OK" ? "pos" : "neutral"}">${s.quality}</td>`;
    },
    "no opportunity above the shortlist threshold"
  );
}

function renderReasons(d) {
  const rows = d.no_trade_reasons || [];
  el("reasons-count").textContent = `${rows.length} candidates rejected now`;
  fillTable(
    "reasons",
    rows,
    (r) => `
      <td>${r.symbol}</td>
      <td>${r.side}</td>
      <td>${fmt(r.probability, 3)}</td>
      <td>${fmt(r.required_move_bps, 1)}bps</td>
      <td class="${signClass(r.expectancy_eur)}">${money(r.expectancy_eur, 3)}</td>
      <td class="wrap">${escapeHtml(r.reason)}</td>`,
    "nothing rejected in the last cycle"
  );
  fillTable(
    "rejects",
    d.reject_summary || [],
    (r) => `<td>${r.count}</td><td class="wrap">${escapeHtml(r.reason)}</td>`,
    "no rejections recorded yet"
  );
}

function renderTrades(d) {
  const rows = d.trades || [];
  el("trades-count").textContent = `${rows.length} most recent`;
  fillTable(
    "trades",
    rows,
    (t) => `
      <td>${timeOf(t.exit_ts)}</td>
      <td>${t.symbol}</td>
      <td class="${t.side === "LONG" ? "pos" : "neg"}">${t.side}</td>
      <td>${fmt(t.hold_s, 1)}s</td>
      <td class="${signClass(t.gross_pnl_eur)}">${money(t.gross_pnl_eur, 3)}</td>
      <td>${money(t.fees_eur, 3)}</td>
      <td>${money(t.slippage_eur, 3)}</td>
      <td class="${signClass(t.net_pnl_eur)}">${money(t.net_pnl_eur, 3)}</td>
      <td class="wrap">${escapeHtml(String(t.exit_reason).slice(0, 60))}</td>`,
    "no closed trades yet"
  );
}

function renderModel(d) {
  const m = d.model;
  const learning = m.learning || {};
  el("model").innerHTML = kv({
    champion: m.champion,
    shadow: m.shadow || "none",
    "learning status": learning.status || "idle",
    detail: learning.detail || "",
    promotion: learning.promotion || "-",
    "labelled samples": (d.counters && d.counters.labelled_decisions) || 0,
    "decisions logged": (d.counters && d.counters.decisions_logged) || 0,
  });
  fillTable(
    "model-events",
    m.events || [],
    (e) => `<td>${timeOf(e.ts_ms)}</td><td>${e.version}</td><td>${e.event}</td>`,
    "no model events yet"
  );
}

function renderMarket(d) {
  const m = d.market;
  el("market").innerHTML = kv({
    source: m.source + (m.simulated ? "  (SIMULATED)" : ""),
    connections: `${m.connections_live}/${m.connections_total} live`,
    symbols: `${m.symbols} in universe, ${m.focus_size} in focus`,
    "books in sync": `${m.books_ok} (${m.focus_books_ok} in focus)`,
    messages: m.messages,
    "reconcile": d.reconcile
      ? `${d.reconcile.trigger} -> ${d.reconcile.ok ? "OK" : "FAILED"} (${
          d.reconcile.differences.length
        } differences)`
      : "not run yet",
  });
  fillTable(
    "connections",
    m.connections || [],
    (c) => `
      <td>${c.name}</td>
      <td class="${c.live ? "pos" : "neg"}">${c.state}</td>
      <td>${c.topics}</td>
      <td>${c.messages}</td>
      <td>${c.reconnects}</td>
      <td>${c.age_ms === null ? "-" : fmt(c.age_ms, 0) + "ms"}</td>`,
    "no connection"
  );
}

function kv(obj) {
  return Object.entries(obj)
    .map(([k, v]) => `<div class="k">${k}</div><div class="v">${escapeHtml(String(v))}</div>`)
    .join("");
}

function fillTable(id, rows, rowFn, emptyText) {
  const body = el(id).querySelector("tbody");
  const columns = el(id).querySelectorAll("thead th").length;
  if (!rows.length) {
    body.innerHTML = `<tr><td class="empty" colspan="${columns}">${emptyText}</td></tr>`;
    return;
  }
  body.innerHTML = rows.map((row) => `<tr>${rowFn(row)}</tr>`).join("");
}

function escapeHtml(text) {
  return String(text).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

/* --------------------------------------------------------------- transport */

function connect(endpoint) {
  state.endpoint = endpoint.replace(/\/$/, "");
  localStorage.setItem("aurum.endpoint", state.endpoint);
  el("endpoint").value = state.endpoint;

  if (state.socket) { try { state.socket.close(); } catch (e) { /* ignore */ } }
  const wsUrl = state.endpoint.replace(/^http/, "ws") + "/ws";
  setBadge("link-badge", "CONNECTING", "warn");
  let socket;
  try {
    socket = new WebSocket(wsUrl);
  } catch (e) {
    startPolling();
    return;
  }
  state.socket = socket;

  socket.onopen = () => {
    setBadge("link-badge", "BACKEND LIVE", "ok");
    stopPolling();
  };
  socket.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    if (payload.type === "state") {
      state.data = payload.data;
      state.receivedAt = Date.now();
      render(state.data);
    }
  };
  socket.onclose = () => {
    setBadge("link-badge", "BACKEND LOST", "bad");
    startPolling();
    setTimeout(() => connect(state.endpoint), 3000);
  };
  socket.onerror = () => socket.close();
}

function startPolling() {
  if (state.polling) return;
  state.polling = setInterval(async () => {
    try {
      const resp = await fetch(`${state.endpoint}/api/state`);
      state.data = await resp.json();
      state.receivedAt = Date.now();
      setBadge("link-badge", "BACKEND (polling)", "warn");
      render(state.data);
    } catch (e) {
      setBadge("link-badge", "BACKEND UNREACHABLE", "bad");
    }
  }, 1500);
}

function stopPolling() {
  if (state.polling) { clearInterval(state.polling); state.polling = null; }
}

function watchFreshness() {
  setInterval(() => {
    const banner = el("banner");
    if (!state.receivedAt) {
      banner.className = "banner";
      banner.textContent =
        "no data from the backend yet - is it running?  " +
        "start it with: python -m aurum_edge run";
      return;
    }
    const age = (Date.now() - state.receivedAt) / 1000;
    el("footer-age").textContent = `data age ${age.toFixed(1)}s`;
    if (age > 5) {
      banner.className = "banner";
      banner.textContent =
        `THIS SCREEN IS ${age.toFixed(0)}s OLD - the backend is not answering. ` +
        "Nothing here is live.";
      document.body.style.opacity = "0.55";
    } else if (age > 2.5) {
      banner.className = "banner stale";
      banner.textContent = `data is ${age.toFixed(1)}s old`;
      document.body.style.opacity = "1";
    } else {
      banner.className = "banner hidden";
      document.body.style.opacity = "1";
    }
  }, 500);
}

async function post(path, body) {
  const resp = await fetch(`${state.endpoint}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  return resp.json();
}

document.addEventListener("DOMContentLoaded", () => {
  el("endpoint").value = defaultEndpoint();
  connect(defaultEndpoint());
  watchFreshness();

  el("btn-reconnect").onclick = () => connect(el("endpoint").value);
  el("btn-kill").onclick = async () => {
    if (!confirm("Engage the kill switch and close every open position?")) return;
    const result = await post("/api/control/kill", { reason: "dashboard" });
    alert(`kill switch engaged, ${result.positions_closed} position(s) closed`);
  };
  el("btn-flatten").onclick = async () => {
    if (!confirm("Close every open position now?")) return;
    const result = await post("/api/control/flatten", {});
    alert(`${result.positions_closed} position(s) closed`);
  };
});
