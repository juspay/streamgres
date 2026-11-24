"use strict";
const $ = (id) => document.getElementById(id);
const state = {
  tab: "overview",
  shapes: [],
  metrics: null,
  events: [],
  seen: new Set(),
  latencies: [],
  selected: null,
  org: 1,
  page: 1,
  wait: true,
  racing: false,
  writer: null,
  verifying: false,
  lab: false,
  orgs: [],
  instance: null,
};
const names = {
  ticket: "One ticket by ID",
  card: "One ticket card by ID",
  queue_cards: "Open ticket cards, newest first",
  queue: "Open tickets, newest first",
  queue_count: "Count open tickets",
  comments: "Tickets with their comments",
  attachment: "An attached file",
  agents: "Agents and their skills",
  sla: "Ticket service-level agreements",
  search: "Search ticket titles",
  recent: "Recently changed tickets",
  org_ticket: "A ticket within an organization",
  unassigned: "Tickets without an assignee",
  escalated: "High-priority or pending tickets",
  open_except: "Open tickets, excluding selected IDs",
  created_between: "Tickets in a time range",
  many: "A hundred ticket IDs",
  queue_end: "A page past the end",
  nothing: "A query with no matches",
  all_tickets: "All tickets for an organization",
  comment_authors: "Comments, including missing authors",
  skilled: "Agents with a skill",
  statuses: "Distinct ticket statuses",
  workload: "Ticket counts by agent",
  last_activity: "Last activity in an organization",
  of_kind: "Tickets using a new enum",
};
const reasons = {
  ticket: "Returns timestamptz columns, which Streamgres v1 does not hold exactly.",
  queue: "Orders by and returns timestamptz columns.",
  comments: "Returns a timestamptz column.",
  attachment: "Returns a bytea column.",
  agents: "Returns a text[] column.",
  sla: "Returns an interval column.",
  recent: "Orders by and returns timestamptz columns.",
  created_between: "Compares timestamptz values.",
  of_kind: "Reads a column added after the engine started; its type is known after a restart.",
  search: "Uses ILIKE, which Streamgres leaves to Postgres.",
  skilled: "Uses an array containment operator.",
  statuses: "Requests DISTINCT values.",
  workload: "Groups ticket counts by agent with raw SQL.",
  last_activity: "Uses the MAX aggregate.",
};
const opNames = {
  status: "Status",
  comment: "Comment",
  insert: "New ticket",
  delete: "Delete own ticket",
  move: "Move organization",
  rename: "Rename agent",
  assign: "Assignment",
  bulk: "Batch update",
  rollback: "Rollback",
  toast: "Large text update",
};
const scenarios = [
  [
    "widen",
    "What if we add a column?",
    "Add a text column with a default. The engine can absorb it while keeping existing queries current.",
  ],
  [
    "new-type",
    "What if a new type appears?",
    "Add an enum column and query it through Diesel. Until a restart, its reads go to Postgres.",
  ],
  [
    "drop",
    "What if a column is removed?",
    "Remove the experiment columns. The engine restarts on a fresh schema and snapshot.",
  ],
  [
    "connection",
    "What if the database connection drops?",
    "Terminate only this demo’s replication connection and observe it reconnecting.",
  ],
  [
    "slot",
    "What if the replication slot disappears?",
    "Drop only this demo’s owned slot. The supervisor rebuilds the feed and its snapshot.",
  ],
  [
    "transaction",
    "What if a transaction has not committed?",
    "Hold a status update inside a transaction. Memory must not show it until it commits, then show it within the feed's delay.",
  ],
];
const esc = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const number = (n) => Number(n || 0).toLocaleString();
const us = (n) =>
  !Number.isFinite(Number(n))
    ? "—"
    : Number(n) >= 1000
      ? `${(Number(n) / 1000).toFixed(2)} ms`
      : `${Number(n).toFixed(Number(n) < 10 ? 1 : 0)} µs`;
const bytes = (n) =>
  Number(n) >= 1048576
    ? `${(Number(n) / 1048576).toFixed(1)} MiB`
    : `${(Number(n || 0) / 1024).toFixed(1)} KiB`;
const date = (value) => {
  const d = new Date(value);
  return Number.isNaN(d.valueOf())
    ? "Unknown date"
    : d.toLocaleString(undefined, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      });
};
const badge = (served, elapsed, extra = "") =>
  served === "unsupported"
    ? `<span class="answer-badge neutral" title="Refused: Streamgres does not support this query">⊘ Unsupported</span>`
    : `<span class="answer-badge ${served === "cache" ? "memory" : "postgres"}" title="${esc(served === "cache" ? "Answered by Streamgres in this process" : "Answered by the application’s Postgres connection")}">${served === "cache" ? "ϟ Memory" : "▤ Postgres"}${elapsed == null ? "" : ` ${us(elapsed)}`}${extra}</span>`;
let toastTimer;
function toast(message, error = false) {
  $("toast").textContent = message;
  $("toast").className = `toast${error ? " error" : ""}`;
  $("toast").hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(
    () => ($("toast").hidden = true),
    error ? 9000 : 5000,
  );
}
async function api(path, body, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    options.timeout || 45000,
  );
  try {
    const response = await fetch(path, {
      method: body === undefined ? "GET" : "POST",
      headers: body === undefined ? {} : { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: controller.signal,
      cache: "no-store",
    });
    const text = await response.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      data = { error: text.slice(0, 400) || response.statusText };
    }
    if (!response.ok)
      throw new Error(data.error || `Request failed (${response.status})`);
    return data;
  } catch (error) {
    if (error.name === "AbortError")
      throw new Error(
        "The response timed out. The operation may still be running; check activity before retrying a write.",
      );
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}
async function busy(button, work, label) {
  const old = button.textContent;
  button.disabled = true;
  if (label) button.textContent = label;
  try {
    return await work();
  } catch (error) {
    toast(error.message, true);
  } finally {
    button.disabled = false;
    if (!label || button.textContent === label) button.textContent = old;
  }
}
function setTab(tab) {
  if (!["overview", "desk", "queries", "lab"].includes(tab)) tab = "overview";
  state.tab = tab;
  $("comparison-metric").hidden = tab !== "overview";
  document.querySelectorAll("[role=tab]").forEach((b) => {
    const active = b.dataset.tab === tab;
    b.setAttribute("aria-selected", String(active));
    b.tabIndex = active ? 0 : -1;
  });
  document.querySelectorAll(".view").forEach((p) => (p.hidden = p.id !== tab));
  history.replaceState(null, "", `#${tab}`);
  if (tab === "desk") loadDesk();
  if (tab === "queries" && !state.shapes.length) loadShapes();
  if (tab === "lab") loadWriter();
}
document.querySelectorAll("[data-tab]").forEach((button) => {
  button.addEventListener("click", () => setTab(button.dataset.tab));
  button.addEventListener("keydown", (e) => {
    if (!["ArrowRight", "ArrowLeft", "Home", "End"].includes(e.key)) return;
    e.preventDefault();
    const tabs = [...document.querySelectorAll("[data-tab]")];
    const index = tabs.indexOf(button);
    const next =
      e.key === "Home"
        ? 0
        : e.key === "End"
          ? tabs.length - 1
          : (index + (e.key === "ArrowRight" ? 1 : -1) + tabs.length) %
            tabs.length;
    tabs[next].focus();
    tabs[next].click();
  });
});
document
  .querySelectorAll("[data-go]")
  .forEach((button) =>
    button.addEventListener("click", () => setTab(button.dataset.go)),
  );
let theme;
try {
  theme = localStorage.getItem("ticketdesk-theme");
} catch {}
theme = theme || "light";
function setTheme(value) {
  document.documentElement.dataset.theme = value;
  theme = value;
  $("theme-toggle").setAttribute(
    "aria-label",
    `Switch to ${value === "light" ? "dark" : "light"} theme`,
  );
  try {
    localStorage.setItem("ticketdesk-theme", value);
  } catch {}
  renderChart();
}
setTheme(theme);
$("theme-toggle").onclick = () =>
  setTheme(theme === "light" ? "dark" : "light");
function waitOptions() {
  return `?wait=${state.wait}`;
}
document.querySelectorAll(".wait-toggle").forEach((box) =>
  box.addEventListener("change", () => {
    state.wait = box.checked;
    document
      .querySelectorAll(".wait-toggle")
      .forEach((other) => (other.checked = state.wait));
    if (!state.wait)
      toast(
        "The next cached read may briefly show an older value until the commit reaches the engine.",
      );
  }),
);
// Anchor each end to its real node, rather than an unrelated fixed offset.
function drawFlow() {
  const flow = $("query-flow"),
    svg = flow.querySelector(".flow-connectors");
  if (!flow.clientWidth || !flow.clientHeight) return;
  const box = flow.getBoundingClientRect();
  const originX = box.left + flow.clientLeft,
    originY = box.top + flow.clientTop;
  const bounds = (selector) => {
    const r = flow.querySelector(selector).getBoundingClientRect();
    return {
      left: r.left - originX,
      right: r.right - originX,
      top: r.top - originY,
      bottom: r.bottom - originY,
      x: (r.left + r.right) / 2 - originX,
      y: (r.top + r.bottom) / 2 - originY,
    };
  };
  const memory = bounds(".memory-node .node-icon"),
    database = bounds(".database-node svg"),
    app = bounds(".app-node");
  const startX = database.right + 6,
    endX = memory.right + 6;
  const lane = Math.min(flow.clientWidth - 9, Math.max(startX, endX) + 26);
  const corner = Math.min(10, (lane - Math.max(startX, endX)) / 2);
  svg.setAttribute("viewBox", `0 0 ${flow.clientWidth} ${flow.clientHeight}`);
  svg
    .querySelector(".stream-path")
    .setAttribute(
      "d",
      `M ${startX} ${database.y} H ${lane - corner} Q ${lane} ${database.y} ${lane} ${database.y - corner} V ${memory.y + corner} Q ${lane} ${memory.y} ${lane - corner} ${memory.y} H ${endX}`,
    );
  const target = database.left - 7,
    bend = Math.min(10, Math.max(0, (database.y - app.bottom - 6) / 2));
  svg
    .querySelector(".fallback-path")
    .setAttribute(
      "d",
      `M ${app.x} ${app.bottom + 6} V ${database.y - bend} Q ${app.x} ${database.y} ${app.x + bend} ${database.y} H ${target}`,
    );
  const caption = flow.querySelector(".stream-caption");
  caption.style.left = `${(startX + lane) / 2}px`;
  caption.style.top = `${database.y - 24}px`;
}
if (typeof ResizeObserver !== "undefined")
  new ResizeObserver(drawFlow).observe($("query-flow"));
document.fonts?.ready.then(drawFlow);
function pulse(kind) {
  const el = $("query-flow");
  const cls = kind === "read" ? "read-pulse" : "write-pulse";
  el.classList.remove(cls);
  void el.offsetWidth;
  el.classList.add(cls);
}
function addEvent(event) {
  if (!event || state.seen.has(event.id)) return;
  state.seen.add(event.id);
  state.events.push(event);
  state.events.sort((a, b) => a.id - b.id);
  while (state.events.length > 120) {
    const old = state.events.shift();
    state.seen.delete(old.id);
  }
  if (event.kind === "read") {
    const d = event.detail || {};
    if (Number.isFinite(d.elapsed_us)) {
      state.latencies.push({
        at: Date.parse(event.at),
        served: d.served,
        us: Math.max(0.01, d.elapsed_us),
      });
      while (state.latencies.length > 1800) state.latencies.shift();
    }
    pulse("read");
  }
  if (event.kind === "write" || event.kind === "writer") pulse("write");
  renderEvents();
}
let eventRenderTimer;
function renderEvents() {
  if (eventRenderTimer) return;
  eventRenderTimer = setTimeout(() => {
    eventRenderTimer = null;
    const html =
      state.events
        .slice(-18)
        .reverse()
        .map(
          (event) =>
            `<li><span class="event-dot ${esc(event.kind)}"></span><div>${esc(event.message)}<time>${esc(date(event.at))}</time></div></li>`,
        )
        .join("") || '<li class="empty">Waiting for activity…</li>';
    $("overview-events").innerHTML = html;
    $("lab-events").innerHTML = html;
  }, 150);
}
const source = new EventSource("/api/events");
source.addEventListener("activity", (event) => {
  try {
    addEvent(JSON.parse(event.data));
  } catch {}
});
source.addEventListener("gap", () => {
  $("flow-activity").textContent =
    "Activity is sampled; counters include all reads";
});
let metricError = false;
async function pollMetrics() {
  try {
    const m = await api("/api/metrics", undefined, { timeout: 5000 });
    state.metrics = m;
    const instance = m.engine && m.engine.instance;
    if (state.instance && instance && state.instance !== instance) {
      state.events = [];
      state.seen.clear();
      state.latencies = [];
    }
    state.instance = instance || state.instance;
    const c = m.cache;
    const live = c.running && c.lag_ms !== null && c.lag_ms <= m.max_lag_ms;
    const status = $("engine-status");
    status.className = `status ${live ? "live" : "starting"}`;
    status.innerHTML = `<i></i><span>${live ? "Live" : c.restarts ? "Restarting" : "Catching up"}</span>`;
    const entries = Object.entries(m.service.routes || {}).filter(([name]) =>
      name.endsWith(":cached"),
    );
    const reads = entries.reduce((s, [, r]) => s + r.reads, 0),
      memory = entries.reduce((s, [, r]) => s + r.memory, 0);
    $("hit-rate").textContent = reads
      ? `${((memory * 100) / reads).toFixed(1)}%`
      : "—";
    $("lag").textContent =
      c.lag_ms === null
        ? "—"
        : c.lag_ms < 1
          ? "<1 ms"
          : `${c.lag_ms.toFixed(0)} ms`;
    $("footer-health").textContent =
      `${number(c.entries)} maintained queries. ${bytes(c.bytes)} of ${bytes(m.budget_bytes)}. ${number(c.restarts)} restarts.`;
    $("flow-activity").textContent =
      `${number(Object.values(m.service.routes || {}).reduce((s, r) => s + r.reads, 0))} service reads`;
    for (const event of m.service.events || []) addEvent(event);
    state.lab = m.lab;
    $("lab-mode").textContent = m.lab
      ? "Schema & fault experiments enabled"
      : "Safe mode";
    $("scenario-help").textContent = m.lab
      ? "Experiments target this demo database and its uniquely identified cache. Background writes pause during each experiment."
      : "Correctness and load checks work now. Start with --lab on a disposable demo database to enable schema and fault experiments.";
    document
      .querySelectorAll("[data-scenario]")
      .forEach((b) => (b.disabled = !m.lab || b.dataset.busy === "true"));
    updateVerification(m.verification);
    state.writer = m.writer.settings;
    renderWriter(m.writer);
    renderChart();
    metricError = false;
  } catch (error) {
    $("engine-status").className = "status error";
    $("engine-status").innerHTML = "<i></i><span>Service unavailable</span>";
    $("footer-health").textContent =
      "Cannot reach the service. Check the terminal, then refresh.";
    if (!metricError) toast(error.message, true);
    metricError = true;
  }
}
function updateVerification(v) {
  if (!v) return;
  state.verifying = v.running;
  $("verify-compared").textContent = number(v.comparisons);
  $("verify-mismatches").textContent = number(v.mismatches);
  $("verify-mismatches").className = v.mismatches
    ? "danger-text"
    : "success-text";
  $("verify-errors").textContent = number(v.errors);
  $("verify-continuous").textContent = v.running
    ? "Pause continuous checks"
    : "Start continuous checks";
  $("verify-once").disabled = v.busy;
  $("verify-note").textContent =
    `${v.running ? "Continuous checks are running. " : ""}${number(v.from_memory)} comparisons used memory; ${number(v.from_postgres)} used Postgres; ${number(v.unsupported)} were refused as unsupported. ${number(v.lagged)} settled after a re-read. ${number(v.live_disagreements)} live snapshots differed.`;
  if (v.last_error)
    $("verify-note").textContent += ` Latest error: ${v.last_error}`;
  $("mismatch-detail").hidden = !v.last_mismatch;
  if (v.last_mismatch)
    $("mismatch-json").textContent = JSON.stringify(v.last_mismatch, null, 2);
}
function renderChart() {
  const el = $("latency-chart");
  if (!el) return;
  const now = Date.now(),
    points = state.latencies
      .filter((p) => p.at >= now - 60000)
      .sort((a, b) => a.at - b.at);
  if (!points.length) {
    el.innerHTML =
      '<p class="empty">Reads will appear here as you use the desk.</p>';
    return;
  }
  const width = 600,
    height = 150,
    left = 44,
    right = 12,
    top = 10,
    bottom = 24,
    plotH = height - top - bottom;
  const max = Math.max(1000, ...points.map((p) => p.us)),
    maxLog = Math.max(3, Math.ceil(Math.log10(max))),
    minLog = -1;
  const y = (us) =>
    top +
    ((maxLog - Math.log10(Math.max(0.1, us))) / (maxLog - minLog)) * plotH;
  const x = (at) =>
    left +
    Math.max(0, Math.min(1, (at - (now - 60000)) / 60000)) *
      (width - left - right);
  let svg = `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Sampled read times on a logarithmic scale">`;
  for (let power = 0; power <= maxLog; power++) {
    const val = 10 ** power,
      yy = y(val);
    svg += `<line x1="${left}" x2="${width - right}" y1="${yy}" y2="${yy}" stroke="var(--line)" stroke-dasharray="3 4"/><text x="${left - 7}" y="${yy + 3}" text-anchor="end">${val >= 1000 ? `${val / 1000} ms` : `${val} µs`}</text>`;
  }
  for (const served of ["postgres", "cache"]) {
    const own = points.filter((p) => p.served === served);
    const color = served === "cache" ? "var(--memory)" : "var(--postgres)";
    if (own.length > 1)
      svg += `<polyline points="${own.map((p) => `${x(p.at).toFixed(1)},${y(p.us).toFixed(1)}`).join(" ")}" fill="none" stroke="${color}" stroke-width="1.5" opacity=".6"/>`;
    for (const p of own.slice(-100))
      svg += `<circle cx="${x(p.at)}" cy="${y(p.us)}" r="2" fill="${color}"/>`;
  }
  svg += `<text x="${left}" y="${height - 4}">60 seconds ago</text><text x="${width - right}" y="${height - 4}" text-anchor="end">Now</text></svg>`;
  el.innerHTML = svg;
}
function optionsHTML() {
  return state.shapes
    .map(
      (shape) =>
        `<option value="${esc(shape.name)}">${esc(names[shape.name] || shape.name)}</option>`,
    )
    .join("");
}
const ms = (value) =>
  value === null || value === undefined
    ? "—"
    : value >= 1000
      ? `${(value / 1000).toFixed(1)} s`
      : `${Number(value).toFixed(value < 10 ? 1 : 0)} ms`;
async function inspectShape(name) {
  const el = $(`inspect-${name}`);
  el.hidden = false;
  try {
    const i = await api(`/api/inspect/${encodeURIComponent(name)}`);
    if (!i.query) {
      el.innerHTML = `<p>${esc(i.state)}</p>`;
      return;
    }
    const facts = [
      ["State", i.state],
      ["Rows kept", number(i.rows)],
      ["Bytes", number(i.bytes)],
      ["TTL", i.ttl || "—"],
      ["Last read", ms(i.last_read_ms) + " ago"],
      ["Freed in", i.expires_in_ms === null ? "never by ttl" : ms(i.expires_in_ms)],
      ["Engine position", i.engine.position || "—"],
      ["Engine lag", ms(i.engine.lag_ms)],
      ["Query id", i.query],
    ];
    const callers = i.callers.length
      ? `<table class="inspect-table"><thead><tr><th>Read from</th><th>Reads</th><th>Memory</th><th>Postgres</th><th>Failed</th><th>Last</th></tr></thead><tbody>${i.callers
          .map(
            (c) =>
              `<tr><td><code>${esc(c.at)}</code></td><td>${number(c.reads)}</td><td>${number(c.memory)}</td><td>${number(c.postgres)}</td><td>${number(c.failed)}</td><td>${ms(c.last_ms)} ago</td></tr>`,
          )
          .join("")}</tbody></table>`
      : "<p>No reads recorded yet.</p>";
    const recent = i.recent.length
      ? `<table class="inspect-table"><thead><tr><th>Answered by</th><th>Rows</th><th>Took</th><th>Why not memory</th></tr></thead><tbody>${i.recent
          .map(
            (t) =>
              `<tr><td>${esc(t.served)}</td><td>${number(t.rows)}</td><td>${us(t.took_us)}</td><td>${esc(t.reason || "")}</td></tr>`,
          )
          .join("")}</tbody></table>`
      : "";
    el.innerHTML = `<dl class="inspect-facts">${facts
      .map(([k, v]) => `<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`)
      .join("")}</dl><h4>Places that read it</h4>${callers}${recent ? `<h4>Last reads</h4>${recent}` : ""}`;
  } catch (error) {
    el.innerHTML = `<p class="danger-text">${esc(error.message)}</p>`;
  }
}
async function loadMemory() {
  try {
    const m = await api("/api/memory");
    $("memory-summary").textContent = `${number(m.engine.queries)} queries, ${number(m.engine.bytes)} bytes${m.engine.position ? `, at ${m.engine.position}` : ""}`;
    $("memory-queries").innerHTML = m.queries.length
      ? `<table class="inspect-table"><thead><tr><th>Query</th><th>State</th><th>Rows</th><th>TTL</th><th>Last read</th></tr></thead><tbody>${m.queries
          .map(
            (q) =>
              `<tr><td><code title="${esc(q.sql)}">${esc(q.sql.length > 110 ? q.sql.slice(0, 110) + "…" : q.sql)}</code></td><td>${esc(q.state)}</td><td>${number(q.rows)}</td><td>${esc(q.ttl)}</td><td>${ms(q.last_read_ms)} ago</td></tr>`,
          )
          .join("")}</tbody></table>`
      : '<p class="empty">Nothing is kept yet. Read a shape to load it.</p>';
  } catch (error) {
    $("memory-queries").innerHTML = `<p class="danger-text">${esc(error.message)}</p>`;
  }
}
$("refresh-memory").onclick = () => loadMemory();
let loadingShapes;
function loadShapes() {
  loadingShapes ??= fetchShapes().finally(() => {
    loadingShapes = undefined;
  });
  return loadingShapes;
}
async function fetchShapes() {
  try {
    const result = await api("/api/lookups");
    state.shapes = (result.shapes || []).map((shape) => ({
      ...shape,
      sample_kind: "inspection",
    }));
    $("shape-count").textContent = result.count;
    const race = $("race-select").value;
    const load = $("load-query").value;
    $("race-select").innerHTML = optionsHTML();
    $("race-select").value = state.shapes.some((s) => s.name === race)
      ? race
      : "card";
    $("load-query").innerHTML = optionsHTML();
    $("load-query").value = state.shapes.some((s) => s.name === load)
      ? load
      : "queue_cards";
    renderShapes();
    loadMemory();
  } catch (error) {
    $("query-shapes").innerHTML =
      `<div class="error-banner">${esc(error.message)} Use Recheck queries after the service recovers.</div>`;
    toast(error.message, true);
  }
}
function reason(shape) {
  if (shape.error) return shape.error;
  if (shape.expected === "unsupported")
    return (
      shape.refusal ||
      "Streamgres does not support this query; the read returns an error."
    );
  if (shape.expected === "postgres")
    return (
      reasons[shape.name] ||
      shape.refusal ||
      "This query is delegated to Postgres."
    );
  if (shape.served === "postgres")
    return "This read is currently using safe Postgres fallback. Expand SQL or run it again for details.";
  return shape.exercises;
}
function renderShapes() {
  const search = $("query-search").value.trim().toLowerCase(),
    filter = $("query-filter").value;
  const visible = state.shapes.filter((s) => {
    const text = `${names[s.name]} ${s.exercises} ${s.sql}`.toLowerCase();
    if (search && !text.includes(search)) return false;
    if (filter === "cache" || filter === "postgres" || filter === "unsupported")
      return s.expected === filter;
    if (filter === "join") return s.exercises.toLowerCase().includes("join");
    if (filter === "enum") return s.exercises.toLowerCase().includes("enum");
    if (filter === "pagination") return /page|offset|limit/i.test(s.exercises);
    return true;
  });
  let html = "";
  const headings = {
    cache: "Expected from memory",
    postgres: "Delegated to Postgres",
    unsupported: "Not supported (an error)",
  };
  for (const expected of ["cache", "postgres", "unsupported"]) {
    const group = visible.filter((s) => s.expected === expected);
    if (!group.length) continue;
    html += `<div class="query-group-heading"><h2>${headings[expected]}</h2><span>${group.length} shapes</span></div>`;
    for (const shape of group) {
      const feature = /join/i.test(shape.exercises)
        ? "Join"
        : /enum/i.test(shape.exercises)
          ? "Enum"
          : /offset|page|limit/i.test(shape.exercises)
            ? "Pagination"
            : /array/i.test(shape.exercises)
              ? "Array"
              : "Filter";
      html += `<article class="shape" id="shape-${esc(shape.name)}"><div class="shape-main"><div class="shape-copy"><h3>${esc(names[shape.name] || shape.name)}</h3><p>${esc(reason(shape))}</p></div><div class="shape-tags"><span class="shape-feature">${feature}</span><span class="shape-verdict" id="verdict-${esc(shape.name)}">${shape.error ? '<span class="answer-badge neutral">Unavailable</span>' : badge(shape.served, shape.elapsed_us)}<small>${shape.sample_kind === "comparison" ? "Latest request" : "Initial inspection"}</small></span><button class="subtle" data-run-shape="${esc(shape.name)}">Run both</button></div></div><details><summary>SQL and cache decision</summary><p>${esc(shape.refusal || "Memory answered this read.")}</p><pre>${esc(shape.sql || "Raw SQL; inspect the query source for its statement.")}</pre><button class="subtle" data-inspect-shape="${esc(shape.name)}">Inspect</button><div class="inspect-result" id="inspect-${esc(shape.name)}" hidden></div></details><div class="shape-result" id="result-${esc(shape.name)}" hidden></div></article>`;
    }
  }
  $("query-shapes").innerHTML =
    html ||
    '<p class="empty">No query shapes match. Try another search or filter.</p>';
  document
    .querySelectorAll("[data-inspect-shape]")
    .forEach(
      (button) =>
        (button.onclick = () =>
          busy(button, () => inspectShape(button.dataset.inspectShape), "Inspecting…")),
    );
  document
    .querySelectorAll("[data-run-shape]")
    .forEach(
      (button) =>
        (button.onclick = () =>
          busy(button, () => runShape(button.dataset.runShape), "Comparing…")),
    );
}
async function runShape(name) {
  const result = await api(
    `/api/compare/${encodeURIComponent(name)}`,
  );
  const shape = state.shapes.find((shape) => shape.name === name);
  if (shape)
    Object.assign(shape, {
      served: result.cached.served,
      elapsed_us: result.cached.elapsed_us,
      refusal: result.cached.refusal,
      error: undefined,
      sample_kind: "comparison",
    });
  const verdict = $(`verdict-${name}`);
  if (verdict)
    verdict.innerHTML =
      badge(result.cached.served, result.cached.elapsed_us) +
      "<small>Latest request</small>";
  const el = $(`result-${name}`);
  el.hidden = false;
  const label = result.cached.served === "unsupported"
    ? "Streamgres refused this query as unsupported; only Postgres answered."
    : result.equal
    ? "✓ Answers match."
    : result.cached.served === "cache"
      ? "The answers still differ after the settle window."
      : "Two Postgres snapshots differed while writes continued.";
  el.innerHTML = `<p class="result-match${!result.equal ? " danger-text" : ""}">${esc(label)} ${result.settled ? `Re-read for ${result.observed_ms.toFixed(0)} ms.` : ""}</p><div class="result-pair"><div><div class="result-heading">Through Streamgres ${badge(result.cached.served, result.cached.elapsed_us)}</div><pre>${esc(JSON.stringify(result.cached.data, null, 2))}</pre></div><div><div class="result-heading">Direct to Postgres ${badge("postgres", result.postgres.elapsed_us)}</div><pre>${esc(JSON.stringify(result.postgres.data, null, 2))}</pre></div></div>`;
  if (result.cached.refusal)
    el.insertAdjacentHTML(
      "afterbegin",
      `<p class="fine-print">${esc(result.cached.refusal)}</p>`,
    );
}
async function race(manual = false) {
  if (state.racing) return;
  state.racing = true;
  const button = $("run-race");
  button.disabled = true;
  const started = performance.now();
  try {
    const data = await api(
      `/api/compare/${encodeURIComponent($("race-select").value)}`,
    );
    const c = data.cached,
      p = data.postgres,
      max = Math.max(c.elapsed_us, p.elapsed_us, 1);
    $("race-cache-time").textContent = us(c.elapsed_us);
    $("race-pg-time").textContent = us(p.elapsed_us);
    const cacheBadge = $("race-cache-badge");
    cacheBadge.classList.remove("memory", "postgres", "neutral");
    cacheBadge.classList.add(
      c.served === "cache" ? "memory" : c.served === "unsupported" ? "neutral" : "postgres",
    );
    cacheBadge.textContent =
      c.served === "cache" ? "ϟ Memory" : c.served === "unsupported" ? "⊘ Unsupported" : "▤ Postgres";
    $("cache-bar").style.width = `${Math.max(2, (c.elapsed_us / max) * 100)}%`;
    $("cache-bar").style.background =
      c.served === "cache" ? "var(--memory)" : "var(--postgres)";
    $("pg-bar").style.width = `${Math.max(2, (p.elapsed_us / max) * 100)}%`;
    $("header-time").innerHTML =
      `${esc(us(c.elapsed_us))}<small>vs ${esc(us(p.elapsed_us))} direct</small>`;
    $("race-round").textContent = new Date().toLocaleTimeString(undefined, {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
    let note = data.equal ? "✓ Answers match. " : "The snapshots differ. ";
    if (c.served === "unsupported")
      note = "Streamgres refused this query as unsupported. ";
    else if (c.served === "postgres") note += "Postgres answered both paths safely.";
    else if (p.elapsed_us > c.elapsed_us)
      note += `The memory read was ${(p.elapsed_us / Math.max(c.elapsed_us, 0.001)).toFixed(1)}× faster on this sample.`;
    else
      note +=
        "Postgres was faster on this sample. A small read can be dominated by safety checks or first-use work; Lab measures sustained traffic.";
    if (data.settled)
      note += ` Agreement was retried for ${data.observed_ms.toFixed(0)} ms.`;
    $("race-note").textContent = note;
    $("race-note").className = `comparison-note${data.equal ? " match" : ""}`;
    $("race-pool").textContent =
      `Pool wait: ${us(c.pool_wait_us)} through Streamgres, ${us(p.pool_wait_us)} direct. Browser round trip for the full comparison: ${(performance.now() - started).toFixed(0)} ms.`;
    if (c.refusal) $("race-cache-badge").title = c.refusal;
    pulse("read");
  } catch (error) {
    $("race-note").textContent = error.message;
    $("race-note").className = "comparison-note";
    if (manual) toast(error.message, true);
  } finally {
    state.racing = false;
    button.disabled = false;
  }
}
$("run-race").onclick = () => race(true);
$("race-select").onchange = () => race(true);
$("query-search").addEventListener("input", renderShapes);
$("query-filter").onchange = renderShapes;
$("refresh-queries").onclick = () =>
  busy($("refresh-queries"), loadShapes, "Rechecking…");
async function overviewTicket() {
  try {
    const answer = await api("/tickets/1");
    const t = answer.data[0];
    if (!t) {
      $("overview-ticket").textContent =
        "Ticket #1 is not present. Reseed the demo to restore it.";
      return;
    }
    $("overview-ticket").innerHTML =
      `<strong>${esc(t.title)}</strong><p><span class="ticket-status ${esc(t.status)}">${esc(t.status)}</span> ${badge(answer.served, answer.elapsed_us)}</p>`;
    $("overview-status").value = t.status;
  } catch (error) {
    $("overview-ticket").textContent = error.message;
  }
}
function writeText(result) {
  return result.visible === true
    ? `Committed to Postgres. Memory showed it ${us(result.visible_after_us)} after the commit.`
    : result.visible === false
      ? "Committed to Postgres. Memory did not show it within 5 seconds; reads fall back to Postgres meanwhile."
      : "Committed to Postgres without waiting. The next read from memory can briefly show the older value.";
}
$("quick-status").onsubmit = async (event) => {
  event.preventDefault();
  const button = event.submitter;
  await busy(
    button,
    async () => {
      const result = await api("/tickets/1/status" + waitOptions(), {
        status: $("overview-status").value,
      });
      $("quick-result").textContent = writeText(result);
      if (result.observed) {
        const t = result.observed.data[0];
        $("overview-ticket").innerHTML =
          `<strong>${esc(t?.title || "Ticket #1")}</strong><p><span class="ticket-status ${esc(t?.status)}">${esc(t?.status)}</span> ${badge(result.observed.served, result.observed.elapsed_us)}</p>`;
      }
      toast("Ticket status saved.");
    },
    "Saving…",
  );
};
async function organizations() {
  try {
    state.orgs = await api("/api/organizations");
    $("org-select").innerHTML = state.orgs
      .map((org) => `<option value="${org.id}">${esc(org.name)}</option>`)
      .join("");
    $("org-select").value = state.org;
  } catch (error) {
    $("org-select").innerHTML = '<option value="1">Organization 1</option>';
    toast(error.message, true);
  }
}
const commentDrafts = new Map();
let commentTarget = null;
function chooseTicket(id) {
  if (state.selected === id) return;
  if (state.selected) {
    const draft = $("comment-body").value;
    if (draft) commentDrafts.set(state.selected, draft);
    else commentDrafts.delete(state.selected);
    while (commentDrafts.size > 100)
      commentDrafts.delete(commentDrafts.keys().next().value);
  }
  state.selected = id;
  commentTarget = null;
  $("comment-body").value = commentDrafts.get(id) || "";
  $("comment-section").hidden = true;
  $("ticket-detail").textContent = id
    ? "Loading ticket…"
    : "Choose a ticket from the queue.";
}
let deskLoading = false,
  deskQueued = false,
  deskGeneration = 0,
  detailGeneration = 0;
async function loadDesk() {
  const generation = ++deskGeneration;
  if (deskLoading) {
    deskQueued = true;
    return;
  }
  deskLoading = true;
  try {
    const query = new URLSearchParams({
      statuses: $("status-filter").value,
      page: state.page,
    });
    const result = await api(`/orgs/${state.org}/queue?${query}`);
    if (generation !== deskGeneration) return;
    $("queue-badge").innerHTML = badge(result.served, result.elapsed_us);
    $("page-label").textContent = `Page ${state.page}. ${result.rows} tickets.`;
    $("previous-page").disabled = state.page === 1;
    $("next-page").disabled = result.rows < 10;
    const rows = result.data || [];
    $("ticket-list").innerHTML =
      rows
        .map(
          (t) =>
            `<button class="ticket-row ${t.id === state.selected ? "selected" : ""}" data-ticket="${t.id}"><span class="row-top"><span>#${t.id}</span><span class="priority ${esc(t.priority)}">${t.priority === "high" ? "↑ High priority" : esc(t.priority)}</span></span><strong>${esc(t.title)}</strong><span class="row-bottom"><span class="ticket-status ${esc(t.status)}">${esc(t.status)}</span><span>${t.agent_id ? "Agent #" + t.agent_id : "Unassigned"}</span></span></button>`,
        )
        .join("") ||
      '<p class="empty">No tickets on this page. Try another status or go back a page.</p>';
    document
      .querySelectorAll("[data-ticket]")
      .forEach(
        (button) =>
          (button.onclick = () => selectTicket(Number(button.dataset.ticket))),
      );
    if (!state.selected && rows.length) await selectTicket(rows[0].id);
    else if (state.selected && state.tab === "desk")
      await loadDetail(state.selected);
  } catch (error) {
    if (generation !== deskGeneration) return;
    $("ticket-list").innerHTML =
      `<div class="error-banner">${esc(error.message)}</div>`;
  } finally {
    deskLoading = false;
    if (deskQueued) {
      deskQueued = false;
      loadDesk();
    }
  }
}
async function selectTicket(id) {
  chooseTicket(id);
  document
    .querySelectorAll("[data-ticket]")
    .forEach((button) =>
      button.classList.toggle("selected", Number(button.dataset.ticket) === id),
    );
  await loadDetail(id);
  if (innerWidth < 590)
    $("ticket-detail").scrollIntoView({
      behavior: matchMedia("(prefers-reduced-motion:reduce)").matches
        ? "instant"
        : "smooth",
      block: "start",
    });
}
async function loadDetail(id) {
  const generation = ++detailGeneration;
  try {
    const [answer, comments] = await Promise.all([
      api(`/tickets/${id}`),
      api(`/tickets/${id}/comments`),
    ]);
    if (state.selected !== id || generation !== detailGeneration) return;
    const t = answer.data[0];
    if (!t) {
      $("ticket-detail").innerHTML =
        '<p class="empty">This ticket no longer exists. Choose another ticket.</p>';
      $("comment-section").hidden = true;
      commentTarget = null;
      return;
    }
    $("ticket-detail").innerHTML =
      `<div class="detail-top"><span>Ticket #${t.id}</span>${badge(answer.served, answer.elapsed_us)}</div><h2 class="detail-title">${esc(t.title)}</h2><div class="detail-facts"><span>Priority: <strong>${esc(t.priority)}</strong></span><span>${t.agent_id ? "Agent #" + t.agent_id : "Unassigned"}</span><span>Created ${esc(date(t.created_at))}</span></div><form class="detail-status-form" id="detail-status-form"><label>Status <select id="detail-status"><option value="open">Open</option><option value="pending">Pending</option><option value="closed">Closed</option></select></label><button class="primary" type="submit">Save status</button></form>`;
    $("detail-status").value = t.status;
    $("detail-status-form").onsubmit = (event) => {
      event.preventDefault();
      busy(
        event.submitter,
        async () => {
          const result = await api(`/tickets/${id}/status` + waitOptions(), {
            status: $("detail-status").value,
          });
          toast(writeText(result));
          if (state.selected === id) await loadDesk();
        },
        "Saving…",
      );
    };
    commentTarget = id;
    $("comment-section").hidden = false;
    $("comments-badge").innerHTML = badge(comments.served, comments.elapsed_us);
    $("comments-list").innerHTML =
      comments.data
        .map(
          (c) =>
            `<li class="comment"><div class="author">${esc(c.agent || "Unassigned author")}</div><p>${esc(c.body)}</p></li>`,
        )
        .join("") ||
      '<li class="empty">No comments yet. Start the conversation below.</li>';
  } catch (error) {
    if (state.selected !== id || generation !== detailGeneration) return;
    commentTarget = null;
    $("comment-section").hidden = true;
    $("ticket-detail").innerHTML =
      `<div class="error-banner">${esc(error.message)}</div>`;
  }
}
$("org-select").onchange = () => {
  state.org = Number($("org-select").value);
  state.page = 1;
  chooseTicket(null);
  loadDesk();
};
$("status-filter").onchange = () => {
  state.page = 1;
  chooseTicket(null);
  loadDesk();
};
$("previous-page").onclick = () => {
  state.page = Math.max(1, state.page - 1);
  loadDesk();
};
$("next-page").onclick = () => {
  state.page++;
  loadDesk();
};
$("refresh-desk").onclick = () => loadDesk();
$("new-ticket-button").onclick = () => {
  $("create-org-label").textContent =
    `Creates the ticket in ${state.orgs.find((o) => o.id === state.org)?.name || "the selected organization"}.`;
  $("create-dialog").showModal();
};
document
  .querySelectorAll("[data-close]")
  .forEach(
    (button) => (button.onclick = () => $(button.dataset.close).close()),
  );
$("create-form").onsubmit = (event) => {
  event.preventDefault();
  busy(
    event.submitter,
    async () => {
      const result = await api("/tickets" + waitOptions(), {
        org_id: state.org,
        title: $("new-title").value,
        priority: $("new-priority").value,
      });
      $("create-dialog").close();
      $("new-title").value = "";
      chooseTicket(result.id);
      state.page = 1;
      await loadDesk();
      toast(`Ticket #${result.id} created. ${writeText(result)}`);
    },
    "Creating…",
  );
};
$("comment-form").onsubmit = (event) => {
  event.preventDefault();
  const id = commentTarget;
  if (!id || id !== state.selected) return;
  const body = $("comment-body").value;
  busy(
    event.submitter,
    async () => {
      const result = await api(`/tickets/${id}/comments` + waitOptions(), {
        body,
      });
      if (state.selected === id) {
        if ($("comment-body").value === body) $("comment-body").value = "";
        await loadDetail(id);
      } else if (commentDrafts.get(id) === body) commentDrafts.delete(id);
      toast(`Comment added. ${writeText(result)}`);
    },
    "Adding…",
  );
};
$("verify-once").onclick = () =>
  busy(
    $("verify-once"),
    async () => {
      const v = await api(
        "/api/verify",
        {},
      );
      updateVerification(v);
      toast(`Verification pass finished. ${v.mismatches} cache mismatches.`);
    },
    "Verifying…",
  );
$("verify-continuous").onclick = () =>
  busy(
    $("verify-continuous"),
    async () => {
      const v = await api("/api/verify/control", {
        running: !state.verifying,
      });
      updateVerification(v);
    },
    "Updating…",
  );
for (const [input, out] of [
  ["load-workers", "worker-value"],
  ["load-seconds", "seconds-value"],
])
  $(input).oninput = () => ($(out).textContent = $(input).value);
$("load-form").onsubmit = (event) => {
  event.preventDefault();
  busy(
    event.submitter,
    async () => {
      $("load-result").innerHTML =
        '<p class="fine-print">Running both routes. The selected duration applies to each route in sequence.</p>';
      const result = await api("/api/load", {
        name: $("load-query").value,
        concurrency: Number($("load-workers").value),
        seconds: Number($("load-seconds").value),
      });
      $("load-result").innerHTML =
        `<table><thead><tr><th>Route</th><th>Reads/s</th><th>p50</th><th>p99</th><th>Errors</th></tr></thead><tbody>${[result.cached, result.postgres].map((row) => `<tr><td>${row.route === "cached" ? "Streamgres path" : "Postgres"}</td><td>${number(Math.round(row.requests_per_second))}</td><td>${us(row.p50_us)}</td><td>${us(row.p99_us)}</td><td>${number(row.errors)}</td></tr>`).join("")}</tbody></table><p class="fine-print">${esc(result.measurement)}. ${number(result.cached.memory)} of ${number(result.cached.requests)} Streamgres-path reads used memory.</p>`;
    },
    "Running load test…",
  );
};
let writerRendered = false;
function renderWriter(writer) {
  if (!writer?.settings) return;
  state.writer = writer.settings;
  const w = writer.settings;
  $("writer-state").className =
    `status ${w.enabled && w.rate ? "live" : "starting"}`;
  $("writer-state").innerHTML =
    `<i></i>${w.enabled && w.rate ? "Writing" : "Paused"}`;
  $("writer-toggle").textContent = w.enabled ? "Pause writer" : "Resume writer";
  if (!writerRendered) {
    $("writer-rate").value = w.rate;
    $("writer-operations").innerHTML =
      "<legend>Operation mix</legend>" +
      Object.keys(opNames)
        .map(
          (op) =>
            `<label title="${esc(op)}"><input type="checkbox" value="${op}" ${w.operations.includes(op) ? "checked" : ""}>${esc(opNames[op])}</label>`,
        )
        .join("");
    writerRendered = true;
  }
  const counts = writer.counts || {};
  $("writer-counts").textContent = Object.entries(counts)
    .map(([name, count]) => `${opNames[name] || name}: ${number(count)}`)
    .join("  /  ");
}
async function loadWriter() {
  try {
    renderWriter(await api("/api/writer"));
  } catch (error) {
    toast(error.message, true);
  }
}
function chosenWriter(enabled) {
  return {
    enabled,
    rate: Number($("writer-rate").value),
    operations: [
      ...$("writer-operations").querySelectorAll("input:checked"),
    ].map((input) => input.value),
  };
}
$("writer-toggle").onclick = () =>
  busy(
    $("writer-toggle"),
    async () => {
      const result = await api(
        "/api/writer",
        chosenWriter(!state.writer?.enabled),
      );
      state.writer = result.settings;
      await loadWriter();
    },
    "Updating…",
  );
$("writer-apply").onclick = () =>
  busy(
    $("writer-apply"),
    async () => {
      const result = await api(
        "/api/writer",
        chosenWriter(state.writer?.enabled ?? true),
      );
      state.writer = result.settings;
      toast("Writer settings updated.");
      await loadWriter();
    },
    "Applying…",
  );
$("scenario-grid").innerHTML = scenarios
  .map(
    ([id, title, description]) =>
      `<article class="scenario"><h3>${esc(title)}</h3><p>${esc(description)}</p><button class="subtle" data-scenario="${id}" disabled>Run experiment</button></article>`,
  )
  .join("");
document.querySelectorAll("[data-scenario]").forEach(
  (button) =>
    (button.onclick = async () => {
      const name = button.dataset.scenario;
      button.dataset.busy = "true";
      await busy(
        button,
        async () => {
          $("scenario-result").hidden = false;
          $("scenario-result-title").textContent = scenarios.find(
            (s) => s[0] === name,
          )[1];
          $("scenario-result-status").textContent = "Running";
          $("scenario-timeline").innerHTML =
            "<li><div>Experiment requested. Live activity below will show progress.</div></li>";
          try {
            const result = await api(
              `/api/lab/${encodeURIComponent(name)}`,
              {},
            );
            $("scenario-result-status").textContent =
              `Finished in ${(result.elapsed_ms / 1000).toFixed(2)} s`;
            $("scenario-timeline").innerHTML = result.timeline
              .map(
                (phase) =>
                  `<li><div><strong>${phase.elapsed_ms.toFixed(0)} ms after starting</strong>${esc(phase.message)}${Object.keys(phase.detail || {}).length ? `<details><summary>Observed details</summary><pre>${esc(JSON.stringify(phase.detail, null, 2))}</pre></details>` : ""}</div></li>`,
              )
              .join("");
            await loadShapes();
            await overviewTicket();
          } catch (error) {
            $("scenario-result-status").textContent = "Could not finish";
            $("scenario-timeline").insertAdjacentHTML(
              "beforeend",
              `<li><div class="danger-text">${esc(error.message)} Check activity for the last observed step.</div></li>`,
            );
            throw error;
          }
        },
        "Running…",
      );
      button.dataset.busy = "false";
    }),
);
$("help-button").onclick = () => $("help-dialog").showModal();
const tourSteps = [
  [
    "engine-status",
    "Is it ready?",
    "The status tells you whether the engine is live, catching up, or restarting. Reads can use Postgres while it recovers.",
  ],
  [
    "race-panel",
    "Try the same read twice",
    "Run a real query through Streamgres and directly on Postgres. The numbers show time inside the service, not an invented benchmark.",
  ],
  [
    "race-cache-badge",
    "Every answer has a source",
    "Green means memory. Blue means Postgres. A safe fallback is an expected behavior, not a failed request.",
  ],
  [
    "tab-lab",
    "Go beyond the happy path",
    "In Lab you can check correctness, generate load, and try guarded recovery experiments.",
  ],
];
let tourIndex = 0,
  tourTarget = null;
function finishTour() {
  if (tourTarget) tourTarget.classList.remove("tour-target");
  $("tour").hidden = true;
  $("tour-shade").hidden = true;
  try {
    localStorage.setItem("ticketdesk-tour", "done");
  } catch {}
  $("help-button").focus();
}
function showTour(index) {
  if (index >= tourSteps.length) {
    finishTour();
    return;
  }
  tourIndex = index;
  if (tourTarget) tourTarget.classList.remove("tour-target");
  const [id, title, copy] = tourSteps[index];
  tourTarget = $(id);
  if (index === 1 || index === 2) setTab("overview");
  tourTarget.scrollIntoView({ block: "center", behavior: "instant" });
  tourTarget.classList.add("tour-target");
  $("tour-title").textContent = title;
  $("tour-copy").textContent = copy;
  $("tour-step").textContent = `${index + 1} of ${tourSteps.length}`;
  $("tour-next").textContent =
    index === tourSteps.length - 1 ? "Explore the desk" : "Next";
  $("tour").hidden = false;
  $("tour-shade").hidden = false;
  requestAnimationFrame(() => {
    const target = tourTarget.getBoundingClientRect(),
      box = $("tour");
    const left = Math.max(
      12,
      Math.min(innerWidth - box.offsetWidth - 12, target.left),
    );
    let top = target.bottom + 18;
    if (top + box.offsetHeight > innerHeight - 12)
      top = Math.max(12, target.top - box.offsetHeight - 18);
    box.style.left = `${left}px`;
    box.style.top = `${top}px`;
    $("tour-next").focus();
  });
}
$("tour-next").onclick = () => showTour(tourIndex + 1);
$("tour-skip").onclick = finishTour;
$("start-tour").onclick = () => {
  $("help-dialog").close();
  setTab("overview");
  showTour(0);
};
$("tour").addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    finishTour();
    return;
  }
  if (event.key === "Tab") {
    const a = $("tour-skip"),
      b = $("tour-next");
    if (event.shiftKey && document.activeElement === a) {
      event.preventDefault();
      b.focus();
    } else if (!event.shiftKey && document.activeElement === b) {
      event.preventDefault();
      a.focus();
    }
  }
});
async function init() {
  setTab(location.hash.slice(1) || "overview");
  await Promise.allSettled([
    organizations(),
    overviewTicket(),
    loadWriter(),
    pollMetrics(),
  ]);
  if (!state.shapes.length) loadShapes();
  await race(false);
  let toured;
  try {
    toured = localStorage.getItem("ticketdesk-tour");
  } catch {}
  if (!toured && state.tab === "overview") setTimeout(() => showTour(0), 600);
}
init();
setInterval(() => {
  if (!document.hidden) pollMetrics();
}, 1000);
setInterval(() => {
  if (document.hidden) return;
  if (state.tab === "overview") race(false);
  if (
    state.tab === "desk" &&
    !document.activeElement?.closest(
      "#desk input, #desk select, #desk textarea, #detail-status-form",
    )
  )
    loadDesk();
}, 5000);
window.addEventListener("resize", () => {
  drawFlow();
  renderChart();
  if (!$("tour").hidden) showTour(tourIndex);
});
