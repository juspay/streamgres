#!/usr/bin/env node
// A self-contained check of a running sync server, with nothing of the application
// behind it: a PostgreSQL with `wal_level = logical`, the server, and this script as a
// Zero client speaking the sync protocol with a query AST of its own. It creates one
// table, subscribes, and checks that the rows hydrate, that an insert, an update out of
// the filter and a delete made straight in PostgreSQL arrive as pokes, that a client
// whose schema the server cannot serve is refused, that a read of more than half the
// row limit is reported by query name, and, when SMOKE_COLLECTOR is given, that the
// collector has received the server's metrics over OTLP. Exits 0 on PASS.
//
//   node scripts/smoke.mjs            (Node 22 or later: the built-in WebSocket)
//
// Environment: SMOKE_PG (postgresql://postgres:postgres@localhost:5432/postgres),
// SMOKE_GATEWAY (ws://localhost:4848/sync), SMOKE_HTTP (http://localhost:4848),
// SMOKE_COLLECTOR (the collector's Prometheus endpoint, http://localhost:9464/metrics;
// unset skips that check). The server is expected to run with
// XYNE_SYNC_READ_ROW_LIMIT=600, so the 400 seeded rows make a heavy read. The table must
// exist before the server starts (it reads the catalog once): run with `--prepare` first.

import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';

const PG = process.env.SMOKE_PG ?? 'postgresql://postgres:postgres@localhost:5432/postgres';
const GATEWAY = process.env.SMOKE_GATEWAY ?? 'ws://localhost:4848/sync';
const HTTP = process.env.SMOKE_HTTP ?? 'http://localhost:4848';
const COLLECTOR = process.env.SMOKE_COLLECTOR;
const SEEDED = 400;
const t0 = Date.now();
const log = (...a) => console.log(`${String(Date.now() - t0).padStart(6)}ms`, ...a);
const fail = (why) => { console.error('FAIL:', why); process.exit(1); };
const sql = (text) => execFileSync('psql', [PG, '-v', 'ON_ERROR_STOP=1', '-Atc', text]).toString().trim();

if (process.argv.includes('--prepare')) {
  sql(`DROP TABLE IF EXISTS smoke_items;
       CREATE TABLE smoke_items (id text PRIMARY KEY, status text NOT NULL, points integer NOT NULL, meta jsonb, "createdAt" timestamptz NOT NULL DEFAULT now());
       INSERT INTO smoke_items (id, status, points, meta) SELECT 'seed-' || n, 'OPEN', n, jsonb_build_object('n', n) FROM generate_series(1, ${SEEDED}) n;
       INSERT INTO smoke_items (id, status, points) VALUES ('closed-1', 'DONE', 0);`);
  log(`prepared smoke_items with ${SEEDED} open rows and one closed`);
  process.exit(0);
}

const open = { table: 'smoke_items', where: { type: 'simple', op: '=', left: { type: 'column', name: 'status' }, right: { type: 'literal', value: 'OPEN' } }, orderBy: [['id', 'asc']] };

/// One connection with its own view of the rows it was sent.
function connect(name, { clientSchema, patch }) {
  const state = { name, rows: new Map(), got: new Set(), pokes: 0, errors: [], closed: false, waiters: [] };
  const init = ['initConnection', { desiredQueriesPatch: patch, ...(clientSchema ? { clientSchema } : {}) }];
  const sec = encodeURIComponent(Buffer.from(JSON.stringify({ initConnectionMessage: init })).toString('base64'));
  const url = `${GATEWAY}/sync/v51/connect?clientID=c-${name}&clientGroupID=g-${name}-${randomUUID().slice(0, 8)}&userID=smoke&baseCookie=&ts=1&lmid=0&wsid=${name}`;
  const ws = new WebSocket(url, [sec]);
  state.ws = ws;
  const wake = () => { state.waiters = state.waiters.filter((w) => !w()); };
  ws.addEventListener('message', (event) => {
    const [tag, body] = JSON.parse(event.data);
    if (tag === 'pokePart') {
      for (const op of body.rowsPatch ?? []) {
        if (op.op === 'put') state.rows.set(op.value.id, op.value);
        if (op.op === 'del') state.rows.delete(op.id.id);
      }
      for (const op of body.gotQueriesPatch ?? []) { if (op.op === 'put') state.got.add(op.hash); }
    } else if (tag === 'pokeEnd') { state.pokes += 1; } else if (tag === 'error') { state.errors.push(body); }
    wake();
  });
  ws.addEventListener('close', () => { state.closed = true; wake(); });
  ws.addEventListener('error', () => wake());
  state.until = (what, test, ms = 15000) => new Promise((resolve) => {
    const timer = setTimeout(() => fail(`${name}: timed out waiting for ${what}`), ms);
    const check = () => { if (!test(state)) return false; clearTimeout(timer); resolve(state); return true; };
    if (!check()) state.waiters.push(check);
  });
  return state;
}

const a = connect('A', { patch: [{ op: 'put', hash: 'open', ast: open, ttl: 300000 }] });
await a.until('hydration', (s) => s.got.has('open'));
if (a.errors.length) fail('A was refused: ' + JSON.stringify(a.errors[0]));
if (a.rows.size !== SEEDED) fail(`A hydrated ${a.rows.size} rows, expected ${SEEDED}`);
const seed = a.rows.get('seed-7');
if (seed?.points !== 7 || seed?.meta?.n !== 7 || typeof seed?.createdAt !== 'number') fail('a row did not arrive whole and typed: ' + JSON.stringify(seed));
log(`A hydrated ${a.rows.size} rows, whole and typed (json embedded, time as milliseconds)`);

sql(`INSERT INTO smoke_items (id, status, points) VALUES ('live-1', 'OPEN', 1), ('live-closed', 'DONE', 2)`);
await a.until('the inserted row', (s) => s.rows.has('live-1'));
if (a.rows.has('live-closed')) fail('a row outside the filter was delivered');
sql(`UPDATE smoke_items SET status = 'DONE' WHERE id = 'seed-1'`);
await a.until('the row that left the filter', (s) => !s.rows.has('seed-1'));
sql(`UPDATE smoke_items SET points = 70 WHERE id = 'seed-7'`);
await a.until('the updated row', (s) => s.rows.get('seed-7')?.points === 70);
sql(`DELETE FROM smoke_items WHERE id = 'live-1'`);
await a.until('the deleted row', (s) => !s.rows.has('live-1'));
log('an insert, an update out of the filter, an update in place and a delete all arrived by poke');

const fits = { tables: { smoke_items: { columns: { id: { type: 'string' }, points: { type: 'number' }, meta: { type: 'json' } }, primaryKey: ['id'] } } };
const b = connect('B', { clientSchema: fits, patch: [{ op: 'put', hash: 'open', ast: open, ttl: 300000 }] });
await b.until('hydration', (s) => s.got.has('open') || s.errors.length > 0);
if (b.errors.length) fail('a fitting client schema was refused: ' + JSON.stringify(b.errors[0]));
const ahead = { tables: { smoke_items: { columns: { id: { type: 'string' }, noSuchColumn: { type: 'string' } }, primaryKey: ['id'] } } };
const c = connect('C', { clientSchema: ahead, patch: [{ op: 'put', hash: 'open', ast: open, ttl: 300000 }] });
await c.until('the refusal', (s) => s.closed);
if (c.errors[0]?.kind !== 'SchemaVersionNotSupported' || c.pokes > 0) fail('a client ahead of the database was not refused as the reference server refuses it: ' + JSON.stringify(c.errors));
log('a fitting client schema was served and one naming an unknown column was refused with SchemaVersionNotSupported');

const stats = await (await fetch(`${HTTP}/stats`)).json();
const limit = stats.gauges.read_row_limit;
const heavy = stats.heavy_queries?.[0];
if (limit !== 600) fail(`the server should run with XYNE_SYNC_READ_ROW_LIMIT=600, it reports ${limit}`);
if (!heavy || heavy.table !== 'smoke_items' || heavy.rows < SEEDED - 1 || heavy.percent_of_limit < 60) fail('the heavy read was not reported: ' + JSON.stringify(stats.heavy_queries));
const metrics = await (await fetch(`${HTTP}/metrics`)).text();
for (const needle of ['xyne_sync_read_row_limit 600', 'xyne_sync_reads_near_limit_total{over="50"}', 'xyne_sync_query_read_rows_max{', 'xyne_sync_connections_total{event="refused",reason="client_schema"} 1', 'xyne_sync_end_to_end_seconds_bucket']) {
  if (!metrics.includes(needle)) fail(`/metrics lacks ${needle}`);
}
log(`the read of ${heavy.rows} rows (${heavy.percent_of_limit}% of the limit of ${limit}) is reported under its query, and /metrics carries it`);

if (COLLECTOR) {
  const deadline = Date.now() + 30000;
  let seen = '';
  while (Date.now() < deadline) {
    seen = await (await fetch(COLLECTOR)).text().catch(() => '');
    if (/^xyne_sync_read_row_limit\{[^}]*\} 600$/m.test(seen) && /^xyne_sync_connections_total\{[^}]*event="refused"[^}]*\} 1$/m.test(seen) && seen.includes('xyne_sync_end_to_end_seconds_bucket')) break;
    await new Promise((r) => setTimeout(r, 1000));
  }
  if (Date.now() >= deadline) fail('the collector did not receive the metrics over OTLP:\n' + seen.split('\n').filter((l) => l.startsWith('xyne_sync_read')).join('\n'));
  const names = (text) => new Set(text.split('\n').filter((l) => l && !l.startsWith('#')).map((l) => l.replace(/[{ ].*/, '')));
  const pushed = names(seen);
  const missing = [...names(metrics)].filter((n) => !pushed.has(n));
  if (missing.length) fail('series served at /metrics and not at the collector: ' + missing.join(', '));
  log(`the collector holds every series /metrics serves (${pushed.size} names), pushed over OTLP`);
}

a.ws.close(); b.ws.close();
log('PASS');
setTimeout(() => process.exit(0), 200);
