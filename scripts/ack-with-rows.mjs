// End-to-end check that a mutation's lastMutationID reaches the client in the same poke as the rows it wrote.
// It needs a running streamgres whose query and mutate endpoints point at this script's app server.
// The query joins members to their conversation, so every new conversation is fetched by a narrowed storage read.
//
//   ACK_PG=postgresql://postgres@127.0.0.1:5432/db node scripts/ack-with-rows.mjs
//
// ACK_PG is the psql connection string, ACK_PSQL the psql binary, ACK_GATEWAY the sync server, ACK_APP_PORT this script's port.

import {randomUUID} from 'node:crypto';
import {createServer} from 'node:http';
import {spawnSync} from 'node:child_process';

const pg = process.env.ACK_PG;
if (!pg) throw new Error('ACK_PG is required');
const psql = process.env.ACK_PSQL ?? 'psql';
const gateway = process.env.ACK_GATEWAY ?? 'ws://127.0.0.1:4848/sync';
const port = Number(process.env.ACK_APP_PORT ?? 4782);
const shardSchema = `${process.env.STREAMGRES_APP_ID ?? 'zero'}_${process.env.STREAMGRES_SHARD ?? '0'}`;
const rounds = Number(process.env.ACK_ROUNDS ?? 20);

const run = `r${randomUUID().slice(0, 8)}`;
const user = `user-${run}`;
const group = `group-${run}`;
const client = `client-${run}`;
const hash = `convs-${run}`;
const q = value => `'${String(value).replaceAll("'", "''")}'`;

function sql(statement) {
  const child = spawnSync(psql, [pg, '-v', 'ON_ERROR_STOP=1', '-qAtc', statement]);
  if (child.status !== 0) throw new Error(child.stderr.toString());
}

function cond(column, value) {
  return {type: 'simple', op: '=', left: {type: 'column', name: column}, right: {type: 'literal', value}};
}

const ast = {
  table: 'ack_members',
  where: cond('user_id', user),
  related: [{correlation: {parentField: ['conv_id'], childField: ['id']}, subquery: {table: 'ack_convs', alias: 'conv'}}],
  orderBy: [['id', 'asc']],
};

async function body(request) {
  let text = '';
  for await (const chunk of request) text += chunk;
  return JSON.parse(text);
}

const app = createServer(async (request, response) => {
  const message = await body(request);
  response.setHeader('content-type', 'application/json');
  if (request.url.startsWith('/query')) {
    const [, queries] = message;
    response.end(JSON.stringify(['transformed', queries.map(({id, name}) => ({id, name, ast}))]));
    return;
  }
  const results = [];
  for (const mutation of message.mutations) {
    const {conv, member} = mutation.args[0];
    // The conversation is written before the member, as an application naturally would.
    sql(`BEGIN;
         INSERT INTO ack_convs (id, title) VALUES (${q(conv)}, 'new');
         INSERT INTO ack_members (id, conv_id, user_id) VALUES (${q(member)}, ${q(conv)}, ${q(user)});
         INSERT INTO ${shardSchema}.clients ("clientGroupID", "clientID", "lastMutationID") VALUES (${q(group)}, ${q(client)}, ${mutation.id})
           ON CONFLICT ("clientGroupID", "clientID") DO UPDATE SET "lastMutationID" = EXCLUDED."lastMutationID";
         COMMIT;`);
    results.push({id: {clientID: client, id: mutation.id}, result: {}});
  }
  response.end(JSON.stringify({kind: 'MutateResponse', mutations: results}));
});
await new Promise(resolve => app.listen(port, '127.0.0.1', resolve));

const init = ['initConnection', {desiredQueriesPatch: [{op: 'put', hash, name: 'myConvs', args: [user]}]}];
const header = encodeURIComponent(Buffer.from(JSON.stringify({initConnectionMessage: init})).toString('base64'));
const url = `${gateway}/sync/v51/connect?clientID=${client}&clientGroupID=${group}&userID=${user}&baseCookie=&ts=1&lmid=0&wsid=${run}`;
const ws = new WebSocket(url, [header]);

// Each finished poke, with the client's lastMutationID it carried and the conversation ids it put.
const pokes = [];
let open = null;
let hydrated = false;
const waiters = [];

function settle() {
  for (const waiter of [...waiters]) {
    if (waiter.done()) {
      waiters.splice(waiters.indexOf(waiter), 1);
      waiter.resolve();
    }
  }
}

function until(done, what) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for ${what}`)), 10_000);
    waiters.push({done, resolve: () => (clearTimeout(timer), resolve())});
    settle();
  });
}

ws.addEventListener('message', event => {
  const [tag, msg] = JSON.parse(event.data);
  if (tag === 'error') throw new Error(`server error: ${JSON.stringify(msg)}`);
  if (tag === 'pokeStart') open = {at: performance.now(), lmid: null, convs: []};
  if (tag === 'pokePart' && open) {
    if (msg.lastMutationIDChanges?.[client] !== undefined) open.lmid = msg.lastMutationIDChanges[client];
    for (const op of msg.rowsPatch ?? []) {
      if (op.op === 'put' && op.tableName === 'ack_convs') open.convs.push(op.value.id);
    }
    if (msg.gotQueriesPatch?.some(op => op.op === 'put' && op.hash === hash)) hydrated = true;
  }
  if (tag === 'pokeEnd' && open) {
    pokes.push(open);
    open = null;
  }
  settle();
});

await new Promise((resolve, reject) => {
  ws.addEventListener('open', resolve);
  ws.addEventListener('error', reject);
});
await until(() => hydrated, 'the query to hydrate');

const lmidPoke = id => pokes.findIndex(poke => poke.lmid !== null && poke.lmid >= id);
const convPoke = conv => pokes.findIndex(poke => poke.convs.includes(conv));

let split = 0;
const acks = [];
const rows = [];
for (let id = 1; id <= rounds; id++) {
  const conv = `conv-${run}-${id}`;
  const sent = performance.now();
  const now = Date.now();
  ws.send(JSON.stringify(['push', {
    clientGroupID: group,
    mutations: [{type: 'custom', id, clientID: client, name: 'convs.create', args: [{conv, member: `member-${run}-${id}`}], timestamp: now}],
    pushVersion: 1,
    timestamp: now,
    requestID: `${run}-${id}`,
  }]));
  await until(() => lmidPoke(id) >= 0 && convPoke(conv) >= 0, `mutation ${id} and its conversation`);
  const ackAt = lmidPoke(id);
  const rowAt = convPoke(conv);
  acks.push(pokes[ackAt].at - sent);
  rows.push(pokes[rowAt].at - sent);
  if (ackAt < rowAt) {
    split++;
    console.log(`mutation ${id}: acknowledged in poke ${ackAt}, its conversation arrived later in poke ${rowAt}`);
  }
}

ws.close();
app.close();
function summary(label, values) {
  values.sort((a, b) => a - b);
  const pct = p => values[Math.min(values.length - 1, Math.floor(p * values.length))].toFixed(1);
  console.log(`${label} ms over ${rounds} mutations: p50 ${pct(0.5)} p90 ${pct(0.9)} max ${values.at(-1).toFixed(1)}`);
}
summary('ack latency', acks);
summary('rows latency', rows);
if (split > 0) {
  console.log(`FAIL: ${split}/${rounds} mutations were acknowledged before their rows`);
  process.exit(1);
}
console.log(`PASS: all ${rounds} mutations were acknowledged together with their rows`);
