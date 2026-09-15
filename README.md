# Xyne-Sync

**A subscription-native sync engine.** Clients subscribe to SQL queries over a
WebSocket and keep receiving the *delta*, the rows that entered, left, or
changed in their result set, instead of polling or re-running the query.
Every subscription is an incrementally maintained materialized view; every
committed change in PostgreSQL is routed to exactly the views it affects, at a
cost that depends on how relevant the write is, never on how many
subscriptions exist.

The design and the algorithms are written up as a paper:
[`paper/xyne-sync.pdf`](paper/xyne-sync.pdf) (source in
[`paper/xyne-sync.tex`](paper/xyne-sync.tex)). This README is the working
summary: what the system is, what is built, what is next, and how to run it.

---

## The idea in one picture

```text
clients                       Xyne-Sync engine                       PostgreSQL
  │                              │                                        │
  ├─ subscribe(client, SQL) ────>│                                        │
  │                              ├─ initial SELECT @ exported snapshot ──>│
  │<─ snapshot: [Add, Add, …] ───┤   (never ahead of the engine)          │
  │                              │<──── committed changes (one pgoutput ──┤ primary
  │                              │      slot, streamed, commit order)     │
  │                              ├─ route: which views does this touch?   │
  │                              ├─ patch: emit Add / Delete per view     │
  │<─ [Add(new) → q1, q2] ───────┤   (grouped per client, one image each) │
```

Recomputing every subscribed query on every write scales as
`writes × subscriptions × query`. Xyne-Sync makes the cost of a write
proportional to the number of **distinct predicates** on the written table and
the number that **actually match**, and then patches only the affected views.

**One position.** The whole engine sits at one WAL position: the point up to
which the change feed has delivered every commit. A snapshot read is taken at
or below that position, never ahead of it, and before its rows land the runtime
applies to them every delivered write they do not hold. So the rows a
subscription is served reflect exactly the engine's position, the same position
every later delta continues from.

---

## Status at a glance

| Area | Status | Where |
| --- | --- | --- |
| Typed query/row model, self-contained ops (`Delete` carries its image; a row that changes in place reaches a client as one `Add`); row images and identities keyed by `ColumnName` | ✅ done | `src/model/` |
| DNF counting index: canonical disjuncts, shared epoch-stamped counters fire exactly | ✅ done | `src/ivm/index.rs` |
| Per-column value index: a write reaches only the conditions it satisfies, `O(columns · log n)` lookups (equality maps, inequality by set difference, range maps per comparison class) | ✅ done | `src/ivm/columns.rs` |
| Shared frames: one frame per table, rows tagged with holders as compact ids (`RowId`, `SubId`); held mirror; twin registration served from the frame through a query-keyed index | ✅ done | `src/ivm/frames.rs`, `src/ivm/registry.rs` |
| `ORDER BY` / `LIMIT` windows: compound order, the page of `L` to the client over a buffer of `2L`, storage frontier, boundary condition in the index, eviction, refill | ✅ done | `src/ivm/window.rs` |
| In-place condition edits: a literal `IN` swapped inside its disjuncts, or a set-valued `IN` (`Value::Set`) gaining/losing one member in O(1) | ✅ done | `src/ivm/index.rs`, `src/ivm/registry.rs` |
| Join tree: `LEFT`, `RIGHT` and `INNER` edges at any depth, existence tests placed anywhere in a node's filter (`EXISTS` inside `OR`), set-valued edges shared by identical subscriptions, cascades, self-joins, intersection on shared driven columns; a child row is shown only under a shown parent | ✅ done | `src/ivm/multi.rs` |
| Client-addressed output: every subscription belongs to a `ClientId`; one step's operations are folded per client and row (`ClientUpdate { client, table, op, targets }`), so a row image travels to a client once | ✅ done | `src/ivm/update.rs` |
| SQL parser (single table, schema-aware, typed coercion, `i64` ids) | ✅ done | `src/parser/` |
| Asynchronous storage seam: the engine records the reads it needs (registration, join fetch, window refill) instead of running them; the runtime holds the one position and brings every read up to it before landing; no read ever blocks the stream; synchronous and asynchronous drivers | ✅ done | `src/ivm/engine.rs`, `src/sync/` |
| In-memory storage answering at once, honoring `ORDER BY` + `LIMIT`; per-table routing between memory and PostgreSQL (`XYNE_SYNC_MEMORY_TABLES`) | ✅ done | `src/sync/storage.rs`, `src/sync/sources.rs` |
| **PostgreSQL**: reads from the exported snapshot of a rotating temporary replication slot, flipped forward only once the feed has passed it; a streaming `pgoutput` change feed over a replication connection with heartbeat progress marks; live tests and a bench scenario against a real server | ✅ done | `src/sync/pg/` |
| Routing counters + benchmark harness | ✅ done | `src/ivm/stats.rs`, `src/bin/bench.rs` |
| **xyne-spaces coverage**: the dashboard's 283 synced queries (and the ACL predicates added to them) rebuilt as tests on a catalog generated from the application's schema; `IS NULL`, `EXISTS` inside `OR` and `whereExists` closed, four expressiveness gaps left and pinned | ✅ tests, ⏳ gaps | `tests/xyne_spaces_queries/` |
| **Sync gateway** speaking Zero's sync protocol (v51, the `@rocicorp/zero` 1.9 client): connect handshake, ping/pong and liveness, desired queries through the app server's query endpoint, pokes per client group, mutations through its mutate endpoint, `lastMutationID` off the app's clients table | ✅ done (no history across restarts, query TTLs not honored) | `src/gateway/` |
| Batching of one write's narrowed reads | ⏳ pending | paper §13 |
| Parser `JOIN` syntax | ⏳ pending | multi-table queries are built programmatically |
| Table-sharded multithreading | ⏳ pending | paper §10.3; engine is single-threaded by design |

Everything marked done is pinned by the scenario tests in `tests/` and the demo
in `src/main.rs`, and has been through three rounds of adversarial review with
mutation checks on the discriminating tests.

---

## How it works

The pipeline is `SQL text → typed query model → IVM routing → operations`.

### 1. Model (`src/model/`)

- `Value`: the dynamic cell type (`Int` is `i64`; manual `Eq`/`Hash` for
  floats and maps; `Set` is the engine's identity-compared shared set).
  `TableName` and `ColumnName` are newtypes. A subscription is addressed by
  `SubId`, a `u64` the engine hands out at registration and never reuses, and
  belongs to a `ClientId`, the transport's handle for one connection. The
  client's own subscription names live in the transport, which maps them to
  `SubId`s and back.
- `DataFrameKey` (primary-key values, identity only), `DataFrameRow` (a
  **full** row image, every column including the key), both keyed by
  `ColumnName`; `DataFrameOperation` (`Add(key, row)` / `Delete(key, row)`).
- `SingleTableReadQuery { table, filter: Where, order_by: Vec<OrderBy>, limit }`
  (the order columns compared in turn);
  `Where` is `AND`/`OR` over leaf `Condition`s (`=`, `!=`, `<`, `<=`, `>`,
  `>=`, `IN`, `NOT IN`, `IS NULL`, `IS NOT NULL`, and `EXISTS` naming one of
  the node's inner joins); no `NOT` node, so DNF is plain distribution.
  `MultiTableReadQuery { main_table, left_joins, right_joins, inner_joins }`
  is the join tree.
- Writes speak the same vocabulary: `InsertQuery`/`UpdateQuery` carry a key
  and a full image; `DeleteQuery` a key.
- `Lsn` is a WAL location; `Snapshot { rows, at }` is what a storage read
  returns: the rows and the position they reflect every commit up to.

**Operation contract.** Every op is self-contained. Ops on the same key are
applied in stream order (the `Delete` of a row a client loses precedes any
`Add` of that row in the same step; a row admitted then evicted in one step
has its `Add` before its `Delete`). Ops on different rows commute; a client
applies `Add` as insert-or-replace and `Delete` as remove and converges. A row
that changes in place is, inside the engine, a `Delete(old)` + `Add(new)` pair
(the join layer diffs it); the client receives only the `Add`.

### 2. Single-table engine (`src/ivm/`)

A write can affect a subscription two ways, and both are checked:

| new row matches | frame holds row | emitted |
| --- | --- | --- |
| yes | no | `Add` |
| yes | yes | `Delete(old)` + `Add(new)` in the engine; one `Add` to the client |
| no | yes | `Delete(old)` |
| no | no | nothing |

- **Way 1, counting** (`index.rs`, `columns.rs`). At registration a filter is
  normalized to DNF (disjuncts canonical: sorted and deduplicated, so
  condition order never splits a counter). Per table, `by_condition:
  Condition → [shared counter]`, `by_disjunct: Disjunct → counter` (identical
  shapes share one counter across subscriptions), and `columns: column →
  value index`. Routing looks each written column value up in its column's
  index, which yields exactly the conditions the value satisfies (equality
  maps with numeric folding, `IN` filed under each list value, `<>` / `NOT IN`
  by set difference, range operators by ordered maps per comparison class),
  bumps their counters, and fires a disjunct when its counter reaches its
  size. Counters are epoch-stamped per write, no reset sweep. Cost:
  `O(columns · log n) + O(matching links)`, independent of how many
  conditions or subscriptions exist.
- **Way 2, membership** (`frames.rs`). One shared `TableFrame` per table:
  key → `RowId` (a `u64` handed out when the row enters the frame, retired
  when its last holder leaves) → `SharedRow { key, data, subscribers }`.
  Deletes and updates-out are found in O(1) off the tags. `held: SubId →
  RowIds` mirrors the tags so a subscription's view is enumerable without
  scanning the table; a (subscription, row) pair costs two small integers.
- **Registration** (`registry.rs`). Hand out a `SubId` under its client,
  index the DNF; serve the initial rows from storage or, for a structurally
  identical query found through a query-keyed index in one lookup, from the
  twin's rows with no storage query (`snapshots_shared`) — even while the
  twin's own read is out: a read lands into every subscription of its
  query, so a burst of identical registrations costs one storage query.
  `replace_condition`
  edits one leaf in place (stored filter + indexed disjuncts, splitting shared
  counters correctly). `replace_query` / `replace_condition` leave
  reconciling the held rows to the caller, in the same call: a widening
  fetch (whose pending read keeps the subscription from donating a twin
  snapshot until it lands) or a narrowing prune.
- **Windows** (`window.rs`). `ORDER BY` is a list of columns compared in
  turn. A finite `LIMIT L` keeps a buffer of `2L` rows (storage queried with
  the doubled limit) and a **frontier**: the worst order key known to be
  covered from storage (every matching row better than it is held). A full
  storage read sets the frontier to its worst key, a short one clears it
  (storage exhausted), an eviction pulls it in. The admission boundary,
  strictly better than the frontier (`col < frontier` for ASC, `>` for DESC;
  over several columns the `OR` of one branch per column, each tying the
  earlier columns and strict on its own), is **published into the index's
  `boundaries` side table** and checked inside `matched()`; admission only; a
  held row that worsens keeps its slot until evicted. Past capacity the worst
  row is evicted; when a removal drains the buffer to `L`, one storage query
  refills from the frontier inclusive (held ties dedup). **The client
  receives the page**: exactly the best `L` rows at registration and, after
  every step, the difference between the page before and after (a page row
  rewritten in place arrives as one `Add`); the buffer behind the page is the
  engine's, so a row leaving the page is replaced from it without a storage
  trip. `NULL`/`NaN` sort largest; an unenforceable frontier publishes no
  boundary rather than rejecting everything; `LIMIT 0` is permanently empty.
- **Client grouping** (`update.rs`). Inside the engine every operation is
  produced per subscription. Before anything leaves, one step's operations
  are folded per (client, table, row) into at most three entries in order:
  a `Delete` for the subscriptions that lost the row, an `Add` naming every
  subscription that holds it now (a subscription that lost and regained the
  row appears only here), and a `Delete` for a row admitted and evicted in
  the same step. A client with ten subscriptions holding one row receives
  that row once, with ten targets.

### 3. Multi-table layer (`src/ivm/multi.rs`)

A multi-table query is a **tree**: every node a single-table query, every
edge a `LEFT`, `RIGHT` or `INNER` join with a column pair, and a child itself
a full multi-table query
(`MultiTableReadQuery { main_table, left_joins, right_joins, inner_joins }`,
`Join { sub, main_table_column, sub_table_column }`). Each node registers as
one inner subscription, a *part* addressed by its path of join indices
(`QueryPart`, root = `[]`, left joins numbered first, then right, then
inner); inner ids are looked up in a map, never parsed.

Every edge has a **driver** side, whose rows decide which join values are
referenced, and a **driven** side, whose part carries `driven_col IN <set>`
inside its filter: `LEFT` keeps the parent, so the parent drives; `RIGHT`
and `INNER` keep the child's evaluation, so the child drives. The operand is
a **shared set** (`Value::Set`, compared by identity) owned by the tree, so
the restriction lives inside the driven filter and driven-table writes route
natively, while a change to the set never rewrites the filter or the index's
counters. Where the leaf sits is the author's choice: an `EXISTS` leaf in the
node's own `WHERE` naming an inner join is bound in place at registration,
so `visibility = 'PUBLIC' OR EXISTS(participants WHERE userId = me)` is one
filter with the set-valued leaf inside its `OR`; an unnamed edge is conjoined
at the top, and unnamed edges driving one node on one column (a `LEFT`
parent above, a `RIGHT` child below, both on `id`) share one set holding the
**intersection** of their referenced values. When a value leaves a set, the
rows held for it are re-evaluated against the filter, so a row another branch
still admits stays.

**The gate.** A row is delivered only under a shown parent row: under a
`LEFT` or `INNER` edge a child row is shown while at least one shown parent
row carries its join value, under a `RIGHT` edge always, the root always.
Per edge and per value the layer counts the shown parent rows; a crossing
admits or retracts the child rows for that value, one `Add` or `Delete`
each, and those rows are counted on the edges below them in turn. Held rows
that are not shown still drive: an `INNER` child's rows are evaluated first
and fill the parent's set whether or not the parent row that makes them
visible has arrived. This is what makes `whereExists` an `INNER` edge: the
parent is shown only while a child matches, and the client never receives a
child without its parent.

Subscriptions that register an identical spec **share one tree** (a `TreeId`
per spec): one inner part per node, one set of edges and counts, one crossing
per event; each part's operations are emitted once per subscriber, a later
identical registration is served from the shared parts, and the tree goes
with its last subscriber.

Per edge and per value the layer keeps `left` (driver rows carrying it) and
`right` (driven rows held); only `left` zero-crossings that change a set act:
`0→1` inserts the member, files the leaf under it in the column index (O(1)),
and fetches that value's driven rows in one narrowed query; `→0` removes it
and prunes held rows with no storage trip. The rows a fetch brings in are
arrivals at the driven node and a prune's rows are departures, and the driven
node may drive further edges, so the same handling cascades through the tree.
Registration is a post-order walk (right and inner children, node, left
children); within a write every driven part is forwarded before its driver,
and a replace pair is diffed per edge so a kept value never churns through
zero. What leaves the layer is the same client-grouped `ClientUpdate`, each
target naming the subscription and the part.

### 4. Runtime and storage (`src/sync/`)

The engine never reads storage. Where it needs rows it does not hold, it
records a `Fetch` request (`src/ivm/engine.rs`) and carries on: a
registration's initial result set (`Snapshot`), a join edge's newly referenced
value (`Narrowed`), a drained window's refill (`Refill`). Until the request
lands, the subscription routes natively (its filter is indexed, the join leaf
already holds the value), a windowed subscription publishes no admission
boundary, and it donates no twin snapshot. The engine itself is
**position-free**.

`Storage` is asynchronous: `select(query)` returns a `Snapshot`, the rows of
one consistent snapshot and the position it reflects every commit up to;
`advance(feed)` tells the storage how far the feed has delivered, `floor()`
says the oldest position its next read can be at, and `absorb(write, at)`
lets a store that mirrors data apply the feed's writes. `MemoryStorage` is at
the position of the last write it applied. `Sources` routes each table's reads
to memory or PostgreSQL: the tables named in `XYNE_SYNC_MEMORY_TABLES`
(comma-separated, read by `Sources::cached_from_env`) or passed to
`Sources::new` are loaded from PostgreSQL once (`warm()`), kept in process
and fed by the same writes; everything else, by default every table, is read
from PostgreSQL.

`Runtime` is the single owner of an engine and pure state (no I/O, no clock).
It holds **the one position** of the whole engine, the point up to which the
feed has delivered every commit, moved by every write and every progress mark.
It hands the engine's requests to a driver, keeps the writes delivered while
reads are out, and lands each result the moment it returns, after **bringing
it up to the engine's position**: every delivered write on the read's table
positioned above the snapshot is applied to the result, a row it deleted or
moved out of the read's filter dropped, a row it rewrote given the newer
image. A snapshot is never ahead of the engine (the storage guarantees it, see
§5), so nothing the stream removed can be resurrected and a landed row the
frame already holds agrees with the frame's image. The buffer of delivered
writes is bounded below by the **floor**, the oldest position a storage's next
read or an outstanding read can be at, and is empty while nothing is out. A
read the storage could not answer is parked and handed out again when the feed
next moves.

Two drivers share the runtime. `Local` answers reads at once and lands every
read before the call returns (tests, demo, bench). `Service` is a tokio
command loop on one `LocalSet`: `Register { client, query }`, `Unregister`,
`UnregisterClient`, `Write { write, at }`, `Progress(at)`. Every read, a
subscription's initial snapshot as much as a join fetch or a window refill,
runs as its own task while the loop keeps routing; the loop never waits on
storage. Until a read lands the subscription routes natively (its filter is
indexed, the join leaf already holds the value, the window publishes no
boundary), and the landing is brought up to the engine's position, so the
order in which reads return does not matter. After each write and progress
mark the service tells every storage how far the feed is and takes the new
floor.

### 5. PostgreSQL (`src/sync/pg/`)

- `PgStorage::connect(dsn, catalog)` runs each read in its own read-only
  `REPEATABLE READ` transaction positioned by an **alias**: a temporary
  logical replication slot created with `EXPORT_SNAPSHOT` over a
  replication-protocol connection (`replication.rs`; tokio-postgres cannot
  open one), which returns a snapshot name and the consistent point it was
  built at, exactly paired by Postgres. A read's first statement is
  `SET TRANSACTION SNAPSHOT '<alias>'` and its position is that point. A
  background task mints a fresh alias every 250 ms (configurable), but the
  **flip rule** is what keeps reads behind the engine: a minted alias becomes
  the current one only once the feed has been delivered past its consistent
  point (`advance`); until then the older alias stays current, and before the
  feed passes the first alias reads are parked. An alias lives as long as its
  connection, held until the last read that adopted it finishes. Queries are
  rendered from the model with the catalog's types cast on the way out
  (`sql.rs`), every leaf wrapped in `IS TRUE` so `NULL` semantics match the
  engine's. One connection per read in flight, pooled.
- `PgStream::open(dsn, slot, catalog)` streams a permanent `pgoutput` slot
  over a replication connection (`START_REPLICATION`, spoken by the
  `pgwire-replication` crate; the row messages decoded by the `pgoutput`
  crate, mapped onto the catalog's tables and types here). It creates the
  slot and a publication `<slot>_pub` for all tables if they are missing.
  Every change becomes a full-image insert/update or a key-only delete
  **positioned at the end of its transaction's commit record**, the scale the
  aliases' consistent points are on: a snapshot at consistent point `X` holds
  exactly the writes positioned at or below `X`. Progress marks come from
  **heartbeats**: a poll commits a tiny `pg_logical_emit_message` and consumes
  the feed until that heartbeat comes back; decoding emits whole transactions
  in commit order, so everything committed before it has been delivered by
  then, and the heartbeat's position is the mark. A primary-key change becomes
  delete + insert; tables with large TOASTed columns need `REPLICA IDENTITY
  FULL`; a `TRUNCATE` on a published table stops the feed. `PgStream::run`
  feeds a `Service`'s command channel and heartbeats on an interval so the
  mark keeps moving while the tables are quiet.
- Requirements: `wal_level = logical`; `max_replication_slots` and
  `max_wal_senders` headroom for the feed's slot plus the live aliases (up to
  three per storage instance at a rotation boundary); a role that may create
  the publication (`FOR ALL TABLES` needs a superuser, or create it beforehand
  under the feed's name). A mint waits for open transactions; the reads keep
  the current alias meanwhile.
- Live scenarios (`tests/pg_live.rs`) hold a snapshot open while writes commit
  behind it, one of them from a transaction already open when the snapshot
  was taken, and run the async service end to end with one table mirrored in
  memory; they need `XYNE_SYNC_PG_DSN` pointing at such a database and
  otherwise report themselves skipped.

### 6. The sync gateway (`src/gateway/`)

The server a Zero client connects to in place of the reference server. The xyne-spaces
dashboard (`@rocicorp/zero` 1.9, sync protocol 51) connects to it unchanged.

- **Threads.** A *feed thread* holds the replication connection, forwards raw
  `pgoutput` events and emits a heartbeat at an interval so the engine's
  position moves while nothing is written. The *engine thread* owns the
  runtime, decodes the events, runs every storage read as a task on its own
  local set (nothing blocks; a read lands when it returns), keeps every client
  group's view and builds the pokes. Every value the engine holds is
  thread-bound, so this is the only thread that touches writes, queries or
  deltas; it hands connections finished frames. The *server threads* (a
  multi-threaded tokio runtime) run the WebSocket connections: the handshake,
  the message loop, the liveness rules, and the HTTP calls to the application
  server for query ASTs and mutations.
- **A connection.** `GET <base>/sync/v51/connect?clientID&clientGroupID&…`
  with the first message base64-encoded in `Sec-WebSocket-Protocol` (echoed
  back, as the browser requires) or sent as the first frame. The gateway
  answers `connected`, then pokes. A `pong` answers every `ping`; when
  nothing has gone downstream for `XYNE_SYNC_PONG_INTERVAL_MS` a `pong` goes
  out anyway (so a client waiting behind a slow request still sees the server
  alive); a WebSocket ping frame goes out every `XYNE_SYNC_PING_INTERVAL_MS`
  and a connection that has sent nothing back for `XYNE_SYNC_CLIENT_TIMEOUT_MS`
  is closed and leaves its client group. A group's subscriptions outlive its
  last connection by `XYNE_SYNC_GROUP_TTL_MS`, then are released.
- **Queries.** A desired query arrives as a name and arguments; the gateway
  posts them to the application server's query endpoint (with the
  connection's cookies and origin, the way the reference server does) and gets query
  ASTs back, which `gateway/ast.rs` translates into the engine's trees:
  `related` edges become LEFT joins, `EXISTS` subqueries INNER joins with an
  `EXISTS` leaf in their place, a keyset `start` the `WHERE` it means, the
  root's `limit` the window; the primary key is appended to the order when
  absent. What the engine cannot run (`LIKE`, `NOT EXISTS`, compound join
  keys) comes back to the client as a `transformError` for that query alone.
  Subqueries the client marks as permission checks register but their rows are not
  shipped, as the reference server withholds them.
- **Pokes.** Per client group the gateway keeps, for every row shipped, the
  subscription parts holding it, so a row is `del`ed only when its last
  holder lets go and a row several queries share ships once. A poke goes out
  per committed transaction (a mutation's rows and its `lastMutationID`,
  read off the application's `xyne_0.clients` table, travel together), per
  landed read, and per query change; `gotQueriesPatch` follows a query once
  every part of its tree is live. Versions are the client's lexicographic cookies.
  The gateway keeps no history: a client reconnecting with the group's
  current cookie continues; with any other (a restart, changes it missed) it
  is told to start a fresh sync (`InvalidConnectionRequestBaseCookie`), which
  the client does on its own.
- **Mutations.** A `push` is forwarded verbatim to the mutate endpoint with
  `schema` and `appID` parameters and the connection's cookies; the answer
  comes back as `pushResponse`, a refusal as the `PushFailed` error the client
  understands. The application server records each mutation in
  `<app>_<shard>.clients` inside the mutation's transaction; the feed delivers
  that row with the rest, and the poke carries the id.
- **Types.** The catalog is read from `information_schema` at startup
  (`sync/pg/catalog.rs`), mapped the way the sync protocol maps Postgres for its clients:
  `timestamp`, `timestamptz` and `date` are milliseconds since the epoch
  (`ValueType::Timestamp`, read through `extract(epoch …)` and parsed off the
  feed's text), `json` and `jsonb` travel as their text and are embedded as
  JSON on the wire (`ValueType::Json`), arrays as JSON arrays, enums and
  uuids as strings; `bytea` is left out.

Configuration is by `XYNE_SYNC_*` variables (see [.env.example](.env.example);
the names a reference-server deployment sets are accepted for the database
and endpoint URLs). Not yet: history across reconnects (every reconnect after
a missed change is a fresh sync), query TTLs, the inspector protocol.

---

## Try it

```bash
cargo run --bin xyne_sync      # scripted demo: SQL in, routed operations + cost counters out
cargo test                    # model, parser, routing, window, join and read/write interleaving scenarios
cargo test --test xyne_spaces_queries   # the xyne-spaces dashboard's 283 queries on the engine (gap table in its main.rs)
cargo run --release --bin bench   # routing / registration / window / join benchmarks, and the xyne-spaces query shapes
cargo run --release --bin server  # the sync gateway on :4848 (reads .env; see .env.example)

# against a real Postgres (wal_level = logical, replication slots to spare, a role that may create a publication):
export XYNE_SYNC_PG_DSN=postgresql://postgres@localhost:5499/xyne_sync
cargo test --test pg_live     # snapshot held open while writes commit behind it; async service end to end
cargo run --release --bin bench   # adds scenario 5: registration, streamed writes, registration under load

# the gateway in front of a local xyne-spaces (backend on :3001 with ENABLE_DEV_AUTH=true, dashboard on :5173,
# Postgres with wal_level = logical and the app's xyne_0.clients / xyne_0.mutations tables):
cp .env.example .env              # set XYNE_SYNC_PG_DSN and the two endpoint URLs
cargo run --release --bin server
node scripts/e2e-protocol.mjs   # two dev users, real mutations, fan-out, reconnects; PASS when the chain holds
```

The demo registers six subscriptions on a `tickets` table and plays an
insert / insert / update / delete sequence through the engine, printing per
write the expected-vs-found impacted subscriptions, the emitted operations, and
the routing cost:

```text
-- INSERT INTO tickets (id, status, priority, assigned_to, points) VALUES (1, 'OPEN', 'LOW', 'aniket', 3)
   expected : ["q-all", "q-mine-active", "q-open", "q-open-dup"]
   impacted : ["q-all", "q-mine-active", "q-open", "q-open-dup"]   [PASS]
   op       : q-all          <- Add(id=1)
   op       : q-mine-active  <- Add(id=1)
   op       : q-open         <- Add(id=1)
   op       : q-open-dup     <- Add(id=1)
   cost     : 5 cond evals (3 hit), 3 disjunct bumps, 2 fired, membership 0/0 hit, 4 impacted, ops +4/-0
   note     : q-open and q-open-dup share one status = 'OPEN' counter — a single bump fires both
```

Every routing step is counted (`IvmStats`): condition evaluations and hits,
disjunct increments and firings, membership probes, condition edits, window
evictions and refills, snapshots shared, emitted operations. These are the
yardstick for every optimization: change the strategy, rerun, compare.

### Measured

`cargo run --release --bin bench` on an Apple M4 Max (rustc 1.89, release,
one thread; scenarios 2 to 4 use a bench-local in-memory storage double,
scenario 1 registers against an empty store so frames fill from writes alone,
scenario 5 runs against PostgreSQL 15 on the same machine over the streaming
feed, scenario 6 runs three xyne-spaces query shapes on the real catalog over
synthetic data; raw output with peak memory in
[paper/bench-2026-09-15-order-limit.txt](paper/bench-2026-09-15-order-limit.txt)
(the whole run; earlier runs beside it), analysis in paper §9. Scenarios 1
to 5 below quote the 2026-09-14 run; on 2026-09-15 the same machine ran
every scenario about 40% slower, the 2026-09-14 code included when rebuilt
and rerun beside the current one, so the day's numbers are comparable among
themselves and the relative results hold):

| Scenario | Result |
| --- | --- |
| Routing, 100 → 10 000 subscriptions | a write touches only the 3 to 4 conditions it satisfies (one probe per column) while the table carries 67 to 85; routing alone costs 0.9 / 2.0 / 13.0 µs per write at 100 / 1 000 / 10 000 subscriptions, delivery included 3.0 / 13.5 / 107.5 µs for an insert, and what grows is the impacted count (1.2 → 92 subscriptions per write), not the lookup |
| Registration, 100 → 10 000 subscriptions | 2.7 / 2.2 / 1.4 µs, flat: the twin lookup is one probe of the query-keyed index; peak memory of the whole run 0.51 GB, 0.76 GB with scenario 6 |
| Twin registration (400-row snapshot) | 547 µs from the shared frame vs 1 517 µs from (in-memory) storage; 1 000 twins hold 400 rows once |
| Window, `ORDER BY … LIMIT 50` over 100 000 rows | the client receives the page: 50 rows at registration over a buffer of 100; non-qualifying writes rejected inside the index at 0.9 µs (12 operations for 10 000 writes); under targeted writes, every delete on the page, the page moves on 9 192 of 10 000 writes (two operations each, the row leaving and the buffered one taking its place) with ten storage refills, and the cost is those refill scans of the storage double (10 × ~90 ms across 10 000 writes) |
| `LEFT JOIN`, 1 000 identical + 100 distinct subscriptions | identical subscriptions share one tree, so a ticket insert costs 117 µs for all 1 000 with 1.8 set edits per write instead of 52.5; the remaining cost is delivery, one operation per subscriber |
| xyne-spaces shapes (1 000 users, 500 channels, 50 000 conversations, 100 000 messages, 10 000 tickets on 20 boards; 7 000 subscriptions in 6 020 trees) | `browsableChannels` (`EXISTS` inside `OR`, participants attached) registers in 14.8 ms and ships the 2 250 rows it asks for (about 108 channels and their participants per user); `conversationMessages` under the channel-access chain (three `INNER` edges, the last inside an `OR`) registers in 1.2 ms with three reads; the board view (`IS NULL` twice, two `LEFT` edges, a page of 50 by `createdAt DESC, id ASC`) is a 0.5 ms twin copy of 150 rows. A message insert routes in 13.6 µs over 7 000 subscriptions (73 000 writes/s); a membership change costs 1.9 ms, moving 10.7 set members, fetching 5.3 uncovered channels and fanning its participant row out to the 230 subscriptions showing that channel (a public channel is shown by all 1 000 users); an in-place ticket update 56 µs: beyond the buffer's frontier it is rejected inside the index, behind the page it changes nothing the client sees, on the page it is one `Add` per subscriber of its board (5.2 client updates per write) |
| Over PostgreSQL (`LEFT JOIN`, 1 000 users, 2 000 tickets), streaming feed | a registration costs its two reads and their landing and nothing else: 3.0 ms end to end (1.8 ms in storage, 1.1 ms in the runtime), a twin 369 µs; 5 000 inserts committed in transactions of 100 stream from commit to delivery at 5 544 writes/s, split between the engine (107 µs per write for 1 001 subscribers, client grouping included), 339 sequential narrowed reads (one per newly referenced user, 229 ms) and 5 ms of feed and decoding; a registration whose snapshot is held open for 300 ms while 500 writes commit and are delivered behind it lands at once with 41 of its 148 rows brought up to the newer image, none dropped, and frames equal to the tables |

### Using the engine programmatically

```rust
use std::rc::Rc;
use xyne_sync::ivm::SingleTableIVM;
use xyne_sync::model::*;
use xyne_sync::parser::{parse_read, parse_write};
use xyne_sync::sync::{Local, MemoryStorage};

let catalog = Catalog::new(vec![DbTable::new("tickets", ["id"], vec![
    DbColumn::new("id", ValueType::Int),
    DbColumn::new("status", ValueType::String),
])]);
let storage = Rc::new(MemoryStorage::new());
let mut ivm = Local::new(SingleTableIVM::new(), storage.clone());   // the synchronous driver

let query = parse_read("SELECT * FROM tickets WHERE status = 'OPEN'", &catalog).unwrap();
let client = ClientId(1);
let (q_open, snapshot) = ivm.register_query(client, query);  // (SubId, Vec<ClientUpdate>), read landed inline

let write = parse_write("INSERT INTO tickets (id, status) VALUES (1, 'OPEN')", &catalog).unwrap();
storage.apply(&write);                                       // commit first …
let updates = ivm.incremental_update(&write);                // … notify second
// updates: Vec<ClientUpdate { client, table, op, targets: Vec<Target { sub, part }> }>
```

Against Postgres, build a `Service` over `MultiTableIVM` and `Sources` (a
`PgStorage` plus the tables to mirror), run it on a `tokio::task::LocalSet`,
send it the feed's first progress mark, `warm()` the mirrored tables, and let
`PgStream::run` feed its command channel; `tests/pg_live.rs` does exactly that.

---

## Semantics and restrictions (v1)

Deliberate simplifications, each enforced with a loud error rather than
silently narrowed:

- **Single table per SQL statement**: the parser refuses `JOIN`; multi-table
  subscriptions are built as `MultiTableReadQuery` in code.
- **Writes address one row by primary key** and carry the **complete** row
  image (every column, pkey included). Predicates are evaluated against that
  image. The parser enforces it for `UPDATE` (an `INSERT` may omit non-key
  columns, which then read as `NULL`); the engine debug-asserts that an image
  carries its key columns. Logical decoding delivers full images given
  `REPLICA IDENTITY FULL` (otherwise an update's new tuple omits unchanged
  TOASTed columns, which the feed reports as an error; paper §7.2).
- **`LIMIT L` ships the page of `L`** over a doubled buffer the engine
  keeps; **`ORDER BY` (one or more columns) decides which rows a window
  keeps**, never the order operations arrive in.
- **Join semantics**: `LEFT` keeps every parent row (an empty child side is
  null); `RIGHT` keeps every child row and shows a parent row only while a
  child matches it; `INNER` shows a parent row only while a child matches
  it and a child row only under a shown parent, evaluated from the child
  (the right default for per-user existence tests; choosing the driver per
  query is open). A finite limit below the root is normalized away.
- **NULL semantics**: any comparison touching `NULL` (or a missing column) is
  false, for every operator; three-valued logic collapsed to two. `IS NULL`
  and `IS NOT NULL` are the null tests, and the only operand `IS` takes is
  `NULL`.
- **Inserts are upserts**; **write literals are coerced** to declared column
  types (`1.0` into an `Int` column becomes `Int(1)`; out-of-range or
  mistyped values are rejected).
- **Single-threaded by design**: enforced at compile time (the index's
  shared counter handles are not `Send`). Multithreading is a later,
  deliberate step: shard by table.
- **DNF has no size cap** yet (exponential for adversarial filters; a cap with
  tree-evaluation fallback is deferred until a workload needs it).
- **Schema changes are not followed**: the feed skips tables the catalog does
  not declare and maps columns by name; a changed table needs a restart with
  the new catalog.

---

## Coverage: the xyne-spaces queries

`tests/xyne_spaces_queries/` rebuilds every synced query of the xyne-spaces
dashboard (the registry in the backend's `queries.ts`, 283 queries at
v1.316.4) on the engine, one test per query, plus the access-control
predicates `defineQuery` adds to each. The catalog is generated from the application's
schema (128 tables, 395 relationships, every one a single-column hop). Each
test registers the closest query the model can express over seeded storage,
asserts the snapshot, routes writes and asserts the deltas; a single-table
query also round-trips through the SQL parser. Where a query needs something
the engine lacks, the test keeps the expressible part and names the gap, so
the assertion shows today's behavior and flips when the gap closes; `gaps.rs`
pins each gap on its own.

| Gap | The queries use | The engine has |
| --- | --- | --- |
| N | `IS NULL` / `IS NOT NULL` (83 sites: `visibleTo IS NULL`, `rootId IS NULL`, `userId IS NULL`, `deletedAt IS NULL`, …) | **closed**: `IS` / `IS NOT` operators with a `NULL` operand, filed under the `NULL` key of the column index; every message, canvas and draft query states its rule in full |
| L | `LIKE` / `ILIKE` (11 sites: name, title, xyneId searches; one over JSON text) | not supported, by decision: no pattern operator |
| X | an existence test inside `OR` (canvas visibility, `browsableChannels`, `channelLinks`, `summaryTemplates`, `getUsers`, the channel-access ACL `visibility = PUBLIC OR EXISTS participants`, the calls ACL) | **closed**: an `EXISTS` leaf naming an inner join, bound in place to the edge's set; one subscription, the set-valued leaf inside the `OR` |
| O | a second `ORDER BY` column (tiebreaks on `id`); `ORDER BY` / `LIMIT` inside `related` | **half closed**: `ORDER BY` takes a list of columns, the page and the boundary decided by every column in turn; below the root every matching row still ships |
| J | `json` columns | opaque strings, serialized and deserialized as they are; no path, containment or pattern operator |
| E | `whereExists` returns no child rows | **closed**: `whereExists` is an `INNER` edge; the matching child rows ship as their own part, only under a shown parent (what the reference server syncs to its client too, though not in the result) |
| S, B | `.one()`, `LIMIT n` | **closed**: the client receives exactly the page of `n` (`.one()` is `LIMIT 1`) and the page's difference after every step; the doubled buffer behind it is the engine's |

Keyset cursors (`.start(row, {inclusive})`) and the empty `IN` list need no
engine change: the builder spells the cursor as the `WHERE` it means, and
`IN ()` is simply false.

---

## Layout

```text
Cargo.toml
paper/
  xyne-sync.tex / .pdf     the design paper (algorithms, join tree, ingestion, evaluation)
  bench-2026-09-15-inner-exists.txt     the benchmark run with the xyne-spaces scenario (all six, peak memory)
  bench-2026-09-14-streaming-feed.txt   the run scenarios 1 to 5 are reported from
  bench-2026-09-11.txt     the earlier run of scenarios 1 to 4 the paper's tables were taken from
  bench-2026-09-12-row-currency.txt, bench-2026-09-11-async-storage.txt,
  bench-2026-09-08-list-edges.txt, bench-2026-09-07-before-frontier.txt   earlier runs, kept for the record
docs/
  pg-lsn-cdc-lab.md        hands-on lab: LSNs, MVCC snapshots, the CDC handoff
src/
  lib.rs                   crate docs, module map, roadmap
  main.rs                  demo binary: SQL in, routed operations + counters out
  model/
    value.rs               Value / ValueType, manual Eq+Hash (floats, maps)
    schema.rs              Catalog / DbTable / DbColumn, TableName + ColumnName newtypes
    query.rs               SingleTableReadQuery / WriteQuery, Where / Condition, SubId, ClientId
    frame.rs               DataFrameKey / DataFrameRow / DataFrameOperation + the shared TableFrame
    position.rs            Lsn and Snapshot: where a write or a read's rows sit, as WAL locations
  ivm/
    mod.rs                 SingleTableIVM: the routing core (analyze + incremental_update)
    engine.rs              Fetch requests and the Engine trait the runtime drives
    update.rs              QueryPart, Target, ClientUpdate: per-client grouping of one step's operations
    registry.rs            subscription lifecycle: register/unregister/replace, twin sharing
    frames.rs              frame surgery + inspection: issue/land reads, adopt/upsert/remove rows, rows_for
    index.rs               TableIndex: shared DNF disjunct counters, boundaries, in-place edits
    columns.rs             per-column value index: equality, inequality and range lookups
    window.rs              ORDER BY / LIMIT: compound order, the page over a doubled buffer, boundary publishing, evict/refill
    multi.rs               MultiTableIVM: the join tree (LEFT / RIGHT / INNER edges, the gate, EXISTS binding) over the single engine
    predicate.rs           Where-tree evaluation, NULL semantics
    stats.rs               IvmStats counters + per-write diffing
  sync/
    storage.rs             the async Storage trait + MemoryStorage
    sources.rs             Sources: per-table routing between memory and Postgres (XYNE_SYNC_MEMORY_TABLES)
    runtime.rs             Runtime: the single owner, the one position, bringing results up to it, SyncStats
    local.rs               Local: the synchronous driver
    service.rs             Service: the async command loop (tokio LocalSet)
    pg/mod.rs              PgStorage: positioned REPEATABLE READ snapshots from exported-snapshot aliases
    pg/replication.rs      a minimal replication-protocol connection (mints the aliases)
    pg/sql.rs              model to SQL rendering
    pg/stream.rs           PgStream: the streaming pgoutput feed (Transport + Feed halves), positioned writes, heartbeat progress marks
    pg/catalog.rs          the catalog read from information_schema, typed the way the protocol types Postgres
    pg/text.rs             the text forms of times, JSON arrays and array literals
  gateway/
    mod.rs                 the threads and their wiring
    config.rs              XYNE_SYNC_* configuration
    protocol.rs            Zero's sync protocol v51: messages, handshake header, cookies
    ast.rs                 the client's query AST to the engine's query tree
    wire.rs                rows and keys as the wire carries them
    backend.rs             the query and mutate endpoints of the application server
    core.rs                the engine thread: client groups, held rows, pokes
    connection.rs          one WebSocket connection: handshake, message loop, liveness
    log.rs                 a leveled stderr log
  parser/
    mod.rs                 lexer + recursive-descent parser, schema-aware against model::Catalog
  bin/
    server.rs              the sync gateway binary
    bench.rs               benchmark harness
tests/
  ivm_scenarios.rs         single-table routing, windows, twin sharing, per-client grouping (assertable spec)
  multi_table_scenarios.rs join reference/fetch/prune, self-join, nested, RIGHT and INNER edges, EXISTS inside OR
  sync_interleaving.rs     reads out while writes stream: bring-up from the floor, parking, refill, post-order
  pg_live.rs               live Postgres: snapshot held open behind writes, async service with a mirrored table
  xyne_spaces_queries/     the xyne-spaces registry (283 queries) and ACL shapes on a catalog generated
                           from the application's schema; gaps.rs pins each expressiveness gap
```

---

## Dependencies

Nine crates, all permissively licensed; the reason for each is beside it in
`Cargo.toml`.

| Crate | Used for | License |
| --- | --- | --- |
| `chrono` | `Date` / `Datetime` values (no timezone database) | MIT OR Apache-2.0 |
| `tokio` | the async drivers and the server | MIT |
| `axum`, `futures-util` | the WebSocket server | MIT, MIT OR Apache-2.0 |
| `serde`, `serde_json` | the protocol's messages and the ASTs | MIT OR Apache-2.0 |
| `reqwest` (rustls) | the calls to the application server | MIT OR Apache-2.0 |
| `base64`, `percent-encoding` | the handshake header | MIT OR Apache-2.0 |
| `tokio-postgres` | SQL reads, slot and publication management, heartbeats | MIT OR Apache-2.0 |
| `postgres-protocol`, `fallible-iterator`, `bytes` | the replication-protocol connection that mints exported snapshots | MIT OR Apache-2.0, MIT OR Apache-2.0, MIT |
| `pgwire-replication` | the change feed's replication connection (`START_REPLICATION`, feedback, transaction boundaries); TLS features off | Apache-2.0 OR MIT |
| `pgoutput` | decoding the `pgoutput` row messages | MIT |

---

## Design notes (open)

Known consequences of the current shapes, kept on purpose and documented so
they are discussed rather than discovered:

1. **`Where::to_dnf` has no size cap** (see restrictions).
2. **No projection**: every query is `SELECT *`; a `columns` field changes
   what `DataFrameRow` holds per query.
3. **`limit: u32::MAX` means unbounded** and the default `ORDER BY` is the
   first declared pkey column ascending, parser conventions that programmatic
   queries must reproduce to share materialization with parsed ones.
4. **No `LIKE` / `BETWEEN`** operators (gap L above); `IS NULL` and `IS NOT
   NULL` are the only tests a `NULL` column can pass.
5. **Strict identity vs loose predicates**: row identity is variant-exact
   (`Float(1.0)` and `Int(1)` are different keys) while predicates coerce; the
   parser closes this for SQL writes, the model API does not.
6. **No `Update` op variant**: a change reaches a client as one `Add`
   (insert-or-replace); no operation carries an ordering position.
7. **Window ties and `NULL`s are pragmatic**, not SQL-exact (strict boundary,
   `NULL`/`NaN` largest, unenforceable boundary dropped).
8. **One heartbeat per poll**: the feed's progress mark costs a tiny
   committed transaction per poll (or per interval under the service); a
   server that forbids `pg_logical_emit_message` needs another mark.

---

## Contributing

- **Engine behavior** lives in `src/ivm/`; anything user-visible about *what
  matches* belongs in `predicate.rs` with a test alongside. Routing changes
  should keep `tests/` green and are expected to move the counters; say so.
- **Model changes** ripple everywhere (these types are index keys); discuss the
  design note you are addressing first.
- **Comment convention**: one `//!` block per file, `///` above every `fn` and
  type, no comments inside function bodies.
- Run `cargo test && cargo clippy --all-targets && cargo run --bin xyne_sync`
  before pushing; the demo must end all-`PASS`.

## Building

Rust **1.88+** (edition 2024; the crate uses let-chains, stable since 1.88). The paper builds with
`tectonic paper/xyne-sync.tex`.
