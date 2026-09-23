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
  │<─ [Add(new) → q1, q2] ───────┤   (folded per row, one image each)     │
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
| Join tree: one vector of edges, each with its **driver** (`Main` or `Sub`) and `is_inner`, so LEFT, RIGHT and the two inner forms (driven from the sub, driven from the main) at any depth; existence tests placed anywhere in a node's filter (`EXISTS` inside `OR`, bound to the set when the sub drives, a per-row gate on the match count when the main does); set-valued edges shared by identical subscriptions, cascades, self-joins, intersection on shared driven columns; a value index on every join column so a crossing costs the matches, not the part; a driven child's `ORDER BY` / `LIMIT` is a window **per parent row** (`related` with a limit) | ✅ done | `src/ivm/multi.rs` |
| Join planning: every node that would be read whole is counted, in one concurrent batch capped at the limit; the inner edges are settled from the root down (the side that fits; the smaller side when both do; a node already driven from above drives the edges below it; a main with a page is restricted by a sub that fits and drives any other, the engine keeping the page to the rows the sub admits), the root never moves, and a query with a side nothing can bound is refused with the reason; decisions are cached by tree (`XYNE_SYNC_JOIN_LIMIT`, `XYNE_SYNC_JOIN_PREFERRED_SIDE`, `XYNE_SYNC_PLAN_TTL_MS`, `XYNE_SYNC_PLAN_CACHE`), and run on the connection's own task | ✅ done | `src/client/plan.rs` |
| Client-free output: the engine knows subscriptions, never clients. One step's operations are folded per row (`Delta { table, op, audiences }`), each audience one part of one tree with **all its subscribers as the tree's shared list** (`Subs::Many(Arc<[SubId]>)`), so an operation costs the engine the same for one subscriber and for ten thousand; the service routes each delta to the group threads owning its subscriptions, and the group thread's row ledger decides what each client is sent (a row once per client group, whatever brought it); images and keys are shared handles (`Arc`), so nothing on the path copies a row | ✅ done | `src/ivm/update.rs`, `src/model/frame.rs` |
| SQL parser (single table, schema-aware, typed coercion, `i64` ids) | ✅ done | `src/parser/` |
| Asynchronous storage seam: the engine records the reads it needs (registration, join fetch, window refill) instead of running them; the runtime holds the one position and brings every read up to it before landing; no read ever blocks the stream; synchronous and asynchronous drivers | ✅ done | `src/ivm/engine.rs`, `src/sync/` |
| In-memory storage answering at once, honoring `ORDER BY` + `LIMIT`; per-table routing between memory and PostgreSQL (`XYNE_SYNC_MEMORY_TABLES`) | ✅ done | `src/sync/storage.rs`, `src/sync/sources.rs` |
| **PostgreSQL**: reads from the exported snapshot of a rotating temporary replication slot, flipped forward only once the feed has passed it; a streaming `pgoutput` change feed over a replication connection whose position comes from commits and PostgreSQL's own keepalives, so nothing is written to the database and a primary, a logical replica and a physical standby are followed alike; a feed whose connection died unannounced is found (nobody holds its slot any more) and reopened; every read bounded by a timeout; live tests and a bench scenario against a real server | ✅ done | `src/sync/pg/` |
| Routing counters + benchmark harness | ✅ done | `src/ivm/stats.rs`, `src/bin/bench.rs` |
| **xyne-spaces coverage**: the dashboard's 283 synced queries (and the ACL predicates added to them) rebuilt as tests on a catalog generated from the application's schema; `IS NULL`, `EXISTS` inside `OR` and `whereExists` closed, four expressiveness gaps left and pinned | ✅ tests, ⏳ gaps | `tests/xyne_spaces_queries/` |
| **Client side** speaking Zero's sync protocol (v51, the `@rocicorp/zero` 1.9 client): connect handshake, ping/pong and liveness, desired queries through the app server's query endpoint, pokes per client group, mutations through its mutate endpoint, `lastMutationID` off the app's clients table; it owns no engine and no database connection, reaching both only through [`sync::Service`]'s channels; verified from the xyne-spaces UI with three users (roles, resource access, channels, threads, reactions, tickets) and load-tested (see below) | ✅ done (no history across restarts) | `src/client/`, `src/sync/pg/threads.rs`, [docs/live-verification-2026-09-15.md](docs/live-verification-2026-09-15.md) |
| **The pipeline as threads**: the feed thread decodes and hands the engine one transaction per commit; the engine thread routes and nothing else; a reads pool runs the storage (one round trip per read); `XYNE_SYNC_GROUP_THREADS` group threads keep the views and build pokes, each row serialized once per flush and frames assembled as bytes; connections translate and plan their own queries; per-stage latency histograms at `/stats` | ✅ done | `src/client/mod.rs`, `src/stats.rs`, [docs/pipeline-2026-09-18.md](docs/pipeline-2026-09-18.md) |
| **Warm start**: the query shapes asked for kept in a file and planned again before readiness, so a restart's first clients hit the plan cache | ✅ done | `src/client/warm.rs` |
| **Transform cache** per identity, query and arguments; **observability**: lock-free stage histograms and counters, `/metrics` in Prometheus format, a sampler thread with a summary line, structured logs through an asynchronous queue; the same metrics and logs **pushed over OTLP** by a thread of their own, configured by the `OTEL_*` variables the reference server is configured by; reads counted against the row limit, with the queries nearing it reported by name | ✅ done | `src/client/transform.rs`, `src/stats.rs`, `src/metric.rs`, `src/otel.rs`, `src/log.rs`, `src/client/sampler.rs`, [docs/observability.md](docs/observability.md) |
| **Image, CI, deployment**: a slim image run unprivileged; on every pull request the lints, the whole suite with the live PostgreSQL scenarios, the benchmark against the base branch, and the image exercised end to end (sync, schema refusal, heavy reads, the OTLP push into a real collector); images published to GHCR | ✅ done | `docker/server/Dockerfile`, `.github/workflows/`, `scripts/smoke.mjs`, `scripts/bench-compare.py`, [docs/deploy.md](docs/deploy.md) |
| **Measured on production-shaped data**: the fixes for releases, memory and acknowledgements, both reference-server deployments under the same shapes, our own driver over the data, and the client-count ladders | ✅ measured | [docs/prod-scale-2026-09-18.md](docs/prod-scale-2026-09-18.md), [docs/prod-scale-2026-09-19.md](docs/prod-scale-2026-09-19.md) |
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
  `SubId`, a `u64` the engine hands out at registration and never reuses.
  The engine knows nothing of clients: connections, client groups and the
  client's own subscription names live in the transport, which maps them
  to `SubId`s and back.
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
and `INNER` keep the child's evaluation, so the child drives — unless the
planner (below) turns an `INNER` edge around because the parent is the small
side. The operand is
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
child without its parent. A row of a node that is itself gated (its own
`EXISTS` evaluated from the main) acts on the edge above it, as a match or
as a driver's reference, **only while its own gate is open**: a private
channel the reader is no participant of drives no conversation in, however
long the channel's row is held. A node with a `LIMIT` that drives an inner
edge is a **page under a gate**: the rows its gate rejects take no place in
the page, the window reaches past them in rounds (each once the tree's
reads have landed) and draws back when a gate opens, and a step's newly
referenced values are fetched in one narrowed read per driven part.

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
zero. What leaves the layer is the same per-row `Delta`, each audience
naming the part and the tree's subscribers (one shared list, not a copy
per subscriber).

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
  exactly the writes positioned at or below `X`. The feed **writes nothing**:
  its position comes from each commit and from the keepalives PostgreSQL
  sends whenever it has gone through log that held nothing for the slot
  (decoding emits whole transactions in commit order, so everything committed
  at or below either has been delivered); both are confirmed back to the
  slot. A poll asks the server where its log ends (a read) and consumes the
  feed until it has been delivered that far. A feed silent for ten seconds
  asks whether a walsender still holds its slot; a slot nobody holds means a
  connection that died unannounced: the slot is reopened, and the
  transactions the server sends a second time are applied once. A primary-key change becomes
  delete + insert; tables with large TOASTed columns need `REPLICA IDENTITY
  FULL`; a `TRUNCATE` on a published table stops the feed. `PgStream::run`
  feeds a `Service`'s command channel, and tells it the feed's position on
  an interval so the engine, and the snapshots waiting on it, keep moving
  while the tables are quiet.
- **Schema changes** reach the feed as the logical messages of the reference server's
  DDL event trigger (`ddl.rs`; `<app>_ddl_end_<shard>`, prefix
  `<app>/<shard>/ddl`), written in the migration's own transaction with the
  published schema before and after it, so they arrive at the change's
  commit on any topology; the server refuses to start without the trigger.
  The decoder grows its catalog the moment the message is absorbed (a row
  of the same transaction after it is decoded in the new shape) and the
  transaction carries the changes and the catalog they make. The service
  applies them before the transaction's writes: a column added is written
  into every held row of the table in memory with the value the migration
  gave existing rows (`NULL` or the constant default), no delta is sent,
  and a row entering later without it (an older snapshot's, a pre-change
  write) is completed on entry by one pointer comparison; a table added
  needs nothing held. Then the pool is told to read by the new catalog from
  that position on and to mint a snapshot at once; the catalog rides with
  the alias under one lock, so a read never pairs a snapshot with a catalog
  from the other side of a migration, and the clients' side (`CatalogHandle`,
  an `ArcSwap`) sees the new shape only once the current snapshot has it.
  Any other change to a served table (a column dropped or renamed, a type
  or key changed, a table dropped, a column added with an expression
  default) stops the feed with one line naming it, and the server with it;
  restarted, it loads the schema as it is. At every start the slot is moved
  up to the first read snapshot's point, so nothing the snapshot holds is
  streamed and a migration stopped on is not met again.
- Requirements: `wal_level = logical`; `max_replication_slots` and
  `max_wal_senders` headroom for the feed's slot plus the live aliases (up to
  three per storage instance at a rotation boundary); a role that may create
  the publication (`FOR ALL TABLES` needs a superuser, or create it beforehand
  under the feed's name). A mint waits for open transactions; the reads keep
  the current alias meanwhile.
- Live scenarios (`tests/pg_live.rs`) hold a snapshot open while writes commit
  behind it, one of them from a transaction already open when the snapshot
  was taken, run the async service end to end with one table mirrored in
  memory, and follow schema changes through the reference server's own trigger stack
  (installed by the test; `ADD COLUMN … DEFAULT`, `CREATE TABLE`, and a
  `DROP COLUMN` that stops the feed); they need `XYNE_SYNC_PG_DSN` pointing
  at such a database and otherwise report themselves skipped.

### 6. The client side (`src/client/`)

The server a Zero client connects to in place of the reference server. The xyne-spaces
dashboard (`@rocicorp/zero` 1.9, sync protocol 51) connects to it unchanged.

Everything in `src/client/` is about clients: their protocol, their ASTs,
their application server, their groups' views. It owns no engine, no storage
and no database connection. It drives the engine through the channels of
[`sync::Service`] — commands in (register, unregister), events out (deltas,
the subscription a registration became, hydration, landings, commits) —
which `sync/pg/threads.rs` wires to PostgreSQL.

- **Threads.** Five stages, each on its own core budget, joined by channels
  that carry shared handles rather than copies
  ([docs/pipeline-2026-09-18.md](docs/pipeline-2026-09-18.md) has the design
  and the measurements). The *feed thread* holds the replication connection,
  decodes the `pgoutput` events into rows and hands the engine **one
  transaction per commit**, and the feed's position a few times a second so
  the engine keeps up while nothing is written. The *engine thread* owns the runtime and the engine and
  does nothing but route: no decoding, no SQL, no JSON. The *reads pool*
  (`XYNE_SYNC_READ_THREADS`) renders the SQL, runs every storage read as one
  simple-query round trip on a pooled connection, decodes the rows, answers
  the planner's counts and the connect-time reads, and mints the snapshots.
  The *group threads* (`XYNE_SYNC_GROUP_THREADS`, each owning the client
  groups that hash to it; the engine routes events by the shard in the client
  id) keep the views and build the pokes: a thread takes everything sent since
  its last wakeup, applies it, and pokes each group once, every row serialized
  once per flush and a group's frame assembled from those bytes. The *server
  threads* (a multi-threaded tokio runtime) run the WebSocket connections: the
  handshake, the message loop, the liveness rules, the HTTP calls to the
  application server, the translation and planning of every query, and the
  writes to the socket (a poke's frames fed together, one flush). Every value
  crosses these threads by handle: `Value` is `Send`, and a row image is one
  allocation shared by the frame, every subscription and every poke.
- **A connection.** `GET <base>/sync/v51/connect?clientID&clientGroupID&…`
  with the first message base64-encoded in `Sec-WebSocket-Protocol` (echoed
  back, as the browser requires) or sent as the first frame. The server
  answers `connected`, then pokes. A `pong` answers every `ping`; when
  nothing has gone downstream for `XYNE_SYNC_PONG_INTERVAL_MS` a `pong` goes
  out anyway (so a client waiting behind a slow request still sees the server
  alive); a WebSocket ping frame goes out every `XYNE_SYNC_PING_INTERVAL_MS`
  and a connection that has sent nothing back for `XYNE_SYNC_CLIENT_TIMEOUT_MS`
  is closed and leaves its client group. A group's subscriptions, and its
  record of the rows it was sent, outlive its last connection by
  `XYNE_SYNC_GROUP_TTL_MS` (a minute by default, an hour in `.env.example`),
  then are released. A client is owed everything after its cookie: a group
  with no connection stands still (its changes wait, coalesced, and its
  version does not move), so a client back within the lifetime is sent
  the net of what it missed; a tab that joins a group under way without a
  cookie is sent the group's whole state from its row ledger, the other
  tabs undisturbed; a tab behind its group is replayed the pokes it missed
  from the group's log (`XYNE_SYNC_GROUP_LOG_BYTES`); anything else
  starts a fresh sync (`docs/client-resume-2026-09-21.md`).
- **Bounded waits.** A storage read is given `XYNE_SYNC_READ_TIMEOUT_MS`
  (10 s): PostgreSQL cancels the statement, the pool stops waiting, and the
  query that needed it is refused by name rather than read again. A call
  to the application server is given `XYNE_SYNC_BACKEND_TIMEOUT_MS` (30 s)
  and is not repeated when it ran out of it
  (`docs/feed-json-timeouts-2026-09-22.md`).
- **Queries.** A desired query arrives as a name and arguments; the client side
  posts them to the application server's query endpoint (with the
  connection's cookies and origin, the way the reference server does) and gets query
  ASTs back, which `client/ast.rs` translates into the engine's trees:
  `related` edges become LEFT joins, `EXISTS` subqueries INNER joins with an
  `EXISTS` leaf in their place, a keyset `start` the `WHERE` it means, the
  root's `limit` the window; the primary key is appended to the order when
  absent. What the engine cannot run (`LIKE`, `NOT EXISTS`, compound join
  keys) comes back to the client as a `transformError` for that query alone.
  Subqueries the client marks as permission checks register but their rows are not
  shipped, as the reference server withholds them.
- **Planning.** Before a translated query registers, `client/plan.rs` decides
  which side of each inner edge drives it, on the connection's own task. A
  node nothing drives is read whole; the planner counts every such node in
  one concurrent batch on the reads pool, no further than
  `XYNE_SYNC_JOIN_LIMIT` + 1 (100 000 by default, so a big table is never
  scanned whole), a node with a page being bounded by its window and a
  driven node by its driver. The inner edges are then settled from the root
  down: an edge with one bounded side is driven from it; one whose sides
  both fit by their own counts is driven from the smaller, with
  `XYNE_SYNC_JOIN_PREFERRED_SIDE` (`parent` by default) winning unless the
  other side is at most half its size (a project's few boards drive the
  workspace's stages); a node already driven from above drives the edges
  below it, so an access rule is read link by link from the row outwards
  (the message's conversation, its channel, the reader's participation);
  a main with a page is restricted by a sub that fits by its own count
  (the window stays exact) and drives any other sub, the engine keeping
  the page to the rows the sub admits (`docs/gated-pages-2026-09-20.md`);
  and an edge left with no bounded side refuses the query with a
  `transformError` naming the sides. The root never moves:
  the decision is the `driver` field of the edge, so part paths, hidden
  parts and `EXISTS` leaves stay where the translation put them, and a
  nested `EXISTS` is planned like one at the root. Decisions are cached by
  tree for `XYNE_SYNC_PLAN_TTL_MS` (10 min), at most `XYNE_SYNC_PLAN_CACHE`
  (10 000) of them, so the counts run once per distinct tree.
- **Warm start.** With `XYNE_SYNC_PLAN_FILE` set, every query shape that
  translates (its name and the application server's AST for it) is kept
  in that file, written every minute and on shutdown; the next process
  plans the kept shapes again, eight at a time for at most
  `XYNE_SYNC_WARM_START_MS` (20 s), after the feed has passed the first
  read snapshot and
  before `/health` turns `200`, so the first client of each shape after a
  restart hits the plan cache instead of paying the counts (`client/warm.rs`).
- **Related windows.** A `related` subquery with `orderBy`/`limit` is the
  best *n* rows per parent row, as the client means it: the engine registers one
  windowed part per referenced parent value, so every window is maintained
  by the single engine like any other, refills included, and the client sees
  one part.
- **Pokes.** Per client group `client/groups.rs` keeps, for every row shipped, the
  subscription parts holding it, so a row is `del`ed only when its last
  holder lets go and a row several queries share ships once. A poke goes out
  per flush of the group thread: idle, that is per committed transaction (a
  mutation's rows and its `lastMutationID`, read off the application's
  `xyne_0.clients` table and carried inside the same transaction's commit
  event, travel together); under load one poke covers every transaction that
  arrived since the last flush, which keeps the frame count per connection
  bounded as the write rate climbs. `gotQueriesPatch` follows a query once
  every part of its tree is live. Versions are the client's lexicographic cookies.
  The server keeps no history: a client reconnecting with the group's
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
  uuids as strings; `bytea` is left out. A JSON value has **one standard
  text** wherever it comes from (`sync/pg/text.rs`): the text `jsonb`
  writes, with a number as its plain digits (`1.50`, `1.5` and `15e-1` are
  all `1.5`). A read selects `column::jsonb::text` (nothing for a `jsonb`
  column, the conversion for a `json` one), the feed trims padded fractions
  off a `jsonb` cell in one pass and rewrites a `json` cell (it knows which
  from the column's type in the relation message), and a query's literal is
  written the same way, so a filter on a JSON column means in the engine
  what `column::jsonb = literal::jsonb` means in PostgreSQL.

- **Query lifetimes.** A query nobody desires any more stays registered
  for the `ttl` the client gave it (capped at ten minutes, five when
  absent), so a client coming back to it, or swapping a paginated query for
  its next page, finds the rows in place; only when the lifetime runs out
  are its rows withdrawn and its `got` revoked. Without this the dashboard
  re-requests some queries several times a second and rows flicker.
- **Reads.** At most `XYNE_SYNC_READ_CONNECTIONS` (16) storage reads hold a
  Postgres connection at once; the rest queue in the server instead of at
  the server's `max_connections`. A burst of two hundred connections opening
  at once used to exhaust a default Postgres and park the reads it refused.
  A read is one simple-query batch (`BEGIN … READ ONLY; SET TRANSACTION
  SNAPSHOT; SELECT; COMMIT`), so its rows are back after one round trip, and
  a read narrowed to one join value renders that value instead of the whole
  set it belongs to. A read may return at most `XYNE_SYNC_READ_ROW_LIMIT`
  (100 000) rows; past that it is refused, the subscriptions depending on it
  are unregistered and their clients get a `transformError` naming the
  table, because a query without a `LIMIT` over a large table would
  otherwise be buffered whole.
- **Readiness and shutdown.** `GET /health` (also `/healthz` and
  `/readyz`) answers `503` until the engine's position has covered the
  storage's first snapshot and the warm start, if one is configured, has
  run, and `200` from then on; a connection that
  arrives earlier waits for that moment before any of its queries is
  planned, so a client never sees a refusal for having been first. On
  `SIGTERM` or ctrl-c the listener stops, every client is closed with
  `1001` (going away) spread over three seconds, and the process exits
  once the sockets are gone or after five seconds; a panic in the engine
  or a group thread ends the process instead of leaving a listener with
  no engine behind it. Client sockets run with `TCP_NODELAY`, since a
  poke is several frames and Nagle's algorithm with delayed
  acknowledgements would hold the later ones back by tens of
  milliseconds.
- **Transform cache.** The application server's AST for a query is kept
  per identity, name and arguments for `XYNE_SYNC_TRANSFORM_TTL_MS`
  (60 s; the reference server keeps its own for 5 s; the rig's hit rates below
  were measured at 5 min), at most
  `XYNE_SYNC_TRANSFORM_CACHE` (20 000) entries, so a shape asked for again
  skips the round trip; failed transforms are never kept
  (`client/transform.rs`). Measured on the rig: 0.6 % hits on five
  minutes of real sessions, 32 to 37 % on the statistical mix, where it
  took a third of the backend's round trips away and the steady select
  from 21 to 15 ms at the median.
- **Observability** ([docs/observability.md](docs/observability.md)).
  Every stage of both paths is a lock-free histogram and every count an
  atomic, recorded by the thread doing the work; `GET /stats` serves them
  as JSON (the load harness's format; `?reset=1` zeroes the histograms
  after reading, which the harness does at the start of its steady phase,
  so do not combine it with scraping), `GET /metrics` in Prometheus
  exposition format (durations in seconds, `_total` counters, gauges for
  what is open, held and queued, `rows_held{table}`,
  `thread_cpu_seconds_total{thread}`), and a `xyne-sync-metrics` thread
  samples the process every `XYNE_SYNC_METRICS_INTERVAL_MS` (10 s) and
  writes a one-line summary every minute. Logs go through a bounded queue
  to a `xyne-sync-log` thread (a full queue drops and counts, never
  blocks), as text or JSON lines (`XYNE_SYNC_LOG_FORMAT`), with structured
  events for connections opened and closed, queries hydrated (at debug;
  at warn past `XYNE_SYNC_SLOW_QUERY_MS`), pushes and the warm start.

Configuration is by `XYNE_SYNC_*` variables (see [.env.example](.env.example);
the names a reference-server deployment sets are accepted for the database
and endpoint URLs). `XYNE_SYNC_LOG=debug` writes a line per poke per client
group and costs throughput; keep it for bring-up. Not
yet: history across reconnects (every reconnect after a missed change is a
fresh sync, and a Zero client that is told so drops its local database, unsent
mutations included, so a restart while people are typing loses their
unsent messages), the inspector protocol.

---

## Try it

```bash
cargo run --bin xyne_sync      # scripted demo: SQL in, routed operations + cost counters out
cargo test                    # model, parser, routing, window, join and read/write interleaving scenarios
cargo test --test xyne_spaces_queries   # the xyne-spaces dashboard's 283 queries on the engine (gap table in its main.rs)
cargo run --release --bin bench   # routing / registration / window / join benchmarks, and the xyne-spaces query shapes
cargo run --release --bin server  # the sync server on :4848 (reads .env; see .env.example)

# against a real Postgres (wal_level = logical, replication slots to spare, a role that may create a publication):
export XYNE_SYNC_PG_DSN=postgresql://postgres@localhost:5499/xyne_sync
cargo test --test pg_live     # snapshot held open while writes commit behind it; async service end to end
XYNE_SYNC_PG_DSN=postgresql://postgres@localhost:5499/xyne_bench cargo run --release --bin bench
                              # adds scenario 5 (registration, streamed writes, registration under load); it drops and
                              # recreates `users` and `tickets` there, so the database's name must contain `bench`

# the server in front of a local xyne-spaces (backend on :3001 with ENABLE_DEV_AUTH=true, dashboard on :5173,
# Postgres with wal_level = logical and the app's xyne_0.clients / xyne_0.mutations tables):
cp .env.example .env              # set XYNE_SYNC_PG_DSN and the two endpoint URLs
cargo run --release --bin server
node scripts/e2e-protocol.mjs   # two dev users, real mutations, fan-out, reconnects; PASS when the chain holds
node scripts/smoke.mjs --prepare && node scripts/smoke.mjs   # no application needed: a PostgreSQL, the server (XYNE_SYNC_READ_ROW_LIMIT=600),
                                     # a scripted Zero client; sync, a JSON filter, a client back after writes, late and lagging tabs,
                                     # schema refusal, heavy reads, and with SMOKE_COLLECTOR the OTLP push. CI runs it on the image
node scripts/load-smoke.mjs --connections 300 --writes 100 --duration 20 --away 100   # the same setup under load: delivery latency, the
                                     # server's stage timings, and how connections that leave and return are answered
node scripts/ui/ui-u2-channel.mjs    # one of the Playwright scripts that drive the dashboard with three users (scripts/ui/)
node scripts/load-protocol.mjs --connections 200 --seed 3000 --seed-replies 2000 --rate 50 --duration 60 \
  --pid $(pgrep -f target/release/server)   # socket-level load: seed, hydrate, steady fan-out, CPU/RSS samples
                                            # (--auth-pool FILE runs it on a list of identities with tokens instead of the test login)
LINUX_BIN=/path/to/linux/server CPUS="2 4 8" scripts/load-matrix.sh   # the same against the server pinned to N CPUs (docker --cpus)
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
synthetic data; raw output in
[paper/bench-2026-09-18-pipeline.txt](paper/bench-2026-09-18-pipeline.txt),
with the same day's run of the tree before the pipeline rework beside it as
[paper/bench-2026-09-18-before-pipeline.txt](paper/bench-2026-09-18-before-pipeline.txt),
earlier runs beside them, analysis in paper §9 and the before/after table in
[docs/pipeline-2026-09-18.md](docs/pipeline-2026-09-18.md) §10. Scenarios 1
to 4 and 6 quote the 2026-09-18 run, whose whole-run time is 10.4 s against
52.9 s for the tree before it on the same machine; scenario 5 quotes the
same day's run over PostgreSQL,
[paper/bench-2026-09-18-postgres.txt](paper/bench-2026-09-18-postgres.txt).
Timings moved by up to 40% between days on this shared
workstation, so numbers from one day compare among themselves and the
relative results hold):

| Scenario | Result |
| --- | --- |
| Routing, 100 → 10 000 subscriptions | a write touches only the 3 to 4 conditions it satisfies (one probe per column) while the table carries 67 to 85; routing alone costs 0.9 / 1.7 / 3.7 µs per write at 100 / 1 000 / 10 000 subscriptions, delivery included 1.3 / 4.3 / 27.9 µs for an insert, and what grows is the impacted count (1.2 → 92 subscriptions per write), not the lookup |
| Registration, 100 → 10 000 subscriptions | 7.4 / 2.2 / 1.5 µs, flat: the twin lookup is one probe of the query-keyed index; peak memory of the whole run 1.40 GB on 2026-09-18 (1.85 GB on 2026-09-15 for the same scenarios; 0.51 GB without scenario 6) |
| Twin registration (400-row snapshot) | 123 µs from the shared frame vs 400 µs from (in-memory) storage; 1 000 twins hold 400 rows once |
| Window, `ORDER BY … LIMIT 50` over 100 000 rows | the client receives the page: 50 rows at registration over a buffer of 100; non-qualifying writes rejected inside the index at 0.21 µs (12 operations for 10 000 writes); under targeted writes, every delete on the page, the page moves on 9 192 of 10 000 writes (two operations each, the row leaving and the buffered one taking its place) with ten storage refills, 28 µs per write with those refill scans of the storage double included |
| `LEFT JOIN`, 1 000 identical + 100 distinct subscriptions | identical subscriptions share one tree, so a ticket insert costs 36 µs for all 1 000 with 1.8 set edits per write instead of 52.5, and an update of a user row that 722 join operations depend on 203 µs; the remaining cost is delivery, one operation per subscriber |
| xyne-spaces shapes (1 000 users, 500 channels, 50 000 conversations, 100 000 messages, 10 000 tickets on 20 boards; 7 000 subscriptions in 6 020 trees) | `browsableChannels` (`EXISTS` inside `OR`, participants attached) registers in 2.4 ms and ships the 2 250 rows it asks for (about 108 channels and their participants per user); `conversationMessages` under the channel-access chain (three `INNER` edges, the last inside an `OR`) registers in 0.36 ms with three reads; the board view (`IS NULL` twice, two `LEFT` edges, a page of 50 by `createdAt DESC, id ASC`) is an 89 µs twin copy of 150 rows. A message insert routes in 11.3 µs over 7 000 subscriptions (88 000 writes/s); a membership change costs 0.29 ms, moving 10.7 set members, fetching 5.3 uncovered channels and fanning its participant row out to the 230 subscriptions showing that channel (a public channel is shown by all 1 000 users); an in-place ticket update 4.8 µs: beyond the buffer's frontier it is rejected inside the index, behind the page it changes nothing the client sees, on the page it is one `Add` per subscriber of its board (5.2 client updates per write) |
| Over PostgreSQL (`LEFT JOIN`, 1 000 users, 2 000 tickets), streaming feed | a registration costs its two reads and their landing and nothing else: 2.4 ms end to end (1.8 ms in storage, 0.4 ms in the runtime), a twin 79 µs; 5 000 inserts committed in transactions of 100 stream from commit to delivery in 162 ms, 26 748 writes/s (5 544 on 2026-09-14), split between the engine (21.6 µs per write for 1 001 subscribers, client grouping included; 107 before), 339 narrowed reads through the reads pool (one per newly referenced user, 41 ms; 229 ms when sequential) and 6 ms of feed and decoding; a registration whose snapshot is held open for 300 ms while 500 writes commit and are delivered behind it lands at once with 41 of its 148 rows brought up to the newer image, none dropped, and frames equal to the tables |
| On production-shaped data (a 78 GB copy of the application's database, 90 M messages, the application's real 151-query mix and 301 identities, on the ART rig; [docs/prod-scale-2026-09-19.md](docs/prod-scale-2026-09-19.md)) | Clients churning a query every 750 ms (about 250× the production cadence), every query on a 5 s TTL: a steady select at 100 / 200 / 300 clients is 10 / 21 / 139 ms at the median and 31 / 72 / 1 081 ms at the 99th percentile, a new client's twelve-query first screen 22 / 26 / 37 ms; the engine's own steps stay under 10 ms at the 99th percentile, the backend's transform (4 → 65 ms) and the one engine thread's inbox are what grows. The reference server with the Rust view-syncer (2 workers) on the same host and data: 16 / 22 / 51 ms medians, 46 / 56 / 160 at the 99th, a 92 to 132 ms first screen, 21 GB resident; the TypeScript reference server 1.9.0 (6 workers): 32 / 132 / 3 994 ms medians. An update reaches 50 clients 112 ms after its commit through the rig's logical replica, of which the server spends 0.5 to 1.5 ms; a mutation is acknowledged in 141 ms at the median (31 ms to its `pushResponse`). |
| Our own driver over the same data (five of the application's queries per client, about 2 100 rows each) | 900 subscriptions a second registered whatever the client count; 200 clients hydrated in 1.0 s, 400 in 2.1 s, 800 in 2.7 s at the median (the two reference servers register about 100 a second and take 6 to 7 s for 200 clients); 160 000 rows a second delivered to 200 clients at 159 ms after their commit (the server's own path 20 ms at the median, 57 at the 99th) on 1.5 cores and 2.5 GB; 37 000 rows a second over twenty channels at 16 ms of server time. |
| How many clients (10 group threads, 10 read threads, 100 read connections) | The engine thread is the only limit: 1.7 to 3.2 ms per query swap as the population grows (registration plus release plus landings), 60 to 70 % busy at 200 to 250 swaps a second. At a swap every 10 s per client: 1 000 clients at 20 %, 2 000 at 55 to 65 %, 3 000 saturated; at the production cadence (a swap per three minutes) 3 000 clients under 15 %, with memory (7 to 8 GB per 1 000 clients) the limit. The group and read pools used under 0.3 cores of their twenty. |
| The server over the wire (`scripts/load-protocol.mjs`: clients holding the chat screen's eight queries each, writes committed straight into PostgreSQL) | 200 clients, 1 600 subscriptions, hydrated in 0.2 to 0.3 s. One channel, every update to all 200 clients: **1 000 updates a second — 200 000 client rows a second — every row delivered, 28 ms median and 34 ms at the 99th percentile, at 1.2 cores**, the generator's limit on this machine, not the server's (400 a second cost 65% of one core with a 99th percentile of 68 ms; on 2026-09-16 that rate was the saturation point with a median of 1.2 s). Twenty channels: **7 400 updates a second, all delivered, at 1.7 cores** (4 000 a second at 67% of a core with a 99th percentile of 50 ms). Inside the server a transaction takes 1.5 ms from the feed to the last frame written at low rates (engine 26 to 40 µs per update where one tree shows the channel, 130 to 210 where ten do; 1.3 to 1.8 µs per client row on the group thread); the 24 ms floor a client sees is the harness's `psql` per batch, PostgreSQL's decoding and the Node client. Pinned to 2, 4 and 8 CPUs the figures agree below 2 000 updates a second and separate above it (2 cores at 103%, 8 at 76% for 4 000 a second). Earlier: 4 000 clients held 32 000 subscriptions at about 120 KB each (2026-09-16), and through the application server the ceiling is its own, about 95 mutations a second. Details in [docs/pipeline-2026-09-18.md](docs/pipeline-2026-09-18.md) §10 and [docs/load-2026-09-16.md](docs/load-2026-09-16.md), raw results in [paper/load-2026-09-18/](paper/load-2026-09-18/) and [paper/load-2026-09-16/](paper/load-2026-09-16/) |

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
let (q_open, snapshot) = ivm.register_query(query);          // (SubId, Vec<Delta>), read landed inline

let write = parse_write("INSERT INTO tickets (id, status) VALUES (1, 'OPEN')", &catalog).unwrap();
storage.apply(&write);                                       // commit first …
let updates = ivm.incremental_update(&write);                // … notify second
// updates: Vec<Delta { table, op, audiences: Vec<Audience { part, subs }> }>; update.targets() lists (sub, part)
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
- **Schema changes are followed only two ways**: a table created (with a
  key) and a column added with no default or a constant one, heard through
  the reference server's DDL trigger (section 5). Every other change to a served
  table stops the server, which restarts on the new schema; a column added
  with an expression default (`now()`, `gen_random_uuid()`) counts as such,
  since the values it gave existing rows are in the database only.

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
| O | a second `ORDER BY` column (tiebreaks on `id`); `ORDER BY` / `LIMIT` inside `related` | **closed**: `ORDER BY` takes a list of columns, the page and the boundary decided by every column in turn; a `related` node's `ORDER BY` / `LIMIT` is a window per parent row, one inner part per referenced value |
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
  pipeline-2026-09-18.md   the pipeline as threads: design, the join model, the hot-path list, the measurements
  load-2026-09-16.md       the load test before the pipeline change, for the record
  live-verification-2026-09-15.md   the UI verification with three users
  pg-lsn-cdc-lab.md        hands-on lab: LSNs, MVCC snapshots, the CDC handoff
src/
  lib.rs                   crate docs, module map, roadmap
  main.rs                  demo binary: SQL in, routed operations + counters out
  model/
    value.rs               Value / ValueType, manual Eq+Hash (floats, maps); SharedSet behind a lock, so values are Send
    schema.rs              Catalog / DbTable / DbColumn, TableName + ColumnName newtypes over shared strings
    query.rs               SingleTableReadQuery / WriteQuery, Where / Condition, Join { driver, is_inner }, SubId
    frame.rs               DataFrameKey / DataFrameRow (shared, immutable) / DataFrameOperation + the shared TableFrame with its value indexes
    position.rs            Lsn and Snapshot: where a write or a read's rows sit, as WAL locations
    ids.rs                 IdMap / IdSet: the engine's integer-keyed maps on a one-multiply hasher
  ivm/
    mod.rs                 SingleTableIVM: the routing core (analyze + incremental_update)
    engine.rs              Fetch requests and the Engine trait the runtime drives
    update.rs              QueryPart, Subs, Audience, Delta: the per-row fold of one step's operations, subscribers as shared lists
    registry.rs            subscription lifecycle: register/unregister/replace, twin sharing
    frames.rs              frame surgery + inspection: issue/land reads, adopt/upsert/remove rows, rows_for
    index.rs               TableIndex: shared DNF disjunct counters, boundaries, in-place edits
    columns.rs             per-column value index: equality, inequality and range lookups
    window.rs              ORDER BY / LIMIT: compound order, the page over a doubled buffer, boundary publishing, evict/refill
    multi.rs               MultiTableIVM: the join tree (edges by driver, the gates, EXISTS binding, driven windows) over the single engine
    predicate.rs           Where-tree evaluation, NULL semantics
    stats.rs               IvmStats counters + per-write diffing
  sync/
    storage.rs             the async Storage trait + MemoryStorage
    sources.rs             Sources: per-table routing between memory and Postgres (XYNE_SYNC_MEMORY_TABLES)
    runtime.rs             Runtime: the single owner, the one position, bringing results up to it, SyncStats
    local.rs               Local: the synchronous driver
    service.rs             Service: the async command loop (tokio LocalSet), the feed's transactions, the event streams per consumer
    pg/mod.rs              PgStorage: positioned REPEATABLE READ snapshots from exported-snapshot aliases, one round trip per read, on the reads pool
    pg/replication.rs      a minimal replication-protocol connection (mints the aliases)
    pg/sql.rs              model to SQL rendering
    pg/stream.rs           PgStream: the streaming pgoutput feed (Transport + Feed halves), positioned writes, the position from commits and keepalives, the silent-feed watch
    pg/catalog.rs          the catalog read from information_schema, typed the way the protocol types Postgres
    pg/text.rs             the text forms of times, JSON arrays and array literals
    pg/threads.rs          the engine side over Postgres: the feed thread (decoding), the storage on the pool, the Service
  client/
    mod.rs                 the threads and their wiring: feed, engine, reads pool, group threads, server
    config.rs              XYNE_SYNC_* configuration
    protocol.rs            Zero's sync protocol v51: messages, handshake header, cookies
    ast.rs                 the client's query AST to the engine's query tree
    plan.rs                the driver of every inner edge: one batch of counts, a fixed point, refusal, the plan cache
    wire.rs                rows and keys as the wire carries them, written straight into bytes
    backend.rs             the query and mutate endpoints of the application server
    groups.rs              the group threads: client groups, held rows, drain-and-flush, pokes serialized once; what a connecting client is owed since its cookie
    schema.rs              the client's schema against the catalog, judged as the reference server judges it: serve or SchemaVersionNotSupported
    connection.rs          one WebSocket connection: handshake, the schema's judgment, message loop, liveness, translate + plan, the writer
  log.rs                   a leveled stderr log, tapped by the telemetry exporter
  stats.rs                 per-stage latency histograms, counters and gauges; reads against the row limit; the catalogue
  metric.rs                one metric, whatever carries it: the catalogue's types and the Prometheus text
  otel.rs                  the push to a collector: OTEL_* configuration as the reference server reads it, OTLP/HTTP JSON, the exporter thread
  parser/
    mod.rs                 lexer + recursive-descent parser, schema-aware against model::Catalog
  bin/
    server.rs              the sync server binary
    bench.rs               benchmark harness
tests/
  ivm_scenarios.rs         single-table routing, windows, twin sharing, per-row folding (assertable spec)
  multi_table_scenarios.rs join reference/fetch/prune, self-join, nested, RIGHT and both INNER forms, EXISTS inside OR, per-parent windows
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
| `tokio-postgres` | SQL reads, slot and publication checks, asking the server where its log ends | MIT OR Apache-2.0 |
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
8. **The feed's position on an idle database is PostgreSQL's keepalive**:
   it is sent when the log moves past what the feed has confirmed, so a
   database where nothing at all is written says nothing, and the position
   stands still with the log. On a physical standby a fresh read snapshot
   waits for the primary's next running-transactions record (every 15 s
   while the primary writes), so snapshots rotate at that pace there.
9. **Values are `Send` through a lock, not a copy**: `SharedSet` is an
   `Arc<RwLock<..>>` so a row, a query or a delta can cross a thread; the
   engine itself stays single-threaded (its index counters are `Rc`). Row
   images and keys are immutable `Arc` maps (a key carries its hash), and
   names are `Arc<str>`: a clone anywhere is a reference count. `Join` has a
   `driver` and `is_inner` in place of three vectors; an `EXISTS` leaf names
   the node's i-th inner edge, whichever side drives it.
10. **A page may drive an inner edge**: the join layer tells the page's
    window which of the rows it shows the gate rejects, and the window
    reaches past them (and draws back when a gate opens), so the page is
    the best rows the edge admits, as the client's `Take` over `Exists`. It gives
    up after eight rejected rows per row of the page, and the query is
    then reported by name. A held row acts on the edge above it (as a
    match, or as a driver's reference) only while its own gate is open.

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
