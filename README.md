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
  ├─ subscribe(id, SQL) ────────>│                                        │
  │                              ├─ initial SELECT @ exported snapshot ──>│ replica
  │<─ snapshot: [Add, Add, …] ───┤                                        │
  │                              │<──── committed changes (one logical ───┤ primary
  │                              │      replication slot, commit-LSN order)│
  │                              ├─ route: which views does this touch?   │
  │                              ├─ patch: emit Add / Delete per view     │
  │<─ [Delete(old), Add(new)] ───┤                                        │
```

Recomputing every subscribed query on every write scales as
`writes × subscriptions × query`. Xyne-Sync makes the cost of a write
proportional to the number of **distinct predicates** on the written table and
the number that **actually match**, and then patches only the affected views.

---

## Status at a glance

| Area | Status | Where |
| --- | --- | --- |
| Typed query/row model, self-contained ops (`Delete` carries its image; replace = `Delete(old)` + `Add(new)`) | ✅ done | `src/model/` |
| DNF counting index: canonical disjuncts, shared epoch-stamped counters fire exactly | ✅ done | `src/ivm/index.rs` |
| Per-column value index: a write reaches only the conditions it satisfies, `O(columns · log n)` lookups (equality maps, inequality by set difference, range maps per comparison class) | ✅ done | `src/ivm/columns.rs` |
| Shared frames: one frame per table, rows tagged with holders as compact ids (`RowId`, `SubId`); held mirror; twin registration served from the frame through a query-keyed index | ✅ done | `src/ivm/frames.rs`, `src/ivm/registry.rs` |
| `ORDER BY` / `LIMIT` windows: doubled buffer, storage frontier, boundary condition in the index, eviction, refill | ✅ done | `src/ivm/window.rs` |
| In-place condition edits: a literal `IN` swapped inside its disjuncts, or a set-valued `IN` (`Value::Set`) gaining/losing one member in O(1) | ✅ done | `src/ivm/index.rs`, `src/ivm/registry.rs` |
| Join tree: `LEFT` and `RIGHT` edges at any depth, set-valued edges shared by identical subscriptions, cascades, self-joins, intersection on shared driven columns | ✅ done | `src/ivm/multi.rs` |
| SQL parser (single table, schema-aware, typed coercion, `i64` ids) | ✅ done | `src/parser/` |
| Asynchronous storage seam: the engine records the reads it needs (registration, join fetch, window refill) instead of running them; a read lands the moment it returns, its result first brought up to the engine, every landed row stamped with the read so the writes it already saw are recognized when they arrive; synchronous and asynchronous drivers | ✅ done | `src/ivm/engine.rs`, `src/sync/` |
| In-memory storage answering at once, honoring `ORDER BY` + `LIMIT`; commit-first test harness | ✅ done | `src/sync/storage.rs` |
| **PostgreSQL**: positioned snapshot reads in two methods (WAL: the exported snapshot of a rotating temporary replication slot, exact at its consistent point; XID: `pg_current_snapshot()` transaction ids), a `test_decoding` change-feed poller that positions every write, a minimal replication-protocol connection, live tests and a bench scenario against a real server | ✅ done | `src/sync/pg/` |
| Routing counters + benchmark harness | ✅ done | `src/ivm/stats.rs`, `src/bin/bench.rs` |
| `INNER` joins: visibility gate on the driven parent | ⏳ pending | paper §6.5 |
| **WebSocket protocol**: subscribe / unsubscribe / op stream, connection & subscription lifecycle | ⏳ pending | `src/ws.rs` is an axum echo base |
| Streaming `pgoutput` consumer (the poller consumes the slot by SQL today), batching of one write's narrowed reads | ⏳ pending | paper §7, §13 |
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
  `TableName` and `ColumnName` are newtypes; a subscription is addressed by
  `SubId`, a `u64` the engine hands out at registration and never reuses.
  The client's own subscription names (`QueryId`) live in the transport
  layer, which maps them to `SubId`s and back.
- `DataFrameKey` (primary-key values, identity only), `DataFrameRow` (a
  **full** row image, every column including the key), `DataFrameOperation`
  (`Add(key, row)` / `Delete(key, row)`).
- `SingleTableReadQuery { table, filter: Where, order_by, limit }`;
  `Where` is `AND`/`OR` over leaf `Condition`s; no `NOT` node, so DNF is plain
  distribution. `MultiTableReadQuery { main_table, left_joins }` today; the
  tree form is in the paper.
- Writes speak the same vocabulary: `InsertQuery`/`UpdateQuery` carry a key
  and a full image; `DeleteQuery` a key.

**Operation contract.** Every op is self-contained. Ops on the same key are
applied in stream order (the `Delete` of a replaced row precedes its `Add`; a
row admitted then evicted in one step has its `Add` before its `Delete`). Ops
on different rows commute; a client applies `Add` as insert-or-replace and
`Delete` as remove and converges.

### 2. Single-table engine (`src/ivm/`)

A write can affect a subscription two ways, and both are checked:

| new row matches | frame holds row | emitted |
| --- | --- | --- |
| yes | no | `Add` |
| yes | yes | `Delete(old)` + `Add(new)` |
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
  when its last holder leaves) → `SharedRow` with its `subscribers` as
  `SubId`s. Deletes and updates-out are found in O(1) off the tags.
  `held: SubId → RowIds` mirrors the tags so a subscription's view is
  enumerable without scanning the table; a (subscription, row) pair costs
  two small integers, not a copied key and a copied name.
- **Registration** (`registry.rs`). Hand out a `SubId`, index the DNF; serve
  the initial rows from storage or, for a structurally identical query found
  through a query-keyed index in one lookup, from the twin's rows with no
  storage query (`snapshots_shared`). `replace_condition` edits one leaf in
  place (stored filter + indexed disjuncts, splitting shared counters
  correctly). `replace_query` / `replace_condition` open a maintenance window
  during which the subscription is not a twin donor until the caller's
  `mark_reconciled`.
- **Windows** (`window.rs`). A finite `LIMIT L` keeps a buffer of `2L` rows
  (storage queried with the doubled limit) and a **frontier**: the worst order
  value known to be covered from storage (every matching row better than it
  is held). A full storage read sets the frontier to its worst value, a short
  one clears it (storage exhausted), an eviction pulls it in. The admission
  boundary (`col < frontier` for ASC, `>` for DESC, strict) is **published
  into the index's `boundaries` side table** and checked inside `matched()`;
  admission only; a held row that worsens keeps its slot until evicted. Past
  capacity the worst row is evicted (`Delete`); when a removal drains the
  buffer to `L`, one storage query refills from the frontier inclusive
  (held ties dedup). `NULL`/`NaN` sort largest; an unenforceable frontier
  publishes no boundary rather than rejecting everything; `LIMIT 0` is
  permanently empty.

### 3. Multi-table layer (`src/ivm/multi.rs`)

A multi-table query is a **tree**: every node a single-table query, every
edge a `LEFT` or `RIGHT` join with a column pair, and a child itself a full
multi-table query (`MultiTableReadQuery { main_table, left_joins, right_joins }`,
`Join { sub, main_table_column, sub_table_column }`). Each node registers as
one inner subscription, a *part* addressed by its path of join indices
(`QueryPart`, root = `[]`); inner ids are looked up in a map, never parsed.

Every edge has a **driver** side, whose rows decide which join values are
referenced, and a **driven** side, whose part carries `driven_col IN <set>`
inside its filter: `LEFT` keeps the parent, so the parent drives; `RIGHT`
keeps the child, so the child drives. The operand is a **shared set**
(`Value::Set`, compared by identity) owned by the tree, so the restriction
lives inside the driven filter and driven-table writes route natively, while
a change to the set never rewrites the filter or the index's counters. A node
driven on one column from both sides (a `LEFT` parent above, a `RIGHT` child
below, both on `id`) holds the **intersection** of the driving edges'
referenced values in one set.

Subscriptions that register an identical spec **share one tree**: one inner
part per node, one set of edges and counts, one crossing per event; each
part's operations are emitted once per subscriber, a later identical
registration is served from the shared parts, and the tree goes with its
last subscriber.

Per edge and per value the layer keeps `left` (driver rows carrying it) and
`right` (driven rows held); only `left` zero-crossings that change a set act:
`0→1` inserts the member, files the leaf under it in the column index (O(1)),
and fetches that value's driven rows in one narrowed query; `→0` removes it
and prunes held rows with no storage trip. The
rows a fetch brings in are arrivals at the driven node and a prune's rows are
departures, and the driven node may drive further edges, so the same handling
cascades through the tree. Registration is a post-order walk (right children,
node, left children); within a write every driven part is forwarded before its
driver, and a replace pair is diffed per edge so a kept value never churns
through zero. Updates arrive as `MultiTableUpdate { query, table, part, op }`.

`INNER` edges (a visibility gate on the driven parent) are specified in the
paper and pending.

### 4. Runtime and storage (`src/sync/`)

The engine never reads storage. Where it needs rows it does not hold, it
records a `Fetch` request (`src/ivm/engine.rs`) and carries on: a
registration's initial result set, a join edge's newly referenced value, a
drained window's refill. Until the request lands, the subscription routes
natively (its filter is indexed, the join leaf already holds the value), a
windowed subscription publishes no admission boundary, and it donates no twin
snapshot.

`Storage` is asynchronous: `select(query, at_least)` returns the rows of one
consistent snapshot **and the WAL location it reflects every commit up to**.
The engine sees nothing but locations (`Lsn`): every write carries its commit
location, every read its snapshot's, every frame row its image's. How a source
arrives at a read's location is its own business (below).

`Runtime` is the single owner of an engine and pure state (no I/O, no clock):
it hands the engine's requests to a driver, remembers the writes routed while
reads are out, and lands each result **the moment it returns**. If the engine
is ahead of the snapshot, the result is first brought up to the engine: a row
that a delivered write the snapshot does not reflect deleted or moved out of
the read's filter is dropped, one it rewrote takes the newer image. If the
snapshot is ahead of the feed, the frame absorbs the difference by currency:
every frame row remembers how current its image is (`RowAt`: the write that
produced it, or the read it landed from) and, per holder, the location of the
image that holder has (`Hold`). A landing inserts rows the frame
lacks stamped with the read and serves only the query that asked; for a row
the frame already holds, the reader gets the newer of the frame's image and
the read's, and when that is the read's it goes *ahead of the frame* on that
row (its `Hold` keeps the read's location and image) while the other
holders keep the frame's image and hear about the write when it arrives. That
write then leaves the reader alone; the first write past the reader's snapshot
ends its lead, its `Delete` carrying the image the reader actually held. When
a write arrives that the frame row itself already reflects, image and holders
are left alone and only the queries the frame's image newly matches are
tagged. No read ever waits for the feed and nothing stalls the stream. Two drivers share the runtime: `Local` (a storage that answers at
once, every read landed before the call returns; what the tests, demo and
bench use) and `Service` (a tokio command loop on one `LocalSet`, one task per
read). Each read is passed the stream position the runtime has seen, which the
WAL method uses to mint a fresh alias when the current one is older.

### 5. PostgreSQL (`src/sync/pg/`)

- `PgStorage::connect(dsn, catalog, mode)` runs each read in its own read-only
  `REPEATABLE READ` transaction, positioned by one of two methods.
  **WAL** (`SnapshotMode::Wal`): a background task mints an *alias* every
  250 ms (configurable), and on demand when a read finds it older than the
  engine's position, by opening a replication-protocol connection
  (`replication.rs`; tokio-postgres cannot) and creating a temporary logical
  slot with `EXPORT_SNAPSHOT`, which returns a snapshot name and the
  consistent point it was built at, exactly paired by Postgres. Each read's
  first statement is `SET TRANSACTION SNAPSHOT '<alias>'` and its location is
  the alias's consistent point; an alias lives as long as its connection,
  held until the last read that adopted it finishes. **XID**
  (`SnapshotMode::Xid(ledger)`): no temporary slot and no location asked of
  Postgres; the first statement returns `pg_current_snapshot()`
  (`xmin:xmax:xip`), and that account is converted through the `XidLedger`
  (`ledger.rs`) the poller fills with every delivered transaction's id and
  commit location: the read's location is just below the earliest delivered
  transaction the snapshot does not see, or the newest delivered location
  when it sees them all. Queries are rendered from the model with the
  catalog's types cast on the way out (`sql.rs`), every leaf wrapped in
  `IS TRUE` so `NULL` semantics match the engine's. One connection per read
  in flight, pooled.
- `PgStream::open(dsn, slot, catalog, ledger)` polls a `test_decoding` logical
  replication slot (`pg_logical_slot_get_changes`), turns each committed change
  into a full-image insert/update or a key-only delete positioned at its
  transaction's commit location, records every transaction in the ledger when
  one is attached, and ends every batch with a progress mark (the flush
  location read before consuming). A primary-key change becomes delete +
  insert; tables with large TOASTed columns need `REPLICA IDENTITY FULL`.
  `PgStream::run` feeds a `Service`'s command channel.
- Both methods are exact. The WAL method needs `max_replication_slots` and
  `max_wal_senders` headroom for the live aliases (two per storage instance at
  a rotation boundary) and delays a mint while a long transaction is open (the
  reads keep the current alias). The XID method needs neither, but positions
  a read no further than the feed has delivered.
- Live scenarios (`tests/pg_live.rs`) hold a snapshot open while writes commit
  behind it, in both modes, and run the async service end to end; they need
  `JUS_SYNC_PG_DSN` pointing at a database with `wal_level = logical` and
  otherwise report themselves skipped.

### 6. WebSocket API (*base only, pending*)

`src/ws.rs` is an axum server with `GET /health` and `GET /ws`; the connection
loop echoes. The protocol to wire in:

```text
client → server : {"subscribe": {"id": "q1", "sql": "SELECT * FROM tickets WHERE status = 'OPEN'"}}
server → client : {"snapshot": {"id": "q1", "ops": [{"table": "tickets", "part": "main", "op": {"Add": {...}}}, …]}}
server → client : {"update":   {"id": "q1", "table": "tickets", "part": "main", "op": {"Delete": {...}}}}
client → server : {"unsubscribe": {"id": "q1"}}
```

One connection carries many subscriptions; all subscriptions of all
connections live in the one engine instance. Per-connection subscription
tables, reconnect/catch-up, and back-pressure are part of this work item.

---

## Try it

```bash
cargo run --bin jus_sync      # scripted demo: SQL in, routed operations + cost counters out
cargo test                    # model, parser, routing, window, join and read/write interleaving scenarios
cargo run --release --bin bench   # routing / registration / window / join benchmarks
cargo run --bin server        # the WebSocket base (echo) on 127.0.0.1:8080

# against a real Postgres (wal_level = logical, a free replication slot):
export JUS_SYNC_PG_DSN=postgresql://postgres@localhost:5499/jus_sync
cargo test --test pg_live     # snapshot held open while writes commit behind it; async service end to end
cargo run --release --bin bench   # adds scenario 5: registration, streamed writes, registration under load
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
scenario 5 runs against PostgreSQL 15 on the same machine; raw output in
[paper/bench-2026-09-11.txt](paper/bench-2026-09-11.txt) and
[paper/bench-2026-09-12-row-currency.txt](paper/bench-2026-09-12-row-currency.txt),
analysis in paper §9):

| Scenario | Result |
| --- | --- |
| Routing, 100 → 10 000 subscriptions | a write touches only the 3 to 4 conditions it satisfies (one probe per column) while the table carries 67 to 85; route-only cost ≈ 1.3 µs fixed + 0.15 µs per impacted subscription; delivery ≈ 0.9 µs per emitted op |
| Registration, 100 → 10 000 subscriptions | 4.8 / 1.9 / 1.5 µs, flat: the twin lookup is one probe of the query-keyed index (17 µs at 10 000 with the earlier linear scan); peak memory of the whole run 0.53 GB (2.2 GB before shared join trees and compact ids) |
| Twin registration (400-row snapshot) | 752 µs from the shared frame vs 1 844 µs from (in-memory) storage; 1 000 twins hold 400 rows once |
| Window, `ORDER BY … LIMIT 50` over 100 000 rows | non-qualifying writes rejected inside the index at 0.5 µs (12 operations for 10 000 writes); under targeted writes the cost is the storage double's refill scans (12 × ~100 ms across 10 000 writes) |
| `LEFT JOIN`, 1 000 identical + 100 distinct subscriptions | identical subscriptions share one tree, so a ticket insert costs 62 µs for all 1 000 (7.4 ms before the shared set-valued edge), with 1.8 set edits per write instead of 52.5; the remaining cost is delivery, ≈ 0.3 to 0.4 µs per operation per subscriber |
| Over PostgreSQL (`LEFT JOIN`, 1 000 users, 2 000 tickets), WAL and XID methods (run taken in the machine's slower state, about 1.5× the rows above) | a registration costs its two reads and their landing and nothing else, 4.0 / 3.7 ms end to end (2.5 / 2.4 ms in storage, 1.2 ms in the runtime); 5 000 inserts committed in transactions of 100 stream at ≈ 7 000 writes/s from commit to delivery, split between the engine (43 µs per write for 1 001 subscribers) and 339 sequential narrowed reads (one per newly referenced user, ≈ 1 ms each), with polling and decoding at 16 ms in total; a registration whose snapshot is held open for 300 ms while 541 writes commit and are delivered behind it lands at once with 41 of its 148 rows brought up to the newer image, none dropped, and frames equal to the tables |

### Using the engine programmatically

```rust
use std::rc::Rc;
use jus_sync::ivm::SingleTableIVM;
use jus_sync::model::*;
use jus_sync::parser::{parse_read, parse_write, Catalog};
use jus_sync::sync::{Local, MemoryStorage};

let catalog = Catalog::new(vec![DbTable::new("tickets", ["id"], vec![
    DbColumn::new("id", ValueType::Int),
    DbColumn::new("status", ValueType::String),
])]);
let storage = Rc::new(MemoryStorage::new());
let mut ivm = Local::new(SingleTableIVM::new(), storage.clone());   // the synchronous driver

let query = parse_read("SELECT * FROM tickets WHERE status = 'OPEN'", &catalog).unwrap();
let (q_open, snapshot) = ivm.register_query(query);          // (SubId, Vec<SingleTableUpdate>), read landed inline

let write = parse_write("INSERT INTO tickets (id, status) VALUES (1, 'OPEN')", &catalog).unwrap();
storage.apply(&write);                                       // commit first …
let updates = ivm.incremental_update(&write);                // … notify second
// updates: Vec<SingleTableUpdate { query: SubId, table, op }>; q_open names ours
```

Against Postgres, build a `Service` over `MultiTableIVM` and a `PgStorage`,
run it on a `tokio::task::LocalSet`, and let `PgStream::run` feed its command
channel; `tests/pg_live.rs` does exactly that.

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
  TOASTed columns; paper §7.6).
- **`LIMIT L` is a doubled window**; **`ORDER BY` decides which rows a window
  keeps**, never the order operations arrive in.
- **Join semantics**: `LEFT` keeps every parent row (an empty child side is
  null); `RIGHT` keeps every child row and shows a parent row only while a
  child matches it. A finite limit below the root is normalized away.
- **NULL semantics**: any comparison touching `NULL` (or a missing column) is
  false, for every operator; three-valued logic collapsed to two.
- **Inserts are upserts**; **write literals are coerced** to declared column
  types (`1.0` into an `Int` column becomes `Int(1)`; out-of-range or
  mistyped values are rejected).
- **Single-threaded by design**: enforced at compile time (the index's
  shared counter handles are not `Send`). Multithreading is a later,
  deliberate step: shard by table.
- **DNF has no size cap** yet (exponential for adversarial filters; a cap with
  tree-evaluation fallback is deferred until a workload needs it).

---

## Layout

```text
Cargo.toml
paper/
  xyne-sync.tex / .pdf     the design paper (algorithms, join tree, ingestion, evaluation)
  bench-2026-09-11.txt     raw output of the benchmark run reported in the paper (scenarios 1 to 4)
  bench-2026-09-12-row-currency.txt      the run with the Postgres scenario (5)
  bench-2026-09-11-async-storage.txt     the earlier Postgres run, before reads landed at once
  bench-2026-09-07-before-frontier.txt   the run before the window frontier fix
  bench-2026-09-08-list-edges.txt        the run before join edges became shared sets
docs/
  pg-lsn-cdc-lab.md        hands-on lab: LSNs, MVCC snapshots, the CDC handoff
src/
  lib.rs                   crate docs, module map, roadmap
  main.rs                  demo binary: SQL in, routed operations + counters out
  model/
    value.rs               Value / ValueType, manual Eq+Hash (floats, maps)
    schema.rs              Catalog / DbTable / DbColumn, TableName + ColumnName newtypes
    query.rs               SingleTableReadQuery / WriteQuery, Where / Condition, SubId, QueryId
    frame.rs               DataFrameKey / DataFrameRow / DataFrameOperation + the shared TableFrame
    position.rs            Lsn, Snapshot, RowAt: where writes, reads and frame rows sit, as WAL locations
  ivm/
    mod.rs                 SingleTableIVM: the routing core (analyze + incremental_update)
    engine.rs              Fetch requests and the Engine trait the runtime drives
    registry.rs            subscription lifecycle: register/unregister/replace, twin sharing
    frames.rs              frame surgery + inspection: issue/land reads, adopt/upsert/remove rows, rows_for
    index.rs               TableIndex: shared DNF disjunct counters, boundaries, in-place edits
    columns.rs             per-column value index: equality, inequality and range lookups
    window.rs              ORDER BY / LIMIT: doubled buffer, boundary publishing, evict/refill
    multi.rs               MultiTableIVM: the join tree (LEFT / RIGHT edges) over the single engine
    predicate.rs           Where-tree evaluation, NULL semantics
    stats.rs               IvmStats counters + per-write diffing
  sync/
    storage.rs             the async Storage trait + MemoryStorage
    runtime.rs             Runtime: the single owner, bringing results up to the engine, SyncStats
    local.rs               Local: the synchronous driver
    service.rs             Service: the async command loop (tokio LocalSet)
    pg/mod.rs              PgStorage: positioned REPEATABLE READ snapshots, WAL (exported snapshot alias) or XID method
    pg/ledger.rs           XidLedger: delivered transactions' ids and commit locations, the XID method's converter
    pg/replication.rs      a minimal replication-protocol connection (mints the aliases)
    pg/sql.rs              model to SQL rendering
    pg/stream.rs           PgStream: test_decoding poller, positioned writes, progress marks
  parser/
    mod.rs                 lexer + recursive-descent parser, schema-aware against model::Catalog
  ws.rs                    axum setup: routes, WebSocket upgrade, connection loop (echo)
  bin/
    server.rs              the server binary
    bench.rs               benchmark harness
tests/
  ivm_scenarios.rs         single-table routing, windows, twin sharing (assertable spec)
  multi_table_scenarios.rs join reference/fetch/prune, self-join, nested and RIGHT edges
  sync_interleaving.rs     reads out while writes stream: merge, row currency, xid vs lsn, refill, post-order
  pg_live.rs               live Postgres: snapshot held open behind writes (both modes), async service
```

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
4. **No `IS NULL` / `LIKE` / `BETWEEN`** operators; with NULL comparisons
   always false, nullable columns cannot be filtered on.
5. **Strict identity vs loose predicates**: row identity is variant-exact
   (`Float(1.0)` and `Int(1)` are different keys) while predicates coerce; the
   parser closes this for SQL writes, the model API does not.
6. **No `Update` op variant**: a change is the `Delete`+`Add` pair; no
   operation carries an ordering position.
7. **Window ties and `NULL`s are pragmatic**, not SQL-exact (strict boundary,
   `NULL`/`NaN` largest, unenforceable boundary dropped).
8. **Twin lookup is a linear scan** over subscriptions; a query-keyed index
   must be maintained inside `replace_query` / `replace_condition` too.

---

## Contributing

- **Engine behavior** lives in `src/ivm/`; anything user-visible about *what
  matches* belongs in `predicate.rs` with a test alongside. Routing changes
  should keep `tests/` green and are expected to move the counters; say so.
- **Model changes** ripple everywhere (these types are index keys); discuss the
  design note you are addressing first.
- **Comment convention**: one `//!` block per file, `///` above every `fn` and
  type, no comments inside function bodies.
- Run `cargo test && cargo clippy --all-targets && cargo run --bin jus_sync`
  before pushing; the demo must end all-`PASS`.

## Building

Rust **1.88+** (edition 2024; the crate uses let-chains, stable since 1.88). The paper builds with
`tectonic paper/xyne-sync.tex`.
