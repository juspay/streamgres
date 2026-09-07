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
| DNF counting index: each distinct condition evaluated once per write, shared epoch-stamped counters fire exactly | ✅ done | `src/ivm/index.rs` |
| Shared frames: one frame per table, rows tagged with holders; held-key mirror; twin registration served from the frame | ✅ done | `src/ivm/frames.rs`, `src/ivm/registry.rs` |
| `ORDER BY` / `LIMIT` windows: doubled buffer, storage frontier, boundary condition in the index, eviction, refill | ✅ done | `src/ivm/window.rs` |
| In-place condition edit (a join's `IN` list gains/loses a value without re-registration) | ✅ done | `src/ivm/index.rs`, `src/ivm/registry.rs` |
| `LEFT JOIN`, one level: a root with any number of left-joined leaf tables, per-value left/right counts, self-joins | ✅ done | `src/ivm/multi.rs` |
| SQL parser (single table, schema-aware, typed coercion, `i64` ids) | ✅ done | `src/parser/` |
| In-memory storage double honoring `ORDER BY` + `LIMIT`; commit-first test harness | ✅ done | `src/ivm/storage.rs` |
| Routing counters + benchmark harness | ✅ done | `src/ivm/stats.rs`, `src/bin/bench.rs` |
| **Join tree**: nested multi-table children, `RIGHT` and `INNER` joins, visibility propagation | ⏳ pending | paper §6; one level of `LEFT` today |
| **PostgreSQL ingester**: permanent `pgoutput` slot + rotating exported snapshots + per-subscription catch-up | ⏳ pending | paper §7; `PgStorage` is a stub |
| **WebSocket protocol**: subscribe / unsubscribe / op stream, connection & subscription lifecycle | ⏳ pending | `src/ws.rs` is an axum echo base |
| `O(columns · log n)` condition matching (per-column hash maps + interval search) | ⏳ pending | paper §8.2 |
| Set-valued join edge (the `IN` list as a per-column set, one edge shared by identical multi-table subscriptions), the measured slow path | ⏳ pending | paper §9.5, §12.1 |
| Query-keyed twin index; interned subscription ids and compact keys in tags / held index | ⏳ pending | paper §9.3, §9.6 |
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
  floats and maps). `TableName`, `ColumnName`, `QueryId` are newtypes so the
  three kinds of string can never be confused.
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

- **Way 1, counting** (`index.rs`). At registration a filter is normalized to
  DNF. Per table, `by_condition: Condition → [shared counter]` and
  `by_disjunct: Disjunct → counter`; identical disjunct shapes share one
  counter across subscriptions. Routing evaluates each distinct condition
  **once**, bumps the counters of matching ones, and fires a disjunct when its
  counter reaches its size. Counters are epoch-stamped per write, no reset
  sweep. Cost: `O(distinct conditions) + O(matching links)`, independent of
  subscription count.
- **Way 2, membership** (`frames.rs`). One shared `TableFrame` per table;
  each `SharedRow` carries its `subscribers`. Deletes and updates-out are
  found in O(1) off the tags. `held: QueryId → keys` mirrors the tags so a
  subscription's view is enumerable without scanning the table.
- **Registration** (`registry.rs`). Index the DNF; serve the initial rows from
  storage or, for a structurally identical query, from the twin's rows with
  no storage query (`snapshots_shared`). `replace_condition` edits one leaf in
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

A multi-table query registers one inner subscription per part
(`{uuid}_0` main, `{uuid}_k` per join). Each sub part is registered as
`sub WHERE AND sub_col IN (referenced values)`: **the join condition lives
inside the inner filter**, so sub-table writes route natively. Per join value
the layer keeps `left` (main rows carrying it) and `right` (sub rows held);
only left zero-crossings act: `0→1` edits the `IN` list in place and fetches
that value's sub rows in one narrowed query; `→0` edits it and prunes held rows
with no storage trip. Sub-part ops are forwarded before main-part ops (the
self-join rule); a main-row replace pair is recognized and its join values
diffed so a kept value never churns through zero. Updates arrive as
`MultiTableUpdate { query, table, part, op }`.

The paper generalizes this to a **tree**: every node a single-table query,
every edge `LEFT` / `RIGHT` / `INNER` with a column pair, referenced values
flowing down driver→driven edges and visibility flowing up `INNER` edges.
Only the one-level `LEFT` form is implemented.

### 4. Storage seam (`src/ivm/storage.rs`)

`trait Storage { fn select(&self, &SingleTableReadQuery) -> Vec<(DataFrameKey, DataFrameRow)> }`.
`MemoryStorage` implements it for tests (honoring order + limit when the limit
is finite; writes mirrored via `apply` **before** routing: commit first,
notify second). `PgStorage` is a stub until the connector lands.

### 5. PostgreSQL ingestion (*designed, pending*)

- One **permanent logical replication slot** (`pgoutput`) created at service
  start; its commit-LSN-ordered stream is the engine's write input, buffered in
  memory keyed by LSN; the highest applied LSN is the watermark.
- Every ~100 ms a **temporary slot with `SNAPSHOT 'export'`** mints the
  current *alias* `(snapshot_name, consistent_lsn)`.
- A new subscription runs its initial `SELECT`s under
  `SET TRANSACTION SNAPSHOT <alias>` on the server that exported it (a snapshot
  cannot be imported on another server; on a replica the alias is a replay LSN
  taken before and after the transaction, retried until equal), registers with
  those rows, and then the buffered changes with `lsn > consistent_lsn` are
  replayed to **that subscription alone** (a `catch_up(uuid, write)` entry
  point, pending). The snapshot and the stream meet at one LSN.
- Rotation swaps snapshot + LSN atomically under a lock; each alias keeps its
  replication connection open until the last registration using it finishes
  (closing it drops the temporary slot); the buffer is trimmed below the oldest
  alias still in use.
- Logical decoding delivers new images for inserts/updates and the key for
  deletes; with `REPLICA IDENTITY FULL` the ingester completes an update's
  unchanged TOASTed columns from the old tuple, giving exactly the engine's
  write model. See
  [`docs/pg-lsn-cdc-lab.md`](docs/pg-lsn-cdc-lab.md) for the hands-on lab that
  established these facts (exported snapshots, xid vs commit order, the
  replica `L1 = L2` sandwich).

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
cargo test                    # model, parser, routing, window and join scenarios
cargo run --release --bin bench   # routing / registration / window / join benchmarks
cargo run --bin server        # the WebSocket base (echo) on 127.0.0.1:8080
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
scenario 1 registers against the empty `PgStorage` stub so frames fill from
writes alone; raw output in
[paper/bench-2026-09-07.txt](paper/bench-2026-09-07.txt), analysis in paper §9):

| Scenario | Result |
| --- | --- |
| Routing, 100 → 10 000 subscriptions | conditions evaluated per write saturate at the vocabulary size (67 at N = 100, 85 from N = 1 000); route-only cost ≈ 2 µs fixed + 0.35 µs per impacted subscription; delivery ≈ 1.2 µs per emitted op |
| Twin registration (400-row snapshot) | 953 µs from the shared frame vs 1 968 µs from (in-memory) storage; 1 000 twins hold 400 rows once |
| Window, `ORDER BY … LIMIT 50` over 100 000 rows | non-qualifying writes rejected inside the index at 0.5 µs (12 operations for 10 000 writes); under targeted writes the cost is the storage double's refill scans (12 × ~90 ms across 10 000 writes) |
| `LEFT JOIN`, 1 000 identical + 100 distinct subscriptions | user updates are pure fan-out (2.6 µs per op); ticket inserts cost 10 ms because every twin edits its `O(\|IN\|)` join edge at each zero crossing, the slow path the set-valued shared edge removes |

### Using the engine programmatically

```rust
use std::rc::Rc;
use jus_sync::ivm::{MemoryStorage, SingleTableIVM, Storage};
use jus_sync::model::*;
use jus_sync::parser::{parse_read, parse_write, Catalog};

let catalog = Catalog::new(vec![DbTable::new("tickets", ["id"], vec![
    DbColumn::new("id", ValueType::Int),
    DbColumn::new("status", ValueType::String),
])]);
let storage = Rc::new(MemoryStorage::new());
let mut ivm = SingleTableIVM::new(storage.clone() as Rc<dyn Storage>);

let query = parse_read("SELECT * FROM tickets WHERE status = 'OPEN'", &catalog).unwrap();
let snapshot = ivm.register_query("q-open", query, None);     // Vec<DataFrameOperation>

let write = parse_write("INSERT INTO tickets (id, status) VALUES (1, 'OPEN')", &catalog).unwrap();
storage.apply(&write);                                       // commit first …
let updates = ivm.incremental_update(&write);                // … notify second
// updates: Vec<SingleTableUpdate { query, table, op }>
```

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
- **`LEFT JOIN`** semantics: main rows visible purely by the main `WHERE`; an
  empty sub side is null. A finite limit on a join's sub side is normalized
  away.
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
  bench-2026-09-07.txt     raw output of the benchmark run reported in the paper
  bench-2026-09-07-before-frontier.txt   the same run before the window frontier fix
docs/
  pg-lsn-cdc-lab.md        hands-on lab: LSNs, MVCC snapshots, the CDC handoff
src/
  lib.rs                   crate docs, module map, roadmap
  main.rs                  demo binary: SQL in, routed operations + counters out
  model/
    value.rs               Value / ValueType, manual Eq+Hash (floats, maps)
    schema.rs              Catalog / DbTable / DbColumn, TableName + ColumnName newtypes
    query.rs               SingleTableReadQuery / WriteQuery, Where / Condition, QueryId
    frame.rs               DataFrameKey / DataFrameRow / DataFrameOperation + the shared TableFrame
  ivm/
    mod.rs                 SingleTableIVM: the routing core (analyze + incremental_update)
    registry.rs            subscription lifecycle: register/unregister/replace, twin sharing
    frames.rs              frame surgery + inspection: fetch/upsert/remove rows, rows_for
    index.rs               TableIndex: shared DNF disjunct counters, boundaries, in-place edits
    window.rs              ORDER BY / LIMIT: doubled buffer, boundary publishing, evict/refill
    multi.rs               MultiTableIVM: LEFT JOIN maintenance over the single engine
    storage.rs             Storage seam: PgStorage stub + MemoryStorage
    predicate.rs           Where-tree evaluation, NULL semantics
    stats.rs               IvmStats counters + per-write diffing
  parser/
    mod.rs                 lexer + recursive-descent parser, schema-aware against model::Catalog
  ws.rs                    axum setup: routes, WebSocket upgrade, connection loop (echo)
  bin/
    server.rs              the server binary
    bench.rs               benchmark harness
tests/
  ivm_scenarios.rs         single-table routing, windows, twin sharing (assertable spec)
  multi_table_scenarios.rs left-join reference/fetch/prune, self-join, sub-limit scenarios
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
