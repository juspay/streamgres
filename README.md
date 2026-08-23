# Xyne-Sync

A sync engine that lets clients subscribe to **read queries over a WebSocket** and
keep receiving **incremental updates** to those queries as the underlying data changes.

Instead of polling a database or re-running a `SELECT` on every change, a client
subscribes once and thereafter receives only the *delta* — the rows that entered or
left its result set.

## The idea

```text
client                          Xyne-Sync                       data
  │                                │                              │
  ├─ subscribe(SELECT …) ─────────>│                              │
  │<─ initial result set ──────────┤                              │
  │                                │<──────── write (INSERT/ ─────┤
  │                                │          UPDATE/DELETE)      │
  │                                │                              │
  │                                ├─ which subscriptions does    │
  │                                │  this write affect?          │
  │                                ├─ what changed for each?      │
  │<─ [Add(row), Delete(key), …] ──┤                              │
```

Recomputing every subscribed query on every write does not scale, so the engine
maintains each subscription as an incrementally-updated materialized view —
**Incremental View Maintenance (IVM)**.

## How it works

The pipeline is `SQL text → query model → IVM routing`:

1. **[`parser`](src/parser/mod.rs)** turns SQL-shaped text into the engine's own
   query representation, validated against a table catalog. The engine never
   touches strings after this point — predicates must be inspectable (to index
   them) and comparable (to route writes).
2. **[`model`](src/model/)** is that representation: `ReadQuery` / `WriteQuery`,
   the `Where` tree of `Condition`s, dynamic `Value`s, and `DataFrame` (a
   materialized result set) with `DataFrameOperation` (`Add` / `Delete`, the
   unit shipped to a client). Schema lives once, in the `Catalog` — queries
   and records reference their table by name.
3. **[`ivm`](src/ivm/mod.rs)** holds the state and does the routing:

| Field | Type | Purpose |
| --- | --- | --- |
| `select_queries` | `uuid -> ReadQuery` | The registered subscriptions; the uuid is the client-facing handle. |
| `forward_index` | `uuid -> DataFrame` | Each subscription's own materialized result set. |
| `reverse_index` | `Condition -> Vec<uuid>` | Leaf predicate conditions with all their subscribers, probed against a write's row image to find candidate queries without scanning every subscription. |

A write can affect a subscription in exactly two ways, and `IVM` checks both:

1. **The new row matches the query** (insert / update-in / change-in-place) —
   found by probing the reverse index with the row image, then verifying each
   candidate's full `Where` tree. Condition-less queries (full-table
   subscriptions) are never in the index and become candidates directly.
2. **The query currently holds the row** (delete / update-out) — found by
   checking same-table frames for the row's key. A delete carries no column
   values, so predicate matching cannot find these.

From those two facts the operation follows:

| new row matches | frame holds row | emitted |
| --- | --- | --- |
| yes | no | `Add` — row enters the result set |
| yes | yes | `Add` — row refreshed in place |
| no | yes | `Delete` — row leaves |
| no | no | not impacted |

`incremental_update` emits `(uuid, operation)` pairs — exactly what the WebSocket
layer will push — and applies the same operations to its own frames, so engine
and client views stay in lockstep.

## Try it

```bash
cargo run    # scripted demo: SQL in, routed operations + cost counters out
cargo test   # predicate semantics, parser, and end-to-end routing scenarios
```

The demo registers six subscriptions on a `tickets` table and plays an
insert / insert / update / delete sequence through the engine. Per write it
prints expected-vs-found impacted subscriptions, the emitted operations, and the
routing cost:

```text
-- INSERT INTO tickets (id, status, priority, assigned_to, points) VALUES (1, 'OPEN', 'LOW', 'aniket', 3)
   expected : ["q-all", "q-mine-active", "q-open", "q-open-dup"]
   impacted : ["q-all", "q-mine-active", "q-open", "q-open-dup"]   [PASS]
   op       : q-all          <- Add(id=1)
   op       : q-mine-active  <- Add(id=1)
   op       : q-open         <- Add(id=1)
   op       : q-open-dup     <- Add(id=1)
   cost     : index 3/5 hit, membership 0/6 hit, 4 full evals, 9 condition evals, 4 impacted, ops +4/-0
   note     : one index probe of status = 'OPEN' routes to both of its subscribers
```

Every routing step is counted ([`IvmStats`](src/ivm/stats.rs)): index probes and
hits, membership probes and hits, full predicate evaluations, per-condition
evaluations, emitted operations. These counters are the yardstick for every
future optimization of the routing strategy — change the strategy, rerun the
demo, compare.

## v1 semantics and restrictions

Deliberate simplifications, each enforced with a loud error rather than silently
narrowed:

- **Single table per query.** Joins are recognised by the parser and refused.
- **Writes address one row by primary key.** `UPDATE`/`DELETE` require a
  `WHERE` that is a conjunction of `pkey = value` covering the whole key.
- **Updates carry the full row image.** `UPDATE` must `SET` every non-pkey
  column, because predicates are evaluated against the write's payload.
  Partial updates need a read-before-write against storage (roadmap step 3).
- **`ORDER BY` / `LIMIT` are parsed but not enforced.** Maintaining a LIMIT
  window incrementally needs storage access (when a row leaves the window, the
  next row must be fetched in) — also roadmap step 3.
- **Frames fill from writes seen after registration.** Each subscription owns
  its frame; `register_query` returns it as the (empty) starting snapshot.
  Serving the true initial result set from the database is roadmap step 3.
- **Write values are type-checked against the schema.** Literals are coerced
  to the column's declared type (`1.0` into an `Int` column becomes
  `Int(1)`); mismatches and `NULL` primary keys are rejected — row identity
  is variant-exact, so an uncoerced `Float(1.0)` key would silently miss the
  row stored under `Int(1)`.
- **Filter/list nesting is capped** (128 levels) so pathological input gets a
  `ParseError` instead of overflowing the stack.
- **NULL semantics:** any comparison touching `NULL` (or a missing column) is
  `false`, for every operator including `NEQ`/`NOT_IN` — SQL three-valued
  logic collapsed to two values.
- **Inserts are upserts:** inserting an existing key replaces the row.

## Design notes (open questions on the data structures)

Known consequences of the current shapes — kept as-is on purpose, documented so
they can be discussed rather than discovered:

1. **`Condition` carries no table**, so the same condition on two different
   tables shares one index entry and its subscriber list mixes tables.
   Verification keeps the *results* correct (a candidate is confirmed against
   its own table + full predicate), but an index shaped
   `table -> Condition -> subscribers` would probe less.
2. **`ReadQuery` has no projection** — every query is `SELECT *`. Fine for v1;
   a `columns` field changes what `DataFrameRow` holds per query.
3. **`limit: u32` and mandatory `order_by`** cannot express "no limit" /
   "unordered"; the parser fills in `u32::MAX` and pkey-ascending. `Option`
   on both would make the defaults explicit.
4. **`ComparisonOperator` has no `IS NULL` / `IS NOT NULL`** (and no
   `LIKE`/`BETWEEN`). With NULL comparisons always false, nullable columns
   currently cannot be filtered on at all.
5. **`Value::Float(f64)` and `Value` as a `HashMap` key** forced manual
   `Eq`/`Hash` (NaN, ±0.0, order-independent map hashing — see
   [`model/value.rs`](src/model/value.rs)). Floats-in-keys stays a recurring
   source of subtle bugs: key identity is variant-exact while predicates
   coerce Int/Float, so a *programmatic* write keyed `Float(1.0)` occupies a
   different frame slot than `Int(1)` even though every predicate treats
   them as equal. The parser closes this for the SQL path by coercing write
   values to declared column types; the model API itself does not.
6. **`InsertQuery`/`UpdateQuery` repeat `pkey_value` inside their `record`.**
   Harmless today (the parser fills both consistently), but one source of
   truth would be cleaner once writes can also be built programmatically.
7. **`DataFrameOperation` has no explicit `Update` variant** — `Add` doubles
   as "replace". Clients cannot distinguish a row entering from a row
   changing, and there is no ordering position for `ORDER BY` maintenance.

Resolved so far: the one-subscriber-per-condition reverse index (now
`HashMap<Condition, Vec<String>>`, pinned by
`duplicate_condition_routes_to_every_subscriber`); the `ReadQuery`-keyed
forward index (now keyed by subscription uuid, pinned by
`identical_queries_maintain_independent_frames` /
`late_identical_registration_starts_empty`); and embedded `DbTable` copies —
queries and records now reference tables by name against the one
authoritative `model::Catalog`, which resolves by name at every level —
`Catalog` maps table names to `DbTable`s, and each `DbTable` maps column
names to `DbColumn`s (each stamped with its owning table by `DbTable::new`).
`pkey: Vec<String>` keeps declaration order, making the default `ORDER BY`
genuinely "first declared pkey column" (pinned by
`default_order_uses_first_declared_pkey_column`).

## Roadmap

1. ✅ Query / value / dataframe model; IVM v1 with routing counters
2. ✅ SQL parser (single-table; joins and multi-row inserts refused for now)
3. Postgres connector via **Diesel** — initial result sets on registration,
   read-before-write for partial updates, `ORDER BY`/`LIMIT` enforcement,
   change feed
4. **WebSocket** subscription transport — register/unregister, push
   `(uuid, DataFrameOperation)` streams, reconnect/catch-up
5. Perf — per-table condition indexing (design note 1), range-friendly
   condition indexing, benchmarks driven by `IvmStats`

## Layout

```text
Cargo.toml
src/
  lib.rs              crate docs, module map, roadmap
  main.rs             demo binary: SQL in, routed operations + counters out
  model/
    mod.rs            re-exports; the model's vocabulary
    value.rs          Value / ValueType, manual Eq+Hash (floats, maps)
    schema.rs         Catalog / DbTable / DbColumn / DbRecord
    query.rs          ReadQuery / WriteQuery, Where / Condition, operators
    frame.rs          DataFrame / DataFrameKey / DataFrameRow / DataFrameOperation
  ivm/
    mod.rs            IVM: register_query, search_impacted_queries, incremental_update
    predicate.rs      Where-tree evaluation, NULL semantics
    stats.rs          IvmStats counters + per-write diffing
  parser/
    mod.rs            lexer + recursive-descent parser + Catalog (schema-aware)
tests/
  ivm_scenarios.rs    end-to-end routing scenarios (the assertable demo)
```

## Contributing

- **Engine behavior** lives in `src/ivm/`; anything user-visible about *what
  matches* belongs in `predicate.rs` and needs a test alongside the existing
  ones. Routing changes should keep `tests/ivm_scenarios.rs` green and are
  expected to move the counters — say so in the PR.
- **Model changes** ripple everywhere (these types are index keys); open an
  issue referencing the design note you are addressing before reshaping them.
- **Parser** restrictions are listed in its module docs; loosening one means
  removing its error, adding the semantics, and flipping the corresponding
  test in `v1_restrictions_are_rejected_loudly`.
- Run `cargo test && cargo run` before pushing; the demo must end all-`PASS`.

## Building

Requires a Rust toolchain with **edition 2024** support (Rust 1.85+).

### Editor setup

`.vscode/settings.json` pins `rust-analyzer.linkedProjects` to the root
`Cargo.toml`. If rust-analyzer reports a missing manifest, restart it via
*rust-analyzer: Restart Server*.
