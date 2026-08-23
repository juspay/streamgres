# JusSync

A sync engine that lets clients subscribe to **read queries over a WebSocket** and
keep receiving **incremental updates** to those queries as the underlying data changes.

Instead of polling a database or re-running a `SELECT` on every change, a client
subscribes once and thereafter receives only the *delta* — the rows that entered or
left its result set.

## The idea

```
client                          JusSync                         data
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

The interesting part is the last two steps. Recomputing every subscribed query on
every write does not scale, so the engine maintains subscribed queries as
incrementally-updated materialized views — **Incremental View Maintenance (IVM)**.

## How it works

The core is [`IVM`](src/ivm.rs), which holds two indexes:

| Index | Type | Purpose |
| --- | --- | --- |
| `forward_index` | `SelectQuery -> DataFrame` | The materialized result set currently held by each subscription. |
| `reverse_index` | `DataFrameKey -> Vec<SelectQuery>` | Which subscriptions currently contain a given row, so a write can be routed to them without scanning every query. |

On a write:

1. **`search_impacted_queries`** — use the `reverse_index` (plus the query predicates)
   to narrow the write down to the subscriptions it can actually affect.
2. **`incremental_update`** — for each impacted query, diff the change against the
   materialized `DataFrame` and emit a list of `DataFrameOperation`s
   (`Add(key, record)` / `Delete(key)`).
3. Apply those operations to the `forward_index` and push them to the subscriber.

A row changing can affect a subscription in more ways than "a value it displays
changed": an update can move a row *into* a result set, *out of* one, or reorder it.
Modelling the output as `Add`/`Delete` operations keyed by `DataFrameKey` covers all
three uniformly.

## Query model

[`src/data_structure.rs`](src/data_structure.rs) defines the query and value model the
engine operates on — deliberately its own representation rather than raw SQL strings,
so predicates can be inspected and compared:

- `Query` — `SELECT` / `UPDATE` / `DELETE` / `INSERT`
- `SelectQuery` — table, `Where`, `OrderBy`, `limit`
- `Where` — a tree of `Condition`s combined with `AND` / `OR`
- `Condition` — `column <op> value`, where the operator is one of
  `EQ NEQ GT GTE LT LTE IN NOT_IN`
- `Value` / `ValueType` — null, string, int, float, bool, date, datetime, list, map
- `Table` / `Column` / `Record` — schema and a single row
- `DataFrame` — `DataFrameKey -> DataFrameRecord`, a materialized result set
- `DataFrameOperation` — `Add` / `Delete`, the unit of an incremental update

## Status

Early scaffolding. The data model is sketched out; the engine is not implemented yet.

- [x] Query, value and dataframe types
- [x] `IVM` index layout
- [ ] `search_impacted_queries` — currently a `TODO`
- [ ] `incremental_update` — currently a `TODO`
- [ ] Predicate evaluation (does a record satisfy a `Where`?)
- [ ] `OrderBy` / `limit` handling in the incremental path
- [ ] WebSocket subscription transport
- [ ] Storage / change-feed integration

The crate **does not compile yet** — the two `TODO` functions have no bodies, and
some types still need their imports and dependencies wired up.

## Building

Requires a Rust toolchain with **edition 2024** support (Rust 1.85+; developed on 1.89).

```bash
cargo check
cargo run
```

### Editor setup

`.vscode/settings.json` pins `rust-analyzer.linkedProjects` to the root
`Cargo.toml`. The crate lives at the repository root — if rust-analyzer reports a
missing manifest, restart it via *rust-analyzer: Restart Server*.

## Layout

```
Cargo.toml
src/
  main.rs             entry point
  data_structure.rs   query, value and dataframe model
  ivm.rs              incremental view maintenance core
```
