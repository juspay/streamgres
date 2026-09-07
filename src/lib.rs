//! # jus_sync
//!
//! A sync engine: clients subscribe to read queries over a WebSocket and
//! keep receiving incremental updates to those queries as the underlying
//! data changes — instead of polling, a client gets only the delta (the
//! rows that entered or left its result set).
//!
//! The core mechanism is **Incremental View Maintenance (IVM)**: every
//! subscription is a materialized view, every write is routed to exactly
//! the views it affects, and each affected view is patched with
//! [`model::DataFrameOperation`]s rather than recomputed.
//!
//! # Module map
//!
//! | Module | What lives there |
//! |--------|------------------|
//! | [`model`] | The data model: values, schema, queries, and the row/operation wire vocabulary. What everything else speaks. |
//! | [`ivm`] | The engine: query registration, write routing (`search_impacted_queries`), delta computation (`incremental_update`), operation counters — plus the multi-table LEFT JOIN layer ([`ivm::MultiTableIVM`]) and the [`ivm::Storage`] seam it fetches through. |
//! | [`parser`] | SQL text → query model: schema-aware parsing against a [`parser::Catalog`]. |
//! | [`ws`] | The axum server: routes, WebSocket upgrade, one connection loop (engine wiring pending). |
//!
//! The demo binary (`src/main.rs`) runs a scripted scenario — SQL text in,
//! routed operations out — and prints, per write, which subscriptions were
//! found and what the routing cost.
//!
//! # Roadmap
//!
//! 1. ✅ Query/value/dataframe model, IVM v1 (single table)
//! 2. ✅ SQL parser producing [`model::SingleTableReadQuery`] / [`model::WriteQuery`]
//!    (single-table; see its module docs for the v1 restrictions)
//! 3. ✅ Multi-table LEFT JOINs over the single-table engine
//!    ([`ivm::MultiTableIVM`]; programmatic specs — parser `JOIN` syntax
//!    still pending)
//! 4. ✅ `ORDER BY` / `LIMIT` windows, twin sharing, in-place condition
//!    edits, and the benchmark harness (`src/bin/bench.rs`)
//! 5. PostgreSQL ingester: one permanent logical replication slot for the
//!    change stream plus rotating exported snapshots for initial result
//!    sets (paper §7); a real [`ivm::Storage`] backend comes with it
//! 6. WebSocket subscription protocol (subscribe / unsubscribe / push ops)
//! 7. Join tree with `RIGHT` and `INNER` edges, parser `JOIN` syntax
//! 8. Performance: set-valued shared join edges, `(column, op, value)`
//!    condition indexing, interned ids; each measured against the counters

pub mod ivm;
pub mod model;
pub mod parser;
pub mod ws;
