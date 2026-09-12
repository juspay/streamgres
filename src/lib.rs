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
//! | [`ivm`] | The engine: query registration, write routing (`search_impacted_queries`), delta computation (`incremental_update`), operation counters — plus the multi-table join layer ([`ivm::MultiTableIVM`]) and the read requests ([`ivm::Fetch`]) it records instead of reading storage itself. |
//! | [`sync`] | Running an engine against a source: positions, the asynchronous [`sync::Storage`] seam, the single-owner [`sync::Runtime`] that lands reads against the write stream, the two drivers, and PostgreSQL ([`sync::pg`]). |
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
//! 5. ✅ Asynchronous storage and the runtime: the engine records reads,
//!    the runtime lands them against the write stream by one rule
//!    ([`sync::Runtime`]); PostgreSQL storage positioned by WAL location
//!    or transaction id and a `test_decoding` change-feed poller
//!    ([`sync::pg`]), with live tests and a bench scenario
//! 6. WebSocket subscription protocol (subscribe / unsubscribe / push ops)
//! 7. `INNER` edges, parser `JOIN` syntax
//! 8. Streaming `pgoutput` consumer, batching of one write's narrowed reads,
//!    table sharding

pub mod ivm;
pub mod model;
pub mod parser;
pub mod sync;
pub mod ws;
