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
//! | [`model`] | The data model: values, schema, queries, dataframes. The vocabulary everything else speaks. |
//! | [`ivm`] | The engine: query registration, write routing (`search_impacted_queries`), delta computation (`incremental_update`), and operation counters. |
//! | [`parser`] | SQL text → query model: schema-aware parsing against a [`parser::Catalog`]. |
//!
//! The demo binary (`src/main.rs`) runs a scripted scenario — SQL text in,
//! routed operations out — and prints, per write, which subscriptions were
//! found and what the routing cost.
//!
//! # Roadmap
//!
//! 1. ✅ Query/value/dataframe model, IVM v1 (single table)
//! 2. ✅ SQL parser producing [`model::ReadQuery`] / [`model::WriteQuery`]
//!    (single-table; see its module docs for the v1 restrictions)
//! 3. Postgres connector via Diesel — initial result sets, full row images
//!    for partial updates, `order_by`/`limit` enforcement
//! 4. WebSocket subscription transport (register / unregister / push ops)
//! 5. Performance: smarter reverse index, benchmarks against the counters

pub mod ivm;
pub mod model;
pub mod parser;
