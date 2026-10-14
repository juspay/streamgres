//! A ticket desk on the Streamgres Diesel wrapper.
//!
//! Two things at once. For someone who found Streamgres: a backend
//! whose reads come from the engine's cache, runnable with one compose
//! file, with an HTTP API to curl and a stats page to watch. For the
//! people working on the wrapper: a fixed list of query shapes
//! ([`queries::Lookup`]) that the scenario test drives through both
//! wrappers and both routes, and the bench times.
//!
//! The modules, in the order to read them:
//!
//! - [`schema`] and [`models`]: five tables and the rows they yield.
//! - [`queries`]: the reads, each a named shape, runnable sync or async.
//! - [`seed`]: the deterministic dataset.
//! - [`pools`]: the connections and how each is routed.
//! - [`writer`]: the background changes that keep the demo moving.
//! - [`api`]: the HTTP face, including `/stats`.
//! - [`bench`]: latency per route, in process.

pub mod api;
pub mod bench;
pub mod models;
pub mod pools;
pub mod queries;
pub mod schema;
pub mod seed;
pub mod writer;
