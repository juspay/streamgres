//! The ticket desk on the embedded Streamgres cache.
//!
//! The same desk the S2S wrapper was tested with: five tables, a fixed
//! list of query shapes ([`queries::Lookup`]), a writer that makes every
//! kind of change, and a verify loop that reads every shape through the
//! cache and through Postgres while the writer runs. Here the cache runs
//! in this process ([`streamgres_diesel::Cache`]) and every read goes
//! through `load_cached`.

pub mod bench;
pub mod lag;
pub mod migrate;
pub mod models;
pub mod queries;
pub mod schema;
pub mod seed;
pub mod stack;
pub mod verify;
pub mod writer;

pub mod asynchronous;
pub mod service;
