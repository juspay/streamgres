//! A support desk whose reads go through the embedded Streamgres cache
//! ([`streamgres_diesel::Cache`]) with `load_cached`: a web service to show
//! it and a test bed to check it.
//!
//! - [`service`]: the HTTP service and its page: the desk, every query
//!   shape compared with Postgres, and the lab's load tests and failure
//!   experiments.
//! - [`queries`]: the desk's reads, one per query shape ([`queries::Lookup`]),
//!   on the sync adapter and, in [`asynchronous`], the async one.
//! - [`writer`]: a background writer making every kind of change.
//! - [`verify`], [`bench`](mod@bench), [`lag`], [`migrate`]: the command-line checks.
//! - [`schema`], [`models`], [`seed`], [`stack`]: the tables, their rows,
//!   the dataset and the cache's setup.

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
