//! A support desk whose reads go through an embedded [`Streamgres`] with
//! `load_and_sync`: a web service to show it and a test bed to check it.
//!
//! - [`service`]: the HTTP service and its page: the desk, every query
//!   shape compared with Postgres and inspected, and the lab's load tests
//!   and experiments.
//! - [`queries`]: the desk's reads, one per query shape
//!   ([`queries::Lookup`]).
//! - [`writer`]: a background writer making every kind of change.
//! - [`verify`], [`bench`](mod@bench), [`migrate`]: the command-line
//!   checks.
//! - [`schema`], [`models`], [`seed`], [`db`]: the tables, their rows, the
//!   dataset and the connections.
//!
//! [`Streamgres`]: streamgres_diesel::Streamgres

pub mod bench;
pub mod db;
pub mod migrate;
pub mod models;
pub mod queries;
pub mod schema;
pub mod seed;
pub mod service;
pub mod verify;
pub mod writer;
