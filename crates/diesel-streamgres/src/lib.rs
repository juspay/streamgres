//! A Diesel connection that sits between an application and Postgres and
//! serves reads from a local copy the Streamgres engine keeps fresh.
//!
//! `SyncedConnection<C>` wraps any Diesel Postgres connection `C` and
//! implements the same traits, so an application swaps its connection type
//! and nothing else changes: the query DSL, `sql_query`, transactions, pools
//! and `#[derive(Queryable)]` all go through unchanged.
//!
//! Every read Diesel performs reaches [`LoadConnection::load`]; every write
//! reaches [`Connection::execute_returning_count`]; raw multi-statement SQL
//! reaches [`SimpleConnection::batch_execute`]. Those three methods are the
//! whole interception surface, and the wrapper owns all of them. Writes
//! always go to Postgres. A read is answered from the [`cache::Cache`]
//! when all of these hold:
//!
//! - the read's route is [`Route::Streamgres`]: flagged so on the query
//!   ([`RouteDsl::via`]), or in a [`SyncedConnection::with_route`] scope,
//!   or by the connection's own route, in that order of precedence;
//! - no transaction is open on it (inside one, the application must see
//!   its own writes), and every write through the cache has reached it:
//!   by the engine's position when it reports one, else for the
//!   [`SyncedConnection::read_your_writes`] window after the last write
//!   (after one, a read must see what was written before the engine has
//!   shipped it);
//! - the statement is a `SELECT` (Diesel sends `INSERT ... RETURNING`
//!   through the same path);
//! - the engine accepted a subscription for the query
//!   ([`subscriptions::Subscriptions`]).
//!
//! On a miss the query is subscribed first. When the engine can serve
//! the rows itself (every projected column has a type the client encodes
//! exactly, see [`postgres`]), the result comes from the engine, is kept as
//! Diesel rows ([`materialized`]), and every later change replaces it in
//! place; Postgres is never read for that query. Otherwise the rows are
//! read from Postgres once and kept byte for byte ([`postgres::OwnedRow`]),
//! and when the engine reports the query touched they are dropped and the
//! next read refills them. Every other read goes to Postgres as before.
//!
//! [`LoadConnection::load`]: diesel::connection::LoadConnection::load
//! [`Connection::execute_returning_count`]: diesel::Connection::execute_returning_count
//! [`SimpleConnection::batch_execute`]: diesel::connection::SimpleConnection::batch_execute
//!
//! # Callbacks on changes
//!
//! The same engine calls back on a query's changes:
//! [`EngineClient::subscribe`](engine::EngineClient::subscribe) takes a
//! Diesel query and an async callback, and gives it each row present,
//! then each row added, updated or removed, per commit, whoever wrote it
//! ([`listen`]). These are signals that a result moved, which coalesce
//! and are not stored: a webhook sender keeps its own durable outbox, and
//! uses them to fill it or to act on it sooner.
//!
//! # Why the opt-in feature
//!
//! Diesel seals `Connection` behind the cargo feature
//! `i-implement-a-third-party-backend-and-opt-into-breaking-changes`.
//! Consumers that already wrap their connection enable it, so this crate
//! does too rather than reaching into Diesel's hidden derive re-exports.

#[cfg(feature = "async")]
compile_error!("this POC branch builds on juspay_diesel; upstream diesel-async cannot use its traits, so `async` is not available");

pub mod cache;
mod connection;
pub mod engine;
pub mod key;
pub mod listen;
pub mod materialized;
pub mod metrics;
pub mod postgres;
mod route;
pub mod rollout;
pub mod shadow;
#[cfg(feature = "sse")]
pub mod sse;
mod session;
pub mod subscriptions;

#[cfg(feature = "async")]
pub use crate::connection::asynchronous::{AsyncSyncedConnection, AsyncSyncedRow, PgRow};
pub use crate::connection::{
    Attachment, DEFAULT_READ_YOUR_WRITES, Settings, SyncedConnection, SyncedManager, install,
    read_your_writes_from_env,
};
pub use crate::listen::{
    Change, Ended, PENDING_LIMIT, SqlRow, SubscribeError, Subscription, WATCH_CAPACITY, Watch,
    WatchError,
};
pub use crate::route::{Route, RouteDsl, Served, Via};
/// The engine inside this process ([`engine::EngineClient::embedded`]):
/// its handle, its configuration and the SQL that sets a database up
/// for it, so an application depends on this crate alone.
#[cfg(feature = "embedded")]
pub use streamgres::embedded;
