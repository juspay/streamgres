//! Running an engine against a real source: the storage reads the engine
//! asks for are asynchronous, the write stream keeps flowing while they
//! run, and this module reconciles the two.
//!
//! | Piece | Role |
//! |-------|------|
//! | [`Lsn`] | Where a write, a read's snapshot, or a frame row sits in the source's history: one WAL location, whatever the source (see `pg` for how the two Postgres methods arrive at it). |
//! | [`Storage`] | The asynchronous read seam every source implements: [`MemoryStorage`] in-process, [`pg::PgStorage`] over Postgres. |
//! | [`Runtime`] | The single owner of an engine: hands out the engine's reads, buffers the writes routed meanwhile, and lands each read by one rule (its module docs). Pure state, no I/O. |
//! | [`Local`] | The synchronous driver: reads answered at once, landed inline (tests, demo, benchmark). |
//! | [`Service`] | The asynchronous driver: a command loop on a `LocalSet`, one task per read. |
//! | [`pg`] | Postgres: snapshot-positioned selects (the WAL method's exported-snapshot alias, or the XID method's `pg_current_snapshot()` converted through the feed's xid ledger) and the change-feed poller that positions every write. |
//!
//! The sources differ only in how they arrive at a read's location; the
//! runtime, the engine's rules, and both drivers are shared.

pub mod pg;
mod local;
mod runtime;
mod service;
mod storage;

pub use crate::model::{Lsn, Snapshot};
pub use local::Local;
pub use runtime::{Runtime, Step, SyncStats};
pub use service::{Command, Service};
pub use storage::{MemoryStorage, Storage, StorageError};
pub use crate::model::SubId;
