//! The engine side of a cached read, as the cache sees it.
//!
//! The cache does not talk to the engine. It asks a [`Subscriptions`] to
//! register a query before it fills an entry for it, and tells it when the
//! entry is gone. Whoever owns the engine connection implements the trait
//! and, when the engine reports a query touched, calls
//! [`Cache::touched`](crate::cache::Cache::touched).
//!
//! The order matters for correctness: the subscription is registered
//! before Postgres is read, so a change that lands after the read is
//! reported, and one that lands between the two is reported redundantly,
//! which costs one refill and never a stale row.
//!
//! Subscribing is two steps because the second may need the database.
//! [`Subscriptions::subscribe`] sends the query and waits for the
//! engine. When the engine ships rows for tables whose column types are
//! not yet known, the answer is [`Outcome::NeedsTypes`], the caller
//! looks the types up on whatever connection it has (blocking or async)
//! and hands them to [`Subscriptions::materialize`], which builds the
//! store and gives the verdict. [`Subscriptions::subscribe_with`] does
//! both on a blocking connection.

use std::collections::HashMap;
use std::sync::Arc;

use crate::cache::Live;
use crate::key::QueryKey;
use crate::materialized::Snapshot;
use crate::row::OwnedRow;
use crate::types::{TableTypes, TypeLookup};

/// The engine's answer to a subscription.
#[derive(Debug, Clone)]
pub enum Verdict {
    /// The engine will report every change to this query's rows; the
    /// cache reads Postgres for them and refills on each report. A read
    /// from a standby must have replayed past `since`, the position of
    /// the last change the engine will not report, for its rows to be
    /// kept; `None` when the engine names none (one from before
    /// positions, or one with no position yet).
    Accepted { since: Option<u64> },
    /// The engine serves the rows itself: here is the result, built
    /// from `live`, which the cache keeps and builds the result from
    /// again on a read after each change. No Postgres read.
    Rows {
        rows: Arc<[OwnedRow]>,
        live: Arc<dyn Live>,
    },
    /// The engine cannot follow this query; the read goes to Postgres and
    /// is not cached. The reason is for logs and stats.
    Refused(String),
}

/// The refusal for a subscribe whose table's known column types lack a
/// column the engine names, or whose table was not in the catalog when
/// it was loaded: that read goes live, the catalog is loaded again on
/// the next miss, and that subscribe is served from rows. Transient, so
/// the cache does not remember it.
pub const STALE_TYPES: &str = "the column types are being reloaded";

/// The engine's answer to [`Subscriptions::subscribe`].
pub enum Outcome {
    Verdict(Verdict),
    /// The engine ships rows, and `missing` are the tables whose column
    /// types the engine side does not know yet. Look them up and call
    /// [`Subscriptions::materialize`].
    NeedsTypes {
        snapshot: Snapshot,
        missing: Vec<String>,
    },
}

/// What the cache needs from the engine.
pub trait Subscriptions: Send + Sync {
    /// Registers a query. Returns once the engine has it, since the cache
    /// reads Postgres only after that. Blocks the calling thread for up
    /// to the verdict timeout.
    fn subscribe(&self, key: &QueryKey) -> Outcome;

    /// Finishes an [`Outcome::NeedsTypes`]: `types` are the column types
    /// of the missing tables (a table left out falls the query back to
    /// refills). Pure computation, no I/O.
    fn materialize(
        &self,
        key: &QueryKey,
        snapshot: Snapshot,
        types: HashMap<String, Arc<TableTypes>>,
    ) -> Verdict;

    /// The cache dropped the entry for good (evicted for size or age, or
    /// abandoned by a read), so the engine need not follow the query any
    /// more.
    fn unsubscribe(&self, key: &QueryKey);

    /// Whether a refusal with this reason will hold for a while (the
    /// engine cannot follow the query's shape), so the cache may stop
    /// asking for a time, or is transient (the engine side was down or
    /// full, or did not answer) and the next read should ask again.
    fn refusal_is_final(&self, _reason: &str) -> bool {
        true
    }

    /// Whether the engine side is too far behind the database for the
    /// cache to be trusted: reads then go to Postgres until it catches up.
    fn behind(&self) -> bool {
        false
    }

    /// The WAL position up to which every change has reached the cache,
    /// for read-your-writes by position; `None` when the engine side
    /// cannot say, and the cache falls back to a time window.
    fn position(&self) -> Option<u64> {
        None
    }

    /// Both steps on a blocking connection: `lookup` is the connection
    /// the read is on. A lookup that fails stops the lookups; the engine
    /// side then reports the fallback for the types it lacks.
    fn subscribe_with(&self, key: &QueryKey, lookup: &mut dyn TypeLookup) -> Verdict {
        match self.subscribe(key) {
            Outcome::Verdict(verdict) => verdict,
            Outcome::NeedsTypes { snapshot, missing } => {
                let mut types = HashMap::new();
                for table in missing {
                    match lookup.table_types(&table) {
                        Ok(found) => {
                            types.insert(table, Arc::new(found));
                        }
                        Err(_) => break,
                    }
                }
                self.materialize(key, snapshot, types)
            }
        }
    }
}

/// No engine: refuses every query, so the connection is a plain
/// passthrough.
#[derive(Debug, Default, Clone, Copy)]
pub struct Unsynced;

impl Subscriptions for Unsynced {
    /// Refused: there is no engine.
    fn subscribe(&self, _: &QueryKey) -> Outcome {
        Outcome::Verdict(Verdict::Refused("no engine".to_string()))
    }

    /// Refused: there is no engine.
    fn materialize(
        &self,
        _: &QueryKey,
        _: Snapshot,
        _: HashMap<String, Arc<TableTypes>>,
    ) -> Verdict {
        Verdict::Refused("no engine".to_string())
    }

    /// Nothing to end.
    fn unsubscribe(&self, _: &QueryKey) {}
}
