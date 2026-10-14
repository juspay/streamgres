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

use std::sync::Arc;

use crate::key::QueryKey;
use crate::row::OwnedRow;
use crate::types::TypeLookup;

/// The engine's answer to a subscription.
#[derive(Debug, Clone)]
pub enum Verdict {
    /// The engine will report every change to this query's rows; the
    /// cache reads Postgres for them and refills on each report.
    Accepted,
    /// The engine serves the rows itself: here is the result, and every
    /// change will replace the cache's entry. No Postgres read.
    Rows(Arc<[OwnedRow]>),
    /// The engine cannot follow this query; the read goes to Postgres and
    /// is not cached. The reason is for logs and stats.
    Refused(String),
}

/// What the cache needs from the engine.
pub trait Subscriptions: Send + Sync {
    /// Registers a query. Returns once the engine has it, since the cache
    /// reads Postgres only after that. `lookup` is the connection the
    /// read is on, for the table's column types when rows are served.
    fn subscribe(&self, key: &QueryKey, lookup: &mut dyn TypeLookup) -> Verdict;

    /// The cache dropped the entry for good (evicted for size), so the
    /// engine need not follow the query any more.
    fn unsubscribe(&self, key: &QueryKey);
}

/// No engine: refuses every query, so the connection is a plain
/// passthrough.
#[derive(Debug, Default, Clone, Copy)]
pub struct Unsynced;

impl Subscriptions for Unsynced {
    fn subscribe(&self, _: &QueryKey, _: &mut dyn TypeLookup) -> Verdict {
        Verdict::Refused("no engine".to_string())
    }

    fn unsubscribe(&self, _: &QueryKey) {}
}
