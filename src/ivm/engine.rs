//! The seam between an engine and the runtime that feeds it: the engine
//! never reads storage itself. Where it needs rows it does not hold (a
//! registration's initial result set, a join edge's newly referenced
//! value, a drained window's refill) it records a [`Fetch`] request and
//! carries on; the runtime runs the read, drops from its result the rows
//! the stream has since removed, and lands the rest through
//! [`Engine::land`] together with the position the read saw. [`Engine`]
//! is what the two engines (single-table and join tree) expose to that
//! runtime.

use crate::model::{DataFrameKey, DataFrameRow, Lsn, SingleTableReadQuery, SubId, WriteQuery};

/// The engine's handle for one storage read it asked for; unique for the
/// life of the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FetchId(pub u64);

/// Why a read was asked for.
///
/// - `Snapshot`: a registration's initial result set.
/// - `Narrowed`: the subscription's filter narrowed to one join value a
///   driving edge started referencing.
/// - `Refill`: a window drained to its limit, read again from its
///   frontier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchKind {
    Snapshot,
    Narrowed,
    Refill,
}

/// One storage read an engine wants run on its behalf.
///
/// - `id`: the engine's handle for it.
/// - `sub`: the (inner) subscription whose rows the read feeds.
/// - `kind`: why it was asked for.
/// - `query`: exactly what to run; its `limit` is what the window
///   bookkeeping treats as the requested row count.
#[derive(Debug, Clone, PartialEq)]
pub struct Fetch {
    pub id: FetchId,
    pub sub: SubId,
    pub kind: FetchKind,
    pub query: SingleTableReadQuery,
}

/// What an engine exposes to the runtime: subscribe, route, land reads,
/// hand over the reads it wants run.
pub trait Engine {
    /// The subscription spec this engine registers.
    type Query;
    /// The per-subscription delta this engine emits.
    type Update;

    /// Register a subscription: its id, and whatever of its initial result
    /// set is available at once (a twin's rows; nothing when a read was
    /// requested instead).
    fn subscribe(&mut self, query: Self::Query) -> (SubId, Vec<Self::Update>);

    /// Remove a subscription; reads still in flight for it land as no-ops.
    fn unsubscribe(&mut self, sub: SubId);

    /// Route one write, committed at `at`, to every subscription it
    /// affects.
    fn route(&mut self, write: &WriteQuery, at: Lsn) -> Vec<Self::Update>;

    /// Land the rows a requested read returned, whose snapshot reflects
    /// every commit up to `at`, after the runtime has dropped the rows the
    /// stream removed since that snapshot; the engine merges each
    /// remaining row against what its frame holds and serves the reading
    /// subscription.
    fn land(&mut self, fetch: &Fetch, rows: &[(DataFrameKey, DataFrameRow)], at: Lsn) -> Vec<Self::Update>;

    /// Take the reads recorded since the last call, in the order they were
    /// asked for.
    fn requests(&mut self) -> Vec<Fetch>;
}
