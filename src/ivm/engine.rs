//! The seam between an engine and the runtime that feeds it: the engine
//! never reads storage itself. Where it needs rows it does not hold (a
//! registration's initial result set, a join edge's newly referenced
//! value, a drained window's refill) it records a [`Fetch`] request and
//! carries on; the runtime runs the read, brings its result up to the
//! point the engine has reached, and lands the rest through
//! [`Engine::land`]. Positions never enter the engine: what lands is
//! current by construction. [`Engine`] is what the two engines
//! (single-table and join tree) expose to that runtime, and every delta
//! they emit is addressed to a client ([`super::ClientUpdate`]).

use super::ClientUpdate;
use crate::model::{ClientId, DataFrameKey, DataFrameRow, SingleTableReadQuery, SubId, WriteQuery};

/// The engine's handle for one storage read it asked for; unique for the
/// life of the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FetchId(pub u64);

/// Why a read was asked for.
///
/// - `Snapshot`: a registration's initial result set; run in the
///   background while writes keep flowing.
/// - `Narrowed`: the subscription's filter narrowed to one join value a
///   driving edge started referencing; run before the next write.
/// - `Refill`: a window drained to its limit, read again from its
///   frontier; run before the next write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchKind {
    Snapshot,
    Narrowed,
    Refill,
}

impl FetchKind {
    /// Whether the driver must finish this read before routing anything
    /// else: a read asked for in the middle of maintaining a subscription
    /// (a join crossing, a refill) lands before the next write.
    pub fn is_blocking(self) -> bool {
        !matches!(self, FetchKind::Snapshot)
    }
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

    /// Register a subscription for `client`: its id, and whatever of its
    /// initial result set is available at once (a twin's rows; nothing
    /// when a read was requested instead).
    fn subscribe(&mut self, client: ClientId, query: Self::Query) -> (SubId, Vec<ClientUpdate>);

    /// Remove a subscription; reads still in flight for it land as no-ops.
    fn unsubscribe(&mut self, sub: SubId);

    /// Remove every subscription of `client` (it disconnected).
    fn unsubscribe_client(&mut self, client: ClientId);

    /// Route one write to every subscription it affects, grouped per
    /// client.
    fn route(&mut self, write: &WriteQuery) -> Vec<ClientUpdate>;

    /// Land the rows a requested read returned, already brought up to the
    /// engine's position by the runtime: each row is adopted into the
    /// shared frame if the frame does not hold it and tagged for the
    /// reading subscription.
    fn land(&mut self, fetch: &Fetch, rows: &[(DataFrameKey, DataFrameRow)]) -> Vec<ClientUpdate>;

    /// Take the reads recorded since the last call, in the order they were
    /// asked for.
    fn requests(&mut self) -> Vec<Fetch>;
}
