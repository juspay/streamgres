//! The seam between an engine and the runtime that feeds it: the engine
//! never reads storage itself. Where it needs rows it does not hold (a
//! registration's initial result set, a join edge's newly referenced
//! value, a drained window's refill) it records a [`Fetch`] request and
//! carries on; the runtime runs the read, brings its result up to the
//! point the engine has reached, and lands the rest through
//! [`Engine::land`]. Positions never enter the engine: what lands is
//! current by construction. [`Engine`] is what the two engines
//! (single-table and join tree) expose to that runtime. An engine knows
//! subscriptions and nothing of the clients behind them: every delta it
//! emits names the subscriptions it applies to ([`super::Delta`]), and
//! whose they are is the transport's business.

use std::sync::Arc;

use super::Delta;
use crate::model::frame::SharedRow;
use crate::model::{DataFrameKey, DataFrameRow, SingleTableReadQuery, SubId, WriteQuery};

/// The engine's handle for one storage read it asked for; unique for the
/// life of the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FetchId(pub u64);

/// Why a read was asked for. Every kind runs the same way, in the
/// background while writes keep flowing; the subscription routes natively
/// meanwhile and the runtime brings the result up to the engine before it
/// lands.
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
///   bookkeeping treats as the requested row count. Shared, so the read
///   travels to the runtime, the driver and the storage's task without
///   the filter tree being copied.
#[derive(Debug, Clone, PartialEq)]
pub struct Fetch {
    pub id: FetchId,
    pub sub: SubId,
    pub kind: FetchKind,
    pub query: Arc<SingleTableReadQuery>,
}

/// What an engine exposes to the runtime: subscribe, route, land reads,
/// hand over the reads it wants run.
pub trait Engine {
    /// The subscription spec this engine registers.
    type Query;

    /// Register a subscription: its id, and whatever of its initial
    /// result set is available at once (a twin's rows; nothing when a read
    /// was requested instead).
    fn subscribe(&mut self, query: Self::Query) -> (SubId, Vec<Delta>);

    /// Remove a subscription; reads still in flight for it land as no-ops.
    fn unsubscribe(&mut self, sub: SubId);

    /// The read `fetch` will never be served: remove every subscription
    /// that was waiting on it (all of a shared tree's) and name them, so
    /// the owner of each can be told.
    fn refuse(&mut self, fetch: &Fetch) -> Vec<SubId>;

    /// The subscriptions whose first rows may complete when `fetch` lands
    /// (all of a shared tree's), so a consumer checks those and not every
    /// subscription still hydrating.
    fn waiting_on(&self, fetch: &Fetch) -> Vec<SubId>;

    /// The rows dropped since the last call (their last holder released
    /// them), handed out so the caller can free them off this thread.
    fn take_dead(&mut self) -> Vec<SharedRow>;

    /// The subscriptions one of whose pages has stopped reaching past the
    /// rows its join gate rejects since the last call (the page then holds
    /// fewer rows than asked for): queries to report by name. None for an
    /// engine without joins.
    fn take_capped(&mut self) -> Vec<SubId> {
        Vec::new()
    }

    /// What the engine holds right now: its subscriptions, its shared
    /// trees and the rows in its frames per table.
    fn footprint(&self) -> Footprint;

    /// Route one write to every subscription it affects, grouped per
    /// client.
    fn route(&mut self, write: &WriteQuery) -> Vec<Delta>;

    /// Land the rows a requested read returned, already brought up to the
    /// engine's position by the runtime: each row is adopted into the
    /// shared frame if the frame does not hold it and tagged for the
    /// reading subscription.
    /// `worst_read` is the worst row the read returned when it came back
    /// full, before it was brought up to date (`None` when it came back
    /// short): what a window's frontier is set from.
    fn land(
        &mut self,
        fetch: &Fetch,
        rows: &[(DataFrameKey, DataFrameRow)],
        worst_read: Option<&DataFrameRow>,
    ) -> Vec<Delta>;

    /// Take the reads recorded since the last call, in the order they were
    /// asked for.
    fn requests(&mut self) -> Vec<Fetch>;

    /// Whether every row of `sub`'s initial result has arrived: no read it
    /// waits on is still out and, for a tree, every part is live. False
    /// for a subscription the engine does not know.
    fn hydrated(&self, sub: SubId) -> bool;

    /// The engine's routing counters.
    fn stats(&self) -> &super::IvmStats;
}

/// What an engine holds: subscriptions (parts, for a multi-table
/// engine), shared trees, and the rows in the shared frames per table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Footprint {
    pub subscriptions: u64,
    pub trees: u64,
    pub rows_by_table: Vec<(String, u64)>,
}
