//! The source traits: a snapshot at a position plus every change after it equals the current state.

use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::catalog::{Catalog, Row};
use crate::change::Txn;
use crate::error::{Result, SourceError};
use crate::query::Query;

/// An ordered point in a source's history, like a WAL position or an offset.
pub trait Position: Ord + Clone + Debug + Send + Sync + 'static {}

impl<T: Ord + Clone + Debug + Send + Sync + 'static> Position for T {}

/// Rows read from one point in time, and that point.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot<P> {
    pub rows: Vec<Row>,
    pub position: P,
}

/// Reads that each come from one point in time.
pub trait Snapshots: Send + Sync + 'static {
    type Position: Position;

    fn catalog(&self) -> Arc<Catalog>;

    /// Matching rows from one point in time, never ahead of `advance` nor below `floor`.
    fn select(
        &self,
        query: &Query,
    ) -> impl Future<Output = Result<Snapshot<Self::Position>>> + Send;

    /// The consumer has applied the stream up to `upto`.
    fn advance(&self, upto: &Self::Position);

    /// Oldest position a future read may return; consumers keep changes after it.
    fn floor(&self) -> Self::Position;

    /// Number of matching rows, stopping at `cap`; sources with a cheaper count override it.
    fn count(&self, query: &Query, cap: u64) -> impl Future<Output = Result<u64>> + Send {
        let query = Query {
            limit: usize::try_from(cap).ok(),
            order: vec![],
            ..query.clone()
        };
        async move { Ok(self.select(&query).await?.rows.len() as u64) }
    }
}

/// The stream of committed changes, in order.
pub trait Changes: Send + Sync + 'static {
    type Position: Position;

    /// Every commit after the start, once and in order; `Some(p)` resumes after `p` or fails.
    fn stream(
        &self,
        from: Option<&Self::Position>,
    ) -> impl Future<Output = Result<Feed<Self::Position>>> + Send;

    /// Everything up to `upto` is applied; the source may drop it.
    fn ack(&self, upto: &Self::Position);
}

/// Optional setup, start and stop hooks.
pub trait Lifecycle: Send + Sync + 'static {
    /// One-time setup, safe to repeat, may need admin rights (for example a publication).
    fn setup(&self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Runs at each process start: connect and create what the stream needs.
    fn open(&self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Stops the stream and releases server resources.
    fn close(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// A full source: reads and changes that share one position type.
pub trait Source:
    Snapshots + Changes<Position = <Self as Snapshots>::Position> + Lifecycle
{
}

impl<S> Source for S where S: Snapshots + Changes<Position = <S as Snapshots>::Position> + Lifecycle {}

/// The receiving end of a change stream.
pub struct Feed<P> {
    /// Changes at or before this position are not delivered.
    pub start: P,
    rx: mpsc::Receiver<Result<Txn<P>>>,
}

impl<P> Feed<P> {
    /// A new feed plus the sender the source writes into.
    pub fn channel(start: P, capacity: usize) -> (mpsc::Sender<Result<Txn<P>>>, Feed<P>) {
        let (tx, rx) = mpsc::channel(capacity);
        (tx, Feed { start, rx })
    }

    /// The next transaction, or `Closed` once the source ends the stream.
    pub async fn next(&mut self) -> Result<Txn<P>> {
        self.rx.recv().await.unwrap_or(Err(SourceError::Closed))
    }
}
