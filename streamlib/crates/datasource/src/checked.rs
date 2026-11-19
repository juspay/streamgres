//! Checks every answer a source gives against the contract rules.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::catalog::{Catalog, Table};
use crate::change::{Change, Txn};
use crate::error::{Result, SourceError};
use crate::query::{Query, Truth};
use crate::source::{Changes, Feed, Lifecycle, Snapshot, Snapshots, Source};

/// Wraps a source and turns any broken rule into a `Contract` error.
pub struct Checked<S: Source> {
    inner: Arc<S>,
    state: Mutex<State<<S as Snapshots>::Position>>,
}

struct State<P> {
    advanced: Option<P>,
    start: Option<P>,
}

impl<S: Source> Checked<S> {
    pub fn new(inner: S) -> Self {
        Checked {
            inner: Arc::new(inner),
            state: Mutex::new(State {
                advanced: None,
                start: None,
            }),
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }
}

fn table<'a>(catalog: &'a Catalog, name: &str) -> Result<&'a Arc<Table>> {
    catalog
        .table(name)
        .ok_or_else(|| SourceError::contract(format!("unknown table `{name}`")))
}

fn check_snapshot<P: Ord + std::fmt::Debug>(
    catalog: &Catalog,
    query: &Query,
    snap: &Snapshot<P>,
    floor: &P,
    state: &State<P>,
) -> Result<()> {
    let bad = |why: String| Err(SourceError::contract(why));
    let p = &snap.position;
    match &state.advanced {
        None => return bad(format!("snapshot at {p:?} before any advance")),
        Some(adv) if p > adv => return bad(format!("snapshot at {p:?} ahead of advance {adv:?}")),
        _ => {}
    }
    if state.start.as_ref().is_some_and(|s| p < s) {
        return bad(format!("snapshot at {p:?} before feed start"));
    }
    if p < floor {
        return bad(format!("snapshot at {p:?} below floor {floor:?}"));
    }
    let table = table(catalog, &query.table)?;
    let mut keys = HashSet::new();
    for row in &snap.rows {
        if !table.is_complete(row) {
            return bad(format!("incomplete row in snapshot of `{}`", table.name));
        }
        if !keys.insert(table.key_of(row)) {
            return bad(format!("duplicate key in snapshot of `{}`", table.name));
        }
        if query.filter.eval(row) != Truth::True {
            return bad(format!("row not matching the filter in `{}`", table.name));
        }
    }
    if let Some(limit) = query.limit {
        if snap.rows.len() > limit {
            return bad(format!("{} rows over limit {limit}", snap.rows.len()));
        }
        let mut sorted = snap.rows.clone();
        query.sort(&mut sorted);
        if sorted
            .iter()
            .zip(&snap.rows)
            .any(|(a, b)| query.order.iter().any(|(c, _)| a.get(c) != b.get(c)))
        {
            return bad("limited rows not in query order".into());
        }
    }
    Ok(())
}

fn check_change(catalog: &Catalog, change: &Change) -> Result<()> {
    let table = table(catalog, change.table())?;
    let bad = |why: &str| Err(SourceError::contract(format!("{why} on `{}`", table.name)));
    let key_len = table.key.len();
    match change {
        Change::Insert { row, .. } => {
            if !table.is_complete(row) {
                return bad("partial insert");
            }
        }
        Change::Update { row, old_key, .. } => {
            if table.key_of(row).is_none() {
                return bad("update without key columns");
            }
            if !table.undeclared(row).is_empty() {
                return bad("update with undeclared columns");
            }
            if old_key.as_ref().is_some_and(|k| k.len() != key_len) {
                return bad("bad old key");
            }
        }
        Change::Delete { key, .. } => {
            if key.len() != key_len {
                return bad("bad delete key");
            }
        }
        Change::Truncate { .. } => {}
    }
    Ok(())
}

fn check_txn<P: Ord + std::fmt::Debug>(catalog: &Catalog, last: &P, txn: &Txn<P>) -> Result<()> {
    if txn.position <= *last {
        return Err(SourceError::contract(format!(
            "txn at {:?} not after {last:?}",
            txn.position
        )));
    }
    txn.changes
        .iter()
        .try_for_each(|c| check_change(catalog, c))
}

impl<S: Source> Snapshots for Checked<S> {
    type Position = <S as Snapshots>::Position;

    fn catalog(&self) -> Arc<Catalog> {
        self.inner.catalog()
    }

    async fn select(&self, query: &Query) -> Result<Snapshot<Self::Position>> {
        let floor = self.inner.floor();
        let snap = self.inner.select(query).await?;
        let state = self.state.lock().unwrap();
        check_snapshot(&self.inner.catalog(), query, &snap, &floor, &state)?;
        Ok(snap)
    }

    fn advance(&self, upto: &Self::Position) {
        let mut state = self.state.lock().unwrap();
        if state.advanced.as_ref().is_none_or(|a| upto > a) {
            state.advanced = Some(upto.clone());
        }
        drop(state);
        self.inner.advance(upto);
    }

    fn floor(&self) -> Self::Position {
        self.inner.floor()
    }

    async fn count(&self, query: &Query, cap: u64) -> Result<u64> {
        let n = self.inner.count(query, cap).await?;
        if n > cap {
            return Err(SourceError::contract(format!("count {n} over cap {cap}")));
        }
        Ok(n)
    }
}

impl<S: Source> Changes for Checked<S> {
    type Position = <S as Snapshots>::Position;

    async fn stream(&self, from: Option<&Self::Position>) -> Result<Feed<Self::Position>> {
        let mut feed = self.inner.stream(from).await?;
        if from.is_some_and(|f| feed.start < *f) {
            return Err(SourceError::contract("resumed feed starts before `from`"));
        }
        self.state.lock().unwrap().start = Some(feed.start.clone());
        let catalog = self.inner.catalog();
        let (tx, checked) = Feed::channel(feed.start.clone(), 64);
        tokio::spawn(async move {
            let mut last = feed.start.clone();
            loop {
                let next = feed.next().await.and_then(|txn| {
                    check_txn(&catalog, &last, &txn)?;
                    last = txn.position.clone();
                    Ok(txn)
                });
                let stop = next.is_err();
                if tx.send(next).await.is_err() || stop {
                    return;
                }
            }
        });
        Ok(checked)
    }

    fn ack(&self, upto: &Self::Position) {
        self.inner.ack(upto);
    }
}

impl<S: Source> Lifecycle for Checked<S> {
    async fn setup(&self) -> Result<()> {
        self.inner.setup().await
    }

    async fn open(&self) -> Result<()> {
        self.inner.open().await
    }

    async fn close(&self) {
        self.inner.close().await
    }
}
