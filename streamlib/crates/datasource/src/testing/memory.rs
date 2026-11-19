//! An in-memory source, with optional faults to prove the test kit catches broken sources.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::catalog::{Catalog, Key, Row, complete};
use crate::change::{Change, Txn};
use crate::error::{Result, SourceError};
use crate::query::{Query, Truth};
use crate::source::{Changes, Feed, Lifecycle, Snapshot, Snapshots};

type Tables = BTreeMap<String, BTreeMap<Key, Row>>;

/// Ways to break the contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    None,
    /// Labels reads with a position the consumer hasn't reached yet.
    AheadOfAdvance,
    /// Returns old rows labelled with a newer position.
    StaleRows,
    /// Sends inserts with columns missing.
    PartialInsert,
}

pub struct MemorySource {
    catalog: Arc<Catalog>,
    /// Columns left out of updates when unchanged, like Postgres TOAST.
    partial: Vec<String>,
    fault: Fault,
    state: Mutex<State>,
    writes: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct State {
    pos: u64,
    live: Tables,
    /// Rows as of `applied_at`, which reads return.
    applied: Tables,
    previous: Tables,
    applied_at: u64,
    /// Committed changes after `applied_at`, with full rows.
    history: Vec<(u64, Vec<Change>)>,
    feeds: Vec<mpsc::Sender<Result<Txn<u64>>>>,
}

impl MemorySource {
    pub fn new(catalog: Catalog) -> Self {
        MemorySource {
            catalog: Arc::new(catalog),
            partial: vec![],
            fault: Fault::None,
            state: Mutex::default(),
            writes: tokio::sync::Mutex::new(()),
        }
    }

    pub fn with_partial(mut self, columns: &[&str]) -> Self {
        self.partial = columns.iter().map(|c| c.to_string()).collect();
        self
    }

    pub fn with_fault(mut self, fault: Fault) -> Self {
        self.fault = fault;
        self
    }

    /// Commit one transaction and return its position.
    pub async fn write(&self, changes: Vec<Change>) -> u64 {
        let _serial = self.writes.lock().await;
        let (txn, feeds) = {
            let mut s = self.state.lock().unwrap();
            let sent = changes.iter().map(|c| self.wire(&s.live, c)).collect();
            for c in &changes {
                apply(&self.catalog, &mut s.live, c);
            }
            s.pos += 1;
            let pos = s.pos;
            s.history.push((pos, changes));
            s.feeds.retain(|f| !f.is_closed());
            (
                Txn {
                    position: pos,
                    changes: sent,
                },
                s.feeds.clone(),
            )
        };
        for f in feeds {
            let _ = f.send(Ok(txn.clone())).await;
        }
        txn.position
    }

    pub async fn insert(&self, table: &str, row: Row) -> u64 {
        self.write(vec![Change::Insert {
            table: table.into(),
            row,
        }])
        .await
    }

    pub async fn update(&self, table: &str, row: Row) -> u64 {
        self.write(vec![Change::Update {
            table: table.into(),
            old_key: None,
            row,
        }])
        .await
    }

    pub async fn delete(&self, table: &str, key: Key) -> u64 {
        self.write(vec![Change::Delete {
            table: table.into(),
            key,
        }])
        .await
    }

    /// Current rows of a table, for tests.
    pub fn live(&self, table: &str) -> Vec<Row> {
        let s = self.state.lock().unwrap();
        s.live
            .get(table)
            .map(|t| t.values().cloned().collect())
            .unwrap_or_default()
    }

    /// The change as the feed sends it, with unchanged partial columns removed.
    fn wire(&self, live: &Tables, change: &Change) -> Change {
        let mut change = change.clone();
        let table = self.catalog.table(change.table()).expect("known table");
        match &mut change {
            Change::Update { row, old_key, .. } => {
                let key = old_key.clone().or_else(|| table.key_of(row));
                let before = key.and_then(|k| live.get(&table.name)?.get(&k).cloned());
                if let Some(before) = before {
                    row.retain(|c, v| !self.partial.contains(c) || before.get(c) != Some(v));
                }
            }
            Change::Insert { row, .. } if self.fault == Fault::PartialInsert => {
                row.retain(|c, _| !self.partial.contains(c));
            }
            _ => {}
        }
        change
    }
}

fn apply(catalog: &Catalog, tables: &mut Tables, change: &Change) {
    let table = catalog.table(change.table()).expect("known table");
    let key_of = |row: &Row| table.key_of(row).expect("key columns present");
    let rows = tables.entry(change.table().to_owned()).or_default();
    match change {
        Change::Insert { row, .. } => {
            rows.insert(key_of(row), row.clone());
        }
        Change::Update { row, old_key, .. } => {
            let before = old_key.as_ref().and_then(|k| rows.remove(k));
            let key = key_of(row);
            let base = before
                .or_else(|| rows.get(&key).cloned())
                .unwrap_or_default();
            rows.insert(key, complete(row, &base));
        }
        Change::Delete { key, .. } => {
            rows.remove(key);
        }
        Change::Truncate { .. } => rows.clear(),
    }
}

impl Snapshots for MemorySource {
    type Position = u64;

    fn catalog(&self) -> Arc<Catalog> {
        self.catalog.clone()
    }

    async fn select(&self, query: &Query) -> Result<Snapshot<u64>> {
        let s = self.state.lock().unwrap();
        if self.catalog.table(&query.table).is_none() {
            return Err(SourceError::Refused(format!("no table `{}`", query.table)));
        }
        let tables = if self.fault == Fault::StaleRows {
            &s.previous
        } else {
            &s.applied
        };
        let mut rows: Vec<Row> = tables
            .get(&query.table)
            .map(|t| {
                t.values()
                    .filter(|r| query.filter.eval(r) == Truth::True)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        query.sort(&mut rows);
        if let Some(n) = query.limit {
            rows.truncate(n);
        }
        let position = if self.fault == Fault::AheadOfAdvance {
            s.pos
        } else {
            s.applied_at
        };
        Ok(Snapshot { rows, position })
    }

    fn advance(&self, upto: &u64) {
        let mut s = self.state.lock().unwrap();
        if *upto <= s.applied_at {
            return;
        }
        s.previous = s.applied.clone();
        let history = std::mem::take(&mut s.history);
        let (done, rest): (Vec<_>, Vec<_>) = history.into_iter().partition(|(p, _)| p <= upto);
        for (_, changes) in done {
            for c in &changes {
                apply(&self.catalog, &mut s.applied, c);
            }
        }
        s.history = rest;
        s.applied_at = *upto;
    }

    fn floor(&self) -> u64 {
        self.state.lock().unwrap().applied_at
    }
}

impl Changes for MemorySource {
    type Position = u64;

    async fn stream(&self, from: Option<&u64>) -> Result<Feed<u64>> {
        let mut s = self.state.lock().unwrap();
        if from.is_some_and(|f| *f != s.pos) {
            return Err(SourceError::CannotResume("memory keeps no history".into()));
        }
        let (tx, feed) = Feed::channel(s.pos, 1024);
        s.feeds.push(tx);
        Ok(feed)
    }

    fn ack(&self, _upto: &u64) {}
}

impl Lifecycle for MemorySource {}
