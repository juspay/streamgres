//! A simple stand-in for the engine: read a snapshot, replay changes, compare with the source.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::catalog::{Key, Row, Table, complete};
use crate::change::{Change, Txn};
use crate::error::{Result, SourceError};
use crate::query::{Query, Truth};
use crate::source::{Feed, Snapshots, Source};

type P<S> = <S as Snapshots>::Position;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub txns: u64,
    /// Partial rows filled from a row we already held.
    pub completed: u64,
    /// Rows read again by key because we didn't hold them.
    pub rereads: u64,
}

pub struct Mirror<S: Source> {
    src: Arc<S>,
    query: Query,
    table: Arc<Table>,
    feed: Feed<P<S>>,
    /// Every row matching the filter; the limit is applied only when comparing.
    view: HashMap<Key, Row>,
    /// Recent transactions, used to bring re-read rows up to date.
    log: VecDeque<Txn<P<S>>>,
    pub stats: Stats,
}

/// Rows seen earlier in the current transaction; `None` means deleted.
type Scratch = HashMap<Key, Option<Row>>;

impl<S: Source> Mirror<S> {
    /// Start the stream, then take the first snapshot.
    pub async fn start(src: Arc<S>, query: Query) -> Result<Self> {
        let table = src
            .catalog()
            .table(&query.table)
            .cloned()
            .ok_or_else(|| SourceError::Refused(format!("no table `{}`", query.table)))?;
        let mut feed = src.stream(None).await?;
        src.advance(&feed.start);

        // Keep reading the stream while waiting, since the source may need it to serve a read.
        let unlimited = Query {
            limit: None,
            ..query.clone()
        };
        let mut buffered = vec![];
        {
            let warm = src.select(&unlimited);
            tokio::pin!(warm);
            loop {
                tokio::select! {
                    biased;
                    snap = &mut warm => {
                        snap?;
                        break;
                    }
                    txn = feed.next() => {
                        let txn = txn?;
                        src.advance(&txn.position);
                        buffered.push(txn);
                    }
                }
            }
        }
        // Read again with nothing pending, so later re-reads never come from the future.
        let snap = src.select(&unlimited).await?;

        let mut mirror = Mirror {
            view: snap
                .rows
                .into_iter()
                .map(|r| (table.key_of(&r).unwrap(), r))
                .collect(),
            src,
            query,
            table,
            feed,
            log: VecDeque::new(),
            stats: Stats::default(),
        };
        for txn in buffered {
            if txn.position > snap.position {
                mirror.apply_changes(&txn).await?;
            }
            mirror.finish(txn);
        }
        Ok(mirror)
    }

    /// Apply incoming transactions for the given time.
    pub async fn pump(&mut self, wait: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + wait;
        while let Ok(txn) = tokio::time::timeout_at(deadline, self.feed.next()).await {
            self.apply(txn?).await?;
        }
        Ok(())
    }

    /// Keep applying until a fresh read covers every change, then compare with it.
    pub async fn settle(&mut self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            self.pump(Duration::from_millis(50)).await?;
            let truth = match self.src.select(&self.query).await {
                // A transient failure (like a lost snapshot) is retried after more stream.
                Err(SourceError::Transient(_)) if Instant::now() < deadline => continue,
                other => other?,
            };
            let behind = self
                .log
                .iter()
                .any(|t| t.position > truth.position && self.touches(t));
            if !behind {
                return self.compare(truth.rows);
            }
            if Instant::now() > deadline {
                return Err(SourceError::Transient("settle timed out".into()));
            }
        }
    }

    /// The current result, with filter, order and limit applied.
    pub fn view(&self) -> Vec<Row> {
        let mut rows: Vec<Row> = self.view.values().cloned().collect();
        rows.sort_by_key(|r| self.table.key_of(r));
        self.query.sort(&mut rows);
        if let Some(n) = self.query.limit {
            rows.truncate(n);
        }
        rows
    }

    fn compare(&self, mut truth: Vec<Row>) -> Result<()> {
        let mut mine = self.view();
        let by_key = |r: &Row| self.table.key_of(r);
        mine.sort_by_key(by_key);
        truth.sort_by_key(by_key);
        if mine == truth {
            return Ok(());
        }
        Err(SourceError::contract(format!(
            "snapshot + changes != truth\nmirror: {mine:?}\ntruth:  {truth:?}"
        )))
    }

    fn touches(&self, txn: &Txn<P<S>>) -> bool {
        txn.changes.iter().any(|c| c.table() == self.table.name)
    }

    async fn apply(&mut self, txn: Txn<P<S>>) -> Result<()> {
        self.apply_changes(&txn).await?;
        self.finish(txn);
        Ok(())
    }

    /// Record the transaction, report progress, and drop history the source no longer needs.
    fn finish(&mut self, txn: Txn<P<S>>) {
        self.stats.txns += 1;
        self.src.advance(&txn.position);
        self.src.ack(&txn.position);
        self.log.push_back(txn);
        let floor = self.src.floor();
        while self.log.front().is_some_and(|t| t.position <= floor) {
            self.log.pop_front();
        }
    }

    async fn apply_changes(&mut self, txn: &Txn<P<S>>) -> Result<()> {
        let mut scratch = Scratch::new();
        let table = self.table.clone();
        for change in txn.changes.iter().filter(|c| c.table() == table.name) {
            self.apply_change(change, &txn.position, &mut scratch)
                .await?;
        }
        Ok(())
    }

    async fn apply_change(
        &mut self,
        change: &Change,
        at: &P<S>,
        scratch: &mut Scratch,
    ) -> Result<()> {
        match change {
            Change::Insert { row, .. } => {
                let key = self.key(row)?;
                scratch.insert(key.clone(), Some(row.clone()));
                self.place(key, row.clone())
            }
            Change::Update { row, old_key, .. } => {
                let key = self.key(row)?;
                let base_key = old_key.clone().unwrap_or_else(|| key.clone());
                // Fill in missing columns before checking the filter.
                let full = if self.table.is_complete(row) {
                    row.clone()
                } else {
                    let base = self.base(&base_key, at, scratch).await?.ok_or_else(|| {
                        SourceError::contract(format!("partial update of unknown row {base_key:?}"))
                    })?;
                    complete(row, &base)
                };
                if let Some(old) = old_key {
                    self.view.remove(old);
                    scratch.insert(old.clone(), None);
                }
                scratch.insert(key.clone(), Some(full.clone()));
                self.place(key, full)
            }
            Change::Delete { key, .. } => {
                self.view.remove(key);
                scratch.insert(key.clone(), None);
                Ok(())
            }
            Change::Truncate { .. } => {
                self.view.clear();
                scratch.clear();
                scratch.insert(vec![], None); // marker: everything gone in this txn
                Ok(())
            }
        }
    }

    /// The row's last full version before `at`.
    async fn base(&mut self, key: &Key, at: &P<S>, scratch: &Scratch) -> Result<Option<Row>> {
        if let Some(seen) = scratch.get(key) {
            return Ok(seen.clone());
        }
        if scratch.contains_key(&vec![]) {
            return Ok(None);
        }
        if let Some(held) = self.view.get(key) {
            self.stats.completed += 1;
            return Ok(Some(held.clone()));
        }
        self.reread(key, at).await
    }

    /// Read the row by key, then replay recent changes up to just before `at`.
    fn reread<'a>(
        &'a mut self,
        key: &'a Key,
        at: &'a P<S>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Row>>> + 'a>> {
        Box::pin(async move {
            self.stats.rereads += 1;
            let snap = self.src.select(&Query::key(&self.table, key)).await?;
            let mut row = snap.rows.into_iter().next();
            let later: Vec<_> = self
                .log
                .iter()
                .filter(|t| t.position > snap.position && t.position < *at)
                .cloned()
                .collect();
            let table = self.table.clone();
            for txn in later {
                for change in txn.changes.iter().filter(|c| c.table() == table.name) {
                    row = match change {
                        // The row came from another key, so read it from there.
                        Change::Update {
                            row: new,
                            old_key: Some(from),
                            ..
                        } if from != key && self.table.key_of(new).as_ref() == Some(key) => {
                            if self.table.is_complete(new) {
                                Some(new.clone())
                            } else {
                                let base =
                                    self.reread(from, &txn.position).await?.ok_or_else(|| {
                                        SourceError::contract("partial key change with no base")
                                    })?;
                                Some(complete(new, &base))
                            }
                        }
                        _ => replay(&self.table, key, change, row)?,
                    };
                }
            }
            Ok(row)
        })
    }

    fn key(&self, row: &Row) -> Result<Key> {
        self.table
            .key_of(row)
            .ok_or_else(|| SourceError::contract("row without key columns"))
    }

    fn place(&mut self, key: Key, row: Row) -> Result<()> {
        match self.query.filter.eval(&row) {
            Truth::True => {
                self.view.insert(key, row);
            }
            Truth::False => {
                self.view.remove(&key);
            }
            Truth::Unknown => return Err(SourceError::contract("filter unknown on a full row")),
        }
        Ok(())
    }
}

/// Apply one change to one row.
fn replay(table: &Table, key: &Key, change: &Change, row: Option<Row>) -> Result<Option<Row>> {
    Ok(match change {
        Change::Insert { row: new, .. } if table.key_of(new).as_ref() == Some(key) => {
            Some(new.clone())
        }
        Change::Update {
            row: new, old_key, ..
        } => {
            if old_key.as_ref() == Some(key) {
                None
            } else if table.key_of(new).as_ref() == Some(key) {
                match row {
                    Some(base) => Some(complete(new, &base)),
                    None if table.is_complete(new) => Some(new.clone()),
                    None => {
                        return Err(SourceError::contract("partial update with no base in log"));
                    }
                }
            } else {
                row
            }
        }
        Change::Delete { key: k, .. } if k == key => None,
        Change::Truncate { .. } => None,
        _ => row,
    })
}
