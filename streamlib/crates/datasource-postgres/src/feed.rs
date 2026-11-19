//! The change feed: reads logical replication and emits one transaction per commit.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pgoutput::events::base::event::BaseEvent;
use pgoutput::events::base::tuple_data::{
    OldDataOrPrimaryKeyTupleData, TupleData, TupleDataColumn,
};
use pgoutput::events::event::{Event, EventType};
use pgoutput::options::{BinaryValueTraitOff, StreamingValueTraitOff};
use pgwire_replication::{ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};
use streamlib_datasource::{
    Catalog, Change, Key, Result, Row, SourceError, Table, Txn, Value, ValueType,
};
use tokio::sync::mpsc;
use tokio_postgres::Config;
use tokio_postgres::config::Host;

use crate::codec::{Lsn, PgCodec};

type Decoded = Event<BinaryValueTraitOff, StreamingValueTraitOff>;
type Tuple = TupleData<BinaryValueTraitOff>;

/// A published table, as the stream describes it.
struct Relation {
    table: Arc<Table>,
    /// Column names and types, in the order the stream sends them.
    columns: Vec<(String, ValueType)>,
}

/// Turns replication messages into changes.
pub struct Decoder {
    catalog: Arc<Catalog>,
    codec: Arc<dyn PgCodec>,
    relations: HashMap<i32, Option<Relation>>,
    pending: Vec<Change>,
}

impl Decoder {
    pub fn new(catalog: Arc<Catalog>, codec: Arc<dyn PgCodec>) -> Self {
        Decoder {
            codec,
            catalog,
            relations: HashMap::new(),
            pending: vec![],
        }
    }

    /// Handle one replication event; returns a transaction at each commit.
    pub fn absorb(&mut self, event: ReplicationEvent) -> Result<Option<Txn<Lsn>>> {
        match event {
            ReplicationEvent::Begin { .. } => self.pending.clear(),
            ReplicationEvent::XLogData { data, .. } => self.message(&data)?,
            ReplicationEvent::Commit { end_lsn, .. } => {
                let changes = std::mem::take(&mut self.pending);
                return Ok(Some(Txn {
                    position: Lsn(end_lsn.as_u64()),
                    changes,
                }));
            }
            _ => {}
        }
        Ok(None)
    }

    fn message(&mut self, data: &[u8]) -> Result<()> {
        let bad = |why: String| SourceError::Transient(format!("pgoutput: {why}"));
        let (&tag, body) = data
            .split_first()
            .ok_or_else(|| bad("empty message".into()))?;
        let kind = EventType::from_char(tag)
            .ok_or_else(|| bad(format!("unknown tag `{}`", tag as char)))?;
        let Event::Base(event) = Decoded::parse(&kind, body).map_err(bad)? else {
            return Ok(());
        };
        match event {
            BaseEvent::Relation(r) => {
                let name = format!("{}.{}", r.relation_namespace, r.name);
                let relation = match self.catalog.table(&name) {
                    None => None,
                    Some(table) => {
                        let columns = r
                            .columns
                            .iter()
                            .map(|c| {
                                let col = table.column(&c.name).ok_or_else(|| {
                                    SourceError::Refused(format!(
                                        "`{name}.{}` not in catalog (schema changed?)",
                                        c.name
                                    ))
                                })?;
                                Ok((c.name.clone(), col.ty))
                            })
                            .collect::<Result<_>>()?;
                        Some(Relation {
                            table: table.clone(),
                            columns,
                        })
                    }
                };
                self.relations.insert(r.oid, relation);
            }
            BaseEvent::Insert(i) => {
                if let Some(rel) = self.relation(i.oid)? {
                    let row = image(self.codec.as_ref(), rel, &i.data)?;
                    self.pending.push(Change::Insert {
                        table: rel.table.name.clone(),
                        row,
                    });
                }
            }
            BaseEvent::Update(u) => {
                if let Some(rel) = self.relation(u.oid)? {
                    let row = image(self.codec.as_ref(), rel, &u.data)?;
                    let new_key = key(rel, &row)?;
                    let old_key = match &u.old_data_or_primary_key {
                        Some(old) => {
                            Some(key(rel, &image(self.codec.as_ref(), rel, old_tuple(old))?)?)
                        }
                        None => None,
                    };
                    let old_key = old_key.filter(|k| *k != new_key);
                    self.pending.push(Change::Update {
                        table: rel.table.name.clone(),
                        old_key,
                        row,
                    });
                }
            }
            BaseEvent::Delete(d) => {
                if let Some(rel) = self.relation(d.oid)? {
                    let old = d.old_data_or_primary_key.as_ref().ok_or_else(|| {
                        SourceError::Refused(format!(
                            "DELETE on `{}` without a key",
                            rel.table.name
                        ))
                    })?;
                    let key = key(rel, &image(self.codec.as_ref(), rel, old_tuple(old))?)?;
                    self.pending.push(Change::Delete {
                        table: rel.table.name.clone(),
                        key,
                    });
                }
            }
            BaseEvent::Truncate(t) => {
                for oid in t.oids {
                    if let Some(rel) = self.relation(oid)? {
                        self.pending.push(Change::Truncate {
                            table: rel.table.name.clone(),
                        });
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// `None` means a table this source doesn't serve.
    fn relation(&self, oid: i32) -> Result<Option<&Relation>> {
        match self.relations.get(&oid) {
            Some(r) => Ok(r.as_ref()),
            None => Err(SourceError::Transient(format!(
                "row for unknown relation {oid}"
            ))),
        }
    }
}

fn old_tuple(old: &OldDataOrPrimaryKeyTupleData<BinaryValueTraitOff>) -> &Tuple {
    match old {
        OldDataOrPrimaryKeyTupleData::OldTupleData(t)
        | OldDataOrPrimaryKeyTupleData::PrimaryKeyTupleData(t) => t,
    }
}

/// Decode a row; unchanged big values are left out rather than set to null.
fn image(codec: &dyn PgCodec, rel: &Relation, tuple: &Tuple) -> Result<Row> {
    let mut row = Row::new();
    for ((name, ty), cell) in rel.columns.iter().zip(tuple) {
        match cell {
            TupleDataColumn::PGNull => {
                row.insert(name.clone(), Value::Null);
            }
            TupleDataColumn::Value(text) => {
                row.insert(name.clone(), codec.decode(*ty, text)?);
            }
            TupleDataColumn::PGUnchangedToastedValue => {}
        }
    }
    Ok(row)
}

fn key(rel: &Relation, row: &Row) -> Result<Key> {
    rel.table
        .key_of(row)
        .ok_or_else(|| SourceError::Refused(format!("row of `{}` without its key", rel.table.name)))
}

fn replication_config(
    cfg: &Config,
    slot: &str,
    publication: &str,
    start: Lsn,
) -> ReplicationConfig {
    let host = match cfg.get_hosts().first() {
        Some(Host::Tcp(h)) => h.clone(),
        _ => "localhost".into(),
    };
    let user = cfg.get_user().unwrap_or("postgres");
    let password = cfg
        .get_password()
        .map(|p| String::from_utf8_lossy(p).into_owned())
        .unwrap_or_default();
    let db = cfg.get_dbname().unwrap_or(user);
    ReplicationConfig::new(host, user, password, db, slot, publication)
        .with_port(cfg.get_ports().first().copied().unwrap_or(5432))
        .with_tls(TlsConfig::disabled())
        .with_start_lsn(pgwire_replication::Lsn(start.0))
        .with_status_interval(Duration::from_secs(1))
}

/// Everything a feed task needs to stream.
pub struct Stream {
    pub cfg: Config,
    pub slot: String,
    pub publication: String,
    pub start: Lsn,
    pub catalog: Arc<Catalog>,
    pub codec: Arc<dyn PgCodec>,
}

/// Stream changes into `tx` until the receiver goes away or the connection fails.
pub async fn run(stream: Stream, acked: Arc<AtomicU64>, tx: mpsc::Sender<Result<Txn<Lsn>>>) {
    let Stream {
        cfg,
        slot,
        publication,
        start,
        catalog,
        codec,
    } = stream;
    let err =
        |e: pgwire_replication::PgWireError| SourceError::Transient(format!("replication: {e}"));
    let mut client = match ReplicationClient::connect(replication_config(
        &cfg,
        &slot,
        &publication,
        start,
    ))
    .await
    {
        Ok(c) => c,
        Err(e) => {
            let _ = tx.send(Err(err(e))).await;
            return;
        }
    };
    let mut decoder = Decoder::new(catalog, codec);
    let mut last = start;
    let mut confirmed = 0;
    // Wake up regularly so acks reach Postgres even when no changes arrive.
    let mut flush = tokio::time::interval(Duration::from_secs(1));
    loop {
        let ack = acked.load(Ordering::Relaxed);
        if ack > confirmed {
            client.update_applied_lsn(pgwire_replication::Lsn(ack));
            confirmed = ack;
        }
        let event = tokio::select! {
            e = client.recv() => e,
            _ = flush.tick() => continue,
            _ = tx.closed() => break,
        };
        let txn = match event {
            Ok(Some(e)) => decoder.absorb(e),
            Ok(None) => Err(SourceError::Transient("replication stream ended".into())),
            Err(e) => Err(err(e)),
        };
        match txn {
            Ok(Some(t)) if t.position > last => {
                last = t.position;
                if tx.send(Ok(t)).await.is_err() {
                    break;
                }
            }
            Ok(_) => {}
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                break;
            }
        }
    }
    let _ = client.shutdown().await;
}
