//! The change feed: a logical replication connection on a permanent slot,
//! streaming the `pgoutput` plugin's messages, turned into [`WriteQuery`]s
//! positioned at the **end of their transaction's commit record**. A
//! snapshot taken at a consistent point `X` holds exactly the writes
//! positioned at or below `X`, so that is the position every read is
//! compared against.
//!
//! The transport is `pgwire-replication` (startup, authentication,
//! `START_REPLICATION`, standby feedback; it decodes the transaction
//! boundaries) and the row messages are decoded by the `pgoutput` crate;
//! this module only maps decoded tuples onto the catalog's tables and
//! types. Progress marks come from **heartbeats**: each poll commits a
//! tiny `pg_logical_emit_message` and waits until the feed returns it.
//! Decoding emits whole transactions in commit order, so once the
//! heartbeat's commit is back every transaction committed before it has
//! been delivered, and the heartbeat's own position is the mark.

use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use pgoutput::events::base::event::BaseEvent;
use pgoutput::events::base::tuple_data::{
    OldDataOrPrimaryKeyTupleData, TupleData, TupleDataColumn,
};
use pgoutput::events::event::{Event, EventType};
use pgoutput::options::{BinaryValueTraitOff, StreamingValueTraitOff};
use pgwire_replication::{ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};
use tokio::sync::mpsc;
use tokio_postgres::Client;
use tokio_postgres::config::Host;

use crate::model::{
    Catalog, ColumnName, DataFrameKey, DataFrameRow, DbTable, DeleteQuery, InsertQuery, Lsn,
    TableName, UpdateQuery, Value, ValueType, WriteQuery,
};
use crate::sync::service::Command;
use crate::sync::storage::StorageError;

/// The message set the feed asks for: text values, whole transactions
/// (no streaming of in-progress ones).
type Decoded = Event<BinaryValueTraitOff, StreamingValueTraitOff>;

/// One tuple as decoded: a value, `NULL`, or an unchanged TOAST marker per
/// column, in the relation's column order.
type Columns = TupleData<BinaryValueTraitOff>;

/// The prefix of the feed's heartbeat messages.
const HEARTBEAT_PREFIX: &str = "xyne_sync";

/// How long a poll waits for its heartbeat to come back before failing.
const HEARTBEAT_WAIT: Duration = Duration::from_secs(10);

/// One poll's yield: the writes delivered since the last poll, in commit
/// order, each at its position; and the position the feed is known to
/// have delivered everything up to.
#[derive(Debug)]
pub struct Batch {
    pub writes: Vec<(WriteQuery, Lsn)>,
    pub progress: Lsn,
}

/// One decoded transaction: the end of its commit record (the position of
/// its writes), its writes on catalog tables, and the heartbeat token it
/// carried, if it was a heartbeat.
#[derive(Debug, PartialEq)]
pub struct Transaction {
    pub at: Lsn,
    pub writes: Vec<WriteQuery>,
    pub heartbeat: Option<String>,
}

/// One published table as the feed described it: the catalog table it
/// maps to (`None` for a table the catalog does not declare) and its
/// column names in message order.
struct Relation {
    table: Option<TableName>,
    columns: Vec<ColumnName>,
}

/// The mapping of decoded messages onto catalog writes: the relations
/// announced so far and the transaction being collected.
pub struct Decoder {
    catalog: Rc<Catalog>,
    relations: HashMap<i32, Relation>,
    pending: Vec<WriteQuery>,
    heartbeat: Option<String>,
}

impl Decoder {
    /// A decoder over `catalog`.
    pub fn new(catalog: Rc<Catalog>) -> Self {
        Decoder {
            catalog,
            relations: HashMap::new(),
            pending: Vec::new(),
            heartbeat: None,
        }
    }

    /// Absorb one event; a `Commit` yields the finished transaction.
    pub fn absorb(&mut self, event: ReplicationEvent) -> Result<Option<Transaction>, StorageError> {
        match event {
            ReplicationEvent::Begin { .. } => {
                self.pending.clear();
                self.heartbeat = None;
                Ok(None)
            }
            ReplicationEvent::Commit { end_lsn, .. } => Ok(Some(Transaction {
                at: position(end_lsn),
                writes: std::mem::take(&mut self.pending),
                heartbeat: self.heartbeat.take(),
            })),
            ReplicationEvent::Message {
                transactional: true,
                prefix,
                content,
                ..
            } if prefix == HEARTBEAT_PREFIX => {
                self.heartbeat = Some(String::from_utf8_lossy(&content).into_owned());
                Ok(None)
            }
            ReplicationEvent::XLogData { data, .. } => {
                self.decode(&data)?;
                Ok(None)
            }
            ReplicationEvent::Message { .. }
            | ReplicationEvent::KeepAlive { .. }
            | ReplicationEvent::StoppedAt { .. } => Ok(None),
        }
    }

    /// Map one `pgoutput` row message onto the pending transaction.
    fn decode(&mut self, data: &Bytes) -> Result<(), StorageError> {
        let Some((&tag, body)) = data.split_first() else {
            return Err(StorageError("empty pgoutput message".to_owned()));
        };
        let kind = EventType::from_char(tag)
            .ok_or_else(|| StorageError(format!("unknown pgoutput message `{}`", tag as char)))?;
        let event = Decoded::parse(&kind, body).map_err(StorageError)?;
        let Event::Base(event) = event else {
            return Ok(());
        };
        if let BaseEvent::Relation(relation) = &event {
            let qualified = format!("{}.{}", relation.relation_namespace, relation.name);
            let declared = self.catalog.table(&qualified).or_else(|| {
                (relation.relation_namespace == "public")
                    .then(|| self.catalog.table(&relation.name))
                    .flatten()
            });
            self.relations.insert(
                relation.oid,
                Relation {
                    table: declared.map(|table| table.name.clone()),
                    columns: relation
                        .columns
                        .iter()
                        .map(|column| ColumnName::from(column.name.as_str()))
                        .collect(),
                },
            );
            return Ok(());
        }
        let writes = self.writes_of(&event)?;
        self.pending.extend(writes);
        Ok(())
    }

    /// The catalog writes one row message stands for: none for a table
    /// the catalog does not declare, two for a primary-key change.
    fn writes_of(
        &self,
        event: &BaseEvent<BinaryValueTraitOff, StreamingValueTraitOff>,
    ) -> Result<Vec<WriteQuery>, StorageError> {
        match event {
            BaseEvent::Insert(insert) => {
                let Some((table, columns)) = self.declared(insert.oid) else {
                    return Ok(Vec::new());
                };
                let row = image(table, columns, &insert.data)?;
                Ok(vec![WriteQuery::INSERT(InsertQuery {
                    table: table.name.clone(),
                    pkey_value: key_of(&row, table),
                    record: row,
                })])
            }
            BaseEvent::Update(update) => {
                let Some((table, columns)) = self.declared(update.oid) else {
                    return Ok(Vec::new());
                };
                let new = image(table, columns, &update.data)?;
                let new_key = key_of(&new, table);
                let old_key = match &update.old_data_or_primary_key {
                    Some(OldDataOrPrimaryKeyTupleData::PrimaryKeyTupleData(old))
                    | Some(OldDataOrPrimaryKeyTupleData::OldTupleData(old)) => {
                        Some(key_of(&image(table, columns, old)?, table))
                    }
                    None => None,
                };
                match old_key.filter(|old_key| *old_key != new_key) {
                    Some(old_key) => Ok(vec![
                        WriteQuery::DELETE(DeleteQuery {
                            table: table.name.clone(),
                            pkey_value: old_key,
                        }),
                        WriteQuery::INSERT(InsertQuery {
                            table: table.name.clone(),
                            pkey_value: new_key,
                            record: new,
                        }),
                    ]),
                    None => Ok(vec![WriteQuery::UPDATE(UpdateQuery {
                        table: table.name.clone(),
                        pkey_value: new_key,
                        record: new,
                    })]),
                }
            }
            BaseEvent::Delete(delete) => {
                let Some((table, columns)) = self.declared(delete.oid) else {
                    return Ok(Vec::new());
                };
                let old = match &delete.old_data_or_primary_key {
                    Some(OldDataOrPrimaryKeyTupleData::PrimaryKeyTupleData(old))
                    | Some(OldDataOrPrimaryKeyTupleData::OldTupleData(old)) => old,
                    None => {
                        return Err(StorageError(format!(
                            "a DELETE on `{}` arrived without its key; the table needs a primary key or REPLICA IDENTITY",
                            table.name
                        )));
                    }
                };
                Ok(vec![WriteQuery::DELETE(DeleteQuery {
                    table: table.name.clone(),
                    pkey_value: key_of(&image(table, columns, old)?, table),
                })])
            }
            BaseEvent::Truncate(_) => Err(StorageError(
                "TRUNCATE on a published table is not supported: the frames cannot follow it"
                    .to_owned(),
            )),
            BaseEvent::Relation(_)
            | BaseEvent::Begin(_)
            | BaseEvent::Commit(_)
            | BaseEvent::Type(_) => Ok(Vec::new()),
        }
    }

    /// The catalog table and column order of relation `oid`, if the feed
    /// announced it and the catalog declares it.
    fn declared(&self, oid: i32) -> Option<(&DbTable, &[ColumnName])> {
        let relation = self.relations.get(&oid)?;
        let table = self.catalog.table(relation.table.as_ref()?.as_str())?;
        Some((table, &relation.columns))
    }
}

/// The change feed over one replication slot: the transport half (the
/// replication connection, and the ordinary connection the heartbeats go
/// through) and the decoding half (the catalog-driven decoder and the
/// delivered position), together for a single-threaded driver, or split
/// ([`PgStream::split`]) so the transport runs on a thread of its own and
/// hands raw events to the decoder over a channel.
pub struct PgStream {
    transport: Transport,
    feed: Feed,
}

/// The connections of a change feed. Nothing in it is tied to a thread:
/// it forwards raw replication events and beats the heart.
pub struct Transport {
    client: Client,
    feed: ReplicationClient,
    heart: Heart,
}

/// The decoding half of a change feed: raw events in, catalog writes and
/// the delivered position out. It holds the catalog by `Rc`, so it lives
/// on the engine's thread.
pub struct Feed {
    decoder: Decoder,
    progress: Lsn,
}

struct Heart {
    prefix: String,
    count: u64,
}

impl Heart {
    fn new(slot: &str) -> Self {
        let started = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        Heart {
            prefix: format!("{slot}@{started}:"),
            count: 0,
        }
    }

    async fn beat(&mut self, client: &Client) -> Result<String, StorageError> {
        let token = format!("{}{}", self.prefix, self.count);
        self.count += 1;
        client
            .execute(
                "SELECT pg_logical_emit_message(true, $1::text, $2::text)",
                &[&HEARTBEAT_PREFIX, &token],
            )
            .await?;
        Ok(token)
    }
}

impl Transport {
    /// Open the connections to `dsn` for `slot`, creating the publication
    /// (`<slot>_pub`, every table) and the slot when they do not exist
    /// yet.
    pub async fn open(dsn: &str, slot: &str) -> Result<Self, StorageError> {
        let config: tokio_postgres::Config = dsn.parse()?;
        let client = super::open(&config).await?;
        let publication = publication_of(slot);
        let published = client
            .query_opt(
                "SELECT 1 FROM pg_publication WHERE pubname = $1",
                &[&publication],
            )
            .await?
            .is_some();
        if !published {
            client
                .batch_execute(&format!(
                    "CREATE PUBLICATION \"{publication}\" FOR ALL TABLES"
                ))
                .await?;
        }
        let exists = client
            .query_opt(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await?
            .is_some();
        if !exists {
            client
                .execute(
                    "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                    &[&slot],
                )
                .await?;
        }
        let feed = ReplicationClient::connect(replication_config(&config, slot, &publication))
            .await
            .map_err(|error| StorageError(format!("replication connection: {error}")))?;
        Ok(Transport {
            client,
            feed,
            heart: Heart::new(slot),
        })
    }

    /// Tell the slot that every commit up to `at` is applied.
    fn acknowledge(&mut self, at: Lsn) {
        self.feed.update_applied_lsn(pgwire_replication::Lsn(at.0));
    }

    /// Run until `events` has no receiver or the connection ends: forward
    /// every event as it arrives, acknowledging each commit to the slot,
    /// and beat the heart every `interval` so the decoder's position keeps
    /// moving while nothing is written.
    pub async fn run(mut self, interval: Duration, events: mpsc::Sender<ReplicationEvent>) {
        let mut ticker = tokio::time::interval(interval);
        loop {
            let event = tokio::select! {
                _ = ticker.tick() => {
                    if let Err(error) = self.heart.beat(&self.client).await {
                        eprintln!("change feed heartbeat failed: {error}");
                    }
                    continue;
                }
                event = self.feed.recv() => event,
            };
            let event = match event {
                Ok(Some(event)) => event,
                Ok(None) => return,
                Err(error) => {
                    eprintln!("change feed ended: {error}");
                    return;
                }
            };
            if let ReplicationEvent::Commit { end_lsn, .. } = &event {
                self.acknowledge(position(*end_lsn));
            }
            if events.send(event).await.is_err() {
                return;
            }
        }
    }
}

impl Feed {
    /// A decoder for `catalog`, at position zero until the first event.
    pub fn new(catalog: Rc<Catalog>) -> Self {
        Feed {
            decoder: Decoder::new(catalog),
            progress: Lsn(0),
        }
    }

    /// Absorb one raw event: a keepalive or a commit moves the position,
    /// and a commit also yields the transaction it closes.
    pub fn absorb(&mut self, event: ReplicationEvent) -> Result<Option<Transaction>, StorageError> {
        if let ReplicationEvent::KeepAlive { wal_end, .. } = &event {
            self.progress = self.progress.max(position(*wal_end));
        }
        let transaction = self.decoder.absorb(event)?;
        if let Some(transaction) = &transaction {
            self.progress = self.progress.max(transaction.at);
        }
        Ok(transaction)
    }

    /// The position every commit has been delivered up to.
    pub fn progress(&self) -> Lsn {
        self.progress
    }
}

impl PgStream {
    /// Open the feed of `slot` at `dsn`, decoding with `catalog`.
    pub async fn open(dsn: &str, slot: &str, catalog: Rc<Catalog>) -> Result<Self, StorageError> {
        Ok(PgStream {
            transport: Transport::open(dsn, slot).await?,
            feed: Feed::new(catalog),
        })
    }

    /// The two halves, to run on separate threads.
    pub fn split(self) -> (Transport, Feed) {
        (self.transport, self.feed)
    }

    /// Drop `slot` and its publication, ending the walsender holding the
    /// slot first (the drop is retried while it lets go).
    pub async fn drop_slot(dsn: &str, slot: &str) -> Result<(), StorageError> {
        let config: tokio_postgres::Config = dsn.parse()?;
        let client = super::open(&config).await?;
        client
            .execute(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL",
                &[&slot],
            )
            .await?;
        let mut attempts = 0;
        loop {
            let dropped = client
                .execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
                    &[&slot],
                )
                .await;
            match dropped {
                Ok(_) => break,
                Err(_) if attempts < 40 => {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        client
            .batch_execute(&format!(
                "DROP PUBLICATION IF EXISTS \"{}\"",
                publication_of(slot)
            ))
            .await?;
        Ok(())
    }

    /// Emit a heartbeat and collect every write the feed delivers up to
    /// it, with the position the feed reached; fails if the heartbeat does
    /// not come back within [`HEARTBEAT_WAIT`].
    pub async fn poll(&mut self) -> Result<Batch, StorageError> {
        let token = self.transport.heart.beat(&self.transport.client).await?;
        let deadline = Instant::now() + HEARTBEAT_WAIT;
        let mut writes = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let event = tokio::time::timeout(remaining, self.transport.feed.recv())
                .await
                .map_err(|_| {
                    StorageError(format!(
                        "the feed did not return heartbeat {token} within {HEARTBEAT_WAIT:?}"
                    ))
                })?;
            let event = event
                .map_err(|error| StorageError(format!("replication stream: {error}")))?
                .ok_or_else(|| StorageError("the replication stream ended".to_owned()))?;
            let Some(transaction) = self.absorb(event)? else {
                continue;
            };
            let done = transaction.heartbeat.as_deref() == Some(token.as_str());
            let at = transaction.at;
            writes.extend(transaction.writes.into_iter().map(|write| (write, at)));
            if done {
                return Ok(Batch {
                    writes,
                    progress: self.feed.progress(),
                });
            }
        }
    }

    fn absorb(&mut self, event: ReplicationEvent) -> Result<Option<Transaction>, StorageError> {
        let transaction = self.feed.absorb(event)?;
        if let Some(transaction) = &transaction {
            self.transport.acknowledge(transaction.at);
        }
        Ok(transaction)
    }

    /// Run on one thread until `commands` has no receiver or the
    /// connection ends: each transaction goes out as
    /// [`Command::Commit`] followed by a [`Command::Progress`], and a
    /// heartbeat every `interval` keeps the position moving while nothing
    /// is written.
    pub async fn run<Q>(mut self, interval: Duration, commands: mpsc::Sender<Command<Q>>) {
        let mut ticker = tokio::time::interval(interval);
        loop {
            let Transport {
                client,
                feed,
                heart,
            } = &mut self.transport;
            let event = tokio::select! {
                _ = ticker.tick() => {
                    if let Err(error) = heart.beat(client).await {
                        eprintln!("change feed heartbeat failed: {error}");
                    }
                    continue;
                }
                event = feed.recv() => event,
            };
            let event = match event {
                Ok(Some(event)) => event,
                Ok(None) => return,
                Err(error) => {
                    eprintln!("change feed ended: {error}");
                    return;
                }
            };
            let transaction = match self.absorb(event) {
                Ok(Some(transaction)) => transaction,
                Ok(None) => continue,
                Err(error) => {
                    eprintln!("change feed decoding failed: {error}");
                    return;
                }
            };
            if commands
                .send(Command::Commit {
                    writes: transaction.writes,
                    at: transaction.at,
                })
                .await
                .is_err()
            {
                return;
            }
            if commands
                .send(Command::Progress(self.feed.progress()))
                .await
                .is_err()
            {
                return;
            }
        }
    }
}

fn publication_of(slot: &str) -> String {
    format!("{slot}_pub")
}

/// The feed's position for a location the transport reports.
fn position(lsn: pgwire_replication::Lsn) -> Lsn {
    Lsn(lsn.as_u64())
}

/// The replication connection's settings, taken from the SQL connection's
/// (`tokio-postgres` cannot open a replication connection itself). Plain
/// TCP or a Unix socket, no TLS; standby feedback once a second.
fn replication_config(
    config: &tokio_postgres::Config,
    slot: &str,
    publication: &str,
) -> ReplicationConfig {
    let host = match config.get_hosts().first() {
        Some(Host::Tcp(host)) => host.clone(),
        Some(Host::Unix(path)) => path.to_string_lossy().into_owned(),
        None => "localhost".to_owned(),
    };
    let port = config.get_ports().first().copied().unwrap_or(5432);
    let user = config.get_user().unwrap_or("postgres");
    let password = config
        .get_password()
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
        .unwrap_or_default();
    let database = config.get_dbname().unwrap_or(user);
    ReplicationConfig::new(host, user, password, database, slot, publication)
        .with_port(port)
        .with_tls(TlsConfig::disabled())
        .with_status_interval(Duration::from_secs(1))
}

/// The primary key of a decoded row image.
fn key_of(row: &DataFrameRow, table: &DbTable) -> DataFrameKey {
    DataFrameKey::new(table.pkey.iter().map(|column| {
        (
            column.clone(),
            row.data.get(column).cloned().unwrap_or(Value::Null),
        )
    }))
}

/// A decoded tuple as a row image of `table`: each value converted by the
/// column's declared type, columns the catalog does not declare skipped,
/// declared columns the relation lacks `NULL`, so every image carries
/// every column.
fn image(
    table: &DbTable,
    columns: &[ColumnName],
    tuple: &Columns,
) -> Result<DataFrameRow, StorageError> {
    let mut data = HashMap::new();
    for (name, column) in columns.iter().zip(tuple) {
        let Some(declared) = table.column(name.as_str()) else {
            continue;
        };
        let value = match column {
            TupleDataColumn::PGNull => Value::Null,
            TupleDataColumn::Value(text) => convert(text, &declared.r#type)?,
            TupleDataColumn::PGUnchangedToastedValue => {
                return Err(StorageError(format!(
                    "column `{name}` arrived without its value; the table needs REPLICA IDENTITY FULL or smaller values"
                )));
            }
        };
        data.insert(declared.name.clone(), value);
    }
    for name in table.columns.keys() {
        data.entry(name.clone()).or_insert(Value::Null);
    }
    Ok(DataFrameRow { data })
}

/// Convert one text value by its declared type.
fn convert(raw: &str, declared: &ValueType) -> Result<Value, StorageError> {
    let unreadable = || StorageError(format!("`{raw}` is not a {declared:?}"));
    Ok(match declared {
        ValueType::Int => Value::Int(raw.parse().map_err(|_| unreadable())?),
        ValueType::Float => Value::Float(raw.parse().map_err(|_| unreadable())?),
        ValueType::String | ValueType::Json | ValueType::Map(_, _) => Value::String(raw.to_owned()),
        ValueType::List(inner) => super::text::array_literal(raw, inner),
        ValueType::Timestamp => Value::Int(super::text::epoch_millis(raw).ok_or_else(unreadable)?),
        ValueType::Bool => Value::Bool(match raw {
            "true" | "t" => true,
            "false" | "f" => false,
            _ => return Err(unreadable()),
        }),
        ValueType::Date => Value::Date(
            chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d").map_err(|_| unreadable())?,
        ),
        ValueType::Datetime => Value::Datetime(
            chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S"))
                .map_err(|_| unreadable())?,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::evaluate;
    use crate::model::{ComparisonOperator, DbColumn, DbTable, Where};

    /// `probe_t`, the table the session was recorded from.
    fn catalog() -> Rc<Catalog> {
        Rc::new(Catalog::new(vec![DbTable::new(
            "probe_t",
            ["id"],
            vec![
                DbColumn::new("id", ValueType::Int),
                DbColumn::new("name", ValueType::String),
                DbColumn::new("points", ValueType::Int),
                DbColumn::new("flag", ValueType::Bool),
                DbColumn::new("d", ValueType::Date),
                DbColumn::new("ts", ValueType::Datetime),
                DbColumn::new("amount", ValueType::Float),
                DbColumn::new("tag", ValueType::String),
            ],
        )]))
    }

    /// A row message as the transport hands it over, from its recorded hex.
    fn xlog(hex: &str) -> ReplicationEvent {
        let data: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
            .collect();
        ReplicationEvent::XLogData {
            wal_start: pgwire_replication::Lsn(0),
            wal_end: pgwire_replication::Lsn(0),
            server_time_micros: 0,
            data: Bytes::from(data),
        }
    }

    /// A transaction boundary as the transport decodes it.
    fn begin() -> ReplicationEvent {
        ReplicationEvent::Begin {
            final_lsn: pgwire_replication::Lsn(0),
            xid: 0,
            commit_time_micros: 0,
        }
    }

    /// A commit whose record ends at `end`.
    fn commit(end: &str) -> ReplicationEvent {
        ReplicationEvent::Commit {
            lsn: pgwire_replication::Lsn(0),
            end_lsn: pgwire_replication::Lsn(Lsn::parse(end).unwrap().0),
            commit_time_micros: 0,
        }
    }

    /// A heartbeat message carrying `token`.
    fn heartbeat(token: &str) -> ReplicationEvent {
        ReplicationEvent::Message {
            transactional: true,
            lsn: pgwire_replication::Lsn(0),
            prefix: HEARTBEAT_PREFIX.to_owned(),
            content: Bytes::copy_from_slice(token.as_bytes()),
        }
    }

    /// A recorded `pgoutput` session (PostgreSQL 15, `proto_version 1`,
    /// text values): two inserts with quoting, nulls and every scalar
    /// type; an update; a primary-key change, a delete and an insert on
    /// an undeclared table; and a heartbeat. Each transaction lands at
    /// the end of its commit record.
    #[test]
    fn decodes_a_recorded_session() {
        let mut decoder = Decoder::new(catalog());
        let events = vec![
            begin(),
            xlog(
                "52000043477075626c69630070726f62655f74006400080169640000000014ffffffff006e616d650000000019ffffffff00706f696e74730000000014ffffffff00666c61670000000010ffffffff0064000000043affffffff007473000000045affffffff00616d6f756e7400000002bdffffffff007461670000000413ffffffff",
            ),
            xlog(
                "49000043474e00087400000001317400000006697427732078740000000133740000000174740000000a323032362d30392d31317400000015323032362d30392d31312031303a30303a30302e357400000003312e357400000003612062",
            ),
            xlog("49000043474e00087400000001326e6e6e6e6e6e6e"),
            commit("0/3EFF460"),
            begin(),
            xlog(
                "55000043474e0008740000000131740000000772656e616d6564740000000134740000000174740000000a323032362d30392d31317400000015323032362d30392d31312031303a30303a30302e357400000003312e357400000003612062",
            ),
            commit("0/3EFF508"),
            begin(),
            xlog("55000043474b00087400000001326e6e6e6e6e6e6e4e00087400000001336e6e6e6e6e6e6e"),
            xlog("44000043474b00087400000001316e6e6e6e6e6e6e"),
            xlog("520000434e7075626c69630070726f62655f6f74686572006400010169640000000014ffffffff"),
            xlog("490000434e4e0001740000000139"),
            commit("0/3EFF6F8"),
            begin(),
            heartbeat("7"),
            commit("0/3EFF768"),
        ];
        let transactions: Vec<Transaction> = events
            .into_iter()
            .filter_map(|event| decoder.absorb(event).unwrap())
            .collect();
        assert_eq!(transactions.len(), 4, "{transactions:?}");

        assert_eq!(transactions[0].at, Lsn::parse("0/3EFF460").unwrap());
        assert_eq!(transactions[0].writes.len(), 2);
        let image = transactions[0].writes[0].new_row_image().unwrap();
        assert_eq!(image.data["name"], Value::String("it's x".into()));
        assert_eq!(image.data["tag"], Value::String("a b".into()));
        assert_eq!(image.data["flag"], Value::Bool(true));
        assert_eq!(image.data["amount"], Value::Float(1.5));
        assert_eq!(
            image.data["d"],
            Value::Date(chrono::NaiveDate::from_ymd_opt(2026, 9, 11).unwrap())
        );
        assert!(matches!(image.data["ts"], Value::Datetime(_)));
        assert!(evaluate(
            &Where::condition("points", ComparisonOperator::GTE, 3),
            &image.data,
            &mut 0
        ));
        assert_eq!(
            transactions[0].writes[1].new_row_image().unwrap().data["name"],
            Value::Null
        );

        assert!(matches!(transactions[1].writes[0], WriteQuery::UPDATE(_)));
        assert_eq!(transactions[1].at, Lsn::parse("0/3EFF508").unwrap());

        let last = &transactions[2].writes;
        assert_eq!(
            last.len(),
            3,
            "a key change is a delete and an insert; the undeclared table is skipped"
        );
        assert!(
            matches!(&last[0], WriteQuery::DELETE(d) if d.pkey_value.pkey_value["id"] == Value::Int(2))
        );
        assert!(
            matches!(&last[1], WriteQuery::INSERT(i) if i.pkey_value.pkey_value["id"] == Value::Int(3))
        );
        assert!(
            matches!(&last[2], WriteQuery::DELETE(d) if d.pkey_value.pkey_value["id"] == Value::Int(1))
        );
        assert_eq!(transactions[2].at, Lsn::parse("0/3EFF6F8").unwrap());
        assert_eq!(transactions[2].heartbeat, None);

        assert!(transactions[3].writes.is_empty());
        assert_eq!(transactions[3].heartbeat.as_deref(), Some("7"));
        assert_eq!(transactions[3].at, Lsn::parse("0/3EFF768").unwrap());
    }
}
