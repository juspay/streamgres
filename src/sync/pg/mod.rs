//! Postgres as a source. [`PgStorage`] answers the engine's reads from
//! one `REPEATABLE READ` snapshot each, positioned by the exported
//! snapshot of a temporary logical replication slot.
//!
//! A background task keeps minting *aliases*: a temporary logical slot
//! created with `EXPORT_SNAPSHOT`, whose exported snapshot and consistent
//! point Postgres pairs exactly (everything visible in the snapshot
//! committed at or below the point, everything after it is not visible).
//! A freshly minted alias only becomes the **current** one once the write
//! stream has been delivered past its consistent point
//! ([`Storage::advance`]); until then the older alias stays current. So
//! every read is positioned at or below what the engine has applied,
//! never ahead of it, and the runtime brings the read's rows up from
//! there. Each read imports the current alias's snapshot with
//! `SET TRANSACTION SNAPSHOT`; an alias lives as long as the replication
//! connection that minted it, held until the last read that adopted it
//! has finished.
//!
//! [`stream::PgStream`] delivers the change feed from a permanent logical
//! slot and positions every write at its commit. Everything here runs on
//! the engine's thread inside a `tokio::task::LocalSet`.

pub mod replication;
pub mod sql;
pub mod stream;

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::task::spawn_local;
use tokio_postgres::{Client, Config, IsolationLevel, NoTls, Row};

use super::storage::{Storage, StorageError};
use crate::model::{
    Catalog, DataFrameKey, DataFrameRow, DbTable, Lsn, SingleTableReadQuery, Snapshot, Value,
    ValueType,
};
use replication::ReplicationConnection;

pub use stream::{Batch, PgStream};

/// How many minted aliases wait for the stream to pass them before the
/// minter pauses (each holds a slot and a connection).
const WAITING_ALIASES: usize = 2;

/// One minted alias: an exported snapshot and the consistent point it
/// was built at, alive as long as the replication connection that created
/// its temporary slot.
pub struct Alias {
    pub snapshot: String,
    pub lsn: Lsn,
    _minter: ReplicationConnection,
}

/// Aliases minted by this process, for unique slot names.
static MINTED: AtomicU64 = AtomicU64::new(0);

/// Mint one alias: open a replication connection and create a temporary
/// logical slot exporting its snapshot. Slot creation waits for every
/// transaction open at that moment to finish, so a long-running
/// transaction delays the mint (never the reads, which keep the current
/// alias).
pub async fn mint(config: &Config) -> Result<Alias, StorageError> {
    let mut minter = ReplicationConnection::open(config).await?;
    let slot = format!(
        "jus_sync_snap_{}_{}",
        std::process::id(),
        MINTED.fetch_add(1, Ordering::Relaxed)
    );
    let rows = minter
        .simple_query(&format!(
            "CREATE_REPLICATION_SLOT \"{slot}\" TEMPORARY LOGICAL pgoutput EXPORT_SNAPSHOT"
        ))
        .await?;
    let unreadable = || StorageError("CREATE_REPLICATION_SLOT returned no usable row".to_owned());
    let row = rows.first().ok_or_else(unreadable)?;
    let lsn = parse_lsn(
        row.get(1)
            .and_then(|column| column.as_deref())
            .ok_or_else(unreadable)?,
    )?;
    let snapshot = row
        .get(2)
        .and_then(|column| column.clone())
        .ok_or_else(unreadable)?;
    Ok(Alias {
        snapshot,
        lsn,
        _minter: minter,
    })
}

impl From<tokio_postgres::Error> for StorageError {
    /// The driver's message.
    fn from(error: tokio_postgres::Error) -> Self {
        StorageError(error.to_string())
    }
}

/// The aliases: the current one, and the newer ones minted since, oldest
/// first, waiting for the stream to pass their consistent points.
#[derive(Default)]
struct Aliases {
    current: Option<Rc<Alias>>,
    waiting: VecDeque<Rc<Alias>>,
}

impl Aliases {
    /// Make the newest waiting alias at or below `feed` current, dropping
    /// the older ones it supersedes.
    fn advance(&mut self, feed: Lsn) {
        while self.waiting.front().is_some_and(|alias| alias.lsn <= feed) {
            self.current = self.waiting.pop_front();
        }
    }
}

/// Read access to a Postgres database.
///
/// - `config`: how to connect; one connection is opened per read in
///   flight and kept for reuse in `idle`, since each read is its own
///   transaction.
/// - `catalog`: the tables' declared columns, which the `SELECT` casts to
///   and the rows decode by.
/// - `aliases`: the current alias and the ones waiting to become it; a
///   read clones the current handle and keeps it until it is done.
/// - `rotation`: how often a fresh alias is minted.
/// - `alive`: cleared on drop, which ends the minting task.
/// - `delay`: a test hook: hold every snapshot open this long before
///   reading, so writes can be committed behind it deliberately.
pub struct PgStorage {
    config: Config,
    catalog: Rc<Catalog>,
    idle: RefCell<Vec<Client>>,
    aliases: Rc<RefCell<Aliases>>,
    rotation: Rc<Cell<Duration>>,
    alive: Rc<Cell<bool>>,
    delay: Option<Duration>,
}

impl Drop for PgStorage {
    /// End the minting task.
    fn drop(&mut self) {
        self.alive.set(false);
    }
}

impl PgStorage {
    /// Connect once (validating `dsn`) and keep that connection for the
    /// first read; mint the first alias (it becomes current with the first
    /// [`Storage::advance`] past its point) and keep minting one every
    /// [`PgStorage::with_rotation`] interval (250 ms by default).
    pub async fn connect(dsn: &str, catalog: Rc<Catalog>) -> Result<Self, StorageError> {
        let config: Config = dsn.parse()?;
        let client = open(&config).await?;
        let storage = PgStorage {
            config,
            catalog,
            idle: RefCell::new(vec![client]),
            aliases: Rc::new(RefCell::new(Aliases::default())),
            rotation: Rc::new(Cell::new(Duration::from_millis(250))),
            alive: Rc::new(Cell::new(true)),
            delay: None,
        };
        let first = Rc::new(mint(&storage.config).await?);
        storage.aliases.borrow_mut().waiting.push_back(first);
        rotate(
            storage.config.clone(),
            storage.aliases.clone(),
            storage.rotation.clone(),
            storage.alive.clone(),
        );
        Ok(storage)
    }

    /// Hold every snapshot open for `delay` before reading (tests).
    pub fn with_read_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Mint a fresh alias every `every`.
    pub fn with_rotation(self, every: Duration) -> Self {
        self.rotation.set(every);
        self
    }

    /// The current alias's consistent point, for inspection; `None` until
    /// the stream has passed the first alias.
    pub fn alias_position(&self) -> Option<Lsn> {
        self.aliases
            .borrow()
            .current
            .as_ref()
            .map(|alias| alias.lsn)
    }

    /// The current alias, or an error while the stream has not passed
    /// the first one yet (the runtime parks the read and asks again).
    fn current_alias(&self) -> Result<Rc<Alias>, StorageError> {
        self.aliases.borrow().current.clone().ok_or_else(|| {
            StorageError("no snapshot at or below the stream's position yet".to_owned())
        })
    }

    /// An idle connection, or a new one.
    async fn acquire(&self) -> Result<Client, StorageError> {
        let idle = self.idle.borrow_mut().pop();
        match idle {
            Some(client) => Ok(client),
            None => open(&self.config).await,
        }
    }

    /// Return a connection after a successful read; a failed read's
    /// connection is dropped instead, in case it is broken.
    fn release(&self, client: Client) {
        self.idle.borrow_mut().push(client);
    }
}

impl Storage for PgStorage {
    /// One `REPEATABLE READ`, read-only transaction importing the current
    /// alias's exported snapshot; the `SELECT` runs against that snapshot
    /// and is positioned at the alias's consistent point.
    async fn select(&self, query: &SingleTableReadQuery) -> Result<Snapshot, StorageError> {
        let table = self.catalog.table(query.table.as_str()).ok_or_else(|| {
            StorageError(format!("table `{}` is not in the catalog", query.table))
        })?;
        let alias = self.current_alias()?;
        let mut client = self.acquire().await?;
        let result = read_snapshot(&mut client, table, query, &alias, self.delay).await;
        if result.is_ok() {
            self.release(client);
        }
        result
    }

    /// Flip to the newest minted alias the stream has passed.
    fn advance(&self, feed: Lsn) {
        self.aliases.borrow_mut().advance(feed);
    }

    /// The current alias's consistent point; zero while there is none.
    fn floor(&self) -> Lsn {
        self.alias_position().unwrap_or_default()
    }
}

/// Keep minting aliases every `rotation` until `alive` clears, pausing
/// while enough are already waiting for the stream; a failed mint is
/// reported and tried again at the next tick.
fn rotate(
    config: Config,
    aliases: Rc<RefCell<Aliases>>,
    rotation: Rc<Cell<Duration>>,
    alive: Rc<Cell<bool>>,
) {
    spawn_local(async move {
        while alive.get() {
            tokio::time::sleep(rotation.get()).await;
            if !alive.get() {
                break;
            }
            if aliases.borrow().waiting.len() >= WAITING_ALIASES {
                continue;
            }
            match mint(&config).await {
                Ok(fresh) => aliases.borrow_mut().waiting.push_back(Rc::new(fresh)),
                Err(error) => {
                    eprintln!("snapshot minting failed, keeping the current alias: {error}")
                }
            }
        }
    });
}

/// Open one connection and drive it on the local task set.
async fn open(config: &Config) -> Result<Client, StorageError> {
    let (client, connection) = config.connect(NoTls).await?;
    spawn_local(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection ended: {error}");
        }
    });
    Ok(client)
}

/// Run one positioned read on `client`: import the alias's exported
/// snapshot as the transaction's first statement, run the `SELECT`
/// against it, and position the result at the alias's consistent point.
async fn read_snapshot(
    client: &mut Client,
    table: &DbTable,
    query: &SingleTableReadQuery,
    alias: &Alias,
    delay: Option<Duration>,
) -> Result<Snapshot, StorageError> {
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    transaction
        .batch_execute(&format!(
            "SET TRANSACTION SNAPSHOT '{}'",
            alias.snapshot.replace('\'', "''")
        ))
        .await?;
    if let Some(delay) = delay {
        transaction
            .execute("SELECT pg_sleep($1)", &[&delay.as_secs_f64()])
            .await?;
    }
    let rows = transaction
        .query(&sql::select_sql(query, table), &[])
        .await?;
    transaction.commit().await?;
    let rows = rows
        .iter()
        .map(|row| decode_row(row, table))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Snapshot {
        rows,
        at: alias.lsn,
    })
}

/// Parse Postgres's `X/Y`.
pub fn parse_lsn(text: &str) -> Result<Lsn, StorageError> {
    Lsn::parse(text).ok_or_else(|| StorageError(format!("unreadable WAL location `{text}`")))
}

/// One result row, in [`sql::select_columns`] order, as the engine's
/// (identity, image) pair.
fn decode_row(row: &Row, table: &DbTable) -> Result<(DataFrameKey, DataFrameRow), StorageError> {
    let mut data: HashMap<String, Value> = HashMap::new();
    for (index, column) in sql::select_columns(table).into_iter().enumerate() {
        let declared = &table.columns[column].r#type;
        let value = decode_value(row, index, declared)?;
        data.insert(column.as_str().to_owned(), value);
    }
    let pkey_value: HashMap<String, Value> = table
        .pkey
        .iter()
        .map(|column| (column.as_str().to_owned(), data[column.as_str()].clone()))
        .collect();
    Ok((DataFrameKey::new(pkey_value), DataFrameRow { data }))
}

/// One column of a result row, decoded by the cast its declared type was
/// read with (list and map columns are read as their text form).
fn decode_value(row: &Row, index: usize, declared: &ValueType) -> Result<Value, StorageError> {
    let value = match declared {
        ValueType::Int => row.try_get::<_, Option<i64>>(index)?.map(Value::Int),
        ValueType::Float => row.try_get::<_, Option<f64>>(index)?.map(Value::Float),
        ValueType::String | ValueType::List(_) | ValueType::Map(_, _) => {
            row.try_get::<_, Option<String>>(index)?.map(Value::String)
        }
        ValueType::Bool => row.try_get::<_, Option<bool>>(index)?.map(Value::Bool),
        ValueType::Date => row
            .try_get::<_, Option<chrono::NaiveDate>>(index)?
            .map(Value::Date),
        ValueType::Datetime => row
            .try_get::<_, Option<chrono::NaiveDateTime>>(index)?
            .map(Value::Datetime),
    };
    Ok(value.unwrap_or(Value::Null))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minted alias becomes current only once the feed passes its
    /// point, and a newer alias the feed has passed supersedes older ones
    /// in one step.
    #[test]
    fn aliases_flip_only_behind_the_feed() {
        let alias = |lsn: u64| {
            Rc::new(Alias {
                snapshot: format!("snap-{lsn}"),
                lsn: Lsn(lsn),
                _minter: ReplicationConnection::detached(),
            })
        };
        let mut aliases = Aliases::default();
        aliases.waiting.push_back(alias(10));
        aliases.waiting.push_back(alias(20));
        aliases.waiting.push_back(alias(30));
        aliases.advance(Lsn(5));
        assert!(aliases.current.is_none());
        aliases.advance(Lsn(25));
        assert_eq!(aliases.current.as_ref().map(|a| a.lsn), Some(Lsn(20)));
        assert_eq!(aliases.waiting.len(), 1);
        aliases.advance(Lsn(25));
        assert_eq!(aliases.current.as_ref().map(|a| a.lsn), Some(Lsn(20)));
        aliases.advance(Lsn(30));
        assert_eq!(aliases.current.as_ref().map(|a| a.lsn), Some(Lsn(30)));
        assert!(aliases.waiting.is_empty());
    }
}
