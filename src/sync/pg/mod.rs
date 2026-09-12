//! Postgres as a source. [`PgStorage`] answers the engine's reads from
//! one `REPEATABLE READ` snapshot each, positioned one of two ways.
//!
//! - **WAL method** ([`SnapshotMode::Wal`]). A background task keeps a
//!   current *alias*: a temporary logical replication slot created with
//!   `EXPORT_SNAPSHOT`, whose exported snapshot and consistent point
//!   Postgres pairs exactly (everything visible in the snapshot committed
//!   at or below the point, everything after it is not visible). Every
//!   read imports the current alias's snapshot with
//!   `SET TRANSACTION SNAPSHOT` and is positioned at its consistent point;
//!   the alias is re-minted on a cadence, and each alias lives as long as
//!   the replication connection that minted it, held until the last read
//!   that adopted it has finished.
//! - **XID method** ([`SnapshotMode::Xid`]). No temporary slot and no
//!   location asked of Postgres: the read's first statement returns
//!   `pg_current_snapshot()`, Postgres's own account of the transactions
//!   the snapshot sees, and that account is converted into a WAL location
//!   through the [`ledger::XidLedger`] the feed fills with every delivered
//!   transaction's id and commit location (see the ledger's docs for the
//!   rule).
//!
//! Either way the engine receives one location per read and one per
//! write. [`stream::PgStream`] delivers the change feed from a permanent
//! logical slot, positions every write at its commit, and fills the
//! ledger. Everything here runs on the engine's thread inside a
//! `tokio::task::LocalSet`.

pub mod ledger;
pub mod replication;
pub mod sql;
pub mod stream;

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::task::spawn_local;
use tokio_postgres::{Client, Config, IsolationLevel, NoTls, Row};

use super::storage::{Storage, StorageError};
use crate::model::{
    Catalog, DataFrameKey, DataFrameRow, DbTable, Lsn, Snapshot, SingleTableReadQuery, Value,
    ValueType,
};
use replication::ReplicationConnection;

pub use ledger::{SharedLedger, XidLedger};
pub use stream::{Batch, PgStream};

/// How a read's snapshot is positioned (see the module docs): by an
/// exported snapshot's consistent point, or by converting the snapshot's
/// transaction ids through the feed's ledger.
#[derive(Clone)]
pub enum SnapshotMode {
    Wal,
    Xid(SharedLedger),
}

impl SnapshotMode {
    /// Whether this is the WAL method.
    fn is_wal(&self) -> bool {
        matches!(self, SnapshotMode::Wal)
    }
}

/// One minted alias of the WAL method: an exported snapshot and the
/// consistent point it was built at, alive as long as the replication
/// connection that created its temporary slot.
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
    let lsn = parse_lsn(row.get(1).and_then(|column| column.as_deref()).ok_or_else(unreadable)?)?;
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

/// Read access to a Postgres database.
///
/// - `config`: how to connect; one connection is opened per read in
///   flight and kept for reuse in `idle`, since each read is its own
///   transaction.
/// - `catalog`: the tables' declared columns, which the `SELECT` casts to
///   and the rows decode by.
/// - `mode`: how snapshots are positioned.
/// - `alias`: the WAL method's current alias, swapped by the rotation
///   task or by a read that found it older than its floor; a read clones
///   the handle and keeps it until it is done.
/// - `rotation`: how often the WAL method mints a fresh alias.
/// - `alive`: cleared on drop, which ends the rotation task.
/// - `stats_minted_on_demand`: aliases minted because a read's floor was
///   past the current one.
/// - `delay`: a test hook: hold every snapshot open this long before
///   reading, so writes can be committed behind it deliberately.
pub struct PgStorage {
    config: Config,
    catalog: Rc<Catalog>,
    mode: SnapshotMode,
    idle: RefCell<Vec<Client>>,
    alias: Rc<RefCell<Option<Rc<Alias>>>>,
    rotation: Rc<Cell<Duration>>,
    alive: Rc<Cell<bool>>,
    stats_minted_on_demand: Cell<u64>,
    delay: Option<Duration>,
}

impl Drop for PgStorage {
    /// End the rotation task.
    fn drop(&mut self) {
        self.alive.set(false);
    }
}

impl PgStorage {
    /// Connect once (validating `dsn`) and keep that connection for the
    /// first read; in the WAL method also mint the first alias and start
    /// re-minting it every [`PgStorage::with_rotation`] interval (250 ms
    /// by default).
    pub async fn connect(dsn: &str, catalog: Rc<Catalog>, mode: SnapshotMode) -> Result<Self, StorageError> {
        let config: Config = dsn.parse()?;
        let client = open(&config).await?;
        let storage = PgStorage {
            config,
            catalog,
            mode,
            idle: RefCell::new(vec![client]),
            alias: Rc::new(RefCell::new(None)),
            rotation: Rc::new(Cell::new(Duration::from_millis(250))),
            alive: Rc::new(Cell::new(true)),
            stats_minted_on_demand: Cell::new(0),
            delay: None,
        };
        if storage.mode.is_wal() {
            *storage.alias.borrow_mut() = Some(Rc::new(mint(&storage.config).await?));
            rotate(
                storage.config.clone(),
                storage.alias.clone(),
                storage.rotation.clone(),
                storage.alive.clone(),
            );
        }
        Ok(storage)
    }

    /// Hold every snapshot open for `delay` before reading (tests).
    pub fn with_read_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Mint a fresh alias every `every` (WAL method).
    pub fn with_rotation(self, every: Duration) -> Self {
        self.rotation.set(every);
        self
    }

    /// The positioning mode.
    pub fn mode(&self) -> &SnapshotMode {
        &self.mode
    }

    /// The current alias's consistent point (WAL method), for inspection.
    pub fn alias_position(&self) -> Option<Lsn> {
        self.alias.borrow().as_ref().map(|alias| alias.lsn)
    }

    /// The current alias if its consistent point is at or past `at_least`,
    /// otherwise a freshly minted one (which also becomes current): a read
    /// must never see less than the engine has already applied.
    async fn fresh_alias(&self, at_least: Option<Lsn>) -> Result<Rc<Alias>, StorageError> {
        let current = self.alias.borrow().clone();
        if let Some(alias) = current
            && at_least.is_none_or(|floor| alias.lsn >= floor)
        {
            return Ok(alias);
        }
        self.stats_minted_on_demand.set(self.stats_minted_on_demand.get() + 1);
        let fresh = Rc::new(mint(&self.config).await?);
        *self.alias.borrow_mut() = Some(fresh.clone());
        Ok(fresh)
    }

    /// How many aliases were minted on demand because the current one was
    /// older than a read's floor (WAL method).
    pub fn minted_on_demand(&self) -> u64 {
        self.stats_minted_on_demand.get()
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
    /// One `REPEATABLE READ`, read-only transaction: the WAL method imports
    /// the current alias's exported snapshot (minting a fresh alias first
    /// if the current one is older than `at_least`), the XID method reads
    /// its position in its first statement (which is also what
    /// establishes the snapshot every later statement sees); then the
    /// `SELECT` runs against that same snapshot.
    async fn select(&self, query: &SingleTableReadQuery, at_least: Option<Lsn>) -> Result<Snapshot, StorageError> {
        let table = self
            .catalog
            .table(query.table.as_str())
            .ok_or_else(|| StorageError(format!("table `{}` is not in the catalog", query.table)))?;
        let alias = match &self.mode {
            SnapshotMode::Wal => Some(self.fresh_alias(at_least).await?),
            SnapshotMode::Xid(_) => None,
        };
        let mut client = self.acquire().await?;
        let result = read_snapshot(&mut client, table, query, &self.mode, alias, self.delay).await;
        if result.is_ok() {
            self.release(client);
        }
        result
    }
}

/// Keep minting aliases every `rotation` until `alive` clears; a failed
/// mint keeps the current alias.
fn rotate(config: Config, alias: Rc<RefCell<Option<Rc<Alias>>>>, rotation: Rc<Cell<Duration>>, alive: Rc<Cell<bool>>) {
    spawn_local(async move {
        while alive.get() {
            tokio::time::sleep(rotation.get()).await;
            if !alive.get() {
                break;
            }
            match mint(&config).await {
                Ok(fresh) => *alias.borrow_mut() = Some(Rc::new(fresh)),
                Err(error) => eprintln!("snapshot rotation failed, keeping the current alias: {error}"),
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

/// Run one positioned read on `client` (see the module docs): the WAL
/// method imports the alias's exported snapshot as the transaction's
/// first statement and is positioned at its consistent point; the XID
/// method reads `pg_current_snapshot()` in its first statement (which is
/// what establishes the snapshot) and converts it through the ledger once
/// the rows are in.
async fn read_snapshot(
    client: &mut Client,
    table: &DbTable,
    query: &SingleTableReadQuery,
    mode: &SnapshotMode,
    alias: Option<Rc<Alias>>,
    delay: Option<Duration>,
) -> Result<Snapshot, StorageError> {
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let mut seen: Option<(u64, u64, Vec<u64>)> = None;
    match mode {
        SnapshotMode::Wal => {
            let alias = alias
                .as_ref()
                .ok_or_else(|| StorageError("the WAL method has no snapshot alias yet".to_owned()))?;
            transaction
                .batch_execute(&format!(
                    "SET TRANSACTION SNAPSHOT '{}'",
                    alias.snapshot.replace('\'', "''")
                ))
                .await?;
        }
        SnapshotMode::Xid(_) => {
            let row = transaction
                .query_one("SELECT pg_current_snapshot()::text", &[])
                .await?;
            seen = Some(parse_xid_snapshot(row.get(0))?);
        }
    }
    if let Some(delay) = delay {
        transaction
            .execute("SELECT pg_sleep($1)", &[&delay.as_secs_f64()])
            .await?;
    }
    let rows = transaction.query(&sql::select_sql(query, table), &[]).await?;
    transaction.commit().await?;
    let rows = rows
        .iter()
        .map(|row| decode_row(row, table))
        .collect::<Result<Vec<_>, _>>()?;
    let at = match (mode, seen) {
        (SnapshotMode::Wal, _) => alias.map(|alias| alias.lsn).unwrap_or_default(),
        (SnapshotMode::Xid(ledger), Some((_, xmax, xip))) => {
            ledger.borrow().position_of(xmax, &xip).unwrap_or_default()
        }
        (SnapshotMode::Xid(_), None) => Lsn(0),
    };
    Ok(Snapshot { rows, at })
}

/// Parse Postgres's `X/Y`.
pub fn parse_lsn(text: &str) -> Result<Lsn, StorageError> {
    Lsn::parse(text).ok_or_else(|| StorageError(format!("unreadable WAL location `{text}`")))
}

/// Parse `pg_current_snapshot()`'s `xmin:xmax:xip1,xip2,…` into its three
/// parts (epoch-extended ids).
pub fn parse_xid_snapshot(text: &str) -> Result<(u64, u64, Vec<u64>), StorageError> {
    let unreadable = || StorageError(format!("unreadable snapshot `{text}`"));
    let mut parts = text.trim().split(':');
    let xmin = parts.next().and_then(|s| s.parse().ok()).ok_or_else(unreadable)?;
    let xmax = parts.next().and_then(|s| s.parse().ok()).ok_or_else(unreadable)?;
    let xip = parts
        .next()
        .unwrap_or("")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().map_err(|_| unreadable()))
        .collect::<Result<Vec<u64>, _>>()?;
    Ok((xmin, xmax, xip))
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

    /// The snapshot text form parses into Postgres's visibility triple.
    #[test]
    fn parses_snapshot_text() {
        assert_eq!(
            parse_xid_snapshot("725:730:726,728").unwrap(),
            (725, 730, vec![726, 728])
        );
        assert_eq!(parse_xid_snapshot("725:725:").unwrap(), (725, 725, Vec::new()));
        assert!(parse_xid_snapshot("garbage").is_err());
    }
}
