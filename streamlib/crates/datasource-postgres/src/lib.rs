//! Postgres as a DataSource: snapshot reads, a logical replication feed, and slot lifecycle.

mod codec;
mod feed;
mod repl;
mod snapshots;
mod spawn;
mod sql;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use streamlib_datasource::{
    Catalog, Changes, Column, Feed, Lifecycle, Query, Result, Snapshot, Snapshots, SourceError,
    Table,
};
use tokio::task::JoinHandle;
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Config, NoTls};

pub use codec::{Lsn, PgCodec, PgType, TextCodec};
use repl::ReplConn;
use snapshots::{Aliases, Pool};
pub use spawn::Spawner;

/// Oldest Postgres this source supports (publication column lists need 15+).
const MIN_SERVER_VERSION: i32 = 160_000;

#[derive(Clone, Debug)]
pub struct PgConfig {
    pub dsn: String,
    pub publication: String,
    /// Tables `setup` publishes, as `schema.table`; empty means all tables.
    pub tables: Vec<String>,
    /// How often a new snapshot is created.
    pub rotation: Duration,
    /// How long a read waits for the first usable snapshot.
    pub first_read_wait: Duration,
    /// Prefix of the slots this source creates.
    pub slot_prefix: String,
    /// Most read connections open at once.
    pub read_connections: usize,
    /// Longest a single read may run.
    pub read_timeout: Duration,
}

impl PgConfig {
    pub fn new(dsn: &str, publication: &str) -> Self {
        PgConfig {
            dsn: dsn.into(),
            publication: publication.into(),
            tables: vec![],
            rotation: Duration::from_millis(200),
            first_read_wait: Duration::from_secs(30),
            slot_prefix: "streamlib".into(),
            read_connections: 8,
            read_timeout: Duration::from_secs(30),
        }
    }
}

pub struct PgSource {
    cfg: PgConfig,
    pg: Config,
    codec: Arc<dyn PgCodec>,
    spawner: Spawner,
    catalog: RwLock<Arc<Catalog>>,
    aliases: Arc<Mutex<Aliases>>,
    pool: Pool,
    acked: Arc<AtomicU64>,
    /// True while a feed task runs; snapshots are only minted then.
    streaming: Arc<AtomicBool>,
    /// Id of the newest feed task, so an old task ending doesn't clear `streaming`.
    feed_id: Arc<AtomicU64>,
    /// Serialises `open` so two callers can't both create a slot.
    opening: tokio::sync::Mutex<()>,
    open: Mutex<Option<Opened>>,
}

struct Opened {
    slot: String,
    start: Lsn,
    rotation: JoinHandle<()>,
    feed: Option<JoinHandle<()>>,
}

impl PgSource {
    pub fn new(cfg: PgConfig) -> Result<Self> {
        Self::with_codec(cfg, Arc::new(TextCodec))
    }

    pub fn with_codec(cfg: PgConfig, codec: Arc<dyn PgCodec>) -> Result<Self> {
        let pg: Config = cfg
            .dsn
            .parse()
            .map_err(|e| SourceError::Refused(format!("bad dsn: {e}")))?;
        if !matches!(pg.get_hosts(), [Host::Tcp(_)]) {
            return Err(SourceError::Refused(
                "the dsn must name exactly one TCP host".into(),
            ));
        }
        if pg.get_ssl_mode() == SslMode::Require {
            return Err(SourceError::Refused("TLS is not supported yet".into()));
        }
        Ok(PgSource {
            pool: Pool::new(
                pg.clone(),
                cfg.read_connections,
                cfg.read_timeout,
                Spawner::default(),
            ),
            cfg,
            pg,
            codec,
            spawner: Spawner::default(),
            catalog: RwLock::default(),
            aliases: Arc::default(),
            acked: Arc::default(),
            streaming: Arc::default(),
            feed_id: Arc::default(),
            opening: tokio::sync::Mutex::new(()),
            open: Mutex::new(None),
        })
    }

    /// Run background tasks (feed, snapshot minting, connections) on this runtime.
    pub fn with_runtime(mut self, handle: tokio::runtime::Handle) -> Self {
        self.spawner = Spawner::on(handle);
        self.pool.spawner = self.spawner.clone();
        self
    }

    /// The feed slot's name, while open.
    pub fn slot(&self) -> Option<String> {
        self.open.lock().unwrap().as_ref().map(|o| o.slot.clone())
    }

    pub async fn client(&self) -> Result<Client> {
        connect(&self.pg, &self.spawner).await
    }

    /// Drop this source's feed slots left inactive for `older_than` (crashed processes). Needs Postgres 17+.
    pub async fn cleanup_inactive_slots(&self, older_than: Duration) -> Result<Vec<String>> {
        let client = self.client().await?;
        if server_version(&client).await? < 170_000 {
            return Err(SourceError::Refused(
                "slot cleanup needs Postgres 17+ (inactive_since)".into(),
            ));
        }
        let prefix = format!("{}_feed_", self.cfg.slot_prefix);
        let age_ms = i64::try_from(older_than.as_millis()).unwrap_or(i64::MAX);
        let rows = client
            .query(
                "SELECT slot_name::text FROM pg_replication_slots \
                 WHERE starts_with(slot_name::text, $1) AND NOT active AND NOT temporary \
                   AND database = current_database() \
                   AND inactive_since <= now() - $2::bigint * interval '1 millisecond'",
                &[&prefix, &age_ms],
            )
            .await
            .map_err(pg_error)?;
        let mut dropped = vec![];
        for row in rows {
            let slot: String = row.get(0);
            match client
                .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
                .await
            {
                Ok(_) => dropped.push(slot),
                // Someone picked it up after we looked; leave it.
                Err(e) if e.code() == Some(&SqlState::OBJECT_IN_USE) => {}
                Err(e) => return Err(pg_error(e)),
            }
        }
        Ok(dropped)
    }

    fn ensure_open(&self) -> Result<()> {
        match *self.open.lock().unwrap() {
            Some(_) => Ok(()),
            None => Err(SourceError::Closed),
        }
    }

    fn table(&self, catalog: &Catalog, name: &str) -> Result<Arc<Table>> {
        self.ensure_open()?;
        catalog
            .table(name)
            .cloned()
            .ok_or_else(|| SourceError::Refused(format!("no table `{name}`")))
    }

    /// The slot's confirmed position, or `CannotResume` if the slot is gone.
    async fn confirmed(&self, slot: &str) -> Result<Option<Lsn>> {
        let row = self
            .client()
            .await?
            .query_opt(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .map_err(pg_error)?
            .ok_or_else(|| SourceError::CannotResume(format!("slot `{slot}` is gone")))?;
        row.get::<_, Option<String>>(0)
            .map(|t| Lsn::parse(&t))
            .transpose()
    }
}

impl Drop for PgSource {
    fn drop(&mut self) {
        let Some(o) = self.open.get_mut().unwrap().take() else {
            return;
        };
        o.rotation.abort();
        if let Some(f) = o.feed {
            f.abort();
        }
        // Best effort: the feed slot would otherwise keep WAL forever.
        if let Some(rt) = self.spawner.handle() {
            let (cfg, spawner) = (self.pg.clone(), self.spawner.clone());
            rt.spawn(async move { drop_slot(&cfg, &spawner, &o.slot).await });
        } else {
            tracing::warn!(slot = %o.slot, "source dropped without close; slot left behind");
        }
    }
}

pub(crate) async fn connect(cfg: &Config, spawner: &Spawner) -> Result<Client> {
    let (client, conn) = cfg.connect(NoTls).await.map_err(pg_error)?;
    spawner.spawn(async move {
        if let Err(e) = conn.await {
            tracing::debug!(error = %e, "postgres connection ended");
        }
    });
    Ok(client)
}

/// Bad SQL or data is permanent; other errors may pass on retry.
pub(crate) fn pg_error(e: tokio_postgres::Error) -> SourceError {
    let class = e.code().map(|c| &c.code()[..2]);
    let text = match e.as_db_error() {
        Some(db) => db.message().to_owned(),
        None => e.to_string(),
    };
    match class {
        Some("22" | "42") => SourceError::Refused(text),
        _ => SourceError::Transient(text),
    }
}

async fn server_version(client: &Client) -> Result<i32> {
    let row = client
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await
        .map_err(pg_error)?;
    Ok(row.get(0))
}

/// Drop a slot, retrying while Postgres still holds it for a closing connection.
async fn drop_slot(cfg: &Config, spawner: &Spawner, slot: &str) {
    let Ok(client) = connect(cfg, spawner).await else {
        tracing::warn!(slot, "can't connect to drop slot");
        return;
    };
    for _ in 0..50 {
        match client
            .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await
        {
            Ok(_) => return,
            Err(e) if e.code() == Some(&SqlState::OBJECT_IN_USE) => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            Err(e) => {
                tracing::warn!(slot, error = %e, "drop slot failed");
                return;
            }
        }
    }
    tracing::warn!(slot, "slot still in use; left behind");
}

/// Refuse publications that don't send every insert, update, delete and truncate.
async fn check_publication(client: &Client, publication: &str) -> Result<()> {
    let row = client
        .query_opt(
            "SELECT pubinsert AND pubupdate AND pubdelete AND pubtruncate \
             FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await
        .map_err(pg_error)?
        .ok_or_else(|| {
            SourceError::Refused(format!("publication `{publication}` not found; run setup"))
        })?;
    if !row.get::<_, bool>(0) {
        return Err(SourceError::Refused(format!(
            "publication `{publication}` must publish insert, update, delete and truncate"
        )));
    }
    Ok(())
}

/// Published tables with their published columns and primary keys.
async fn load_catalog(client: &Client, publication: &str, codec: &dyn PgCodec) -> Result<Catalog> {
    let rows = client
        .query(
            "SELECT pt.schemaname || '.' || pt.tablename, a.attname::text, t.typname::text, \
                    t.typtype::text, array_position(i.indkey::int2[], a.attnum), \
                    c.relreplident::text, pt.rowfilter IS NOT NULL \
             FROM pg_publication_tables pt \
             JOIN pg_namespace n ON n.nspname = pt.schemaname \
             JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename \
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped \
                  AND a.attname = ANY(pt.attnames) \
             JOIN pg_type t ON t.oid = a.atttypid \
             LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary \
             WHERE pt.pubname = $1 \
             ORDER BY 1, a.attnum",
            &[&publication],
        )
        .await
        .map_err(pg_error)?;
    let mut tables: Vec<(Table, Vec<(i32, String)>)> = vec![];
    for r in rows {
        let name: String = r.get(0);
        let column: String = r.get(1);
        let ty = PgType {
            name: r.get(2),
            kind: r.get::<_, String>(3).chars().next().unwrap_or('b'),
        };
        let key_pos: Option<i32> = r.get(4);
        let identity: String = r.get(5);
        let filtered: bool = r.get(6);
        if filtered {
            return Err(SourceError::Refused(format!(
                "`{name}` has a publication row filter; reads would see rows the stream hides"
            )));
        }
        // Default identity sends the primary key; full sends the whole row. Others can't key deletes.
        if identity != "d" && identity != "f" {
            return Err(SourceError::Refused(format!(
                "`{name}` needs REPLICA IDENTITY DEFAULT or FULL"
            )));
        }
        if tables.last().is_none_or(|(t, _)| t.name != name) {
            let table = Table {
                name: name.clone(),
                columns: vec![],
                key: vec![],
            };
            tables.push((table, vec![]));
        }
        let (table, keys) = tables.last_mut().unwrap();
        let value_type = codec.value_type(&ty).ok_or_else(|| {
            SourceError::Refused(format!(
                "`{name}.{column}`: type `{}` not supported by the codec",
                ty.name
            ))
        })?;
        table.columns.push(Column {
            name: column.clone(),
            ty: value_type,
        });
        if let Some(p) = key_pos {
            keys.push((p, column));
        }
    }
    let primary_keys = primary_key_sizes(client, publication).await?;
    let mut out = vec![];
    for (mut table, mut keys) in tables {
        let wanted = primary_keys.get(&table.name).copied().unwrap_or(0);
        if wanted == 0 {
            return Err(SourceError::Refused(format!(
                "`{}` has no primary key",
                table.name
            )));
        }
        if keys.len() != wanted {
            return Err(SourceError::Refused(format!(
                "`{}`: every primary key column must be published",
                table.name
            )));
        }
        keys.sort();
        table.key = keys.into_iter().map(|(_, c)| c).collect();
        out.push(table);
    }
    Ok(Catalog::new(out))
}

/// Number of primary key columns per published table.
async fn primary_key_sizes(
    client: &Client,
    publication: &str,
) -> Result<std::collections::HashMap<String, usize>> {
    let rows = client
        .query(
            "SELECT pt.schemaname || '.' || pt.tablename, coalesce(i.indnkeyatts, 0)::int \
             FROM pg_publication_tables pt \
             JOIN pg_namespace n ON n.nspname = pt.schemaname \
             JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename \
             LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary \
             WHERE pt.pubname = $1",
            &[&publication],
        )
        .await
        .map_err(pg_error)?;
    Ok(rows
        .into_iter()
        .map(|r| (r.get(0), r.get::<_, i32>(1) as usize))
        .collect())
}

/// SQL that gives read connections the same output settings as replication connections.
async fn session_settings(cfg: &Config) -> Result<String> {
    let mut conn = ReplConn::open(cfg).await?;
    let names = [
        "TimeZone",
        "DateStyle",
        "IntervalStyle",
        "extra_float_digits",
        "bytea_output",
    ];
    let select = names
        .iter()
        .map(|n| format!("current_setting('{n}')"))
        .collect::<Vec<_>>()
        .join(", ");
    let rows = conn.query(&format!("SELECT {select}")).await?;
    let row = rows
        .first()
        .ok_or_else(|| SourceError::Transient("no session settings returned".into()))?;
    let sets = names
        .iter()
        .zip(row)
        .filter_map(|(name, value)| {
            let value = value.as_ref()?.replace('\'', "''");
            Some(format!("SET {name} TO '{value}';"))
        })
        .collect::<Vec<_>>();
    Ok(sets.join(" "))
}

impl Snapshots for PgSource {
    type Position = Lsn;

    fn catalog(&self) -> Arc<Catalog> {
        self.catalog.read().unwrap().clone()
    }

    async fn select(&self, query: &Query) -> Result<Snapshot<Lsn>> {
        let table = self.table(&self.catalog(), &query.table)?;
        let sql = sql::select(&table, query)?;
        let alias = snapshots::current(&self.aliases, self.cfg.first_read_wait).await?;
        let rows = snapshots::read(
            &self.pool,
            &self.aliases,
            &alias,
            &table,
            &sql,
            self.codec.as_ref(),
        )
        .await?;
        Ok(Snapshot {
            rows,
            position: alias.lsn,
        })
    }

    fn advance(&self, upto: &Lsn) {
        self.aliases.lock().unwrap().advance(*upto);
    }

    fn floor(&self) -> Lsn {
        self.aliases.lock().unwrap().floor()
    }

    /// A capped `count(*)`; counts don't need a snapshot.
    async fn count(&self, query: &Query, cap: u64) -> Result<u64> {
        let table = self.table(&self.catalog(), &query.table)?;
        let capped = Query {
            order: vec![],
            limit: usize::try_from(cap).ok(),
            ..query.clone()
        };
        let sql = format!(
            "SELECT count(*) FROM ({}) AS q",
            sql::select(&table, &capped)?
        );
        let n = snapshots::scalar(&self.pool, &sql)
            .await?
            .unwrap_or_default();
        n.parse()
            .map_err(|_| SourceError::Transient(format!("bad count `{n}`")))
    }
}

impl Changes for PgSource {
    type Position = Lsn;

    async fn stream(&self, from: Option<&Lsn>) -> Result<Feed<Lsn>> {
        let (slot, created) = {
            let open = self.open.lock().unwrap();
            let o = open.as_ref().ok_or(SourceError::Closed)?;
            (o.slot.clone(), o.start)
        };
        let confirmed = self.confirmed(&slot).await?;
        let start = match from {
            // Postgres resumes from the confirmed point, so a fresh feed starts there.
            None => confirmed.map_or(created, |c| c.max(created)),
            Some(p) if confirmed.is_some_and(|c| c > *p) => {
                return Err(SourceError::CannotResume(format!("slot moved past {p}")));
            }
            Some(p) => *p,
        };
        // Snapshots must be at or after the feed's start.
        self.aliases.lock().unwrap().require(start);

        let (tx, feed) = Feed::channel(start, 256);
        let stream = feed::Stream {
            cfg: self.pg.clone(),
            slot,
            publication: self.cfg.publication.clone(),
            start,
            catalog: self.catalog(),
            codec: self.codec.clone(),
        };
        let id = self.feed_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (streaming, feed_id, acked) = (
            self.streaming.clone(),
            self.feed_id.clone(),
            self.acked.clone(),
        );
        streaming.store(true, Ordering::Relaxed);
        let task = self.spawner.spawn(async move {
            feed::run(stream, acked, tx).await;
            if feed_id.load(Ordering::Relaxed) == id {
                streaming.store(false, Ordering::Relaxed);
            }
        });
        if let Some(o) = self.open.lock().unwrap().as_mut()
            && let Some(old) = o.feed.replace(task)
        {
            old.abort();
        }
        Ok(feed)
    }

    fn ack(&self, upto: &Lsn) {
        self.acked.fetch_max(upto.0, Ordering::Relaxed);
    }
}

impl Lifecycle for PgSource {
    async fn setup(&self) -> Result<()> {
        let client = self.client().await?;
        let publication = sql::ident(&self.cfg.publication);
        let all_tables: Option<bool> = client
            .query_opt(
                "SELECT puballtables FROM pg_publication WHERE pubname = $1",
                &[&self.cfg.publication],
            )
            .await
            .map_err(pg_error)?
            .map(|r| r.get(0));
        let tables: Vec<_> = self
            .cfg
            .tables
            .iter()
            .map(|t| sql::table_ident(t))
            .collect();
        let sql = match all_tables {
            None if tables.is_empty() => format!("CREATE PUBLICATION {publication} FOR ALL TABLES"),
            None => format!(
                "CREATE PUBLICATION {publication} FOR TABLE {}",
                tables.join(", ")
            ),
            Some(true) => return Ok(()),
            Some(false) => {
                // Add tables configured since the publication was created.
                let published: Vec<String> = client
                    .query(
                        "SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1",
                        &[&self.cfg.publication],
                    )
                    .await
                    .map_err(pg_error)?
                    .into_iter()
                    .map(|r| r.get(0))
                    .collect();
                let missing: Vec<_> = self
                    .cfg
                    .tables
                    .iter()
                    .filter(|t| !published.contains(t))
                    .map(|t| sql::table_ident(t))
                    .collect();
                if missing.is_empty() {
                    return Ok(());
                }
                format!(
                    "ALTER PUBLICATION {publication} ADD TABLE {}",
                    missing.join(", ")
                )
            }
        };
        client.batch_execute(&sql).await.map_err(pg_error)
    }

    async fn open(&self) -> Result<()> {
        let _opening = self.opening.lock().await;
        if self.open.lock().unwrap().is_some() {
            return Ok(());
        }
        let client = self.client().await?;
        let version = server_version(&client).await?;
        if version < MIN_SERVER_VERSION {
            return Err(SourceError::Refused(format!(
                "needs Postgres 16+, server is {version}"
            )));
        }
        check_publication(&client, &self.cfg.publication).await?;
        let catalog = load_catalog(&client, &self.cfg.publication, self.codec.as_ref()).await?;
        *self.catalog.write().unwrap() = Arc::new(catalog);
        self.pool.set_session(session_settings(&self.pg).await?);

        // Create the feed slot first, so every later snapshot is at or after its start.
        let slot = format!(
            "{}_feed_{}",
            self.cfg.slot_prefix,
            uuid::Uuid::new_v4().simple()
        );
        let row = client
            .query_one(
                "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .await
            .map_err(pg_error)?;
        let start = Lsn::parse(row.get(0))?;
        let rotation = snapshots::rotate(
            self.spawner.clone(),
            self.pg.clone(),
            self.aliases.clone(),
            self.streaming.clone(),
            self.cfg.rotation,
            self.cfg.slot_prefix.clone(),
        );
        *self.open.lock().unwrap() = Some(Opened {
            slot,
            start,
            rotation,
            feed: None,
        });
        Ok(())
    }

    async fn close(&self) {
        let Some(o) = self.open.lock().unwrap().take() else {
            return;
        };
        self.streaming.store(false, Ordering::Relaxed);
        o.rotation.abort();
        if let Some(f) = o.feed {
            f.abort();
        }
        *self.aliases.lock().unwrap() = Aliases::default();
        drop_slot(&self.pg, &self.spawner, &o.slot).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(dsn: &str) -> bool {
        matches!(
            PgSource::new(PgConfig::new(dsn, "p")),
            Err(SourceError::Refused(_))
        )
    }

    #[test]
    fn refuses_connections_it_cant_make_safely() {
        assert!(refused("host=/tmp user=u dbname=d"));
        assert!(refused("postgresql://u@localhost/d?sslmode=require"));
        assert!(refused("not a dsn ="));
        assert!(!refused("postgresql://u@localhost/d"));
    }
}
