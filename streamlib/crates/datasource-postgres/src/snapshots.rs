//! Consistent reads from exported snapshots, used only once the stream has passed them.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use streamlib_datasource::{Result, Row, SourceError, Table, Value};
use tokio::sync::Semaphore;
use tokio_postgres::{Client, Config, SimpleQueryMessage};

use crate::codec::{Lsn, PgCodec};
use crate::repl::ReplConn;
use crate::{connect, pg_error};

/// An exported snapshot and its WAL position; it lives as long as its connection.
pub struct Alias {
    pub snapshot: String,
    pub lsn: Lsn,
    _conn: ReplConn,
}

/// The snapshot reads use now, plus newer ones waiting for the stream to pass them.
#[derive(Default)]
pub struct Aliases {
    current: Option<Arc<Alias>>,
    waiting: VecDeque<Arc<Alias>>,
    /// Snapshots older than this can't be used by the current feed.
    min: Lsn,
    /// The current snapshot died; reads fail fast until a newer one is promoted.
    lost: bool,
}

impl Aliases {
    /// Make the newest snapshot the stream has passed the current one.
    pub fn advance(&mut self, upto: Lsn) {
        while self.waiting.front().is_some_and(|a| a.lsn <= upto) {
            let next = self.waiting.pop_front();
            if next.as_ref().is_some_and(|a| a.lsn >= self.min) {
                self.current = next;
                self.lost = false;
            }
        }
    }

    /// Forget snapshots older than a new feed's start.
    pub fn require(&mut self, min: Lsn) {
        self.min = self.min.max(min);
        if self.current.as_ref().is_some_and(|a| a.lsn < self.min) {
            self.current = None;
        }
        self.waiting.retain(|a| a.lsn >= self.min);
    }

    /// Drop the current snapshot if it is the one that failed.
    pub fn lose(&mut self, alias: &Arc<Alias>) {
        if self.current.as_ref().is_some_and(|a| Arc::ptr_eq(a, alias)) {
            self.current = None;
            self.lost = true;
        }
    }

    pub fn floor(&self) -> Lsn {
        self.current.as_ref().map_or(self.min, |a| a.lsn)
    }
}

/// How many snapshots may wait for the stream at once.
const MAX_WAITING: usize = 2;

async fn mint(cfg: &Config, prefix: &str) -> Result<Alias> {
    let mut conn = ReplConn::open(cfg).await?;
    let slot = format!("{prefix}_snap_{}", uuid::Uuid::new_v4().simple());
    let rows = conn
        .query(&format!(
            "CREATE_REPLICATION_SLOT \"{slot}\" TEMPORARY LOGICAL pgoutput EXPORT_SNAPSHOT"
        ))
        .await?;
    let bad = || SourceError::Transient("CREATE_REPLICATION_SLOT: unexpected reply".into());
    let row = rows.first().ok_or_else(bad)?;
    let cell = |i: usize| row.get(i).cloned().flatten().ok_or_else(bad);
    Ok(Alias {
        lsn: Lsn::parse(&cell(1)?)?,
        snapshot: cell(2)?,
        _conn: conn,
    })
}

/// Keeps creating snapshots while a feed runs, and nudges the stream so an idle database advances.
pub fn rotate(
    cfg: Config,
    aliases: Arc<Mutex<Aliases>>,
    streaming: Arc<AtomicBool>,
    every: Duration,
    prefix: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker: Option<Client> = None;
        loop {
            tokio::time::sleep(every).await;
            if !streaming.load(Ordering::Relaxed) {
                continue;
            }
            let waiting = aliases.lock().unwrap().waiting.len();
            if waiting < MAX_WAITING {
                match mint(&cfg, &prefix).await {
                    Ok(a) => aliases.lock().unwrap().waiting.push_back(Arc::new(a)),
                    Err(e) => tracing::warn!(error = %e, "snapshot mint failed"),
                }
            }
            if aliases.lock().unwrap().waiting.is_empty() {
                continue;
            }
            if ticker.as_ref().is_none_or(Client::is_closed) {
                ticker = connect(&cfg).await.ok();
            }
            if let Some(c) = &ticker {
                let tick = "SELECT pg_logical_emit_message(true, 'streamlib.tick', '')";
                if let Err(e) = c.simple_query(tick).await {
                    tracing::warn!(error = %e, "stream nudge failed");
                }
            }
        }
    })
}

/// Read connections: capped, reused, and set up with the stream's session settings.
pub struct Pool {
    cfg: Config,
    idle: Mutex<Vec<Client>>,
    permits: Semaphore,
    timeout: Duration,
    /// SQL that gives a new connection the same output settings as the replication stream.
    session: RwLock<String>,
}

impl Pool {
    pub fn new(cfg: Config, max: usize, timeout: Duration) -> Self {
        Pool {
            cfg,
            idle: Mutex::default(),
            permits: Semaphore::new(max.max(1)),
            timeout,
            session: RwLock::default(),
        }
    }

    pub fn set_session(&self, sql: String) {
        *self.session.write().unwrap() = sql;
        self.idle.lock().unwrap().clear();
    }

    async fn take(&self) -> Result<Client> {
        let reuse = self.idle.lock().unwrap().pop();
        if let Some(c) = reuse.filter(|c| !c.is_closed()) {
            return Ok(c);
        }
        let client = connect(&self.cfg).await?;
        let session = self.session.read().unwrap().clone();
        if !session.is_empty() {
            client.batch_execute(&session).await.map_err(pg_error)?;
        }
        Ok(client)
    }

    fn put(&self, c: Client) {
        if !c.is_closed() {
            self.idle.lock().unwrap().push(c);
        }
    }

    /// Run a batch on a pooled connection, within the read timeout.
    async fn run(&self, batch: &str) -> std::result::Result<Vec<SimpleQueryMessage>, Failed> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| SourceError::Closed)?;
        let client = self.take().await?;
        // Server-side limit first; the client-side one also covers a dead network.
        let grace = self.timeout + Duration::from_secs(1);
        match tokio::time::timeout(grace, client.simple_query(batch)).await {
            Err(_) => Err(Failed::Source(SourceError::Transient(format!(
                "read timed out after {:?}",
                self.timeout
            )))),
            Ok(Ok(messages)) => {
                self.put(client);
                Ok(messages)
            }
            Ok(Err(e)) => {
                if client.batch_execute("ROLLBACK").await.is_ok() {
                    self.put(client);
                }
                Err(Failed::Pg(e))
            }
        }
    }
}

/// Why a pooled batch failed: a Postgres error to classify, or anything else.
enum Failed {
    Pg(tokio_postgres::Error),
    Source(SourceError),
}

impl From<SourceError> for Failed {
    fn from(e: SourceError) -> Self {
        Failed::Source(e)
    }
}

impl From<Failed> for SourceError {
    fn from(f: Failed) -> Self {
        match f {
            Failed::Pg(e) => pg_error(e),
            Failed::Source(e) => e,
        }
    }
}

/// True when Postgres couldn't import the snapshot, because its exporting connection is gone.
fn snapshot_gone(e: &tokio_postgres::Error) -> bool {
    use tokio_postgres::error::SqlState;
    e.as_db_error().is_some_and(|db| {
        let code = db.code();
        // 42704 "snapshot does not exist", 22023 invalid id, 55000 exporter gone.
        (code == &SqlState::UNDEFINED_OBJECT
            || code == &SqlState::INVALID_PARAMETER_VALUE
            || code == &SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
            && db.message().contains("snapshot")
    })
}

/// The current snapshot; waits for the first one, fails fast if the last one was lost.
pub async fn current(aliases: &Mutex<Aliases>, wait: Duration) -> Result<Arc<Alias>> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        {
            let a = aliases.lock().unwrap();
            if let Some(alias) = a.current.clone() {
                return Ok(alias);
            }
            if a.lost {
                return Err(SourceError::Transient(
                    "snapshot lost; retry once the stream advances".into(),
                ));
            }
        }
        if tokio::time::Instant::now() > deadline {
            return Err(SourceError::Transient(
                "no snapshot became current; is the stream being read?".into(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Run `sql` inside the snapshot in a single round trip.
pub async fn read(
    pool: &Pool,
    aliases: &Mutex<Aliases>,
    alias: &Arc<Alias>,
    table: &Table,
    sql: &str,
    codec: &dyn PgCodec,
) -> Result<Vec<Row>> {
    let batch = format!(
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; \
         SET TRANSACTION SNAPSHOT '{}'; \
         SET LOCAL statement_timeout = {}; \
         {sql}; COMMIT",
        alias.snapshot.replace('\'', "''"),
        pool.timeout.as_millis()
    );
    let messages = match pool.run(&batch).await {
        Err(Failed::Pg(e)) if snapshot_gone(&e) => {
            // Retryable, not a bad query: a newer snapshot will replace it.
            aliases.lock().unwrap().lose(alias);
            return Err(SourceError::Transient(format!("snapshot lost: {e}")));
        }
        other => other?,
    };
    let mut rows = vec![];
    for m in messages {
        let SimpleQueryMessage::Row(r) = m else {
            continue;
        };
        let mut row = Row::new();
        for (i, col) in table.columns.iter().enumerate() {
            let value = match r.get(i) {
                Some(text) => codec.decode(col.ty, text)?,
                None => Value::Null,
            };
            row.insert(col.name.clone(), value);
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Run a query outside any snapshot and return its first value.
pub async fn scalar(pool: &Pool, sql: &str) -> Result<Option<String>> {
    let messages = pool.run(sql).await.map_err(SourceError::from)?;
    Ok(messages
        .into_iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).map(str::to_owned)),
            _ => None,
        })
        .flatten())
}
