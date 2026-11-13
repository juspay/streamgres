//! Opt-in `diesel-async` reads. Cache waits and decoding run on Tokio's
//! blocking pool; cancelling the future does not block a runtime worker.
use crate::diesel::deserialize::FromSqlRow;
use crate::diesel::pg::{Pg, PgMetadataLookup, PgTypeMetadata};
use crate::diesel::query_builder::{AsQuery, Query, QueryFragment, QueryId};
use crate::diesel::query_dsl::CompatibleType;
use crate::diesel::result::{Error, QueryResult};
use crate::{Cache, CacheAttempt, Reader, Refusal, decode_cached, rendered, session};
use diesel_async::TransactionManager;
use diesel_async::methods::LoadQuery;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use std::future::Future;
use std::sync::{Arc, OnceLock};

/// A process-wide ceiling includes running and queued blocking cache work.
/// Saturation refuses the cache attempt so ordinary database reads continue.
const MAX_BLOCKING_JOBS: usize = 64;
fn worker_permit() -> Option<tokio::sync::OwnedSemaphorePermit> {
    static WORKERS: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    WORKERS
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(MAX_BLOCKING_JOBS)))
        .clone()
        .try_acquire_owned()
        .ok()
}

/// Async cached reads on `AsyncPgConnection`. Share the engine in an Arc;
/// blocking cache fills never occupy the async executor's worker thread.
pub trait AsyncLoadCached<'query, U: Send>: Sized + Send {
    fn load_cached_async(
        self,
        cache: Arc<Cache>,
        conn: &mut AsyncPgConnection,
    ) -> impl Future<Output = QueryResult<Vec<U>>> + Send;
    fn inspect_cached_async(
        self,
        cache: Arc<Cache>,
        conn: &mut AsyncPgConnection,
    ) -> impl Future<Output = QueryResult<CacheAttempt<Vec<U>>>> + Send;
}
impl<'query, T, U> AsyncLoadCached<'query, U> for T
where
    T: AsQuery + Send,
    T::Query: QueryFragment<Pg> + QueryId + LoadQuery<'query, AsyncPgConnection, U> + Send + 'query,
    T::SqlType: CompatibleType<U, Pg>,
    U: FromSqlRow<<T::SqlType as CompatibleType<U, Pg>>::SqlType, Pg> + Send + 'static,
{
    async fn load_cached_async(
        self,
        cache: Arc<Cache>,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<Vec<U>> {
        let mut query = self.as_query();
        match attempt::<_, U>(&mut query, cache, conn).await? {
            CacheAttempt::Hit(rows) => Ok(rows),
            CacheAttempt::Refused(_) => RunQueryDsl::load(query, conn).await,
        }
    }
    async fn inspect_cached_async(
        self,
        cache: Arc<Cache>,
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<CacheAttempt<Vec<U>>> {
        let mut query = self.as_query();
        attempt::<_, U>(&mut query, cache, conn).await
    }
}

/// Built-in bind types carry their OIDs statically. A custom type needs
/// async metadata resolution, so this version safely delegates it to Diesel.
#[derive(Default)]
struct BuiltinMetadata {
    custom: bool,
}
impl PgMetadataLookup for BuiltinMetadata {
    fn lookup_type(&mut self, _: &str, _: Option<&str>) -> PgTypeMetadata {
        self.custom = true;
        PgTypeMetadata::new(0, 0)
    }
}
async fn reader(conn: &mut AsyncPgConnection, cache: &Cache) -> Result<Reader, Refusal> {
    let status =
        <AsyncPgConnection as AsyncConnection>::TransactionManager::transaction_manager_status_mut(
            conn,
        );
    if !matches!(status.transaction_depth(), Ok(None)) {
        return Err(Refusal::NotReady(
            "an async transaction must read Postgres".into(),
        ));
    }
    let version = cache
        .catalog_version()
        .ok_or_else(|| Refusal::NotReady("the cache is starting".into()))?;
    let tracker = session::tracker_of(conn.instrumentation())
        .ok_or_else(|| Refusal::NotReady("the connection has no outer Tracker".into()))?;
    if tracker.session.in_transaction {
        return Err(Refusal::NotReady("a raw transaction is open".into()));
    }
    if let Some(reader) = tracker
        .session
        .current_reader()
        .filter(|reader| reader.is_current(&version))
    {
        return Ok(reader.clone());
    }
    let check = cache.reader_check()?;
    let cacheable = !session::tracker_of(conn.instrumentation())
        .is_some_and(|tracker| tracker.session.uncached_reader_check);
    let mut answer: QueryResult<crate::answer::ReaderAnswer> =
        crate::reader_query::ReaderQuery(&check, cacheable)
            .get_result(conn)
            .await;
    if answer.is_err() && cacheable {
        if let Some(tracker) = session::tracker_of(conn.instrumentation()) {
            tracker.session.uncached_reader_check = true;
        }
        answer = crate::reader_query::ReaderQuery(&check, false)
            .get_result(conn)
            .await;
    }
    let answer = answer
        .map_err(|_| Refusal::NotReady("the async reader identity could not be verified".into()))?;
    let reader = Reader::new(
        &answer.cluster,
        &answer.database,
        &answer.excluded,
        check.version,
    );
    if let Some(tracker) = session::tracker_of(conn.instrumentation()) {
        tracker.session.reader = Some((reader.clone(), std::time::Instant::now()));
        tracker.session.changed = false;
    }
    Ok(reader)
}
async fn attempt<Q, U>(
    query: &mut Q,
    cache: Arc<Cache>,
    conn: &mut AsyncPgConnection,
) -> QueryResult<CacheAttempt<Vec<U>>>
where
    Q: QueryFragment<Pg> + QueryId + Query + Send,
    Q::SqlType: CompatibleType<U, Pg>,
    U: FromSqlRow<<Q::SqlType as CompatibleType<U, Pg>>::SqlType, Pg> + Send + 'static,
{
    let mut metadata = BuiltinMetadata::default();
    let text = rendered(query, &mut metadata);
    if metadata.custom {
        let reason =
            Refusal::Unsupported("async custom bind metadata is delegated to Postgres".into());
        cache.report_refusal(&reason);
        return Ok(CacheAttempt::Refused(reason));
    }
    let (sql, binds) = text?;
    // Asked before the reader is checked: a refusal costs no round trip.
    if let Some(reason) = cache.refuses(&sql, &binds) {
        cache.report_refusal(&reason);
        return Ok(CacheAttempt::Refused(reason));
    }
    let reader = match reader(conn, &cache).await {
        Ok(reader) => reader,
        Err(reason) => {
            cache.report_refusal(&reason);
            return Ok(CacheAttempt::Refused(reason));
        }
    };
    let Some(permit) = worker_permit() else {
        let reason = Refusal::NotReady("the async cache worker budget is full".into());
        cache.report_refusal(&reason);
        return Ok(CacheAttempt::Refused(reason));
    };
    let event_sql = sql.clone();
    let events = session::CacheEvents::new(conn.instrumentation(), &event_sql);
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let answer = match cache.read(&sql, &binds, &reader) {
            Ok(answer) => answer,
            Err(reason) => return Ok(CacheAttempt::Refused(reason)),
        };
        match decode_cached::<Q, U>(&answer) {
            Ok(rows) => Ok(CacheAttempt::Hit(rows)),
            Err(_) => {
                cache.report_unusable();
                Ok(CacheAttempt::Refused(Refusal::Unsupported(
                    "the cached answer could not be decoded".into(),
                )))
            }
        }
    })
    .await
    .map_err(|error| Error::QueryBuilderError(format!("cache worker: {error}").into()))?;
    events.finish(result.as_ref().err());
    result
}

/// Wait for this async connection's visible WAL without blocking its executor.
pub async fn wait_for_writes_async(
    cache: Arc<Cache>,
    conn: &mut AsyncPgConnection,
    timeout: std::time::Duration,
) -> QueryResult<bool> {
    let now: crate::answer::WalNow = crate::diesel::sql_query(
        "SELECT (CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END)::text AS lsn"
    ).get_result(conn).await?;
    let position = crate::sync_position(&now.lsn)?;
    let Some(permit) = worker_permit() else {
        return Ok(false);
    };
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        cache.wait_for(position, timeout)
    })
    .await
    .map_err(|error| Error::QueryBuilderError(format!("cache wait worker: {error}").into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cancelled_requests_leave_capacity_with_their_worker() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let gate = Arc::new(tokio::sync::Semaphore::new(1));
            let permit = gate.clone().try_acquire_owned().unwrap();
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let worker = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            started_rx.await.unwrap();
            drop(worker); // Exactly what cancellation of the caller drops.
            assert!(gate.clone().try_acquire_owned().is_err());
            release_tx.send(()).unwrap();
            let _returned = gate.acquire().await.unwrap();
        });
    }
    #[test]
    fn blocking_job_admission_has_a_fixed_ceiling() {
        let permits: Vec<_> = (0..MAX_BLOCKING_JOBS)
            .map(|_| worker_permit().unwrap())
            .collect();
        assert!(worker_permit().is_none());
        drop(permits);
        assert!(worker_permit().is_some());
    }
}
