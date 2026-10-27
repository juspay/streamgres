//! A cached answer goes only to a connection that would have read the
//! same rows: one logged in as the same role, with the same search path,
//! whose session has not changed since. Against a live PostgreSQL
//! (`DATABASE_URL`, as a role that may create roles), with an engine side
//! that accepts every read, so only those rules stand between a cached
//! answer and a reader who must not see it.
//!
//! The table has row-level security on a tenant setting, as in the
//! security review that asked for this: a reader as one tenant must never
//! be served what was read as another.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::Text;
use diesel_streamgres::cache::Cache;
use diesel_streamgres::key::QueryKey;
use diesel_streamgres::subscriptions::{Outcome, Subscriptions, Verdict};
use diesel_streamgres::{Route, Served, SyncedConnection};

/// The test database's URL.
fn url() -> Option<String> {
    let url = std::env::var("DATABASE_URL").ok();
    if url.is_none() {
        eprintln!("DATABASE_URL not set: session context checks skipped");
    }
    url
}

/// An engine side that accepts every read and reports nothing.
struct AcceptAll;

impl Subscriptions for AcceptAll {
    /// Accepted.
    fn subscribe(&self, _: &QueryKey) -> Outcome {
        Outcome::Verdict(Verdict::Accepted { since: None })
    }

    /// Accepted.
    fn materialize(
        &self,
        _: &QueryKey,
        _: diesel_streamgres::materialized::Snapshot,
        _: std::collections::HashMap<String, Arc<diesel_streamgres::types::TableTypes>>,
    ) -> Verdict {
        Verdict::Accepted { since: None }
    }

    /// Nothing to end.
    fn unsubscribe(&self, _: &QueryKey) {}

    /// Never behind.
    fn behind(&self) -> bool {
        false
    }

    /// No positions.
    fn position(&self) -> Option<u64> {
        None
    }
}

/// One note's text.
#[derive(QueryableByName, Debug, PartialEq)]
struct Note {
    #[diesel(sql_type = Text)]
    body: String,
}

/// A test's table of tenant notes under row-level security, the role
/// that reads it with only `SELECT`, and a schema of its own; dropped
/// with it.
struct Tenants {
    url: String,
    table: String,
    role: String,
    schema: String,
}

impl Tenants {
    /// Sets them up for the test tagged `tag`: notes `ALICE_ONLY` for
    /// tenant `alice` and `BOB_ONLY` for `bob`, each visible only under
    /// its own `app.tenant`.
    fn create(url: String, tag: &str) -> Tenants {
        let pid = std::process::id();
        let tenants = Tenants {
            table: format!("sg_ctx_notes_{tag}_{pid}"),
            role: format!("sg_ctx_reader_{tag}_{pid}"),
            schema: format!("sg_ctx_{tag}_{pid}"),
            url,
        };
        let (table, role, schema) = (&tenants.table, &tenants.role, &tenants.schema);
        let mut admin = PgConnection::establish(&tenants.url).expect("connect");
        tenants.drop_all(&mut admin);
        admin
            .batch_execute(&format!(
                "CREATE TABLE {table} (id int PRIMARY KEY, tenant text NOT NULL, body text NOT NULL);
                 INSERT INTO {table} VALUES (1, 'alice', 'ALICE_ONLY'), (2, 'bob', 'BOB_ONLY');
                 ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;
                 CREATE POLICY tenant ON {table} USING (tenant = current_setting('app.tenant', true));
                 CREATE ROLE {role} LOGIN PASSWORD 'reader';
                 GRANT USAGE ON SCHEMA public TO {role};
                 GRANT SELECT ON {table} TO {role};
                 CREATE SCHEMA {schema};
                 GRANT USAGE ON SCHEMA {schema} TO {role};"
            ))
            .expect("set up the tenants");
        tenants
    }

    /// The URL as the reader role, with `options` added when given.
    fn reader_url(&self, options: Option<&str>) -> String {
        let (scheme, rest) = self.url.split_once("://").expect("a URL");
        let host = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
        let url = format!("{scheme}://{}:reader@{host}", self.role);
        match options {
            Some(options) => {
                let joiner = if url.contains('?') { '&' } else { '?' };
                format!("{url}{joiner}options={options}")
            }
            None => url,
        }
    }

    /// A wrapped connection at `url` through `cache`.
    fn connect(url: &str, cache: &Arc<Cache>) -> SyncedConnection<PgConnection> {
        let inner = PgConnection::establish(url).expect("connect");
        SyncedConnection::with_cache(inner, Arc::clone(cache), Route::Streamgres)
    }

    /// Every note `conn` reads.
    fn notes(&self, conn: &mut SyncedConnection<PgConnection>) -> Vec<String> {
        let notes: Vec<Note> = sql_query(format!("SELECT body FROM {} ORDER BY id", self.table))
            .load(conn)
            .expect("read the notes");
        notes.into_iter().map(|note| note.body).collect()
    }

    /// Drops the table, the role and the schema.
    fn drop_all(&self, admin: &mut PgConnection) {
        let (table, role, schema) = (&self.table, &self.role, &self.schema);
        let _ = admin.batch_execute(&format!(
            "DROP TABLE IF EXISTS {table};
             DROP SCHEMA IF EXISTS {schema};
             DO $$ BEGIN
               IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                 EXECUTE 'DROP OWNED BY {role}';
                 EXECUTE 'DROP ROLE {role}';
               END IF;
             END $$;"
        ));
    }
}

impl Drop for Tenants {
    fn drop(&mut self) {
        if let Ok(mut admin) = PgConnection::establish(&self.url) {
            self.drop_all(&mut admin);
        }
    }
}

/// A fresh cache with an engine side that accepts everything.
fn cache() -> Arc<Cache> {
    Arc::new(Cache::new(16 * 1024 * 1024, Arc::new(AcceptAll)))
}

/// Reads off the cache's count of reads sent to Postgres for their
/// connection's context.
fn other_context(cache: &Cache) -> u64 {
    cache.stats.other_context.load(Ordering::Relaxed)
}

/// The review's case: one connection reads as tenant `alice`, then
/// switches its session to `bob` and repeats the same SQL. It must read
/// `bob`'s note, from Postgres, and nothing read under a tenant setting
/// is kept for anyone.
#[test]
fn a_tenant_setting_is_never_served_another_tenants_rows() {
    let Some(url) = url() else { return };
    let tenants = Tenants::create(url, "tenant");
    let cache = cache();
    let mut conn = Tenants::connect(&tenants.reader_url(None), &cache);

    conn.batch_execute("SET app.tenant = 'alice'").expect("as alice");
    assert_eq!(tenants.notes(&mut conn), ["ALICE_ONLY"]);
    assert_eq!(tenants.notes(&mut conn), ["ALICE_ONLY"]);
    assert_eq!(conn.last_served(), Some(Served::Postgres), "a changed session reads Postgres");

    conn.batch_execute("SET app.tenant = 'bob'").expect("as bob");
    assert_eq!(tenants.notes(&mut conn), ["BOB_ONLY"], "never alice's cached rows");
    assert_eq!(conn.last_served(), Some(Served::Postgres));

    sql_query("SELECT set_config('app.tenant', 'alice', false)")
        .execute(&mut conn)
        .expect("as alice again, by function");
    assert_eq!(tenants.notes(&mut conn), ["ALICE_ONLY"]);
    assert_eq!(cache.len(), 0, "nothing read under a tenant is kept");
    assert!(other_context(&cache) >= 4, "each counted");

    conn.batch_execute("RESET ALL").expect("back as established");
    assert_eq!(tenants.notes(&mut conn), Vec::<String>::new(), "no tenant: no rows");
    assert_eq!(tenants.notes(&mut conn), Vec::<String>::new());
    assert_eq!(conn.last_served(), Some(Served::Cache), "the cache serves it again");
}

/// Two connections logged in as different roles never share an answer:
/// the owner reads every note and the cache keeps them; the reader, who
/// may see none without a tenant, reads Postgres and sees none.
#[test]
fn another_role_reads_postgres() {
    let Some(url) = url() else { return };
    let tenants = Tenants::create(url, "role");
    let cache = cache();
    let mut owner = Tenants::connect(&tenants.url, &cache);
    assert_eq!(tenants.notes(&mut owner), ["ALICE_ONLY", "BOB_ONLY"]);
    assert_eq!(tenants.notes(&mut owner), ["ALICE_ONLY", "BOB_ONLY"]);
    assert_eq!(owner.last_served(), Some(Served::Cache), "the owner's answer is cached");

    let mut reader = Tenants::connect(&tenants.reader_url(None), &cache);
    for _ in 0..2 {
        assert_eq!(tenants.notes(&mut reader), Vec::<String>::new(), "not the owner's rows");
        assert_eq!(reader.last_served(), Some(Served::Postgres));
    }
    assert_eq!(other_context(&cache), 2);
}

/// `SET ROLE` takes the connection off the cache until `RESET ROLE`.
#[test]
fn a_role_set_on_the_session_reads_postgres_until_reset() {
    let Some(url) = url() else { return };
    let tenants = Tenants::create(url, "setrole");
    let cache = cache();
    let mut owner = Tenants::connect(&tenants.url, &cache);
    assert_eq!(tenants.notes(&mut owner), ["ALICE_ONLY", "BOB_ONLY"]);

    owner
        .batch_execute(&format!("SET ROLE {}", tenants.role))
        .expect("as the reader");
    assert_eq!(tenants.notes(&mut owner), Vec::<String>::new(), "the reader's rows");
    assert_eq!(owner.last_served(), Some(Served::Postgres));

    owner.batch_execute("RESET ROLE").expect("as the owner again");
    assert_eq!(tenants.notes(&mut owner), ["ALICE_ONLY", "BOB_ONLY"]);
    assert_eq!(owner.last_served(), Some(Served::Cache));
}

/// A search path that puts another schema before `public` could make a
/// bare name another table than the engine's: that connection reads
/// Postgres, and its context is not the cache's. (A schema the role may
/// not use is left out of its path, and would change nothing.)
#[test]
fn a_search_path_before_public_reads_postgres() {
    let Some(url) = url() else { return };
    let tenants = Tenants::create(url, "path");
    let cache = cache();
    let options = format!("-c%20search_path%3D{}%2Cpublic", tenants.schema);
    let mut elsewhere = Tenants::connect(&tenants.reader_url(Some(&options)), &cache);
    elsewhere.batch_execute("SET app.tenant = 'alice'").ok();
    elsewhere.batch_execute("RESET ALL").expect("reset");
    for _ in 0..2 {
        tenants.notes(&mut elsewhere);
        assert_eq!(elsewhere.last_served(), Some(Served::Postgres));
    }
    assert_eq!(cache.context(), None, "a context the engine does not share is never the cache's");
}

/// The async wrapper keeps to the same rules: a tenant set on the
/// session is never served what was read as another.
#[cfg(feature = "async")]
#[tokio::test]
async fn the_async_wrapper_never_serves_another_tenants_rows() {
    use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
    use diesel_streamgres::AsyncSyncedConnection;

    let Some(url) = url() else { return };
    let tenants = tokio::task::spawn_blocking(move || Tenants::create(url, "async"))
        .await
        .expect("set up");
    let cache = cache();
    let inner = AsyncPgConnection::establish(&tenants.reader_url(None))
        .await
        .expect("connect");
    let mut conn = AsyncSyncedConnection::with_cache(inner, Arc::clone(&cache), Route::Streamgres);
    let sql = format!("SELECT body FROM {} ORDER BY id", tenants.table);
    let notes = async |conn: &mut AsyncSyncedConnection| -> Vec<String> {
        let notes: Vec<Note> = diesel_async::RunQueryDsl::load(sql_query(&sql), conn)
            .await
            .expect("read the notes");
        notes.into_iter().map(|note| note.body).collect()
    };
    for _ in 0..3 {
        assert_eq!(notes(&mut conn).await, Vec::<String>::new());
    }
    assert_eq!(
        conn.last_served(),
        Some(Served::Cache),
        "established, the cache serves it once its context is read and the miss filled"
    );

    conn.batch_execute("SET app.tenant = 'alice'").await.expect("as alice");
    assert_eq!(notes(&mut conn).await, ["ALICE_ONLY"], "not the cached empty answer");
    conn.batch_execute("SET app.tenant = 'bob'").await.expect("as bob");
    assert_eq!(notes(&mut conn).await, ["BOB_ONLY"]);
    assert_eq!(conn.last_served(), Some(Served::Postgres));

    conn.batch_execute("DISCARD ALL").await.expect("back as established");
    assert_eq!(notes(&mut conn).await, Vec::<String>::new(), "its context read again");
    assert_eq!(conn.last_served(), Some(Served::Postgres));
    assert_eq!(notes(&mut conn).await, Vec::<String>::new());
    assert_eq!(conn.last_served(), Some(Served::Cache), "the entry from before");
    tokio::task::spawn_blocking(move || drop(tenants)).await.expect("drop");
}
