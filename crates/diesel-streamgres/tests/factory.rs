//! Settings make connections and pools themselves, so a process can hold
//! several with no global state. Its own binary, with no `install` in it:
//! a connection Diesel establishes by itself stays a passthrough.

use std::sync::Arc;

use diesel::prelude::*;
use diesel::r2d2::Pool;
use diesel_streamgres::cache::Cache;
use diesel_streamgres::subscriptions::Unsynced;
use diesel_streamgres::{Route, Settings, SyncedConnection};

/// The test database's URL.
fn url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres:///diesel_streamgres_test?host=/tmp".to_string())
}

/// A cache no engine feeds.
fn cache() -> Arc<Cache> {
    Arc::new(Cache::new(1024, Arc::new(Unsynced)))
}

/// Two settings side by side each attach their own connections to their
/// own cache, and neither is in place for Diesel's own `establish`.
#[test]
fn settings_attach_their_own_connections() {
    let (first, second) = (cache(), cache());
    let one = Settings::single(Arc::clone(&first), |_| Route::Streamgres);
    let other = Settings::single(Arc::clone(&second), |_| Route::Postgres);

    let a = one.establish::<PgConnection>(&url()).expect("connect");
    let b = other.establish::<PgConnection>(&url()).expect("connect");
    assert!(Arc::ptr_eq(a.cache().expect("attached"), &first));
    assert_eq!(a.route(), Route::Streamgres);
    assert!(Arc::ptr_eq(b.cache().expect("attached"), &second));
    assert_eq!(b.route(), Route::Postgres);

    let plain = SyncedConnection::<PgConnection>::establish(&url()).expect("connect");
    assert!(plain.cache().is_none(), "nothing is installed");
}

/// A URL the settings give nothing is a passthrough, attached or
/// established.
#[test]
fn a_url_the_settings_skip_is_a_passthrough() {
    let settings = Settings::by_url(|_| None);
    let conn = settings.establish::<PgConnection>(&url()).expect("connect");
    assert!(conn.cache().is_none());
    assert_eq!(conn.route(), Route::Postgres);
    let attached = settings.attach(PgConnection::establish(&url()).expect("connect"), &url());
    assert!(attached.cache().is_none());
}

/// An r2d2 pool built on the settings' manager hands out connections
/// attached to their cache, which read through it.
#[test]
fn a_pool_hands_out_attached_connections() {
    let shared = cache();
    let settings = Settings::single(Arc::clone(&shared), |_| Route::Streamgres);
    let pool = Pool::builder()
        .max_size(2)
        .build(settings.manager::<PgConnection>(url()))
        .expect("pool");
    let mut conn = pool.get().expect("checkout");
    assert!(Arc::ptr_eq(conn.cache().expect("attached"), &shared));
    assert_eq!(conn.route(), Route::Streamgres);
    let one: i32 = diesel::select(1.into_sql::<diesel::sql_types::Integer>())
        .get_result(&mut *conn)
        .expect("read");
    assert_eq!(one, 1);
}

/// The async twin attaches the same way.
#[cfg(feature = "async")]
#[tokio::test]
async fn settings_attach_async_connections() {
    let shared = cache();
    let settings = Settings::single(Arc::clone(&shared), |_| Route::Streamgres);
    let conn = settings.establish_async(&url()).await.expect("connect");
    assert!(Arc::ptr_eq(conn.cache().expect("attached"), &shared));
    assert_eq!(conn.route(), Route::Streamgres);
}
