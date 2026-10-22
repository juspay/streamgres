//! The process-wide settings pick a cache and a route by the URL a
//! connection is established with. Its own binary: `install` is once per
//! process.

use std::sync::Arc;

use diesel::prelude::*;
use diesel_streamgres::cache::Cache;
use diesel_streamgres::key::QueryKey;
use diesel_streamgres::materialized::Snapshot;
use diesel_streamgres::subscriptions::{Outcome, Subscriptions, Verdict};
use diesel_streamgres::types::TableTypes;
use diesel_streamgres::{Attachment, Route, Settings, SyncedConnection, install};

/// An engine side that refuses everything.
struct Unsynced;

impl Subscriptions for Unsynced {
    /// Refused.
    fn subscribe(&self, _: &QueryKey) -> Outcome {
        Outcome::Verdict(Verdict::Refused("test".into()))
    }
    /// Refused.
    fn materialize(&self, _: &QueryKey, _: Snapshot, _: std::collections::HashMap<String, Arc<TableTypes>>) -> Verdict {
        Verdict::Refused("test".into())
    }
    /// Nothing to end.
    fn unsubscribe(&self, _: &QueryKey) {}
}

/// The test database's URL.
fn url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres:///diesel_streamgres_test?host=/tmp".to_string())
}

/// Settings attach a cache per database and a route per pool, by the URL a
/// connection is established with.
///
/// The application name stands in for the database here; a real
/// configuration keys on the database the URL names.
#[test]
fn a_cache_per_database_and_a_route_per_pool() {
    let main = Arc::new(Cache::new(1024, Arc::new(Unsynced)));
    let accounts = Arc::new(Cache::new(1024, Arc::new(Unsynced)));
    let (main_for, accounts_for) = (Arc::clone(&main), Arc::clone(&accounts));
    install(Settings::by_url(move |url| {
        if url.contains("application_name=accounts") {
            Some(Attachment {
                cache: Arc::clone(&accounts_for),
                route: Route::Streamgres,
            })
        } else if url.contains("application_name=main_replica") {
            Some(Attachment {
                cache: Arc::clone(&main_for),
                route: Route::Streamgres,
            })
        } else if url.contains("application_name=main") {
            Some(Attachment {
                cache: Arc::clone(&main_for),
                route: Route::Postgres,
            })
        } else {
            None
        }
    }))
    .ok();

    let sep = if url().contains('?') { "&" } else { "?" };
    let establish = |name: &str| {
        SyncedConnection::<PgConnection>::establish(&format!("{}{sep}application_name={name}", url()))
            .expect("connect")
    };

    let master = establish("main");
    let replica = establish("main_replica");
    let accounts_conn = establish("accounts");
    let other = establish("elsewhere");

    assert!(Arc::ptr_eq(master.cache().unwrap(), &main));
    assert!(Arc::ptr_eq(replica.cache().unwrap(), &main), "one cache per database");
    assert!(Arc::ptr_eq(accounts_conn.cache().unwrap(), &accounts), "another database, another cache");
    assert_eq!(master.route(), Route::Postgres, "the master pool goes to Postgres");
    assert_eq!(replica.route(), Route::Streamgres, "the replica pool through Streamgres");
    assert_eq!(accounts_conn.route(), Route::Streamgres);
    assert!(other.cache().is_none(), "a URL with no attachment is a passthrough");
    assert_eq!(other.route(), Route::Postgres);
}
