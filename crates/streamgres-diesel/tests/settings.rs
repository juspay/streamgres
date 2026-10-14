//! The process-wide settings pick a cache and a route by the URL a
//! connection is established with. Its own binary: `install` is once per
//! process.

use std::sync::Arc;

use diesel::prelude::*;
use streamgres_diesel::cache::Cache;
use streamgres_diesel::key::QueryKey;
use streamgres_diesel::subscriptions::{Subscriptions, Verdict};
use streamgres_diesel::types::TypeLookup;
use streamgres_diesel::{Attachment, Route, Settings, SyncedConnection, install};

struct Unsynced;

impl Subscriptions for Unsynced {
    fn subscribe(&self, _: &QueryKey, _: &mut dyn TypeLookup) -> Verdict {
        Verdict::Refused("test".into())
    }
    fn unsubscribe(&self, _: &QueryKey) {}
}

fn url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres:///streamgres_diesel_test?host=/tmp".to_string())
}

#[test]
fn a_cache_per_database_and_a_route_per_pool() {
    let main = Arc::new(Cache::new(1024, Arc::new(Unsynced)));
    let accounts = Arc::new(Cache::new(1024, Arc::new(Unsynced)));
    let (main_for, accounts_for) = (Arc::clone(&main), Arc::clone(&accounts));
    install(Settings::by_url(move |url| {
        // The application name stands in for the database here; a real
        // configuration keys on the database the URL names.
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
