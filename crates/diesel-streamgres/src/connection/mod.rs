//! The connections an application swaps its own for, and what decides
//! which cache each one reads through.
//!
//! [`SyncedConnection`] wraps a blocking Diesel connection and
//! [`asynchronous::AsyncSyncedConnection`] a diesel-async one; both
//! follow the same rules. [`Settings`] says, by the URL a connection is
//! established with, which cache it reads through and the route its
//! reads take, and makes such connections and pools of them. [`install`]
//! puts one in place for the whole process, for code that establishes
//! connections through Diesel's own `establish` and pools.

#[cfg(feature = "async")]
pub mod asynchronous;
mod policy;
mod synchronous;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::cache::Cache;
use crate::route::Route;

pub use self::synchronous::{SyncedConnection, SyncedManager};

/// What a connection established with some URL gets: the cache its
/// reads go through and the route they take by default.
#[derive(Clone)]
pub struct Attachment {
    pub cache: Arc<Cache>,
    pub route: Route,
}

/// What a process shares between its connections, by the URL they are
/// established with.
///
/// One engine streams one database, and a cache is kept fresh by one
/// engine, so a cache is per database: pools against the same database
/// (a master and its replicas) share one, pools against different
/// databases each get their own, attached to the engine streaming that
/// database. The route is
/// per pool: a master pool can go to Postgres while a replica pool goes
/// through Streamgres.
///
/// Settings make connections and pools themselves
/// ([`Settings::establish`], [`Settings::manager`], and their async
/// twins), so a process may hold several, each with its own caches, and
/// a test its own, with no global state. [`install`] puts one in place
/// for every connection Diesel establishes, for an application whose
/// pools call `establish` themselves. A clone shares the same
/// attachments.
#[derive(Clone)]
pub struct Settings {
    for_url: Arc<AttachmentFor>,
}

/// What [`Settings`] asks of a URL.
type AttachmentFor = dyn Fn(&str) -> Option<Attachment> + Send + Sync;

impl Settings {
    /// One database: every connection shares `cache`, routed by its URL.
    pub fn single(
        cache: Arc<Cache>,
        route_for: impl Fn(&str) -> Route + Send + Sync + 'static,
    ) -> Settings {
        Settings::by_url(move |url| {
            Some(Attachment {
                cache: Arc::clone(&cache),
                route: route_for(url),
            })
        })
    }

    /// A cache and a route per URL. A URL that yields `None` gets a
    /// passthrough connection: every read goes to Postgres.
    pub fn by_url(
        for_url: impl Fn(&str) -> Option<Attachment> + Send + Sync + 'static,
    ) -> Settings {
        Settings {
            for_url: Arc::new(for_url),
        }
    }

    /// What a connection established with `url` gets.
    pub fn attachment_for(&self, url: &str) -> Option<Attachment> {
        (self.for_url)(url)
    }
}

/// The settings [`install`] put in place, for every connection
/// established after.
static SETTINGS: OnceLock<Settings> = OnceLock::new();

/// How long after a write a connection's reads go to Postgres when the
/// environment says nothing.
pub const DEFAULT_READ_YOUR_WRITES: Duration = Duration::from_millis(1000);

/// `STREAMGRES_READ_YOUR_WRITES_MS`: how long after a write a
/// connection's reads go to Postgres when the engine reports no
/// position, so a flow that writes and then reads sees its own write
/// before the engine has shipped it. 0 turns read-your-writes off, by
/// position too. Read once per process.
pub fn read_your_writes_from_env() -> Duration {
    static WINDOW: OnceLock<Duration> = OnceLock::new();
    *WINDOW.get_or_init(|| {
        std::env::var("STREAMGRES_READ_YOUR_WRITES_MS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_READ_YOUR_WRITES)
    })
}

/// Installs the process-wide settings. Connections established after this
/// (including by pools, which call [`Connection::establish`](diesel::Connection::establish)) use them;
/// before it, every connection is a passthrough. Returns the settings back
/// if some were already installed. A convenience over the settings' own
/// [`Settings::establish`] and [`Settings::manager`], which need no
/// install.
pub fn install(settings: Settings) -> Result<(), Settings> {
    SETTINGS.set(settings)
}
