//! The result cache: rows by query key, bounded by bytes, kept fresh by the
//! engine's touches.
//!
//! An entry is one of three things. The rows one Postgres read
//! returned, byte for byte, for a query the engine reports touches for.
//! The engine's own rows for a query it serves ([`Live`]): the result is
//! built from them when a read asks for it after a change, so a burst of
//! deltas costs one build, on the reading thread. Or a mark that the
//! query is stale: the engine reported it touched, and the next read
//! refills it from Postgres. The mark weighs only its key; it is kept so
//! that the clocks below still end the subscription of a query nobody
//! reads again.
//!
//! An entry is evicted when the cache is over its byte budget or the
//! entry has expired ([`Limits`]), either of which also ends the
//! subscription. Two clocks bound an entry's life. Idle time drops a
//! query nobody has read for a while, with its subscription, so a
//! backend's cache follows what it reads now; the engine changing a
//! query's rows is not a read. Time to live drops an entry on a timer
//! however often it is read: a safety net that makes the next read
//! resubscribe and refill from the engine, off by default since the
//! engine keeps the entry fresh.
//!
//! Between subscribing and filling there is a window: the read runs, and a
//! touch may arrive for the key before the rows are in. A generation per
//! key closes it. [`Cache::reserve`] takes the generation before the read,
//! [`Cache::fill`] stores the rows only if no touch bumped it since. A
//! generation exists only while a read holds a [`Reservation`] on its
//! key: a touch on a key nobody is filling has nothing to invalidate but
//! the entry, so the map holds the keys in flight, not every key ever
//! seen.
//!
//! The cache also keeps the one clock the read-your-writes window needs:
//! when any connection through it last wrote. A pool's connections
//! share a cache, so a write on one is followed by reads on another, and
//! those must see it too.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use diesel::pg::PgMetadataCache;
use moka::notification::RemovalCause;

use crate::key::QueryKey;
use crate::row::OwnedRow;
use crate::subscriptions::Subscriptions;

/// Rows the engine keeps current for one query, as an entry holds them.
pub trait Live: Send + Sync + std::fmt::Debug {
    /// The result as of the latest change, built now if a change came
    /// since it last was. `None` once the rows cannot be served (a value
    /// arrived that cannot be encoded): the query then falls back to
    /// refills, and the implementation marks it stale.
    fn rows(&self) -> Option<Arc<[OwnedRow]>>;

    /// What the rows held weigh.
    fn bytes(&self) -> usize;
}

/// What an entry holds.
#[derive(Debug)]
enum Content {
    /// The rows a Postgres read returned.
    Rows(Arc<[OwnedRow]>),
    /// The engine's rows, built into the result when read.
    Live(Arc<dyn Live>),
    /// Touched: the next read refills.
    Stale,
}

/// One cached query.
#[derive(Debug)]
pub struct Entry {
    content: Content,
    bytes: usize,
}

impl Entry {
    /// A Postgres read's rows.
    fn rows(rows: Arc<[OwnedRow]>, key: &QueryKey) -> Entry {
        let bytes = key.byte_len() + rows_bytes(&rows);
        Entry {
            content: Content::Rows(rows),
            bytes,
        }
    }

    /// The engine's rows: `built` is what the result weighs, when it has
    /// just been built; otherwise it is taken to weigh what the rows do.
    fn live(live: Arc<dyn Live>, key: &QueryKey, built: Option<usize>) -> Entry {
        let held = live.bytes();
        let bytes = key.byte_len() + held + built.unwrap_or(held);
        Entry {
            content: Content::Live(live),
            bytes,
        }
    }

    /// The mark of a touched query.
    fn stale(key: &QueryKey) -> Entry {
        Entry {
            content: Content::Stale,
            bytes: key.byte_len(),
        }
    }

    /// What the entry weighs against the byte budget.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

/// What a result's rows weigh.
fn rows_bytes(rows: &[OwnedRow]) -> usize {
    rows.iter().map(OwnedRow::byte_len).sum()
}

/// How many bytes of keys of refused queries are remembered at most.
const REFUSED_BYTES: u64 = 8 * 1024 * 1024;

/// What bounds the cache: bytes, idle time, time to live, and how long a
/// refusal is remembered. From the environment through
/// [`Limits::from_env`]:
///
/// | variable                           | meaning                                      | default   |
/// |------------------------------------|----------------------------------------------|-----------|
/// | `STREAMGRES_CACHE_MAX_BYTES`       | rows and keys held, in bytes                 | 268435456 |
/// | `STREAMGRES_CACHE_MAX_ENTRY_BYTES` | the largest result kept; 0 no limit          | 16777216  |
/// | `STREAMGRES_CACHE_IDLE_MS`         | drop an entry unread for this long; 0 never  | 1800000   |
/// | `STREAMGRES_CACHE_TTL_MS`          | drop an entry this old however read; 0 never | 0         |
/// | `STREAMGRES_REFUSED_TTL_MS`        | do not re-ask the engine about a refused query for this long; 0 always ask | 30000 |
///
/// A value that is missing or not a number leaves the default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    pub max_bytes: u64,
    /// A result larger than this is served but not kept, and its
    /// subscription ends: one query must not take the whole budget.
    pub max_entry_bytes: u64,
    pub time_to_idle: Option<Duration>,
    pub time_to_live: Option<Duration>,
    /// A query the engine refused (a shape outside its subset) would
    /// otherwise cost a round trip to the engine on every read; for this
    /// long it goes straight to Postgres instead.
    pub refused_for: Option<Duration>,
}

/// The cache's byte budget when the environment says nothing: 256 MiB.
pub const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// The largest result kept when the environment says nothing: 16 MiB.
pub const DEFAULT_MAX_ENTRY_BYTES: u64 = 16 * 1024 * 1024;
/// How long an unread entry stays when the environment says nothing.
pub const DEFAULT_TIME_TO_IDLE: Duration = Duration::from_secs(30 * 60);
/// How long a refusal is remembered when the environment says nothing.
pub const DEFAULT_REFUSED_FOR: Duration = Duration::from_secs(30);

impl Default for Limits {
    /// The documented defaults: idle expiry on, time to live off.
    fn default() -> Limits {
        Limits {
            max_bytes: DEFAULT_MAX_BYTES,
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
            time_to_idle: Some(DEFAULT_TIME_TO_IDLE),
            time_to_live: None,
            refused_for: Some(DEFAULT_REFUSED_FOR),
        }
    }
}

impl Limits {
    /// The limits from the environment, each missing or unreadable one
    /// at its default; a zero turns a clock or the entry limit off.
    pub fn from_env() -> Limits {
        let defaults = Limits::default();
        let millis = |name: &str, default: Option<Duration>| match env_number(name) {
            Some(0) => None,
            Some(millis) => Some(Duration::from_millis(millis)),
            None => default,
        };
        Limits {
            max_bytes: env_number("STREAMGRES_CACHE_MAX_BYTES").unwrap_or(defaults.max_bytes),
            max_entry_bytes: match env_number("STREAMGRES_CACHE_MAX_ENTRY_BYTES") {
                Some(0) => u64::MAX,
                Some(bytes) => bytes,
                None => defaults.max_entry_bytes,
            },
            time_to_idle: millis("STREAMGRES_CACHE_IDLE_MS", defaults.time_to_idle),
            time_to_live: millis("STREAMGRES_CACHE_TTL_MS", defaults.time_to_live),
            refused_for: millis("STREAMGRES_REFUSED_TTL_MS", defaults.refused_for),
        }
    }
}

/// A number from the environment.
fn env_number(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.trim().parse::<u64>().ok()
}

/// Counters for dashboards and tests.
#[derive(Debug, Default)]
pub struct Stats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    /// Reads the engine (or the client's own limits) refused; each one
    /// asked.
    pub refused: AtomicU64,
    /// Reads sent to Postgres without asking because the query was
    /// refused recently.
    pub refused_recently: AtomicU64,
    pub touched: AtomicU64,
    /// Entries dropped for the byte budget.
    pub evicted: AtomicU64,
    /// Entries dropped by the idle or time-to-live clock.
    pub expired: AtomicU64,
    /// Fills dropped because the key was touched during the read.
    pub stale_fills: AtomicU64,
    /// Results served but not kept because they were larger than
    /// [`Limits::max_entry_bytes`].
    pub oversized: AtomicU64,
    /// Entries the engine's own rows replaced (rows mode).
    pub replaced: AtomicU64,
    /// Reads sent to Postgres because a write had not reached the cache.
    pub after_write: AtomicU64,
    /// Reads sent to Postgres because the read's lock (`FOR UPDATE`)
    /// cannot be taken on a cache.
    pub locking: AtomicU64,
    /// Reads sent to Postgres because the engine was further behind the
    /// database than the cache may be.
    pub lagging: AtomicU64,
    /// Reads from a standby not kept: it had not replayed past the last
    /// change the engine will not report for the query.
    pub unreplayed: AtomicU64,
    /// Reads sent to Postgres because their connection's context cannot
    /// be served ([`Cache::context_id`]: a shadowed name, another
    /// database, one context too many) or its session has changed since
    /// it was established.
    pub other_context: AtomicU64,
}

impl Stats {
    /// Counts one more.
    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// A key's generation while reads are in flight for it.
#[derive(Default)]
struct Slot {
    generation: u64,
    inflight: u32,
}

/// The generation of every key with a read in flight.
type Generations = Arc<Mutex<HashMap<QueryKey, Slot>>>;

/// A read's claim on a key between reserving it and filling it. Dropping
/// it, with or without a fill, lets the key's generation go once no
/// other read holds one.
pub struct Reservation {
    generations: Generations,
    key: QueryKey,
    generation: u64,
}

impl Reservation {
    /// The key reserved.
    pub fn key(&self) -> &QueryKey {
        &self.key
    }
}

impl Drop for Reservation {
    /// Lets the key's generation go once no other read holds one.
    fn drop(&mut self) {
        let mut generations = self.generations.lock().expect("generations");
        if let Some(slot) = generations.get_mut(&self.key) {
            slot.inflight = slot.inflight.saturating_sub(1);
            if slot.inflight == 0 {
                generations.remove(&self.key);
            }
        }
    }
}

/// Query results by key, kept current by the engine side.
pub struct Cache {
    entries: moka::sync::Cache<QueryKey, Arc<Entry>>,
    generations: Generations,
    subscriptions: Arc<dyn Subscriptions>,
    /// Queries refused recently, with their values, each with the
    /// reason, dropped on their own clock. By the whole key, values and
    /// all: the engine refuses on values too (a time at infinity, more
    /// rows than it follows for this user), and a refusal for one value
    /// must not send every other value of the query to Postgres.
    refused: Option<moka::sync::Cache<QueryKey, Arc<str>>>,
    limits: Limits,
    /// When a connection through this cache last wrote, as nanoseconds
    /// since `started` plus one; 0 is never.
    last_write: AtomicU64,
    started: Instant,
    /// How many times a connection through this cache has marked a
    /// write; each write is marked as it starts and as it ends.
    writes: AtomicU64,
    /// How many of those marks [`Cache::target`] covers: every write
    /// marked by then had committed below it, if it committed at all.
    resolved: AtomicU64,
    /// A WAL position past the commit of every write [`Cache::resolved`]
    /// counts: once the engine side has delivered every change up to it,
    /// the cache holds them all.
    target: AtomicU64,
    /// Whether reads through the cache have gone to a standby, which
    /// cannot say where the primary's WAL is: writes then read it
    /// themselves, on their own connection, once committed.
    standby_reads: AtomicBool,
    /// The OIDs of user-defined types, resolved once per database for
    /// the async wrapper (the blocking one asks its connection).
    type_metadata: Arc<Mutex<PgMetadataCache>>,
    /// The database this cache's reads go to, read once through the
    /// first connection that misses; `None` inside when it could not be
    /// read. The engine side compares it with the one its engine streams.
    database: OnceLock<Option<Database>>,
    /// The contexts this cache keeps apart ([`Cache::context_id`]).
    contexts: Mutex<Contexts>,
    pub stats: Arc<Stats>,
}

/// A database as Postgres identifies it to any role: the cluster's
/// `system_identifier` and the database's name. Two clusters restored
/// from one backup share an identifier, so the name is kept too.
#[derive(Debug, Clone, PartialEq, Eq, diesel::QueryableByName)]
pub struct Database {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub system: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub name: String,
}

/// Reads a [`Database`] through a connection; the engine announces the
/// same in its `hello`.
pub const DATABASE_SQL: &str = "SELECT system_identifier::text AS system, current_database()::text AS name FROM pg_control_system()";

/// What a connection reads as: its database, the role whose privileges
/// apply (`current_user`), the role it logged in as, its search path,
/// the settings that can change a supported query's rows, whether a
/// relation before `public` on its path shadows one in `public`, and the
/// tables its role may not read whole. Connections share cached answers
/// only within one context ([`Cache::context_id`]).
///
/// Custom settings (`app.tenant`) are not here: PostgreSQL does not list
/// them, and they cannot change what a query the engine follows returns.
/// The engine refuses tables with row-level security and runs no
/// functions, so a setting has nothing to act on. The session's own
/// statements are still watched for them (`crate::session`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, diesel::QueryableByName)]
pub struct Context {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub system: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub role: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub login: String,
    /// The search path's schemas that exist and the role may use, in
    /// order, comma-separated.
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub schemas: String,
    /// The settings that can change what a supported query means, as
    /// `name=value` pairs: whether `= NULL` means `IS NULL`, and how a
    /// string literal reads a backslash. (Not `TimeZone`: the engine takes
    /// no time literal and calls no function, and rows travel binary.)
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub settings: String,
    /// Whether a bare table name may mean a relation other than the
    /// engine's: `public` is not on the path, or a relation of the same
    /// name as one in `public` sits before it, or among the session's
    /// temporary tables.
    #[diesel(sql_type = diesel::sql_types::Bool)]
    pub shadowed: bool,
    /// The tables, named as the engine names them (bare in `public`,
    /// `schema.table` elsewhere), the role may not `SELECT` whole, in
    /// byte order.
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Text>)]
    pub unreadable: Vec<String>,
    /// Their OIDs, in the same order.
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::BigInt>)]
    pub unreadable_oids: Vec<i64>,
    /// The newest table's OID: a table unreadable now with an OID no
    /// greater existed when this was read.
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub newest: i64,
}

/// Reads a [`Context`] through a connection.
pub const CONTEXT_SQL: &str = r#"SELECT
  system_identifier::text AS system,
  current_database()::text AS name,
  current_user::text AS role,
  session_user::text AS login,
  array_to_string(current_schemas(false), ',') AS schemas,
  concat_ws(',',
    'transform_null_equals=' || current_setting('transform_null_equals'),
    'standard_conforming_strings=' || current_setting('standard_conforming_strings')) AS settings,
  (array_position(current_schemas(false), 'public'::name) IS NULL
   OR EXISTS (
     SELECT 1 FROM unnest(current_schemas(false)) WITH ORDINALITY AS path(schema, at)
     JOIN pg_namespace n ON n.nspname = path.schema
     JOIN pg_class shadow ON shadow.relnamespace = n.oid AND shadow.relkind NOT IN ('i', 'I')
     JOIN pg_class own ON own.relname = shadow.relname AND own.relnamespace = to_regnamespace('public')
     WHERE path.at < array_position(current_schemas(false), 'public'::name))
   OR EXISTS (
     SELECT 1 FROM pg_class temp
     JOIN pg_class own ON own.relname = temp.relname AND own.relnamespace = to_regnamespace('public')
     WHERE temp.relnamespace = pg_my_temp_schema() AND temp.relkind NOT IN ('i', 'I'))) AS shadowed,
  unreadable.names AS unreadable,
  unreadable.oids AS unreadable_oids,
  (SELECT coalesce(max(oid::int8), 0) FROM pg_class WHERE relkind IN ('r', 'p')) AS newest
FROM pg_control_system(), LATERAL (
  SELECT
    coalesce(array_agg(name ORDER BY name COLLATE "C"), '{}') AS names,
    coalesce(array_agg(oid ORDER BY name COLLATE "C"), '{}') AS oids
  FROM (
    SELECT CASE WHEN n.nspname = 'public' THEN c.relname::text ELSE n.nspname || '.' || c.relname END AS name,
           c.oid::int8 AS oid
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\_%'
      AND NOT has_table_privilege(c.oid, 'SELECT')) AS table_
) AS unreadable"#;

/// The most identities (a database, role, search path and settings) one
/// cache keeps apart; a connection with one more reads Postgres. A change
/// of an identity's privileges gives it a fresh context, not one more.
pub const MAX_CONTEXTS: usize = 64;

/// The contexts a cache keeps apart: each identity's current context
/// under its id. An identity is a context without its privileges. When a
/// table it could read becomes unreadable (a `REVOKE`), the identity gets
/// a fresh id and the old one is retired, so nothing read under the old
/// privileges is served again. A table created since, or a grant, only
/// updates the context under its id: nothing cached read the one, and
/// the other takes nothing away. The count of contexts stays that of
/// identities.
#[derive(Default)]
struct Contexts {
    by_identity: HashMap<Context, u32>,
    by_id: HashMap<u32, Arc<Context>>,
    next: u32,
}

impl Context {
    /// The context with no tables listed as unreadable: what identifies
    /// a reader, apart from privileges that change while it reads.
    fn identity(&self) -> Context {
        Context {
            unreadable: Vec::new(),
            unreadable_oids: Vec::new(),
            newest: 0,
            ..self.clone()
        }
    }

    /// Whether a table this context's role may not read was readable in
    /// `before`, the same identity's context as read earlier: unreadable
    /// now, not then, and no newer than the newest table then (a table
    /// created since cannot have been read). A wrapped-around OID reads
    /// as revoked, which costs a fresh id, never a stale answer.
    fn revoked_since(&self, before: &Context) -> bool {
        self.unreadable_oids
            .iter()
            .any(|oid| *oid <= before.newest && !before.unreadable_oids.contains(oid))
    }

    /// The database it reads.
    pub fn database(&self) -> Database {
        Database {
            system: self.system.clone(),
            name: self.name.clone(),
        }
    }

    /// Whether its role may read `table` (as the engine names it) whole.
    pub fn reads_whole(&self, table: &str) -> bool {
        self.unreadable
            .binary_search_by(|unreadable| unreadable.as_str().cmp(table))
            .is_err()
    }
}

/// Where the next WAL record will go, as [`WAL_POSITION_SQL`] reads it:
/// past the commit record of every transaction that has committed; `None`
/// on a standby, which writes no WAL of its own.
#[derive(Debug, Clone, PartialEq, Eq, diesel::QueryableByName)]
pub struct WalPosition {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub lsn: Option<String>,
}

/// What a [`WalPosition`] query read, when it could be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Read {
    /// A position.
    At(u64),
    /// `NULL`: no insert position on a standby ([`WAL_POSITION_SQL`]),
    /// no replay position on a primary ([`REPLAY_POSITION_SQL`]).
    Null,
}

impl WalPosition {
    /// What it read; `None` for a position that does not parse.
    pub fn read(&self) -> Option<Read> {
        match self.lsn {
            None => Some(Read::Null),
            Some(_) => self.position().map(Read::At),
        }
    }

    /// The position as a number, `hi/lo` read as `hi << 32 | lo` as
    /// PostgreSQL and the engine count it; `None` on a standby or when it
    /// does not parse.
    pub fn position(&self) -> Option<u64> {
        let (hi, lo) = self.lsn.as_deref()?.split_once('/')?;
        let hi = u64::from_str_radix(hi, 16).ok()?;
        let lo = u64::from_str_radix(lo, 16).ok()?;
        (hi <= u64::from(u32::MAX) && lo <= u64::from(u32::MAX)).then_some(hi << 32 | lo)
    }
}

/// Reads a [`WalPosition`]. The insert position, not the write or flush
/// one: with `synchronous_commit` off a commit returns before its record
/// is written, and only the insert position is sure to be past it. A
/// standby has none (the function raises there), so it reads `NULL`:
/// reads through a replica fall back to the window.
pub const WAL_POSITION_SQL: &str = "SELECT CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_insert_lsn()::text END AS lsn";

/// Reads a [`WalPosition`]: how far a standby has replayed, `NULL` on a
/// primary, which is current (a promoted one included, whose last replay
/// position stays behind).
pub const REPLAY_POSITION_SQL: &str = "SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()::text END AS lsn";

/// Whether a read through the cache may be served from it, given the
/// writes through it ([`Cache::after_writes`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AfterWrites {
    /// Every write through the cache has reached it, or the window
    /// since the last one has passed.
    Serve,
    /// A write through the cache may not have reached it yet: the read
    /// goes to Postgres.
    Postgres,
    /// Writes through the cache have no position yet: the read goes to
    /// Postgres, and reads [`WAL_POSITION_SQL`] with it to give them one
    /// ([`Cache::resolve_writes`] with this count).
    Resolve(u64),
}

impl Cache {
    /// A cache holding at most `max_bytes` of rows and keys, with the
    /// default clocks, subscribing through `subscriptions`.
    pub fn new(max_bytes: u64, subscriptions: Arc<dyn Subscriptions>) -> Cache {
        Self::with_limits(
            Limits {
                max_bytes,
                ..Limits::default()
            },
            subscriptions,
        )
    }

    /// A cache bounded by `limits`, subscribing through `subscriptions`.
    pub fn with_limits(limits: Limits, subscriptions: Arc<dyn Subscriptions>) -> Cache {
        let stats = Arc::new(Stats::default());
        let generations: Generations = Arc::default();
        let listener_subscriptions = Arc::clone(&subscriptions);
        let listener_stats = Arc::clone(&stats);
        let listener_generations = Arc::clone(&generations);
        let mut builder = moka::sync::Cache::builder()
            .max_capacity(limits.max_bytes)
            .weigher(|_: &QueryKey, entry: &Arc<Entry>| {
                u32::try_from(entry.bytes).unwrap_or(u32::MAX)
            })
            .eviction_listener(move |key: Arc<QueryKey>, _: Arc<Entry>, cause| {
                let ended = match cause {
                    RemovalCause::Size => &listener_stats.evicted,
                    RemovalCause::Expired => &listener_stats.expired,
                    RemovalCause::Explicit | RemovalCause::Replaced => return,
                };
                Stats::bump(ended);
                bump(&listener_generations, &key);
                listener_subscriptions.unsubscribe(&key);
            });
        if let Some(idle) = limits.time_to_idle {
            builder = builder.time_to_idle(idle);
        }
        if let Some(live) = limits.time_to_live {
            builder = builder.time_to_live(live);
        }
        let refused = limits.refused_for.map(|window| {
            moka::sync::Cache::builder()
                .max_capacity(REFUSED_BYTES)
                .weigher(|key: &QueryKey, _: &Arc<str>| {
                    u32::try_from(key.byte_len()).unwrap_or(u32::MAX)
                })
                .time_to_live(window)
                .build()
        });
        Cache {
            entries: builder.build(),
            generations,
            subscriptions,
            refused,
            limits,
            last_write: AtomicU64::new(0),
            started: Instant::now(),
            writes: AtomicU64::new(0),
            resolved: AtomicU64::new(0),
            target: AtomicU64::new(0),
            standby_reads: AtomicBool::new(false),
            type_metadata: Arc::new(Mutex::new(PgMetadataCache::new())),
            database: OnceLock::new(),
            contexts: Mutex::new(Contexts::default()),
            stats,
        }
    }

    /// Counts a refusal of `key`, and when the engine side says the
    /// reason will hold, remembers it so reads of the same query with the
    /// same values skip the engine for [`Limits::refused_for`].
    pub fn refused(&self, key: &QueryKey, reason: &str) {
        Stats::bump(&self.stats.refused);
        if let Some(refused) = &self.refused
            && self.subscriptions.refusal_is_final(reason)
        {
            refused.insert(key.clone(), Arc::from(reason));
        }
    }

    /// Why `key` was refused recently, if it was; counts the skipped ask.
    pub fn refused_recently(&self, key: &QueryKey) -> Option<Arc<str>> {
        let reason = self.refused.as_ref()?.get(key)?;
        Stats::bump(&self.stats.refused_recently);
        Some(reason)
    }

    /// Forgets every remembered refusal: the engine connection dropped,
    /// and the engine that answers next may be a restarted one with a
    /// catalog that knows more (a type created after the old one
    /// started, say), so each refused shape is asked about once more.
    pub fn forget_refusals(&self) {
        if let Some(refused) = &self.refused {
            refused.invalidate_all();
        }
    }

    /// What bounds the cache.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// The rows for a key, if the cache has them: a Postgres read's as
    /// they were, the engine's built as of its latest change. A stale
    /// entry has none.
    pub fn get(&self, key: &QueryKey) -> Option<Arc<[OwnedRow]>> {
        let rows = self.entries.get(key).and_then(|entry| match &entry.content {
            Content::Rows(rows) => Some(Arc::clone(rows)),
            Content::Live(live) => live.rows(),
            Content::Stale => None,
        });
        Stats::bump(match rows {
            Some(_) => &self.stats.hits,
            None => &self.stats.misses,
        });
        rows
    }

    /// The engine side the cache subscribes through.
    pub fn subscriptions(&self) -> &dyn Subscriptions {
        &*self.subscriptions
    }

    /// The same, owned, for a subscribe on another thread.
    pub fn shared_subscriptions(&self) -> Arc<dyn Subscriptions> {
        Arc::clone(&self.subscriptions)
    }

    /// The key's generation, taken before the read that will fill it.
    pub fn reserve(&self, key: &QueryKey) -> Reservation {
        let generation = {
            let mut generations = self.generations.lock().expect("generations");
            let slot = generations.entry(key.clone()).or_default();
            slot.inflight += 1;
            slot.generation
        };
        Reservation {
            generations: Arc::clone(&self.generations),
            key: key.clone(),
            generation,
        }
    }

    /// Stores the rows a Postgres read returned, unless the key was
    /// touched since the reservation was taken or the result is over the
    /// entry limit. Returns whether they were stored.
    pub fn fill(&self, reservation: Reservation, rows: Arc<[OwnedRow]>) -> bool {
        let entry = Entry::rows(rows, &reservation.key);
        self.fill_entry(reservation, entry)
    }

    /// [`Cache::fill`] for a query the engine serves from its own rows:
    /// `rows` is the result just built from them, which a later read
    /// gets again until a change.
    pub fn fill_live(
        &self,
        reservation: Reservation,
        live: Arc<dyn Live>,
        rows: &[OwnedRow],
    ) -> bool {
        let entry = Entry::live(live, &reservation.key, Some(rows_bytes(rows)));
        self.fill_entry(reservation, entry)
    }

    /// The generation is checked before and after the insert: a touch
    /// or a replace between the two would otherwise leave the older
    /// rows in, since nothing holds a lock across the insert (moka runs
    /// the eviction listener on the inserting thread, and the listener
    /// takes the generations lock). Losing to something newer leaves the
    /// key stale, which costs one refill, never a stale row, and keeps
    /// its clocks running.
    fn fill_entry(&self, reservation: Reservation, entry: Entry) -> bool {
        if self.generation(&reservation.key) != reservation.generation {
            Stats::bump(&self.stats.stale_fills);
            return false;
        }
        if !self.insert(&reservation.key, entry) {
            return false;
        }
        if self.generation(&reservation.key) != reservation.generation {
            Stats::bump(&self.stats.stale_fills);
            self.stale(&reservation.key);
            return false;
        }
        true
    }

    /// The engine side holds rows for the key afresh (a store built for
    /// a resubscribe), or its rows weigh much more or less than when the
    /// entry was weighed: store them, failing any fill in flight, whose
    /// rows are older. The caller reserves the key before it builds the
    /// store, so a subscription ended meanwhile (which bumps the
    /// generation once more) leaves the key stale again: the engine
    /// side's writes are held to the same rule as a Postgres read's.
    pub fn live(&self, reservation: Reservation, live: Arc<dyn Live>) {
        Stats::bump(&self.stats.replaced);
        let key = &reservation.key;
        bump(&self.generations, key);
        let entry = Entry::live(live, key, None);
        if self.insert(key, entry) && self.generation(key) != reservation.generation + 1 {
            Stats::bump(&self.stats.stale_fills);
            self.stale(key);
        }
    }

    /// The database this cache's reads go to, once it has been read:
    /// `None` until then, and when it could not be read.
    pub fn database(&self) -> Option<&Database> {
        self.database.get().and_then(Option::as_ref)
    }

    /// Whether the database has been looked for, found or not.
    pub fn knows_database(&self) -> bool {
        self.database.get().is_some()
    }

    /// Records the database a connection read; the first record stands.
    /// `None` says there is none to check the engine's against, and
    /// trusts it: the wrapper records only a database it read, and goes
    /// live on a read that failed.
    pub fn learn_database(&self, database: Option<Database>) {
        let _ = self.database.set(database);
    }

    /// The id of `context`, under which its readers share answers with
    /// each other and with no one else; `None` when its readers must read
    /// Postgres: a bare table name may mean another relation than the
    /// engine's, it reads another database than the cache's first
    /// context (the one the engine side checks its engine against), or
    /// the cache already keeps [`MAX_CONTEXTS`] identities apart. When a
    /// table a known identity could read has become unreadable, it gets a
    /// fresh id and the old one is retired; a table created since, or a
    /// grant, keeps the id.
    pub fn context_id(&self, context: &Context) -> Option<u32> {
        if context.shadowed {
            return None;
        }
        let mut contexts = self.contexts.lock().expect("contexts");
        let identity = context.identity();
        let known = contexts.by_identity.get(&identity).copied();
        if let Some(id) = known
            && let Some(held) = contexts.by_id.get(&id).cloned()
        {
            if *held == *context {
                return Some(id);
            }
            if !context.revoked_since(&held) {
                contexts.by_id.insert(id, Arc::new(context.clone()));
                return Some(id);
            }
        }
        if known.is_none() {
            let other_database = self.database().is_some_and(|database| *database != context.database());
            if contexts.by_identity.len() >= MAX_CONTEXTS || other_database {
                return None;
            }
            if contexts.by_identity.is_empty() {
                self.learn_database(Some(context.database()));
            }
        }
        let id = contexts.next;
        if id >= crate::listen::LISTENING_BASE {
            return None;
        }
        contexts.next += 1;
        if let Some(retired) = contexts.by_identity.insert(identity, id) {
            contexts.by_id.remove(&retired);
        }
        contexts.by_id.insert(id, Arc::new(context.clone()));
        Some(id)
    }

    /// The context with id `id`, while it is current; `None` for one
    /// retired, or never given.
    pub fn context(&self, id: u32) -> Option<Arc<Context>> {
        let contexts = self.contexts.lock().expect("contexts");
        contexts.by_id.get(&id).cloned()
    }

    /// The engine changed a query it serves from its own rows: counted;
    /// the entry builds the new result on its next read.
    pub fn changed(&self) {
        Stats::bump(&self.stats.replaced);
    }

    /// Fails any fill or replace in flight for `key`, without touching
    /// the entry. For the engine side to call while it drops the key,
    /// under its own lock, so that no rows built before the drop can
    /// land after it.
    pub fn bump_generation(&self, key: &QueryKey) {
        bump(&self.generations, key);
    }

    /// Stores `entry`, unless it is over the entry limit, in which case
    /// the key is abandoned. Whether it was stored.
    fn insert(&self, key: &QueryKey, entry: Entry) -> bool {
        if entry.bytes as u64 > self.limits.max_entry_bytes {
            Stats::bump(&self.stats.oversized);
            self.abandon(key);
            return false;
        }
        self.entries.insert(key.clone(), Arc::new(entry));
        true
    }

    /// The engine reported a change to the query's rows: mark the key
    /// stale, so the next read refills from Postgres, and fail any fill
    /// in flight.
    pub fn touched(&self, key: &QueryKey) {
        Stats::bump(&self.stats.touched);
        bump(&self.generations, key);
        self.stale(key);
    }

    /// Leaves `key` stale: no rows, its subscription kept, its clocks
    /// running.
    fn stale(&self, key: &QueryKey) {
        self.entries.insert(key.clone(), Arc::new(Entry::stale(key)));
    }

    /// Ends the key's subscription from the cache's side: a read gave up
    /// on it (dropped, or its Postgres read failed) or its result was
    /// too large to keep. The generation is bumped first, so a fill in
    /// flight for the key by another read is dropped rather than kept
    /// with nothing to update it: the sequence the eviction listener
    /// uses, and the only way the wrapper may unsubscribe.
    ///
    /// The subscription goes first: the engine side bumps the generation
    /// as it drops the key, so an engine delta that checked the key
    /// before this point and inserts after it is dropped by its own
    /// re-check. Then the bump again for a Postgres fill in flight, and
    /// the entry.
    pub fn abandon(&self, key: &QueryKey) {
        self.subscriptions.unsubscribe(key);
        bump(&self.generations, key);
        self.entries.invalidate(key);
    }

    /// A connection through this cache wrote.
    pub fn mark_write(&self) {
        let nanos = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX - 1);
        self.last_write.fetch_max(nanos + 1, Ordering::Release);
        self.writes.fetch_add(1, Ordering::AcqRel);
    }

    /// Whether a read may be served from the cache after the writes
    /// through it, a connection's `window` being its read-your-writes
    /// window. By position when the engine side reports one: a read
    /// waits for every write to have reached the cache, however long
    /// that takes, and no longer. Otherwise by the window: a read goes
    /// to Postgres for `window` after the last write. A zero window
    /// turns both off.
    pub fn after_writes(&self, window: Duration) -> AfterWrites {
        if window.is_zero() {
            return AfterWrites::Serve;
        }
        let Some(position) = self.subscriptions.position() else {
            return self.by_window(window);
        };
        let writes = self.writes.load(Ordering::Acquire);
        if writes > self.resolved.load(Ordering::Acquire) {
            return AfterWrites::Resolve(writes);
        }
        if position >= self.target.load(Ordering::Acquire) {
            AfterWrites::Serve
        } else {
            AfterWrites::Postgres
        }
    }

    /// [`Cache::after_writes`] by the window alone, for when the position
    /// of the writes could not be read.
    pub fn by_window(&self, window: Duration) -> AfterWrites {
        if self.wrote_within(window) {
            AfterWrites::Postgres
        } else {
            AfterWrites::Serve
        }
    }

    /// The first `writes` marks committed, if at all, below `position`,
    /// read after the count was taken. The target only rises: a later
    /// read of the position covers every earlier one.
    pub fn resolve_writes(&self, writes: u64, position: u64) {
        self.target.fetch_max(position, Ordering::AcqRel);
        self.resolved.fetch_max(writes, Ordering::AcqRel);
    }

    /// How many write marks there have been, for a write that reads the
    /// position itself once it has committed ([`Cache::resolve_writes`]).
    pub fn write_count(&self) -> u64 {
        self.writes.load(Ordering::Acquire)
    }

    /// A read through the cache went to a standby.
    pub fn saw_standby(&self) {
        self.standby_reads.store(true, Ordering::Release);
    }

    /// Whether reads through the cache have gone to a standby, so a
    /// committed write reads the WAL position on its own connection
    /// rather than leaving it to a read that cannot.
    pub fn reads_standbys(&self) -> bool {
        self.standby_reads.load(Ordering::Acquire)
    }

    /// Whether a connection through this cache wrote within `window`.
    pub fn wrote_within(&self, window: Duration) -> bool {
        if window.is_zero() {
            return false;
        }
        match self.last_write.load(Ordering::Acquire) {
            0 => false,
            marked => {
                let since = self
                    .started
                    .elapsed()
                    .saturating_sub(Duration::from_nanos(marked - 1));
                since < window
            }
        }
    }

    /// The OIDs of user-defined types resolved so far for this database.
    pub fn type_metadata(&self) -> &Arc<Mutex<PgMetadataCache>> {
        &self.type_metadata
    }

    /// Runs the maintenance the clocks need, so expired entries are
    /// dropped (and unsubscribed) now rather than on the next access.
    /// moka does this on its own as the cache is used; this is for
    /// tests and quiet processes.
    pub fn run_pending_tasks(&self) {
        self.entries.run_pending_tasks();
    }

    /// Leaves every entry stale and fails every fill in flight: every
    /// key's next read refills, and every subscription keeps its clocks.
    pub fn clear(&self) {
        for slot in self.generations.lock().expect("generations").values_mut() {
            slot.generation += 1;
        }
        let keys: Vec<Arc<QueryKey>> = self.entries.iter().map(|(key, _)| key).collect();
        for key in keys {
            self.stale(&key);
        }
    }

    /// How many entries are in, after pending maintenance.
    pub fn len(&self) -> u64 {
        self.entries.run_pending_tasks();
        self.entries.entry_count()
    }

    /// Whether no entry is in, stale ones counted.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The bytes the entries weigh, after pending maintenance.
    pub fn weighted_size(&self) -> u64 {
        self.entries.run_pending_tasks();
        self.entries.weighted_size()
    }

    /// How many keys have a read in flight, for tests.
    pub fn reservations(&self) -> usize {
        self.generations.lock().expect("generations").len()
    }

    /// The key's generation now; 0 when no read is in flight.
    fn generation(&self, key: &QueryKey) -> u64 {
        self.generations
            .lock()
            .expect("generations")
            .get(key)
            .map_or(0, |slot| slot.generation)
    }
}

/// Fails every fill in flight for `key`. A key with none has no
/// generation to bump: its entry, if any, is what the caller drops.
fn bump(generations: &Generations, key: &QueryKey) {
    if let Some(slot) = generations.lock().expect("generations").get_mut(key) {
        slot.generation += 1;
    }
}

impl std::fmt::Debug for Cache {
    /// The entry count and the counters.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache")
            .field("entries", &self.entries.entry_count())
            .field("stats", &self.stats)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::materialized::Snapshot;
    use crate::subscriptions::{Outcome, Unsynced, Verdict};
    use crate::types::TableTypes;

    /// A context of `role` on database `app`, its role unable to read
    /// the tables of `unreadable` (OID and name), the newest table's OID
    /// 100.
    fn context(role: &str, unreadable: &[(i64, &str)]) -> Context {
        let mut unreadable = unreadable.to_vec();
        unreadable.sort_by_key(|(_, table)| *table);
        Context {
            system: "1".to_owned(),
            name: "app".to_owned(),
            role: role.to_owned(),
            login: role.to_owned(),
            schemas: "public".to_owned(),
            settings: String::new(),
            shadowed: false,
            unreadable: unreadable.iter().map(|(_, table)| (*table).to_owned()).collect(),
            unreadable_oids: unreadable.iter().map(|(oid, _)| *oid).collect(),
            newest: 100,
        }
    }

    /// Each identity gets an id of its own and keeps it while nothing
    /// changes; a change of its privileges gives it a fresh id and
    /// retires the old, without counting as one more identity. A shadowed
    /// context, one on another database, and one identity too many get
    /// none.
    #[test]
    fn contexts_are_kept_apart_by_identity_and_privileges() {
        let cache = Cache::new(1 << 20, Arc::new(Unsynced));
        let app = cache.context_id(&context("app", &[])).expect("an id");
        assert_eq!(cache.context_id(&context("app", &[])), Some(app), "the same context, the same id");
        let admin = cache.context_id(&context("admin", &[])).expect("an id");
        assert_ne!(admin, app, "another role, apart");
        assert_eq!(cache.database().map(|database| database.name.as_str()), Some("app"));

        let created = Context { newest: 101, ..context("app", &[(101, "audit")]) };
        assert_eq!(cache.context_id(&created), Some(app), "a table created since keeps the id");
        assert!(!cache.context(app).expect("current").reads_whole("audit"), "and is known unreadable");
        let revoked = Context { newest: 101, ..context("app", &[(7, "secrets"), (101, "audit")]) };
        let revoked = cache.context_id(&revoked).expect("an id");
        assert_ne!(revoked, app, "a table revoked, a fresh id");
        assert!(cache.context(app).is_none(), "the old one retired");
        assert!(!cache.context(revoked).expect("current").reads_whole("secrets"));
        assert!(cache.context(revoked).expect("current").reads_whole("tickets"));
        let granted = Context { newest: 101, ..context("app", &[(101, "audit")]) };
        assert_eq!(cache.context_id(&granted), Some(revoked), "a grant keeps the id");

        let shadowed = Context { shadowed: true, ..context("app", &[]) };
        assert_eq!(cache.context_id(&shadowed), None);
        let elsewhere = Context { name: "other".to_owned(), ..context("app", &[]) };
        assert_eq!(cache.context_id(&elsewhere), None, "another database than the engine's");

        for at in 2..MAX_CONTEXTS {
            assert!(cache.context_id(&context(&format!("role{at}"), &[])).is_some());
        }
        assert_eq!(cache.context_id(&context("one-too-many", &[])), None);
        for change in 0..3 {
            let table = format!("table{change}");
            let changed = context("app", &[(change, table.as_str())]);
            assert!(cache.context_id(&changed).is_some(), "a privilege change is no new identity");
        }
    }

    /// A key for `sql` with no binds.
    fn key(sql: &str) -> QueryKey {
        QueryKey {
            sql: sql.to_string(),
            binds: Vec::new(),
            context: 0,
        }
    }

    /// A position reads as PostgreSQL prints it, and nothing else does.
    #[test]
    fn a_wal_position_reads_as_postgres_prints_it() {
        let read = |lsn: &str| {
            WalPosition {
                lsn: Some(lsn.to_string()),
            }
            .position()
        };
        assert_eq!(read("0/0"), Some(0));
        assert_eq!(read("16/B374D848"), Some(0x16_B374_D848));
        assert_eq!(read("FFFFFFFF/FFFFFFFF"), Some(u64::MAX));
        for bad in ["", "16", "16/", "/1", "1/2/3", "x/1", "100000000/0"] {
            assert_eq!(read(bad), None, "{bad}");
        }
        assert_eq!(WalPosition { lsn: None }.position(), None, "a standby");
        assert_eq!(WalPosition { lsn: None }.read(), Some(Read::Null));
        let read = |lsn: &str| WalPosition { lsn: Some(lsn.to_string()) }.read();
        assert_eq!(read("1/0"), Some(Read::At(1 << 32)));
        assert_eq!(read("1/"), None, "unreadable, not NULL");
    }

    /// An empty result.
    fn no_rows() -> Arc<[OwnedRow]> {
        Arc::from(Vec::new())
    }

    /// Accepts everything and records what is unsubscribed.
    #[derive(Default)]
    struct Recording {
        unsubscribed: Mutex<Vec<QueryKey>>,
    }

    impl Subscriptions for Recording {
        /// Accepted.
        fn subscribe(&self, _: &QueryKey) -> Outcome {
            Outcome::Verdict(Verdict::Accepted { since: None })
        }

        /// Accepted, with no rows.
        fn materialize(
            &self,
            _: &QueryKey,
            _: Snapshot,
            _: HashMap<String, Arc<TableTypes>>,
        ) -> Verdict {
            Verdict::Accepted { since: None }
        }

        /// Records the key.
        fn unsubscribe(&self, key: &QueryKey) {
            self.unsubscribed.lock().unwrap().push(key.clone());
        }
    }

    /// Two readers of one key; the second gives up (its future was
    /// dropped, or its read failed) and ends the subscription. The first
    /// must not then store an entry nothing will invalidate.
    #[test]
    fn abandoning_a_key_fails_the_other_readers_fill() {
        let engine = Arc::new(Recording::default());
        let cache = Cache::new(1 << 20, engine.clone());
        let k = key("SELECT 1");
        let first = cache.reserve(&k);
        let second = cache.reserve(&k);
        drop(second);
        cache.abandon(&k);
        assert_eq!(engine.unsubscribed.lock().unwrap().len(), 1);
        assert!(
            !cache.fill(first, no_rows()),
            "the first reader's fill is refused"
        );
        assert!(cache.is_empty());
        assert_eq!(cache.reservations(), 0);
    }

    /// A rows-mode caller holds the snapshot's rows; a delta replaces the
    /// entry before the caller fills. The older rows must lose.
    #[test]
    fn engine_rows_beat_a_fill_in_flight() {
        let cache = Cache::new(1 << 20, Arc::new(Unsynced));
        let k = key("SELECT 1");
        let reservation = cache.reserve(&k);
        let columns: crate::row::Columns = Arc::from(vec!["a".to_string()]);
        let newer: Arc<[OwnedRow]> = Arc::from(vec![OwnedRow::new(columns, vec![None])]);
        let engine_side = cache.reserve(&k);
        cache.live(engine_side, Arc::new(Fixed(Arc::clone(&newer))));
        assert!(
            !cache.fill(reservation, no_rows()),
            "the snapshot's rows are older than the delta's"
        );
        assert_eq!(
            cache.get(&k).map(|rows| rows.len()),
            Some(1),
            "the delta's rows stay"
        );
        assert_eq!(cache.stats.stale_fills.load(Ordering::Relaxed), 1);
    }

    /// The engine side reserved before building its rows; the key is
    /// abandoned (which bumps as the subscription goes) in between, so
    /// the rows are not served.
    #[test]
    fn a_replace_after_the_key_was_abandoned_does_not_stick() {
        let engine = Arc::new(Recording::default());
        let cache = Cache::new(1 << 20, engine.clone());
        let k = key("SELECT 1");
        let engine_side = cache.reserve(&k);
        cache.abandon(&k);
        cache.live(engine_side, Arc::new(Fixed(no_rows())));
        assert!(
            cache.get(&k).is_none(),
            "rows built for a subscription that ended are not served"
        );
    }

    /// The engine's rows over the entry limit end the subscription, and
    /// with it another reader's smaller fill.
    #[test]
    fn an_oversized_result_ends_the_subscription_and_the_other_fill() {
        let engine = Arc::new(Recording::default());
        let cache = Cache::with_limits(
            Limits {
                max_entry_bytes: 8,
                ..Limits::default()
            },
            engine.clone(),
        );
        let k = key("SELECT a long enough statement");
        let other = cache.reserve(&k);
        let engine_side = cache.reserve(&k);
        cache.live(engine_side, Arc::new(Fixed(no_rows())));
        assert_eq!(engine.unsubscribed.lock().unwrap().len(), 1);
        assert!(
            !cache.fill(other, no_rows()),
            "the other reader's smaller fill cannot keep a key with no subscription"
        );
    }

    /// The defaults are the documented ones.
    #[test]
    fn limits_default_to_idle_on_and_ttl_off() {
        let limits = Limits::default();
        assert_eq!(limits.max_bytes, DEFAULT_MAX_BYTES);
        assert_eq!(limits.max_entry_bytes, DEFAULT_MAX_ENTRY_BYTES);
        assert_eq!(limits.time_to_idle, Some(DEFAULT_TIME_TO_IDLE));
        assert_eq!(limits.time_to_live, None);
        assert_eq!(env_number("STREAMGRES_CACHE_TEST_UNSET_VARIABLE"), None);
    }

    /// A key's generation lives only while a read of it is in flight:
    /// touches, evictions and expiries of keys nobody is reading leave
    /// nothing behind, and a reservation dropped without a fill goes too.
    #[test]
    fn generations_live_only_while_a_read_is_in_flight() {
        let cache = Cache::new(1 << 20, Arc::new(Unsynced));
        let k = key("SELECT 1");
        assert_eq!(cache.reservations(), 0);
        let reservation = cache.reserve(&k);
        assert_eq!(cache.reservations(), 1);
        cache.touched(&k);
        assert!(
            !cache.fill(reservation, no_rows()),
            "touched during the read"
        );
        assert_eq!(
            cache.reservations(),
            0,
            "the reservation went with the fill"
        );
        for i in 0..1000 {
            cache.touched(&key(&format!("SELECT {i}")));
        }
        assert_eq!(cache.reservations(), 0);
        let dropped = cache.reserve(&k);
        drop(dropped);
        assert_eq!(cache.reservations(), 0);
    }

    /// A read that started before a touch cannot fill after a read that
    /// started after it did.
    #[test]
    fn an_older_reservation_cannot_fill_after_a_newer_one_did() {
        let cache = Cache::new(1 << 20, Arc::new(Unsynced));
        let k = key("SELECT 1");
        let older = cache.reserve(&k);
        cache.touched(&k);
        let newer = cache.reserve(&k);
        assert!(
            cache.fill(newer, no_rows()),
            "the newer read saw the change"
        );
        assert!(!cache.fill(older, no_rows()), "the older read did not");
        assert_eq!(cache.reservations(), 0);
    }

    /// A result over the entry limit is not kept, from Postgres or from
    /// the engine.
    #[test]
    fn a_result_over_the_entry_limit_is_not_kept() {
        let cache = Cache::with_limits(
            Limits {
                max_entry_bytes: 8,
                ..Limits::default()
            },
            Arc::new(Unsynced),
        );
        let k = key("SELECT a long enough statement");
        let reservation = cache.reserve(&k);
        assert!(!cache.fill(reservation, no_rows()));
        assert_eq!(cache.stats.oversized.load(Ordering::Relaxed), 1);
        assert!(cache.is_empty());
        let engine_side = cache.reserve(&k);
        cache.live(engine_side, Arc::new(Fixed(no_rows())));
        assert!(
            cache.is_empty(),
            "the engine's rows are bounded the same way"
        );
    }

    /// The write clock is the cache's, and a zero window is off.
    #[test]
    fn the_write_clock_is_shared() {
        let cache = Cache::new(1 << 20, Arc::new(Unsynced));
        assert!(!cache.wrote_within(Duration::from_secs(1)));
        cache.mark_write();
        assert!(cache.wrote_within(Duration::from_secs(1)));
        assert!(!cache.wrote_within(Duration::ZERO), "a zero window is off");
    }

    /// A refusal is remembered for the query with its values: the
    /// engine refuses on values too, and another value asks again.
    #[test]
    fn a_refusal_is_remembered_for_its_values() {
        let cache = Cache::new(1 << 20, Arc::new(Unsynced));
        let mut a = key("SELECT $1");
        a.binds.push(crate::key::Bind {
            type_oid: 25,
            bytes: Some(b"a".to_vec()),
        });
        let mut b = a.clone();
        b.binds[0].bytes = Some(b"b".to_vec());
        cache.refused(&a, "unsupported");
        assert!(cache.refused_recently(&a).is_some());
        assert!(cache.refused_recently(&b).is_none());
    }

    /// A touched key stays stale rather than gone, so the idle clock
    /// still ends the subscription of a query nobody reads again.
    #[test]
    fn a_touched_key_unread_still_ends_its_subscription() {
        let engine = Arc::new(Recording::default());
        let cache = Cache::with_limits(
            Limits {
                time_to_idle: Some(Duration::from_millis(50)),
                ..Limits::default()
            },
            engine.clone(),
        );
        let k = key("SELECT 1");
        assert!(cache.fill(cache.reserve(&k), no_rows()));
        cache.touched(&k);
        assert!(cache.get(&k).is_none(), "stale: the next read refills");
        std::thread::sleep(Duration::from_millis(120));
        cache.run_pending_tasks();
        assert_eq!(*engine.unsubscribed.lock().unwrap(), vec![k]);
    }

    /// The engine's rows for one query, fixed.
    #[derive(Debug)]
    struct Fixed(Arc<[OwnedRow]>);

    impl Live for Fixed {
        /// The fixed rows.
        fn rows(&self) -> Option<Arc<[OwnedRow]>> {
            Some(Arc::clone(&self.0))
        }

        /// Weighs nothing.
        fn bytes(&self) -> usize {
            0
        }
    }
}
