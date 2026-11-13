# Embedding Streamgres with Diesel

Share one `Arc<Cache>` per database in an application process. The engine
owns its runtime and threads; a blocking Diesel application needs no Tokio
runtime. Disable the core crate's default features to omit the server stack.
The Diesel adapter already does this for its core dependency.

## Database setup and cost

Use PostgreSQL 14 or later with `wal_level = logical`, a publication containing
the cached tables, and sufficient replication slots, WAL senders and connections.
An administrator runs `CacheConfig::setup_sql()` to install the DDL event
triggers. Rerun setup when upgrading the trigger stack: startup refuses an
older stack because it may miss security or collation changes.

The `dsn` endpoint must support direct, session based connections for
replication, snapshot export and slot ownership. `read_dsn` may route ordinary
snapshot reads through a transaction pool reaching the same database.

Each process needs a replication slot/WAL sender, a slot-ownership connection,
a snapshot-export connection, a source-freshness connection, and up to
`read_connections` snapshot readers (four by default): approximately eight
connections, with temporary headroom for startup and snapshot rotation. Budget
slots and connections per process, including rolling deployments. PostgreSQL's
default is ten replication slots; snapshot slots also need headroom. Dead owners'
slots are swept; monitor retained WAL independently.

## Start and read

```rust,ignore
use std::sync::Arc;
use std::time::Duration;
use diesel::prelude::*;
use streamgres_diesel::{Cache, CacheConfig, FirstCached, LoadCached, Tracker};

let mut config = CacheConfig::new(dsn, "app_publication");
config.admit_after = 2; // Avoid subscribing to lookup keys read only once.
let cache = Arc::new(Cache::start(config));
// Optional: otherwise reads fall back while the engine starts.
cache.wait_ready(Duration::from_secs(30))?;
conn.set_instrumentation(Tracker::new());

let rows: Vec<Order> = orders::table
    .filter(orders::merchant_id.eq(merchant_id))
    .select(Order::as_select())
    .load_cached(&cache, &mut conn)?;
let first: Order = orders::table.select(Order::as_select())
    .order(orders::id).first_cached(&cache, &mut conn)?;
```

`first_cached` adds `LIMIT 1`. `get_result_cached` takes the first returned
row and returns Diesel's `NotFound` for absence. Use Diesel's
`OptionalExtension::optional()` on that result, or `optional_cached` directly.

Put application instrumentation inside `Tracker::wrapping(my_instrumentation)`.
The tracker must be the connection's outer instrumentation. `install()` installs
it for future connections only when doing so would not replace an application's
existing default; otherwise it returns an error explaining how to wrap that
factory. Cached hits emit balanced query events around encoding/decoding. Async
cache attempts also finish their event if the caller cancels its future.

The upstream adapter pins Diesel 2.3.14 because it uses unstable backend APIs.
The separate `streamgres-juspay-diesel` package pins Juspay 2.3.1 and shares the
synchronous source. Upstream Diesel 2.2 lacks instrumentation downcasting and
is not supported by this tracker design. Upgrade these pins deliberately and
run the adapter checks.

## Refusals and consistency

`load_cached` falls back to Diesel when the cache refuses, reader validation
fails, or cached bytes cannot be decoded. `try_load_cached` returns `Ok(None)`
without executing the application's query on Postgres. `inspect_cached` returns
`CacheAttempt::Hit(rows)` or `CacheAttempt::Refused(reason)`. Query rendering or
bind serialization can still return the same error the database path would.

The cache is asynchronous. A read reflects applied WAL, not necessarily the
latest commit. Before a read that must see your write, call
`wait_for_writes(&cache, &mut conn, timeout)` and check its boolean result.
Reads inside Diesel or tracked raw SQL transactions use Postgres.

Freshness is checked against WAL positions sampled over a separate SQL
connection every 200 ms, using an instant captured before the round trip.
A source sample is confirmed only after its position has been applied. A busy
feed replaying old transactions cannot renew freshness. When no recently
sampled position has been applied within `max_lag`, reads refuse. Very small
`max_lag` settings can cause fallback between samples. This needs no agreement
between the database and application's wall clocks.

A reader is checked for cluster identity, database, table resolution and SELECT
privileges. The check is one round trip, paid by the next cached read, so when
it is repeated decides what the cache saves. Its SQL and bind types are reused
through Diesel's prepared-statement cache; each recheck still executes the full
identity, table-resolution and privilege checks. Validation uses explicitly
qualified PostgreSQL builtins so a caller's schema cannot replace the check's
functions or operators. If the host clears server prepared statements behind
the driver's cache, the adapter retries the read-only check without preparing
it and keeps using unnamed checks for that connection:

- **`Recheck::AfterEveryStatement`** (the default). Every statement the
  connection sends to the database invalidates the check, including a SELECT
  calling a function that changes session state. No session change is missed.
  A cached read that follows a database statement costs one round trip; cached
  reads that follow one another cost none.
- **`Recheck::AfterSessionStatements`**
  (`Tracker::new().rechecking(Recheck::AfterSessionStatements)`). Only
  statements that name a change invalidate the check: `SET`, `RESET`, `DISCARD`,
  `set_config`, `CALL`, `DO`, schema changes and grants. A change made inside a
  function a query calls (a `set_tenant()` that sets `search_path`, a
  `SET ROLE` in a trigger) is not seen for up to ten seconds, and reads in that
  time may be answered for the session as it was. Choose it only when the
  application changes `search_path` and role in plain statements alone.

Cache hits cannot execute functions or change a session and never invalidate the
check. A read the cache refuses whoever asks (an unsupported statement, an
engine not ready) is refused before the reader is checked, so it costs no round
trip beyond the query's own. Under either choice checks expire after ten
seconds, bounding detection of privilege changes made by another session.
Applications requiring immediate external privilege revocation should read
directly from Postgres.

RLS tables are refused. Enabling RLS on a cached table retires that engine and
its results; the restarted engine excludes the protected table. Like other WAL
changes, this takes effect when the feed applies the migration.

The parser accepts only SQL the engine can reproduce exactly. Unsupported
shapes, unsafe type comparisons, incomplete joins, transactions, startup and lag
cause fallback. A first admitted read may wait up to `fill_timeout` for rows;
a previously filled join that becomes incomplete refuses immediately.

PostgreSQL parameter types are checked before decoding, including typed NULLs.
Mixed `numeric`/float binds and `timestamp`/`timestamptz` joins fall back: exact
numeric indexes and session-independent timestamps cannot reproduce those
comparisons. Non-NULL timestamp literals also fall back, since PostgreSQL's
session input rules differ from a generic UTC parser; use typed binary binds.
Plain `json` can be projected, but its SQL equality is refused
(Postgres defines equality for `jsonb`, not `json`). JSONB binds normalize key
order, whitespace, duplicate keys, exponents and decimal scale without rounding
through a float. Malformed or unrepresentable JSON input falls back.
Canonical JSON text is bounded to 1 MiB before allocation: compact exponents
cannot expand into unbounded decimal output. JSONB arrays share that limit, and
multiple parameters divide the evaluation allowance conservatively.

A dot **inside** a quoted table or schema identifier is currently refused.
Ordinary schema qualification (`billing.invoices`) is supported. These ambiguous
identifiers are excluded consistently from startup catalogs, DDL handling and
replication routing. Arrays with non-one lower bounds, like multidimensional
arrays, retain their original wire bytes; they can be projected but are not
interpreted as flat filter lists.

Core integrations must pass the `excluded` column of `ReaderCheck` unchanged to
`Reader::new`: it is a JSON array of names, not a delimiter-separated string.
Malformed exclusion data refuses all tables.

## Bounds and lifecycle

Defaults are 10,000 entries, a 256 MiB estimated entry budget, 32 MiB per entry,
and 100,000 rows per query part and composed join result. Entry accounting
includes query metadata, row images, composed joins and projections. Hydration
checks the budget between deltas; join enumeration stops at its row/byte limit
before materializing a Cartesian product; projection checks before cloning cells.
Snapshot reads also enforce the row limit while streaming and retain at most
`min(max_entry_bytes, max_bytes)` bytes of wire payload per read. Exceeding a
budget closes that read's connection and refuses the subscription. This
includes full source row images even when the query projects only a small field.
One row must be received in full before its size can be checked. Up to
`read_connections` reads may have their own buffers concurrently; decoded rows,
engine indexes and replication transactions have additional costs.

Refused queries are bounded by both count and estimated bytes (at most 8 MiB,
or one eighth of `max_bytes`). The two sharded parse memos retain at most
64 MiB by their own estimates. A thread may retain one private answer of at
most 64 KiB (also limited by the entry budget); it is reused only while its
cache identity, full query/binds, reader, state revision and freshness lease
match. Every state writer publishes a new revision before unlocking, so a
write barrier, eviction, schema update or stop invalidates the answer before
that change becomes visible. Its private Arc counters avoid hot-key contention.
This per-thread storage is additional to the entry budget. SQL is limited to 64 KiB, and serialized bind
bytes to at most 1 MiB or one sixteenth of the smaller entry/cache budget.
Larger queries fall back to Postgres.

These are retention estimates, not a hard process RSS cap. Engine state,
in-flight reads, temporary indexes, allocation overhead and answers retained by
callers also consume memory. Budget conservatively and monitor the process.
Admission controls subscription churn. Distinct bind values still identify
distinct subscriptions, but reuse a compiled SQL shape: only typed substitution
and validation recur. A bounded second memo accelerates repeated values.

`Cache::metrics()` exposes hit/refusal, unusable-answer, admission, eviction and
restart counts, estimated entry bytes and lag. Counters and settings belong to
each cache. Library-only logs go through the host's `log` facade; enabling the
server feature retains the server logger.

`stop()` and dropping the cache cancel startup, blocked feed connections and
pending unsubscribes. Startup has a deadline. Feed joining, slot deletion and
read-runtime shutdown have grace periods, so stopping is not instantaneous.
Failed or panicking engines release their resources and restart with backoff.
Positions exposed by the cache are opaque; compare or wait on them rather than
editing a numeric field. Backend implementers retain the model's source-position
API separately.

## Optional async adapter

Enable the upstream adapter's `async` Cargo feature (diesel-async 0.7.4). Juspay's
fork has no corresponding compatible async package, so this feature belongs to
the upstream adapter only.

```rust,ignore
use streamgres_diesel::AsyncLoadCached;
let rows: Vec<Order> = orders::table.select(Order::as_select())
    .load_cached_async(cache.clone(), &mut async_conn).await?;
```

Use `Tracker` as the `AsyncPgConnection` instrumentation too. Cache waiting and
decoding run on Tokio's blocking pool. `inspect_cached_async` preserves refusal
reasons; `wait_for_writes_async` provides the write barrier. Cancellation releases
the borrow on the database connection; an already-running blocking cache task
may finish its bounded fill afterwards. A process-wide limit admits at most
64 running or queued async cache jobs, including write-barrier waits. The worker
owns its permit until it exits, even if its caller is cancelled. When full,
reads fall back to async Postgres; a write-barrier attempt returns `false`.

Built-in bind types use the cache. Custom bind metadata is delegated to the
normal async Postgres query, preserving correctness without a synchronous
metadata lookup on the executor. This is an explicit coverage limit, not an
error for applications using custom SQL types.

## Validation

CI runs workspace tests and Clippy with async enabled, library-only Clippy,
and the separately locked Juspay adapter. Live tests require
`STREAMGRES_PG_DSN` pointing at a disposable logical-replication database.
`exactness_live` and `boundaries_live` compare edge-case queries with Postgres;
the latter also covers restricted-reader exclusions, typed NULLs, exact JSONB
binds, array bounds, colliding identifiers and streaming byte limits. `cache_live` covers
RLS changes, restart and migrations. Additional regressions cover stalled
startup/feed cancellation, panic recovery, saturated unsubscribe queues, memory
limits, hidden session changes and decoder fallback. The ignored
`concurrent_read_scaling` test measures 1/4/8/16-reader throughput; it is a
measurement, not a timing threshold in CI.

## Adapter source and release maintenance

Both Diesel adapters contain their own `src` files so their Cargo packages are
self-contained. `crates/streamgres-diesel/src` is the canonical implementation;
run `python3 scripts/sync-diesel-adapters.py` after editing it. CI runs the script
with `--check` and checks each package's contents. Do not edit the fork's copies
independently. This avoids a second implementation while keeping published
crates independent of sibling paths. The fork keeps its own lockfile because
its derive dependency cannot share the upstream workspace's resolution.

All path dependencies also have version requirements. Publish `streamgres`
before either adapter, then verify each adapter against that published version.
The Juspay package has sync support only; its shared async source is inactive.
