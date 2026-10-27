# Streamgres behind a Diesel backend

This guide covers running Streamgres as the read cache of a Rust backend
that uses Diesel, through the `diesel-streamgres` wrapper:
- the two ways to run the engine, and how to choose
- what each needs from PostgreSQL, and what it costs there
- reading from replicas
- read-your-writes
- who a cached answer is for
- what to watch

The wrapper itself (routing, the cache, what is served from rows and what
is refilled) is described in
[`crates/diesel-streamgres/docs/design.md`](../crates/diesel-streamgres/docs/design.md).
The S2S server's own deployment is in [`deploy.md`](deploy.md).

## 1. Choosing a mode

| | embedded | S2S server |
|---|---|---|
| where the engine runs | inside every backend process | a few engine servers the backends connect to |
| PostgreSQL it follows | the primary | the primary, or a PostgreSQL 16+ standby |
| setup (publication, event trigger) | installs it itself, or requires it | once, by a migration on the primary |
| slots and walsenders on the database | about 3 per backend process | about 3 per engine server, whatever the number of backends |
| backends read from | the primary | the primary or replicas |
| for | one or a few processes | a fleet |

**Embedded is for small applications.** Every process runs its own engine
with its own replication slot, so the cost on the database grows with the
process count:
- About 3 replication slots and 3 walsenders per engine at its peak: the
  feed's, and two read snapshots while they rotate. PostgreSQL's defaults
  (`max_replication_slots = 10`, `max_wal_senders = 10`) fit about three
  engines. Before each start an engine checks there is room for 3 more of
  each and refuses to take the last ones another consumer may need.
- About 7 to 10% of a core of walsender CPU on the database per engine at
  1,000 transactions a second.
- Each process holds its own copy of the rows its queries follow, and
  spends its own CPU applying every change to them (section 8).

**The S2S server is for anything larger.** Backend processes scale out
freely and add no slots: only the engine servers hold them, so the cost on
the database stays fixed. Backends can read from replicas (section 4).

## 2. Embedded mode

### Turning it on

Build the wrapper with its `embedded` feature and start the client from
the environment:

```toml
diesel-streamgres = { path = "...", features = ["embedded"] }  # and "async" for diesel-async
```

```rust
// STREAMGRES_ENGINE=embedded runs the engine in this process; without it
// the client connects to STREAMGRES_S2S_URL. The same code serves both.
let engine = EngineClient::from_env()?;
let cache = Arc::new(Cache::new(256 * 1024 * 1024, Arc::new(engine.clone())));
engine.attach(&cache);
diesel_streamgres::install(Settings::single(cache, |_| Route::Streamgres)).ok();
```

`EngineClient::embedded(engine, config)` takes an engine started by hand
(`diesel_streamgres::embedded::Engine::start(Config { ... })`), for an
application that does not configure through the environment.

### What it needs from PostgreSQL

- **The primary**, with `wal_level = logical`. An embedded engine refuses
  a standby: it is for the database its backend writes to.
- **A role** with `REPLICATION` and `SELECT` on the followed schemas.
- **The publication and the schema-change event trigger.** By default
  (`STREAMGRES_EMBEDDED_SETUP=install`) the engine creates whichever is
  missing when it starts: the publication `FOR ALL TABLES`, and the
  trigger stack for its app and shard. It does this once, in one
  transaction under an advisory lock, so processes starting together
  install it once, and it leaves what is there alone. Creating them
  needs a superuser, or a managed database's admin role. Without one,
  install them by migration (`streamgres::embedded::setup_sql(app, shard,
  publication)` gives the trigger stack's SQL) and set
  `STREAMGRES_EMBEDDED_SETUP=require`.

### How it runs

- **`Engine::start` returns at once.** A supervisor thread brings the
  engine up in the background. Until it serves, reads go to Postgres.
  `Engine::state()` says `Starting`, `Serving`, `Down(reason)` or
  `Stopped`.
- **A long transaction delays the start.** Making a slot waits for every
  transaction open at that moment, so an engine started behind one stays
  `Starting` until it ends. The caller never waits, and `Engine::stop`
  returns at once even then.
- **Its slot is temporary by default.** PostgreSQL drops it when the
  feed's connection ends, however the process ends, so a crashed or
  killed process leaves no slot holding WAL.
- **On a temporary slot, a lost feed means a full restart.** A temporary
  slot cannot be resumed, so the engine starts over on a fresh slot and
  snapshot. Its sessions end, and the wrapper reconnects, resubscribes
  and refills meanwhile reading Postgres. A failed start is retried after 1 s,
  doubling up to 30 s while starts keep failing; an engine that served
  for 60 s starts the count again.
- **A permanent slot rides out a lost feed instead**
  (`STREAMGRES_EMBEDDED_SLOT=permanent`, or `Config::slot`). The feed
  reopens the same slot and the engine serves on: sessions and the cache
  stay as they are, and reads go to Postgres only while the lag the
  heartbeats measure is past `STREAMGRES_S2S_MAX_LAG_MS`. The costs:
  - The engine drops its slot when it stops or starts over, but a process
    that dies without stopping leaves its slot holding WAL.
  - Every engine on the database drops such slots once they have been
    inactive for `STREAMGRES_SLOT_CLEANUP_AGE_MS` (default 5 minutes), at
    its start and every minute after. This needs PostgreSQL 17 or later;
    on 16, set it to `0` and rely on `max_slot_wal_keep_size` instead.
  - Set `max_slot_wal_keep_size` either way, for the case where no
    engine is left running to sweep.

  A restart costs the same with either kind: every start makes a fresh
  slot and snapshot, since the engine holds its rows in memory. Choose
  permanent when the network to the database drops often or the rows
  followed are slow to read again; otherwise keep the default.
- **It owns its threads**: one for the engine, one for the feed, and a
  small runtime for reads and sessions. It never runs on the host's
  runtime, so blocking and async Diesel can both use it.
- **Its log lines go to the `log` facade**, under the target
  `streamgres`.

### Configuration

The embedded engine reads the server's variable names
(`streamgres::embedded::Config::from_env`), with defaults sized for a
backend process:

| variable | default | |
|---|---|---|
| `STREAMGRES_ENGINE` | | `embedded` to run the engine in this process |
| `STREAMGRES_PG_DSN` | required | the primary, a direct connection (not a pooler) |
| `STREAMGRES_READ_PG_DSN` | `STREAMGRES_PG_DSN` | where ordinary reads go; may be PgBouncer in transaction mode |
| `STREAMGRES_APP_ID`, `STREAMGRES_SHARD` | `zero`, `0` | name the trigger stack (`<app>_ddl_end_<shard>`) |
| `STREAMGRES_PUBLICATION` | `xyne_sync_pub` | |
| `STREAMGRES_SCHEMAS` | `public,<app>_<shard>` | |
| `STREAMGRES_EMBEDDED_SETUP` | `install` | or `require` |
| `STREAMGRES_EMBEDDED_SLOT` | `temporary` | or `permanent` (section 2, How it runs) |
| `STREAMGRES_SLOT_CLEANUP_AGE_MS` | 300000 | on a permanent slot: how long a dead process's slot stays before an engine drops it; 0 never (PostgreSQL 17+ otherwise) |
| `STREAMGRES_READ_CONNECTIONS` | 4 | read connections the engine opens |
| `STREAMGRES_READ_THREADS` | 1 | at least 1 |
| `STREAMGRES_HEARTBEAT_MS` | 500 | how often the engine measures its lag; 0 off |
| `STREAMGRES_ROW_LIMIT` | 100000 | the most rows one read may return |

The rest (`STREAMGRES_MEMORY_TABLES`, `STREAMGRES_PLAN_*`,
`STREAMGRES_S2S_CONNECTION_SUBSCRIPTIONS` and so on) mean what they mean
for the server; see [`.env.example`](../.env.example).

## 3. S2S mode

Run the server as [`deploy.md`](deploy.md) describes, with setup done once
on the primary as a migration (the publication, and the event trigger
from `scripts/sql/ddl-triggers.sql`), a token in `STREAMGRES_S2S_TOKEN`
(the server serves no S2S endpoint without one), and preferably the
endpoint on a private listener of its own (`STREAMGRES_S2S_ADDR`).
Backends connect with:

| variable | default | |
|---|---|---|
| `STREAMGRES_S2S_URL` | required | `ws://<host>:4848/<base>/s2s/v1`; `wss://` off a private network |
| `STREAMGRES_S2S_TOKEN` | required | the server's bearer token (one of them, while it rotates) |
| `STREAMGRES_S2S_CONNECTIONS` | 2 | sockets per backend process |
| `STREAMGRES_S2S_MAX_SUBSCRIPTIONS` | 10000 | per process; past it reads go to Postgres |
| `STREAMGRES_S2S_MAX_LAG_MS` | 2000 | past it reads go to Postgres; 0 trusts the engine |

The cache's own limits (`STREAMGRES_CACHE_MAX_BYTES`,
`STREAMGRES_CACHE_IDLE_MS`, `STREAMGRES_CACHE_TTL_MS`,
`STREAMGRES_CACHE_MAX_ENTRY_BYTES`, `STREAMGRES_REFUSED_TTL_MS`) apply in
both modes.

## 4. Reading from replicas

A backend with a primary pool for writes and a replica pool for reads
shares one cache between them and routes by URL:

```rust
diesel_streamgres::install(Settings::single(cache, |url| {
    if url.contains("replica") { Route::Streamgres } else { Route::Postgres }
})).ok();
```

Two things keep reads through a replica correct:

- **A refill is kept only once the replica has caught up.** The engine
  names, for every query it follows, the position of the last change it
  will not report. A refill on a standby reads how far it has replayed
  before its query, and keeps the rows only if it is past that position;
  otherwise it serves them, as a plain replica read would, and the next
  read refills again. These count as `unreplayed`. On a primary the check
  is asked once per connection and never again. Rows the engine serves
  itself read no replica at all.
- **Writes give the replicas' reads their position.** A standby cannot
  say where the primary's WAL is. Once reads have gone to one, a write
  that commits on its own, or a `COMMIT`, reads the position on its own
  connection right after: one more small query per such write on the
  blocking wrapper, pipelined behind the write on the async one. Reads
  then use the cache as soon as the engine has delivered past it
  (section 5).

Reads on a replica still see only what the replica has replayed. A write
followed at once by a read through the replica may not see the write,
cache or no cache, exactly as without Streamgres.

### The engine on a standby (S2S only, optional)

An S2S server can follow a PostgreSQL 16+ physical standby instead of the
primary, moving its walsender CPU and slots off the primary. The costs:
- `hot_standby_feedback = on` on the standby.
- A new slot on a standby waits for the primary's next running-transactions
  record: up to 15 s on a primary with writes, and for as long as an idle
  one stays idle (`SELECT pg_log_standby_snapshot()` on the primary writes
  one). This holds the server's start back; see
  [`deploy.md`](deploy.md#physical-standbys).
- The standby's replay lag adds to the engine's.
- A standby cannot write heartbeats, so the engine reports no lag and
  backends trust it whatever its delay (`STREAMGRES_S2S_MAX_LAG_MS` has
  nothing to act on).
- The feed, its slot and the read snapshots must all be on that one
  server.

Measured against a local PostgreSQL 17 standby, with writes on the
primary and the wrapper's live suites (`engine_live`, `async_live`,
`shape_coverage`) passing: a change reached the engine's rows in about
5 ms, and a write to the next read served from the cache took 8 to 18 ms.
A physical standby shares the primary's WAL positions, so a write's
position compares directly with the engine's. These runs are manual: CI
has no standby.

## 5. Read your writes

A write through a cache sends reads through it to Postgres until the
cache holds the write, on every connection of the cache:
- **By position.** The engine reports the WAL position up to which it has
  delivered every change. The first read after a write (or, with
  replicas, the write itself) reads the write's position; reads go to
  Postgres until the engine is past it. On a local engine that is 6 to
  13 ms after the write.
- **By a window**, against an engine that reports no position, or when
  the position cannot be read: `STREAMGRES_READ_YOUR_WRITES_MS` (default
  1000) after the last write.

`STREAMGRES_READ_YOUR_WRITES_MS=0` turns both off. Writes from other
processes or services are not covered: their changes reach the cache when
the engine ships them, typically a few milliseconds after the commit.

## 6. Who a cached answer is for

The cache keys a read by its SQL and binds, but which rows a reader may
see also depends on who reads, and how. The wrapper keeps every answer
to readers who would have read the same rows.

- **One partition per context.** A connection reads its context on its
  first read through the cache. The context is:
  - its database, its role (`current_user`) and the role it logged in as
  - its search path
  - the settings that change what an accepted query means
    (`transform_null_equals`, `standard_conforming_strings`)
  - the tables its role may not `SELECT` whole

  Readers in one context share answers and subscriptions; readers in
  different contexts never do. An admin connection and the application's
  connections are each cached, apart. One cache keeps up to 64
  identities (a database, role, search path and settings) apart. When a
  table an identity could read becomes unreadable (a `REVOKE`), it gets a
  fresh context rather than one more, and nothing cached under the old
  privileges is served again. A table created since, or a `GRANT`, keeps
  its answers: nothing cached read the one, and the other takes nothing
  away.
  On the blocking wrapper, reading the context is one more small query;
  the async one runs it beside the read, which then goes to Postgres.
- **Read again when it may have changed.** A connection reads its
  context again:
  - after a statement that may change it without saying so: DDL
    (`CREATE`, `ALTER`, `DROP`), `GRANT`, `REVOKE`, `CALL`, or a read
    that calls a function of the database's own (one that moves the
    search path, say)
  - at least every 10 s. This bounds how long a change made by another
    session (a `REVOKE`, a table created) goes unnoticed.
- **The session as it was established.** After `SET` (a role, a search
  path, a tenant setting; anything but a few timeouts and
  `application_name`), `SET ROLE`, `SET SESSION AUTHORIZATION`, a call
  to `set_config` or a `DO` block, the connection reads Postgres until
  `RESET` of the same setting, `RESET ALL` or `DISCARD ALL` (only the
  last two after `set_config` or `DO`). `SET LOCAL` needs nothing:
  reads inside a transaction go to Postgres anyway.
- **Custom settings cannot change an answer.** A setting such as
  `app.tenant`, from `SET`, the connection URL (`options=-c …`) or
  `ALTER ROLE … SET`, only matters to a query through row-level security
  or a function. The engine refuses both, so those reads go to Postgres.
- **No row-level security.** The engine reads as its own role, so it
  cannot apply a policy for a backend's. It refuses every query over a
  table with row-level security enabled. Turning it off, or on in the
  transaction that creates the table, is followed as it commits. Turning
  it on for a table followed before restarts the engine, which takes the
  change from the catalog it reads again, so no change made after it
  reaches a backend. A trigger stack older than this misses
  it until a restart: re-run the setup SQL (`scripts/sql/ddl-triggers.sql`,
  or `setup_sql`). The server says so at startup; an embedded engine that
  installs its setup updates it itself.
- **Privileges.** The engine reads with its own role. For a context
  whose role may not read one of a query's tables whole, the engine side
  builds no rows. The read refills from Postgres on that reader's own
  connection, where its privileges apply, and is cached for that context
  only. The engine still sends every column of a followed table's rows
  to the backend's process.
- **Bare names mean `public`.** The engine reads a bare table name in
  `public`. A connection whose search path puts a relation of the same
  name before it reads Postgres. That covers a table in an earlier
  schema, or one of the session's temporary tables. A search path with
  other schemas first, holding no such name, is cached as usual.

`other_context` counts the reads sent to Postgres for any of these
reasons:
- a changed session
- a shadowed name
- another database than the cache's
- a 65th identity

## 7. What to watch

- **`Cache::stats`**, per cache: `hits`, `misses`, `replaced` (rows the
  engine's deltas changed), `touched` (refills a change caused),
  `refused`, `after_write` (reads sent to Postgres for read-your-writes),
  `lagging` (sent there because the engine was behind), `unreplayed`
  (replica refills not kept), `other_context` (sent there because the
  connection's context cannot be served, section 6),
  `oversized`, `evicted`, `expired`.
- **`EngineClient`**: `is_connected()`, `lag()`, `subscribed()`,
  `materialized()` (queries served from rows), `fallbacks()` and
  `refusals()` (recent reasons), `warnings()` (configuration worth a log
  line at startup).
- **Embedded**: `Engine::state()` and `Engine::states()` (a watch), and
  `Engine::slot()`, the slot it streams from.
- **On the database**: `pg_replication_slots` and walsender counts
  against their limits, above all in embedded mode.

## 8. Measured on one machine

Local PostgreSQL 17, the engine and the backend on one host:

| | embedded | S2S |
|---|---|---|
| a commit reaching the cache, p50 | 2.4 ms | 2.4 ms |
| a miss served from the engine's rows, p50 | 1.0 ms | 1.2 ms |
| a write to the next read served from the cache | 6 to 13 ms | |

Over a network the S2S numbers grow by its round trip.

### Embedded engines under write load

Three backend processes, each with an embedded engine following the same
200 queries over a 100,000-row table (four columns; every row in some
query), with 1,000 single-row updates a second on the primary, all on one
4-core host:

| | per engine |
|---|---|
| walsender CPU on the database | 8.4% of a core (25.2% for the three) |
| the engine's CPU in its process | about 20% of a core; 48% at 5,000 updates a second |
| memory, engine started, no queries | 7.5 MB |
| memory, holding the 100,000 rows | 90 MB at first, levelling off at 109 MB once every row had changed |
| replication slots | 2 while serving, 3 while a read snapshot rotates |

One engine alone cost the same per engine. The memory is about 1 KB per
held row in this table, for the rows and the indexes the queries need;
it grows with the rows followed, not with the write rate. The CPU is
mostly work every committed transaction costs whether or not a query
follows it, decoding the feed and stepping the engine: an engine following nothing still
took 15% of a core at 1,000 updates a second. Per transaction it falls
as the rate rises, since busier feeds arrive in batches.
