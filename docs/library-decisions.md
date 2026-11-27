# Streamgres library: implementation decisions

The library is one call, `load_and_sync`, in the core crate and in the Diesel adapter. This file records how that call behaves, what is set at startup, how queries are kept and evicted, which connections it uses, what the engine answers exactly, and how a read is inspected. Each decision says what it is, why, and what it costs. The questions still open are listed at the end.

## 1. Shape

| Piece | Where | Visibility |
|---|---|---|
| `Streamgres`, `Builder`, `Sql`, `Rows`, `Ttl`, `Fallback`, `OnUnsupported`, `Error`, `Metrics` | `src/cache/api.rs`, `config.rs`, `rows.rs`, `metrics.rs` | Public (`streamgres::cache::…`) |
| `Trace`, `Inspect`, `KeptQuery`, `EngineState`, `Caller`, `QueryState`, `Served` | `src/cache/inspect.rs` | Public |
| `Bind`, `SessionError`; `Op` and `block_on` (await or wait) | `src/sync/pg/connections.rs`, `src/cache/api.rs` | Public |
| Streamgres's own pool and connections: `Session`, `Lease`, `OwnPool` | `src/sync/pg/connections.rs` | Private to the library API |
| The query cache: kept queries, ttl, budgets, reuse, admission, encoding | `src/cache.rs`, `src/cache/state.rs`, `rows.rs`, `parse.rs`, `local.rs`, `reader.rs` | Private |
| Result composition: joins, post-filters, ordering, projection, paging | `src/compose.rs` | Private |
| The seam to the engine: `trait Engine`, `Instance`, the supervisor | `src/cache/engine.rs` | `pub(crate)` |
| The Postgres engine behind the seam: start, feed, slot, sampling, origin reads | `src/sync/embed.rs` (`PgEngine`) | Private |
| Diesel adapter: `q.load_and_sync(&lib)` (`.await` it, or `.wait()`), `q.inspect(&lib)` | `crates/streamgres-diesel` | Public crate |
| Ticket desk: the adapter's showcase and test bed | `examples/ticket-desk` | Example |

- The cache reaches the engine only through `trait Engine`. It imports nothing from the engine's threads, feed or storage.
- The cache uses two pure Postgres helpers: the binary codec (`sync::pg::binary`, for encoding rows) and SQL quoting (`sync::pg::sql`, for the reader check).
- `compose` has no cache, Postgres or client dependency. It uses the engine's values, query plan and comparison rules, and takes its budgets from the caller. A future client can reuse it without copying the join algorithm. There is no public composer trait.
- The adapter uses only the public API. Rows reach it as Postgres binary bytes, and Diesel's own `FromSql` decodes them.
- `trait Engine` is private. It is not the public `Storage` / `Replication` / `Client` traits planned for the engine refactor, and does not commit to their shape.

## 2. The call

```rust
let lib = Streamgres::connect(dsn)
    .publication("app_pub")
    .start()
    .await?;

let rows: Rows = lib
    .load_and_sync(Sql::new("SELECT … WHERE merchant_id = $1").bind(mid))
    .ttl(Ttl::Forever)
    .await?;

let cfg: Vec<MerchantConfig> = merchant_config::table
    .filter(merchant_config::merchant_id.eq(mid))
    .select(MerchantConfig::as_select())
    .load_and_sync(&lib)
    .ttl(Duration::from_secs(300))
    .await?;
```

Synchronous Diesel code makes the same call and ends it with `.wait()` instead of `.await`. There is one trait and one builder; the caller picks how to wait at each call:

```rust
let lib = Streamgres::connect(dsn).publication("app_pub").start().wait()?;

let cfg: Vec<MerchantConfig> = merchant_config::table
    .filter(merchant_config::merchant_id.eq(mid))
    .select(MerchantConfig::as_select())
    .load_and_sync(&lib)
    .ttl(Duration::from_secs(300))
    .wait()?;
```

| Async | Blocking |
|---|---|
| `.load_and_sync(&lib).await` | `.load_and_sync(&lib).wait()` |
| `.load_and_sync(&lib).ttl(ttl).await` | `.load_and_sync(&lib).ttl(ttl).wait()` |
| `.load_and_sync(&lib).inspect().await` | `.load_and_sync(&lib).inspect().wait()` |
| `start().await`, `shutdown().await`, `refresh_types().await` | `start().wait()`, `shutdown().wait()`, `refresh_types().wait()` |

- `.wait()` runs the same future as `.await`, parking the calling thread until Streamgres's runtime wakes it. On a multi-thread Tokio worker it moves the worker's other tasks off first (`block_in_place`).
- Every I/O runs on Streamgres's own runtime, so waiting never needs the caller's runtime and cannot deadlock it, a current-thread runtime included.
- Core reads have the same pair: await `lib.load_and_sync(sql)`, or call `.wait()` on it.

| Case | What happens |
|---|---|
| The query is kept and its rows are ready | Answered from memory: no I/O, no thread hop. |
| The query is not kept | Registered with the engine; this read goes to the fallback. |
| The query is still loading | The fallback answers. A read never waits. |
| The statement cannot be answered from memory against the current catalog: SQL Streamgres does not parse, an unknown table or column, a column it does not hold exactly, a table the reader check excludes | `Error::Unsupported(reason)`; nothing is sent to Postgres and the query is not registered. `.on_unsupported(OnUnsupported::Fallback)` sends it to the fallback instead. |
| The engine cannot answer this read (too many rows or too heavy, a value it cannot encode yet) | Fallback. Engine query refusals are remembered for 60 s. Result-composition and encoding refusals use a separate bounded memo keyed by SQL, binds, catalog, query id and row generation; they expire after 60 s or cease to match after those rows or the catalog change. Different projections and sort orders remain independent. Result-refusal bookkeeping is capped at 256 entries (also limited by `max_queries`) and at the smaller of 1 MiB or 1/64 of the cache budget; under pressure older refusals may be retried sooner. |
| The engine is not ready (starting, restarting, behind by more than `max_lag`, rows settling after a change) | Fallback |

- Unsupported is decided from the statement and the catalog, not from load or lag, so it is reported as an error rather than hidden behind Postgres. It changes only when the catalog does: a column added or dropped, row-level security turned on or off, a grant.
- Every other refusal is a run-time condition and goes to the fallback (section 7).

- The return is the rows only: `Rows` in core, `Vec<U>` in Diesel. How a read went comes through `.inspect()` (section 8).
- The caller needs no runtime of its own (section 6, Runtime):
  - an awaited read may be polled by any executor;
  - a blocking read blocks only the calling thread.
- The Diesel call takes no connection.
- The adapter renders the query when the read is awaited, exactly as Diesel would send it. The query's values must be `Send` and live until the read completes.
- Reads inside a transaction stay on the application's own Diesel connection. Streamgres never sees them.

## 3. Startup: what is set on the builder

| Setting | Default | Meaning |
|---|---|---|
| `connect(dsn)` | required | The database: Streamgres's own pool, the feed, snapshot exports and the slot (section 6). |
| `.pool_size(n)` | 8 | Connections in Streamgres's own pool. |
| `.publication(name)` | `streamgres` | The publication the feed streams. |
| `.schemas([..])` | `public` | The schemas whose tables are carried. |
| `.app(id, shard)` | `streamgres`, `0` | Names the schema-change trigger stack. |
| `.slot_cleanup_age(d)` | never | Drops this kind's slots left inactive for `d` (PostgreSQL 17+). |
| `.cache_budget(bytes)` | 256 MiB | What all kept rows may weigh together. |
| `.max_query_size(bytes, rows)` | 32 MiB, 100 000 | Per query. A larger query goes to the fallback. |
| `.default_ttl(ttl)` | 5 min | The ttl of a read that names none. |
| `.max_lag(d)` | 2 s | How far behind Postgres the engine may be and still answer. |
| `.fallback(..)` | `Postgres { 8, 1 s, 10 s }` | What answers a read refused at run time (section 7). |
| `.on_unsupported(..)` | `Error` | Whether an unsupported statement is an error or goes to the fallback (section 7). |
| `.start_timeout(d)` | 30 s | How long `start()` waits for readiness, and how long one engine may take to start. |
| `.allow_degraded_start()` | off | Lets `start()` succeed before the engine is ready; reads fall back while it comes up. |
| `.debug_history(n)` | 0 | Reads recorded per query for `inspect` (section 8). |
| `Builder::setup_sql()` | none | The DDL trigger stack, for an administrator to run once. |

- `start()` connects, starts the engine and waits up to `start_timeout` for it to be ready. Setup, readiness and fallback are separate:
  - a missing DDL trigger is `Error::Setup`, with or without a degraded start: no wait mends it;
  - one deadline, `start_timeout`, covers trigger verification, the readiness wait and initial type discovery. If no time remains for type discovery, the background watcher retries it once the catalog is available;
  - an engine not ready in time is `Error::NotReady`, unless `allow_degraded_start()` lets reads fall back while it comes up;
  - `fallback(..)` decides only what answers refused reads once started.
- A failed `start()` returns at its deadline and leaves cleanup to the supervisor. A missing trigger fails before any replication slot exists; after a readiness timeout, a slot already created stays visible until that cleanup finishes.
- `shutdown().await` stops the engine and waits for cleanup, including dropping its replication slot. Concurrent shutdown calls wait for the same supervisor join. Dropping the last handle starts the same cleanup in the background without waiting; process exit can cut it short.
- Fixed, not on the builder:

| Value | Setting |
|---|---|
| Queries kept at most | 10 000, a bound on bookkeeping; the byte budget normally binds first |
| A query still loading is dropped as stuck after | 10 s; its next read registers it again |
| A refusal is remembered for | 60 s |
| Sweep | every 1 s |
| Source WAL sampling | every 200 ms |
| Type refresh check | every 250 ms |
| Restart backoff | 1 s, doubling to 30 s |
| Engine storage reads at once | 4 |

## 4. Retention and eviction

**ttl**

| Rule | Decision |
|---|---|
| Values | `Ttl::Immediate` (freed at the next sweep after its last read), `Ttl::After(d)` (a `Duration` converts), `Ttl::Forever`. |
| For one read | The read's own `.ttl()`; without it, `default_ttl`. |
| Several reads of one query | The longest ttl any read asked for stands while the query is kept. Reads answered from a thread's local copy apply their ttl too. |
| Freed when | Ready, and unread for longer than its ttl. Checked every second. |
| Not freed by ttl | A query still loading or settling. Only a stuck one goes. |

**Budgets**

| Limit | When it is hit |
|---|---|
| `cache_budget` | The least recently read queries go first. `Ttl::Forever` queries are eligible too, after every other. About 1/16 of the queries go per pass until the total fits. |
| `max_query_size` | That query is refused (remembered for 60 s) and served by the fallback. |

- An evicted query is unsubscribed from the engine. Its next read registers it again, and that read goes to the fallback.
- A kept result is identified by its node layout, root, full ordering, post-filters, projection and catalog. Queries that differ only in projection share the underlying rows and one composed ordering.

## 5. Freshness

- The engine answers only while all of these hold:
  - a source WAL position sampled within `max_lag` has been applied;
  - the engine has heard the feed within `max_lag`;
  - its own queueing delay is within `max_lag`.
- Positions are sampled every 200 ms on an ordinary connection (`pg_current_wal_lsn()`, or `pg_last_wal_replay_lsn()` on a standby). A busy feed replaying old WAL cannot look fresh.
- A read does not see the application's own just-committed write until the feed applies it. Read-your-writes (waiting on a WAL position) is not in v1.

## 6. Connections

| Work | Connection |
|---|---|
| Memory hit | None |
| Fallback reads | A checkout from Streamgres's pool |
| Catalog, trigger check, custom types, reader check | A checkout from Streamgres's pool |
| WAL position sampling | One connection of its own to `connect(dsn)`, apart from any pool |
| Loading a query's rows | The engine's storage connections (4), each read one simple-query batch (`BEGIN`, snapshot import, `SELECT`, `COMMIT`) in Postgres's text format |
| WAL replication, exported-snapshot minting, slot create and drop | Dedicated connections |

- Streamgres's pool is its own: `pool_size` connections to `connect(dsn)`, opened on demand and kept idle. The application's pool is never used or sized for it.
- A checkout is exclusive and outside a transaction. A successful operation normally returns it. Failure, timeout, cancellation and unwinding discard it. Fallback in permissive SQL mode discards the checkout even after success, since arbitrary read expressions can change session state.
- The role of `connect(dsn)` needs replication privileges and read access to the carried tables.
- `Debug` of a `Builder` shows the DSN only as its parsed user, hosts, ports and database (and `password=***` when one is set), never the string itself.

**Runtime**

- Streamgres runs a Tokio runtime of its own, two `streamgres-io` threads, created by `start()`. Its pool checkouts and connection drivers, fallback reads, reader checks, type refreshes, timeouts and background tasks run there.
- A read answered from memory never leaves the caller's thread: no hop, no runtime.
- Any other read runs as a task on that runtime:
  - an awaited read waits on the task's handle;
  - a blocking read waits on a channel.
- Dropping an awaited slow read cancels its queued task. Once an origin task has begun database work, it retains its concurrency permit until it finishes or times out; cancellation cannot free a turn while SQL is still running.
- The runtime outlives the engine that uses it. When the last handle goes, it is shut down from a thread of its own, so a handle may be dropped anywhere, an async context included.

**Only Streamgres's own connections**

- Streamgres never uses or borrows the application's pool. Fallback reads, catalog work, reader checks and storage reads all run on its own connections, on its own runtime. Why: the application's load cannot starve Streamgres's checks and Streamgres's fallback reads cannot starve the application's writes; errors keep Postgres's real SQLSTATE; storage reads stay one simple-query batch; and no read ever needs the caller's runtime.
- The cost is connections per process: `pool_size`, the storage reads, the feed, snapshot minting and the sampling connection. Size `pool_size` and the server's `max_connections` for the number of replicas.
- The role of `connect(dsn)` reads on behalf of every caller: give it read access to the carried tables only.
- Admission excludes every RLS-enabled table, including for bypass roles: the WAL stream does not enforce row policies. Embedded-library DDL commits invalidate reader checks even when column shapes do not change. Reinstall `Builder::setup_sql()` on existing installations to enable the database-local GRANT and REVOKE invalidation trigger. An authorization check expires after five seconds even without a WAL signal, covering role membership changes and installations awaiting the trigger upgrade. Failed revalidation cannot authorize a memory read. Immediate revocation still requires coordinating with the application.
- A schema change or an expired authorization check needs a fresh reader check on a pool connection; while every pool connection is held by slow fallback reads, warm queries fall back until one frees.
- Source-position sampling has a connection of its own, so a pool saturated by fallback reads cannot starve it and turn every read into a fallback. It samples the `connect(dsn)` endpoint, primary or standby. Each sample has a two-second deadline covering connecting, execution and decoding; a failure records its cause, drops the connection, leaves freshness to expire and retries.
- Streamgres's own `Session` declares every bind by its type OID, custom types included, as the application's own driver does; `unknown` would let Postgres infer a type the bind alone decides.
- A fallback read's failure reaches the caller as `Error::Origin(SessionError)`:

| Variant | Meaning | Reliable for retry decisions |
|---|---|---|
| `Database { code, message }` | Postgres refused or failed the statement | `code` is the SQLSTATE Postgres returned. |
| `Timeout` | Postgres cancelled it for `statement_timeout`, or a Streamgres deadline (the fallback's `timeout`) passed | Streamgres's own deadlines always. Postgres's: told from a cancel request by its message, best effort for an untranslated server. |
| `Cancelled` | Postgres cancelled it on request (`pg_cancel_backend`, a client cancel) | As for `Timeout`: both share SQLSTATE 57014. |
| `Other` | The connection, protocol, pool or task | This category does not determine whether retrying will help. |
Diesel reads continue to return `QueryResult`. Known SQLSTATE values map to the corresponding Diesel database error kinds. Other Streamgres failures, including timeouts and cancellations, are boxed inside `QueryBuilderError` as a downcastable `StreamgresError`.

## 7. Fallback

| `Fallback` | Behaviour |
|---|---|
| `Postgres { max_concurrent, wait, timeout }` | Postgres answers over a checkout. At most `max_concurrent` at once. A read waits up to `wait` for a turn (else `Error::Busy`) and takes up to `timeout` (else `Error::Origin`). The rows are Postgres's own binary bytes. |
| `Error` | `Error::Refused(reason)`, including a query's reads while it loads. |

| `OnUnsupported` | Behaviour |
|---|---|
| `Error` (default) | `Error::Unsupported(reason)`; the trace says `Unsupported`. |
| `Fallback` | The statement goes to `Fallback`, as a run-time refusal does. |

- `Fallback` applies to run-time refusals: not ready, loading, budgets, values not yet encodable.
- A budget decides only whether memory answers, never whether a statement may reach Postgres. A statement whose bind values exceed the evaluation budget (the smaller of 1 MiB and 1/16 of the entry budget) is classified by its SQL shape, bind count and types, and the reader's permissions. Admission and inspection skip bound parsing for oversized values. Query tracing still fingerprints the binds. Only an admitted statement falls back.

- Fallback only accepts a single read statement and runs it in a read-only transaction with transaction-local server timeouts. The built-in pool batches transaction and timeout setup, prepares the read to preserve column metadata for empty results, and pipelines execution with rollback on reusable checkouts. It recycles a checkout only after PostgreSQL confirms rollback, including after a failed read. With `OnUnsupported::Fallback`, it discards the session to avoid leaking session changes. The built-in pool closes that checkout without a separate rollback request. This API expects application-controlled SQL. Use a least-privilege database role for fallback.
- A fallback read runs as its own task and holds its turn until the work completes or reaches its deadline, even if the caller is cancelled. The built-in pool aborts the connection driver when discarding a session.
- With `OnUnsupported::Error`, degraded-engine reads are validated against a source catalog before fallback. This admission check is single-flight and memoized for 250 ms. Failure to obtain that catalog is a visible readiness error, rather than permission to execute unclassified SQL.
- On an established built-in connection, a typical fallback read waits for three exchanges covering setup, preparation, and execution with any rollback. Cold type lookups can add waits. Prepared fallback statements are not cached. The manual `concurrent_fallback_benchmark` test reports read latency and pool wait for the built-in path.
- The reader check runs once per catalog version. It catches tables whose name resolves elsewhere under the connection's search path, and tables the role may not read. Reads of those tables are unsupported.
- `Metrics` counts `fallbacks` (Postgres answered) and `fallback_errors` (busy, timed out, failed, or `Fallback::Error`).

## 8. Inspection

| Call | What it gives | Cost |
|---|---|---|
| `lib.load_and_sync(sql).inspect().await` / `q.load_and_sync(&lib).inspect().await` | The rows and a `Trace`: who answered (`Memory`, `Postgres`, `Unsupported`, `Failed`), why not memory, the ttl it is kept for, rows, time taken, the engine's WAL position and lag, and the place in the code that made the read. Core inspection returns a trace even when execution fails. Diesel inspection returns `None` for the trace if rendering or parameter metadata lookup fails before execution. | Builds the trace for that read only. |
| `lib.inspect(&sql)` / `q.inspect(&lib)` | Where the query stands: `NotKept`, `Loading`, `Ready`, `Settling`, `Refused`, `Unsupported` or `NotReady`, with rows kept, bytes, ttl, last read, time until freed, and the engine's state. With `debug_history`, also each place that reads it (with counts by answerer) and its last reads. | No I/O; registers and touches nothing. |
| `lib.queries()` | Every kept query: its SQL, state, rows, bytes, ttl and last read. | No I/O. |
| `lib.engine()` | Whether the engine runs, its instance (the replication slot's name), WAL position, lag, and what it keeps. | No I/O. |

- The place of a read is captured with `#[track_caller]` on `load_and_sync`; the adapter passes on its own caller's place.
- `debug_history(n)` records per query its callers (at most 16) and last `n` traces, for at most 20 000 queries, the least recently read forgotten. It takes a lock per read, so it is off by default.
- A read that falls back is logged at debug level with its query id, place and reason. `Sql` debug output contains only counts. Explicit inspection can retain full SQL and error text and should be shared only with authorized operators.
- `cargo run --release --example warm_reads -- <dsn> [callers] [seconds] [async|blocking]` measures warm reads through the public API: it warms every query it times until memory answers it, runs concurrent callers that each stop at their own deadline, and reports throughput, p50 and p99 with the share memory answered, so a run that was not warm throughout shows it. Each run creates and owns uniquely named fixtures. Async readers yield periodically between measured reads, and a run fails if any caller records no samples.

- Successful memory metrics are incremented only after binary encoding. Unsupported and failed demo reads are not labeled as PostgreSQL answers or counted as successful fallbacks.
- Recent history continues recording after the 16-caller aggregation limit. Caller aggregation and recent-trace retention have independent bounds.

## 9. What the engine answers exactly in v1

A query is answered from memory only if every column it returns or compares holds values exactly as Postgres does. Anything else is unsupported (section 2), which means an error by default and fallback only when explicitly configured. Columns added with unmodeled or unknown OIDs follow that same policy. JSON is admitted under the current implementation; its exactness remains an engine limitation.

| Column type | Returned | `=`, `IN` | `<`, `>`, `BETWEEN`, `ORDER BY` |
|---|---|---|---|
| `int2`, `int4`, `int8`, `oid` | Yes | Yes | Yes |
| `bool` | Yes | Yes | Yes |
| `text`, `varchar`, `name`, `"char"` | Yes | Yes (deterministic collation) | Only with a byte-order collation (`C`, `POSIX`, `C.UTF-8`, `ucs_basic`, `pg_c_utf8`) |
| `uuid` | Yes | Yes | Yes |
| enum | Yes | Yes | No (Postgres orders by declaration) |
| `float4`, `float8` | Yes | Yes | No (NaN ordering differs) |
| `json`, `jsonb` | Yes | No | No |
| `numeric`, `timestamp`, `timestamptz` | Admitted; encoding falls back until main's exact values land | Engine's current rules | Engine's current rules |
| `char(n)` | No (padding comparison) | No | No |
| `date`, `time`, `bytea`, `interval`, arrays, other types | No | No | No |

- Numeric and timestamp columns pass the projection admission check. Integrating main's exact values still requires updating and validating the library codec, bind handling and parser. Until then, a value the encoder cannot write exactly sends the read to the fallback, a run-time refusal. Nothing is encoded lossily.
- Bind values arrive in Postgres's binary format with their type OIDs. A bind whose type does not compare with its column the way Postgres compares them is refused.
- Custom type OIDs (enums, domains) are read at start. A watcher checks catalog identity every 250 ms, including its first observation, and retries failed refreshes without marking them complete; 250 ms is the polling interval, not a completion deadline. Unqualified names use PostgreSQL type visibility for the configured search path. An awaited or blocking Diesel read that names an unknown type refreshes once and renders again; refresh errors preserve the underlying query error. `refresh_types()` returns `Result<(), Error>`, awaited or waited.

**SQL shapes**

| Shape | Answered |
|---|---|
| Quoted and qualified names; a table outside `public` qualified by its bare name | Yes |
| `$n` binds, `AND`/`OR`/`NOT`, `IN`, `= ANY($n)`, `!= ALL($n)`, `BETWEEN`, `IS [NOT] NULL` | Yes |
| `ORDER BY`, `LIMIT`/`OFFSET`, `COUNT(*)`, `SELECT *` | Yes |
| `INNER`, `LEFT` and `RIGHT` joins on equality keys; aliases, self-joins, composite keys | Yes |
| `USING` and `NATURAL` joins with same-type common columns | Yes |
| Filters on a join's nullable side, including anti-joins (`WHERE child.id IS NULL`) | Yes |
| Ordering by a joined column with `LIMIT`/`OFFSET` | Yes; the full candidate rows are composed, then windowed, within the budgets |
| `FULL` and `CROSS` joins, non-equality or computed keys, predicates comparing two joined tables | No |
| `USING` needing type coercion, `NATURAL` with no common columns, aliases of a parenthesized join | No |
| `DISTINCT`, aggregates other than `COUNT(*)`, `GROUP BY`, functions, extra expressions beside `*` or `COUNT(*)` | No |

Joins are translated into the engine's existing join model; no engine, storage or replication change is needed for them. Join results are checked against Postgres cell by cell, for named cases and every two-edge `INNER`/`LEFT`/`RIGHT` chain, star and nested arrangement, before and after inserts, updates and deletes.

## 10. Engine-side changes in this branch

Brought in, because the library cannot run without them:

| Change | Why |
|---|---|
| `server` feature (default off) | The default build is the library: the engine without axum, WebSockets, the profiler or jemalloc. `--features server` adds the server and its binaries. |
| Opaque `Position` (`u128`); `sync::pg::Lsn` holds WAL locations | The cache and runtime order by position, not by Postgres's `X/Y`. Postgres logs still print `X/Y`. |
| Columns carry `type_oid`, `element_oid`, `collation`, `enum_type`; tables their declared order | Encoding rows for Diesel, the exactness check, `SELECT *` order. |
| `parse_select` and prepared statements | Parses ORM `SELECT`s with binds. The Zero parser's tests are unchanged and pass. |
| Settle reports (`Event::Unsettled`, opt-in) | The cache never answers from rows a storage read is still filling. Off for the server. |
| Tables quoted part by part (`"schema"."table"`) | Tables outside `public` could not be read at all. |
| An absorbed `ADD COLUMN` keeps the other columns as they were | Otherwise every column lost its OID and collation, and `SELECT *` order changed. |
| A storage read rounds a timestamp down to the millisecond | The feed's text parse truncates; the `::int8` cast rounded, so a value with half a millisecond or more read 1 ms apart from the feed's copy. |
| `sync::pg::binary` for the exact types only | Decodes binds and encodes rows. |
| `log_debug!` in the engine-only build | Fallback reads log at debug level. |

Left out, for separate PRs:

| Change | Effect of leaving it out |
|---|---|
| Exact numeric, timestamps, dates, times, bytea, JSON; reading Postgres in binary | Those values go to the fallback. Coming on main. |
| Postgres comparison rules in the engine (floats, NaN, collations) | Float ordering and non-C text ordering go to the fallback. |
| A schema change keeping the new column's source type | A column added by `ALTER TABLE`, and a table created after start, have no type OID; reads of them fall back until the next restart. |
| Carrying only fully published tables | All tables of the schemas are carried, as on main. |
| Slot ownership by advisory lock | A process that dies without cleanup leaves its slot behind. `slot_cleanup_age` drops old inactive ones on PostgreSQL 17+. |
| Source lag reported by the feed (`behind`) | Freshness comes from the 200 ms samples alone. |

## 11. The ticket desk

- One `diesel-async` pool for writes and direct reads, and one `Streamgres` with its own pool and runtime. No libpq, no per-connection tracking.
- The blocking trait is tested apart: a plain thread with no runtime, sync Diesel writes, reads from memory and the fallback, a write followed, an unsupported error, and async reads polled by a non-Tokio executor.
- 25 shapes, each with its expected verdict:
  - memory: `card`, `queue_cards`, `queue_count`, `comment_authors` and the always-empty `nothing`;
  - Postgres at run time: shapes returning a `timestamptz`, until it is encoded, and raw SQL;
  - unsupported (an error): shapes comparing or ordering by a `timestamptz`, returning `bytea`, `text[]` or `interval`, or using `ILIKE`, `@>`, `DISTINCT` or `MAX`.
- The desk's ticket list reads `queue_cards`.
- Organizations are read with `.ttl(Ttl::Forever)`.
- Every answer in the UI carries its trace. The Queries tab groups the shapes by verdict, lists the kept queries (`lib.queries()`) and inspects any shape (`q.inspect(&lib)`).
- "Wait until memory shows it" on a write reads the change back through Streamgres, and succeeds only when memory returns the new value.
- Lab experiments: `widen`, `new-type`, `drop`, `connection`, `slot`, `transaction`. The fault experiments find this process's slot through `lib.engine().instance`.
- Verification fails on a mismatch, an error, or a verdict other than the expected one. A 30-second run under 2,654 writes made 21,875 comparisons with no mismatches and no errors.

## 12. Known limits of v1

- `.wait()` is for synchronous code. Inside a `LocalSet` on a multi-thread Tokio runtime, a read that must wait (a miss) panics: Tokio cannot move a `LocalSet`'s pinned tasks off the thread, so it forbids blocking there. A memory hit returns before any blocking and works anywhere. Async code, a `LocalSet` included, uses `.await`.
- A query's reads until it has loaded go to the fallback; with `Fallback::Error` they fail.
- On an idle database the engine is ready only once the feed reports a position past the latest sample, which can take until the server's next keepalive; `start()` may wait for most of `start_timeout`.
- Not yet covered: publication coverage, numeric predicate exactness, JSON value exactness, and schemas that change under a `SELECT *`.

## 13. Open for review

1. **Fixed values.** Section 3 lists them. Should any be on the builder?
2. **Exact types from main.** Integrate main's exact numeric and timestamp values when they land, including codec, bind and parser updates and exactness regressions.
3. **Idle start.** Should `start()` ask the server for a keepalive so an idle database is ready at once?
