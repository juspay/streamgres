# Streamgres as a library: design decisions

Decisions made while building Streamgres as an embedded Rust library with
Diesel as its first integration, on the `library-integration` branch.
Each entry says what was decided, why, what was rejected, and where it
lives in the code. The newest entries are at the end.

## Principles

- **Fix the cause, not the symptom.** When the engine cannot do something
  exactly, the engine is fixed. No fallback layer is added around it.
- **Exact or refused.** A cached answer must equal what Postgres would
  return. Anything the engine cannot answer exactly is refused with a
  reason, and the caller reads Postgres.
- **Database-specific code stays behind seams.** Decoding a source's
  cells, snapshot reads and the change feed are traits. Postgres is the
  first implementation, so another Postgres-wire database (YugabyteDB,
  CockroachDB) is a new `sync/<db>/` module, not a core change.
- **Main is the baseline.** The `feat/s2s-endpoint` branch is a source of
  lessons, not code to merge.

## 1. One exact value model for the whole engine

**Decision.** The engine's `Value` holds what Postgres holds, exactly:

| Postgres | Engine value |
|---|---|
| `numeric` | `Numeric`: base-10000 digits, weight, sign and scale |
| `timestamp`, `timestamptz` | `Timestamp`: microseconds since 2000-01-01, with infinities |
| `date` | `Day`: days since 2000-01-01, with infinities |
| `time` | `Time`: microseconds since midnight |
| `bytea` | `Bytes` |
| any other type (`interval`, `inet`, `money`, ranges, ...) | `Raw`: the type OID and its binary bytes |

`Numeric` compares and hashes as Postgres does: `1.5 = 1.50`, `NaN`
equals `NaN` and sorts above everything else, and the infinities sort
below and above every finite value. Its scale is kept, so writing it back
gives the digits Postgres sent. The infinities of a timestamp are
`i64::MIN` and `i64::MAX`, exactly as Postgres stores them, so ordering
the raw numbers is Postgres's ordering. `Raw` values can be stored and
passed on, but never compared.

**Why.** Main kept `numeric` as `f64` and times as epoch milliseconds,
and did not carry `bytea` at all. A cache built on that cannot hand
Diesel the bytes Postgres would have sent.

**Rejected.**
- *Carrying Postgres's bytes next to the existing values.* That means two
  representations of every row and about twice the memory.
- *A "refill mode" that reads Postgres again for inexact types* (the s2s
  branch). It patches the symptom.

**Where.** `src/model/numeric.rs`, `src/model/time.rs`,
`src/model/value.rs`.

## 2. The engine reads Postgres in binary format

**Decision.** Snapshot reads and the change feed both take values in
Postgres's binary format. Each cell is decoded by its column's type
(`sync::pg::binary`), so a row holds the same values whichever way it
arrived.

- **Snapshot reads** select the bare columns, with no casts. The setup
  (`BEGIN; SET TRANSACTION SNAPSHOT`), the query (`query_typed`, binary
  results) and the `COMMIT` are pipelined, so a read is still one round
  trip.
- **The feed** asks `pgoutput` for binary values (`binary 'true'`,
  PostgreSQL 14+). A column whose type has no binary send function
  arrives as text. It is kept only when the column is declared as text;
  otherwise it is an error.

**Why.** Text parsing and casts were where exactness was lost. Main's
snapshot rounded milliseconds while the feed floored them, so the same
row could read differently depending on how it arrived.

**Rejected.**
- *The `pgoutput` crate in binary mode.* It panics (`assert!`) on a text
  cell, which Postgres sends for types without a binary send function. We
  read the row messages ourselves instead (`sync::pg::pgoutput`, about 200
  lines, borrowing cells from the message).

**Where.** `src/sync/pg/binary.rs`, `src/sync/pg/pgoutput.rs`,
`src/sync/pg/stream.rs` (`image`), `src/sync/pg/mod.rs` (`Pool::read`,
`decode_row`). Live test:
`every_type_reads_the_same_from_a_snapshot_and_the_feed`.

## 3. Cell decoding sits behind a database-neutral trait

**Decision.** `sync::CellDecoder` turns one source cell into an exact
`Value`. `PgBinary` is its first implementation. A CockroachDB changefeed
would add a JSON decoder producing the same values.

**Where.** `src/sync/codec.rs`.

**Still to do.** The engine's position type is `Lsn` everywhere. It should
become an opaque, ordered position that each backend defines. That is a
separate mechanical change, kept out of this work so the two do not mix.

## 4. The catalog carries each column's Postgres type

**Decision.**
- `DbColumn` gains `type_oid`, the column's type OID; for a domain it is
  the base type's OID.
- Text columns also gain `collation`: the collation's name, with
  `default` resolved to the database's own (libc, ICU or the builtin
  provider), and whether it is deterministic.
- Unknown types are carried as `Raw` instead of being left out.
- A table whose primary key is a type the engine cannot compare (`Raw`,
  a list, JSON) is left out.

**Why.**
- The OID tells `int2`, `int4` and `int8` apart (one engine `Int`), and
  `timestamp` from `timestamptz`.
- The collation decides whether the engine's byte-wise text order is
  Postgres's.

**Where.** `src/model/schema.rs`, `src/sync/pg/catalog.rs`,
`src/sync/pg/ddl.rs`.

**Known gap.** A column whose type is a domain may stop the server when a
schema change touches its table. The DDL message names the domain, while
the catalog uses the base type. Main has the same problem.

## 5. Time literals are written for their column's type

**Decision.** A `timestamptz` comparison is written as a `timestamptz`
literal, so the result does not depend on the session's `TimeZone`.

**Where.** `src/sync/pg/sql.rs` (`literal_as`).

## 6. The Zero server converts at its own edge

**Decision.** The Zero sync protocol keeps showing its clients what they
saw before. The conversion happens in the server, not the engine:

- **Times** are shown as epoch milliseconds. A comparison on a time
  column compares the millisecond the client sees: `= X` matches every
  microsecond inside that millisecond, and `> X` starts at the next one.
  That keeps cursor pagination right on microsecond data.
- **Numerics** are shown as numbers.
- **Raw values** are shown as Postgres's text for `interval`, `timetz`,
  `inet`, `cidr` and `macaddr`.

**Behaviour changes.**
- Filtering a Zero query on a `Raw` column is refused. Before, it
  compared the text, which was wrong for those types.
- `money` and `tsvector` are shown as `null`. Main's feed failed on
  `money`.

**Where.** `src/client/ast.rs` (`compare`, `shown_time`),
`src/client/wire.rs`, `src/client/pg_text.rs`.

## 7. The engine builds without the server

**Decision.**
- A `server` Cargo feature, on by default, covers the Zero server,
  telemetry, profiling and the `server`, `bench` and `load` binaries.
- With `default-features = false` the crate is the engine alone. Its
  direct dependencies drop to 16; axum, reqwest, tungstenite, pyroscope
  and jemalloc are gone.
- The schema-change trigger stack moves to `sync::pg::ddl_triggers`,
  since the engine needs it wherever it runs.
- CI lints both builds.

**Not moved yet.** The join planner (`client::plan`) depends on the Zero
query AST. It moves out when the cache serves joins.

**Where.** `Cargo.toml`, `src/lib.rs`, `.github/workflows/ci-pr.yml`.

## 8. A parser for the SQL an ORM renders

**Decision.** `parser::parse_select(sql, binds, catalog)` reads a
single-table `SELECT` as Diesel writes it:

- quoted and schema-qualified names;
- `*`, `table.*`, `COUNT(*)` or a column list;
- `$n` binds in Postgres binary format;
- `= ANY($n)`, `!= ALL($n)` and `BETWEEN`;
- `NULLS FIRST` and `NULLS LAST` when they match Postgres's default;
- `LIMIT` and `OFFSET` as numbers or binds.

It returns the engine's query, what the statement projects, and its own
limit and offset. The engine follows `limit + offset` rows.

Every read now brings a literal or bind to its column's type when the two
are one value in different forms:
- epoch milliseconds or text for a time;
- a number for a numeric;
- an enum label sent as its own type for a text column.

The old read parser compared filter values unconverted, so after
decision 1 a millisecond literal would have matched nothing.

**Refusals** (`check_exact`). These are rules, not stopgaps:

- **No comparison at all** on `citext`, `char(n)`, a text column under a
  nondeterministic collation, or a `Raw` column. Postgres's equality
  differs there (case, padding, its own rules).
- **No range filter or `ORDER BY`** on text whose collation does not order
  by bytes, on an enum (ordered by declaration), or on JSON.
- **No value of another type** than its column's.
- **Not supported:** `DISTINCT`, computed or renamed columns,
  `FOR UPDATE` / `FOR SHARE`, and joins (for now).

**Rejected.**
- *A second parser beside main's.* The lexer and condition parser are
  extended instead, so writes, `parse_read` and `parse_select` share how
  they read columns.

**Where.** `src/parser/select.rs`, `src/parser/mod.rs` (`operand`,
`bind_value`, `coerce_read`).

## 9. Slots are owned by an advisory lock and swept when the owner is gone

**Decision.**
- Each process holds a session advisory lock, keyed by its slot's name,
  on a connection of its own for as long as it runs. It takes the lock
  again if that connection is lost.
- At startup and every minute, every engine drops the inactive
  `xyne_sync_slot_*` slots whose lock it can take, because their owner is
  gone.
- A slot whose owner still holds the lock is kept, however long its feed
  takes to reconnect.

**Why.** A crashed process's slot holds WAL. Main's cleanup only ran at
startup, only on PostgreSQL 17 (`inactive_since`), and only after an
age set by `STREAMGRES_SLOT_CLEANUP_AGE_MS`.

**Rejected.**
- *Age-based cleanup.* It is removed, along with its setting.

**Trade-off.** If the owner's lock connection drops and a sweep runs
before it reconnects, the slot can be dropped while its feed reconnects.
The feed then finds the slot gone and stops rather than skip changes.

**Where.** `src/sync/pg/slots.rs`, `src/sync/pg/threads.rs`
(`keep_and_sweep`). Live test: `a_sweep_drops_only_slots_whose_owner_is_gone`.

## Test setup

- The live suite runs one more scenario that uses replication slots, so
  CI's Postgres allows 64 slots instead of 32. Main's suite was already
  logging "all replication slots are in use".

## 10. The catalog carries only tables the publication streams in full

**Decision.** At load, the catalog keeps only the tables whose every
change the feed carries:
- the publication must publish inserts, updates and deletes;
- a table with a row filter, a column list or a generated column is left
  out, with a note. (Generated columns are not replicated before
  PostgreSQL 18.)

This applies to the server as well as the cache.

**Why.** Every table in the configured schemas used to be carried. A table
outside the publication was read from snapshots and never changed again,
so its rows silently went stale.

**Where.** `src/sync/pg/catalog.rs` (`restrict_to_publication`), called by
`threads::load_catalog_at`.

## 11. Tables outside `public` are named part by part in SQL

**Decision.** A catalog name `schema.table` is written as
`"schema"."table"` (`quote_table`).

**Why.** It was quoted as one name, `"schema.table"`, so no table outside
`public` could be read. That is a bug on main.

**Known gap.** The planner's statistics lookup (`pg_stats` by table name)
still assumes `public`. Estimates for other schemas fall back to having
no statistics.

## 12. The embedded cache

**Decision.** `streamgres::cache::Cache`:

- **Runs on its own threads.** A supervisor thread runs one engine at a
  time, each on a single-threaded runtime of its own, plus a two-thread
  runtime for storage reads. The host needs no tokio runtime, so blocking
  Diesel works.
- **Restarts on failure.** A failed engine (lost slot, service ended) is
  torn down and its slot dropped. A fresh engine starts on a fresh slot
  after 1 s, doubling up to 30 s while engines keep failing within a
  minute of starting.
- **Reads by SQL and binds.** `read(sql, binds)` parses against the
  engine's catalog. The first read of a query subscribes it and waits up
  to `fill_timeout` for its rows. Later reads are answered from the rows
  the engine keeps current: in the statement's order with ties broken by
  primary key, offset and limit applied, the projection taken, and each
  column with its type's OID. The cache key is the engine query itself,
  so two SQL texts that mean the same query share one subscription.
- **Refuses in two ways.** `Unsupported` means the engine cannot answer
  exactly; it is remembered for `refusal_ttl`. `NotReady` means the
  engine is starting, restarting, unheard from for more than `max_lag`,
  or still filling the query. Either way the caller reads Postgres.
- **Read-your-writes is explicit.** `wait_for(lsn)` waits until the engine
  has applied a position, such as `pg_current_wal_lsn()` read after a
  write. Freshness is never assumed silently.
- **Bounded.** Queries unread for `idle_ttl` are unsubscribed. When
  `max_entries` are kept, the one read longest ago gives way.
- **Its own trigger stack.** It hears schema changes through
  `{app}_ddl_end_{shard}`, by default `streamgres`, shard 0, next to any
  Zero stack. `CacheConfig::setup_sql` and `teardown_sql` give the SQL;
  event triggers need a superuser, so an administrator runs it.

**Rejected.**
- *A per-read freshness check against Postgres.* It costs a round trip
  per read, which is the cost the cache exists to remove.

**Where.** `src/cache.rs`, `src/cache/{config,engine,rows,state}.rs`.
Live tests: `tests/cache_live.rs`.

## 13. Range conditions route on every exact type

**Decision.** The routing index gives every exact type a comparison
class:
- `Numeric` shares the class of the numbers;
- `Float` and `Numeric` compare through the float, as Postgres compares
  `float8` with `numeric`.

**Why.** Without a class, a range filter on a numeric, timestamp, date,
time or bytea column was never re-evaluated when a row changed. That
missed rows in the cache and in the Zero server's time ranges. The
adapter's live test found it.

**Where.** `src/ivm/columns.rs` (`class_of`), `src/model/value.rs`.

## 14. The Diesel adapter

**Decision.**
- **The API.** `streamgres-diesel` gives every Diesel query
  `.load_cached(&cache, &mut conn)`.
  - It takes the connection so a refusal can fall back to `.load(conn)`
    on its own. The connection is also Diesel's metadata lookup for
    custom types' OIDs.
  - `try_load_cached` returns `None` on a refusal instead.
  - Writes and transactions stay on Diesel, unchanged.
- **The read path.**
  - The query is rendered and its binds collected exactly as Diesel
    sends them.
  - Cached values are written in Postgres's binary format for their
    column's type (`sync::pg::binary::encode`) and read through Diesel's
    own `FromSql`, via a `Row` of the adapter's own.
  - A value that cannot be written exactly falls back to Postgres. A
    deserialization error is returned, not hidden.
- **Diesel versions.** It builds on Diesel 2.2 and 2.3.
  `juspay_diesel` 2.3.1 needs `diesel_derives` 2.2, and Cargo allows only
  one 2.x version of a crate per build. As an optional dependency it
  would have pinned every application to Diesel 2.2. So the same source
  builds as `streamgres-juspay-diesel`, with the fork renamed to `diesel`
  in its manifest, outside the workspace with its own lockfile.

**Rejected.**
- *A connection wrapper* (the s2s branch's `SyncedConnection`). It caches
  every read implicitly. The extension trait is explicit, opt-in per
  query, and easier to roll out one query at a time.
- *`.load_cached(&cache)` without a connection* (the PRD's sketch). A
  refusal would then leave the caller to fall back by hand.

**Where.** `crates/streamgres-diesel`, `crates/streamgres-juspay-diesel`.
Live test: `crates/streamgres-diesel/tests/load_cached.rs`, covering
every query shape and column type, before and after writes.

## Known gaps

- **Joins.** The parser refuses them. The s2s branch's join tree is the
  model to follow, and the join planner still lives in the server.
- **Positions.** The engine's position type is still `Lsn`. A
  backend-defined position is a separate change (decision 3).
- **Domains and schema changes.** A domain-typed column can stop the
  server when a schema change touches its table. Main has the same gap.
- **Arrays added by a schema change.** An array column added this way has
  no element type known (`element_oid` 0), so the cache refuses to answer
  for it until the next restart.
- **Zero output.** `money` and `tsvector` are shown to Zero clients as
  `null`.
- **A flaky live test.** `an_idle_feed_brings_the_first_snapshot_into_use`
  fails about one run in six. It does so on main too.
- **Test leftovers.** `schema_changes_follow_the_trigger` leaves its
  trigger stack installed when it fails, which breaks later runs on the
  same database. Main has the same problem.
