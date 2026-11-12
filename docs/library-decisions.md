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
