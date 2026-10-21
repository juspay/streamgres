# diesel_streamgres: design notes

Status: both models below are implemented end to end and the client
picks per query. B (the engine is the source) is the default: when every
projected column has a type the client can encode exactly, the result
comes from the engine's rows and every change replaces the entry in
place with no Postgres read. A (the engine invalidates, Postgres
refills) is the fallback for a query that projects a column the engine
cannot reproduce exactly: a `numeric` (held as a float), an `interval`,
ranges, composites. `EngineClient::fallbacks()` says why a query fell back.

## Where the wrapper sits

Diesel funnels every operation into three methods on the connection type:

| application call                         | connection method                     |
|------------------------------------------|---------------------------------------|
| `load`, `first`, `get_result(s)`, `count`| `LoadConnection::load`                |
| `execute`, writes without `RETURNING`    | `Connection::execute_returning_count` |
| `sql_query` with several statements      | `SimpleConnection::batch_execute`     |

`SyncedConnection<C>` owns all three and delegates to the inner
`PgConnection`. Transactions reuse Diesel's ANSI manager over the inner
connection's state, so there is one transaction depth, not two.

An application that names its connection type through one alias adopts
the wrapper by changing that alias and nothing else. Pools built on
`bb8` through `async-bb8-diesel` run every Diesel call on a blocking
thread, which is where a cache miss can safely wait on the engine.

## What a read looks like at the interception point

At `load` the wrapper holds the typed Diesel AST. From it, with no network:

- the SQL text, via `to_sql` into a `PgQueryBuilder` (placeholders `$1..`);
- the bind values as Postgres binary bytes plus type OIDs, via
  `collect_binds` with the inner connection as the type lookup;
- whether the statement's SQL is static (`is_safe_to_cache_prepared`), which
  is false for `IN (...)` lists and `sql_query`.

SQL plus bind bytes is the cache key. Diesel's own statement-cache key
(`TypeId` of the query shape) ignores argument values, so it cannot be ours.

## Two ways to keep the cache fresh

### A. Engine as invalidator, Postgres as source (proposed first)

- Miss: run the query on Postgres through the inner connection, capture
  each row's wire bytes (as Deja does), store them under the key, and
  subscribe the query with the engine.
- Change: the engine's poke says which subscriptions a transaction
  touched. The wrapper drops those entries. The next read refills from
  Postgres, or a background task refills eagerly for hot keys.
- Rows served from cache are byte-identical to what Postgres sent, so
  `FromSql` needs nothing new and every column type works on day one.
- Cost: one Postgres read per change per affected query, which for config
  tables is close to none.

This is the usual in-process cache with its pub/sub invalidation
replaced by the WAL: no publish call to remember, writes from outside the
app invalidate too, no TTL staleness, no per-pod refill storm because the
engine says exactly which keys moved.

### B. Engine as source (later)

- Miss: subscribe, take the engine's first snapshot, serve it.
- Change: apply the engine's add/delete rows to the cache directly; no
  Postgres read at all.
- Needs an encoder from the engine's JSON rows to Postgres binary for every
  column type the application uses, and the engine's parser extended from
  its v1 subset (`SELECT *`, bare identifiers, literals) to Diesel's output
  (quoted and qualified identifiers, column lists, `$n` binds).

A ships in weeks and is correct for all types. B removes the read on every
change and is the end state; A's cache, keys, subscription bookkeeping and
reconnect logic carry over unchanged.

## Rows mode (B), as implemented

- Every subscribe asks for rows. `accepted` carries the hydrated result
  with the table, its key columns, the projected columns and their output
  names, the order and limit, and the rows as JSON objects of every
  column. Each later transaction sends `delta` with the rows that entered
  or changed and the keys of rows that left.
- The client learns a table's exact column types once from
  `pg_attribute` through the connection the read came in on
  (`types.rs`), encodes each cell to Postgres's binary format
  (`encode.rs`: integers by width, floats, text-like, UUID, JSON and
  JSONB, dates, timestamps at Postgres's own microsecond precision,
  `bytea`, user enums, one-dimensional arrays of those), and keeps the
  result by primary key (`materialized.rs`), ordering it as the query
  orders (Postgres defaults: ASC NULLs last, DESC NULLs first; strings
  bytewise) and cutting it to the limit on each rebuild.
- `COUNT(*)` is one `int8` row computed from the kept rows.
- A reconnect resubscribes and the engine's snapshot refills the entry
  with no Postgres read; the types are already known.
- Joins, composed in the library as agreed: the engine parses Diesel's
  `FROM ("a" INNER JOIN ("b" ...) ON (...))` into its tree (INNER and
  LEFT edges on one column each side; `ON` conditions and `WHERE`
  conjuncts to the one table they name; `ORDER BY` and `LIMIT` on the
  root), plans it like a Zero query, and ships the hydrated rows per
  table with each node's key and join columns, then per-table puts and
  deletes. The client keeps rows per table by key and composes the
  result on each rebuild: root rows in query order, a hash join down
  each edge, inner edges dropping unmatched parents and left edges
  null-extending, then the limit over joined rows, projection, count.
  Refused by name: a table joined twice, RIGHT/FULL/CROSS, OR across
  tables, WHERE on a LEFT-joined table, ORDER BY on a joined table
  other than its primary key ascending after the root's columns (which
  the composition yields anyway, by value: 9 before 10), compound keys,
  `LIKE`, `DISTINCT`.
- `OFFSET`: the engine keeps `limit + offset` rows and the client skips
  `offset` of them on each rebuild. `bytea`: the engine carries it as
  Postgres's hex text and the client turns it back into bytes. A bind of
  a user enum (or an array of one) is read by the engine as the label
  text; the catalog tells it which OIDs are enums.
- A query the engine refuses is remembered for
  `STREAMGRES_REFUSED_TTL_MS` (default 30 s, 0 never), so its reads go
  straight to Postgres instead of asking the engine every time. A
  refusal from the client's own side (not connected, over the
  subscription cap, no answer in time) is not remembered, and every
  remembered refusal is forgotten when the engine connection drops: the
  engine that comes back may be a restarted one whose catalog knows a
  type created after the old one started.
- Times: the engine keeps every `timestamp`, `timestamptz` and `date`
  as microseconds since the epoch, Postgres's own precision, and ships
  them so to S2S clients; the Zero protocol keeps seeing milliseconds
  (scaled on its way in and out). So a `timestamp(6)` column, Postgres's
  default, is served exactly in rows mode.
- Known gaps: a column under a linguistic collation may order
  differently from Postgres; `numeric` (held as a float), `interval`,
  ranges and composites fall back to A.

## Contract with the engine (implemented)

WebSocket at `{base}/s2s/v1`, JSON `[tag, body]` frames.

Upstream:
- `["subscribe", {"id", "sql", "binds": [{"oid", "bytes": base64|null}]}]`:
  the SQL as Diesel rendered it (`$n` placeholders, quoted and qualified
  names, a column list or `COUNT(*)`), each bind as its Postgres type OID
  and binary bytes. The engine parses it against its catalog, decoding
  binds by OID (integers, floats, numerics, booleans, text-like, UUID,
  JSON, timestamps, dates).
- `["unsubscribe", {"id"}]` when the cache evicts the key.
- `["ping"]`.

Downstream:
- `["hello", {"protocol": 1}]`, `["pong"]`.
- `["accepted", {"id", "table", "key", "columns", "names", "count", "order", "limit", "rows"}]`
  once the engine's rows for the query are hydrated; in rows mode the
  body is the result itself, otherwise the wrapper reads Postgres only
  after it, so every later change is reported.
- `["delta", {"id", "put": [rows], "del": [keys]}]` per transaction for
  a rows-mode subscription. One entry per key: a row that changed is a
  put of its new image, a row that came and went inside the transaction
  is a delete, and no key is in both lists, so the order the client
  applies them in does not matter.
- `["refused", {"id", "reason"}]` for anything outside the subset (joins,
  `LIKE`, `OFFSET`, `DISTINCT`, unknown columns, too many rows); the read
  is served live. A refusal after acceptance means the engine stopped
  following the query; the client drops the entry.
- `["touched", {"ids"}]` per transaction that changed any of those
  subscriptions' rows.

The engine accepts first and the client reads second, so a change that
lands between the two is reported redundantly and costs one refill,
never a stale row.

Reconnect: a dropped socket refuses every subscribe waiting on it,
drops the cache entries of the keys it carried (they are served live
until it is back), and on reconnect resubscribes every held key under
its old id; a rows-mode key refills from the snapshot with no Postgres
read. A position cookie for replay is a later refinement. Measured on
a 32-core box (2026-10-14): one hour of 15.9 M reads over nine shapes
with a write every 200 ms and the engine restarted every four minutes
(seven graceful, seven `kill -9`); 3.18 M reads compared row for row
against Postgres, none stale, no errors; every restart had all 285
subscriptions back by the time the engine was healthy (5.5 s), the
first read after it a hit; both processes' memory grew only with the
data (6,000 comments inserted).

## The connection pool

A backend subscribes to whatever its code reads; nothing like a screen
bounds the set. So the client holds a pool of sockets to the engine and
a cap on the subscriptions it will carry, both from the environment:

| variable                            | meaning                                   | default |
|-------------------------------------|-------------------------------------------|---------|
| `STREAMGRES_S2S_CONNECTIONS`        | sockets to the engine, each on its thread | 2       |
| `STREAMGRES_S2S_MAX_SUBSCRIPTIONS`  | subscriptions held across all sockets     | 10000   |
| `STREAMGRES_S2S_VERDICT_TIMEOUT_MS` | wait for `accepted` or `refused`          | 5000    |

`EngineClient::connect` reads them (`Config::from_env`);
`EngineClient::with_config` takes them explicitly. Each socket is a
worker with its own thread, runtime, subscription ids, waiting
subscribes and rows-mode stores; the pool keeps one map from key to
worker. A miss goes to the worker already holding the key, else to the
connected worker with the fewest subscriptions. Past the cap the engine
is not asked and the read is served live, exactly as a refused query
is, and `Stats::refused` counts it; a key freed by eviction or an
engine refusal gives its slot back. One socket dropping affects only
its own keys.

## What an application's queries do through it

The column types of a table are looked up on the first rows-mode
subscribe of it and kept; a snapshot naming a column they lack (one a
migration added) has them looked up again, and on the async wrapper
that read goes live once while the catalog is reloaded.

`tests/shape_coverage.rs` drives the shapes a model layer produces
through the wrapper and a passthrough connection side by side: a primary
key, a two-column key, `AND` and `OR` predicates, an enum bind, a list,
`ORDER BY ... LIMIT`, `LIMIT OFFSET`, a `bytea`, a timestamp bind and a
count. Every answer equals Postgres's before and after a change, every
shape is served from the engine's rows, the repeat is a cache hit, and a
change reaches the cached answer within milliseconds. The first run of
this kind, against a production schema, turned up three engine changes:
`bytea` columns were unknown to the engine, enum binds were not decoded,
and `OFFSET` was refused.

## Running it locally

- Postgres 15 or newer with `wal_level = logical` (the engine's DDL
  trigger stack needs 15+). A private Homebrew `postgresql@16` on its own
  port works: `initdb`, set `wal_level = logical` and a short
  `unix_socket_directories`, `pg_ctl start`.
- In the database: `CREATE PUBLICATION xyne_sync_pub FOR ALL TABLES;`
  and `psql -f scripts/sql/ddl-triggers.sql` from the engine repo.
- The engine: `STREAMGRES_APP_ID=xyne STREAMGRES_PG_DSN=... STREAMGRES_QUERY_URL=http://127.0.0.1:1/q STREAMGRES_MUTATE_URL=http://127.0.0.1:1/m cargo run --release --bin server`.
  The query and mutate URLs are required by the config and never called
  on the S2S path.
- For tokio-console: build the server with
  `RUSTFLAGS="--cfg tokio_unstable" cargo build --release --bin server --features console`,
  run it, then `tokio-console` (installed with `cargo install tokio-console`)
  attaches on `127.0.0.1:6669`; the S2S task is named `s2s`.

## diesel-async

`AsyncSyncedConnection` (cargo feature `async`) is the same wrapper over
diesel-async's `AsyncPgConnection`: it implements `AsyncConnection` and
`PoolableConnection`, so an application on diesel-async with bb8 or
deadpool swaps its connection type and nothing else changes. Routing,
the cache, the read-your-writes window, the settings and the engine
side are shared with the blocking wrapper.

What differs is how a read reaches them. `AsyncConnection::load` must
return a future that neither borrows the connection nor carries the
query, so, as diesel-async itself does, everything that needs either
happens synchronously inside `load`: the key is rendered, the route
decided, the cache checked, and the futures the read may need are built
up front: the inner connection's own load (polled only when the read
goes to Postgres), and a lookup of every table's column types
(`types::ALL_TABLES_SQL`, polled only the first time the engine ships
rows for a table whose types are not known). A subscribe blocks on the
engine, so it runs on a blocking thread. That split is also why
`Subscriptions::subscribe` is two steps: it answers `NeedsTypes` with
the tables it does not know, and `materialize` finishes the job once
the caller, blocking or async, has looked them up.

A bind of a user-defined type (an enum) needs its OID for the key. The
blocking wrapper resolves it on the spot through the connection; the
async one cannot, so the first read with such a bind goes to Postgres
while the OID is resolved and remembered in the cache, for every
connection sharing it, and from the next read on the query is cached
like any other. An array of an enum (`eq_any`) cannot even be rendered
until the OID is known, and takes the same path. A table created after
the one-shot type load falls its query back to refills, and the next
miss loads the types again.

Two things the async world adds. diesel-async lets several futures
from one connection be pending at once, and shares its transaction
state with each of them; asking for that state while one is pending is
a panic in diesel-async itself. The wrapper counts its own futures in
flight and, while any is, sends a read to Postgres without asking: a
read pipelined behind a write must see that write anyway. And a future
can be dropped before it completes: a `with_route` scope restores the
outer route on drop, and a read dropped after it subscribed ends the
subscription it made rather than leaving one nobody fills.

## Cache limits

`cache::Limits` bounds the cache three ways, each from the environment
through `Limits::from_env` or set in code with `Cache::with_limits`:

| variable                           | meaning                                      | default |
|------------------------------------|----------------------------------------------|---------|
| `STREAMGRES_CACHE_MAX_BYTES`       | rows and keys held                           | 256 MiB |
| `STREAMGRES_CACHE_MAX_ENTRY_BYTES` | the largest result kept; 0 no limit          | 16 MiB  |
| `STREAMGRES_CACHE_IDLE_MS`         | drop an entry unread for this long; 0 never  | 30 min  |
| `STREAMGRES_CACHE_TTL_MS`          | drop an entry this old however read; 0 never | off     |
| `STREAMGRES_REFUSED_TTL_MS`        | skip the engine for a refused query this long; 0 never | 30 s |

A rows-mode entry weighs its rows plus the store the engine side keeps
for it, so the budget covers both. A result over the entry limit is
served but not kept (`Stats::oversized`), and its subscription ends. A
refusal is remembered by the query's shape, its SQL and bind types, not
its bind values: what the engine refuses, it refuses for every value.

Idle time makes the cache follow what the backend reads now: a query
nobody asks for anymore loses its entry and its subscription. Time to
live is a safety net: the entry is dropped on a timer however often it
is read, and the next read resubscribes and refills from the engine. It
is off by default because the engine keeps the entry fresh; an engine
delta that replaces an entry restarts its clock. Both clocks, like the
byte budget, end the subscription and bump the key's generation, so a
fill in flight for an entry that just expired is dropped rather than
kept without anything to update it. A generation exists only while a
read holds a reservation on its key (`Cache::reserve` returns one, and
`fill` consumes it); a touch on a key nobody is filling has nothing to
invalidate but the entry, so the map holds the keys in flight, not
every key ever seen.

The same rule binds the engine side's own writes. A delta or a
snapshot reserves the key before it builds the rows, `replace` bumps
the generation (so a Postgres fill in flight, which holds older rows,
loses) and checks it again after the insert; and every path that ends
a subscription bumps the generation as it drops the key, under the
worker's lock. So rows built for a key that was unsubscribed in the
meantime, by a dropped read, a failed read, an oversized result or an
eviction, are dropped again rather than kept with nothing to update
them. The wrapper never unsubscribes directly: `Cache::abandon` does,
in that order. Nothing holds a lock across a moka insert (the eviction
listener runs on the inserting thread), so a fill checks the
generation before and after instead, and a newer image that landed in
between costs one refill, never a stale row.

Between the engine's `accepted` and the store being built there is a
window too: the reader thread puts an empty `Building` store in place
under the same lock in which it ends the wait, so a delta right behind
the frame queues there; `materialize` drains the queue and turns that
one store `Ready` in place, so a delta handler holding the store finds
the live one. A key resubscribed after a reconnect is pending again
until the engine accepts it, and a read of it goes live meanwhile; a
subscribe cut short by the drop is forgotten, and an acceptance for an
id nobody holds is answered with an unsubscribe.

Three reads never cache whatever the flags say: `FOR UPDATE` and its
variants (the lock is the point, `Stats::locking`), a `WITH` whose
expressions modify data, and a read whose Postgres read fails after
the engine accepted it, which ends the subscription with the error.

## Routing: two flags, per query and per connection pool

A read goes one of two ways, `Route::Streamgres` (the cache, kept fresh
by the engine) or `Route::Postgres` (live). The route is decided at
three levels, the most specific winning:

1. The query: `query.via(Route::Postgres).first(conn)` (or
   `via_postgres()` / `via_streamgres()`, from `RouteDsl`). The marker
   renders a leading SQL comment, `/*streamgres:postgres*/`, that the
   wrapper reads and strips at the interception point; the cache key and
   the SQL the engine sees are the plain query, so flagged and unflagged
   reads share one entry. `conn.with_route(route, |c| ...)` is the same
   flag for every read inside the closure, for code that holds the
   connection anyway (a pool's `interact` block).
2. The connection pool: every connection carries a route chosen by the
   URL it was established with (`Settings`), so a master pool goes to
   Postgres and a replica pool through Streamgres, the usual split.
   `set_route` changes it on one connection.
   `last_served()` says where the connection's last read came from
   (`Served::Cache`, `Engine` or `Postgres`), per connection, so a
   request handler can report it exactly where the cache's shared
   counters could not.
   The same lookup picks the cache: one engine streams one database
   and keeps one cache fresh, so pools against one database share a
   cache (`Settings::single`) and pools against different databases
   each get their own, attached to the engine streaming that database
   (`Settings::by_url`). A URL with no cache is a passthrough.
3. The process default, Streamgres.

Whatever the flags say, three reads go to Postgres: a statement that is
not a `SELECT` (Diesel sends `INSERT ... RETURNING` through `load`), a
read inside an open transaction (the ANSI manager's depth is on the
inner connection and is checked before the cache), and a query the
engine refuses. A query flagged Streamgres on a pool whose database no
engine streams is therefore just a live read.

`Consistency::Eventual` and `Consistency::Strong` remain as the earlier
names of the two routes.

The two routes can disagree for a moment, by design: between a commit
and the engine's delta, a Postgres read sees the new row and a
Streamgres read of the same query still sees the old one. A Postgres
read is a bypass, not a write-through: it does not refresh the entry.
Where that bites is one flow that writes and then reads, usually on
two connections of the same pool. So after any write through a cache
(`execute`, raw SQL, `INSERT ... RETURNING`, a transaction's COMMIT;
not a plain SELECT, BEGIN, ROLLBACK, SAVEPOINT or SET) every read
through that cache
goes to Postgres for `STREAMGRES_READ_YOUR_WRITES_MS` (default 1000; 0
turns it off; `set_read_your_writes` per connection sets the window,
the clock is the cache's), counted in `Stats::after_write`. A read that must never be behind the database is
flagged Postgres; everything else accepts being the engine's
propagation delay behind, a fraction of a millisecond locally and the
WAL lag in production.

## Open points carried from the roadmap call

- id construct: subscription id = hash of SQL plus binds; client id per
  process; group id per application. Proposed, not agreed.
- memory limit: bytes, idle time and time to live from the environment
  (see "Cache limits"), unsubscribe on eviction or expiry.
- engine connections: a pool with a per-process cap, both from the
  environment (decided; see "The connection pool").
- joins: the engine ships rows per table; for A this is invisible (the
  rows come from Postgres), for B the merge is the hard part.
