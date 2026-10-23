# diesel_streamgres: design

`diesel_streamgres` is a Diesel connection that serves an application's
reads from a local cache the Streamgres engine keeps fresh, and sends
everything else to Postgres. The application swaps its connection type
and changes nothing else.

```text
  application code (Diesel DSL, sql_query, transactions, pools)
                         |
                         v
  SyncedConnection<PgConnection>  or  AsyncSyncedConnection
     |  load / execute / batch_execute
     |
     +-- writes, transactions, non-SELECT, flagged Postgres --> Postgres
     |
     +-- cacheable read
            |
            v
         Cache (moka, keyed by SQL + bind bytes) <---- delta / touched
            | miss                                          ^
            v                                               |
         EngineClient (pool of WebSockets) --- subscribe ---> Streamgres engine
            |  accepted (rows) / delta / touched / refused    |
            v                                               | logical
         Materialized store per query (rows mode)           | replication
                                                            v
                                                         Postgres
```

## Modules

| module | role |
|---|---|
| `lib.rs` | `SyncedConnection`, `Settings`, `install`, the interception of Diesel's three connection methods |
| `asynchronous.rs` | `AsyncSyncedConnection`, the same wrapper over diesel-async (feature `async`) |
| `route.rs` | `Route`, `RouteDsl`, `Via`, `Served`: where a read goes and where it came from |
| `key.rs` | `QueryKey`: what identifies a read, and a refusal |
| `cache.rs` | `Cache`, `Limits`, `Stats`, the `Live` trait, reservations and generations |
| `subscriptions.rs` | the `Subscriptions` trait between the cache and the engine side |
| `engine.rs` | `EngineClient`: the pool of sockets to the engine, `Config` |
| `materialized.rs` | a query's result kept from the engine's rows; joins composed here |
| `types.rs` | exact column types of a table, from `pg_attribute` |
| `encode.rs` | engine JSON cells to Postgres binary, per column type |
| `row.rs` | `OwnedRow`: a row that outlives its statement and decodes like a live one |
| `cursor.rs` | one cursor type for rows from the cache and rows from Postgres |

## The interception surface

Diesel funnels every operation into three methods on the connection type:

| application call                         | connection method                     |
|------------------------------------------|---------------------------------------|
| `load`, `first`, `get_result(s)`, `count`| `LoadConnection::load`                |
| `execute`, writes without `RETURNING`    | `Connection::execute_returning_count` |
| `sql_query` with several statements      | `SimpleConnection::batch_execute`     |

`SyncedConnection<C>` implements all three and delegates to the inner
connection `C`, normally `PgConnection`. Transactions use Diesel's ANSI
manager over the inner connection's state, so there is one transaction
depth. Writes always go to Postgres. Pools (`r2d2`, and `bb8` or
`deadpool` through diesel-async) and `#[derive(Queryable)]` work
unchanged, because the wrapper is just another connection type.

Diesel seals `Connection` behind the cargo feature
`i-implement-a-third-party-backend-and-opt-into-breaking-changes`; the
crate enables it.

## A read

At `load` the wrapper holds the typed Diesel AST. From it, with no
network, it renders the SQL with `$n` placeholders (`to_sql` into a
`PgQueryBuilder`), collects every bind as its Postgres type OID and
binary bytes (`collect_binds`), and reads the route flag if the query
carries one. Then:

1. **Route.** The query's flag, else a `with_route` scope, else the
   connection's route. `Route::Postgres` reads live.
2. **Reads that are never cached** go live: a statement that is not a
   `SELECT` (Diesel sends `INSERT ... RETURNING` through `load`), a read
   inside an open transaction, a read inside the read-your-writes window,
   `FOR UPDATE` and its variants, a `WITH` whose expressions modify data.
3. **Behind.** While the engine says it is further behind the database
   than `STREAMGRES_S2S_MAX_LAG_MS`, or has said nothing for that long,
   reads go live and `lagging` counts them. A miss whose engine rows
   arrive with a lag past it, as on a socket that just reconnected to
   a lagging engine, keeps them for later reads and serves this one
   live.
4. **Refused recently.** A query the engine refused, with the same
   values, within `STREAMGRES_REFUSED_TTL_MS` goes live without asking
   again.
5. **Hit.** The entry under the key is served; in rows mode the result
   is built from the engine's rows if a change came since it last was.
6. **Miss.** The key is reserved and the query subscribed. Rows mode
   builds the entry from the engine's rows; refill mode reads Postgres
   and keeps what it returned; a refusal goes live.

`SyncedConnection::last_served()` reports where the connection's last
read came from: `Served::Cache`, `Served::Engine` or `Served::Postgres`.

## The cache key

A key is the rendered SQL plus every bind as Postgres receives it, OID
and bytes. Two reads with equal keys get equal rows. (Diesel's own
statement-cache key is the `TypeId` of the query shape, which ignores
argument values.)

A refusal is remembered under the same key, values and all: the engine
refuses on values too (a time at infinity, more rows than it follows for
one user), so a refusal for one value says nothing of another.

## Two ways an entry is kept fresh

Every subscribe asks the engine for rows. Which mode a query ends up in
is decided by its projected columns:

| | rows mode | refill mode |
|---|---|---|
| when | every projected column has a type the client encodes exactly | a projected column cannot be reproduced exactly from the engine's values |
| miss | built from the engine's `accepted` rows; Postgres is not read | Postgres is read once; rows kept byte for byte |
| change | the engine's `delta` updates the store in place; the next read builds the result | the engine's `touched` marks the entry stale; the next read refills |
| reconnect | rebuilt from the engine's snapshot | refilled on next read |

`EngineClient::fallbacks()` lists the queries in refill mode and why;
`EngineClient::refusals()` lists refused queries and why.

### Rows mode

The engine's `accepted` carries the result per table of the query, root
first: the table, its primary-key columns, its join edge to its parent,
and every column of every row as JSON. The body also carries the
projected columns and their output names, a `COUNT(*)` flag, the root's
order, the limit and the offset. Each later transaction that changes the
result sends a `delta`: per table, the rows that entered or changed and
the keys of rows that left.

**Types.** The engine has coarse types (string, integer, float, time);
Diesel decodes Postgres's binary format, where an `int4` is four bytes
and an `int8` eight. So the client looks up each table's exact column
types from `pg_attribute`, once per table, through the connection the
read came in on, and encodes each cell for its type. A cached row then
decodes through the same `FromSql` as a live one. A snapshot that names
a column the known types lack has the table's types looked up again.

| encoded exactly (rows mode) | not encodable (refill mode) |
|---|---|
| `bool`, `int2`, `int4`, `int8`, `oid`, `float4`, `float8` | `numeric` (the engine holds a float) |
| `text`, `varchar`, `bpchar`, `name`, `char` | `interval` |
| `uuid`, `json`, `jsonb`, `bytea` | ranges, composites |
| `date`, `timestamp`, `timestamptz` at any precision | any other type |
| user enums | arrays of an unencodable element type |
| one-dimensional arrays of the above | |

The check runs once per query over every projected column, so an
unencodable column puts the whole query in refill mode up front.

Values on the wire: times are microseconds since the Unix epoch,
Postgres's own precision; `bytea` is Postgres's hex text; an enum is its
label; a float's `NaN` and infinities are the strings `"NaN"`,
`"Infinity"` and `"-Infinity"`.

**The materialized store** keeps the rows by primary key per table. The
cache entry holds the store itself (`cache::Live`): a delta moves rows
in and out of it on the socket's thread, and the next read builds the
result from it, on the reading thread, and keeps it until the next
change. A burst of deltas costs one build, and none if nobody reads in
between. The build:

1. The root table's rows in query order. `ASC` puts NULLs last, `DESC`
   first, as Postgres does by default. Strings compare bytewise; the
   engine only accepts an `ORDER BY` on a column Postgres orders the
   same way (see below). Rows that tie, and child rows under one parent,
   come out in primary-key order by value (9 before 10).
2. A hash join down each edge, the joined tables depth first. An inner
   edge drops a parent with no match; a left edge extends it with NULLs.
3. `OFFSET` rows skipped, then `LIMIT` applied over the joined rows. The
   engine keeps `limit + offset` root rows so there is enough to skip.
4. Projection to the selected columns and names, each cell encoded. A
   `COUNT(*)` counts every joined row (the engine keeps every row for
   it), and its `LIMIT` and `OFFSET` cut the one `int8` row it is, as
   Postgres applies them after counting.

A value that cannot be encoded after all (a delta brings one) falls the
query back to refill mode at the read that meets it.

### Refill mode

On a miss the client subscribes first, then reads Postgres through the
inner connection and keeps each row's OID and raw bytes (`OwnedRow`). A
cached row is byte-identical to the one Postgres sent, so any column
type works. On `touched` the entry is marked stale and the next read
refills. The cost is one Postgres read per change per affected query.

## What the engine follows

The engine parses the SQL as Diesel renders it: quoted and qualified
identifiers, a column list, `*` or `COUNT(*)`, `$n` placeholders.

| accepted | refused, with a reason |
|---|---|
| `=`, `<>`/`!=`, `<`, `<=`, `>`, `>=`, `[NOT] IN`, `IS [NOT] NULL`, `AND`, `OR`, parentheses | `DISTINCT`, `GROUP BY`, aggregates other than `COUNT(*)` |
| `= ANY($n)` with an array bind (`eq_any`), read as `IN`; `!= ALL($n)` (`ne_all`), read as `NOT IN`, an empty array being no condition | `LIKE`, `ILIKE`, array operators such as `@>` |
| `ORDER BY` on the root table by a column Postgres orders as the engine does: numbers, booleans, times, `uuid`, `bytea`, `name`, `time`, and strings under the `C` collation; `LIMIT`, `OFFSET` | an `ORDER BY` on an enum, on a string under any other collation, on JSON; table aliases, unknown tables or columns |
| `INNER` and `LEFT [OUTER]` joins on a one-column equality each side; an `INNER JOIN` inside a `LEFT JOIN`'s parentheses | a table joined twice; `RIGHT`, `FULL`, `CROSS`; compound join keys; an `INNER JOIN` onto a table under a `LEFT JOIN`, written after it |
| `ON` conditions and `WHERE` conjuncts that name one table | an `OR` across tables; a `WHERE` on any table under a `LEFT JOIN` |
| joined tables ordered by their primary keys ascending, depth first, after root columns that cover the root's whole key | any other `ORDER BY` on a joined table; an unqualified column in a join |
| binds of integers, floats, numerics, booleans, text-like types, UUIDs, JSON, timestamps, dates, enums (one created after the engine started is learnt on its first bind) | any other bind type; a time or date at infinity; a result over the engine's row limit |
| filters that expand to at most 1,024 alternatives, with at most 10,000 `IN` and `NOT IN` values in all (`STREAMGRES_S2S_MAX_DISJUNCTS`, `STREAMGRES_S2S_MAX_LIST_VALUES` on the engine) | a larger query: an `AND` of `OR`s multiplies its alternatives, and the engine expands them on the thread every subscriber's changes go through |

In a join, Diesel's `FROM ("a" INNER JOIN ("b" ...) ON (...))` becomes a
tree: each `JOIN` is an edge from the table on its left (the parent) to
the item on its right (the child). Each top-level `WHERE` conjunct and
each extra `ON` condition becomes the filter of the one table it names.
`ORDER BY` and `LIMIT` belong to the root.

## The protocol

WebSocket at `{base}/s2s/v1`; JSON text frames, each `[tag, body]`.

Client to engine:

| frame | meaning |
|---|---|
| `["subscribe", {"id", "sql", "binds": [{"oid", "bytes": base64 or null}], "rows": true}]` | follow this query; `id` is the client's own, per socket |
| `["unsubscribe", {"id"}]` | the cache dropped the key |
| `["ping"]` | keepalive; the client sends one while the socket is quiet |

Engine to client:

| frame | meaning |
|---|---|
| `["hello", {"protocol": 1, "max_subscriptions": n, "database": {"system", "name"}}]` | first frame; `max_subscriptions` is the per-connection limit, `null` for none; `database` is what the engine streams (the cluster's `system_identifier` and the database's name); an engine that cannot read it closes the socket instead of greeting, and an older engine sends none |
| `["accepted", {"id", "nodes", "columns", "names", "count", "order", "limit", "offset"}]` | the engine follows the query; the body is the hydrated result, `nodes` per table with `table`, `key`, `parent`, `parent_column`, `child_column`, `inner`, `rows` |
| `["delta", {"id", "changes": [{"table", "put": [row], "del": [key]}]}]` | one transaction's change to a rows-mode result; one entry per key, its last state, never in both lists, so apply order does not matter |
| `["touched", {"ids"}]` | one transaction changed these refill-mode subscriptions |
| `["refused", {"id", "reason"}]` | the engine cannot follow the query; after an `accepted`, it stopped following it |
| `["pong"]` | reply to `ping` |
| `["lag", {"ms"}]` | right after `hello`, then every 500 ms: how far at most the engine's applied changes trail the database, the age of the newest heartbeat it applied; `null` before the first heartbeat is sent |
| `["error", {"reason"}]` | a frame that could not be read and names no id; a `subscribe` whose id reads is answered `refused` instead |

An id is never reused on a socket: frames queued for an unsubscribed id
may still arrive after the `unsubscribe`.

On the engine, one task owns every S2S subscription. It consumes the
same engine events as the Zero client groups, with its own index of
subscriptions, and maps the engine's ids back to the socket and the
client's id. A row a transaction changed is serialized once and spliced
into every `delta` that carries it. Each socket has a request queue of
1,024 frames and an outgoing queue of 4,096 frames and 64 MiB; a client
that stops reading is closed when its outgoing queue fills rather than
buffered for, and its socket is cut off 5 seconds later if its writer
still cannot flush.

## Correctness rules

**Subscribe before read.** The engine accepts first and the client reads
second. A change after the read is reported; a change between the two
is reported redundantly, which costs one refill and never a stale row.

**Generations.** `Cache::reserve` takes the key's generation before a
fill; `Cache::fill` stores the rows only if nothing bumped it since. A
touch, a delta and every path that ends a subscription bump it. The map
holds generations only for keys with a reservation in flight.

The engine side follows the same rule. A snapshot for a resubscribe
reserves the key before building its store; `Cache::live` bumps the
generation, so a slower Postgres fill with older rows loses, and checks
it again after the insert. A delta only changes the store the entry
already holds, and stores it again only when its weight drifted. Every path that ends a subscription (dropped read, failed read,
oversized result, eviction, expiry) bumps the generation as it drops the
key, under the worker's lock, so rows built for a key unsubscribed
meanwhile are dropped. The wrapper never unsubscribes directly;
`Cache::abandon` does it in that order. No lock is held across a moka
insert, because the eviction listener runs on the inserting thread; a
fill checks the generation before and after instead.

**Stale, not gone.** A touch, a lost fill and a dropped socket leave the
key marked stale rather than removed: the mark weighs only the key and
keeps the entry's clocks, so the subscription of a query nobody reads
again still ends when its idle time runs out.

**The `Building` store.** When `accepted` arrives, the reader thread
puts an empty `Building` store in place under the lock that ends the
subscriber's wait, so a delta right behind it queues there.
`materialize` drains the queue and turns that store `Ready` in place.

**Late answers.** A subscribe the client gave up on is withdrawn. An
`accepted` for an id nobody holds is answered with `unsubscribe`.

## The engine client

`EngineClient` holds a pool of sockets and a cap on subscriptions, from
the environment (`Config::from_env`) or explicitly
(`EngineClient::with_config`):

| variable                            | meaning                                   | default |
|-------------------------------------|-------------------------------------------|---------|
| `STREAMGRES_S2S_CONNECTIONS`        | sockets to the engine, each on its thread | 2       |
| `STREAMGRES_S2S_MAX_SUBSCRIPTIONS`  | subscriptions held across all sockets     | 10000   |
| `STREAMGRES_S2S_VERDICT_TIMEOUT_MS` | wait for `accepted` or `refused`          | 5000    |
| `STREAMGRES_S2S_SILENCE_MS`         | a socket silent this long is taken for dead | 10000 |
| `STREAMGRES_S2S_MAX_LAG_MS`         | the engine's lag past which reads go live; 0 trusts the cache | 2000 |
| `STREAMGRES_S2S_TOKEN`              | bearer token sent when a socket opens     | none    |

The token goes out as `Authorization: Bearer <token>` on each socket's
upgrade, for an engine started with the same `STREAMGRES_S2S_TOKEN`. An
engine that wants a token and is not given the right one answers 401.
The worker then retries like any failed connect, so every read goes to
Postgres until the two agree. The token never appears in `Config`'s
`Debug` output. Over `ws://` the token crosses the network in plain
text; `wss://` connects with TLS (rustls, the system's root
certificates), and `EngineClient::warnings` names a token sent over
`ws://` to another machine, for the application to log.

**Connect failures.** A worker keeps why its latest connect failed: a
401 (the token), a 503 (the engine not serving yet, or out of
connections), another status, a network error, or a URL or token that
cannot go in a request. While the socket is down, each refusal it gives
says so (`the engine is not connected: ...`), and
`EngineClient::connect_errors` lists them. A successful connect clears
it.

**The engine's database.** The engine's `hello` names the database it
streams. The first miss through a cache reads the same of the database
its reads go to (`cache::DATABASE_SQL`, readable by any role), once per
cache, before it subscribes. A probe that fails is not recorded: that
read goes live without asking the engine, and the next miss probes
again. A socket whose engine
streams another database is not asked for rows, which would otherwise
be served as this database's: every subscribe through it is refused,
naming both, and the reads go to Postgres. A key resubscribed after a
reconnect is checked the same way when its `accepted` arrives. An engine
whose `hello` names no database, as engines from before the check do,
is taken on trust; this engine closes a socket it cannot name its
database on rather than greet it.

**The engine's lag.** A large transaction, a slow apply or a stalled
replication stream delay the cache without dropping the socket. The
engine writes a heartbeat into the WAL every 500 ms
(`pg_logical_emit_message`, not transactional, under the prefix
`<slot>/heartbeat`; `STREAMGRES_HEARTBEAT_MS`, 0 off) and notes when it
sent each. When its replication stream brings a heartbeat back and every
change before it is applied and queued to the sockets, that heartbeat is
applied, and so is every change committed before it was sent. Its lag is the age of the newest
applied heartbeat, a bound on how stale it can be: under one interval
while it keeps up, growing while the stream is stuck. Each socket is
told the lag right after `hello`, so before any answer, and every
500 ms. The wrapper adds the time since the last
`lag` frame, so an engine that stops sending them is taken as falling
behind too, and goes live while the largest lag over the connected
sockets is past `STREAMGRES_S2S_MAX_LAG_MS`. An engine that reports no
lag (`null`, or one that never sends the frame) is trusted, as before.

**The engine's limit.** The engine's `hello` names how many
subscriptions it takes on one connection (`max_subscriptions`). The pool
passes over a socket at that limit. With every socket full, a subscribe
is refused here, not by the engine, so the read goes live at once
instead of after a round trip. A subscribe sent before the `hello`
lands may still be refused by the engine; both refusals are transient.

Each socket is a worker with its own thread, runtime, ids, waiting
subscribes and rows-mode stores. The pool maps each key to its worker. A
miss goes to the worker already holding the key, else to the connected
worker with the fewest subscriptions. Past the cap the engine is not
asked and the read goes live; a key freed by eviction or refusal gives
its slot back.

A subscribe blocks the calling thread on a channel until the answer
arrives or the verdict timeout passes, so the wrapper needs no async
context.

| timer | value | on expiry |
|---|---|---|
| verdict timeout | 5 s, configurable | the read goes live; the id is withdrawn |
| send timeout | 10 s | the socket is given up on, so an engine that stops reading cannot block the worker |
| ping | a third of the silence window | a quiet socket is pinged |
| silence | 10 s, configurable | nothing heard: the socket is taken for dead (an engine killed without a close, a network gone without a reset), and its keys go stale; checked every thirtieth of the window, a second at most, so the cache serves a frozen engine's rows for the window and no longer |
| reconnect delay | 1 s | a lost socket is retried every second |

**Refusals.** A refusal from the engine is remembered for the query and
its values for `STREAMGRES_REFUSED_TTL_MS`. Some refusals are not remembered:
- those from the client's own side (not connected, over the cap, no
  answer in time);
- the engine's per-connection limit (`the subscription limit of ...`),
  which frees up as other keys go. All
remembered refusals are forgotten when a socket drops, since the engine
that comes back may know types the old one did not; so are the column
types learnt, since a migration may have changed one.

**Reconnect.** A dropped socket refuses the subscribes waiting on it,
marks the cache entries of its keys stale, which go live until it is
back, and
on reconnect resubscribes every key it holds under the same ids.
The drop forgets the column types learnt, and a resubscribe has no
connection to look them up on, so a rows-mode key is let go when its
rows come back and its next read subscribes it afresh, looks the types
up and is served from rows again (types learnt again before a
resubscribe answers let it rebuild with no Postgres read). Refill-mode
keys refill on their next read; keys now refused are
forgotten. A key is pending until the engine accepts it again, and reads
of it go live meanwhile. One socket dropping affects only its keys. With
the engine unreachable, every subscribe is refused at once, so an engine
outage costs the application its cache and nothing else.

## Cache limits

`cache::Limits`, from the environment (`Limits::from_env`) or in code
(`Cache::with_limits`):

| variable                           | meaning                                      | default |
|------------------------------------|----------------------------------------------|---------|
| `STREAMGRES_CACHE_MAX_BYTES`       | rows and keys held                           | 256 MiB |
| `STREAMGRES_CACHE_MAX_ENTRY_BYTES` | the largest result kept; 0 no limit          | 16 MiB  |
| `STREAMGRES_CACHE_IDLE_MS`         | drop an entry unread for this long; 0 never  | 30 min  |
| `STREAMGRES_CACHE_TTL_MS`          | drop an entry this old however read; 0 never | off     |
| `STREAMGRES_REFUSED_TTL_MS`        | skip the engine for a refused query this long; 0 always ask | 30 s |

A rows-mode entry weighs its store twice, for the rows and for the
result built from them, and is weighed again when the store drifts by a
quarter (64 KiB at least). A refill-mode entry weighs its rows; a stale
mark its key. A result over the entry limit is served but not kept, and
its subscription ends.

When the cache is full, what stays is decided by how often a key is
asked for, not by size or age (moka's frequency-based admission): a key
read again and again displaces an entry read once, and a run of keys
each read once cannot displace it. Reads are counted, misses included,
once the cache is past half its budget.

Idle time makes the cache follow what the backend reads now; the engine
changing a query's rows is not a read. Time to live drops an entry on a
timer regardless of reads; it is off by default because the engine
keeps entries fresh. The byte budget and both clocks end the
subscription and bump the generation.

`cache::Stats` counts: `hits`, `misses`, `refused`, `refused_recently`,
`touched`, `evicted`, `expired`, `stale_fills`, `oversized`, `replaced`,
`after_write`, `locking`, `lagging`.

## Routing

A read goes through the cache (`Route::Streamgres`) or live
(`Route::Postgres`). The most specific level wins:

1. **The query.** `query.via(Route::Postgres)`, or `via_postgres()` /
   `via_streamgres()` from `RouteDsl`, renders a leading comment such as
   `/*streamgres:postgres*/`. The wrapper reads and strips it, so the
   key and the SQL the engine sees are the plain query and flagged and
   unflagged reads share an entry. `conn.with_route(route, |c| ...)`
   flags every read in the closure.
2. **The pool.** Each connection's route and cache come from the URL it
   was established with, through the process-wide `Settings`
   (`install`, once per process). `Settings::single` shares one cache
   and routes by URL; `Settings::by_url` gives each database its own
   cache, since one engine streams one database. A URL with no cache
   is a passthrough. `set_route` changes one connection.
3. **The default:** Streamgres.

`Consistency::Eventual` and `Consistency::Strong` are aliases of the two
routes.

**Read your writes.** Between a commit and the engine's delta, a
Postgres read sees the new row and a cache read the old one. So after a
write through a cache (`execute`, raw SQL with any statement but
`SELECT`, `BEGIN`, `ROLLBACK`, `SAVEPOINT` or `SET`, `INSERT ...
RETURNING`, a `COMMIT`), every read through that cache goes live for
`STREAMGRES_READ_YOUR_WRITES_MS` (default 1000, 0 off;
`set_read_your_writes` per connection), counted from when the write
ends, however long it took. The clock lives on the cache, so a write on
one pooled connection covers reads on the others. A Postgres read never
writes into the cache.

## diesel-async

`AsyncSyncedConnection` (feature `async`) wraps `AsyncPgConnection` and
implements `AsyncConnection` and `PoolableConnection`, so bb8 and
deadpool pools work unchanged. Routing, the cache, read-your-writes, the
settings and the engine client are shared with the blocking wrapper.

`AsyncConnection::load` must return a future that borrows neither the
connection nor the query. So inside `load`, synchronously, the key is
rendered, the route decided, the cache checked, and the futures the read
might need are built: the inner connection's load (polled only if the
read goes to Postgres) and a lookup of every table's column types
(`types::ALL_TABLES_SQL`, polled only when the engine ships rows for a
table whose types are unknown). The subscribe itself runs on a blocking
thread. This is why `Subscriptions::subscribe` has two steps: it answers
`NeedsTypes` for unknown tables, and `materialize` finishes once the
caller has looked them up.

An enum bind needs its OID for the key. The async wrapper cannot look it
up inside `load`, so the first read with such a bind goes live while the
OID is resolved and stored in the cache for every connection sharing it;
later reads are cached normally. An array of an enum (`eq_any`) cannot
be rendered until the OID is known and takes the same path.

diesel-async allows several pending futures per connection and panics
if its transaction state is queried while one is pending. The wrapper
counts its own futures in flight and, while any is, sends reads live
without asking. A future dropped after it subscribed ends its
subscription, and a dropped `with_route` scope restores the outer route.

## Engine behaviour the wrapper relies on

- **Catalog.** The engine loads its catalog at start and follows schema
  changes through the DDL event trigger. An added column is absorbed;
  new tables keep the known enum types; a new enum type is learnt the
  first time a subscribe binds it. A dropped
  column or a `TRUNCATE` of a published table stops the engine; it
  restarts and serves the schema as it is, and the client reconnects.
- **Times.** The engine keeps every time in UTC microseconds, and every
  storage and replication session runs with `TimeZone=UTC`, so a time in
  a filter means the same instant to Postgres and to the engine.

## Tests

| file | covers |
|---|---|
| `tests/passthrough.rs` | every Diesel read and write behaves the same through the wrapper |
| `tests/owned_row.rs` | a captured row decodes like the live row |
| `tests/cached_reads.rs` | cache rules against a fake engine side: generations, limits, clocks, refusals, routing, read-your-writes, admission |
| `tests/async_cached.rs` | the same through diesel-async |
| `tests/settings.rs` | cache and route chosen by URL |
| `tests/join_sql_shape.rs` | the exact join SQL Diesel renders, which the engine parser expects |
| `tests/silent_engine.rs` | an engine that never answers, and one that drops every socket |
| `tests/engine_live.rs`, `tests/async_live.rs` | the whole path against a running engine |
| `tests/shape_coverage.rs` | production-shaped lookups compared with Postgres before and after a change |
| `tests/engine_latency.rs` | commit-to-cache latency |
| `tests/s2s_live.rs` (engine crate) | the protocol frame by frame |
| `crates/streamgres-demo/tests/scenario.rs` | every lookup of the demo app through both wrappers under writes, an engine restart and migrations |

`crates/streamgres-demo` is a ticket desk built on the wrapper: a
list of query shapes, each declaring rows, refill or refused; a writer
that makes every kind of change; a verify loop comparing both routes;
latency and migration commands. `demo/` runs it with Postgres and the
engine under docker compose.

## Running it locally

- Postgres 15 or newer with `wal_level = logical`. A Homebrew
  `postgresql@16` on its own port works: `initdb`, set
  `wal_level = logical` and a short `unix_socket_directories`, `pg_ctl
  start`.
- In the database: the demo's publication and schema-change trigger
  stack, both for app `demo`: `psql -f demo/postgres-init.sql` and
  `psql -f demo/ddl-triggers.sql`.
- The engine: `STREAMGRES_APP_ID=demo STREAMGRES_PUBLICATION=demo_pub STREAMGRES_PG_DSN=... STREAMGRES_QUERY_URL=http://127.0.0.1:1/q STREAMGRES_MUTATE_URL=http://127.0.0.1:1/m cargo run --release --bin server`.
  The query and mutate URLs are required by the config and unused on
  the S2S path.
- Or everything at once: `docker compose -f demo/docker-compose.yml up --build`.
- tokio-console: build the server with
  `RUSTFLAGS="--cfg tokio_unstable" cargo build --release --bin server --features console`,
  run it, and `tokio-console` attaches on `127.0.0.1:6669`; the S2S
  task is named `s2s`.
