# streamgres_diesel: design notes

Status: option A below is chosen and implemented on the wrapper side
(`cache`, `key`, `cursor`, `subscriptions` modules) against a fake engine.
The engine client and the engine's S2S endpoint are next.

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

Hyperswitch already names its connection through one alias
(`diesel_models::DejaPgConnection`), so adopting the wrapper there is a
cargo feature and no code change. Its pools (`bb8` through
`async-bb8-diesel`) run every Diesel call on a blocking thread, which is
where a cache miss can safely wait on the engine.

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

This is Hyperswitch's existing moka cache with its Redis pub/sub replaced
by the WAL: no `publish_and_redact` to remember, writes from outside the
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

## Contract with the engine (for A)

The engine already parses a SQL subset against its catalog
(`src/parser`): single table, filter of `=`, `!=`, `<`, `>`, `IN`,
`IS NULL` over `AND`/`OR`, `ORDER BY`, `LIMIT`. For A the client sends:

- `subscribe { id, sql, binds }` where `sql` is Diesel's text with `$n`
  and `binds` the values as text. The engine substitutes and parses.
  Queries outside the subset are refused with a reason, and the wrapper
  then serves that query straight from Postgres with no cache.
- `unsubscribe { id }` when the cache evicts the key.
- Downstream: `touched { ids, position }` per transaction, and the
  existing row pokes once B lands.

Reconnect: on a dropped stream the wrapper clears the whole cache and
resubscribes, so no change is missed. A `position` cookie lets the
engine replay instead once it keeps a short history.

## Consistency

- `Eventual` (default): cache first.
- `Strong`: never the cache. Set per connection or per pool, so a master
  pool is strong and a replica pool eventual, matching Hyperswitch's
  split.
- Inside a transaction, always Postgres: the ANSI manager's depth is on
  the inner connection and is checked before the cache.

## Open points carried from the roadmap call

- id construct: subscription id = hash of SQL plus binds; client id per
  process; group id per application. Proposed, not agreed.
- memory limit: entries and bytes, from an init config, with LRU and
  unsubscribe on eviction.
- one engine connection per process, multiplexing subscriptions; measured
  under tokio-console before deciding on a pool.
- joins: the engine ships rows per table; for A this is invisible (the
  rows come from Postgres), for B the merge is the hard part.
