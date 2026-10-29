# diesel_streamgres

A Diesel connection wrapper for Postgres that an application swaps in for
`PgConnection`. Reads are served from a local cache the Streamgres engine
keeps current; writes, transactions and queries routed to Postgres go to
Postgres as before.

A read on a connection routed to Streamgres, outside a transaction, is
keyed by its SQL and bind bytes and subscribed with the engine over its
S2S endpoint. When every projected column has a type the client can
encode exactly, the engine serves the rows and keeps them current with
each commit: no Postgres read, and a commit lands in the cache in about
0.3 ms locally. Otherwise the rows are read from Postgres once and read
again when the engine reports a change. Queries the engine refuses go to
Postgres. An engine restart resubscribes and refills, and asks again
about what was refused. See `docs/design.md`.

## Using it

```rust
// Pool size and subscription cap from STREAMGRES_S2S_CONNECTIONS and
// STREAMGRES_S2S_MAX_SUBSCRIPTIONS (defaults 2 and 10000); the bearer
// token from STREAMGRES_S2S_TOKEN when the engine asks for one. Use
// wss:// (or a TLS-terminating proxy) unless the engine is on this
// machine or a private network: the token travels in the upgrade.
let engine = EngineClient::connect("wss://engine:4848/zero/s2s/v1");
for warning in engine.warnings() {
    log::warn!("{warning}");
}
let cache = Arc::new(Cache::new(256 * 1024 * 1024, Arc::new(engine.clone())));
engine.attach(&cache);
// One cache per database (one engine streams one database), a route per pool.
diesel_streamgres::install(Settings::single(
    cache,
    |url| if url.contains("replica") { Route::Streamgres } else { Route::Postgres },
)).ok();
// Several databases: Settings::by_url(|url| Some(Attachment { cache: ..., route: ... }))
// then use SyncedConnection<PgConnection> where PgConnection was, pools included.
// One query can override its pool's route:
//   configs::table.find(key).via(Route::Postgres).first(conn)
//   conn.with_route(Route::Streamgres, |c| configs::table.find(key).first(c))
```

## Callbacks on changes

The same `EngineClient` calls back when a query's result changes, which
makes a webhook on any query one subscription:

```rust
let subscription = engine
    .subscribe(&database_url, payments::table.filter(payments::status.eq("SUCCESS")),
        |change: Change<Payment>| async move {
            if let Change::Added(payment) = change {
                notify_merchant(&payment).await;
            }
        })
    .await?;
```

Without Diesel types of this crate's Diesel (an application on another
Diesel version, a fork, or none), `subscribe_sql` takes the query as SQL
and gives each row as a `SqlRow`, read by column name:

```rust
let subscription = engine
    .subscribe_sql(&database_url, r#"SELECT id, status FROM payments"#,
        |change: Change<SqlRow>| async move {
            if let Change::Updated { new, .. } = change {
                println!("{:?} is now {:?}", new.text("id"), new.text("status"));
            }
        })
    .await?;
```

To push one query's changes to many clients, `watch_sql` shares a single
subscription among every watcher of the same SQL; the last watcher dropped
stops it. With the `sse` feature, `sse::body` turns events into the text
of a `text/event-stream` response for any web framework:

```rust
let feed = sse::Feed::new(engine, database_url);
let body = feed.notifications([
    sse::Query::new("orders", "SELECT id, status FROM orders"),
    sse::Query::new("payments", "SELECT id, status FROM payments"),
]).await?; // a Stream of String; serve it as text/event-stream
```

The feed shares subscriptions, skips the initial rows, sends fifteen-second
heartbeats, and closes the whole response if any query stops being followed.
A missed-change notice becomes another refresh event. With the `embedded`
feature, `sse::Feed::try_embedded(config)?` starts the engine inside the application
with default client settings. Call `feed.ready(Duration::from_secs(30)).await?`
at startup to report replication setup failures before accepting requests.
`embedded_from_env(workspace_schemas)` intentionally loads both engine and client
configuration from the environment; explicit constructors never do. The application supplies its authorized workspace queries and
adapts the body to its HTTP framework; the library owns stream lifecycle.

The callback first gets `Change::Present` for each row already there.
Then, per commit, it gets `Added`, `Updated { old, new }` and `Removed`,
whoever made the change. It runs one change at a time. After a lost
engine connection, the difference is delivered. See
[`docs/backends.md`](../../docs/backends.md), section 7.

## Watching it, and rolling it out

`metrics::set_metrics` installs a hook over your telemetry: where each
read was served, fallbacks, refusals, connections, lag, lossy or ended
subscriptions, and shadow-read outcomes. The same events are logged
(`log` target `diesel_streamgres`) and kept in `metrics::history()`.
`STREAMGRES_CACHE_SHARE` sends only a share of reads through the cache,
and `STREAMGRES_SHADOW_SAMPLE` checks a share of cached answers against
Postgres. See [`docs/backends.md`](../../docs/backends.md), section 8.

## The engine in this process

With the `embedded` feature, `EngineClient::from_env()` runs the engine
inside the application when `STREAMGRES_ENGINE=embedded`, against the
primary at `STREAMGRES_PG_DSN`, and connects to `STREAMGRES_S2S_URL`
otherwise; the rest of the code is the same. It suits one or a few
processes: each holds its own replication slot. See
[`docs/backends.md`](../../docs/backends.md) for choosing a mode, reading
from replicas and read-your-writes.

## diesel-async

With the `async` feature, `AsyncSyncedConnection` wraps diesel-async's
`AsyncPgConnection` the same way and pools through bb8 or deadpool:

```rust
let conn = AsyncSyncedConnection::establish(&url).await?;   // picks up the installed settings
configs::table.find(key).first::<Config>(&mut conn).await?; // cached
configs::table.find(key).via(Route::Postgres).first::<Config>(&mut conn).await?;
```

## Live tests

With an engine streaming the test database (see `docs/design.md` for a
local setup):

```sh
STREAMGRES_S2S_URL=ws://127.0.0.1:4848/zero/s2s/v1 \
DATABASE_URL=postgres://sg@127.0.0.1:5434/sg \
cargo test --test engine_live --test listen_live
```

```sh
# needs a Postgres; default is the local socket database diesel_streamgres_test
createdb diesel_streamgres_test
cargo test
```

Notification events are refresh hints, not durable delivery: refresh authoritative
data on connection and after each event. Initial rows are skipped; added, updated
and removed rows invalidate by default, including deletion of the last row.
`Query::ignore_removals()` is only appropriate if a successor is guaranteed.

To limit Postgres fallback bursts during an engine outage, set
`STREAMGRES_FALLBACK_MAX_CONCURRENT` (per cache; default `0`, disabled).
`STREAMGRES_FALLBACK_WAIT_MS` bounds admission waits (default `50` ms); exhaustion
returns a Diesel error. Sync and async reads share the same capacity, held until
the fallback cursor ends or is dropped. Cache/engine hits, explicit Postgres
routes, transactions, locking queries and rollout bypasses do not consume it.
Programmatic configuration uses `cache::Limits`. This bounds eligible fallback
reads, not every database operation; size the normal connection pool separately.
