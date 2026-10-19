# streamgres_diesel

A Diesel connection wrapper for Postgres that an application swaps in for
`PgConnection`. Reads will be served from a local copy kept in sync by the
Streamgres engine; writes, strong reads and transactions go to Postgres as
before.

Current state: works end to end against a running engine. A read on an
eventual connection outside a transaction is keyed by its SQL and bind
bytes and subscribed with the engine over its S2S endpoint. When every
projected column has a type the client can encode exactly, the engine
serves the rows and every change replaces the entry in place: no
Postgres read, a commit lands in the cache in about 0.3 ms locally.
Otherwise the rows are read from Postgres once and refilled when the
engine reports a change. Strong connections, transactions, writes and
queries the engine refuses go to Postgres as before. An engine restart
resubscribes and refills, and asks again about what was refused. See
`docs/design.md`.

## Using it

```rust
// Pool size and subscription cap from STREAMGRES_S2S_CONNECTIONS and
// STREAMGRES_S2S_MAX_SUBSCRIPTIONS (defaults 2 and 10000); the bearer
// token from STREAMGRES_S2S_TOKEN when the engine asks for one.
let engine = EngineClient::connect("ws://engine:4848/zero/s2s/v1");
let cache = Arc::new(Cache::new(256 * 1024 * 1024, Arc::new(engine.clone())));
engine.attach(&cache);
// One cache per database (one engine streams one database), a route per pool.
streamgres_diesel::install(Settings::single(
    cache,
    |url| if url.contains("replica") { Route::Streamgres } else { Route::Postgres },
)).ok();
// Several databases: Settings::by_url(|url| Some(Attachment { cache: ..., route: ... }))
// then use SyncedConnection<PgConnection> where PgConnection was, pools included.
// One query can override its pool's route:
//   configs::table.find(key).via(Route::Postgres).first(conn)
//   conn.with_route(Route::Streamgres, |c| configs::table.find(key).first(c))
```

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
cargo test --test engine_live
```

```sh
# needs a Postgres; default is the local socket database streamgres_diesel_test
createdb streamgres_diesel_test
cargo test
```
