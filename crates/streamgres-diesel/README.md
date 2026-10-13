# streamgres_diesel

A Diesel connection wrapper for Postgres that an application swaps in for
`PgConnection`. Reads will be served from a local copy kept in sync by the
Streamgres engine; writes, strong reads and transactions go to Postgres as
before.

Current state: works end to end against a running engine. A read on an
eventual connection outside a transaction is keyed by its SQL and bind
bytes, subscribed with the engine over its S2S endpoint, read from
Postgres once, and kept byte for byte until the engine reports it
touched; locally a commit reaches the cache in 20 to 30 ms. Strong
connections, transactions, writes and queries the engine refuses go to
Postgres as before. An engine restart clears the cache and resubscribes.
See `docs/design.md`.

## Using it

```rust
let engine = EngineClient::connect("ws://engine:4848/zero/s2s/v1");
let cache = Arc::new(Cache::new(256 * 1024 * 1024, Arc::new(engine.clone())));
engine.attach(&cache);
streamgres_diesel::install(Settings {
    cache,
    consistency_for: Box::new(|url| if url.contains("replica") { Consistency::Eventual } else { Consistency::Strong }),
}).ok();
// then use SyncedConnection<PgConnection> where PgConnection was, pools included
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
