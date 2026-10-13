# Hyperswitch integration

`hyperswitch-diesel_models.patch` (against juspay/hyperswitch 906a402) adds
a `streamgres` feature to `diesel_models` that makes its connection alias
`SyncedConnection<PgConnection>`. The dependency is a local path in the
patch; replace it with a git source when the crate is published.

Verified with `cargo check -p diesel_models --features v1,streamgres` and
`cargo check -p storage_impl --features v1,diesel_models/streamgres`.

The patch also adds `crates/diesel_models/tests/streamgres_live.rs`: with
`DATABASE_URL` (a database carrying Hyperswitch's migrations, streamed by
an engine) and `STREAMGRES_S2S_URL`, `cargo test -p diesel_models
--features v1,streamgres --test streamgres_live` drives Hyperswitch's own
`Config::find_by_key` and `Config::update_by_key` through the wrapper:
the lookup is subscribed, the repeat is a cache hit, and the update is
seen by the cached lookup through the engine (7.7 ms locally, write
included).
