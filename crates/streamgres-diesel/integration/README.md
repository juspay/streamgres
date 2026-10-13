# Hyperswitch integration

`hyperswitch-diesel_models.patch` (against juspay/hyperswitch 906a402) adds
a `streamgres` feature to `diesel_models` that makes its connection alias
`SyncedConnection<PgConnection>`. The dependency is a local path in the
patch; replace it with a git source when the crate is published.

Verified with `cargo check -p diesel_models --features v1,streamgres` and
`cargo check -p storage_impl --features v1,diesel_models/streamgres`.
