# pgwire-replication 0.4.0, vendored

A copy of [pgwire-replication](https://crates.io/crates/pgwire-replication)
0.4.0 (Apache-2.0 OR MIT), trimmed to its library, with one change:
`ReplicationConfig::with_temporary_slot`, which has the worker create the
slot as a `TEMPORARY` logical slot on the replication connection just
before `START_REPLICATION` (`create_temporary_slot` in
`src/client/worker.rs`). PostgreSQL drops such a slot when its connection
ends, however it ends, so an engine killed outright leaves no slot holding
WAL. A temporary slot can only be streamed by the connection that made it,
which is why it cannot be made beforehand with an ordinary query.

The root `Cargo.toml` points `[patch.crates-io]` here. Once the same option
is released upstream, drop this directory and the patch.
