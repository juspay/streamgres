# streamgres_diesel

A Diesel connection wrapper for Postgres that an application swaps in for
`PgConnection`. Reads will be served from a local copy kept in sync by the
Streamgres engine; writes, strong reads and transactions go to Postgres as
before.

Current state: the cache path works against a fake engine side. A read
on an eventual connection outside a transaction is keyed by its SQL and
bind bytes, subscribed with the engine, read from Postgres once, and kept
byte for byte until the engine reports it touched. Strong connections,
transactions, writes and queries the engine refuses go to Postgres as
before. What is missing is the real engine client behind the
`Subscriptions` trait; see `docs/design.md`.

```sh
# needs a Postgres; default is the local socket database streamgres_diesel_test
createdb streamgres_diesel_test
cargo test
```
