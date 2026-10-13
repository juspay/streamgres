# streamgres_diesel

A Diesel connection wrapper for Postgres that an application swaps in for
`PgConnection`. Reads will be served from a local copy kept in sync by the
Streamgres engine; writes, strong reads and transactions go to Postgres as
before.

Current state: transparent passthrough, proven against every Diesel read
and write path (`load`, `first`, `count`, `execute`, `get_result`,
`sql_query`, row-by-row loading, nested transactions, `r2d2` pools,
instrumentation).

```sh
# needs a Postgres; default is the local socket database streamgres_diesel_test
createdb streamgres_diesel_test
cargo test
```
