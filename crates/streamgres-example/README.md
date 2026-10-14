# streamgres-example

A ticket desk on the Streamgres Diesel wrapper. It is the engine's demo
(`demo/README.md` at the repository root walks through it) and the
wrapper's test bed.

## The query list

`src/queries.rs` holds every read the desk does, as `Lookup`: one named
shape each, runnable through the blocking wrapper (`run_sync`) and the
async one (`run_async`). The shapes are chosen for what they exercise:

| lookup        | exercises                                                 |
|---------------|-----------------------------------------------------------|
| `ticket`      | primary key, enum columns, timestamps                     |
| `queue`       | enum array bind, `ORDER BY` timestamp `DESC`, `LIMIT`, `OFFSET` |
| `queue_count` | `COUNT(*)` with an enum array bind                        |
| `comments`    | inner join composed from engine rows                      |
| `attachment`  | `bytea`                                                   |
| `agents`      | `text[]` column                                           |
| `sla`         | `interval` column: refill mode, not rows mode             |
| `search`      | `ILIKE`: refused by the engine, served live               |
| `recent`      | `ORDER BY updated_at DESC LIMIT`                          |

A shape that starts to matter to the wrapper goes in this list, so it
is exercised the day it changes.

## The scenario test

`tests/scenario.rs` runs every lookup against a live engine through
both wrappers and asserts what the wrapper promises: the first read
matches Postgres, the second is a hit, the blocking wrapper shares the
entry, a change made elsewhere is seen within a bound and the result
again matches Postgres, a refused shape never hits and is always
correct, a write on one connection is seen by the next read on another
of the same cache, and a read dropped mid-flight leaves no subscription
behind. It reseeds the dataset, so run it against a database nobody is
watching:

```bash
STREAMGRES_S2S_URL=ws://127.0.0.1:4848/zero/s2s/v1 \
DATABASE_URL=postgres://user@host:5432/db \
cargo test -p streamgres_example --test scenario -- --nocapture
```

## The binary

```bash
example seed                      # schema and dataset; safe to repeat
example serve                     # the HTTP API and the stats page
example bench --lookup queue      # p50/p95/p99 per route, in process
example bench --list
```

`DATABASE_URL` and `STREAMGRES_S2S_URL` are required; `EXAMPLE_ADDR`
(`0.0.0.0:8080`) and `EXAMPLE_WRITER_MS` (`1000`; `0` turns the
background writer off) are optional, as are the wrapper's own
`STREAMGRES_CACHE_*`, `STREAMGRES_S2S_*` and
`STREAMGRES_READ_YOUR_WRITES_MS`.

## Pools

`src/pools.rs` opens three pools against one database: `main`, through
the cache and routed Streamgres; `reporting`, the same wrapper routed
Postgres by its URL; and `plain`, diesel-async's own connection with no
wrapper, which the background writer uses so its changes reach the cache
the way another service's would. A small blocking pool of the sync
wrapper shares the same cache.
