# streamgres-example

A ticket desk on the Streamgres Diesel wrapper. It is the engine's demo
(`demo/README.md` at the repository root walks through it) and the
wrapper's test bed: a fixed list of query shapes, a writer that makes
every kind of change, a verify loop that compares every shape across
the routes while the writer runs, and a scenario test that holds every
shape to what it expects of the engine.

## The query list

`src/queries.rs` holds every read the desk does, as `Lookup`: one named
shape each, runnable through the blocking wrapper (`run_sync`) and the
async one (`run_async`), with what it expects of the engine
(`expected()`): served from the engine's rows, refilled from Postgres
on each change, or refused and served live.

| lookup            | exercises                                                   | expects |
|-------------------|-------------------------------------------------------------|---------|
| `ticket`          | primary key, enum columns, timestamps                       | rows    |
| `queue`           | enum array bind, `ORDER BY` timestamp `DESC`, `LIMIT`, `OFFSET` | rows |
| `queue_count`     | `COUNT(*)` with an enum array bind                          | rows    |
| `comments`        | inner join composed from engine rows                        | rows    |
| `attachment`      | `bytea`                                                     | rows    |
| `agents`          | `text[]` column                                             | rows    |
| `sla`             | `interval` column                                           | refill  |
| `search`          | `ILIKE`                                                     | live    |
| `recent`          | `ORDER BY updated_at DESC LIMIT`                            | rows    |
| `org_ticket`      | two-column key                                              | rows    |
| `unassigned`      | `IS NULL`                                                   | rows    |
| `escalated`       | `OR` of two enum binds                                      | rows    |
| `open_except`     | `<>` and `NOT IN`                                           | rows    |
| `created_between` | timestamp range, two binds                                  | rows    |
| `many`            | `IN` list of a hundred ids                                  | rows    |
| `queue_end`       | page past the end: empty, subscribed, stays empty           | rows    |
| `nothing`         | never any rows: empty, subscribed, stays empty              | rows    |
| `all_tickets`     | largest result; not kept under a small per-entry cap        | rows    |
| `comment_authors` | left join with an empty side                                | rows    |
| `skilled`         | array `@>`                                                  | live    |
| `statuses`        | `DISTINCT`                                                  | live    |
| `workload`        | `GROUP BY`                                                  | live    |
| `last_activity`   | `MAX`                                                       | live    |
| `of_kind`         | an enum type added by `example migrate new-type`; not in the demo list | rows after a restart |

A shape that starts to matter to the wrapper goes in this list, so it
is exercised the day it changes.

## The writer

`src/writer.rs` makes one change per tick, drawn from a mix: a status
moved on, a comment added, a ticket inserted into the first
organization's queue, one of the writer's own tickets deleted with its
comments cascading, one moved to another organization, an agent renamed
under the joins, an assignee flipped between an agent and `NULL`,
several tickets touched in one transaction, a change rolled back, and a
comment with a body too large for its row written and then updated
without touching the body. `EXAMPLE_WRITER_MIX` names the operations to
draw from; the default is all of them. The seeded rows keep their ids,
so the demo's URLs stay valid.

## The verify loop

`src/verify.rs` reads every lookup through the cache and through
Postgres and compares. A pair that differs is read again until it
agrees or a settle time runs out; one that never agrees is a mismatch,
kept with both answers. `example verify` runs it for a while under the
writer and exits 1 on any mismatch; `serve` runs it in the background
when `EXAMPLE_VERIFY_MS` is set; `/verify` runs one pass; `/stats`
carries the counters.

## The scenario test

`tests/scenario.rs` runs against a live engine and asserts what the
wrapper promises:

- every lookup matches Postgres on the first read, is served the way
  it expects (rows, refills or live), is a hit by the third read, shares
  its entry between the two wrappers, and sees a change made elsewhere
  within a bound, after which it matches Postgres again; the empty
  results stay empty;
- a write on one connection is seen by the next read on another of the
  same cache;
- a flag on the query beats the connection's route both ways, a scope
  routes everything inside it, and the read-your-writes window ends;
- a result over the per-entry cap is served and correct but not kept;
- a read dropped mid-flight leaves no subscription behind;
- sixteen readers comparing every lookup across the routes while the
  writer runs its whole mix never see the cache disagree with Postgres
  for long;
- the same under an engine restart in the middle (needs
  `STREAMGRES_RESTART_CMD`);
- the three kinds of schema change: a widened table is absorbed and
  the cache keeps serving, a new enum type is absorbed but a bind of it
  is refused until the engine restarts, and a dropped column stops the
  engine, which restarted serves the schema as it is (needs
  `STREAMGRES_RESTART_CMD`).

The load tests tell a wrong answer in this process's cache from one in
the engine, by reading the lookup again through a fresh subscription.
The first fails the test; the second is reported, and fails only under
`STREAMGRES_STRICT`, since it is the engine's to fix.

The tests reseed the dataset, so run them against a database nobody is
watching, one at a time:

```bash
STREAMGRES_S2S_URL=ws://127.0.0.1:4848/zero/s2s/v1 \
DATABASE_URL=postgres://user@host:5432/db \
STREAMGRES_RESTART_CMD='scripts/restart-engine.sh' \
cargo test -p streamgres_example --test scenario -- --test-threads=1 --nocapture
```

`STREAMGRES_RESTART_CMD` is a shell command that restarts the engine
and returns once it serves again; without it the two tests that need
one skip themselves.

## The binary

```bash
example seed                        # schema and dataset; safe to repeat
example serve                       # the HTTP API and the stats page
example bench --lookup queue        # p50/p95/p99 per route, in process
example bench --list
example verify --for 60             # every lookup on both routes, under the writer; exits 1 on a mismatch
example lag --rate 20 --for 10      # commit to cached answer, under 20 writes/s
example migrate widen|new-type|drop # a schema change of each kind
```

`DATABASE_URL` and `STREAMGRES_S2S_URL` are required. Optional:
`EXAMPLE_ADDR` (`0.0.0.0:8080`), `EXAMPLE_WRITER_MS` (`1000`; `0` turns
the writer off), `EXAMPLE_WRITER_MIX` (all operations), `EXAMPLE_VERIFY_MS`
(`0`: off), `EXAMPLE_SCALE` (`1`; multiplies the seed's tickets, comments
and attachments), and the wrapper's own `STREAMGRES_CACHE_*`,
`STREAMGRES_S2S_*` and `STREAMGRES_READ_YOUR_WRITES_MS`.
