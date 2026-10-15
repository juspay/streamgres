# Streamgres in five minutes

A ticket-desk backend whose reads are served from an in-memory cache the
Streamgres engine keeps fresh from Postgres's replication stream. One
compose file brings up Postgres, the engine and the backend; a
background writer changes a ticket every second so there is something to
keep fresh.

```bash
docker compose -f demo/docker-compose.yml up --build
```

When the three services are up, open <http://localhost:8080> for the
stats page, or walk through it with curl. If a port is taken on your
machine, override it: `EXAMPLE_PORT=8081 ENGINE_PORT=4850
POSTGRES_PORT=5435 docker compose -f demo/docker-compose.yml up --build`.

## 1. A read, twice

```bash
curl -s localhost:8080/tickets/1 | jq '{source, micros, rows}'
curl -s localhost:8080/tickets/1 | jq '{source, micros}'
```

The first read subscribes the query with the engine, which ships the
rows (`"source": "engine"`, no Postgres read); the second is served
from the cache (`"source": "cache"`), with no round trip at all.

## 2. A change from outside

Change the ticket with psql, the way another service would:

```bash
docker compose -f demo/docker-compose.yml exec postgres \
  psql -U streamgres -d tickets -c "UPDATE tickets SET priority = 'high' WHERE id = 1"
curl -s localhost:8080/tickets/1 | jq '{source, rows: [.rows[].priority]}'
```

The read still comes from the cache, and shows the new priority: the
engine saw the commit and replaced the cached rows before you could type
the second command.

## 3. The shapes

```bash
curl -s localhost:8080/lookups | jq -r '.[] | "\(.name): \(.exercises)"'
```

Each lookup is a query shape the wrapper handles differently: an enum
bound as an array, a page with an offset, a join composed from the
engine's rows per table, a left join, a `bytea`, a two-column key, `IS
NULL`, `OR`, `NOT IN`, a timestamp range, a hundred-id `IN` list, empty
results, an `interval` column the engine cannot ship (so that query is
refilled from Postgres on each change instead), and the shapes the
engine refuses and serves live: `ILIKE`, `DISTINCT`, `GROUP BY`, `MAX`
and the array operator `@>`. The stats page has a button per shape;
`crates/streamgres-example/README.md` lists them with their paths.

## 4. Routing

A read can be sent to Postgres regardless of the cache, per query or
per pool:

```bash
curl -s 'localhost:8080/tickets/1?route=postgres' | jq '{route, source}'
curl -s 'localhost:8080/tickets/1?pool=reporting' | jq '{route, source}'
```

The first flags the query itself. The second uses a pool whose
connections are routed to Postgres by the URL they were opened with, the
way a pool against a primary would be.

## 5. Your own writes

```bash
curl -s -X POST localhost:8080/tickets/1/status -H 'content-type: application/json' -d '{"status":"closed"}'
curl -s localhost:8080/tickets/1 | jq '{source, rows: [.rows[].status]}'
```

The write went through the backend's own pool, so for the next second
every read through the cache goes to Postgres (`"source": "postgres"`)
and sees the write at once, before the engine has shipped it.

## 6. Counters

```bash
curl -s localhost:8080/stats | jq
```

`cache.hits` and `cache.misses` are what they say; `cache.replaced` is
how many times the engine's deltas replaced cached rows; `touched` how
many times a change made the cache refill from Postgres (the `interval`
query); `after_write` the reads sent to Postgres inside the
read-your-writes window; `engine.materialized` the queries served from
engine rows rather than refills; `writer` how many changes of each kind
the background writer made; `verify` what the self-check found.

## 7. The demo checks itself

```bash
curl -s localhost:8080/verify | jq '{compared, lagged, mismatches}'
```

Every lookup is read through the cache and through Postgres and
compared. A pair that differs is read again for up to a second, since
the cache may lag a commit by the engine's propagation delay; one that
never agrees is a mismatch, returned with both answers. With
`EXAMPLE_VERIFY_MS=500` the backend runs this pass every half second
in the background, under the writer, and `/stats` carries the totals.
The writer's mix covers inserts, deletes, moves between organizations,
renames under the joins, assignees flipped to `NULL` and back,
multi-row transactions, rollbacks and oversized comment bodies
(`EXAMPLE_WRITER_MIX` narrows it), so the stack is a continuous
correctness test of the wrapper, not only a demo.

## 8. Schema changes

```bash
docker compose -f demo/docker-compose.yml exec example /app/example migrate widen
curl -s localhost:8080/tickets/1 | jq '{source}'
```

A column added with a constant default is absorbed: the read above is
still served from the cache. `migrate new-type` adds an enum type and a
column of it, which the engine absorbs too, but a bind of the new type
(`/orgs/1/kind/bug`) is refused and served live until the engine
restarts. `migrate drop` removes both: the engine cannot follow a
dropped column, so it stops, compose restarts it, and reads go to
Postgres meanwhile. A `TRUNCATE` of a published table stops it the same
way: the replication stream carries no rows for it to follow.

## 9. How stale is the cache?

```bash
docker compose -f demo/docker-compose.yml exec example /app/example lag --rate 20 --for 10
```

prints how long a commit takes to reach the cached answer, as p50, p95,
p99 and max over ten seconds, under twenty background writes per
second.

## Latency

Without the compose stack in the way, against the same services:

```bash
docker compose -f demo/docker-compose.yml exec example \
  /app/example bench --lookup queue --concurrency 16 --requests 5000
```

prints p50, p95, p99 and throughput for the Postgres route and the
Streamgres route side by side.

## Running it against your own Postgres

The backend is `crates/streamgres-example`; `example seed`, `example
serve` and `example bench` take `DATABASE_URL` and
`STREAMGRES_S2S_URL`; `example verify` and `example migrate` too.
`EXAMPLE_SCALE=10` seeds ten times the tickets, so pages run deep and
the cache's byte limits matter. Postgres needs `wal_level = logical`,
the publication and the schema-change trigger (`demo/postgres-init.sql`
and `scripts/sql/ddl-triggers.sql`); see `docs/deploy.md`.
