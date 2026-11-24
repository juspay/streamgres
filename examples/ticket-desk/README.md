# Streamgres ticket desk

A support desk whose reads go through an embedded Streamgres with
`load_and_sync`, and a live test bed for the Diesel adapter. The binary serves
its own HTML, CSS and JavaScript; there is no Node build, CDN or separate
frontend server.

## Run locally

Use PostgreSQL with `wal_level=logical`, free replication slots, and an
administrative account for the first install of the publication and trigger
stack. Use a dedicated demo database: the writer changes its data and lab mode
alters its schema.

```sh
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ticketdesk
cargo run --release --manifest-path examples/ticket-desk/Cargo.toml -- serve --init
```

Open <http://127.0.0.1:8080>. `--init` creates and seeds a missing fixture and
installs the publication and trigger stack; it keeps an existing dataset.
`--seed` replaces the demo's rows. `DEMO_SCALE` multiplies the fixture's 180
tickets across three organizations. Add `--lab` to enable the schema and fault
experiments (loopback only).

Other commands:

| Command | What it does |
|---|---|
| `demo seed` | Installs the schema, publication and trigger stack, and reloads the data. |
| `demo verify --for 60` | Compares every supported shape through Streamgres and Postgres under the writer; exits 1 on a mismatch or a wrong verdict (memory, Postgres or unsupported). |
| `demo bench --lookup queue_cards` | Times one shape on each route. |
| `demo migrate widen\|new-type\|drop` | Runs one schema change. |

## How the desk reads

Streamgres opens its own small pool of Postgres connections, apart from the
application's `diesel-async` pool:

```rust
let lib = Streamgres::connect(replication_dsn).start().await?;
```

Snapshots and fallback queries use that pool; replication and snapshot
exporters use their own connections. Writes and direct reads use the app pool.

Every read is one call:

```rust
let (rows, trace) = query.load_and_sync(&lib).inspect().await;
```

- **Memory**: the query is kept and its rows are ready.
- **Postgres, while loading**: a query's first read registers it, and Postgres
  answers it (and every read until the query has loaded).
- **Postgres, at run time**: Streamgres supports the shape but cannot answer
  this read from memory (a value it cannot encode yet, lag, a budget).
- **Unsupported, an error**: the shape compares or orders by something
  Streamgres does not hold (timestamptz, bytea, arrays, intervals), or uses an
  operator it does not answer (`ILIKE`, `@>`, `DISTINCT`, `MAX`). The read
  returns `Error::Unsupported`; it is not sent to Postgres. Raw SQL never
  reaches Streamgres and goes to Postgres.

Every answer in the UI says who answered it and why not memory. Writes go to
Postgres through the pool and reach memory through the replication stream;
"Wait until memory shows it" reads the change back through Streamgres and
reports how long that took.

`card`, `queue_cards`, `queue_count`, `comment_authors` and the always-empty
`nothing` query are answered from memory. Shapes that only return a
`timestamptz` are answered by Postgres until the engine encodes it. The rest
are unsupported and shown as such; the desk's ticket list reads `queue_cards`.

## Inspecting

- **Queries → In memory now** lists every query Streamgres keeps
  (`lib.queries()`): its SQL, state, rows, ttl and last read.
- **Queries → a shape → Inspect** shows where that query stands
  (`query.inspect(&lib)`): kept or not and why, rows and bytes, ttl and when it
  will be freed, the engine's WAL position and lag, each place in the code that
  read it, and its last reads with who answered and why.
- `GET /api/inspect/{shape}` and `GET /api/memory` return the same as JSON.

The desk records the last 32 reads of each query (`Builder::debug_history`).

## Lab

| Experiment | What it shows |
|---|---|
| `widen` | A column added with a default is absorbed; reads from memory continue. |
| `new-type` | A new enum column is absorbed; reads of it are unsupported until a restart. |
| `drop` | A dropped column restarts the engine; reads fall back, then return to memory. |
| `connection` | This demo's replication connection is terminated and reconnects. |
| `slot` | This demo's replication slot is dropped; the engine starts over on a new one. |
| `transaction` | A held transaction's write is not seen from memory until it commits, then within the feed's delay. |

Fault experiments act only on this process's own slot, which Streamgres names
(`lib.engine().instance`). Every experiment pauses the writer and restores it.
To restore the usual fixture after an interrupted lab:

```sql
ALTER TABLE tickets DROP COLUMN IF EXISTS tags;
ALTER TABLE tickets DROP COLUMN IF EXISTS kind;
DROP TYPE IF EXISTS ticket_kind;
```

## Run in containers

```sh
docker compose -f examples/ticket-desk/docker-compose.yml up --build -d
```

Open `http://127.0.0.1:8080` (`DEMO_HTTP_PORT` picks another host port).
Postgres is internal to the Compose network and keeps its data in a volume.
The image runs as UID 10001 and needs no libpq. SIGTERM and Ctrl-C stop
Streamgres gracefully, dropping its replication slot.

## Configuration

| Flag | Environment | Default |
|---|---|---|
| `--listen` | `DEMO_LISTEN` | `127.0.0.1:8080` |
| `--pool-size` | `DEMO_POOL_SIZE` | 8: the app pool has 2 more; Streamgres opens its own 8 |
| `--writer-rate` | `DEMO_WRITER_RATE` | 8 operations per second |
| `--max-bytes` | `DEMO_MAX_BYTES` | 268435456 |
| `--max-lag-ms` | `DEMO_MAX_LAG_MS` | 2000 |
| `--lab` | `DEMO_LAB` | off |

`DATABASE_URL` is required. `DEMO_SCALE` defaults to 1, and `DEMO_WRITER_MIX`
picks the writer's operations.

## Code layout

| File | What it holds |
|---|---|
| `db.rs` | The `diesel-async` pool, the install, and Streamgres's setup. |
| `queries.rs` | Every shape, its route, and how a read is reported. |
| `verify.rs`, `bench.rs` | The comparison loop and the timing runs, shared by the CLI and the service. |
| `writer.rs` | The background writer. |
| `service/mod.rs` | HTTP routes, reads, writes and inspection. |
| `service/lab.rs` | Verification, load runs and experiments. |
| `service/telemetry.rs` | Bounded activity and per-route samples. |

## Validate

```sh
cargo clippy --manifest-path examples/ticket-desk/Cargo.toml --all-targets -- -D warnings
TICKETDESK_TEST_DSN=postgresql://postgres:postgres@localhost:5432/ticketdesk_tests \
  cargo test --manifest-path examples/ticket-desk/Cargo.toml
```

The HTTP scenario resets its disposable fixture, checks every shape's verdict
and data against Postgres, inspection, writes shown from memory, verification,
a load run, every lab experiment, and that shutdown drops the slot.

## Visual identity

Coral `#ff4f4f`, white and charcoal surfaces, fine grid lines, and the
system's own sans-serif and monospace fonts: the page loads nothing but
its own HTML, CSS and JavaScript.
