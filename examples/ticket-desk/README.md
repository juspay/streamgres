# Streamgres ticket desk

A working support desk and a live test bed for the embedded Streamgres Diesel
adapters. The binary serves its own HTML, CSS and JavaScript. No Node build,
CDN or separate frontend server is required.

## Run locally

Use PostgreSQL 17 with `wal_level=logical`, free replication slots, and an
administrative account for initial publication/trigger installation. Use a
dedicated demo database: the writer changes its data and lab mode alters schemas.

```sh
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ticketdesk
cargo run --release --manifest-path examples/ticket-desk/Cargo.toml -- serve --init
```

Open <http://127.0.0.1:8080>. `--init` creates and seeds a missing fixture,
installs the publication and trigger stack, and preserves an existing dataset.
`--seed` deliberately replaces the demo's rows. The original CLI commands
`seed`, `explain`, `verify`, `bench`, `lag`, `schema` and `migrate` remain available.
`DEMO_SCALE` multiplies the fixture's 180 tickets across three organizations.

The writer starts at eight operations per second. Its rate, mix and pause switch
are available in Lab. Use `--writer-rate 0` for a stable initial dataset.

```sh
# Enable the guarded schema and fault experiments on loopback.
cargo run --release --manifest-path examples/ticket-desk/Cargo.toml -- serve --init --lab

# CLI correctness under writes remains useful for a longer soak.
cargo run --release --manifest-path examples/ticket-desk/Cargo.toml -- verify --for 60
```

## Run in containers

From the repository root, Docker Compose builds the image and starts a dedicated
PostgreSQL 17 database with logical replication:

```sh
docker compose -f examples/ticket-desk/docker-compose.yml up --build -d
```

With Podman and `podman-compose`, select the example's build ignore file
explicitly. The root ignore file belongs to the separate server image.

```sh
podman-compose -p ticketdesk -f examples/ticket-desk/docker-compose.yml \
  --podman-build-args='--ignorefile=examples/ticket-desk/Dockerfile.dockerignore' \
  up --build -d
```

Open `http://127.0.0.1:8080`. Set `DEMO_HTTP_PORT=18080` on the Compose command
to use another host port. Postgres is internal to the Compose network and its
fixture is kept in a project volume; normal restarts preserve rows. Lab mode is
off by default. The runtime image uses UID 10001 and needs no source files or
frontend build tools. SIGTERM and Ctrl-C both shut down the cache gracefully.
The lockfile keeps the thread-pool dependency compatible with Rust 1.90.

## What to try

- **Overview:** run one real query through both routes, edit a ticket, and see
  actual answerer badges, writer activity and sampled latency history.
- **Ticket desk:** switch organization/status, browse pages, create tickets,
  change status and add comments. “Wait for my write” checks a WAL barrier.
- **Queries:** inspect all 23 normal shapes, their current verdicts and actual
  Diesel SQL. Unsupported operations use Postgres. Select sync or async on
  Overview to choose the adapter for comparisons and Lab checks. Continuous
  verification records the selected adapter. Async custom bind metadata can
  require Postgres fallback even for a shape supported by the sync path.
- **Lab:** compare results continuously, measure pooled service reads and run
  schema/session/transaction/recovery experiments. Each timeline records what
  was observed. Experiments pause and then restore the background writer.

A blue Postgres badge can be the correct result. `ILIKE`, array containment,
`DISTINCT`, raw grouping and `MAX` are deliberately included to exercise fallback.
The async adapter also delegates custom bind metadata to Postgres, including
some enum-bound queries; this is shown in its actual refusal reason.

## Measurements and correctness

Response `elapsed_us` measures the query/adapter, Diesel decoding and JSON
serialization inside the service. `pool_wait_us` is separate. Neither measures
the browser's network round trip. First reads can include subscription filling.
The Overview compares samples; it does not promise that every sample is faster. A read following a Postgres statement can pay a complete reader recheck,
which can cost more than a small direct lookup even though consecutive memory
reads are much cheaper. The library reuses the prepared validation statement
without skipping checks. Lab measures sustained traffic.

Query-card times start as **Initial inspection**, which may include cache filling
and reader validation. **Run both** replaces the card's badge with its **Latest
request** sample. The header's last-request sample is shown only on Overview.
The latency chart orders samples by event timestamp, including replayed events.

Load results include pooled service calls and serialization, excluding HTTP.
Throughput uses the complete run; percentiles use up to the latest 512 samples
per worker. Route telemetry retains 1,000 latency samples per route. Activity
emits at most 20 read samples per second; counters include every read.

Verification retries disagreements for 2.5 seconds while the writer runs. A
Postgres fallback is tracked separately because two live database snapshots may
differ between requests. Mismatches from actual cache answers and query errors
are reported, with the last differing pair available in the UI. Use the stable
HTTP scenario and core engine tests alongside this concurrent test bed.

Memory is bounded for telemetry, samples and blocking-job admission. Read, writer
and truth connections have separate r2d2 pools. Async reads use bb8 and
`diesel-async`. Pools have acquisition deadlines; SQL connections use statement
and idle-transaction timeouts. Cached reads avoid a checkout ping, since a ping
would invalidate the reader check on every request. Failed synchronous reads
explicitly retire their connection, since Diesel's default sync pool check only
inspects transaction state and can retain a disconnected idle socket. Plain
writer/truth pools validate on checkout; the async manager detects closed
connections. Initial reads during a disconnect can fail, then later requests
recover with new connections. Mutations are never retried automatically.

## Lab scope

Lab mode is off by default, and fault routes require a loopback Host header.
The service labels its cache-owner connections uniquely. Slot/connection faults
join that owner with its advisory slot lock and refuse if identification is
ambiguous. They never select arbitrary slots by database or prefix alone.

RLS, search-path and transaction scenarios attempt cleanup even on a failed
observation. Experiment tasks survive a disconnected HTTP caller so cleanup can
finish. Stopping the entire process during an experiment can still leave demo
schema changes behind. To restore the usual fixture after an interrupted lab:

```sql
ALTER TABLE attachments DISABLE ROW LEVEL SECURITY;
DROP SCHEMA IF EXISTS demo_tenant CASCADE;
DROP FUNCTION IF EXISTS public.demo_set_tenant();
ALTER TABLE tickets DROP COLUMN IF EXISTS tags;
ALTER TABLE tickets DROP COLUMN IF EXISTS kind;
DROP TYPE IF EXISTS ticket_kind;
```

Then restart the service with `--init`. Use a demo database, never production.
The app is an instrumentation showcase, not a multi-tenant authenticated product.

## Configuration

| Flag | Environment | Default |
|---|---|---|
| `--listen` | `DEMO_LISTEN` | `127.0.0.1:8080` |
| `--pool-size` | `DEMO_POOL_SIZE` | 8 tracked sync and 8 async |
| `--writer-rate` | `DEMO_WRITER_RATE` | 8 operations/second |
| `--max-bytes` | `DEMO_MAX_BYTES` | 268435456 |
| `--max-lag-ms` | `DEMO_MAX_LAG_MS` | 2000 |
| `--admit-after` | `DEMO_ADMIT_AFTER` | 1 |
| `--lab` | `DEMO_LAB` | off |

`DATABASE_URL` is required. `DEMO_SCALE` defaults to 1. The writer and truth pools
have 2 and 4 connections respectively. Saturated HTTP blocking admission returns
503; it does not create an unbounded queue. API inputs and body sizes are capped.

## HTTP interface

- `GET /tickets/{id}`, `/tickets/{id}/comments`, `/orgs/{org}/queue`,
  `/orgs/{org}/agents`, `/api/organizations`
- `POST /tickets`, `/tickets/{id}/status`, `/tickets/{id}/comments` with optional
  `?wait=true`; a committed write reports its follow-up observation separately
- `GET /api/query/{name}`, `POST /api/query` with a serialized `Lookup`;
  `?route=postgres` forces the ordinary path and `?adapter=async` selects async
- `GET /api/compare/{name}`, `/api/lookups`, `/api/metrics`
- `GET /api/events` and `/api/verify/stream` are SSE feeds; missed samples are
  signaled, and the metrics snapshot includes the recent bounded activity list
- `POST /api/verify` runs one pass; `/api/verify/control` starts/stops continuous
  checks; `GET /api/verify` reads totals
- `GET/POST /api/writer`, `POST /api/load`, and guarded `POST /api/lab/{scenario}`

Writes report `committed`, `caught_up`, `wait_error` and `observation_error`
separately. A failed post-commit cache barrier still returns the committed row
ID; it must not invite a duplicate insert. A missing referenced organization or
ticket is a client error. Browser comment drafts stay with their ticket while
switching selections (up to 100 drafts in memory; a page reload clears them).

## Code layout

`queries.rs` owns the Diesel query constructors used by both the sync and async
paths. `asynchronous.rs` dispatches the same shapes through `diesel-async`.
The service modules separate HTTP and write validation (`service/mod.rs`),
connection setup (`service/pools.rs`), bounded metrics (`service/telemetry.rs`),
and verification, load and recovery experiments (`service/lab.rs`). Browser
assets live in `service/assets` and are embedded into the binary.

Reads use tracked pools so connection/session checks remain attached to the
connection that executes the query. Writes and comparison reads use separate
plain pools. A cache refusal follows the ordinary Postgres execution path;
responses report the actual answerer and refusal reason. The async adapter
accepts owned `Send` boxed Diesel queries without requiring their erased
fragments to implement `Sync`.

## Validate

```sh
cargo clippy --manifest-path examples/ticket-desk/Cargo.toml --all-targets --locked -- -D warnings
TICKETDESK_TEST_DSN=postgresql://postgres:postgres@localhost:5432/ticketdesk_tests \
  cargo test --manifest-path examples/ticket-desk/Cargo.toml --locked
```

The HTTP scenario uses a random port and resets its disposable fixture. It checks
all expected sync shape verdicts, async hits, forced fallback, write visibility,
verification under writes, load, all eight lab experiments, and SSE shutdown.
The example is an independent Cargo workspace so its service dependencies do
not enter normal library builds.

The container smoke test mutates a disposable Compose fixture. In addition to
HTTP checks, the optional runtime arguments verify non-root execution, SIGTERM
with an open activity stream, replication-slot cleanup, and row/comment
persistence across application and PostgreSQL restarts:

```sh
python3 examples/ticket-desk/scripts/container-smoke.py http://127.0.0.1:18080 \
  --runtime podman --app ticketdesk-review_demo_1 \
  --postgres ticketdesk-review_postgres_1
```

Use the names/IDs from your own Compose project. CI runs this against the actual
Dockerfile image with Docker Compose and removes its test volume afterward.

## Visual identity

The demo uses coral `#ff4f4f`,
white and charcoal surfaces, fine grid lines, Google Sans Flex headings,
Manrope body text and Geist Mono labels. Semantic
success and failure colors remain distinct. Dark mode uses the same brand values
with readable surfaces and text contrast.

The unmodified Latin webfont assets are embedded and served locally, with no
font CDN dependency. Font asset sources retrieved on 13 November 2026:

- [Google Sans Flex](https://xyne.ai/_next/static/media/f80b3746a961f2e0.p.31dwzsyi8ahjt.woff2)
- [Manrope](https://xyne.ai/_next/static/media/a343f882a40d2cc9-s.p.0p-wng2nn8o-p.woff2)
- [Geist Mono](https://xyne.ai/_next/static/media/797e433ab948586e-s.p.0r6juujl39pe6.woff2)

Upstream font licenses are in `src/service/assets/fonts`, also available from the
running binary at `/fonts/licenses.txt`.
