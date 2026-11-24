# Deploying Streamgres

This guide covers running the sync server in production:
- what it needs from PostgreSQL and from your application server
- how to size it
- every setting and its default
- how to route traffic to it, and what to monitor

For a local first run, see the README's Quick start.

## 1. What you need

- **PostgreSQL** with `wal_level = logical`. It can be a primary, a logical
  replica, or a physical standby on PostgreSQL 17+. Section 3 has the details.
- **An application server** with two HTTP endpoints. The *query* endpoint turns
  a query name and its arguments into a query AST. The *mutate* endpoint
  applies a client's mutations. Section 4 has the details.
- **A host or container** with about 2 cores and 4 GiB of memory to start.
  Section 2 has the sizing.

## 2. Image and sizing

### The image

```bash
docker build -f docker/server/Dockerfile -t streamgres .
```

The image runs on Google's distroless `cc-debian13` (built on
`rust:1.89-trixie`). The runtime has no shell, no package manager and no
coreutils:
- It has one binary, `/app/server`, run as the unprivileged user `nonroot`
  (uid 65532).
- It listens on port 4848 and writes JSON log lines to stderr.
- It has no Docker `HEALTHCHECK` (there is no curl to run one): probe
  `/healthz` and `/readyz` over HTTP from the platform instead, which
  answer 503 until the server is ready (section 6).
- It needs no file system, except an optional writable directory for
  `STREAMGRES_PLAN_FILE` (section 5), and a writable `/tmp` for heap
  profiles when Pyroscope is on and the root file system is read-only.

`.github/workflows/docker-publish.yml` publishes the image to the GitHub
Container Registry as `ghcr.io/<owner>/streamgres`. It runs on pushes to the
branches listed in that file and on `v*` tags, tagging each image with the
branch name and the short commit SHA.

### Sizing

| | starting point |
|---|---|
| CPU | 2 cores requested; limit 4 or none |
| memory | 4 GiB requested, 8 GiB limit |
| replicas | 1 |

- **CPU.** The engine runs on one thread that should never wait for a core.
  Load comes in bursts: after a restart, every client re-downloads its data at
  once. A CPU limit that throttles shows up directly as latency, so set it
  generously or not at all. In tests, 300 connections each holding 12 queries
  and opening a new one every 0.75 s used about 0.6 of a core on average.
- **Memory.** It grows with the rows clients hold: about 2 KB per row. Clients
  holding the same row share one copy. A client group's rows are kept for
  `STREAMGRES_GROUP_TTL_MS` after its last connection closes, so memory tracks
  the clients seen within that window, not just the ones connected now.
- **One replica.** State lives in memory. Every tab of one client group has to
  reach the same instance, which nothing routes for yet, so run a single
  replica and replace it on deploys rather than rolling (in Kubernetes,
  `strategy: Recreate`).
- **Restarts are safe but cost a full re-sync.** Clients reconnect, are told
  to start over, and download their data again. Deploy at quiet times.

## 3. PostgreSQL

### Requirements

- **`wal_level = logical`.**
- **Spare replication capacity.** Leave about eight free slots in
  `max_replication_slots` and `max_wal_senders`. Each server uses one slot
  for its change feed, `xyne_sync_slot_<uuid>`, plus short-lived ones,
  `xyne_sync_snap_*`, behind its read snapshots.
- **Spare connections.** Without PgBouncer, allow about
  `STREAMGRES_READ_CONNECTIONS` direct read connections, plus the feed and a
  few snapshot-minting connections. With PgBouncer, size PostgreSQL's read
  connection budget for PgBouncer's server pool instead.
- **A role** with `REPLICATION` and `SELECT` on the served schemas.
- **A direct connection for replication.** `STREAMGRES_PG_DSN` connects
  directly to PostgreSQL for the logical feed and snapshot minting.
  Replication must not use the read pool.

The change feed writes nothing to the database it follows. Its position comes
from commits and PostgreSQL keepalives. `STREAMGRES_PG_DSN` can point at:
- the primary,
- a logical replica, or
- a physical standby (PostgreSQL 16 or later, the first version that can do
  logical decoding on a standby).

Ordinary SQL reads can optionally use a separate endpoint:
`STREAMGRES_READ_PG_DSN` may point to PgBouncer configured for transaction
pooling. It defaults to `STREAMGRES_PG_DSN`, preserving direct reads when
unset. The PgBouncer database must resolve to the same PostgreSQL database
and server as the direct DSN: each read imports a snapshot minted over the
direct connection. Reads run in one `BEGIN`/`COMMIT` transaction, so the
PgBouncer backend remains assigned through snapshot import and query. Keep
the feed and snapshot minting on the direct DSN. `STREAMGRES_READ_CONNECTIONS`
limits concurrent reads sent toward PgBouncer; `STREAMGRES_READ_IDLE_CONNECTIONS`
limits how many Streamgres-to-PgBouncer client connections remain open while
idle. PgBouncer's own pool settings control the PostgreSQL backend connections.

### Startup setup

`STREAMGRES_MASTER_PG_DSN` connects to the writable primary. On first startup,
Streamgres creates the `<app>_<shard>` mutation schema, publication, DDL
functions, and event triggers. Existing objects are validated without
replacement; mismatches stop startup. The master role must be a PostgreSQL
superuser to create the publication and event triggers. Choose
`STREAMGRES_PUBLICATION` and `STREAMGRES_SCHEMAS` for the publication scope.
Trigger names and the WAL message prefix come from `STREAMGRES_APP_ID` and
`STREAMGRES_SHARD`; the triggers record the published schema before and after
DDL in that transaction's WAL.

### Replication slots

- Each process creates a fresh permanent slot before its first snapshot and feed
  opening. Graceful shutdown stops the feed and attempts to drop its slot.
- Enabled instances await an initial sweep before creating their slot, within
  `STREAMGRES_SLOT_CLEANUP_STARTUP_TIMEOUT_MS` (default 10000, positive milliseconds).
  This total budget includes connection, lock waiting, discovery and deletion. Startup
  waits for a busy sweeper lock within that budget; timeout/error warns and initialization
  continues. Shutdown cancels the wait. A guarded background task then handles periodic
  cleanup, starting after the first configured interval; failed startup/shutdown cancels it.
  Before scanning, each periodic attempt tries
  the session advisory lock derived from `STREAMGRES_SLOT_SWEEP_LOCK_KEY`
  (default `streamgres.slot_cleanup`); a busy attempt skips. The holder
  retains it for the sweep and releases it afterward. This serializes cooperating
  cleaners in the same database on the same PostgreSQL server, not across servers.
  PostgreSQL hashes the bound string with `hashtextextended(key, 0)` into a 64-bit lock
  key. Every cooperating instance must use the same exact nonblank string. Different
  names do not coordinate; changing the name does not change the candidate scope.
  Unset/empty environment values use the default; whitespace-only and NUL values fail
  validation. Leading/trailing whitespace in otherwise nonblank keys is significant.
- `STREAMGRES_SLOT_CLEANUP_AGE_MS` sets the inactivity grace period (default 600000,
  ten minutes). Zero means immediate eligibility, NOT disabled cleanup. Supply the
  same value to all instances; the library does not enforce deployment consistency.
  SQL discovery and deletion bind the configured duration and compare PostgreSQL's
  `inactive_since` with the database clock. Invalidity does not bypass this threshold.
  No custom PostgreSQL settings or server-side WAL/idle policy are required or set.
- Candidates are matching permanent logical slots in the current database, excluding
  active, temporary, synced, and the instance's own slot. Deletion rechecks all these
  filters and the inactivity age. Active/missing errors are harmless. PostgreSQL
  arbitrates deletion against active use, but the inactivity-age check is not atomic
  with deletion: a reconnect/disconnect can occur between the check and slot acquisition.
- `STREAMGRES_SLOT_SWEEP_INTERVAL_MS` sets a positive interval (default 60000).
  Each wait uses exactly that interval, measured after the previous attempt completes;
  slow sweeps do not cause catch-up bursts. Integer milliseconds support subsecond through
  long intervals. `STREAMGRES_SLOT_CLEANUP_ENABLED=false` disables both startup and
  periodic cleanup. Shutdown and failed startup cancel the background task.
- PG17+ tracks `inactive_since` automatically. No tracking feature must be enabled.
  Each sweep uses one short-lived SQL connection for acquiring the shared lock,
  scanning candidates, and deleting them. Normal SQL errors retain session ownership
  and continue. Fatal/closed connections and client deadlines discard the session;
  any replacement must acquire the same lock before continuing. If busy, that attempt
  ends and periodic cleanup retries later. No cleanup connection remains between attempts.
- Attempts are bounded individually, not as a whole list: 5 seconds to connect,
  5 seconds per SQL statement, 1 second for SQL lock waits, and a 30-second client
  deadline per candidate/metadata operation. Candidate errors log and continue;
  discovery errors end the attempt. Server deadlines bound already submitted statements.
  Network partitions can delay session failure detection and lock release. The cleanup
  DSN must reach PostgreSQL directly or use session pooling, never transaction pooling:
  session advisory locks require the same PostgreSQL backend throughout the sweep.
- Own-slot recovery is separate: a valid disconnected feed reconnects with backoff.
  Confirmed missing/invalid own-slot state stops the standalone server so an external
  restart creates a fresh slot and snapshot. Inspection errors do not establish slot
  loss. Engine-only rebuilding is not implemented by this change.
- The shared sweeper lock does not protect slots during startup or reconnection.
  A finite grace period can be exceeded by slow initialization or an extended outage;
  zero permits deletion during even brief disconnections. Periodic cleanup handles
  orphans missed by an early startup scan, but unavailable databases delay cleanup.
  Disabled cleaners or large thresholds can leave orphans retaining resources.
- The shared lock limits concurrent scans, not their total frequency: instances may
  take turns scanning within one interval. Large fleets should use a longer interval
  or enable cleanup on a subset. Final eligibility checks scan the slot view per
  candidate, so cleanup work increases with both slot count and candidate count.
  Initial cleanup adds up to its configured budget to initialization: include that in
  startup-probe budgets. It is best effort; timeout, insufficiently old candidates, or
  DB/permission errors can still leave capacity exhausted and slot creation can fail.
  Already completed drops are not rolled back on timeout. Each waiting startup holds
  one SQL connection, so large rollouts still need enough connection capacity. Slots from separate applications sharing this prefix/database share the cleanup scope; the
  shortest configured age can determine retention. Active lagging slots are never
  reclaimed by this inactivity policy, so it is not a WAL/disk usage cap.

### Physical standbys

- A new read snapshot on a standby waits for the primary's next
  running-transactions record. A busy primary writes one every 15 s;
  `SELECT pg_log_standby_snapshot()` on the primary forces one. Until it
  arrives, reads keep using the current snapshot, so the only cost is memory,
  not correctness.
- Set `hot_standby_feedback = on` on the standby, so the primary's vacuum does
  not cancel the snapshots.

### Networking

- **Read snapshots sit on silent connections.** Each read snapshot lives on a
  connection that must stay idle inside a transaction for its whole life,
  because any command would discard the snapshot.
  - TCP keepalive (`STREAMGRES_PG_KEEPALIVE_*`) keeps these connections through
    NATs and load balancers, so keep its idle time below whatever drops silent
    connections on your network.
  - The server turns off `idle_in_transaction_session_timeout` for its own
    sessions.
- **No TLS to PostgreSQL yet.** If your database requires TLS, connect through
  a local proxy, such as the Cloud SQL proxy or a sidecar.

### What is served

- Tables without a primary key are skipped. So are columns of types the wire
  format cannot carry (`bytea`). The startup log lists both.
- Tables with large TOASTed columns need `REPLICA IDENTITY FULL`.

### Schema changes

Two kinds of change are followed while the server runs:
- **a table created** with a primary key, and
- **a column added** with no default or a constant default (`DEFAULT 'x'`,
  `DEFAULT 0`, `DEFAULT true`, `DEFAULT '{}'::jsonb`).

The rows already in memory get the new column, and the new shape is served
from the next snapshot on. Clients are sent nothing for the change.

**Any other change to a served table stops the server**, with one log line
naming the change. That includes dropping or renaming a column or table,
changing a type or key, and adding a column with an expression default such as
`now()`. Once restarted, the server loads the schema as it then is, and every
client starts over. Plan those migrations as a restart.

A client whose schema names a table or column the database does not have yet
is refused with `SchemaVersionNotSupported`. Migrate the database before
shipping a frontend that needs the change.

## 4. The application server

The server sends the client's requests to your application server:

- **Queries.** A client asks for queries by name and arguments. The server
  posts them to `STREAMGRES_QUERY_URL`, forwarding the connection's cookies
  unless `STREAMGRES_FORWARD_COOKIES=false`, and receives query ASTs back.
- **Mutations.** A client's pushes are forwarded unchanged to
  `STREAMGRES_MUTATE_URL`, with the cookies, plus `schema` and `appID`
  parameters.
- **Recording mutations.** In the same transaction as each mutation, your
  application server records the client's last mutation id in
  `<app>_<shard>.clients`. When it refuses a mutation with an application
  error, it records the result in `<app>_<shard>.mutations`. The publication
  must carry both tables: the server reads them from the change feed and
  sends them to the client together with the mutation's rows.
- **Cleanup.** When a client acknowledges its mutation results, the server
  sends the mutate endpoint a cleanup push, so the results table does not grow
  without bound.

## 5. Configuration

Everything is set by environment variables. A `.env` file in the working
directory is read first, and variables already set take precedence.
[`.env.example`](../.env.example) documents each one.

### Required

| variable | meaning |
|---|---|
| `STREAMGRES_PG_DSN` | direct PostgreSQL endpoint for catalog, feed and snapshot minting (section 3) |
| `STREAMGRES_MASTER_PG_DSN` | writable primary used during startup setup |
| `STREAMGRES_QUERY_URL` | the application server's query endpoint |
| `STREAMGRES_MUTATE_URL` | the application server's mutate endpoint |

`STREAMGRES_READ_PG_DSN` is optional. Set it to the PgBouncer endpoint for
reads; when unset, reads use `STREAMGRES_PG_DSN` directly.

### Set explicitly

These have built-in defaults. Set them anyway, so the deployment does not
depend on what the defaults happen to be:

| variable | example | meaning |
|---|---|---|
| `STREAMGRES_BASE_PATH` | `/sync` | URL path prefix clients connect under. A client's server URL ends in it |
| `STREAMGRES_APP_ID` | `xyne` | app id. With the shard, it names the `<app>_<shard>` schema of the mutation tables, the event trigger and its message prefix |
| `STREAMGRES_SHARD` | `0` | shard number |

### Recommended for production

| variable | suggested | default | why |
|---|---|---|---|
| `STREAMGRES_GROUP_TTL_MS` | `3600000` | `60000` | how long a disconnected client group's state is kept. A client back within it resumes instead of re-downloading everything |
| `STREAMGRES_GROUP_THREADS` | `2` | `1` | threads that build and send updates to clients |
| `STREAMGRES_READ_THREADS` | `4` | `2` | threads that run and decode database reads |
| `STREAMGRES_READ_CONNECTIONS` | `32` | `16` | maximum concurrent storage reads; limits how many requests Streamgres can send to PostgreSQL/PgBouncer at once |
| `STREAMGRES_READ_IDLE_CONNECTIONS` | `16` | `16` | maximum read connections Streamgres keeps idle for reuse; `0` closes them after use. With transaction pooling these are idle PgBouncer client connections, not PostgreSQL backends |
| `STREAMGRES_ROW_LIMIT` | sized to your data | `100000` | the most rows read into memory at once. A larger read is refused, and the query named in logs and metrics |
| `STREAMGRES_PLAN_FILE` | `/var/lib/streamgres/plans.json` | unset | query shapes seen so far, planned again at startup, so the first clients after a restart are fast. Needs a writable volume |
| `STREAMGRES_LOG_FORMAT` | `json` | `text` | already set in the image |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://<collector>:4318` | unset | push metrics (and optionally logs) over OTLP. See [observability](observability.md) |

### All other settings

| variable | default | meaning |
|---|---|---|
| `STREAMGRES_ADDR` | `0.0.0.0:4848` | listen address |
| `STREAMGRES_SCHEMAS` | `public,<app>_<shard>` | schemas whose tables are served |
| `STREAMGRES_PUBLICATION` | `xyne_sync_pub` | the publication the change feed streams |
| `STREAMGRES_FORWARD_COOKIES` | `true` | forward the connection's cookies to the application server |
| `STREAMGRES_SNAPSHOT_ROTATION_MS` | `1000` | how often a fresh read snapshot is created |
| `STREAMGRES_SLOT_SWEEP_LOCK_KEY` | `streamgres.slot_cleanup` | shared string identifying the advisory lock; identical across cooperating instances |
| `STREAMGRES_SLOT_CLEANUP_STARTUP_TIMEOUT_MS` | `10000` | total initial cleanup budget in milliseconds, including lock contention |
| `STREAMGRES_SLOT_SWEEP_INTERVAL_MS` | `60000` | fixed wait in milliseconds after each completed attempt |
| `STREAMGRES_SLOT_CLEANUP_ENABLED` | `true` | enable startup and periodic cleanup on this instance |
| `STREAMGRES_SLOT_CLEANUP_AGE_MS` | `600000` | inactivity threshold in milliseconds; zero means immediate eligibility |
| `STREAMGRES_READ_TIMEOUT_MS` | `10000` | longest a database read may take before it is cancelled and its query refused. `0` means no limit |
| `STREAMGRES_BACKEND_TIMEOUT_MS` | `30000` | longest a call to the application server may take. `0` means no limit |
| `STREAMGRES_PG_KEEPALIVE_IDLE_MS`, `_INTERVAL_MS`, `_RETRIES` | `30000`, `10000`, `3` | TCP keepalive on database connections. `0` idle time turns it off |
| `STREAMGRES_JOIN_PREFERRED_SIDE` | `parent` | which side drives a join when both plans cost the same |
| `STREAMGRES_PLAN_TTL_MS`, `STREAMGRES_PLAN_CACHE` | `600000`, `10000` | how long each query tree's plan (or refusal) is remembered before it is sized again, and how many are kept. A tree is sized from PostgreSQL's estimates (`EXPLAIN` and `pg_stats`, nothing executed; a page without its cursor), and a side that cannot be estimated is counted |
| `STREAMGRES_ESTIMATE_TTL_MS`, `STREAMGRES_ESTIMATE_CACHE` | `86400000`, `300000` | how long the planner's estimates (each `EXPLAIN` by its text, each join column's statistics) are remembered, and how many. A table's estimates are also dropped when its `reltuples`/`relpages` change (an `ANALYZE` or `VACUUM`, read every 5 minutes) and on a schema change; `0` keeps none, but a statement already being asked is still shared |
| `STREAMGRES_TRANSFORM_TTL_MS`, `STREAMGRES_TRANSFORM_CACHE` | `60000`, `20000` | how long query ASTs from the application server are cached per user, and how many |
| `STREAMGRES_WARM_START_MS` | `20000` | most time spent re-planning saved query shapes at startup |
| `STREAMGRES_PING_INTERVAL_MS`, `STREAMGRES_CLIENT_TIMEOUT_MS`, `STREAMGRES_PONG_INTERVAL_MS` | `30000`, `45000`, `3000` | connection liveness |
| `STREAMGRES_GROUP_LOG_BYTES` | `262144` | recent updates kept per client group, so a briefly disconnected tab catches up instead of starting over. `0` keeps none |
| `STREAMGRES_MAX_MESSAGE_BYTES` | `16777216` | largest message accepted from a client |
| `STREAMGRES_BYTES_PER_PART` | `768000` | bytes of serialized row changes per message part (750 KiB) |
| `STREAMGRES_LOG` | `info` | `error`, `warn`, `info` or `debug`. `debug` costs throughput |
| `STREAMGRES_SLOW_QUERY_MS` | `1000` | queries and pushes slower than this are logged at `warn` |
| `STREAMGRES_METRICS_INTERVAL_MS` | `10000` | how often process metrics are sampled |
| `STREAMGRES_METRICS_PREFIX` | `xyne_sync` | prefix for exported metric names. Keep the default for existing Grafana dashboards; set `streamgres` to use `streamgres_*` names and update dashboard queries accordingly |
| other `OTEL_*` | | see [observability](observability.md), section 5 |

## 6. Ports, probes and shutdown

- **One port, 4848.** It serves:
  - the WebSocket at `<base path>/sync/v51/connect`
  - `/health`, `/metrics` and `/stats`

  Keep `/metrics` and `/stats` private.
- **Health.** `GET /health` (also `/healthz`, `/readyz`, and each of them
  under the base path) returns `503` until the server is ready. That means
  the change feed has passed the first read snapshot, and the warm start, if
  configured, has run. After that it returns `200`. Use it for both readiness
  and liveness, and allow 60 s for startup.
- **Shutdown.** On `SIGTERM` the server closes client connections spread over
  three seconds, so clients don't all reconnect at once. It then stops the
  feed and drops its slot. All timed phases fit in 25 seconds, so give it a
  grace period of 30 seconds or more (in Kubernetes,
  `terminationGracePeriodSeconds`).
- **The proxy in front:**
  - must pass WebSocket upgrades;
  - must pass the `Sec-WebSocket-Protocol` header, which carries the client's
    first message and can be several kilobytes, so allow 128 KB of request
    headers;
  - should not close idle WebSockets in under 60 s.

## 7. Routing traffic

Clients connect to `<scheme>://<host>/<base path>`.

- **One sync server per path.** Route the base path, for example `/sync`, to
  the server's port 4848. Switching clients to or from another sync server is a
  routing change. The servers don't share state, so a client that moves
  starts a fresh sync on its own.
- **Two servers side by side.** For example, to test a new deployment:
  - give each server its own base path (`STREAMGRES_BASE_PATH`) and route
    each path to its own server;
  - build the client that uses the second path with a different local storage
    key, so the two don't overwrite each other's local data in the browser.

## 8. Monitoring

Metrics are available two ways:
- pushed over OTLP/HTTP to `OTEL_EXPORTER_OTLP_ENDPOINT`;
- served at `GET /metrics` in Prometheus format.

Logs are JSON lines on stderr. `GET /stats` returns the same figures as JSON,
including refused and heavy queries by name.

[`observability.md`](observability.md) lists every metric, every log event,
and alert rules. Alerts to start with:

| what | rule |
|---|---|
| a query is close to the row limit | `xyne_sync_read_rows_max / xyne_sync_read_row_limit >= 0.8`; `xyne_sync_query_read_rows_max{name,table}` names it |
| a query was refused | `increase(xyne_sync_queries_refused_total[5m]) > 0` |
| the engine thread is nearly saturated | `rate(xyne_sync_engine_busy_seconds_total[5m]) > 0.7` |
| changes reach clients late | p99 of `xyne_sync_end_to_end_seconds` above 0.5 s |
| the feed is behind or silent | p99 of `xyne_sync_feed_lag_seconds` above 5 s, or `xyne_sync_feed_heartbeat_age_seconds > 90` |
| reads are timing out | `increase(xyne_sync_queries_refused_total{reason="read_timeout"}[5m]) > 0` |
| memory | `xyne_sync_process_rss_bytes` above 75 % of the container's limit |
| clients built for a different schema | `increase(xyne_sync_connections_total{event="refused"}[5m]) > 0` |

**Dead connections.** The server recovers a dead feed connection by itself.
After 10 s of silence it asks PostgreSQL whether its slot is still held, and
reopens it if not. PostgreSQL notices a dead peer after `wal_sender_timeout`
(60 s by default; keep it set). The feed alert is for when that doesn't help.
On a standby whose primary writes nothing at all, the heartbeat age grows even
though nothing is wrong.

## 9. Known limits

- **Unsupported queries are refused, not run.** This covers `LIKE`/`ILIKE`,
  `NOT EXISTS`, and any read over `STREAMGRES_ROW_LIMIT`.
  Each one is refused by name, in `/stats.refused_queries` and in the
  `query refused` log event. The client sees that one query fail; the rest of
  the app is unaffected.
- **No history across restarts.** A client that reconnects after a restart
  starts a fresh sync, which drops its unsent mutations.
- **One replica**, with state in memory.
- **No TLS to PostgreSQL** (section 3).
