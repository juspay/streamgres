# Deploying xyne-sync

For whoever runs the first deployment: what to run, with how much, every
setting with its default, what PostgreSQL has to allow, how traffic gets to
it, and what to watch. The target is the first rollout: about 300 connected
clients, 100 row updates a second, 20 newly opened queries a second.

## 1. The image

`.github/workflows/docker-publish.yml` builds `docker/server/Dockerfile` on
every push to `main` and `feat/build` and on a `v*` tag, and pushes to the
GitHub Container Registry:

```
ghcr.io/juspay/xyne-sync:<branch>        e.g. ghcr.io/juspay/xyne-sync:main
ghcr.io/juspay/xyne-sync:<short sha>
```

It is a Debian slim image of about 125 MB with one binary, `/app/server`,
run as the unprivileged user `xyne-sync` (uid 10001). It listens on 4848,
writes JSON log lines to stderr, and needs no file system beyond an
optional state directory (section 3, `XYNE_SYNC_PLAN_FILE`).

## 2. Resources

| | request | limit |
|---|---|---|
| CPU | 2 cores | 4 cores, or none |
| memory | 4 GiB | 8 GiB |
| replicas | 1 | 1 |

Where the numbers come from, on the production-shaped test database:

- 300 connections, each holding 12 queries and opening a new one every
  0.75 s (about 300 new queries a second, fifteen times this rollout's
  rate), used 0.6 of a core on average and 1.3 to 1.6 GB of memory, with a
  median hydration of about 15 ms.
- 100 updates a second cost the engine well under a tenth of a core; the
  fan-out to the clients that hold the rows is the larger part and stays
  under a third of a core at this size.
- The two cores are for the moments, not the average: a restart makes
  every client hydrate everything again at once, and the engine is one
  thread that must never wait for a core. A CPU limit that throttles shows
  up directly as latency, so set it generously or not at all.
- Memory is the rows clients hold, about 2 KB a row, shared between
  clients that hold the same row. A client group's rows stay for
  `XYNE_SYNC_GROUP_TTL_MS` after its last tab closes (an hour below), so
  the footprint follows the clients seen in the last hour, not the ones
  connected now. Alert at 6 GB.

**One replica, `strategy: Recreate`.** The server's state is in memory and
it owns one PostgreSQL replication slot; two instances cannot share the
slot, and a second one on its own slot would need every tab of a client
group routed to the same instance, which nothing does yet. A restart is
safe and costs a full re-sync: clients reconnect, are told their cookie is
unknown, drop their local store and hydrate from nothing. Deploy off-peak.

## 3. Settings

Everything is an environment variable. **Must set** has no default.
**Set for this rollout** differs from the default on purpose. The rest are
listed with their defaults so the manifest can carry them explicitly.

### Must set

| variable | value |
|---|---|
| `XYNE_SYNC_PG_DSN` | the application's PostgreSQL, reached directly (no PgBouncer: logical replication does not pass through it). The primary, as the reference server's upstream database is, or a replica of it, logical or physical: the server writes nothing to the database it follows (section 4) |
| `XYNE_SYNC_QUERY_URL` | the backend's query endpoint, `http://<backend>:3001/api/sync/query`, as the reference server has it |
| `XYNE_SYNC_MUTATE_URL` | the backend's push endpoint, `http://<backend>:3001/api/sync/push` |
| `XYNE_SYNC_APP_ID` | the same app id the reference server runs with (default: see `src/client/config.rs`); with the shard it names the schema (`<app>_<shard>`) the backend records mutation ids in |

### Set for this rollout

| variable | value | default | why |
|---|---|---|---|
| `XYNE_SYNC_ROW_LIMIT` | `20000` | `100000` | the most rows the server reads into memory at once, for the planner and the storage alike: a join side past it is driven from the other side or the query refused by name, and a storage read past it is refused. One number, so the planner never reads whole what the storage would refuse (the older `XYNE_SYNC_JOIN_LIMIT` and `XYNE_SYNC_READ_ROW_LIMIT` are still read). 20 000 is what the test campaigns ran as production policy |
| `XYNE_SYNC_WHOLE_PAGE_LIMIT` | `5000` | `5000` | the most rows a page that drives an inner join is read whole for (the limit applied in memory, the join's restriction exact); past it the page is read in batches that double per round, ten rounds at most, the rows the join rejects dropped |
| `XYNE_SYNC_GROUP_THREADS` | `2` | `1` | threads that build and send pokes |
| `XYNE_SYNC_READ_THREADS` | `4` | `2` | threads that run and decode storage reads |
| `XYNE_SYNC_READ_CONNECTIONS` | `32` | `16` | PostgreSQL connections for storage reads |
| `XYNE_SYNC_GROUP_TTL_MS` | `3600000` | `60000` | a client back within the hour resumes from its cookie instead of downloading everything again |
| `XYNE_SYNC_LOG_FORMAT` | `json` | `text` | already set in the image |
| `XYNE_SYNC_PLAN_FILE` | `/var/lib/xyne-sync/plans.json` | unset | the query shapes seen, planned again at start so the first clients after a restart do not pay the planner's counts. Needs a writable volume at `/var/lib/xyne-sync`; leave unset without one |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | the collector the reference server pushes to, `http://<collector>:4318` | unset | section 5 |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/json` | `http/json` | |
| `OTEL_METRICS_EXPORTER` | `otlp` | | |
| `OTEL_LOGS_EXPORTER` | `none` (or `otlp` to have logs in the collector too) | | |
| `OTEL_TRACES_EXPORTER` | `none` | | |
| `OTEL_METRIC_EXPORT_INTERVAL` | `5000` | `60000` | |
| `OTEL_SERVICE_NAME` | `xyne-sync` | `xyne-sync` | |
| `OTEL_RESOURCE_ATTRIBUTES` | `deployment.environment=sandbox` | none | |

### Defaults, for the record

| variable | default | meaning |
|---|---|---|
| `XYNE_SYNC_ADDR` | `0.0.0.0:4848` | listen address |
| `XYNE_SYNC_BASE_PATH` | `/sync` | the path prefix clients connect under (section 6) |
| `XYNE_SYNC_SCHEMAS` | `public,<app>_<shard>` | schemas whose tables are served |
| `XYNE_SYNC_SHARD` | `0` | |
| `XYNE_SYNC_PUBLICATION` | `xyne_sync_pub` | the publication the change feed streams; created `FOR ALL TABLES` at start when missing (section 4). The replication slot is not configurable: each process uses its own, `xyne_sync_slot_<uuid>` |
| `XYNE_SYNC_DDL_TRIGGER` | `<app>_ddl_end_<shard>` (`xyne_ddl_end_0` for app `xyne`) | the reference server's event trigger on `ddl_command_end`, through which schema changes are heard (section 4); the server refuses to start without it |
| `XYNE_SYNC_DDL_PREFIX` | `<app>/<shard>/ddl` (`xyne/0/ddl` for app `xyne`) | the prefix of that trigger's logical messages |
| `XYNE_SYNC_FORWARD_COOKIES` | `true` | the connection's cookies go to the backend's endpoints |
| `XYNE_SYNC_READ_TIMEOUT_MS` | `10000` | how long one storage read may take before PostgreSQL is told to cancel it and the query that needed it is refused by name (`reason="read_timeout"`); `0` sets no limit |
| `XYNE_SYNC_PG_KEEPALIVE_IDLE_MS`, `XYNE_SYNC_PG_KEEPALIVE_INTERVAL_MS`, `XYNE_SYNC_PG_KEEPALIVE_RETRIES` | `30000`, `10000`, `3` | TCP keepalive on every connection to the database: after the idle time without a byte either way the kernel probes the peer, again at the interval while unanswered, and gives the connection up after that many unanswered in a row. Keep the idle time under whatever a NAT or load balancer between drops silent flows at (section 4); `0` turns the probing off |
| `XYNE_SYNC_BACKEND_TIMEOUT_MS` | `30000` | how long one call to the backend (a transform, a push) may take; a call that timed out is not made again; `0` sets no limit |
| `XYNE_SYNC_SNAPSHOT_ROTATION_MS` | `1000` | how often a fresh read snapshot is minted |
| `XYNE_SYNC_JOIN_PREFERRED_SIDE` | `parent` | which side of an inner join drives when reading the node whole and having its subs drive it would hold the same rows (the plan holding fewer wins otherwise; the subs drive when they cut the node down by more than they cost, the leaves of an access rule driving the page they narrow) |
| `XYNE_SYNC_PLAN_TTL_MS`, `XYNE_SYNC_PLAN_CACHE` | `600000`, `10000` | join plans remembered |
| `XYNE_SYNC_PLAN_QUERY_TTL_MS` | `86400000` | how long a plan made for a query is laid onto every later query of the same name (and join skeleton), whatever its arguments, before the name is counted again; `0` plans every tree on its own |
| `XYNE_SYNC_TRANSFORM_TTL_MS`, `XYNE_SYNC_TRANSFORM_CACHE` | `60000`, `20000` | the backend's query transforms remembered per identity; the reference server keeps its own for 5 s, which cost a hydration about 8 ms at the median on the test rig |
| `XYNE_SYNC_WARM_START_MS` | `20000` | the most time spent planning the kept shapes at start |
| `XYNE_SYNC_PING_INTERVAL_MS`, `XYNE_SYNC_CLIENT_TIMEOUT_MS`, `XYNE_SYNC_PONG_INTERVAL_MS` | `30000`, `45000`, `3000` | liveness of a connection |
| `XYNE_SYNC_GROUP_LOG_BYTES` | `262144` | bytes of its most recent pokes a client group keeps, so a tab that returns behind its group is sent what it missed instead of starting over; `0` keeps none |
| `XYNE_SYNC_MAX_MESSAGE_BYTES` | `16777216` | the largest inbound message |
| `XYNE_SYNC_ROWS_PER_PART` | `500` | row operations per poke part |
| `XYNE_SYNC_LOG` | `info` | `error`, `warn`, `info`, `debug` |
| `XYNE_SYNC_SLOW_QUERY_MS` | `1000` | a hydration or a push slower than this is logged at warn |
| `XYNE_SYNC_METRICS_INTERVAL_MS` | `10000` | the sampler's period |
| the other `OTEL_*` | | `docs/observability.md` section 5 |

## 4. PostgreSQL

- `wal_level = logical`, and room for this server beside the reference server:
  `max_replication_slots` and `max_wal_senders` with eight to spare (one
  slot per running server, and short-lived ones, `xyne_sync_snap_*`, behind the read
  snapshots), about 40 connections (`XYNE_SYNC_READ_CONNECTIONS` plus the
  feed and a handful).
- The role needs `REPLICATION` and `SELECT` on the served schemas.
- **The server writes nothing to the database it follows.** It reads rows,
  and it reads the change feed, whose position comes from what PostgreSQL
  sends anyway (each commit, and the keepalives that say how far the log
  has been gone through). So `XYNE_SYNC_PG_DSN` may name the primary, a
  logical replica, or a physical standby (PostgreSQL 16 or later, which is
  when a standby learned logical decoding).
- **The publication is created only when it is missing.** At start the
  server looks for the publication `XYNE_SYNC_PUBLICATION` names
  (`xyne_sync_pub` by default) and uses it as it is. To keep creation in
  your own hands, run once, on the primary:

  ```sql
  CREATE PUBLICATION xyne_sync_pub FOR ALL TABLES;
  ```

  `CREATE PUBLICATION ... FOR ALL TABLES` needs a superuser, and a standby
  cannot run it at all (it reaches the standby through replication); a
  server started against a standby without it stops and says which
  statement to run on the primary.
- **Each server process has a slot of its own**, `xyne_sync_slot_<uuid>`,
  created at start on the server `XYNE_SYNC_PG_DSN` names (the standby
  itself, when it is one). A restart needs nothing from the slot before it,
  because the slot is moved up to the first read snapshot anyway. **The
  server never drops a slot**, so every restart leaves the previous
  process's slot behind, inactive and holding the log. Drop the inactive
  `xyne_sync_slot_*` slots from outside the server, for example with a
  scheduled job:

  ```sql
  SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
   WHERE slot_name LIKE 'xyne\_sync\_slot\_%' AND NOT active;
  ```

  A slot is briefly inactive while its process reconnects. A process
  whose slot disappears while it runs stops rather than make the slot
  again, which would skip the changes in between; its restart begins on a
  fresh slot and snapshot.
- On a physical standby a read snapshot (a temporary slot,
  `xyne_sync_snap_*`) waits for the primary's next running-transactions
  record, which a busy primary writes every 15 s; reads keep using the
  snapshot they have until the next one is ready, so this costs memory for
  the writes kept in between and no correctness. `SELECT
  pg_log_standby_snapshot()` on the primary (PostgreSQL 16) forces one.
  Set `hot_standby_feedback = on` on the standby so the primary's vacuum
  does not cancel the snapshots.
- **The connection behind each read snapshot is silent for its whole
  life.** Nothing may be sent on it, since any command discards the
  snapshot, so TCP keepalive is all that keeps it through a NAT or a load
  balancer (`XYNE_SYNC_PG_KEEPALIVE_*`, section 3), from both ends: the
  session asks PostgreSQL to probe it with the same values. Its session is
  idle in a transaction the whole time, and it turns
  `idle_in_transaction_session_timeout` off for itself at connect (a
  setting any role may make for its own session), since that timeout
  would end the session at its interval, the snapshot with it, and reads
  would fail with `snapshot "…" does not exist` until the next one is
  minted. Nothing else on the database side may end an idle session
  short of a restart, a failover, or a recovery conflict on a standby
  without `hot_standby_feedback`.
- Tables without a primary key are left out, as are columns of types the
  wire cannot carry (`bytea`); the startup log lists both.
- **Schema changes are heard through the reference server's DDL event trigger**, the
  one the reference server installs on the upstream database for its app and shard
  (`<app>_ddl_end_<shard>`, `xyne_ddl_end_0` for app `xyne`, shard 0; `XYNE_SYNC_DDL_TRIGGER`
  and `XYNE_SYNC_DDL_PREFIX` name it and its messages' prefix). It writes
  the published schema before and after every migration into the WAL, in
  the migration's own transaction, so the server learns of the change at
  its commit and on any topology (the message replays on a standby like
  any WAL). **The server refuses to start without the trigger** (the log
  says so). On a database the reference server has never run against, install the
  stack it would have installed: `cargo run --example ddl_triggers --
  <app> <shard> <publication>... | psql "$DSN"` on the primary.
  - **A table created** (with a primary key) and **a column added with no
    default or a constant one** (`DEFAULT 'x'`, `DEFAULT 0`, `DEFAULT true`,
    `DEFAULT '{}'::jsonb`) are followed while the server runs: the rows the
    engine holds get the column in memory, the read snapshot that has the
    change is minted at once, and the new shape is served from that
    snapshot on. No client is sent anything for the change; a client whose
    schema names the new column is admitted once the catalog has it.
  - **Anything else** on a served table (a column dropped or renamed, a
    type changed, a key changed, a table dropped or renamed, a column added
    with an expression default such as `now()` or `gen_random_uuid()`)
    stops the server with one error line naming the change; restarted, it
    loads the schema as it is then and every client starts over. Plan such
    migrations as a restart.
  - At every start the slot is moved up to the first read snapshot's point
    (`pg_replication_slot_advance`, on a standby too), so nothing the
    snapshot already holds is streamed and a migration the server stopped
    on is not met again.
- **Connections to PostgreSQL are not encrypted.** If the database
  enforces TLS, reach it through a local proxy (the Cloud SQL proxy, a
  sidecar) until TLS is added here.
- **The slot holds WAL while the server is down.** Set
  `max_slot_wal_keep_size` on the database, and when the server is retired
  drop its slot: `SELECT pg_drop_replication_slot('xyne_sync');`.

## 5. Metrics, logs, alerts

As the reference server: metrics are pushed over OTLP/HTTP to the collector named by
`OTEL_EXPORTER_OTLP_ENDPOINT`, every `OTEL_METRIC_EXPORT_INTERVAL`, by a
thread of its own; logs are JSON lines on stderr for the platform's log
collection, and also OTLP log records when `OTEL_LOGS_EXPORTER=otlp`. The
same metrics are at `GET /metrics` in Prometheus format for a scrape, and
`GET /stats` is the JSON the load tools read, with the refused and the
heavy queries by name. `docs/observability.md` is the catalogue: section 3
the metrics, section 4 the log events, section 5 alert rules to start
from. The ones to have on day one:

| what | rule |
|---|---|
| a query is within a fifth of the row limit | `xyne_sync_read_rows_max / xyne_sync_read_row_limit >= 0.8`; `xyne_sync_query_read_rows_max{name,table}` and the `heavy read` log event name the query |
| a query was refused | `increase(xyne_sync_queries_refused_total[5m]) > 0`; `/stats.refused_queries` and the `query refused` event name it |
| the engine is running out of its one core | `rate(xyne_sync_engine_busy_seconds_total[5m]) > 0.7` |
| writes reach clients late | p99 of `xyne_sync_end_to_end_seconds` above 0.5 s |
| the feed is behind or silent | p99 of `xyne_sync_feed_lag_seconds` above 5 s; `xyne_sync_feed_heartbeat_age_seconds > 90`. The server handles a dead connection itself: after 10 s of silence it asks PostgreSQL whether a walsender still holds its slot, and reopens the slot when none does, which PostgreSQL decides after `wal_sender_timeout` (60 s by default; keep it set) without hearing from the peer (the `change feed ... giving the connection up` warning). The alert is for when that does not help. On a standby of a primary that writes nothing at all the age grows although nothing is wrong |
| reads are timing out | `increase(xyne_sync_queries_refused_total{reason="read_timeout"}[5m]) > 0`: a storage read ran past `XYNE_SYNC_READ_TIMEOUT_MS`; the `query refused` event names the query and the table |
| memory | `xyne_sync_process_rss_bytes > 6e9` |
| clients built for another database | `increase(xyne_sync_connections_total{event="refused"}[5m]) > 0` |

## 6. Probes, ports, shutdown

- `GET /health` (also `/healthz`, `/readyz`, and each under the base
  path): `503` until the change feed has passed the first read snapshot
  (a fraction of a second) and the warm start,
  `200` after. Use it for readiness and liveness; give startup 60 s.
- One port, 4848: the WebSocket under `<base path>/sync/v51/connect`, and
  `/health`, `/metrics`, `/stats`. Keep `/metrics` and `/stats` inside the
  cluster.
- `SIGTERM` closes the connections over three seconds, so the clients do
  not all reconnect in the same instant, and exits within five more. A
  `terminationGracePeriodSeconds` of 15 is enough.
- The proxy in front must pass WebSocket upgrades and the
  `Sec-WebSocket-Protocol` header (the client's first message travels in
  it, several kilobytes: allow 128 KB of request headers, as
  `--max-http-header-size=131072` does for the reference server), and should not
  close an idle WebSocket before 60 s.

## 7. Getting traffic to it

The dashboard does not read a sync-server address from its environment in a
deployed build: it connects to `https://<the host it was served from>/sync`
(the sync path its build was configured with). The dashboard's sync-server URL build variable is only what the dev
server and the sandbox's proxy forward `/sync` to. So:

- **Everyone at once (cut-over).** Nothing changes in the frontend. Point
  whatever serves `/sync` today at `xyne-sync:4848` instead of
  `ref-server:4848`: the ingress rule for the `/sync` path, or, where the
  backend's WebSocket proxy sits in front, the backend's
  sync upstream setting (`http://xyne-sync:4848`). Going back is the same edit
  reversed. The two servers keep separate state, so a client that moves
  between them starts a fresh sync, on its own.
- **Beside the reference server (a test lane).** Build the dashboard with
  its sync-server path build variable set to `/sync-rs` and its storage-key build variable set to `rs`, route the path
  `/sync-rs` to `xyne-sync:4848`, and run the server with
  `XYNE_SYNC_BASE_PATH=/sync-rs`. The storage key must differ from the main
  bundle's, or the two Zero clients share one local store on the origin
  and wipe each other (`docs/sdlc-fast-lane.md` in xyne-spaces has the
  mechanism; the SDLC lane already runs this way).
- The backend needs nothing new: its query and mutate URL settings
  name its endpoints from the server's side, and it keeps writing mutation
  ids to `<app>_<shard>.clients`, and the results of refused mutations to
  `<app>_<shard>.mutations`, as it does for the reference server. The publication must
  carry both tables; the server cleans up results a client has received
  through the mutate endpoint (the cleanup-results push), so the results
  table no longer grows while this server serves the clients.

## 8. Known limits at this rollout

- Four query shapes are refused, by name, in `/stats.refused_queries` and
  the `query refused` log event: `LIKE`/`ILIKE` conditions
  (`searchChannelParticipants`, `createdOatsRecordings`), `NOT EXISTS`
  (`getSdlcRepoById`), and any read past the row limit
  (`scopedCollectionsWithItems` on large workspaces). The client sees the
  query as errored, the rest of the app is unaffected.
- A client whose schema names a table or column the database does not have
  yet is refused with `SchemaVersionNotSupported` and reloads, as with
  the reference server: migrate the database before shipping the frontend that needs
  it. A table created or a column added with a constant default is picked
  up while the server runs; every other migration of a served table
  restarts it (section 4).
- One replica, state in memory, no TLS to PostgreSQL (sections 2 and 4).
