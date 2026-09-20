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
| `XYNE_SYNC_PG_DSN` | the application's PostgreSQL, the primary, reached directly (no PgBouncer: logical replication does not pass through it). The same value the reference server has as its upstream database |
| `XYNE_SYNC_QUERY_URL` | the backend's query endpoint, `http://<backend>:3001/api/sync/query`, as the reference server has it |
| `XYNE_SYNC_MUTATE_URL` | the backend's push endpoint, `http://<backend>:3001/api/sync/push` |
| `XYNE_SYNC_APP_ID` | the same app id the reference server runs with (default: see `src/client/config.rs`); with the shard it names the schema (`<app>_<shard>`) the backend records mutation ids in |

### Set for this rollout

| variable | value | default | why |
|---|---|---|---|
| `XYNE_SYNC_READ_ROW_LIMIT` | `20000` | `100000` | the most rows one storage read may return; a query past it is refused by name. 20 000 is what the test campaigns ran as production policy |
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
| `XYNE_SYNC_SLOT` | `xyne_sync` | the permanent replication slot; the publication is `<slot>_pub` |
| `XYNE_SYNC_FORWARD_COOKIES` | `true` | the connection's cookies go to the backend's endpoints |
| `XYNE_SYNC_HEARTBEAT_MS` | `1000` | the feed's heartbeat |
| `XYNE_SYNC_SNAPSHOT_ROTATION_MS` | `1000` | how often a fresh read snapshot is minted |
| `XYNE_SYNC_JOIN_LIMIT` | `100000` | the most rows the planner lets one side of a join read |
| `XYNE_SYNC_JOIN_PREFERRED_SIDE` | `parent` | which side of an inner join drives when both fit |
| `XYNE_SYNC_PLAN_TTL_MS`, `XYNE_SYNC_PLAN_CACHE` | `600000`, `10000` | join plans remembered |
| `XYNE_SYNC_TRANSFORM_TTL_MS`, `XYNE_SYNC_TRANSFORM_CACHE` | `60000`, `20000` | the backend's query transforms remembered per identity; the reference server keeps its own for 5 s, which cost a hydration about 8 ms at the median on the test rig |
| `XYNE_SYNC_WARM_START_MS` | `20000` | the most time spent planning the kept shapes at start |
| `XYNE_SYNC_PING_INTERVAL_MS`, `XYNE_SYNC_CLIENT_TIMEOUT_MS`, `XYNE_SYNC_PONG_INTERVAL_MS` | `30000`, `45000`, `3000` | liveness of a connection |
| `XYNE_SYNC_MAX_MESSAGE_BYTES` | `16777216` | the largest inbound message |
| `XYNE_SYNC_ROWS_PER_PART` | `500` | row operations per poke part |
| `XYNE_SYNC_LOG` | `info` | `error`, `warn`, `info`, `debug` |
| `XYNE_SYNC_SLOW_QUERY_MS` | `1000` | a hydration or a push slower than this is logged at warn |
| `XYNE_SYNC_METRICS_INTERVAL_MS` | `10000` | the sampler's period |
| the other `OTEL_*` | | `docs/observability.md` section 5 |

## 4. PostgreSQL

- `wal_level = logical`, and room for this server beside the reference server:
  `max_replication_slots` and `max_wal_senders` with eight to spare (one
  permanent slot, and short-lived ones, `xyne_sync_snap_*`, behind the read
  snapshots), about 40 connections (`XYNE_SYNC_READ_CONNECTIONS` plus the
  feed and a handful).
- The role needs `REPLICATION`, `SELECT` on the served schemas, and at the
  first start the right to `CREATE PUBLICATION xyne_sync_pub FOR ALL
  TABLES`, which only a superuser has; on a managed database create that
  publication once by hand and the server finds it.
- Tables without a primary key are left out, as are columns of types the
  wire cannot carry (`bytea`); the startup log lists both.
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
| the feed is behind or silent | p99 of `xyne_sync_feed_lag_seconds` above 5 s; `xyne_sync_feed_heartbeat_age_seconds > 30` |
| memory | `xyne_sync_process_rss_bytes > 6e9` |
| clients built for another database | `increase(xyne_sync_connections_total{event="refused"}[5m]) > 0` |

## 6. Probes, ports, shutdown

- `GET /health` (also `/healthz`, `/readyz`, and each under the base
  path): `503` until the change feed's first heartbeat and the warm start,
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
  ids to `<app>_<shard>.clients` as it does for the reference server.

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
  it.
- One replica, state in memory, no TLS to PostgreSQL (sections 2 and 4).
