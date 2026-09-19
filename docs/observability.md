# Observability: what the server measures and logs, and how it stays out of the way

The server serves three kinds of work, and the question for each is the
same: how much of its time is ours (compute), how much is someone else's
(the application server, PostgreSQL, the network), how much work went
through, and what it holds. This document is the catalogue of what is
measured and logged, the way it is recorded without slowing the threads
that do the work, and how to read it.

## 1. The shape

|  | compute (ours) | IO (theirs) | counts | rows |
| --- | --- | --- | --- | --- |
| **select**: transform | – | the application server's round trip | asked, served from cache, errored | – |
| **select**: plan | the decision | the counts on PostgreSQL | from cache, counted, refused | – |
| **select**: IVM | register step, landing step, release step | the read's round trip | reads issued, landed, refused, served by a twin | rows per read, rows held |
| **select**: client | flush, socket write | – | pokes, frames | rows serialized, rows shared |
| **update**: feed | decoding a transaction | commit → arrival (replication lag) | transactions, writes | – |
| **update**: IVM | the engine's step | waiting for the engine (inbox) | subscriptions impacted, narrowed reads | client updates |
| **update**: client | flush, socket write | – | pokes | rows serialized |
| **mutation** | – | the push's round trip; push → acknowledgement | pushes ok / failed | – |
| **memory** | | | connections, groups, clients, subscriptions, trees, caches | rows held per table, RSS |

Every cell is a metric below; the rows of the table are the labels.

## 2. How it is recorded

- **Metrics are atomics.** A histogram is 257 `AtomicU64` buckets (four
  per power of two) plus count, sum and max: one `fetch_add` per sample,
  no lock, no allocation, on whichever thread does the work. Counters are
  one `fetch_add`. Gauges are stores. Nothing on the engine, group, read
  or feed threads waits for anything to be observed.
- **Reading is elsewhere.** `/stats` (JSON, the load harness's format),
  `/metrics` (Prometheus exposition, for scraping) and the periodic
  summary line are rendered on the request task or the metrics thread
  from the atomics; percentiles are computed at read time.
- **Sampling is a thread.** `xyne-sync-metrics` wakes every
  `XYNE_SYNC_METRICS_INTERVAL_MS` (10 s), reads what only sampling can
  give (the process's resident set, each thread's CPU time by name from
  `/proc`, the queues' depths, the caches' sizes) into gauges, and every
  sixth wake-up writes the summary line.
- **Logs are a queue and a thread.** A log call checks its level first
  (a load of one atomic), formats only if enabled, and hands the line to
  a bounded queue; `xyne-sync-log` drains the queue to stderr in
  batches. A full queue drops the line and counts the drop; the count is
  a metric and appears in the summary, so a flood is visible without
  ever blocking the thread that logged. Format is `text` or `json`
  (`XYNE_SYNC_LOG_FORMAT`); every line carries its time, level, the
  thread that wrote it and, for structured events, its fields.

## 3. The metrics

Names as `/metrics` exposes them (`/stats` has the same numbers under the
older names); durations are histograms in seconds with `_bucket`, `_sum`
and `_count`; counts are `_total` counters; the rest are gauges.

### Select

| metric | what |
| --- | --- |
| `xyne_sync_transform_seconds` | one round trip to the application server for a desired-queries change's ASTs (IO) |
| `xyne_sync_transforms_total{result=hit,miss,error}` | queries answered from the transform cache, sent to the application server, errored there |
| `xyne_sync_plan_seconds` | translating and planning one query (a cache hit is microseconds) |
| `xyne_sync_count_seconds` | one planner count on PostgreSQL (IO) |
| `xyne_sync_hydrate_seconds{kind=cold,warm}` | registration sent → rows present, per query; cold read storage, warm was served from held frames |
| `xyne_sync_engine_step_seconds{step=register,land,unregister}` | the engine's own compute per step |
| `xyne_sync_read_seconds` | a storage read from issue to its rows back on the engine thread (queue + PostgreSQL + decode) |
| `xyne_sync_read_rows` | rows per storage read (histogram over counts) |
| `xyne_sync_reads_total{outcome=issued,landed,refused,shared}` | storage reads, and registrations served from a twin without one |
| `xyne_sync_subscriptions`, `xyne_sync_trees` | what the engine holds |

### Update

| metric | what |
| --- | --- |
| `xyne_sync_feed_decode_seconds` | the feed thread decoding one transaction (compute) |
| `xyne_sync_feed_lag_seconds` | PostgreSQL's commit time → the engine taking the transaction up (replication + queue) |
| `xyne_sync_feed_to_engine_seconds` | decoded → taken up by the engine (the inbox's wait) |
| `xyne_sync_engine_step_seconds{step=write}` | routing one transaction |
| `xyne_sync_engine_to_groups_seconds`, `xyne_sync_groups_flush_seconds`, `xyne_sync_groups_to_socket_seconds`, `xyne_sync_end_to_end_seconds` | the client side's stages and the whole path inside the server |
| `xyne_sync_feed_transactions_total`, `xyne_sync_feed_writes_total` | what came through the feed |
| `xyne_sync_writes_impacting_total`, `xyne_sync_client_updates_total{op=add,delete}`, `xyne_sync_narrowed_reads_total` | routing counters |
| `xyne_sync_pokes_total`, `xyne_sync_frames_total`, `xyne_sync_rows_serialized_total`, `xyne_sync_rows_shared_total` | the client side's output |
| `xyne_sync_feed_lsn`, `xyne_sync_feed_heartbeat_age_seconds` | where the feed is and how long since it last spoke |
| `xyne_sync_engine_inbox`, `xyne_sync_groups_inbox{shard}` | queue depths, sampled |

### Mutation

| metric | what |
| --- | --- |
| `xyne_sync_push_seconds` | the application server's push round trip (IO) |
| `xyne_sync_pushes_total{result=ok,failed}` | pushes forwarded and their outcome |
| `xyne_sync_mutation_ack_seconds` | push → the `lastMutationIDChanges` poke that acknowledges it to the client (the commit's way back through PostgreSQL, the feed and the engine) |

### Memory and process

| metric | what |
| --- | --- |
| `xyne_sync_process_rss_bytes` | resident set, sampled |
| `xyne_sync_thread_cpu_seconds_total{thread}` | CPU by thread name (engine, groups, reads, server, feed, reaper, log, metrics), sampled; the engine's rate is its utilisation |
| `xyne_sync_rows_held{table}` | rows in the shared frames per table |
| `xyne_sync_connections_open`, `xyne_sync_connections_total{event=opened,closed}` | sockets |
| `xyne_sync_client_groups`, `xyne_sync_clients` | what the group threads hold |
| `xyne_sync_plan_cache_entries`, `xyne_sync_transform_cache_entries`, `xyne_sync_warm_shapes` | the caches |
| `xyne_sync_log_dropped_total` | lines the log queue refused |
| `xyne_sync_uptime_seconds` | since start |

## 4. The logs

Text: `HH:MM:SS.mmm LEVEL [thread] message key=value ...`. JSON: one
object per line, `{"ts","level","thread","msg", ...fields}`.

| event | level | fields |
| --- | --- | --- |
| server up, feed connected, ready, warm start, drain | info | address, slot, position, shapes planned |
| connection opened / closed | info | wsid, group, client, whether authenticated, origin; on close: seconds open and the close reason (client, error, server) |
| query hydrated | debug | group, name, hash, kind (cold or warm), ms from registration to rows present |
| slow query | warn | the same, when hydration exceeds `XYNE_SYNC_SLOW_QUERY_MS` (1 000) |
| query refused or errored | warn | wsid, name, hash, reason (as today) |
| transaction routed | debug | position, writes, client updates, reads asked, ms |
| feed lag | warn | when commit → engine exceeds 5 s, once per minute |
| push forwarded | debug (warn when failed or slower than the slow threshold) | wsid, group, mutations, ms, failed |
| summary | info, every minute | connections, groups, clients, subscriptions, rows held, RSS, engine busy %, feed lag, transform and plan hit rates, registrations/s, transactions/s, pokes/s, log drops |

The reference server's equivalents, for whoever reads both: its `slow hydrate`
warnings (its slow-hydrate threshold), its sync, replication and server
OpenTelemetry instruments, and its
JSON log with `component` and `worker` fields map onto the rows above.

## 5. Configuration

| variable | default | meaning |
| --- | --- | --- |
| `XYNE_SYNC_LOG` | `info` | level |
| `XYNE_SYNC_LOG_FORMAT` | `text` | `text` or `json` |
| `XYNE_SYNC_SLOW_QUERY_MS` | `1000` | a query hydrating slower than this is logged at warn |
| `XYNE_SYNC_METRICS_INTERVAL_MS` | `10000` | the sampler's period; the summary line every sixth sample; `0` turns the sampler off |

## 6. What it costs, measured

### Locally (Apple M4 Max, the server over the wire, `scripts/load-sweep.sh`)

The same sweep as the paper's table of 2026-09-18 (200 sync-protocol
clients holding the chat screen's queries, rows committed straight into
PostgreSQL), on the observability build with JSON logs on and the
sampler at its default; raw results in
`paper/load-2026-09-19/sweep-observability/`.

| shape | rows/s | delivery p50 / p99 (ms) | server end to end p50 / p99 (ms) | cores | 2026-09-18: p50 / p99 / server / cores |
| --- | --- | --- | --- | --- | --- |
| one channel | 200 | 22 / 26 | 2.6 / 4.1 | 0.30 | 28 / 35 / 4.1 / 0.38 |
| one channel | 400 | 26 / 31 | 5.1 / 7.2 | 0.50 | 36 / 68 / 7.2 / 0.65 |
| one channel | 800 | 28 / 35 | 6.1 / 8.2 | 0.40 | 30 / 37 / 5.1 / 0.95 (at 40 tx/s) |
| twenty channels | 1 000 | 22 / 26 | 1.5 / 2.0 | 0.17 | 29 / 36 / 2.6 / 0.21 |
| twenty channels | 4 000 | 32 / 37 | 5.1 / 6.1 | 0.51 | 43 / 50 / 7.2 / 0.67 |
| twenty channels | 8 000 | 45 / 56 | 8.2 / 10.2 | 0.98 | 94 / 129 / 20.5 / 1.68 (at 7 400) |

Hydration of 200 / 400 / 800 clients: 136 / 211 / 408 ms at the median.
Nothing got slower; the day's engine fixes made most of it faster. The
800-row one-channel run delivered two thirds of its rows inside the
window for the same reason as on the rig: the single Node driver parsing
160 000 rows a second, with the server's own path at 6 ms.

During the sweep the log wrote 1 840 connection-opened and 1 632
connection-closed lines (all closed by the client), zero slow-query
warnings, and dropped nothing; the gauges read 208 sockets open (200
subscribers and 8 writers), 208 groups, 492 MB resident.

### On the rig

PENDING.

## 7. What it costs, by construction

Nothing on the hot paths that was not already there: the histograms
existed; the new counters are one atomic add each at points that already
did more (a read's rows were already counted, a push already awaited);
the log calls that are new are at debug (checked and skipped at info)
or at events that happen once per connection. The measurement that
proves it is the one every change here is held to: the bench, the local
load sweep and the rig's ladder before and after (section 7 of
`prod-scale-2026-09-19.md` has the before).
