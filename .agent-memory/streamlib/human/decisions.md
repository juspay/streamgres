# streamlib v1 — decisions (skim)
Details: ../agent/decisions.md (source of truth; update both together)

## API
```rust
let rt = Streamlib::start(cfg).await?;

// Primary: read (caching is the product)
let r = rt.read(q).await?;                        // hit → cache; miss → Postgres, then cache
let r = rt.read(q).idle(d).await?;                // custom idle time
let r = rt.read(q).idle(ZERO).await?;             // one-shot, no cache
let r = rt.read(q).cached().await?;               // cache only, else Err

// Advanced: own the cache
let w = rt.watch(q).await?;                       // pinned cache (Watch)
let v = w.view()?;                                // current view, no query parse
let sub = rt.watch(q).on_event(h).await?;         // with handler (Subscription)
let sub = w.on_event(h);                          // attach later; consumes w
w.unwatch().await;                                // or drop(w)
sub.stop().await;                                 // or return Flow::Stop

rt.shutdown(grace).await;
```

## Cache
- One cache per query, shared by reads and watches
- Same query = same table, filter, order, limit, values
- Kept alive by: an idle timer (reads) or a handle (watch)
- Freed when the timer expires and no handle is left

## Read
- Read is the main API; it caches by default
- Miss → returns Postgres result now, builds cache in background
- Idle time resets on every read; busy queries never expire
- Default idle time is set once in config
- Many idle values on one cache → longest wins

## Watch
- Use it to: pre-warm, never evict, read hot paths, get events
- Watch (no handler): yours; drop = unwatch
- Subscription (handler): library-owned; runs until Flow::Stop, stop(), or shutdown
- Unwatch/stop consume the handle → use-after-unwatch won't compile
- unwatch()/stop() return only after the handler has finished
- Subscribe once (startup/event), not per request

## Memory cap
- Runtime-wide cap in config
- Full → evict least-recently-used caches held only by a timer
- Caches held by a handle are never evicted
- Only handles left → read() still works (no caching), watch() returns Err(CacheFull)

## Events
- Snapshot(rows): initial view; can fire again after restart/resync
- Changes(net changes + view): one per txn
- Change = Entered | Changed | Left ("Left" ≠ deleted in DB)
- Stopped: only when the library stops it, never on your own stop

## Handlers
- Async; return () or Flow::Stop
- One at a time per subscription, in commit order, off the engine thread
- No retries; slow handler → pending changes coalesce

## Queries
- Diesel only, SELECT *, single table
- Unsupported queries fail at compile time where possible, else Err

## Dev must know
- Every replica fires side effects → make handlers idempotent
- Restart = re-sync; changes during downtime are not replayed
- First read of a query hits Postgres; pre-warm with watch if that matters
- Hot path: hold a Watch and call view(); read() parses the query each call
- Testing: in-memory harness, no Postgres needed

## v2
- health(), changed()
- Joins, raw SQL, read-your-writes, every-commit delivery

## Still open
- Embedded vs sidecar, setup as migration, observability, config API

## Why this way?
1. **What is a runtime?** Everything streamlib runs for one database: 1 replication slot, feed thread, engine, reads pool, watch layer (caches), handler runtime. Starts and shuts down together. One per database per process.
2. **Why `rt.read(q)` caches by default?** Caching is our moat. Devs just read; the library decides what stays cached.
3. **Why keep watch?** Some devs need to own the lifetime: pre-warm, pin, hot path, events.
4. **Why idle time, not a fixed TTL?** A busy query should never expire. Only unused caches go.
5. **Why `rt.watch(q)` and not a Diesel trait?** Methods on `rt` stay ORM-agnostic. A Diesel trait would be 2 lines of sugar.
6. **Why two handle types?** Drop behaviour is visible in the type. A Watch cleans up on drop. A handler runs until you stop it.
7. **Why no idle time on watch?** The handle already sets the lifetime. A timer could evict the cache while you hold it, and view() would fail. Want expiry? Use read.
8. **Why doesn't read return a handle?** read means nothing to manage. A handle would bring back drop questions, and on a miss the cache isn't ready yet. Need a handle? Call watch(q); it reuses the same cache.
9. **Compile-time safety:** no use after unwatch, no double unwatch, no struct/table mismatch, no non-Send handler.

---
Above: API. Below: making streamlib extensible.

# Extensibility — DataSource

## Rule
- Engine stays private and unchanged
- Everything else plugs in at the edges

## Is the engine generic? Yes
- Nothing in ivm/ or model/ depends on Postgres
- DNF = plain boolean logic, evaluated in memory
- Tables/rows/PK = a data model, not Postgres
- Engine has its own Value types; sources map into them
- Lsn is just a u64 → rename to a generic Position
- Real coupling is semantics: ordering, NULL rules, PK, full row images

## Components
1. Catalog: tables, primary keys, column types
2. Codec: native values → engine values
3. Snapshots: consistent reads + position
4. Changes: ordered txns + position, resume, ack
5. Position: one ordered type shared by 3 and 4
6. Optional: DDL/schema changes, planner stats (count/estimate)

## Your 4 questions, answered
1. **Snapshot:** each read = one point in time, tagged with its position. Not "same snapshot again"; the runtime replays later writes to catch up.
2. **Checkpoint:** that tag is the Position. Ordered, comparable, same type as the stream's.
3. **Stream:** in scope. Contract: ordered txns, resume from a Position, ack. Can't resume → clear error → re-sync.
4. **DDL:** optional capability. Postgres has it; other sources may not.

## How we enforce the contract
- Types: a read without a Position won't compile
- Runtime checks: library wraps every source; rejects positions ahead of stream, below floor, duplicate keys
- Conformance kit: tests every source must pass (snapshot + replay == truth)
- Docs: the guarantee in plain words

## Codec rules
- Each source declares type mappings to engine types
- Unmapped column → refused at catalog load, not at first read
- Snapshot path and stream path must decode a row to identical values
- Else: fake Changed events or missed matches (kit tests this)

## Snapshots and Changes are separate traits
- Joined only by the same Position type
- Allows: Postgres + Postgres (today), Postgres + Debezium/Kafka, Memory + test stream

## Postgres + Debezium/Kafka rules
- Use the LSN inside each Debezium event, not the Kafka offset
- Kafka offset = adapter's own resume/ack only
- Need one partition or Debezium txn metadata for commit order
- Deletes need only the key (no REPLICA IDENTITY FULL needed for deletes)

## Sources must meet
- Rows with a primary key
- Values mapped to engine Value types
- Changes as insert/update/delete (deletes: key only; updates may be partial)
- Ordering by engine rules (for ORDER BY + LIMIT)
- No consistent snapshot or stream (e.g. HTTP) → snapshot-diff adapter

## Who writes what
- We write the hard, general parts. Source devs write only what's specific to them.
- We ship: Postgres source, snapshot-diff adapter, Debezium/Kafka (later), Checked wrapper, conformance kit

## Three levels for source devs
1. **Snapshot-only (easy):** one "fetch all rows" method + codec. Diff adapter does the rest. For HTTP APIs.
2. **Full source (expert):** Snapshots + Changes + codec, shared Position, pass the kit.
3. **Mix and match:** existing Snapshots + existing Changes, same Position type.
- Snapshot-only still needs a primary key per row and a poll interval
- Diff adapter's Position = its own poll counter

## Timing
- Traits behind an `unstable` feature flag at first
- Stabilise only after a second real source works


## half baked thoughts
1. Publication creation, slot cleanups, snapshot minting, database read request, change feed (replication slot), now how do we define all of these?
 Ans : All of these steps will go in the lifecycle of the dataSource, and the minting will be the internal code of postgres datasource for giving the snapshots.

2. 

## Partial rows (Postgres TOAST)
- Postgres skips big unchanged values in updates
- Decoder marks those columns absent (not NULL)
- Engine fills them from the row it holds
- Holds nothing → re-reads the row by key (Snapshots::select)
- Contract: Changes may send partial updates; source declares it
- Codec maps "not sent" markers to absent, never a value
- Debezium sends a placeholder string here → must map to absent
- Handlers and caches only ever see full rows

## Known bugs (also in streamgres today; found by reading code, not yet tested)
1. **Filter on a skipped column hides the row.**
   An update skips a big column. We don't hold the row. The filter uses that column.
   The filter sees "missing" as no match, so we never re-read. The row never shows up.
   Fix: missing column in a filter = unknown → re-read.
2. **Primary key change loses the big column.**
   A PK change becomes delete old key + insert new key.
   The insert is missing the big column. We look it up under the new key and find nothing.
   Fix: fill it from the old key's row before the delete, or re-read.
