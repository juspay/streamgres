# streamlib — Decisions (v1)
Human summary: ../human/decisions.md — update it in the same edit.

## Goal
In-process runtime that keeps Postgres query results current (IVM over logical replication). Caching is the product: dev reads a query → served from a live cache. Advanced: pin a cache with `watch`, or get net changes with `on_change`. State convergence, not CDC.

## Runtime
- One `Runtime` = everything for one database: 1 replication slot, feed thread, engine thread, reads runtime, watch-layer thread (caches), handler runtime. Starts and shuts down as a unit.
- One runtime per database per process: `start` returns `Err` if one already exists for that DB (a second one only adds a slot + decoder, no gain). Multiple runtimes for different databases are fine.
- `Runtime` is `Clone + Send + Sync` (store in app state).
- Methods hang off `rt` (`rt.watch(q)`), not a Diesel extension trait (`q.watch(&rt)`): ORM-agnostic (more ORMs / own ORM later), discoverable, no trait import. A Diesel trait, if wanted, is ~2 lines of sugar on top.

## Architecture (threads)
```
feed thread ─txns→ engine thread ─Event→ watch-layer thread ─mailbox→ handler runtime
(slot, pgoutput)   (Service+IVM,          (registry, ledgers,          (task per subscription)
                    LocalSet, !Send)       decode→T, netting)
                        │ fetches
                   reads runtime (hydration / window refill)
```
| Part | Streamgres today | streamlib |
|---|---|---|
| Feed | `sync::pg::threads::spawn_feed` | reuse |
| Engine | `threads::start` → `sync::Service` | reuse, one consumer sink |
| Reads | `streamgres-reads` multi-thread runtime | reuse |
| Watch layer | `client/groups.rs` (group threads) | new, 1 thread in v1 (shard later) |
| Handler runtime | — | new, own small multi-thread tokio runtime (option to pass host handle later) |
- Start order: reads → engine (starts feed) → watch layer → handler runtime. Shutdown reverse.
- Watch layer owns: registry (query key ↔ `SubId` ↔ refcount ↔ subscriptions), validation, per-cache row ledger (key → image; needed because engine emits only Add/Delete), decode `DataFrameRow → T`, per-txn netting, ORDER BY + LIMIT trim, snapshot gating (buffer `Landed`/`Committed` until `Hydrated`), `max_lag`.
- **LIMIT trim:** engine holds a 2·L doubled buffer (`src/ivm/window.rs`); Zero client trims today. Watch layer must sort by ORDER BY, keep top L, compute Entered/Left against top L.
- Handler runtime: per-subscription mailbox (pending slot + running flag); while running, new Changes merge into pending (coalesce). Each call wrapped in `catch_unwind`.
- Needs change in core for library use: `Shutdown::fail` / `log.rs:175` `process::exit` → runtime "failed" state + `Stopped` to all subscriptions. Global allocator, pyroscope, thread naming out of core.

## API surface (v1 — complete)
```rust
Streamlib::start(cfg).await?                     -> Runtime
rt.read(q).await?                                -> Arc<[T]>         // hit → cache; miss → Postgres + start cache
rt.read(q).idle(d).await?                        -> Arc<[T]>         // custom idle time
rt.read(q).idle(ZERO).await?                     -> Arc<[T]>         // one-shot, never creates a cache
rt.read(q).cached().await?                       -> Arc<[T]>         // cache, else Err(NotCached)
rt.watch(q).await?                               -> Watch<T>         // pinned cache, RAII
rt.on_change(q, h).await?                        -> Subscription<T>  // handler, library-owned
rt.shutdown(grace).await
w.view()?            -> Arc<[T]>
w.unwatch().await    // or drop(w)
sub.view()?          -> Arc<[T]>
sub.stop().await
handler returns () or Flow::{Continue, Stop}
```
- `T` inferred from `q.select(T::as_select())`; `T: Selectable<Pg>` → struct/table mismatch is a compile error. `T: Send + Sync + 'static`.
- `watch()` returns after hydration; `on_change()` returns after the handler's `Snapshot` call has run.
- All async methods are cancel-safe: dropping a `watch`/`on_change`/`read` future mid-hydration cleans up its registration.

## Shared cache per query
- One cache per query, shared by reads, `Watch`es and `Subscription`s. Any of them reuses an existing cache (no rehydration).
- Query identity = parsed form (table, filter, order, limit, bind values), not SQL text. Semantically equal but differently written queries may get separate caches (documented).
- Cache stores raw rows; each reader/watch decodes into its own `T` (same query, different structs OK).
- Late subscriber gets `Snapshot` from the cache immediately (no Postgres read), then `Changes`.

## Cache lifetime (holders)
- Two kinds of holder: idle timer (from reads) and handles (`Watch`, `Subscription`).
- Cache lives while a handle exists OR the idle timer has not expired.
- Freed when the timer has expired AND handle count = 0.
- Idle = time since last read. Every read resets it. Not a fixed TTL: busy queries never expire.
- Default idle time: one runtime-wide value in `cfg`. `.idle(d)` overrides per call.
- Several idle values on one cache → longest wins (timer = max of requested).
- `.idle(ZERO)` → one-shot: served from cache if one exists, else Postgres. Never creates a cache, never resets a timer.
- Replaces earlier "read never creates a cache" and "idle in v2".

## Read vs watch boundary
- Rule: read = library owns the lifetime (idle timer). watch = dev owns it (handle).
- No idle time on `watch`/`Subscription`:
  - The handle already sets the lifetime; a timer would be a second owner.
  - Eviction while held → `view()` fails on its own. A handle must not go invalid by itself.
  - A handler needs the cache to keep getting events; eviction would stop it silently.
  - Linger after drop = call `rt.read(q).idle(d)` once; after drop the cache falls back to its timer.
- `read` returns rows only, never a handle:
  - read exists so there is nothing to manage.
  - RAII handle → every read would build and tear down a cache. Inert handle → useless.
  - On a miss the cache is still hydrating; the handle would point at nothing ready.
  - Need a handle → `rt.watch(q)`; it reuses the read's cache, no rehydration.

## Memory cap
- Runtime-wide cap in `cfg` (`max_cached_rows`; exact unit decided with config API).
- Full on new cache from read → evict least-recently-read caches held only by a timer.
- Caches with a handle are never evicted.
- Nothing evictable → `read` still returns the Postgres result, just doesn't cache; `watch` → `Err(CacheFull)`.

## Watch<T> vs Subscription<T> (ownership)
- `watch` is the advanced path. Use it to: pre-warm at startup, pin (never evict), hot path (`view()` skips the query parse).
- A `Watch` pins: no idle timer while it is held. After drop, the cache falls back to its idle timer (if any read set one).

| | `Watch<T>` | `Subscription<T>` |
|---|---|---|
| Created by | `rt.watch(q)` | `rt.on_change(q, h)` |
| Owned by | dev code | library |
| Drop | unwatches (RAII) | nothing; handler keeps running |
| Ends via | drop / `unwatch().await` | `Flow::Stop`, `sub.stop().await`, shutdown, library stop |
- Rationale: drop behaviour is in the type. Watch-without-handler can't leak (RAII). A handler is almost always meant to run until a deliberate stop, so no `.detach()` ceremony.
- Both: `#[must_use]` on `Watch`, not `Clone`, `Send + Sync`. Unwatch/stop consume `self` → use-after-unwatch and double unwatch are compile errors.
- One handler per subscription. To change it: stop, subscribe again. (No replace-in-place, no `off()`.)
- Caveat: `rt.on_change(q, h)` per request without `Flow::Stop` adds a handler each time (shared cache, but each fires side effects). Subscribe once (startup / business event).
- `rt.watch(q).await?.view()?` drops the temporary `Watch` at once. Not harmful now: use `rt.read(q)` for that. Bind the watch only if you need to pin.

## on_change (decided 2026-11-20)
- One call for handlers: `rt.on_change(q, h)`. Reuses the query's cache if one exists, else creates it. No separate watch step.
- Same semantics as before: cache is shared per query, so this is ergonomics only.
- Removed: `rt.watch(q).on_event(h)` and `w.on_event(h)`. One concept per call: read = rows, watch = pin, on_change = handler.
- Holding a `Watch` and wanting events → call `rt.on_change(q, h)`; it hits the same cache.
- First call gets `Snapshot` (current rows), then `Changes`. Name kept as `on_change` because it says what devs care about.
- Returns a builder you `.await`, so options can be added without breaking callers (e.g. `.max_lag(d)`).

## Stop / unwatch semantics
- `unwatch().await` / `stop().await` return only when no handler call is running and none will run → safe to tear down handler resources. Drop of `Watch` = fire-and-forget.
- `Flow::Stop` from handler: ends that subscription after the current call; no `Stopped` event. Any held `Subscription` becomes inert (`view()` → `Err(Stopped)`, `stop()` no-op).
- Deadlock guard: `stop()`/`unwatch()` called from inside its own handler (would wait on itself) is detected via task-local → marks stopped, returns without waiting. `Flow::Stop` is the documented way.
- `Stopped` event only for library-initiated stops (DDL, refused read, `max_lag`). Never on unwatch/stop/`Flow::Stop`/shutdown.
- `view()` returns `Result` because a library stop can happen while the handle is held; never serve stale data silently.

## Reads
- `read(q)` is the primary API. Read-through by default.
- Hit (hydrated, live cache) → return view, reset idle timer.
- Miss → query Postgres via library read pool, return that result now. Start hydration in background (engine does its own consistent snapshot; read result is not used to seed the cache).
- Concurrent misses on one query → one hydration (dedupe in watch layer). Each miss still gets its own Postgres read.
- Cache hydrating → treat as miss (Postgres).
- `.cached()`: cache only; hydrating or missing → `Err(NotCached)`. Async (cache lives on watch-layer thread). Resets idle timer on hit.
- Returns `Arc<[T]>`: a hit hands out the shared view, no copy.
- A miss can be fresher than a cache (cache can lag ms). Both are valid committed states.
- Hot path: `rt.read(q)` costs render → parse → hash per call (~tens of µs); `w.view()` is an atomic load. Hot code holds a `Watch`.
- First read of a query always hits Postgres. Pre-warm with `watch` if that matters.
- Read-your-writes (`at_least(lsn)`) — NOT in v1.

## Events
```rust
enum Event<T> {
    Snapshot(Arc<[T]>),                                   // initial view; may re-fire (restart/resync) → treat as replace
    Changes { changes: Vec<Change<T>>, view: Arc<[T]> },  // one per txn or coalesced batch; view = state after
    Stopped(StopReason),                                  // library-initiated only
}
enum Change<T> { Entered(T), Changed { old: T, new: T }, Left(T) }
enum Flow { Continue, Stop }   // handler output; `()` = Continue (via trait impl for () and Flow)
```
- `Left` = left the view (window/filter), not necessarily deleted in DB. No reason in v1.
- Merge per txn, net per key: Entered+Left → nothing; Left+Entered new image → Changed; same image → nothing.

## Handler execution
- Async: `F: Fn(Event<T>) -> Fut + Send + Sync + 'static`, `Fut: Future<Output = O> + Send`, `O: HandlerOutput` (`()` or `Flow`). Deps captured as `Arc` clones.
- Runs on handler runtime, never engine thread. Sequential per subscription, commit order. No ordering across subscriptions.
- No retry/error policy. Panics caught + logged.
- Slow handler → coalesce (backlog bounded by view size, no cancellation). Optional `max_lag` → `Stopped(Lagging)`, off by default.

## Shutdown / lifecycle
- `rt.shutdown(grace)`: idempotent, any clone. Stop new calls → in-flight handlers get `grace`, then dropped → watch layer → engine → feed → reads.
- After: `watch`/`read` → `Err(Closed)`, `view()` → `Err(Stopped)`. No `Stopped` event (host asked).
- Last `Runtime` clone dropped without `shutdown` → best-effort stop + warn log.
- Restart = full re-sync (`Snapshot` re-fires). Downtime changes not replayed.

## Queries
- Diesel only, no raw SQL. Sealed trait over supported nodes + `#[diagnostic::on_unimplemented]`; pin Diesel major.
- Runtime checks (table not in publication, no PK, planner refusal) → `watch()` `Err`.
- `SELECT *` only, single table only (joins via `ivm::MultiTableIVM` later).
- Bind extraction: `to_sql` + `RawBytesBindCollector<Pg>` → binary-format decoder needed (spike first).

## Testing
- `streamlib::testing`: same `Runtime` API on `sync::Local` + `MemoryStorage`, plus `apply(write)` to simulate a commit. No Postgres.

## Documented caveats
- Every replica fires side effects → handlers idempotent.
- Snapshot re-fires; downtime changes not replayed.
- Convergence, not an event log — not a CDC/outbox replacement.

## v2
- `rt.health()` (feed connected, lag, fatal error), `w.changed().await` (pull-based wait).
- Joins, raw SQL, read-your-writes, `Delivery::EveryCommit`.

## Open
- Embedded vs sidecar transport (slot per pod).
- Setup (publication, DDL trigger) as migration step, not at start.
- Observability: per-watch lag, handler duration.
- Init/config API (`cfg` shape).

---
# Extensibility — DataSource

## Principle
- Engine (`src/ivm`, `src/model`) stays private and unchanged. All extension is at the edges.
- Design the contract now. Stabilise traits only after a second real (non-Postgres) stream source works.

## Engine is generic (verified)
- No Postgres references in `src/ivm/` or `src/model/`.
- DNF = boolean logic over predicates, evaluated in memory (`ivm::evaluate`).
- Tables / rows / PK = relational data model, source-agnostic.
- `model::Value` (Null, String, Int, Float, Bool, Date, Datetime, List, Map, Set) is the engine's own type set. pg layer maps into it.
- IVM consumes `WriteQuery` (insert/update/delete). Producer-agnostic.
- `Lsn(u64)` (`model/position.rs:14`): only parsing is Postgres-specific. Generalise to `Position`.
- Real coupling = semantics a source must match:
  - Ordering/collation: for `ORDER BY + LIMIT`, `select` must return the same top rows the engine would pick.
  - NULL + comparison rules (SQL-style; Int/Float coerce). Sources map into these.
  - Primary key required. No key → no IVM.
  - Full row images on inserts. Updates may be partial (see Partial rows). Deletes need only the key.

## Today's state
- Read side is already a trait: `Storage` (`sync/storage.rs:62`), impls `PgStorage`, `MemoryStorage`.
- Stream side is not: engine takes `mpsc::Receiver<Transaction>` (`service.rs:320`); decoding in `pg/stream.rs`; feed thread is Postgres-specific.

## Components
1. Catalog: tables, PKs, column types.
2. Codec: native values → engine `Value` (one per path, identical output).
3. Snapshots: consistent reads + position.
4. Changes: ordered txns + position, resume, ack.
5. Position: shared ordered type tying 3 and 4.
6. Optional capabilities: DDL/schema changes, planner stats (`count`/`estimate`), snapshot minting ahead.

## Core invariant
- Snapshot at P + every change after P == current state. No gaps, no duplicates.
- Each read = ONE consistent point in time, tagged with that point. Not "repeatable snapshot": runtime replays delivered writes above P to bring it up (`floor`).
- Read position must be ≤ last `advance` and ≥ `floor`.

## Contract shape (illustrative, not compiled)
```rust
pub trait Position: Ord + Clone + Debug + 'static {}
pub struct Snapshot<P: Position> { pub rows: Vec<(Key, Row)>, pub position: P }

trait Snapshots {                       // futures need not be Send (engine single-threaded)
    type Position: Position;
    async fn select(&self, q: &Query) -> Result<Snapshot<Self::Position>, SourceError>;
    fn advance(&self, upto: Self::Position);
    fn floor(&self) -> Self::Position;
}
trait Changes {
    type Position: Position;
    fn stream(&self, from: Self::Position) -> Stream<Txn<Self::Position>>;  // ordered, monotonic
    fn ack(&self, upto: Self::Position);                                    // source may drop history
}
// DataSource = one Snapshots + one Changes with the SAME Position type
```
- `stream(from)` must fail clearly when it can't resume (slot dropped, retention expired) → runtime re-syncs. Never silent loss.

## Enforcement (3 layers + docs)
1. Types: `select` returns `Snapshot { rows, position }` → can't forget position. Associated `Position` shared → can't pair wrong stream.
2. Library wrapper `Checked<S>` around every source: rejects position > advanced, position < floor, duplicate keys, position going backwards → `SourceError::contract`.
3. Conformance kit `streamlib::testing::conformance(src)`: write while reading; assert snapshot + replayed changes == truth. Also codec parity (below).
4. Docs: "each read is one point in time, tagged with its position".
- Source author's real job: read from one snapshot, report that snapshot's position.

## Codec
- Separate component. Each source declares native type → engine `ValueType` mappings.
- Unmapped column → refused at catalog load, not first read.
- Critical: snapshot path and stream path must decode the same row to identical values (numeric precision, timestamp tz, JSON normalisation). Mismatch → fake `Changed` or missed matches. Kit tests this.

## Split Snapshots / Changes
- Combinations: Postgres + Postgres replication (today); Postgres snapshots + Debezium/Kafka; Memory + test stream.
- Postgres + Debezium/Kafka:
  - One position space: adapter uses the LSN inside each Debezium event. Kafka offset only for adapter's own resume/ack.
  - Ordering: Kafka orders per partition only. Need single partition or Debezium txn metadata to rebuild whole txns in commit order.
  - Deletes: key only is enough (`DeleteQuery` has only `pkey_value`, `model/query.rs:328`). No `REPLICA IDENTITY FULL` needed for deletes.

## Sources that can't meet the contract
- No consistent snapshot / no change stream (most HTTP APIs) → snapshot-diff adapter: poll full result, diff vs last poll, emit changes. Cost: poll latency + diff work. Contract stays strict.

## Open
- Exact `Query` type exposed to sources (engine's `SingleTableReadQuery` or a narrower one).
- Collation declaration vs forcing engine ordering.
- Build order: Position → Changes trait → merge with `Storage` → snapshot-diff adapter as first non-Postgres proof.

## Ownership: who writes what
- Principle: devs write only what only they know (fetch from their source, map its types). Library supplies the correctness machinery.
- First-party (we ship): Postgres `Snapshots` + `Changes` (today's code); generic snapshot-diff adapter; Debezium/Kafka adapter later (position space, commit order, delete images are too subtle to leave to users); `Checked` wrapper; conformance kit.

## Extension levels
1. Snapshot-only (easy): implement one "fetch all rows" method + codec. Diff adapter turns it into a full source.
   - Requires a PK per row; dev sets a poll interval.
   - Adapter mints its own Position = poll counter (monotonic u64). Changes = diff of poll N vs N-1.
   - Cost: poll latency + full fetch + diff. Fine for small/slow-changing data.
2. Full source (expert): `Snapshots` + `Changes` + codec, shared `Position`, must pass conformance kit.
3. Mix and match: compose an existing `Snapshots` with an existing `Changes` (e.g. Postgres snapshots + custom stream) when `Position` types match. Compile error otherwise.

## Stability
- Source traits exposed behind `unstable` cargo feature. External devs can try them; we keep freedom to change the contract.
- Stabilise after a second real (non-Postgres) source works end to end.

## Partial rows (Postgres TOAST)
- Postgres sends `UnchangedToastedValue` for large values an UPDATE did not touch. No value in the WAL.
- Today (verified):
  1. Decoder leaves the column out (absent, not NULL); row marked `RowData::partial` (`pg/stream.rs:1156`, `model/row.rs:111`).
  2. Engine fills from the held image: `complete_image` (`ivm/mod.rs:660`). No Postgres call. Common case.
  3. No held image → `read_row` by PK (`ivm/frames.rs:96`); row reaches subscribers when the read lands.
  4. Reads being brought up fill partial updates from the read's image (`rows_completed_total`).
  5. Stat `partial_rows_sent` > 0 = bug (a partial row reached a client).
- Engine is generic here: sees "absent columns", never "TOAST".
- Contract clause: `Changes` may emit partial UPDATE images (absent columns). Source declares the capability. Inserts must be full.
- Codec: map the source's "not sent" marker to absent. Debezium sends `__debezium_unavailable_value` → must map to absent, never a String. Kit tests this.
- Snapshots: must support read by PK (`select` with key filter). Nothing new.
- Boundary rule: watch layer, caches, and handlers only ever receive full rows. `T` decode requires every column.

## Partial rows — edge cases (to verify / design)
1. **PK change + partial.** UPDATE that changes PK is split into Delete(old) + Insert(new) (`pg/stream.rs:355`). Unchanged TOAST columns arrive absent → a partial INSERT. Frame lookup by new key finds nothing; the image is under the old key. Must complete from old key's image before the delete, or re-read.
2. **Filter / ORDER BY uses the absent column. LIKELY BUG (also in streamgres today).** Re-read is triggered only for subs `analyze` says match (`ivm/mod.rs:371`). Predicates treat a missing column as `false` (`ivm/predicate.rs:5`), and `IS NULL` as true. So with no held image, `WHERE big_col IS NOT NULL` / `= 'x'` → no match → no re-read → row silently missing from the view. Fix: a predicate touching an absent column = "unknown" → re-read. Needs a test first.
3. **Writes arriving while the re-read is in flight.** Later updates/deletes to the same key must apply after the read lands, in order. A later partial update must complete from the read's image.
4. **Re-read races with insert/delete.** Read snapshot ≤ stream: the row may be newer than the snapshot (insert after P) or deleted after it. Correct only if delivered writes above P are kept (`floor`) and replayed.
5. **Re-read fails / times out / refused.** Row stuck; that key's later changes blocked; view lags. Refused → `Stopped` to affected subscriptions. Need a retry policy and a lag signal.
6. **Eviction makes re-reads common.** Idle eviction / memory cap drop held images. Next partial update for those rows → no image → re-read. Hot tables with big values → bursts of large reads. Batch re-reads by key; count them (`row_reads_total`).
7. **Re-read cost.** The column is TOASTed because it's big. Re-reads fetch MBs per update under load. Consider reading only the missing columns.
8. **Schema change between held image and update.** Column added/dropped after the image was stored. `complete_image` must not mix layouts; DDL path must align held images first.
9. **Partial INSERT from a buggy source.** Contract says inserts are full. `Checked` wrapper rejects partial inserts (except case 1, which is internal).
10. **Snapshot-diff adapter and memory source.** Must never emit partials (they have full rows). Checked enforces.
11. **Placeholder leaks.** Any codec that forgets the marker (Debezium placeholder, a future source's sentinel) silently stores a fake value. Codec parity test: same row via snapshot vs stream must be identical.
12. **TRUNCATE (related gap).** Postgres decoder errors on TRUNCATE (`pg/stream.rs:395`). Contract must say: Changes may emit Truncate → engine resets the table (or source must refuse). Today: unsupported.

## Known bugs (streamgres core; found by code reading, not yet tested)
1. **Filter on an absent column drops the row.**
   - Path: partial UPDATE, no held image (`old_data.is_none()`), sub filters on the absent column.
   - `analyze` decides matches before the re-read (`ivm/mod.rs:371`). Predicates treat a missing column as `false`, `IS NULL` as true (`ivm/predicate.rs:5`).
   - Result: `= 'x'` / `IS NOT NULL` on the absent column → no impact → no `read_row` → row silently missing.
   - Fix: a predicate touching an absent column evaluates to "unknown"; unknown → `read_row` for that sub.
   - Next: failing test in `ivm`.
2. **PK-changing UPDATE + unchanged TOAST → partial INSERT.**
   - Path: decoder splits into `DELETE(old_key)` + `INSERT(new_key)` (`pg/stream.rs:355`); the new image may be partial.
   - `complete_image` looks up the frame by new key → nothing. The full image sits under old key and is deleted first.
   - Result: partial row inserted, or missing from the view (also hits bug 1).
   - Fix: complete the new image from the old key's image before the delete (decoder or engine), else `read_row` by new key.
   - Next: failing test with a TOASTed column + PK update.
