use super::*;
use crate::migrate::Step;
use diesel::sql_types::{Bool, Integer, Nullable, Text};
use std::collections::{BTreeMap, VecDeque};
use tokio::task::JoinSet;

#[derive(Default, Clone, Serialize)]
pub struct Verification {
    pub adapter: Adapter,
    pub running: bool,
    pub busy: bool,
    pub comparisons: u64,
    pub matches: u64,
    pub mismatches: u64,
    pub live_disagreements: u64,
    pub errors: u64,
    pub lagged: u64,
    pub from_memory: u64,
    pub from_postgres: u64,
    pub last_mismatch: Option<Value>,
    pub last_error: Option<String>,
    pub by_lookup: BTreeMap<String, u64>,
}
#[derive(Serialize)]
pub struct Comparison {
    pub cached: Answer,
    pub postgres: Answer,
    pub equal: bool,
    pub settled: bool,
    pub observed_ms: f64,
}
async fn compare(
    app: &Arc<App>,
    lookup: Lookup,
    adapter: Adapter,
    settle: Duration,
) -> Result<Comparison, ApiError> {
    let started = Instant::now();
    let mut settled = false;
    loop {
        let cached = app
            .read(lookup.clone(), Route::Cached, adapter, false)
            .await?;
        let postgres = app
            .read(lookup.clone(), Route::Postgres, adapter, false)
            .await?;
        let equal = cached.data == postgres.data;
        if equal || started.elapsed() >= settle || app.stop.load(Ordering::Acquire) {
            return Ok(Comparison {
                cached,
                postgres,
                equal,
                settled,
                observed_ms: started.elapsed().as_secs_f64() * 1000.0,
            });
        }
        settled = true;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
pub async fn compare_named(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Comparison>, ApiError> {
    let lookup = Lookup::named(&name)
        .ok_or_else(|| ApiError::NotFound("That query does not exist.".into()))?;
    compare(&app, lookup, options.adapter, Duration::from_secs(2))
        .await
        .map(Json)
}
pub async fn verification(State(app): State<Arc<App>>) -> Json<Verification> {
    Json(lock(&app.verification).clone())
}
#[derive(Deserialize)]
pub struct VerifyControl {
    running: bool,
    #[serde(default)]
    adapter: Adapter,
}
pub async fn verify_control(
    State(app): State<Arc<App>>,
    Json(input): Json<VerifyControl>,
) -> Json<Verification> {
    {
        let mut stats = lock(&app.verification);
        stats.running = input.running;
        stats.adapter = input.adapter;
    }
    app.telemetry.emit(
        "verify",
        if input.running {
            "Continuous correctness checks started.".into()
        } else {
            "Continuous correctness checks paused.".into()
        },
        json!({}),
    );
    Json(lock(&app.verification).clone())
}
async fn pass(app: &Arc<App>, adapter: Adapter) -> Result<(), ApiError> {
    {
        let mut stats = lock(&app.verification);
        if stats.busy {
            return Err(ApiError::Busy);
        }
        stats.busy = true;
    }
    // The pass runs in its own task when invoked over HTTP, so disconnecting
    // the browser cannot strand this flag or cancel its in-flight workers.
    for lookup in Lookup::demo() {
        if app.stop.load(Ordering::Acquire) {
            break;
        }
        let result = compare(app, lookup.clone(), adapter, Duration::from_millis(2500)).await;
        let mut stats = lock(&app.verification);
        match result {
            Ok(result) => {
                stats.comparisons = stats.comparisons.saturating_add(1);
                let covered = stats.by_lookup.entry(lookup.name().into()).or_default();
                if result.cached.served == Served::Cache {
                    *covered = covered.saturating_add(1);
                    stats.from_memory = stats.from_memory.saturating_add(1);
                } else {
                    stats.from_postgres = stats.from_postgres.saturating_add(1);
                }
                if result.settled {
                    stats.lagged = stats.lagged.saturating_add(1);
                }
                if result.equal {
                    stats.matches = stats.matches.saturating_add(1);
                } else if result.cached.served == Served::Postgres {
                    stats.live_disagreements = stats.live_disagreements.saturating_add(1);
                } else {
                    stats.mismatches = stats.mismatches.saturating_add(1);
                    stats.last_mismatch = Some(
                        json!({"lookup": lookup.name(), "cached": result.cached.data, "postgres": result.postgres.data}),
                    );
                    app.telemetry.emit(
                        "mismatch",
                        format!("{} did not agree after the settle window.", lookup.name()),
                        json!({"lookup": lookup.name()}),
                    );
                }
            }
            Err(error) => {
                stats.errors = stats.errors.saturating_add(1);
                stats.last_error = Some(format!("{}: {error:?}", lookup.name()));
            }
        }
    }
    let mut stats = lock(&app.verification);
    stats.busy = false;
    app.telemetry.emit(
        "verify",
        format!(
            "Correctness pass finished: {} comparisons, {} mismatches.",
            stats.comparisons, stats.mismatches
        ),
        json!({"comparisons": stats.comparisons, "mismatches": stats.mismatches}),
    );
    Ok(())
}
pub async fn verify_once(
    State(app): State<Arc<App>>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Verification>, ApiError> {
    let work = app.clone();
    tokio::spawn(async move { pass(&work, options.adapter).await })
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))??;
    Ok(Json(lock(&app.verification).clone()))
}
pub fn background(app: &Arc<App>) {
    let weak = Arc::downgrade(app);
    tokio::spawn(async move {
        let mut previous_restarts = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let Some(app) = weak.upgrade() else {
                break;
            };
            if app.stop.load(Ordering::Acquire) {
                break;
            }
            let metrics = app.stack.cache.metrics();
            if metrics.restarts != previous_restarts {
                app.telemetry.emit(
                    "engine",
                    format!(
                        "The engine restarted; {} restart(s) since this service started.",
                        metrics.restarts
                    ),
                    json!({"restarts": metrics.restarts}),
                );
                previous_restarts = metrics.restarts;
            }
            let (enabled, adapter) = {
                let stats = lock(&app.verification);
                (stats.running, stats.adapter)
            };
            if enabled {
                let _ = pass(&app, adapter).await;
            }
        }
    });
    let weak = Arc::downgrade(app);
    tokio::spawn(async move {
        loop {
            let Some(app) = weak.upgrade() else {
                break;
            };
            if app.stop.load(Ordering::Acquire) {
                break;
            }
            let rate = lock(&app.writer).rate;
            tokio::time::sleep(Duration::from_millis(if rate == 0 {
                250
            } else {
                1000 / u64::from(rate)
            }))
            .await;
            if app.stop.load(Ordering::Acquire) {
                break;
            }
            let work = app.clone();
            let _ = blocking(work, |app| {
                let settings = lock(&app.writer).clone();
                if !settings.enabled || settings.rate == 0 {
                    return Ok(());
                }
                use rand::Rng;
                let index = rand::rng().random_range(0..settings.operations.len());
                let operation = settings
                    .operations
                    .get(index)
                    .ok_or_else(|| ApiError::Internal("writer operation list is empty".into()))?
                    .clone();
                let mut conn = app.writes.get().map_err(|_| ApiError::Busy)?;
                let mut session = lock(&app.writer_session);
                if !lock(&app.writer).enabled {
                    return Ok(());
                }
                let result = session.run(&operation, &mut conn, &app.counts);
                match result {
                    Ok(()) => app.telemetry.emit(
                        "writer",
                        if operation == "rollback" {
                            "Rollback check completed; no changes committed.".into()
                        } else {
                            format!("Writer operation completed: {}.", operation)
                        },
                        json!({"operation": operation}),
                    ),
                    Err(error) => app.telemetry.emit(
                        "error",
                        format!("Background writer could not run {operation}: {error}"),
                        json!({}),
                    ),
                }
                Ok(())
            })
            .await;
        }
    });
}
#[derive(Deserialize)]
pub struct LoadRequest {
    name: String,
    concurrency: usize,
    seconds: u64,
    #[serde(default)]
    adapter: Adapter,
}
#[derive(Serialize)]
struct LoadResult {
    route: &'static str,
    requests: u64,
    errors: u64,
    memory: u64,
    requests_per_second: f64,
    p50_us: f64,
    p99_us: f64,
    sample_count: usize,
    elapsed_ms: f64,
}
async fn load_route(
    app: Arc<App>,
    lookup: Lookup,
    route: Route,
    request: &LoadRequest,
) -> Result<LoadResult, ApiError> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(request.seconds);
    let mut tasks = JoinSet::new();
    for _ in 0..request.concurrency {
        let app = app.clone();
        let lookup = lookup.clone();
        let adapter = request.adapter;
        tasks.spawn(async move {
            let mut requests = 0u64;
            let mut errors = 0u64;
            let mut memory = 0u64;
            let mut samples = VecDeque::new();
            while Instant::now() < deadline && !app.stop.load(Ordering::Acquire) {
                match app.read(lookup.clone(), route, adapter, false).await {
                    Ok(answer) => {
                        requests += 1;
                        if answer.served == Served::Cache {
                            memory += 1;
                        }
                        if samples.len() == 512 {
                            samples.pop_front();
                        }
                        samples.push_back(answer.elapsed_us);
                    }
                    Err(_) => {
                        errors += 1;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
            (requests, errors, memory, samples)
        });
    }
    let mut requests = 0;
    let mut errors = 0;
    let mut memory = 0;
    let mut samples = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let (n, e, m, own) = result.map_err(|error| ApiError::Internal(error.to_string()))?;
        requests += n;
        errors += e;
        memory += m;
        samples.extend(own);
    }
    let elapsed = started.elapsed();
    samples.sort_by(f64::total_cmp);
    let at = |q: usize| {
        samples
            .get(samples.len().saturating_sub(1) * q / 100)
            .copied()
            .unwrap_or(0.0)
    };
    Ok(LoadResult {
        route: route_name(route),
        requests,
        errors,
        memory,
        requests_per_second: requests as f64 / elapsed.as_secs_f64().max(1e-6),
        p50_us: at(50),
        p99_us: at(99),
        sample_count: samples.len(),
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
    })
}
pub async fn load(
    State(app): State<Arc<App>>,
    Json(request): Json<LoadRequest>,
) -> Result<Json<Value>, ApiError> {
    if !(1..=16).contains(&request.concurrency) || !(1..=10).contains(&request.seconds) {
        return Err(ApiError::BadRequest(
            "Use 1–16 workers and 1–10 seconds per route.".into(),
        ));
    }
    let _gate = app.scenario.try_lock().map_err(|_| ApiError::Busy)?;
    let lookup = Lookup::named(&request.name)
        .ok_or_else(|| ApiError::NotFound("That query does not exist.".into()))?;
    let cached = load_route(app.clone(), lookup.clone(), Route::Cached, &request).await?;
    let postgres = load_route(app.clone(), lookup, Route::Postgres, &request).await?;
    app.telemetry.emit("load", format!("Load test finished for {}.", request.name), json!({"cached_rps": cached.requests_per_second, "postgres_rps": postgres.requests_per_second}));
    Ok(Json(
        json!({"cached": cached, "postgres": postgres, "measurement": "service reads with pooled connections and serialization; no HTTP round trip; percentiles use the last 512 samples per worker"}),
    ))
}
#[derive(QueryableByName)]
struct Slot {
    #[diesel(sql_type = Text)]
    slot_name: String,
    #[diesel(sql_type = Nullable<Integer>)]
    active_pid: Option<i32>,
}
fn owned_slot(app: &App, conn: &mut PgConnection) -> Result<Slot, ApiError> {
    // Match the unique application label on THIS cache's slot-owner session.
    // Hash collisions or any uncertainty refuse the experiment. Never select
    // a slot just because it shares a prefix or database with this service.
    let mut slots: Vec<Slot> = diesel::sql_query("SELECT s.slot_name::text AS slot_name, s.active_pid FROM pg_replication_slots s JOIN pg_locks l ON l.locktype='advisory' AND l.granted AND l.objsubid=2 AND l.classid::bigint=1397183308 AND l.objid::bigint=(hashtext(s.slot_name)::bigint & 4294967295) JOIN pg_stat_activity owner ON owner.pid=l.pid WHERE owner.application_name=$1 AND s.database=current_database() AND NOT s.temporary")
        .bind::<Text,_>(&app.engine_label).load(conn)?;
    if slots.len() != 1 {
        return Err(ApiError::BadRequest(
            "Could not identify this demo's own slot safely. No slot was changed.".into(),
        ));
    }
    let slot = slots
        .pop()
        .ok_or_else(|| ApiError::Internal("slot disappeared".into()))?;
    if !slot.slot_name.starts_with("xyne_sync_slot_") {
        return Err(ApiError::BadRequest(
            "This slot does not have the expected demo engine identity.".into(),
        ));
    }
    Ok(slot)
}
#[derive(QueryableByName)]
struct BoolRow {
    #[diesel(sql_type = Bool)]
    value: bool,
}
async fn interrupt(app: Arc<App>, drop_slot: bool) -> Result<Option<i32>, ApiError> {
    blocking(app, move |app| {
        let mut conn = app.writes.get().map_err(|_| ApiError::Busy)?;
        let slot = owned_slot(&app, &mut conn)?;
        if let Some(pid) = slot.active_pid {
            let result: BoolRow = diesel::sql_query("SELECT pg_terminate_backend(active_pid) AS value FROM pg_replication_slots WHERE slot_name=$1 AND active_pid=$2")
                .bind::<Text,_>(&slot.slot_name).bind::<Integer,_>(pid).get_result(&mut conn)?;
            if !result.value { return Err(ApiError::Database("Postgres did not terminate the demo feed.".into())); }
        }
        if drop_slot {
            let deadline = Instant::now() + Duration::from_millis(700);
            loop {
                let current = owned_slot(&app, &mut conn)?;
                if current.slot_name != slot.slot_name { return Err(ApiError::BadRequest("The engine already replaced its slot. No replacement slot was dropped.".into())); }
                if current.active_pid.is_none() { break; }
                if Instant::now() >= deadline { return Err(ApiError::Database("The demo feed did not close in time; its slot was kept.".into())); }
                std::thread::sleep(Duration::from_millis(10));
            }
            diesel::sql_query("SELECT pg_drop_replication_slot($1)").bind::<Text,_>(&slot.slot_name).execute(&mut conn)?;
        }
        Ok(slot.active_pid)
    }).await
}
#[derive(Serialize)]
struct Phase {
    message: String,
    elapsed_ms: f64,
    detail: Value,
}
fn phase(app: &App, timeline: &mut Vec<Phase>, started: Instant, message: &str, detail: Value) {
    timeline.push(Phase {
        message: message.into(),
        elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        detail: detail.clone(),
    });
    app.telemetry.emit("scenario", message.into(), detail);
}
async fn recover(
    app: &Arc<App>,
    restarts: Option<u64>,
    old_pid: Option<i32>,
) -> Result<Answer, ApiError> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if app.stop.load(Ordering::Acquire) {
            return Err(ApiError::Busy);
        }
        let m = app.stack.cache.metrics();
        let restarted = restarts.is_none_or(|before| m.restarts > before);
        let feed_replaced = if let Some(old) = old_pid {
            blocking(app.clone(), move |app| {
                let mut conn = app.writes.get().map_err(|_| ApiError::Busy)?;
                Ok(owned_slot(&app, &mut conn)
                    .is_ok_and(|slot| slot.active_pid.is_some_and(|pid| pid != old)))
            })
            .await?
        } else {
            true
        };
        if restarted
            && feed_replaced
            && m.running
            && let Ok(answer) = app
                .read(
                    Lookup::Ticket { id: 1 },
                    Route::Cached,
                    Adapter::Sync,
                    false,
                )
                .await
            && answer.served == Served::Cache
        {
            return Ok(answer);
        }
        if Instant::now() >= deadline {
            return Err(ApiError::Database("Recovery did not finish within 20 seconds. Reads continue to use normal Postgres fallback.".into()));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn scenario_run(app: Arc<App>, name: String) -> Result<Value, ApiError> {
    let _gate = app.scenario.try_lock().map_err(|_| ApiError::Busy)?;
    let saved = lock(&app.writer).clone();
    lock(&app.writer).enabled = false;
    let started = Instant::now();
    let mut timeline = Vec::new();
    phase(
        &app,
        &mut timeline,
        started,
        "Experiment started; background writes paused.",
        json!({"scenario": name}),
    );
    let result = async {
        // Wait for a writer operation already inside its critical section.
        blocking(app.clone(), |app| {
            drop(lock(&app.writer_session));
            let mut conn = app.writes.get().map_err(|_| ApiError::Busy)?;
            if !streamgres_diesel::wait_for_writes(&app.stack.cache, &mut *conn, Duration::from_secs(5))? {
                return Err(ApiError::Database("The cache did not settle before the experiment.".into()));
            }
            Ok(())
        }).await?;
        match name.as_str() {
            "widen" | "new-type" | "drop" => {
                let step = Step::parse(&name).ok_or_else(|| ApiError::BadRequest("Unknown migration.".into()))?;
                let before = app.stack.cache.metrics().restarts;
                let caught = blocking(app.clone(), move |app| {
                    let mut conn = app.writes.get().map_err(|_| ApiError::Busy)?;
                    step.run(&mut *conn)?;
                    Ok(streamgres_diesel::wait_for_writes(&app.stack.cache, &mut *conn, Duration::from_secs(10))?)
                }).await?;
                phase(&app, &mut timeline, started, "Schema change committed in Postgres.", json!({"caught_up": caught}));
                let answer = recover(&app, (name == "drop").then_some(before), None).await?;
                if name == "new-type" { app.read(Lookup::named("of_kind").ok_or_else(|| ApiError::Internal("Missing kind query".into()))?, Route::Cached, Adapter::Sync, true).await?; }
                phase(&app, &mut timeline, started, "The desk is being answered from memory again.", json!({"served": answer.served, "restarts": app.stack.cache.metrics().restarts}));
            }
            "connection" | "slot" => {
                let before = app.stack.cache.metrics().restarts;
                let pid = interrupt(app.clone(), name == "slot").await?;
                phase(&app, &mut timeline, started, if name == "slot" { "Only this demo's replication slot was dropped." } else { "Only this demo's replication connection was terminated." }, json!({"pid": pid}));
                let answer = recover(&app, (name == "slot").then_some(before), if name == "connection" { pid } else { None }).await?;
                phase(&app, &mut timeline, started, "Replication recovered and a fresh memory answer was observed.", json!({"elapsed_us": answer.elapsed_us, "restarts": app.stack.cache.metrics().restarts}));
            }
            "rls" => {
                let rls_before = app.stack.cache.metrics().restarts;
                blocking(app.clone(), |app| { let mut conn = app.writes.get().map_err(|_| ApiError::Busy)?; conn.batch_execute("ALTER TABLE attachments ENABLE ROW LEVEL SECURITY")?; Ok(()) }).await?;
                let protected = async {
                    let deadline = Instant::now() + Duration::from_secs(10);
                    loop {
                        if let Ok(answer) = app.read(Lookup::Attachment { id: 1 }, Route::Cached, Adapter::Sync, false).await
                            && app.stack.cache.metrics().restarts > rls_before
                            && app.stack.cache.metrics().running
                            && answer.served == Served::Postgres && answer.refusal.is_some() { return Ok::<_, ApiError>(answer); }
                        if Instant::now() >= deadline { return Err(ApiError::Database("RLS fallback was not observed in time.".into())); }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }.await;
                // Cleanup is attempted even when the observation failed.
                blocking(app.clone(), |app| { let mut conn = app.writes.get().map_err(|_| ApiError::Busy)?; conn.batch_execute("ALTER TABLE attachments DISABLE ROW LEVEL SECURITY")?; Ok(()) }).await?;
                let answer = protected?;
                phase(&app, &mut timeline, started, "The protected table was sent to Postgres; RLS was then restored to its demo default.", json!({"refusal": answer.refusal}));
                let before = app.stack.cache.metrics().restarts;
                interrupt(app.clone(), true).await?;
                recover(&app, Some(before), None).await?;
                phase(&app, &mut timeline, started, "A fresh snapshot restored the original demo table coverage.", json!({}));
            }
            "session" => {
                let detail = blocking(app.clone(), |app| {
                    let mut conn = app.reads.get().map_err(|_| ApiError::Busy)?;
                    conn.batch_execute("DROP SCHEMA IF EXISTS demo_tenant CASCADE; CREATE SCHEMA demo_tenant; CREATE TABLE demo_tenant.tickets (LIKE public.tickets INCLUDING ALL); INSERT INTO demo_tenant.tickets SELECT * FROM public.tickets WHERE id=1; UPDATE demo_tenant.tickets SET title='A different tenant'; CREATE OR REPLACE FUNCTION public.demo_set_tenant() RETURNS void LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config('search_path','demo_tenant,public',false); END $$;")?;
                    let result = (|| {
                        conn.batch_execute("SELECT public.demo_set_tenant()")?;
                        let mut ctx = Ctx::new(&mut conn, &app.stack.cache, Route::Cached);
                        let data = Lookup::Ticket { id: 1 }.run(&mut ctx)?;
                        if ctx.served != Served::Postgres || data.get(0).and_then(|row| row.get("title")).and_then(Value::as_str) != Some("A different tenant") {
                            return Err(ApiError::Internal("The session isolation check did not return the shadow tenant through Postgres.".into()));
                        }
                        Ok::<_, ApiError>(json!({"served": ctx.served, "refusal": ctx.refusal, "data": data}))
                    })();
                    let cleanup = conn.batch_execute("RESET search_path; DROP FUNCTION IF EXISTS public.demo_set_tenant(); DROP SCHEMA IF EXISTS demo_tenant CASCADE;");
                    cleanup?;
                    result
                }).await?;
                phase(&app, &mut timeline, started, "A function changed the search path; the reader was rechecked and the other tenant came from Postgres.", detail);
            }
            "transaction" => {
                let before = app.read(Lookup::Ticket { id: 1 }, Route::Postgres, Adapter::Sync, false).await?;
                let old_status = before.data.get(0).and_then(|row| row.get("status")).and_then(Value::as_str).unwrap_or("open");
                let next = if old_status == "pending" { TicketStatus::Open } else { TicketStatus::Pending };
                let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                let work = app.clone();
                let transaction = tokio::spawn(async move {
                    blocking(work, move |app| {
                        let mut conn = app.writes.get().map_err(|_| ApiError::Busy)?;
                        conn.transaction::<_, diesel::result::Error, _>(|conn| {
                            diesel::update(schema::tickets::table.filter(schema::tickets::id.eq(1))).set(schema::tickets::status.eq(next)).execute(conn)?;
                            let _ = started_tx.send(());
                            release_rx.recv_timeout(Duration::from_secs(5)).map_err(|_| diesel::result::Error::RollbackTransaction)?;
                            Ok(())
                        })?;
                        Ok(streamgres_diesel::wait_for_writes(&app.stack.cache, &mut *conn, Duration::from_secs(5))?)
                    }).await
                });
                started_rx.await.map_err(|_| ApiError::Database("The transaction did not start.".into()))?;
                let hidden = app.read(Lookup::Ticket { id: 1 }, Route::Postgres, Adapter::Sync, false).await?;
                let memory = app.read(Lookup::Ticket { id: 1 }, Route::Cached, Adapter::Sync, false).await?;
                let isolated = before.data == hidden.data && before.data == memory.data;
                phase(&app, &mut timeline, started, "The writer is paused inside an open transaction; another connection still sees the committed value.", json!({"uncommitted_hidden": isolated}));
                release_tx.send(()).map_err(|_| ApiError::Database("The transaction already ended.".into()))?;
                let caught = transaction.await.map_err(|error| ApiError::Internal(error.to_string()))??;
                if !isolated || !caught { return Err(ApiError::Internal("The transaction isolation or cache write barrier check failed.".into())); }
                phase(&app, &mut timeline, started, "The transaction committed and its cache barrier completed.", json!({"caught_up": caught}));
            }
            _ => return Err(ApiError::BadRequest("Choose widen, new-type, drop, connection, slot, rls, session or transaction.".into())),
        }
        Ok::<_, ApiError>(())
    }.await;
    *lock(&app.writer) = saved;
    match result {
        Ok(()) => Ok(
            json!({"scenario": name, "ok": true, "timeline": timeline, "elapsed_ms": started.elapsed().as_secs_f64()*1000.0}),
        ),
        Err(error) => {
            app.telemetry.emit(
                "error",
                format!("Experiment {name} could not finish: {error:?}"),
                json!({}),
            );
            Err(error)
        }
    }
}
pub async fn scenario(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if !app.options.lab {
        return Err(ApiError::BadRequest(
            "Schema and fault experiments require demo serve --lab on a disposable demo database."
                .into(),
        ));
    }
    // Dropping an HTTP request does not cancel cleanup after a schema change.
    tokio::spawn(async move { scenario_run(app, name).await })
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?
        .map(Json)
}
