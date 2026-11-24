//! Comparisons, continuous verification, the background writer, load runs
//! and the experiments.
//!
//! Every experiment pauses the writer, records a timeline of what it
//! observed, and restores the writer however it ends. The fault
//! experiments act only on this process's own replication slot, which
//! Streamgres names ([`streamgres_diesel::EngineState::instance`]).

use std::time::{Duration, Instant};

use diesel::sql_types::{Bool, Integer, Nullable, Text};
use diesel::{QueryableByName, sql_query};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use rand::Rng;

use super::*;
use crate::bench;
use crate::migrate::Step;
use crate::verify::{self, Comparison};

pub async fn compare_named(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Comparison>, ApiError> {
    let lookup = named(&name, &options)?;
    validate(&lookup)?;
    Ok(Json(
        verify::compare(
            &app.lib,
            &app.pool,
            &lookup,
            Duration::from_secs(2),
            &app.stop,
        )
        .await?,
    ))
}

pub async fn verification(State(app): State<Arc<App>>) -> Json<Verification> {
    Json(lock(&app.verification).clone())
}

#[derive(Deserialize)]
pub struct VerifyControl {
    running: bool,
}

pub async fn verify_control(
    State(app): State<Arc<App>>,
    Json(input): Json<VerifyControl>,
) -> Json<Verification> {
    lock(&app.verification).running = input.running;
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

/// One verification pass over every shape; busy while another runs.
async fn pass(app: &Arc<App>) -> Result<(), ApiError> {
    {
        let mut stats = lock(&app.verification);
        if stats.busy {
            return Err(ApiError::Busy);
        }
        stats.busy = true;
    }
    verify::pass(
        &app.lib,
        &app.pool,
        &Lookup::demo(),
        Duration::from_millis(2500),
        &app.stop,
        &app.verification,
    )
    .await;
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

pub async fn verify_once(State(app): State<Arc<App>>) -> Result<Json<Verification>, ApiError> {
    // In a task of its own: a disconnected browser cannot strand the busy
    // flag or cancel the pass.
    let work = app.clone();
    tokio::spawn(async move { pass(&work).await })
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))??;
    Ok(Json(lock(&app.verification).clone()))
}

/// The service's background work: continuous verification and restart
/// notices, and the writer.
pub fn background(app: &Arc<App>) {
    let weak = Arc::downgrade(app);
    tokio::spawn(async move {
        let mut restarts = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let Some(app) = weak.upgrade() else { break };
            if app.stopping() {
                break;
            }
            let seen = app.lib.metrics().restarts;
            if seen != restarts {
                app.telemetry.emit(
                    "engine",
                    format!("The engine restarted; {seen} restart(s) since this service started."),
                    json!({"restarts": seen, "instance": app.lib.engine().instance}),
                );
                restarts = seen;
            }
            if lock(&app.verification).running {
                let _ = pass(&app).await;
            }
        }
    });
    let weak = Arc::downgrade(app);
    tokio::spawn(async move {
        loop {
            let Some(app) = weak.upgrade() else { break };
            if app.stopping() {
                break;
            }
            let settings = lock(&app.writer).clone();
            let every = match settings.rate {
                0 => Duration::from_millis(250),
                rate => Duration::from_millis(1000 / u64::from(rate)),
            };
            drop(app);
            tokio::time::sleep(every).await;
            let Some(app) = weak.upgrade() else { break };
            if settings.enabled && settings.rate > 0 {
                write_once(&app).await;
            }
        }
    });
}

/// One writer operation, drawn from the mix.
async fn write_once(app: &App) {
    let operation = {
        let settings = lock(&app.writer);
        if !settings.enabled || settings.operations.is_empty() {
            return;
        }
        let index = rand::rng().random_range(0..settings.operations.len());
        settings.operations[index].clone()
    };
    let mut session = app.writer_session.lock().await;
    // Paused while waiting for the session: an experiment holds it.
    if !lock(&app.writer).enabled {
        return;
    }
    let outcome = match app.pool.get().await {
        Ok(mut conn) => session.run(&operation, &mut conn, &app.counts).await,
        Err(error) => Err(diesel::result::Error::DatabaseError(
            diesel::result::DatabaseErrorKind::UnableToSendCommand,
            Box::new(error.to_string()),
        )),
    };
    match outcome {
        Ok(()) => app.telemetry.emit(
            "writer",
            if operation == "rollback" {
                "Rollback check completed; no changes committed.".into()
            } else {
                format!("Writer operation completed: {operation}.")
            },
            json!({"operation": operation}),
        ),
        Err(error) => app.telemetry.emit(
            "error",
            format!("Background writer could not run {operation}: {error}"),
            json!({}),
        ),
    }
}

#[derive(Deserialize)]
pub struct LoadRequest {
    name: String,
    concurrency: usize,
    seconds: u64,
}

/// The same lookup on each route, one after the other.
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
    let duration = Duration::from_secs(request.seconds);
    let run = |route| {
        bench::run(
            &app.lib,
            &app.pool,
            &lookup,
            route,
            request.concurrency,
            duration,
            app.stop.clone(),
        )
    };
    let cached = run(Route::Streamgres).await;
    let postgres = run(Route::Postgres).await;
    app.telemetry.emit(
        "load",
        format!("Load test finished for {}.", request.name),
        json!({"cached_rps": cached.requests_per_second, "postgres_rps": postgres.requests_per_second}),
    );
    Ok(Json(json!({
        "cached": cached,
        "postgres": postgres,
        "measurement": "in-service reads with Diesel decoding; no serialization, pool wait or HTTP; percentiles use the last 512 samples per worker",
    })))
}

pub async fn scenario(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if !app.options.lab {
        return Err(ApiError::BadRequest(
            "Experiments require demo serve --lab on a disposable demo database.".into(),
        ));
    }
    // A disconnected caller does not cancel an experiment's cleanup.
    tokio::spawn(async move { run_scenario(app, name).await })
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?
        .map(Json)
}

/// One step of an experiment's timeline.
#[derive(Serialize)]
struct Phase {
    message: String,
    elapsed_ms: f64,
    detail: Value,
}

/// An experiment's record: its phases, also sent as activity.
struct Timeline<'a> {
    app: &'a App,
    started: Instant,
    phases: Vec<Phase>,
}

impl Timeline<'_> {
    fn note(&mut self, message: &str, detail: Value) {
        self.phases.push(Phase {
            message: message.into(),
            elapsed_ms: self.started.elapsed().as_secs_f64() * 1e3,
            detail: detail.clone(),
        });
        self.app.telemetry.emit("scenario", message.into(), detail);
    }
}

async fn run_scenario(app: Arc<App>, name: String) -> Result<Value, ApiError> {
    let _gate = app.scenario.try_lock().map_err(|_| ApiError::Busy)?;
    let saved = lock(&app.writer).clone();
    lock(&app.writer).enabled = false;
    let mut timeline = Timeline {
        app: &app,
        started: Instant::now(),
        phases: Vec::new(),
    };
    timeline.note(
        "Experiment started; background writes paused.",
        json!({"scenario": name}),
    );
    // Waits out a writer operation already under way.
    drop(app.writer_session.lock().await);
    let outcome = experiment(&app, &name, &mut timeline).await;
    *lock(&app.writer) = saved;
    let phases = std::mem::take(&mut timeline.phases);
    match outcome {
        Ok(()) => Ok(json!({
            "scenario": name,
            "ok": true,
            "timeline": phases,
            "elapsed_ms": timeline.started.elapsed().as_secs_f64() * 1e3,
        })),
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

async fn experiment(
    app: &Arc<App>,
    name: &str,
    timeline: &mut Timeline<'_>,
) -> Result<(), ApiError> {
    match name {
        "widen" | "new-type" | "drop" => {
            let step = Step::parse(name)
                .ok_or_else(|| ApiError::BadRequest("Unknown migration.".into()))?;
            let restarts = app.lib.metrics().restarts;
            step.run(&mut *write_conn(app).await?).await?;
            timeline.note(
                "Schema change committed in Postgres.",
                json!({"expectation": step.expectation()}),
            );
            let answer = recover(app, (step == Step::Drop).then_some(restarts), None).await?;
            timeline.note(
                "The desk's cards are answered from memory again.",
                json!({"answered_by": answer.answered_by, "restarts": app.lib.metrics().restarts}),
            );
            if step == Step::NewType {
                // Streamgres reads the new type within a moment of the
                // schema change; until then Diesel cannot name it.
                let lookup = Lookup::named("of_kind")
                    .ok_or_else(|| ApiError::Internal("no of_kind query".into()))?;
                let deadline = Instant::now() + Duration::from_secs(5);
                let kinds = loop {
                    match app.read(&lookup, Route::Streamgres).await {
                        Ok(kinds) => break kinds,
                        Err(_) if Instant::now() < deadline => {
                            tokio::time::sleep(Duration::from_millis(100)).await
                        }
                        Err(error) => return Err(error),
                    }
                };
                timeline.note(
                    "Tickets were read by the new enum column.",
                    json!({"served": kinds.served, "refusal": kinds.refusal, "rows": kinds.rows}),
                );
            }
        }
        "connection" | "slot" => {
            let restarts = app.lib.metrics().restarts;
            let pid = interrupt(app, name == "slot").await?;
            timeline.note(
                if name == "slot" {
                    "Only this demo's replication slot was dropped."
                } else {
                    "Only this demo's replication connection was terminated."
                },
                json!({"pid": pid}),
            );
            let answer = recover(
                app,
                (name == "slot").then_some(restarts),
                (name == "connection").then_some(pid).flatten(),
            )
            .await?;
            timeline.note(
                "Replication recovered and a fresh answer from memory was observed.",
                json!({"elapsed_us": answer.elapsed_us, "restarts": app.lib.metrics().restarts}),
            );
        }
        "transaction" => transaction(app, timeline).await?,
        _ => {
            return Err(ApiError::BadRequest(
                "Choose widen, new-type, drop, connection, slot or transaction.".into(),
            ));
        }
    }
    Ok(())
}

/// A write held open in a transaction is not seen from memory; once it
/// commits, memory shows it after the feed's delay.
async fn transaction(app: &Arc<App>, timeline: &mut Timeline<'_>) -> Result<(), ApiError> {
    let card = Lookup::Card { id: 1 };
    let before = app.read(&card, Route::Streamgres).await?;
    let status = |answer: &Answer| {
        answer
            .data
            .get(0)
            .and_then(|row| row.get("status"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    let old = status(&before).unwrap_or_else(|| "open".into());
    let next = if old == "pending" {
        TicketStatus::Open
    } else {
        TicketStatus::Pending
    };
    let mut conn = write_conn(app).await?;
    conn.batch_execute("BEGIN").await?;
    let held = async {
        diesel::update(schema::tickets::table.filter(schema::tickets::id.eq(1)))
            .set(schema::tickets::status.eq(next))
            .execute(&mut conn)
            .await?;
        let during = app.read(&card, Route::Streamgres).await?;
        Ok::<_, ApiError>(during)
    }
    .await;
    let during = match held {
        Ok(during) => during,
        Err(error) => {
            let _ = conn.batch_execute("ROLLBACK").await;
            return Err(error);
        }
    };
    let hidden = status(&during) == Some(old.clone());
    timeline.note(
        "A write is held inside an open transaction; memory still shows the committed value.",
        json!({"uncommitted_hidden": hidden, "served": during.served}),
    );
    conn.batch_execute("COMMIT").await?;
    drop(conn);
    let (visible, after) = wait_visible(app, &card, |rows| {
        rows.get(0)
            .and_then(|row| row.get("status"))
            .and_then(Value::as_str)
            == Some(next.label())
    })
    .await;
    if !hidden || !visible {
        return Err(ApiError::Internal(
            "The transaction was seen before it committed, or not after.".into(),
        ));
    }
    timeline.note(
        "The transaction committed and memory showed it.",
        json!({"visible_after_us": after.as_secs_f64() * 1e6}),
    );
    Ok(())
}

#[derive(QueryableByName)]
struct ActivePid {
    #[diesel(sql_type = Nullable<Integer>)]
    active_pid: Option<i32>,
}

#[derive(QueryableByName)]
struct Done {
    #[diesel(sql_type = Bool)]
    done: bool,
}

/// This process's replication slot, as Streamgres names it.
fn own_slot(app: &App) -> Result<String, ApiError> {
    app.lib
        .engine()
        .instance
        .filter(|slot| slot.starts_with("xyne_sync_slot_"))
        .ok_or_else(|| {
            ApiError::BadRequest("The engine is not running on a slot of its own now.".into())
        })
}

/// The backend streaming `slot`, if one is.
async fn slot_pid(
    conn: &mut diesel_async::AsyncPgConnection,
    slot: &str,
) -> Result<Option<i32>, ApiError> {
    let rows: Vec<ActivePid> =
        sql_query("SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1")
            .bind::<Text, _>(slot)
            .load(conn)
            .await?;
    rows.into_iter()
        .next()
        .map(|row| row.active_pid)
        .ok_or_else(|| ApiError::BadRequest("This demo's slot is gone already.".into()))
}

/// Terminates this process's replication connection and, with
/// `drop_slot`, drops its slot once the connection is gone; the pid that
/// was streaming.
async fn interrupt(app: &App, drop_slot: bool) -> Result<Option<i32>, ApiError> {
    let slot = own_slot(app)?;
    let mut conn = write_conn(app).await?;
    let pid = slot_pid(&mut conn, &slot).await?;
    if let Some(pid) = pid {
        let terminated: Done = sql_query("SELECT pg_terminate_backend($1) AS done")
            .bind::<Integer, _>(pid)
            .get_result(&mut conn)
            .await?;
        if !terminated.done {
            return Err(ApiError::Database(
                "Postgres did not terminate the demo's feed.".into(),
            ));
        }
    }
    if drop_slot {
        let deadline = Instant::now() + Duration::from_millis(700);
        while slot_pid(&mut conn, &slot).await?.is_some() {
            if Instant::now() >= deadline {
                return Err(ApiError::Database(
                    "The demo's feed did not close in time; its slot was kept.".into(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        sql_query("SELECT pg_drop_replication_slot($1)")
            .bind::<Text, _>(&slot)
            .execute(&mut conn)
            .await?;
    }
    Ok(pid)
}

/// Waits until the engine has restarted (when `restarts` is given), its
/// feed is on another backend than `old_pid` (when given), and a card is
/// answered from memory; that answer.
async fn recover(
    app: &Arc<App>,
    restarts: Option<u64>,
    old_pid: Option<i32>,
) -> Result<Answer, ApiError> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if app.stopping() {
            return Err(ApiError::Busy);
        }
        let restarted = restarts.is_none_or(|before| app.lib.metrics().restarts > before);
        let moved = match (old_pid, own_slot(app)) {
            (Some(old), Ok(slot)) => slot_pid(&mut *write_conn(app).await?, &slot)
                .await
                .is_ok_and(|pid| pid.is_some_and(|pid| pid != old)),
            (Some(_), Err(_)) => false,
            (None, _) => true,
        };
        if restarted
            && moved
            && let Ok(answer) = app.read(&Lookup::Card { id: 1 }, Route::Streamgres).await
            && answer.served == "cache"
        {
            return Ok(answer);
        }
        if Instant::now() >= deadline {
            return Err(ApiError::Database(
                "Recovery did not finish within 20 seconds. Reads go to Postgres meanwhile.".into(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
