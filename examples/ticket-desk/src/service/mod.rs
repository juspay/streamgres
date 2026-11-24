//! The HTTP service: the desk, every query shape compared with Postgres
//! and inspected, and the lab.
//!
//! Reads go through [`Streamgres`] (or, with `?route=postgres`, straight
//! to Postgres); every answer says who answered it and why not memory.
//! Writes go to Postgres through the pool and reach memory through the
//! replication stream; `?wait=true` reads the change back through
//! Streamgres until memory shows it and reports how long that took.

mod lab;
mod telemetry;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::sse::{Event as SseEvent, KeepAlive};
use axum::response::{Html, IntoResponse, Response, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use streamgres_diesel::{LoadAndSync, Streamgres};
use telemetry::{Telemetry, lock};
use tokio::sync::Semaphore;

use crate::db::{self, Pool};
use crate::models::{Organization, TicketPriority, TicketStatus};
use crate::queries::{self, Answer, Lookup, Route};
use crate::verify::Verification;
use crate::{schema, writer};

#[derive(Clone, clap::Args)]
pub struct Options {
    #[arg(long, default_value = "127.0.0.1:8080", env = "DEMO_LISTEN")]
    pub listen: SocketAddr,
    /// Enables schema changes and faults on this demo's own replication
    /// slot.
    #[arg(long, env = "DEMO_LAB")]
    pub lab: bool,
    /// Postgres connections for writes and direct reads.
    #[arg(long, default_value_t = 8, env = "DEMO_POOL_SIZE", value_parser = clap::value_parser!(u32).range(1..=64))]
    pub pool_size: u32,
    #[arg(long, default_value_t = 8, env = "DEMO_WRITER_RATE", value_parser = clap::value_parser!(u32).range(0..=100))]
    pub writer_rate: u32,
    #[arg(long, default_value_t = 268435456, env = "DEMO_MAX_BYTES")]
    pub max_bytes: usize,
    #[arg(long, default_value_t = 2000, env = "DEMO_MAX_LAG_MS")]
    pub max_lag_ms: u64,
    /// Replaces the demo's data before serving. Use only a demo database.
    #[arg(long)]
    pub seed: bool,
    /// Creates a missing demo schema, data, publication and trigger stack
    /// without replacing existing data.
    #[arg(long)]
    pub init: bool,
}

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    NotFound(String),
    Busy,
    Database(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::BadRequest(s) => (StatusCode::BAD_REQUEST, s),
            Self::NotFound(s) => (StatusCode::NOT_FOUND, s),
            Self::Busy => (
                StatusCode::SERVICE_UNAVAILABLE,
                "The service is busy. Try again shortly.".into(),
            ),
            Self::Database(s) => (StatusCode::SERVICE_UNAVAILABLE, s),
            Self::Internal(s) => (StatusCode::INTERNAL_SERVER_ERROR, s),
        };
        (status, Json(json!({"error": message}))).into_response()
    }
}

impl From<diesel::result::Error> for ApiError {
    fn from(error: diesel::result::Error) -> Self {
        match error {
            diesel::result::Error::NotFound => {
                Self::NotFound("The requested record does not exist.".into())
            }
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::ForeignKeyViolation,
                _,
            ) => Self::BadRequest("The referenced organization or ticket does not exist.".into()),
            other => Self::Database(format!("The read or write did not complete: {other}")),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ReadOptions {
    pub route: Option<String>,
    pub org: Option<i64>,
    pub id: Option<i64>,
    pub ticket: Option<i64>,
    pub page: Option<i64>,
    pub statuses: Option<String>,
    pub pattern: Option<String>,
}

impl ReadOptions {
    fn route(&self) -> Result<Route, ApiError> {
        match self.route.as_deref() {
            None | Some("streamgres") | Some("cached") | Some("cache") => Ok(Route::Streamgres),
            Some("postgres") => Ok(Route::Postgres),
            _ => Err(ApiError::BadRequest(
                "route must be streamgres or postgres".into(),
            )),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct WriterSettings {
    pub enabled: bool,
    pub rate: u32,
    pub operations: Vec<String>,
}

pub struct App {
    pub(crate) lib: Streamgres,
    pub(crate) pool: Pool,
    /// Bounds the reads and writes in flight; past it a request is told
    /// the service is busy rather than queued without end.
    pub(crate) admission: Arc<Semaphore>,
    pub(crate) telemetry: Telemetry,
    pub(crate) writer: Mutex<WriterSettings>,
    pub(crate) writer_session: tokio::sync::Mutex<writer::Session>,
    pub(crate) counts: Arc<writer::Counts>,
    pub(crate) verification: Mutex<Verification>,
    pub(crate) scenario: tokio::sync::Mutex<()>,
    pub(crate) stop: Arc<AtomicBool>,
    pub(crate) options: Options,
}

pub async fn start(dsn: String, options: Options) -> Result<Arc<App>, String> {
    let pool = db::pool(&dsn, options.pool_size + 2).await?;
    if options.seed || options.init {
        let mut conn = pool
            .get()
            .await
            .map_err(|error| format!("Connect to the demo database: {error}"))?;
        db::install(&mut conn, options.seed)
            .await
            .map_err(|error| format!("Installing the demo: {error}"))?;
    }
    let lib = db::start(
        &dsn,
        &db::Settings {
            cache_budget: options.max_bytes,
            max_lag: Duration::from_millis(options.max_lag_ms),
            pool_size: options.pool_size as usize,
            ..db::Settings::default()
        },
    )
    .await?;
    let app = Arc::new(App {
        lib,
        pool,
        admission: Arc::new(Semaphore::new(options.pool_size as usize * 8)),
        telemetry: Telemetry::default(),
        writer: Mutex::new(WriterSettings {
            enabled: options.writer_rate > 0,
            rate: options.writer_rate,
            operations: writer::OPS.iter().map(|s| (*s).into()).collect(),
        }),
        writer_session: tokio::sync::Mutex::new(writer::Session::default()),
        counts: Arc::new(writer::Counts::default()),
        verification: Mutex::new(Verification::default()),
        scenario: tokio::sync::Mutex::new(()),
        stop: Arc::new(AtomicBool::new(false)),
        options,
    });
    lab::background(&app);
    app.telemetry.emit(
        "engine",
        "Ticket desk is connected to Streamgres and Postgres.".into(),
        json!({"instance": app.lib.engine().instance}),
    );
    Ok(app)
}

impl App {
    /// Whether the service is stopping.
    pub(crate) fn stopping(&self) -> bool {
        AtomicBool::load(&self.stop, Ordering::Acquire)
    }

    pub async fn shutdown(&self) {
        self.stop.store(true, Ordering::Release);
        self.lib.shutdown().await;
    }

    /// Reads `lookup` on `route`, counted and reported.
    pub(crate) async fn read(&self, lookup: &Lookup, route: Route) -> Result<Answer, ApiError> {
        validate(lookup)?;
        let _admitted = self.admission.try_acquire().map_err(|_| ApiError::Busy)?;
        let answer = queries::read(&self.lib, &self.pool, lookup, route).await?;
        self.telemetry.read(
            &format!("{}:{}", answer.lookup, route_name(route)),
            answer.served,
            route == Route::Streamgres,
            answer.elapsed_us,
            answer.refusal.as_deref(),
        );
        self.telemetry.emit(
            "read",
            format!(
                "{}: {} in {:.1} µs",
                answer.lookup.replace('_', " "),
                match answer.served {
                    "cache" => "answered from memory",
                    "postgres" => "answered by Postgres",
                    "unsupported" => "unsupported",
                    _ => "failed",
                },
                answer.elapsed_us
            ),
            json!({"lookup": answer.lookup, "served": answer.served, "elapsed_us": answer.elapsed_us, "refusal": answer.refusal}),
        );
        Ok(answer)
    }
}

fn route_name(route: Route) -> &'static str {
    match route {
        Route::Streamgres => "streamgres",
        Route::Postgres => "postgres",
    }
}

fn validate(lookup: &Lookup) -> Result<(), ApiError> {
    let encoded =
        serde_json::to_vec(lookup).map_err(|error| ApiError::BadRequest(error.to_string()))?;
    if encoded.len() > 16000 {
        return Err(ApiError::BadRequest(
            "The query parameters are too large.".into(),
        ));
    }
    match lookup {
        Lookup::Queue { page, statuses, .. } | Lookup::QueueCards { page, statuses, .. }
            if !(1..=10000).contains(page) || statuses.len() > 3 =>
        {
            Err(ApiError::BadRequest(
                "Use a page from 1 to 10000 and at most three statuses.".into(),
            ))
        }
        Lookup::Many { ids } if ids.len() > 1000 => Err(ApiError::BadRequest(
            "At most 1000 ticket IDs may be read together.".into(),
        )),
        Lookup::Recent { limit, .. } if !(1..=1000).contains(limit) => Err(ApiError::BadRequest(
            "Limit must be between 1 and 1000.".into(),
        )),
        _ => Ok(()),
    }
}

pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("assets/index.html")) }),
        )
        .route(
            "/app.css",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
                    include_str!("assets/app.css"),
                )
            }),
        )
        .route(
            "/app.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                    include_str!("assets/app.js"),
                )
            }),
        )
        .route("/fonts/{name}", get(font))
        .route("/api/metrics", get(metrics))
        .route("/api/lookups", get(lookups))
        .route("/api/query/{name}", get(named_query))
        .route("/api/query", post(query))
        .route("/api/compare/{name}", get(lab::compare_named))
        .route("/api/inspect/{name}", get(inspect))
        .route("/api/memory", get(memory))
        .route("/api/organizations", get(organizations))
        .route("/tickets/{id}", get(ticket))
        .route("/tickets", post(create_ticket))
        .route("/tickets/{id}/status", post(update_status))
        .route("/tickets/{id}/comments", get(comments).post(add_comment))
        .route("/orgs/{org}/queue", get(queue))
        .route("/orgs/{org}/agents", get(agents))
        .route("/api/events", get(events))
        .route("/api/writer", get(writer_settings).post(set_writer))
        .route("/api/verify", get(lab::verification).post(lab::verify_once))
        .route("/api/verify/control", post(lab::verify_control))
        .route("/api/load", post(lab::load))
        .route("/api/lab/{scenario}", post(lab::scenario))
        .layer(DefaultBodyLimit::max(65536))
        .layer(axum::middleware::from_fn(guard))
        .with_state(app)
}

/// Lab routes only through a loopback host, changes only from the page's
/// own origin, and nothing cached by the browser.
async fn guard(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    if request.uri().path().starts_with("/api/lab/") {
        let local = host
            .parse::<axum::http::uri::Authority>()
            .is_ok_and(|host| matches!(host.host(), "127.0.0.1" | "localhost" | "[::1]" | "::1"));
        if !local {
            return ApiError::BadRequest(
                "Experiments are available only through a loopback host.".into(),
            )
            .into_response();
        }
    }
    if !matches!(
        *request.method(),
        axum::http::Method::GET | axum::http::Method::HEAD
    ) && let Some(origin) = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        && origin != format!("http://{host}")
    {
        return ApiError::BadRequest("Cross-origin changes are not accepted.".into())
            .into_response();
    }
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}

async fn font(Path(name): Path<String>) -> Response {
    let font: &'static [u8] = match name.as_str() {
        "google-sans-flex-latin.woff2" => {
            include_bytes!("assets/fonts/google-sans-flex-latin.woff2")
        }
        "manrope-latin.woff2" => include_bytes!("assets/fonts/manrope-latin.woff2"),
        "geist-mono-latin.woff2" => include_bytes!("assets/fonts/geist-mono-latin.woff2"),
        "licenses.txt" => {
            return (
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                include_str!("assets/fonts/LICENSES.txt"),
            )
                .into_response();
        }
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    ([(header::CONTENT_TYPE, "font/woff2")], font).into_response()
}

async fn query(
    State(app): State<Arc<App>>,
    Query(options): Query<ReadOptions>,
    Json(lookup): Json<Lookup>,
) -> Result<Json<Answer>, ApiError> {
    app.read(&lookup, options.route()?).await.map(Json)
}

/// A shape by name, its defaults overridden by the query string.
fn named(name: &str, options: &ReadOptions) -> Result<Lookup, ApiError> {
    let mut lookup = Lookup::named(name)
        .ok_or_else(|| ApiError::NotFound("That query shape does not exist.".into()))?;
    match &mut lookup {
        Lookup::Ticket { id } | Lookup::Card { id } | Lookup::Attachment { id } => {
            *id = options.id.unwrap_or(*id);
        }
        Lookup::Queue {
            org,
            page,
            statuses,
        }
        | Lookup::QueueCards {
            org,
            page,
            statuses,
        } => {
            *org = options.org.unwrap_or(*org);
            *page = options.page.unwrap_or(*page);
            if let Some(value) = &options.statuses {
                *statuses = parse_statuses(value)?;
            }
        }
        Lookup::Search { org, pattern } => {
            *org = options.org.unwrap_or(*org);
            if let Some(value) = &options.pattern {
                *pattern = value.clone();
            }
        }
        Lookup::Comments { ticket } | Lookup::CommentAuthors { ticket } => {
            *ticket = options.ticket.unwrap_or(*ticket);
        }
        _ => {}
    }
    Ok(lookup)
}

async fn named_query(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Answer>, ApiError> {
    app.read(&named(&name, &options)?, options.route()?)
        .await
        .map(Json)
}

/// Where a shape's query stands in Streamgres, and who has read it.
async fn inspect(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Value>, ApiError> {
    let lookup = named(&name, &options)?;
    let Some(inspected) = lookup.inspect(&app.lib)? else {
        return Ok(Json(json!({
            "lookup": name,
            "state": "raw SQL: never handed to Streamgres",
        })));
    };
    Ok(Json(inspect_json(&inspected)))
}

/// An [`streamgres_diesel::Inspect`] as JSON.
fn inspect_json(inspected: &streamgres_diesel::Inspect) -> Value {
    let millis = |duration: Option<Duration>| duration.map(|duration| duration.as_secs_f64() * 1e3);
    json!({
        "query": inspected.query.to_string(),
        "sql": inspected.sql,
        "state": format!("{:?}", inspected.state),
        "rows": inspected.rows,
        "bytes": inspected.bytes,
        "ttl": inspected.ttl.map(|ttl| ttl.to_string()),
        "last_read_ms": millis(inspected.last_read),
        "expires_in_ms": millis(inspected.expires_in),
        "engine": engine_json(&inspected.engine),
        "callers": inspected.callers.iter().map(|caller| json!({
            "at": caller.at.to_string(),
            "reads": caller.reads,
            "memory": caller.memory,
            "postgres": caller.postgres,
            "failed": caller.failed,
            "last_ms": caller.last.as_secs_f64() * 1e3,
        })).collect::<Vec<_>>(),
        "recent": inspected.recent.iter().map(|trace| json!({
            "served": trace.served.to_string(),
            "reason": trace.reason,
            "rows": trace.rows,
            "took_us": trace.took.as_secs_f64() * 1e6,
            "at": trace.at.to_string(),
            "position": trace.position,
        })).collect::<Vec<_>>(),
    })
}

fn engine_json(engine: &streamgres_diesel::EngineState) -> Value {
    json!({
        "running": engine.running,
        "instance": engine.instance,
        "position": engine.position,
        "lag_ms": engine.lag.map(|lag| lag.as_secs_f64() * 1e3),
        "queries": engine.queries,
        "bytes": engine.bytes,
    })
}

/// Every query Streamgres keeps now.
async fn memory(State(app): State<Arc<App>>) -> Json<Value> {
    let kept = app.lib.queries();
    Json(json!({
        "engine": engine_json(&app.lib.engine()),
        "queries": kept.iter().map(|query| json!({
            "sql": query.sql,
            "state": format!("{:?}", query.state),
            "rows": query.rows,
            "bytes": query.bytes,
            "ttl": query.ttl.to_string(),
            "last_read_ms": query.last_read.as_secs_f64() * 1e3,
        })).collect::<Vec<_>>(),
    }))
}

async fn ticket(
    State(app): State<Arc<App>>,
    Path(id): Path<i64>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Answer>, ApiError> {
    app.read(&Lookup::Ticket { id }, options.route()?)
        .await
        .map(Json)
}

async fn comments(
    State(app): State<Arc<App>>,
    Path(id): Path<i64>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Answer>, ApiError> {
    app.read(&Lookup::CommentAuthors { ticket: id }, options.route()?)
        .await
        .map(Json)
}

fn parse_statuses(value: &str) -> Result<Vec<TicketStatus>, ApiError> {
    value
        .split(',')
        .map(|label| {
            TicketStatus::parse(label).ok_or_else(|| {
                ApiError::BadRequest("Status must be open, pending or closed.".into())
            })
        })
        .collect()
}

async fn queue(
    State(app): State<Arc<App>>,
    Path(org): Path<i64>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Answer>, ApiError> {
    let statuses = options
        .statuses
        .as_deref()
        .map(parse_statuses)
        .transpose()?
        .unwrap_or_else(|| vec![TicketStatus::Open, TicketStatus::Pending]);
    // The cards, not the full rows: the full rows sort by a timestamptz,
    // which Streamgres does not support yet.
    let lookup = Lookup::QueueCards {
        org,
        statuses,
        page: options.page.unwrap_or(1),
    };
    app.read(&lookup, options.route()?).await.map(Json)
}

async fn agents(
    State(app): State<Arc<App>>,
    Path(org): Path<i64>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Answer>, ApiError> {
    app.read(&Lookup::Agents { org }, options.route()?)
        .await
        .map(Json)
}

/// The organizations, from memory: they never change while the desk
/// runs, so the query is kept forever.
async fn organizations(State(app): State<Arc<App>>) -> Result<Json<Vec<Organization>>, ApiError> {
    Ok(Json(
        schema::organizations::table
            .select(Organization::as_select())
            .order(schema::organizations::id)
            .load_and_sync(&app.lib)
            .ttl(streamgres_diesel::Ttl::Forever)
            .await?,
    ))
}

async fn lookups(State(app): State<Arc<App>>) -> Json<Value> {
    let mut shapes = Vec::new();
    for lookup in Lookup::demo() {
        shapes.push(match app.read(&lookup, Route::Streamgres).await {
            Ok(answer) => json!({
                "name": lookup.name(),
                "exercises": lookup.exercises(),
                "expected": lookup.expected(),
                "served": answer.served,
                "answered_by": answer.answered_by,
                "refusal": answer.refusal,
                "sql": answer.trace.as_ref().map(|trace| trace.sql.clone()),
                "elapsed_us": answer.elapsed_us,
            }),
            Err(error) => json!({
                "name": lookup.name(),
                "exercises": lookup.exercises(),
                "expected": lookup.expected(),
                "error": format!("{error:?}"),
            }),
        });
    }
    Json(json!({"shapes": shapes, "count": shapes.len()}))
}

async fn metrics(State(app): State<Arc<App>>) -> Json<Value> {
    let m = app.lib.metrics();
    Json(json!({
        "engine": engine_json(&app.lib.engine()),
        "cache": {
            "running": m.running,
            "lag_ms": m.lag.map(|lag| lag.as_secs_f64() * 1e3),
            "entries": m.entries,
            "bytes": m.bytes,
            "answered": m.answered,
            "filled": m.filled,
            "unsupported": m.unsupported,
            "not_ready": m.not_ready,
            "fallbacks": m.fallbacks,
            "fallback_errors": m.fallback_errors,
            "evicted": m.evicted,
            "restarts": m.restarts,
        },
        "budget_bytes": app.options.max_bytes,
        "max_lag_ms": app.options.max_lag_ms,
        "lab": app.options.lab,
        "service": app.telemetry.snapshot(),
        "writer": {"settings": lock(&app.writer).clone(), "counts": app.counts.json()},
        "verification": lock(&app.verification).clone(),
        "pool": {"connections": app.pool.state().connections, "idle": app.pool.state().idle_connections},
        "timing": "the read and Diesel's decoding inside the service; pool wait and HTTP time are separate",
    }))
}

async fn events(
    State(app): State<Arc<App>>,
) -> Sse<impl futures_util::Stream<Item = Result<SseEvent, std::convert::Infallible>>> {
    let receiver = app.telemetry.events.subscribe();
    let owner = Arc::downgrade(&app);
    let stream =
        futures_util::stream::unfold((receiver, owner), |(mut receiver, owner)| async move {
            loop {
                if owner.upgrade().is_none_or(|app| app.stopping()) {
                    return None;
                }
                let Ok(result) =
                    tokio::time::timeout(Duration::from_secs(1), receiver.recv()).await
                else {
                    continue;
                };
                let event = match result {
                    Ok(event) => SseEvent::default()
                        .id(event.id.to_string())
                        .event("activity")
                        .data(serde_json::to_string(&event).unwrap_or_else(|_| "{}".into())),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        SseEvent::default().event("gap").data(skipped.to_string())
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                };
                return Some((Ok(event), (receiver, owner)));
            }
        });
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(10)))
}

async fn writer_settings(State(app): State<Arc<App>>) -> Json<Value> {
    Json(
        json!({"settings": lock(&app.writer).clone(), "counts": app.counts.json(), "operations": writer::OPS}),
    )
}

async fn set_writer(
    State(app): State<Arc<App>>,
    Json(settings): Json<WriterSettings>,
) -> Result<Json<Value>, ApiError> {
    if settings.rate > 100
        || settings.operations.is_empty()
        || settings
            .operations
            .iter()
            .any(|op| !writer::OPS.contains(&op.as_str()))
    {
        return Err(ApiError::BadRequest(
            "Choose at least one writer operation and a rate from 0 to 100.".into(),
        ));
    }
    *lock(&app.writer) = settings.clone();
    app.telemetry.emit(
        "writer",
        if settings.enabled {
            format!("Background writes set to {} per second.", settings.rate)
        } else {
            "Background writes paused.".into()
        },
        json!({"settings": settings}),
    );
    Ok(Json(json!({"settings": settings})))
}

#[derive(Default, Deserialize)]
struct WriteOptions {
    #[serde(default)]
    wait: bool,
}

#[derive(Deserialize)]
struct NewTicket {
    org_id: i64,
    title: String,
    #[serde(default)]
    priority: Option<TicketPriority>,
}

#[derive(Deserialize)]
struct StatusChange {
    status: TicketStatus,
}

#[derive(Deserialize)]
struct NewComment {
    body: String,
}

/// A committed write, and, when asked, when memory showed it.
#[derive(Serialize)]
struct Written {
    id: i64,
    ticket_id: i64,
    committed: bool,
    waited: bool,
    /// Whether a read through Streamgres showed the change within five
    /// seconds; `None` when not asked.
    visible: Option<bool>,
    /// How long after the commit it did.
    visible_after_us: Option<f64>,
    /// The ticket as read through Streamgres after the write.
    observed: Option<Answer>,
    observation_error: Option<String>,
}

/// How long a write's visibility is waited for.
const VISIBLE_WITHIN: Duration = Duration::from_secs(5);

/// Reads `lookup` through Streamgres until `shows` holds for its rows or
/// [`VISIBLE_WITHIN`] passes: whether it did, and after how long.
async fn wait_visible(
    app: &App,
    lookup: &Lookup,
    shows: impl Fn(&Value) -> bool,
) -> (bool, Duration) {
    let started = Instant::now();
    loop {
        if let Ok(answer) = app.read(lookup, Route::Streamgres).await
            && answer.served == "cache"
            && shows(&answer.data)
        {
            return (true, started.elapsed());
        }
        if started.elapsed() >= VISIBLE_WITHIN || app.stopping() {
            return (false, started.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The response to a committed write: the commit must not be reported as a
/// failure (which would invite a duplicate insert), so a failed
/// observation is reported apart.
async fn written(
    app: &App,
    id: i64,
    ticket_id: i64,
    visibility: Option<(bool, Duration)>,
) -> Json<Written> {
    app.telemetry.emit(
        "write",
        format!("Ticket #{ticket_id} changed in Postgres."),
        json!({"ticket_id": ticket_id, "visible_after_us": visibility.map(|(_, after)| after.as_secs_f64() * 1e6)}),
    );
    let (observed, observation_error) = match app
        .read(&Lookup::Ticket { id: ticket_id }, Route::Streamgres)
        .await
    {
        Ok(answer) => (Some(answer), None),
        Err(error) => (None, Some(format!("{error:?}"))),
    };
    Json(Written {
        id,
        ticket_id,
        committed: true,
        waited: visibility.is_some(),
        visible: visibility.map(|(visible, _)| visible),
        visible_after_us: visibility.map(|(_, after)| after.as_secs_f64() * 1e6),
        observed,
        observation_error,
    })
}

/// A connection for a write, or busy.
async fn write_conn(app: &App) -> Result<db::Conn<'_>, ApiError> {
    app.pool.get().await.map_err(|_| ApiError::Busy)
}

async fn create_ticket(
    State(app): State<Arc<App>>,
    Query(options): Query<WriteOptions>,
    Json(input): Json<NewTicket>,
) -> Result<Json<Written>, ApiError> {
    let title = input.title.trim().to_owned();
    if title.is_empty() || title.len() > 160 || input.org_id <= 0 {
        return Err(ApiError::BadRequest(
            "Use a title of 1–160 bytes and a valid organization.".into(),
        ));
    }
    let id: i64 = diesel::insert_into(schema::tickets::table)
        .values((
            schema::tickets::id.eq(diesel::dsl::sql::<diesel::sql_types::BigInt>(
                "nextval('tickets_id_seq')",
            )),
            schema::tickets::org_id.eq(input.org_id),
            schema::tickets::title.eq(title),
            schema::tickets::status.eq(TicketStatus::Open),
            schema::tickets::priority.eq(input.priority.unwrap_or(TicketPriority::Normal)),
        ))
        .returning(schema::tickets::id)
        .get_result(&mut write_conn(&app).await?)
        .await?;
    let visibility = if options.wait {
        Some(
            wait_visible(&app, &Lookup::Card { id }, |rows| {
                rows.as_array().is_some_and(|rows| !rows.is_empty())
            })
            .await,
        )
    } else {
        None
    };
    Ok(written(&app, id, id, visibility).await)
}

async fn update_status(
    State(app): State<Arc<App>>,
    Path(id): Path<i64>,
    Query(options): Query<WriteOptions>,
    Json(input): Json<StatusChange>,
) -> Result<Json<Written>, ApiError> {
    let changed = diesel::update(schema::tickets::table.filter(schema::tickets::id.eq(id)))
        .set((
            schema::tickets::status.eq(input.status),
            schema::tickets::updated_at
                .eq(diesel::dsl::sql::<diesel::sql_types::Timestamptz>("now()")),
        ))
        .execute(&mut write_conn(&app).await?)
        .await?;
    if changed == 0 {
        return Err(ApiError::NotFound("That ticket does not exist.".into()));
    }
    let visibility = if options.wait {
        let status = input.status;
        Some(
            wait_visible(&app, &Lookup::Card { id }, move |rows| {
                serde_json::from_value::<Vec<TicketCardView>>(rows.clone()).is_ok_and(|cards| {
                    <[TicketCardView]>::first(&cards).is_some_and(|card| card.status == status)
                })
            })
            .await,
        )
    } else {
        None
    };
    Ok(written(&app, id, id, visibility).await)
}

/// A card's status, as the card read returns it.
#[derive(Deserialize)]
struct TicketCardView {
    status: TicketStatus,
}

async fn add_comment(
    State(app): State<Arc<App>>,
    Path(ticket_id): Path<i64>,
    Query(options): Query<WriteOptions>,
    Json(input): Json<NewComment>,
) -> Result<Json<Written>, ApiError> {
    let body = input.body.trim().to_owned();
    if body.is_empty() || body.len() > 10000 {
        return Err(ApiError::BadRequest(
            "Use a comment of 1–10000 bytes.".into(),
        ));
    }
    let id: i64 = diesel::insert_into(schema::comments::table)
        .values((
            schema::comments::id.eq(diesel::dsl::sql::<diesel::sql_types::BigInt>(
                "nextval('comments_id_seq')",
            )),
            schema::comments::ticket_id.eq(ticket_id),
            schema::comments::body.eq(body),
        ))
        .returning(schema::comments::id)
        .get_result(&mut write_conn(&app).await?)
        .await?;
    let visibility = if options.wait {
        Some(
            wait_visible(
                &app,
                &Lookup::CommentAuthors { ticket: ticket_id },
                move |rows| {
                    rows.as_array().is_some_and(|rows| {
                        rows.iter()
                            .any(|row| row.get("comment_id").and_then(Value::as_i64) == Some(id))
                    })
                },
            )
            .await,
        )
    } else {
        None
    };
    Ok(written(&app, id, ticket_id, visibility).await)
}

pub async fn serve(dsn: String, options: Options) -> Result<(), String> {
    // Container orchestrators send SIGTERM; terminals send SIGINT. Both
    // stop Streamgres (its slot dropped) and close the activity streams.
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|error| format!("Installing the shutdown handler: {error}"))?;
    let listen = options.listen;
    let app = start(dsn, options).await?;
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|error| format!("Listening: {error}"))?;
    println!(
        "Ticket desk: http://{}",
        listener.local_addr().map_err(|error| error.to_string())?
    );
    let shutdown = app.clone();
    axum::serve(listener, router(app))
        .with_graceful_shutdown(async move {
            #[cfg(unix)]
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = terminate.recv() => {},
            }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
            shutdown.shutdown().await;
        })
        .await
        .map_err(|error| error.to_string())
}
