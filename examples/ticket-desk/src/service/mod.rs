//! HTTP showcase: bounded pools, actual cache outcomes, and guarded lab controls.
mod lab;
mod pools;
mod telemetry;

use crate::models::{Organization, TicketPriority, TicketStatus};
use crate::queries::{Ctx, Lookup, Route, Served};
use crate::{
    asynchronous, schema, seed,
    stack::{self, Stack},
    writer,
};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::sse::{Event as SseEvent, KeepAlive};
use axum::response::{Html, IntoResponse, Response, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use telemetry::{Telemetry, lock};
use tokio::sync::Semaphore;

#[derive(Clone, clap::Args)]
pub struct Options {
    #[arg(long, default_value = "127.0.0.1:8080", env = "DEMO_LISTEN")]
    pub listen: SocketAddr,
    /// Enables changes to schemas and this demo's own replication slot.
    #[arg(long, env = "DEMO_LAB")]
    pub lab: bool,
    #[arg(long, default_value_t = 8, env = "DEMO_POOL_SIZE", value_parser = clap::value_parser!(u32).range(1..=32))]
    pub pool_size: u32,
    #[arg(long, default_value_t = 8, env = "DEMO_WRITER_RATE", value_parser = clap::value_parser!(u32).range(0..=100))]
    pub writer_rate: u32,
    #[arg(long, default_value_t = 268435456, env = "DEMO_MAX_BYTES")]
    pub max_bytes: usize,
    #[arg(long, default_value_t = 2000, env = "DEMO_MAX_LAG_MS")]
    pub max_lag_ms: u64,
    #[arg(long, default_value_t = 1, env = "DEMO_ADMIT_AFTER")]
    pub admit_after: u32,
    /// Replaces the demo's data before serving. Use only a demo database.
    #[arg(long)]
    pub seed: bool,
    /// Initializes a missing demo schema without replacing existing data.
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
            other => Self::Database(format!(
                "Postgres could not complete the operation: {other}"
            )),
        }
    }
}
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Adapter {
    #[default]
    Sync,
    Async,
}
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ReadOptions {
    pub route: Option<String>,
    #[serde(default)]
    pub adapter: Adapter,
    pub org: Option<i64>,
    pub id: Option<i64>,
    pub ticket: Option<i64>,
    pub page: Option<i64>,
    pub statuses: Option<String>,
    pub pattern: Option<String>,
    pub limit: Option<i64>,
}
impl ReadOptions {
    fn route(&self) -> Result<Route, ApiError> {
        match self.route.as_deref() {
            None | Some("cached") | Some("cache") => Ok(Route::Cached),
            Some("postgres") => Ok(Route::Postgres),
            _ => Err(ApiError::BadRequest(
                "route must be cached or postgres".into(),
            )),
        }
    }
}
#[derive(Clone, Serialize)]
pub struct Answer {
    pub lookup: String,
    pub adapter: Adapter,
    pub route: &'static str,
    pub served: Served,
    pub refusal: Option<String>,
    pub elapsed_us: f64,
    pub pool_wait_us: f64,
    pub rows: usize,
    pub data: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct WriterSettings {
    pub enabled: bool,
    pub rate: u32,
    pub operations: Vec<String>,
}
pub struct App {
    pub(crate) stack: Arc<Stack>,
    pub(crate) reads: pools::Pool,
    pub(crate) writes: pools::Pool,
    pub(crate) truth: pools::Pool,
    pub(crate) async_reads: pools::AsyncPool,
    pub(crate) jobs: Arc<Semaphore>,
    pub(crate) telemetry: Telemetry,
    pub(crate) writer: Mutex<WriterSettings>,
    pub(crate) writer_session: Mutex<writer::Session>,
    pub(crate) counts: Arc<writer::Counts>,
    pub(crate) verification: Mutex<lab::Verification>,
    pub(crate) scenario: tokio::sync::Mutex<()>,
    pub(crate) stop: AtomicBool,
    pub(crate) options: Options,
    pub(crate) engine_label: String,
}
/// A task keeps its admission permit even if the HTTP caller disconnects.
pub(crate) async fn blocking<T, F>(app: Arc<App>, operation: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(Arc<App>) -> Result<T, ApiError> + Send + 'static,
{
    let permit = app
        .jobs
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError::Busy)?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation(app)
    })
    .await
    .map_err(|error| ApiError::Internal(format!("The service worker ended: {error}")))?
}
fn labeled_url(dsn: &str, label: &str) -> Result<String, String> {
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        let mut url = url::Url::parse(dsn).map_err(|error| format!("DATABASE_URL: {error}"))?;
        let pairs: Vec<_> = url
            .query_pairs()
            .filter(|(key, _)| key != "application_name" && key != "connect_timeout")
            .map(|(key, val)| (key.into_owned(), val.into_owned()))
            .collect();
        url.set_query(None);
        url.query_pairs_mut()
            .extend_pairs(pairs)
            .append_pair("application_name", label)
            .append_pair("connect_timeout", "3");
        Ok(url.into())
    } else {
        Ok(format!(
            "{dsn} application_name='{label}' connect_timeout=3"
        ))
    }
}
pub async fn start(dsn: String, options: Options) -> Result<Arc<App>, String> {
    let engine_label = format!("ticket-desk-{}", uuid::Uuid::new_v4().simple());
    let cache_dsn = labeled_url(&dsn, &engine_label)?;
    let pool_dsn = labeled_url(&dsn, "ticket-desk-requests")?;
    let config_options = options.clone();
    let ordinary_dsn = pool_dsn.clone();
    let (stack, reads, writes, truth) = tokio::task::spawn_blocking(move || {
        let mut config = stack::config(&cache_dsn);
        config.max_bytes = config_options.max_bytes;
        config.max_entry_bytes = config.max_entry_bytes.min(config.max_bytes);
        config.max_lag = Duration::from_millis(config_options.max_lag_ms);
        config.admit_after = config_options.admit_after;
        if config_options.seed || config_options.init {
            let mut conn = diesel::PgConnection::establish(&ordinary_dsn)
                .map_err(|error| format!("Connect to the demo database: {error}"))?;
            conn.batch_execute(seed::SCHEMA)
                .map_err(|error| format!("Schema: {error}"))?;
            let existing: i64 = schema::organizations::table
                .count()
                .get_result(&mut conn)
                .map_err(|error| format!("Existing fixture: {error}"))?;
            if config_options.seed || existing == 0 {
                seed::run(&mut conn).map_err(|error| format!("Seed: {error}"))?;
            }
            stack::install_database(&mut conn, &config)?;
        }
        let stack = Arc::new(Stack::start_config(
            &ordinary_dsn,
            config,
            Duration::from_secs(30),
        )?);
        let reads = pools::create(&ordinary_dsn, config_options.pool_size, true)?;
        let writes = pools::create(&ordinary_dsn, 2, false)?;
        let truth = pools::create(&ordinary_dsn, 4, false)?;
        Ok::<_, String>((stack, reads, writes, truth))
    })
    .await
    .map_err(|error| error.to_string())??;
    let async_reads = pools::create_async(&pool_dsn, options.pool_size).await?;
    let app = Arc::new(App {
        stack,
        reads,
        writes,
        truth,
        async_reads,
        jobs: Arc::new(Semaphore::new(options.pool_size as usize + 12)),
        telemetry: Telemetry::default(),
        writer: Mutex::new(WriterSettings {
            enabled: options.writer_rate > 0,
            rate: options.writer_rate,
            operations: writer::OPS.iter().map(|s| (*s).into()).collect(),
        }),
        writer_session: Mutex::new(writer::Session::default()),
        counts: Arc::new(writer::Counts::default()),
        verification: Mutex::new(lab::Verification::default()),
        scenario: tokio::sync::Mutex::new(()),
        stop: AtomicBool::new(false),
        options,
        engine_label,
    });
    lab::background(&app);
    app.telemetry.emit(
        "engine",
        "Ticket desk is connected to Streamgres and Postgres.".into(),
        json!({}),
    );
    Ok(app)
}
impl App {
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::Release);
        self.stack.cache.stop();
    }
    pub(crate) async fn read(
        self: &Arc<Self>,
        lookup: Lookup,
        route: Route,
        adapter: Adapter,
        explain: bool,
    ) -> Result<Answer, ApiError> {
        validate(&lookup)?;
        let name = lookup.name().to_owned();
        let answer = match adapter {
            Adapter::Sync => {
                blocking(self.clone(), move |app| {
                    let waiting = Instant::now();
                    let pool = if route == Route::Postgres {
                        &app.truth
                    } else {
                        &app.reads
                    };
                    let mut conn = pool.get().map_err(|_| ApiError::Busy)?;
                    let pool_wait_us = waiting.elapsed().as_secs_f64() * 1e6;
                    let mut ctx = Ctx::new(&mut conn, &app.stack.cache, route);
                    ctx.explain = explain;
                    let started = Instant::now();
                    let data = match lookup.run(&mut ctx) {
                        Ok(data) => data,
                        Err(error) => {
                            pools::discard(&mut conn);
                            return Err(error.into());
                        }
                    };
                    Ok(Answer {
                        lookup: lookup.name().into(),
                        adapter,
                        route: route_name(route),
                        served: ctx.served,
                        refusal: ctx.refusal,
                        elapsed_us: started.elapsed().as_secs_f64() * 1e6,
                        pool_wait_us,
                        rows: data.as_array().map_or(0, Vec::len),
                        data,
                        sql: ctx.sql,
                    })
                })
                .await?
            }
            Adapter::Async => {
                let _permit = self
                    .jobs
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| ApiError::Busy)?;
                let waiting = Instant::now();
                let mut conn = self.async_reads.get().await.map_err(|_| ApiError::Busy)?;
                let pool_wait_us = waiting.elapsed().as_secs_f64() * 1e6;
                let mut ctx =
                    asynchronous::Context::new(&mut conn, self.stack.cache.clone(), route);
                let started = Instant::now();
                let data = asynchronous::run(&lookup, &mut ctx).await?;
                Answer {
                    lookup: name,
                    adapter,
                    route: route_name(route),
                    served: ctx.served,
                    refusal: ctx.refusal,
                    elapsed_us: started.elapsed().as_secs_f64() * 1e6,
                    pool_wait_us,
                    rows: data.as_array().map_or(0, Vec::len),
                    data,
                    sql: None,
                }
            }
        };
        let key = format!(
            "{}:{}:{}",
            answer.lookup,
            if adapter == Adapter::Sync {
                "sync"
            } else {
                "async"
            },
            route_name(route)
        );
        self.telemetry.read(
            &key,
            answer.served,
            route == Route::Cached,
            answer.elapsed_us,
            answer.refusal.as_deref(),
        );
        self.telemetry.emit("read", format!("{}: {} in {:.1} µs", answer.lookup.replace('_', " "), if answer.served == Served::Cache { "answered from memory" } else { "answered by Postgres" }, answer.elapsed_us), json!({"lookup": answer.lookup, "served": answer.served, "adapter": adapter, "elapsed_us": answer.elapsed_us, "refusal": answer.refusal}));
        Ok(answer)
    }
}
fn route_name(route: Route) -> &'static str {
    if route == Route::Cached {
        "cached"
    } else {
        "postgres"
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
        Lookup::Queue { page, statuses, .. }
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
        .route(
            "/fonts/{name}",
            get(|Path(name): Path<String>| async move {
                let font: Option<&'static [u8]> = match name.as_str() {
                    "google-sans-flex-latin.woff2" => {
                        Some(include_bytes!("assets/fonts/google-sans-flex-latin.woff2"))
                    }
                    "manrope-latin.woff2" => {
                        Some(include_bytes!("assets/fonts/manrope-latin.woff2"))
                    }
                    "geist-mono-latin.woff2" => {
                        Some(include_bytes!("assets/fonts/geist-mono-latin.woff2"))
                    }
                    "licenses.txt" => {
                        return (
                            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                            include_str!("assets/fonts/LICENSES.txt"),
                        )
                            .into_response();
                    }
                    _ => None,
                };
                match font {
                    Some(bytes) => ([(header::CONTENT_TYPE, "font/woff2")], bytes).into_response(),
                    None => StatusCode::NOT_FOUND.into_response(),
                }
            }),
        )
        .route("/api/metrics", get(metrics))
        .route("/api/lookups", get(lookups))
        .route("/api/query/{name}", get(named_query))
        .route("/api/compare/{name}", get(lab::compare_named))
        .route("/api/query", post(query))
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
        .route("/api/verify/stream", get(events))
        .route("/api/load", post(lab::load))
        .route("/api/lab/{scenario}", post(lab::scenario))
        .layer(DefaultBodyLimit::max(65536))
        .layer(axum::middleware::from_fn(
            |request: axum::extract::Request, next: axum::middleware::Next| async move {
                if request.uri().path().starts_with("/api/lab/") {
                    let local = request
                        .headers()
                        .get(header::HOST)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|host| host.parse::<axum::http::uri::Authority>().ok())
                        .is_some_and(|host| {
                            matches!(host.host(), "127.0.0.1" | "localhost" | "[::1]" | "::1")
                        });
                    if !local {
                        return ApiError::BadRequest(
                            "Fault experiments are available only through a loopback host.".into(),
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
                    .and_then(|v| v.to_str().ok())
                {
                    let host = request
                        .headers()
                        .get(header::HOST)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("");
                    let expected = format!("http://{host}");
                    if origin != expected {
                        return ApiError::BadRequest(
                            "Cross-origin changes are not accepted.".into(),
                        )
                        .into_response();
                    }
                }
                let mut response = next.run(request).await;
                response.headers_mut().insert(
                    header::CACHE_CONTROL,
                    axum::http::HeaderValue::from_static("no-store"),
                );
                response
            },
        ))
        .with_state(app)
}
async fn query(
    State(app): State<Arc<App>>,
    Query(options): Query<ReadOptions>,
    Json(lookup): Json<Lookup>,
) -> Result<Json<Answer>, ApiError> {
    app.read(lookup, options.route()?, options.adapter, false)
        .await
        .map(Json)
}
async fn named_query(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Answer>, ApiError> {
    let mut lookup = Lookup::named(&name)
        .ok_or_else(|| ApiError::NotFound("That query shape does not exist.".into()))?;
    match &mut lookup {
        Lookup::Ticket { id } | Lookup::Attachment { id } => {
            if let Some(v) = options.id {
                *id = v;
            }
        }
        Lookup::Queue {
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
            if let Some(v) = &options.pattern {
                *pattern = v.clone();
            }
        }
        Lookup::Comments { ticket } | Lookup::CommentAuthors { ticket } => {
            *ticket = options.ticket.unwrap_or(*ticket);
        }
        _ => {}
    }
    app.read(lookup, options.route()?, options.adapter, true)
        .await
        .map(Json)
}
async fn ticket(
    State(app): State<Arc<App>>,
    Path(id): Path<i64>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Answer>, ApiError> {
    app.read(
        Lookup::Ticket { id },
        options.route()?,
        options.adapter,
        false,
    )
    .await
    .map(Json)
}
async fn comments(
    State(app): State<Arc<App>>,
    Path(id): Path<i64>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Answer>, ApiError> {
    app.read(
        Lookup::CommentAuthors { ticket: id },
        options.route()?,
        options.adapter,
        false,
    )
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
    app.read(
        Lookup::Queue {
            org,
            statuses,
            page: options.page.unwrap_or(1),
        },
        options.route()?,
        options.adapter,
        false,
    )
    .await
    .map(Json)
}
async fn agents(
    State(app): State<Arc<App>>,
    Path(org): Path<i64>,
    Query(options): Query<ReadOptions>,
) -> Result<Json<Answer>, ApiError> {
    app.read(
        Lookup::Agents { org },
        options.route()?,
        options.adapter,
        false,
    )
    .await
    .map(Json)
}
async fn organizations(State(app): State<Arc<App>>) -> Result<Json<Vec<Organization>>, ApiError> {
    blocking(app, |app| {
        let mut conn = app.truth.get().map_err(|_| ApiError::Busy)?;
        Ok(Json(
            schema::organizations::table
                .select(Organization::as_select())
                .order(schema::organizations::id)
                .load(&mut conn)?,
        ))
    })
    .await
}
async fn lookups(State(app): State<Arc<App>>) -> Result<Json<Value>, ApiError> {
    let mut shapes = Vec::new();
    for lookup in Lookup::demo() {
        let answer = app
            .read(lookup.clone(), Route::Cached, Adapter::Sync, true)
            .await;
        shapes.push(match answer {
            Ok(answer) => json!({"name": lookup.name(), "exercises": lookup.exercises(), "expected": lookup.expected(), "served": answer.served, "refusal": answer.refusal, "sql": answer.sql, "elapsed_us": answer.elapsed_us}),
            Err(error) => json!({"name": lookup.name(), "exercises": lookup.exercises(), "expected": lookup.expected(), "error": format!("{error:?}")}),
        });
    }
    Ok(Json(json!({"shapes": shapes, "count": shapes.len()})))
}
async fn metrics(State(app): State<Arc<App>>) -> Json<Value> {
    let m = app.stack.cache.metrics();
    Json(
        json!({"instance": app.engine_label, "cache": {"running": m.running, "lag_ms": m.lag.map(|lag| lag.as_secs_f64()*1000.0), "entries": m.entries, "bytes": m.bytes, "answered": m.answered, "filled": m.filled, "unsupported": m.unsupported, "not_ready": m.not_ready, "not_admitted": m.not_admitted, "unusable": m.unusable, "evicted": m.evicted, "restarts": m.restarts}, "budget_bytes": app.options.max_bytes, "max_lag_ms": app.options.max_lag_ms, "admit_after": app.options.admit_after, "lab": app.options.lab, "service": app.telemetry.snapshot(), "writer": {"settings": lock(&app.writer).clone(), "counts": app.counts.json()}, "verification": lock(&app.verification).clone(), "pools": {"read": app.reads.state().connections, "read_idle": app.reads.state().idle_connections, "write": app.writes.state().connections, "truth": app.truth.state().connections}, "timing": "Diesel query, adapter and result serialization inside the service; pool wait and HTTP time are separate"}),
    )
}
async fn events(
    State(app): State<Arc<App>>,
) -> Sse<impl futures_util::Stream<Item = Result<SseEvent, std::convert::Infallible>>> {
    let receiver = app.telemetry.events.subscribe();
    let owner = Arc::downgrade(&app);
    let stream =
        futures_util::stream::unfold((receiver, owner), |(mut receiver, owner)| async move {
            loop {
                if owner
                    .upgrade()
                    .is_none_or(|app| app.stop.load(Ordering::Acquire))
                {
                    return None;
                }
                let result =
                    match tokio::time::timeout(Duration::from_secs(1), receiver.recv()).await {
                        Ok(result) => result,
                        Err(_) => continue,
                    };
                match result {
                    Ok(event) => {
                        let data = serde_json::to_string(&event).unwrap_or_else(|_| "{}".into());
                        return Some((
                            Ok(SseEvent::default()
                                .id(event.id.to_string())
                                .event("activity")
                                .data(data)),
                            (receiver, owner),
                        ));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        return Some((
                            Ok(SseEvent::default().event("gap").data(skipped.to_string())),
                            (receiver, owner),
                        ));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
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
#[derive(Serialize)]
struct Written {
    id: i64,
    ticket_id: i64,
    committed: bool,
    waited: bool,
    caught_up: Option<bool>,
    wait_us: f64,
    observed: Option<Answer>,
    observation_error: Option<String>,
    wait_error: Option<String>,
}
// The mutation has already committed. A barrier failure must not turn its
// response into a retryable write failure (which could duplicate an insert).
struct Visibility {
    caught_up: Option<bool>,
    elapsed_us: f64,
    error: Option<String>,
}
fn wait_after(conn: &mut PgConnection, app: &App, wait: bool) -> Visibility {
    let started = Instant::now();
    let (caught_up, error) = if wait {
        match streamgres_diesel::wait_for_writes(&app.stack.cache, conn, Duration::from_secs(5)) {
            Ok(caught) => (Some(caught), None),
            Err(error) => (Some(false), Some(error.to_string())),
        }
    } else {
        (None, None)
    };
    Visibility {
        caught_up,
        elapsed_us: started.elapsed().as_secs_f64() * 1e6,
        error,
    }
}
async fn finish_write(
    app: Arc<App>,
    id: i64,
    ticket_id: i64,
    wait: bool,
    visibility: Visibility,
) -> Json<Written> {
    app.telemetry.emit(
        "write",
        format!(
            "Ticket #{ticket_id} changed in Postgres{}.",
            if wait {
                " and a cache write barrier was checked"
            } else {
                ""
            }
        ),
        json!({"ticket_id": ticket_id, "caught_up": visibility.caught_up, "wait_us": visibility.elapsed_us, "wait_error": visibility.error}),
    );
    let (observed, observation_error) = match app
        .read(
            Lookup::Ticket { id: ticket_id },
            Route::Cached,
            Adapter::Sync,
            false,
        )
        .await
    {
        Ok(answer) => (Some(answer), None),
        Err(error) => (None, Some(format!("{error:?}"))),
    };
    Json(Written {
        id,
        ticket_id,
        committed: true,
        waited: wait,
        caught_up: visibility.caught_up,
        wait_us: visibility.elapsed_us,
        wait_error: visibility.error,
        observed,
        observation_error,
    })
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
    let wait = options.wait;
    let (id, visibility) = blocking(app.clone(), move |app| {
        let mut conn = app.writes.get().map_err(|_| ApiError::Busy)?;
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
            .get_result(&mut conn)?;
        let visibility = wait_after(&mut conn, &app, wait);
        Ok((id, visibility))
    })
    .await?;
    Ok(finish_write(app, id, id, wait, visibility).await)
}
async fn update_status(
    State(app): State<Arc<App>>,
    Path(id): Path<i64>,
    Query(options): Query<WriteOptions>,
    Json(input): Json<StatusChange>,
) -> Result<Json<Written>, ApiError> {
    let wait = options.wait;
    let visibility = blocking(app.clone(), move |app| {
        let mut conn = app.writes.get().map_err(|_| ApiError::Busy)?;
        let changed = diesel::update(schema::tickets::table.filter(schema::tickets::id.eq(id)))
            .set((
                schema::tickets::status.eq(input.status),
                schema::tickets::updated_at
                    .eq(diesel::dsl::sql::<diesel::sql_types::Timestamptz>("now()")),
            ))
            .execute(&mut conn)?;
        if changed == 0 {
            return Err(ApiError::NotFound("That ticket does not exist.".into()));
        }
        Ok(wait_after(&mut conn, &app, wait))
    })
    .await?;
    Ok(finish_write(app, id, id, wait, visibility).await)
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
    let wait = options.wait;
    let (id, visibility) = blocking(app.clone(), move |app| {
        let mut conn = app.writes.get().map_err(|_| ApiError::Busy)?;
        let id: i64 = diesel::insert_into(schema::comments::table)
            .values((
                schema::comments::id.eq(diesel::dsl::sql::<diesel::sql_types::BigInt>(
                    "nextval('comments_id_seq')",
                )),
                schema::comments::ticket_id.eq(ticket_id),
                schema::comments::body.eq(body),
            ))
            .returning(schema::comments::id)
            .get_result(&mut conn)?;
        let visibility = wait_after(&mut conn, &app, wait);
        Ok((id, visibility))
    })
    .await?;
    Ok(finish_write(app, id, ticket_id, wait, visibility).await)
}
pub async fn serve(dsn: String, options: Options) -> Result<(), String> {
    // Container orchestrators send SIGTERM; terminals normally send SIGINT.
    // Both must stop the cache and close activity streams before exiting.
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|error| format!("Installing shutdown handler: {error}"))?;
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
            shutdown.stop.store(true, Ordering::Release);
            let _ = tokio::task::spawn_blocking(move || shutdown.shutdown()).await;
        })
        .await
        .map_err(|error| error.to_string())
}
