//! The HTTP face: one endpoint per lookup, a write, and the stats.
//!
//! Every read answers with the rows, which route it took, how long it
//! took, and whether it came from the cache. The last is read off the
//! cache's hit counter before and after the query, which is exact when
//! one request runs at a time and a good guess under load; the counters
//! on `/stats` are the exact figures.
//!
//! Query parameters on every read:
//!
//! - `route=postgres` or `route=streamgres` flags the query itself,
//!   which beats the pool's route.
//! - `pool=reporting` uses the Postgres-routed pool instead of the main
//!   one.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::models::TicketStatus;
use crate::pools::Stack;
use crate::queries::Lookup;
use crate::schema::tickets;

pub type App = Arc<Stack>;

pub fn router(stack: App) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/stats", get(stats))
        .route("/lookups", get(lookups))
        .route("/tickets/{id}", get(ticket))
        .route("/tickets/{id}/comments", get(comments))
        .route("/tickets/{id}/status", post(set_status))
        .route("/attachments/{id}", get(attachment))
        .route("/orgs/{org}/queue", get(queue))
        .route("/orgs/{org}/queue/count", get(queue_count))
        .route("/orgs/{org}/agents", get(agents))
        .route("/orgs/{org}/sla", get(sla))
        .route("/orgs/{org}/search", get(search))
        .route("/orgs/{org}/recent", get(recent))
        .with_state(stack)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

/// The counters: the cache's and the engine client's.
async fn stats(State(app): State<App>) -> Json<Value> {
    Json(stats_json(&app))
}

pub fn stats_json(app: &Stack) -> Value {
    let s = &app.cache.stats;
    let n = |counter: &AtomicU64| AtomicU64::load(counter, Ordering::Relaxed);
    json!({
        "cache": {
            "hits": n(&s.hits),
            "misses": n(&s.misses),
            "touched": n(&s.touched),
            "replaced": n(&s.replaced),
            "refused": n(&s.refused),
            "refused_recently": n(&s.refused_recently),
            "after_write": n(&s.after_write),
            "locking": n(&s.locking),
            "stale_fills": n(&s.stale_fills),
            "evicted": n(&s.evicted),
            "expired": n(&s.expired),
            "oversized": n(&s.oversized),
            "entries": app.cache.len(),
            "bytes": app.cache.weighted_size(),
        },
        "engine": {
            "connected": app.engine.is_connected(),
            "connections": app.engine.connections(),
            "subscribed": app.engine.subscribed(),
            "materialized": app.engine.materialized(),
            "store_bytes": app.engine.store_bytes(),
            "per_worker": app.engine.per_worker(),
            "fallbacks": app.engine.fallbacks(),
            "refusals": app.engine.refusals(),
        },
    })
}

/// The lookups and what each exercises.
async fn lookups() -> Json<Value> {
    Json(Value::Array(
        Lookup::demo()
            .into_iter()
            .map(|lookup| json!({"name": lookup.name(), "exercises": lookup.exercises(), "args": lookup}))
            .collect(),
    ))
}

#[derive(Deserialize, Default)]
struct ReadParams {
    route: Option<String>,
    pool: Option<String>,
    status: Option<String>,
    page: Option<i64>,
    q: Option<String>,
    limit: Option<i64>,
}

impl ReadParams {
    fn statuses(&self) -> Vec<TicketStatus> {
        match &self.status {
            Some(list) => list.split(',').filter_map(TicketStatus::parse).collect(),
            None => vec![TicketStatus::Open, TicketStatus::Pending],
        }
    }
}

type Reply = Result<Json<Value>, (StatusCode, String)>;

/// Runs a lookup on the pool and route the parameters ask for, and
/// reports what happened.
async fn read(app: &Stack, params: &ReadParams, lookup: Lookup) -> Reply {
    let pool = match params.pool.as_deref() {
        Some("reporting") => &app.reporting,
        _ => &app.main,
    };
    let mut conn = pool.get().await.map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?;
    let route = match params.route.as_deref() {
        Some("postgres") => Some(streamgres_diesel::Route::Postgres),
        Some("streamgres") => Some(streamgres_diesel::Route::Streamgres),
        _ => None,
    };
    let hits_before = AtomicU64::load(&app.cache.stats.hits, Ordering::Relaxed);
    let started = Instant::now();
    // The query flag for one request: set on the pooled connection for
    // the read and put back before the connection returns to the pool.
    let pool_route = conn.route();
    if let Some(route) = route {
        conn.set_route(route);
    }
    let rows = lookup.run_async(&mut conn).await;
    conn.set_route(pool_route);
    let rows = rows.map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let elapsed = started.elapsed();
    let hit = AtomicU64::load(&app.cache.stats.hits, Ordering::Relaxed) > hits_before;
    Ok(Json(json!({
        "lookup": lookup.name(),
        "route": route.map(|route| format!("{route:?}").to_lowercase()).unwrap_or_else(|| match params.pool.as_deref() {
            Some("reporting") => "postgres (reporting pool)".to_string(),
            _ => "streamgres (main pool)".to_string(),
        }),
        "source": if hit { "cache" } else { "postgres" },
        "micros": elapsed.as_micros(),
        "rows": rows,
    })))
}

async fn ticket(State(app): State<App>, Path(id): Path<i64>, Query(params): Query<ReadParams>) -> Reply {
    read(&app, &params, Lookup::Ticket { id }).await
}

async fn comments(State(app): State<App>, Path(id): Path<i64>, Query(params): Query<ReadParams>) -> Reply {
    read(&app, &params, Lookup::Comments { ticket: id }).await
}

async fn attachment(State(app): State<App>, Path(id): Path<i64>, Query(params): Query<ReadParams>) -> Reply {
    read(&app, &params, Lookup::Attachment { id }).await
}

async fn queue(State(app): State<App>, Path(org): Path<i64>, Query(params): Query<ReadParams>) -> Reply {
    let lookup = Lookup::Queue {
        org,
        statuses: params.statuses(),
        page: params.page.unwrap_or(0),
    };
    read(&app, &params, lookup).await
}

async fn queue_count(State(app): State<App>, Path(org): Path<i64>, Query(params): Query<ReadParams>) -> Reply {
    let lookup = Lookup::QueueCount {
        org,
        statuses: params.statuses(),
    };
    read(&app, &params, lookup).await
}

async fn agents(State(app): State<App>, Path(org): Path<i64>, Query(params): Query<ReadParams>) -> Reply {
    read(&app, &params, Lookup::Agents { org }).await
}

async fn sla(State(app): State<App>, Path(org): Path<i64>, Query(params): Query<ReadParams>) -> Reply {
    read(&app, &params, Lookup::Sla { org }).await
}

async fn search(State(app): State<App>, Path(org): Path<i64>, Query(params): Query<ReadParams>) -> Reply {
    let pattern = format!("%{}%", params.q.clone().unwrap_or_else(|| "login".to_string()));
    read(&app, &params, Lookup::Search { org, pattern }).await
}

async fn recent(State(app): State<App>, Path(org): Path<i64>, Query(params): Query<ReadParams>) -> Reply {
    let lookup = Lookup::Recent {
        org,
        limit: params.limit.unwrap_or(5),
    };
    read(&app, &params, lookup).await
}

#[derive(Deserialize)]
struct StatusChange {
    status: TicketStatus,
}

/// A write through the main pool: the cache marks it, and for the
/// read-your-writes window every read through the cache goes to
/// Postgres, so the caller sees its own change at once.
async fn set_status(State(app): State<App>, Path(id): Path<i64>, Json(change): Json<StatusChange>) -> Reply {
    let mut conn = app.main.get().await.map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?;
    let changed = diesel::update(tickets::table.filter(tickets::id.eq(id)))
        .set((tickets::status.eq(change.status), tickets::updated_at.eq(diesel::dsl::now)))
        .execute(&mut conn)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if changed == 0 {
        return Err((StatusCode::NOT_FOUND, format!("no ticket {id}")));
    }
    Ok(Json(json!({"id": id, "status": change.status, "read_your_writes_ms": streamgres_diesel::read_your_writes_from_env().as_millis()})))
}

/// For handlers that take a map of parameters rather than the struct.
#[allow(dead_code)]
fn param<'a>(params: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    params.get(name).map(String::as_str)
}

impl IntoResponse for Lookup {
    fn into_response(self) -> axum::response::Response {
        Json(self).into_response()
    }
}
