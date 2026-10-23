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
//!
//! `/verify` reads every lookup through both routes and reports the
//! pairs that never agreed; `/stats` carries the verify loop's and the
//! writer's counters next to the cache's and the engine's.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeZone, Utc};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use diesel_streamgres::Served;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::models::{TicketKind, TicketStatus};
use crate::pools::Stack;
use crate::queries::Lookup;
use crate::schema::tickets;
use crate::verify;

/// What every handler shares.
pub type App = Arc<Stack>;

/// Every route of the demo's HTTP API.
pub fn router(stack: App) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/stats", get(stats))
        .route("/lookups", get(lookups))
        .route("/verify", get(verify_once))
        .route("/tickets", get(many))
        .route("/tickets/{id}", get(ticket))
        .route("/tickets/{id}/comments", get(comments))
        .route("/tickets/{id}/authors", get(comment_authors))
        .route("/tickets/{id}/status", post(set_status))
        .route("/attachments/{id}", get(attachment))
        .route("/orgs/{org}/queue", get(queue))
        .route("/orgs/{org}/queue/count", get(queue_count))
        .route("/orgs/{org}/queue/end", get(queue_end))
        .route("/orgs/{org}/agents", get(agents))
        .route("/orgs/{org}/sla", get(sla))
        .route("/orgs/{org}/search", get(search))
        .route("/orgs/{org}/recent", get(recent))
        .route("/orgs/{org}/tickets", get(all_tickets))
        .route("/orgs/{org}/tickets/{id}", get(org_ticket))
        .route("/orgs/{org}/unassigned", get(unassigned))
        .route("/orgs/{org}/escalated", get(escalated))
        .route("/orgs/{org}/open", get(open_except))
        .route("/orgs/{org}/created", get(created_between))
        .route("/orgs/{org}/nothing", get(nothing))
        .route("/orgs/{org}/skilled", get(skilled))
        .route("/orgs/{org}/statuses", get(statuses))
        .route("/orgs/{org}/workload", get(workload))
        .route("/orgs/{org}/activity", get(last_activity))
        .route("/orgs/{org}/kind/{kind}", get(of_kind))
        .with_state(stack)
}

/// The stats page.
async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

/// The counters: the cache's, the engine client's, the writer's and
/// the verify loop's.
async fn stats(State(app): State<App>) -> Json<Value> {
    Json(stats_json(&app))
}

/// One pass of the verify loop, with every pair that never agreed.
async fn verify_once(State(app): State<App>) -> Json<Value> {
    let outcome = verify::once(&app, &Lookup::demo(), Duration::from_secs(1)).await;
    Json(json!({
        "compared": outcome.compared,
        "lagged": outcome.lagged,
        "mismatches": outcome.mismatches,
        "live_disagreed": outcome.live_disagreed,
        "totals": app.verify.json(),
    }))
}

/// The counters as JSON, for `/stats` and the stats page.
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
            "lagging": n(&s.lagging),
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
            "lag_ms": app.engine.lag().map(|lag| lag.as_millis() as u64),
            "store_bytes": app.engine.store_bytes(),
            "per_worker": app.engine.per_worker(),
            "fallbacks": app.engine.fallbacks(),
            "refusals": app.engine.refusals(),
        },
        "writer": app.writer.json(),
        "verify": app.verify.json(),
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

/// A read's query parameters: the route and pool to take, and each
/// lookup's own arguments.
#[derive(Deserialize, Default)]
struct ReadParams {
    route: Option<String>,
    pool: Option<String>,
    status: Option<String>,
    page: Option<i64>,
    q: Option<String>,
    limit: Option<i64>,
    except: Option<String>,
    from: Option<String>,
    to: Option<String>,
    ids: Option<String>,
    skill: Option<String>,
}

impl ReadParams {
    /// The statuses asked for, open and pending unless named.
    fn statuses(&self) -> Vec<TicketStatus> {
        match &self.status {
            Some(list) => list.split(',').filter_map(TicketStatus::parse).collect(),
            None => vec![TicketStatus::Open, TicketStatus::Pending],
        }
    }

    /// A comma-separated list of ids, or `default`.
    fn id_list(list: Option<&str>, default: Vec<i64>) -> Vec<i64> {
        match list {
            Some(list) => list
                .split(',')
                .filter_map(|id| id.trim().parse().ok())
                .collect(),
            None => default,
        }
    }

    /// An RFC 3339 instant, or `default`.
    fn instant(text: Option<&str>, default: DateTime<Utc>) -> DateTime<Utc> {
        text.and_then(|text| text.parse::<DateTime<Utc>>().ok())
            .unwrap_or(default)
    }
}

/// A handler's answer: the read's report, or an error status.
type Reply = Result<Json<Value>, (StatusCode, String)>;

/// Runs a lookup on the pool and route the parameters ask for, and
/// reports what happened. A route for one request is set on the pooled
/// connection for the read and put back before the connection returns
/// to the pool. Where the rows came from is read per connection, so it
/// is right under concurrent requests, where a delta of the cache's
/// shared counters would not be.
async fn read(app: &Stack, params: &ReadParams, lookup: Lookup) -> Reply {
    let pool = match params.pool.as_deref() {
        Some("reporting") => &app.reporting,
        _ => &app.main,
    };
    let mut conn = pool
        .get()
        .await
        .map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?;
    let route = match params.route.as_deref() {
        Some("postgres") => Some(diesel_streamgres::Route::Postgres),
        Some("streamgres") => Some(diesel_streamgres::Route::Streamgres),
        _ => None,
    };
    let started = Instant::now();
    let pool_route = conn.route();
    if let Some(route) = route {
        conn.set_route(route);
    }
    let rows = lookup.run_async(&mut conn).await;
    conn.set_route(pool_route);
    let rows = rows.map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let elapsed = started.elapsed();
    let source = match conn.last_served() {
        Some(Served::Cache) => "cache",
        Some(Served::Engine) => "engine",
        Some(Served::Postgres) | None => "postgres",
    };
    Ok(Json(json!({
        "lookup": lookup.name(),
        "route": route.map(|route| format!("{route:?}").to_lowercase()).unwrap_or_else(|| match params.pool.as_deref() {
            Some("reporting") => "postgres (reporting pool)".to_string(),
            _ => "streamgres (main pool)".to_string(),
        }),
        "source": source,
        "micros": elapsed.as_micros(),
        "rows": rows,
    })))
}

/// `GET /tickets/{id}`: the [`Lookup::Ticket`] lookup.
async fn ticket(
    State(app): State<App>,
    Path(id): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::Ticket { id }).await
}

/// `GET /tickets/{id}/comments`: the [`Lookup::Comments`] lookup.
async fn comments(
    State(app): State<App>,
    Path(id): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::Comments { ticket: id }).await
}

/// `GET /attachments/{id}`: the [`Lookup::Attachment`] lookup.
async fn attachment(
    State(app): State<App>,
    Path(id): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::Attachment { id }).await
}

/// `GET /orgs/{org}/queue`: the [`Lookup::Queue`] lookup.
async fn queue(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    let lookup = Lookup::Queue {
        org,
        statuses: params.statuses(),
        page: params.page.unwrap_or(0),
    };
    read(&app, &params, lookup).await
}

/// `GET /orgs/{org}/queue/count`: the [`Lookup::QueueCount`] lookup.
async fn queue_count(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    let lookup = Lookup::QueueCount {
        org,
        statuses: params.statuses(),
    };
    read(&app, &params, lookup).await
}

/// `GET /orgs/{org}/agents`: the [`Lookup::Agents`] lookup.
async fn agents(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::Agents { org }).await
}

/// `GET /orgs/{org}/sla`: the [`Lookup::Sla`] lookup.
async fn sla(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::Sla { org }).await
}

/// `GET /orgs/{org}/search`: the [`Lookup::Search`] lookup.
async fn search(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    let pattern = format!(
        "%{}%",
        params.q.clone().unwrap_or_else(|| "login".to_string())
    );
    read(&app, &params, Lookup::Search { org, pattern }).await
}

/// `GET /orgs/{org}/recent`: the [`Lookup::Recent`] lookup.
async fn recent(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    let lookup = Lookup::Recent {
        org,
        limit: params.limit.unwrap_or(5),
    };
    read(&app, &params, lookup).await
}

/// `GET /orgs/{org}/tickets/{id}`: the [`Lookup::OrgTicket`] lookup.
async fn org_ticket(
    State(app): State<App>,
    Path((org, id)): Path<(i64, i64)>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::OrgTicket { org, id }).await
}

/// `GET /orgs/{org}/unassigned`: the [`Lookup::Unassigned`] lookup.
async fn unassigned(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::Unassigned { org }).await
}

/// `GET /orgs/{org}/escalated`: the [`Lookup::Escalated`] lookup.
async fn escalated(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::Escalated { org }).await
}

/// `GET /orgs/{org}/open`: the [`Lookup::OpenExcept`] lookup.
async fn open_except(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    let except = ReadParams::id_list(params.except.as_deref(), vec![1, 2, 3]);
    read(&app, &params, Lookup::OpenExcept { org, except }).await
}

/// `GET /orgs/{org}/created`: the [`Lookup::CreatedBetween`] lookup.
async fn created_between(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    let lookup = Lookup::CreatedBetween {
        org,
        from: ReadParams::instant(
            params.from.as_deref(),
            Utc.with_ymd_and_hms(2026, 10, 1, 9, 0, 0).unwrap(),
        ),
        to: ReadParams::instant(
            params.to.as_deref(),
            Utc.with_ymd_and_hms(2026, 10, 1, 12, 0, 0).unwrap(),
        ),
    };
    read(&app, &params, lookup).await
}

/// `GET /tickets`: the [`Lookup::Many`] lookup, a hundred ids unless
/// `ids` names others.
async fn many(State(app): State<App>, Query(params): Query<ReadParams>) -> Reply {
    let ids = ReadParams::id_list(params.ids.as_deref(), (1..=100).collect());
    read(&app, &params, Lookup::Many { ids }).await
}

/// `GET /orgs/{org}/queue/end`: the [`Lookup::QueueEnd`] lookup.
async fn queue_end(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::QueueEnd { org }).await
}

/// `GET /orgs/{org}/nothing`: the [`Lookup::Nothing`] lookup.
async fn nothing(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::Nothing { org }).await
}

/// `GET /orgs/{org}/tickets`: the [`Lookup::AllTickets`] lookup.
async fn all_tickets(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::AllTickets { org }).await
}

/// `GET /tickets/{id}/authors`: the [`Lookup::CommentAuthors`] lookup.
async fn comment_authors(
    State(app): State<App>,
    Path(id): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::CommentAuthors { ticket: id }).await
}

/// `GET /orgs/{org}/skilled`: the [`Lookup::Skilled`] lookup.
async fn skilled(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    let skill = params
        .skill
        .clone()
        .unwrap_or_else(|| "billing".to_string());
    read(&app, &params, Lookup::Skilled { org, skill }).await
}

/// `GET /orgs/{org}/statuses`: the [`Lookup::Statuses`] lookup.
async fn statuses(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::Statuses { org }).await
}

/// `GET /orgs/{org}/workload`: the [`Lookup::Workload`] lookup.
async fn workload(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::Workload { org }).await
}

/// `GET /orgs/{org}/activity`: the [`Lookup::LastActivity`] lookup.
async fn last_activity(
    State(app): State<App>,
    Path(org): Path<i64>,
    Query(params): Query<ReadParams>,
) -> Reply {
    read(&app, &params, Lookup::LastActivity { org }).await
}

/// `GET /orgs/{org}/kind/{kind}`: the [`Lookup::OfKind`] lookup.
async fn of_kind(
    State(app): State<App>,
    Path((org, kind)): Path<(i64, String)>,
    Query(params): Query<ReadParams>,
) -> Reply {
    let kind = TicketKind::parse(&kind)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, format!("no kind `{kind}`")))?;
    read(&app, &params, Lookup::OfKind { org, kind }).await
}

/// The body of a status change.
#[derive(Deserialize)]
struct StatusChange {
    status: TicketStatus,
}

/// A write through the main pool: the cache marks it, and for the
/// read-your-writes window every read through the cache goes to
/// Postgres, so the caller sees its own change at once.
async fn set_status(
    State(app): State<App>,
    Path(id): Path<i64>,
    Json(change): Json<StatusChange>,
) -> Reply {
    let mut conn = app
        .main
        .get()
        .await
        .map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?;
    let changed = diesel::update(tickets::table.filter(tickets::id.eq(id)))
        .set((
            tickets::status.eq(change.status),
            tickets::updated_at.eq(diesel::dsl::now),
        ))
        .execute(&mut conn)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if changed == 0 {
        return Err((StatusCode::NOT_FOUND, format!("no ticket {id}")));
    }
    Ok(Json(
        json!({"id": id, "status": change.status, "read_your_writes_ms": diesel_streamgres::read_your_writes_from_env().as_millis()}),
    ))
}

/// For handlers that take a map of parameters rather than the struct.
#[allow(dead_code)]
fn param<'a>(params: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    params.get(name).map(String::as_str)
}

impl IntoResponse for Lookup {
    /// The lookup as JSON.
    fn into_response(self) -> axum::response::Response {
        Json(self).into_response()
    }
}
