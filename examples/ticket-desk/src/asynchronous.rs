//! Async reads use the same Diesel query constructors as the synchronous desk.
use crate::models::{TicketSla, TicketStatus, Workload as WorkloadRow};
use crate::queries::{self as q, Lookup, Route, Served};
use diesel::QueryResult;
use diesel_async::methods::LoadQuery;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use futures_util::future::BoxFuture;
use serde_json::Value;
use std::sync::Arc;
use streamgres_diesel::{AsyncLoadCached, Cache, CacheAttempt};

pub struct Context<'a> {
    pub conn: &'a mut AsyncPgConnection,
    pub cache: Arc<Cache>,
    pub route: Route,
    pub served: Served,
    pub refusal: Option<String>,
}
impl<'a> Context<'a> {
    pub fn new(conn: &'a mut AsyncPgConnection, cache: Arc<Cache>, route: Route) -> Self {
        Self {
            conn,
            cache,
            route,
            served: Served::Postgres,
            refusal: None,
        }
    }
}
// Erase each borrowed connection future before composing the dispatch. This
// avoids Rust #100013 on the example's supported compiler, without requiring
// boxed Diesel queries to implement Sync or tying their connection to 'static.
fn inspect<'a, Q, U>(
    query: Q,
    cache: Arc<Cache>,
    conn: &'a mut AsyncPgConnection,
) -> BoxFuture<'a, QueryResult<CacheAttempt<Vec<U>>>>
where
    Q: AsyncLoadCached<'static, U> + Send + 'a,
    U: Send + 'static,
{
    Box::pin(query.inspect_cached_async(cache, conn))
}
fn postgres<'a, Q, U>(
    query: Q,
    conn: &'a mut AsyncPgConnection,
) -> BoxFuture<'a, QueryResult<Vec<U>>>
where
    Q: LoadQuery<'static, AsyncPgConnection, U> + Send + 'static,
    U: Send + 'static,
{
    Box::pin(RunQueryDsl::load(query, conn))
}
fn load<'a, 'conn, Q, U, F>(
    ctx: &'a mut Context<'conn>,
    make: F,
) -> BoxFuture<'a, QueryResult<Vec<U>>>
where
    Q: AsyncLoadCached<'static, U> + LoadQuery<'static, AsyncPgConnection, U> + Send + 'static,
    U: Send + 'static,
    F: Fn() -> Q + Send + Sync + 'a,
    'conn: 'a,
{
    Box::pin(async move {
        if ctx.route == Route::Cached {
            match inspect(make(), ctx.cache.clone(), ctx.conn).await? {
                CacheAttempt::Hit(rows) => {
                    ctx.served = Served::Cache;
                    return Ok(rows);
                }
                CacheAttempt::Refused(reason) => ctx.refusal = Some(reason.to_string()),
                _ => ctx.refusal = Some("the adapter returned an unknown outcome".into()),
            }
        }
        ctx.served = Served::Postgres;
        postgres(make(), ctx.conn).await
    })
}
pub fn run<'a, 'conn>(
    lookup: &'a Lookup,
    ctx: &'a mut Context<'conn>,
) -> BoxFuture<'a, QueryResult<Value>>
where
    'conn: 'a,
{
    Box::pin(async move {
        use Lookup::*;
        match lookup {
            Ticket { id } => q::json(load(ctx, || q::ticket(*id)).await?),
            Queue {
                org,
                statuses,
                page,
            } => q::json(load(ctx, || q::queue(*org, statuses.clone(), *page)).await?),
            QueueCount { org, statuses } => {
                q::json(load::<_, i64, _>(ctx, || q::queue_count(*org, statuses.clone())).await?)
            }
            Comments { ticket } => q::json(q::comments_of(
                load::<_, q::CommentRow, _>(ctx, || q::ticket_comments(*ticket)).await?,
            )),
            Attachment { id } => q::json(load(ctx, || q::attachment(*id)).await?),
            Agents { org } => q::json(load(ctx, || q::org_agents(*org)).await?),
            Sla { org } => q::json(
                load(ctx, || q::sla(*org))
                    .await?
                    .into_iter()
                    .map(TicketSla::from_row)
                    .collect::<Vec<_>>(),
            ),
            Search { org, pattern } => {
                q::json(load(ctx, || q::search(*org, pattern.clone())).await?)
            }
            Recent { org, limit } => q::json(load(ctx, || q::recent(*org, *limit)).await?),
            OrgTicket { org, id } => q::json(load(ctx, || q::org_ticket(*org, *id)).await?),
            Unassigned { org } => q::json(load(ctx, || q::unassigned(*org)).await?),
            Escalated { org } => q::json(load(ctx, || q::escalated(*org)).await?),
            OpenExcept { org, except } => {
                q::json(load(ctx, || q::open_except(*org, except.clone())).await?)
            }
            CreatedBetween { org, from, to } => {
                q::json(load(ctx, || q::created_between(*org, *from, *to)).await?)
            }
            Many { ids } => q::json(load(ctx, || q::many(ids.clone())).await?),
            QueueEnd { org } => q::json(load(ctx, || q::queue_end(*org)).await?),
            Nothing { org } => q::json(load(ctx, || q::nothing(*org)).await?),
            AllTickets { org } => q::json(load(ctx, || q::all_tickets(*org)).await?),
            CommentAuthors { ticket } => q::json(q::authors_of(
                load::<_, q::AuthorRow, _>(ctx, || q::comment_authors(*ticket)).await?,
            )),
            Skilled { org, skill } => q::json(load(ctx, || q::skilled(*org, skill.clone())).await?),
            Statuses { org } => {
                q::json(load::<_, TicketStatus, _>(ctx, || q::statuses(*org)).await?)
            }
            LastActivity { org } => q::json(
                load::<_, Option<chrono::DateTime<chrono::Utc>>, _>(ctx, || q::last_activity(*org))
                    .await?,
            ),
            OfKind { org, kind } => q::json(load(ctx, || q::of_kind(*org, *kind)).await?),
            Workload { org } => {
                ctx.served = Served::Postgres;
                ctx.refusal = Some("raw GROUP BY SQL runs directly on Postgres".into());
                q::json(RunQueryDsl::load::<WorkloadRow>(q::workload(*org), ctx.conn).await?)
            }
        }
    })
}
