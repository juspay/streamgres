//! The reads the ticket desk does, as one list: each is a named query
//! with a known shape, run through Streamgres or straight on Postgres.
//!
//! A shape that matters to the engine (an enum bound as an array, an
//! offset page, a join, a column of a type the engine does not hold
//! exactly, an operator it refuses) is here so it is exercised the day it
//! changes. Each says whether memory is expected to answer it
//! ([`Lookup::expected`]); the verify loop holds every shape to it.

use chrono::{DateTime, TimeZone, Utc};
use diesel::deserialize::FromSqlRow;
use diesel::dsl::{AsSelect, InnerJoinQuerySource, LeftJoinQuerySource};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::{BoxedSelectStatement, FromClause, QueryFragment, QueryId};
use diesel::query_dsl::CompatibleType;
use diesel::sql_types::{BigInt, Int8, Interval, Nullable, Text, Timestamptz};
use diesel_async::RunQueryDsl;
use diesel_async::methods::LoadQuery;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use streamgres_diesel::{LoadAndSync, Served, Streamgres, Trace};

use crate::db::{Conn, Pool};
use crate::models::{
    Agent, Attachment, CommentAuthor, Ticket, TicketCard, TicketComment, TicketKind, TicketOfKind,
    TicketPriority, TicketSla, TicketStatus, TicketStatusType, Workload,
};
use crate::schema::{agents, attachments, comments, tickets, tickets_with_kind};

/// Tickets per page of a queue.
pub const PAGE: i64 = 10;

/// Who is expected to answer a shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Expected {
    /// Memory, from the rows the engine keeps.
    Cache,
    /// Postgres, through the fallback: Streamgres could answer the shape
    /// but not yet (values its encoder cannot write until the exact-value
    /// engine work lands), or the read never reaches Streamgres (raw SQL).
    Postgres,
    /// Nobody: Streamgres can never answer the shape, and the read is an
    /// error.
    Unsupported,
}

/// Where a read goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Route {
    /// `load_and_sync`: memory when the engine answers, Postgres otherwise.
    Streamgres,
    /// `.load`: Postgres.
    Postgres,
}

/// One read's way to the data, and what it found out.
pub struct Ctx<'a> {
    pub lib: &'a Streamgres,
    pub pool: &'a Pool,
    pub route: Route,
    /// How the read through Streamgres went.
    pub trace: Option<Trace>,
    /// How long the read waited for a Postgres connection.
    pub pool_wait: std::time::Duration,
}

impl<'a> Ctx<'a> {
    pub fn new(lib: &'a Streamgres, pool: &'a Pool, route: Route) -> Self {
        Ctx {
            lib,
            pool,
            route,
            trace: None,
            pool_wait: std::time::Duration::ZERO,
        }
    }

    /// Who answered: memory, or Postgres.
    pub fn from_memory(&self) -> bool {
        self.trace
            .as_ref()
            .is_some_and(|trace| trace.served == Served::Memory)
    }

    /// A connection of the pool, its wait counted.
    async fn conn(&mut self) -> QueryResult<Conn<'a>> {
        let waiting = std::time::Instant::now();
        let conn = self.pool.get().await.map_err(|error| {
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::UnableToSendCommand,
                Box::new(error.to_string()),
            )
        });
        self.pool_wait += waiting.elapsed();
        conn
    }
}

/// One read of the ticket desk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "lookup", rename_all = "snake_case")]
pub enum Lookup {
    Ticket {
        id: i64,
    },
    Card {
        id: i64,
    },
    Queue {
        org: i64,
        statuses: Vec<TicketStatus>,
        page: i64,
    },
    QueueCards {
        org: i64,
        statuses: Vec<TicketStatus>,
        page: i64,
    },
    QueueCount {
        org: i64,
        statuses: Vec<TicketStatus>,
    },
    Comments {
        ticket: i64,
    },
    Attachment {
        id: i64,
    },
    Agents {
        org: i64,
    },
    Sla {
        org: i64,
    },
    Search {
        org: i64,
        pattern: String,
    },
    Recent {
        org: i64,
        limit: i64,
    },
    OrgTicket {
        org: i64,
        id: i64,
    },
    Unassigned {
        org: i64,
    },
    Escalated {
        org: i64,
    },
    OpenExcept {
        org: i64,
        except: Vec<i64>,
    },
    CreatedBetween {
        org: i64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    Many {
        ids: Vec<i64>,
    },
    QueueEnd {
        org: i64,
    },
    Nothing {
        org: i64,
    },
    AllTickets {
        org: i64,
    },
    CommentAuthors {
        ticket: i64,
    },
    Skilled {
        org: i64,
        skill: String,
    },
    Statuses {
        org: i64,
    },
    Workload {
        org: i64,
    },
    LastActivity {
        org: i64,
    },
    OfKind {
        org: i64,
        kind: TicketKind,
    },
}

impl Lookup {
    /// The lookup's name.
    pub fn name(&self) -> &'static str {
        match self {
            Lookup::Ticket { .. } => "ticket",
            Lookup::Card { .. } => "card",
            Lookup::Queue { .. } => "queue",
            Lookup::QueueCards { .. } => "queue_cards",
            Lookup::QueueCount { .. } => "queue_count",
            Lookup::Comments { .. } => "comments",
            Lookup::Attachment { .. } => "attachment",
            Lookup::Agents { .. } => "agents",
            Lookup::Sla { .. } => "sla",
            Lookup::Search { .. } => "search",
            Lookup::Recent { .. } => "recent",
            Lookup::OrgTicket { .. } => "org_ticket",
            Lookup::Unassigned { .. } => "unassigned",
            Lookup::Escalated { .. } => "escalated",
            Lookup::OpenExcept { .. } => "open_except",
            Lookup::CreatedBetween { .. } => "created_between",
            Lookup::Many { .. } => "many",
            Lookup::QueueEnd { .. } => "queue_end",
            Lookup::Nothing { .. } => "nothing",
            Lookup::AllTickets { .. } => "all_tickets",
            Lookup::CommentAuthors { .. } => "comment_authors",
            Lookup::Skilled { .. } => "skilled",
            Lookup::Statuses { .. } => "statuses",
            Lookup::Workload { .. } => "workload",
            Lookup::LastActivity { .. } => "last_activity",
            Lookup::OfKind { .. } => "of_kind",
        }
    }

    /// What the shape exercises.
    pub fn exercises(&self) -> &'static str {
        match self {
            Lookup::Ticket { .. } => "primary key, enum columns, timestamptz columns",
            Lookup::Card { .. } => "primary key, enum columns, no times",
            Lookup::Queue { .. } => "enum array bind, ORDER BY timestamptz DESC, LIMIT, OFFSET",
            Lookup::QueueCards { .. } => "enum array bind, ORDER BY id DESC, LIMIT, OFFSET",
            Lookup::QueueCount { .. } => "COUNT(*) with an enum array bind",
            Lookup::Comments { .. } => "inner join, timestamptz column",
            Lookup::Attachment { .. } => "bytea column",
            Lookup::Agents { .. } => "text[] column",
            Lookup::Sla { .. } => "interval column",
            Lookup::Search { .. } => "ILIKE",
            Lookup::Recent { .. } => "ORDER BY updated_at DESC LIMIT",
            Lookup::OrgTicket { .. } => "two-column condition",
            Lookup::Unassigned { .. } => "IS NULL",
            Lookup::Escalated { .. } => "OR of two enum binds",
            Lookup::OpenExcept { .. } => "<> and != ALL",
            Lookup::CreatedBetween { .. } => "timestamp range, two binds",
            Lookup::Many { .. } => "= ANY of a hundred ids",
            Lookup::QueueEnd { .. } => "page past the end: stays empty",
            Lookup::Nothing { .. } => "never any rows: stays empty",
            Lookup::AllTickets { .. } => "largest result",
            Lookup::CommentAuthors { .. } => "left join with a null side",
            Lookup::Skilled { .. } => "array @>",
            Lookup::Statuses { .. } => "DISTINCT",
            Lookup::Workload { .. } => "GROUP BY, raw SQL",
            Lookup::LastActivity { .. } => "MAX",
            Lookup::OfKind { .. } => "enum column added after the engine started",
        }
    }

    /// Who is expected to answer the shape: memory for the shapes whose
    /// every column and operator the engine holds exactly in v1, Postgres
    /// for the rest (times, bytea, arrays, intervals, and the operators
    /// it refuses). Always-empty results need no value encoding.
    pub fn expected(&self) -> Expected {
        match self {
            Lookup::Card { .. }
            | Lookup::QueueCards { .. }
            | Lookup::QueueCount { .. }
            | Lookup::CommentAuthors { .. }
            | Lookup::Nothing { .. } => Expected::Cache,
            // Ordering or comparing a timestamptz, a column type the engine
            // does not hold, an operator it refuses, or (until a restart)
            // a column added after it started.
            Lookup::Queue { .. }
            | Lookup::QueueEnd { .. }
            | Lookup::Recent { .. }
            | Lookup::CreatedBetween { .. }
            | Lookup::Attachment { .. }
            | Lookup::Agents { .. }
            | Lookup::Sla { .. }
            | Lookup::Search { .. }
            | Lookup::Skilled { .. }
            | Lookup::Statuses { .. }
            | Lookup::LastActivity { .. }
            | Lookup::OfKind { .. } => Expected::Unsupported,
            // Returning a timestamptz (encoded once main's exact values
            // land), and raw SQL.
            _ => Expected::Postgres,
        }
    }

    /// Whether the result stays the same under the writer.
    pub fn is_static(&self) -> bool {
        matches!(self, Lookup::QueueEnd { .. } | Lookup::Nothing { .. })
    }

    /// The demo's lookups with their default arguments.
    pub fn demo() -> Vec<Lookup> {
        vec![
            Lookup::Ticket { id: 1 },
            Lookup::Card { id: 1 },
            Lookup::Queue {
                org: 1,
                statuses: vec![TicketStatus::Open, TicketStatus::Pending],
                page: 1,
            },
            Lookup::QueueCards {
                org: 1,
                statuses: vec![TicketStatus::Open, TicketStatus::Pending],
                page: 1,
            },
            Lookup::QueueCount {
                org: 1,
                statuses: vec![TicketStatus::Open],
            },
            Lookup::Comments { ticket: 1 },
            Lookup::Attachment { id: 1 },
            Lookup::Agents { org: 1 },
            Lookup::Sla { org: 1 },
            Lookup::Search {
                org: 1,
                pattern: "%login%".to_string(),
            },
            Lookup::Recent { org: 1, limit: 5 },
            Lookup::OrgTicket { org: 1, id: 5 },
            Lookup::Unassigned { org: 1 },
            Lookup::Escalated { org: 1 },
            Lookup::OpenExcept {
                org: 1,
                except: vec![1, 2, 3],
            },
            Lookup::CreatedBetween {
                org: 1,
                from: Utc.with_ymd_and_hms(2026, 10, 1, 9, 0, 0).unwrap(),
                to: Utc.with_ymd_and_hms(2026, 10, 1, 12, 0, 0).unwrap(),
            },
            Lookup::Many {
                ids: (1..=100).collect(),
            },
            Lookup::QueueEnd { org: 1 },
            Lookup::Nothing { org: 1 },
            Lookup::AllTickets { org: 1 },
            Lookup::CommentAuthors { ticket: 1 },
            Lookup::Skilled {
                org: 1,
                skill: "billing".to_string(),
            },
            Lookup::Statuses { org: 1 },
            Lookup::Workload { org: 1 },
            Lookup::LastActivity { org: 1 },
        ]
    }

    /// A lookup by name: the demo list, plus the one that needs the
    /// migration.
    pub fn named(name: &str) -> Option<Lookup> {
        if name == "of_kind" {
            return Some(Lookup::OfKind {
                org: 1,
                kind: TicketKind::Bug,
            });
        }
        Self::demo()
            .into_iter()
            .find(|lookup| lookup.name() == name)
    }

    /// Runs the lookup on `ctx`'s route; `ctx.trace` says how a read
    /// through Streamgres went.
    pub async fn run(&self, ctx: &mut Ctx<'_>) -> QueryResult<Value> {
        match self {
            Lookup::Ticket { id } => json(load(ctx, ticket(*id)).await?),
            Lookup::Card { id } => json(load(ctx, card(*id)).await?),
            Lookup::Queue {
                org,
                statuses,
                page,
            } => json(load(ctx, queue(*org, statuses.clone(), *page)).await?),
            Lookup::QueueCards {
                org,
                statuses,
                page,
            } => json(load(ctx, queue_cards(*org, statuses.clone(), *page)).await?),
            Lookup::QueueCount { org, statuses } => {
                json(load::<_, i64>(ctx, queue_count(*org, statuses.clone())).await?)
            }
            Lookup::Comments { ticket } => json(comments_of(
                load::<_, CommentRow>(ctx, ticket_comments(*ticket)).await?,
            )),
            Lookup::Attachment { id } => json(load(ctx, attachment(*id)).await?),
            Lookup::Agents { org } => json(load(ctx, org_agents(*org)).await?),
            Lookup::Sla { org } => json(
                load(ctx, sla(*org))
                    .await?
                    .into_iter()
                    .map(TicketSla::from_row)
                    .collect::<Vec<_>>(),
            ),
            Lookup::Search { org, pattern } => {
                json(load(ctx, search(*org, pattern.clone())).await?)
            }
            Lookup::Recent { org, limit } => json(load(ctx, recent(*org, *limit)).await?),
            Lookup::OrgTicket { org, id } => json(load(ctx, org_ticket(*org, *id)).await?),
            Lookup::Unassigned { org } => json(load(ctx, unassigned(*org)).await?),
            Lookup::Escalated { org } => json(load(ctx, escalated(*org)).await?),
            Lookup::OpenExcept { org, except } => {
                json(load(ctx, open_except(*org, except.clone())).await?)
            }
            Lookup::CreatedBetween { org, from, to } => {
                json(load(ctx, created_between(*org, *from, *to)).await?)
            }
            Lookup::Many { ids } => json(load(ctx, many(ids.clone())).await?),
            Lookup::QueueEnd { org } => json(load(ctx, queue_end(*org)).await?),
            Lookup::Nothing { org } => json(load(ctx, nothing(*org)).await?),
            Lookup::AllTickets { org } => json(load(ctx, all_tickets(*org)).await?),
            Lookup::CommentAuthors { ticket } => json(authors_of(
                load::<_, AuthorRow>(ctx, comment_authors(*ticket)).await?,
            )),
            Lookup::Skilled { org, skill } => json(load(ctx, skilled(*org, skill.clone())).await?),
            Lookup::Statuses { org } => json(load::<_, TicketStatus>(ctx, statuses(*org)).await?),
            // Raw SQL is not a query Diesel can render for Streamgres: it
            // always runs on Postgres.
            Lookup::Workload { org } => {
                let mut conn = ctx.conn().await?;
                json(postgres::<_, Workload>(&mut conn, workload(*org)).await?)
            }
            Lookup::LastActivity { org } => {
                json(load::<_, Option<DateTime<Utc>>>(ctx, last_activity(*org)).await?)
            }
            Lookup::OfKind { org, kind } => json(load(ctx, of_kind(*org, *kind)).await?),
        }
    }
}

/// One read as the desk reports it: who answered, why not memory, how
/// long it took, and the rows.
#[derive(Debug, Clone, Serialize)]
pub struct Answer {
    pub lookup: &'static str,
    pub route: Route,
    /// `cache` when memory answered, `postgres` when Postgres did, and
    /// `unsupported` when Streamgres can never answer the shape.
    pub served: &'static str,
    /// What the trace says: `memory`, `postgres` or `failed`.
    pub answered_by: Option<String>,
    /// Why the engine did not answer, when it did not.
    pub refusal: Option<String>,
    pub elapsed_us: f64,
    pub pool_wait_us: f64,
    pub rows: usize,
    pub data: Value,
    pub trace: Option<TraceView>,
}

/// A [`Trace`] as JSON.
#[derive(Debug, Clone, Serialize)]
pub struct TraceView {
    pub query: String,
    pub sql: String,
    pub ttl: Option<String>,
    pub position: Option<String>,
    pub lag_ms: Option<f64>,
    pub took_us: f64,
    pub at: String,
}

impl TraceView {
    pub fn of(trace: &Trace) -> Self {
        TraceView {
            query: trace.query.to_string(),
            sql: trace.sql.clone(),
            ttl: trace.ttl.map(|ttl| ttl.to_string()),
            position: trace.position.clone(),
            lag_ms: trace.lag.map(|lag| lag.as_secs_f64() * 1e3),
            took_us: trace.took.as_secs_f64() * 1e6,
            at: trace.at.to_string(),
        }
    }
}

/// Reads `lookup` on `route`.
pub async fn read(
    lib: &Streamgres,
    pool: &Pool,
    lookup: &Lookup,
    route: Route,
) -> QueryResult<Answer> {
    let mut ctx = Ctx::new(lib, pool, route);
    let started = std::time::Instant::now();
    let data = lookup.run(&mut ctx).await;
    let elapsed = started.elapsed().saturating_sub(ctx.pool_wait);
    let unsupported = ctx
        .trace
        .as_ref()
        .is_some_and(|trace| trace.served == Served::Unsupported);
    // An unsupported shape is an error by design: reported, not raised.
    let data = if unsupported { Value::Null } else { data? };
    let memory = ctx.from_memory();
    Ok(Answer {
        lookup: lookup.name(),
        route,
        served: if unsupported {
            "unsupported"
        } else if memory {
            "cache"
        } else {
            "postgres"
        },
        answered_by: ctx.trace.as_ref().map(|trace| trace.served.to_string()),
        refusal: match (&ctx.trace, route) {
            (Some(trace), _) => trace.reason.clone(),
            // Raw SQL is never handed to Streamgres.
            (None, Route::Streamgres) => {
                Some("raw SQL: not a query Streamgres can read".to_owned())
            }
            (None, Route::Postgres) => None,
        },
        elapsed_us: elapsed.as_secs_f64() * 1e6,
        pool_wait_us: ctx.pool_wait.as_secs_f64() * 1e6,
        rows: data.as_array().map_or(0, Vec::len),
        data,
        trace: ctx.trace.as_ref().map(TraceView::of),
    })
}

impl Lookup {
    /// Where the lookup's query stands in Streamgres
    /// ([`streamgres_diesel::Inspect`]); `None` for raw SQL, which is never
    /// handed to it.
    pub fn inspect(&self, lib: &Streamgres) -> QueryResult<Option<streamgres_diesel::Inspect>> {
        Ok(Some(match self {
            Lookup::Ticket { id } => ticket(*id).inspect(lib)?,
            Lookup::Card { id } => card(*id).inspect(lib)?,
            Lookup::Queue {
                org,
                statuses,
                page,
            } => queue(*org, statuses.clone(), *page).inspect(lib)?,
            Lookup::QueueCards {
                org,
                statuses,
                page,
            } => queue_cards(*org, statuses.clone(), *page).inspect(lib)?,
            Lookup::QueueCount { org, statuses } => {
                queue_count(*org, statuses.clone()).inspect(lib)?
            }
            Lookup::Comments { ticket } => ticket_comments(*ticket).inspect(lib)?,
            Lookup::Attachment { id } => attachment(*id).inspect(lib)?,
            Lookup::Agents { org } => org_agents(*org).inspect(lib)?,
            Lookup::Sla { org } => sla(*org).inspect(lib)?,
            Lookup::Search { org, pattern } => search(*org, pattern.clone()).inspect(lib)?,
            Lookup::Recent { org, limit } => recent(*org, *limit).inspect(lib)?,
            Lookup::OrgTicket { org, id } => org_ticket(*org, *id).inspect(lib)?,
            Lookup::Unassigned { org } => unassigned(*org).inspect(lib)?,
            Lookup::Escalated { org } => escalated(*org).inspect(lib)?,
            Lookup::OpenExcept { org, except } => open_except(*org, except.clone()).inspect(lib)?,
            Lookup::CreatedBetween { org, from, to } => {
                created_between(*org, *from, *to).inspect(lib)?
            }
            Lookup::Many { ids } => many(ids.clone()).inspect(lib)?,
            Lookup::QueueEnd { org } => queue_end(*org).inspect(lib)?,
            Lookup::Nothing { org } => nothing(*org).inspect(lib)?,
            Lookup::AllTickets { org } => all_tickets(*org).inspect(lib)?,
            Lookup::CommentAuthors { ticket } => comment_authors(*ticket).inspect(lib)?,
            Lookup::Skilled { org, skill } => skilled(*org, skill.clone()).inspect(lib)?,
            Lookup::Statuses { org } => statuses(*org).inspect(lib)?,
            Lookup::Workload { .. } => return Ok(None),
            Lookup::LastActivity { org } => last_activity(*org).inspect(lib)?,
            Lookup::OfKind { org, kind } => of_kind(*org, *kind).inspect(lib)?,
        }))
    }
}

/// The rows of `query` on `ctx`'s route.
async fn load<Q, U>(ctx: &mut Ctx<'_>, query: Q) -> QueryResult<Vec<U>>
where
    Q: LoadAndSync + LoadQuery<'static, diesel_async::AsyncPgConnection, U> + Send + 'static,
    Q::Query: QueryFragment<Pg> + QueryId + Send,
    Q::SqlType: CompatibleType<U, Pg> + 'static,
    U: FromSqlRow<<Q::SqlType as CompatibleType<U, Pg>>::SqlType, Pg> + Send + 'static,
{
    match ctx.route {
        Route::Streamgres => {
            let (rows, trace) = query.load_and_sync(ctx.lib).inspect().await;
            ctx.trace = trace;
            rows
        }
        Route::Postgres => {
            let mut conn = ctx.conn().await?;
            postgres(&mut conn, query).await
        }
    }
}

/// `query`'s rows straight from Postgres. The future is boxed here, where
/// its types are known, so callers can hold it across a spawn (rustc
/// cannot see a generic diesel-async load future as `Send` otherwise).
fn postgres<'c, Q, U>(
    conn: &'c mut diesel_async::AsyncPgConnection,
    query: Q,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = QueryResult<Vec<U>>> + Send + 'c>>
where
    Q: LoadQuery<'static, diesel_async::AsyncPgConnection, U> + Send + 'static,
    U: Send + 'static,
{
    Box::pin(query.load(conn))
}

/// The rows as JSON, the form the verify loop compares.
pub(crate) fn json<T: Serialize>(rows: T) -> QueryResult<Value> {
    serde_json::to_value(rows)
        .map_err(|error| diesel::result::Error::SerializationError(Box::new(error)))
}

/// Cards: tickets without their times.
type CardQuery = tickets::BoxedQuery<'static, Pg, AsSelect<TicketCard, Pg>>;
/// Tickets, as the API serves them.
type TicketQuery = tickets::BoxedQuery<'static, Pg, AsSelect<Ticket, Pg>>;
/// A count of tickets.
type CountQuery = tickets::BoxedQuery<'static, Pg, BigInt>;
/// Tickets with their `interval` SLA.
type SlaQuery = tickets::BoxedQuery<'static, Pg, (Int8, Text, Nullable<Interval>)>;
/// Agents.
type AgentsQuery = agents::BoxedQuery<'static, Pg, AsSelect<Agent, Pg>>;
/// An attachment, `bytea` and all.
type AttachmentQuery = attachments::BoxedQuery<'static, Pg, AsSelect<Attachment, Pg>>;
/// Ticket statuses.
type StatusesQuery = tickets::BoxedQuery<'static, Pg, TicketStatusType>;
/// The latest update time.
type LastActivityQuery = tickets::BoxedQuery<'static, Pg, Nullable<Timestamptz>>;
/// Tickets with the kind a migration adds.
type OfKindQuery = tickets_with_kind::BoxedQuery<'static, Pg, AsSelect<TicketOfKind, Pg>>;
/// Tickets joined to their comments.
type CommentsQuery = BoxedSelectStatement<
    'static,
    (Int8, Text, Int8, Text, Timestamptz),
    FromClause<InnerJoinQuerySource<tickets::table, comments::table>>,
    Pg,
>;
/// Comments left-joined to their authors.
type AuthorsQuery = BoxedSelectStatement<
    'static,
    (Int8, Text, Nullable<Text>),
    FromClause<LeftJoinQuerySource<comments::table, agents::table>>,
    Pg,
>;
/// Raw SQL with its binds.
type WorkloadQuery =
    diesel::query_builder::BoxedSqlQuery<'static, Pg, diesel::query_builder::SqlQuery>;

/// One ticket by id.
pub(crate) fn ticket(id: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::id.eq(id))
        .select(Ticket::as_select())
        .into_boxed()
}

/// A page of an organization's queue, newest first: an enum bound as
/// an array, and an offset.
pub(crate) fn queue(org: i64, statuses: Vec<TicketStatus>, page: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::status.eq_any(statuses))
        .order((tickets::created_at.desc(), tickets::id.asc()))
        .limit(PAGE)
        .offset(page * PAGE)
        .select(Ticket::as_select())
        .into_boxed()
}

/// One ticket's card by id.
pub(crate) fn card(id: i64) -> CardQuery {
    tickets::table
        .filter(tickets::id.eq(id))
        .select(TicketCard::as_select())
        .into_boxed()
}

/// A page of an organization's queue as cards, newest first by id.
pub(crate) fn queue_cards(org: i64, statuses: Vec<TicketStatus>, page: i64) -> CardQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::status.eq_any(statuses))
        .order(tickets::id.desc())
        .limit(PAGE)
        .offset(page * PAGE)
        .select(TicketCard::as_select())
        .into_boxed()
}

/// How many tickets the queue holds: a `COUNT(*)`.
pub(crate) fn queue_count(org: i64, statuses: Vec<TicketStatus>) -> CountQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::status.eq_any(statuses))
        .count()
        .into_boxed()
}

/// A ticket's comments: an inner join, ordered by both keys.
pub(crate) fn ticket_comments(ticket: i64) -> CommentsQuery {
    tickets::table
        .inner_join(comments::table)
        .filter(tickets::id.eq(ticket))
        .order((tickets::id.asc(), comments::id.asc()))
        .select((
            tickets::id,
            tickets::title,
            comments::id,
            comments::body,
            comments::created_at,
        ))
        .into_boxed()
}

/// One attachment: a `bytea`.
pub(crate) fn attachment(id: i64) -> AttachmentQuery {
    attachments::table
        .filter(attachments::id.eq(id))
        .select(Attachment::as_select())
        .into_boxed()
}

/// An organization's agents: a `text[]` column.
pub(crate) fn org_agents(org: i64) -> AgentsQuery {
    agents::table
        .filter(agents::org_id.eq(org))
        .order(agents::id.asc())
        .select(Agent::as_select())
        .into_boxed()
}

/// Tickets with an SLA: an `interval` the wrapper cannot encode.
pub(crate) fn sla(org: i64) -> SlaQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::sla.is_not_null())
        .order(tickets::id.asc())
        .select((tickets::id, tickets::title, tickets::sla))
        .into_boxed()
}

/// Tickets whose title matches: `ILIKE`, which the engine refuses.
pub(crate) fn search(org: i64, pattern: String) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::title.ilike(pattern))
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

/// The most recently updated tickets: a limit.
pub(crate) fn recent(org: i64, limit: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .order((tickets::updated_at.desc(), tickets::id.asc()))
        .limit(limit)
        .select(Ticket::as_select())
        .into_boxed()
}

/// One ticket by organization and id: a two-column condition.
pub(crate) fn org_ticket(org: i64, id: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::id.eq(id))
        .select(Ticket::as_select())
        .into_boxed()
}

/// Tickets with no agent: `IS NULL`.
pub(crate) fn unassigned(org: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::agent_id.is_null())
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

/// High-priority or pending tickets: an `OR`.
pub(crate) fn escalated(org: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(
            tickets::priority
                .eq(TicketPriority::High)
                .or(tickets::status.eq(TicketStatus::Pending)),
        )
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

/// Open tickets but the named ones: `!= ALL`.
pub(crate) fn open_except(org: i64, except: Vec<i64>) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::status.ne(TicketStatus::Closed))
        .filter(tickets::id.ne_all(except))
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

/// Tickets created in a range: a timestamp range.
pub(crate) fn created_between(org: i64, from: DateTime<Utc>, to: DateTime<Utc>) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::created_at.ge(from))
        .filter(tickets::created_at.lt(to))
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

/// Tickets by id: an `IN` list of up to a hundred.
pub(crate) fn many(ids: Vec<i64>) -> TicketQuery {
    tickets::table
        .filter(tickets::id.eq_any(ids))
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

/// A page past the end of the queue: an empty result.
pub(crate) fn queue_end(org: i64) -> TicketQuery {
    queue(org, TicketStatus::ALL.to_vec(), 1000)
}

/// A condition no ticket meets: an empty result.
pub(crate) fn nothing(org: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::id.lt(0))
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

/// Every ticket of an organization.
pub(crate) fn all_tickets(org: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

/// A ticket's comments and their authors: a left join.
pub(crate) fn comment_authors(ticket: i64) -> AuthorsQuery {
    comments::table
        .left_join(agents::table)
        .filter(comments::ticket_id.eq(ticket))
        .order(comments::id.asc())
        .select((comments::id, comments::body, agents::name.nullable()))
        .into_boxed()
}

/// Agents with a skill: the array operator `@>`, which the engine
/// refuses.
pub(crate) fn skilled(org: i64, skill: String) -> AgentsQuery {
    agents::table
        .filter(agents::org_id.eq(org))
        .filter(agents::skills.contains(vec![skill]))
        .order(agents::id.asc())
        .select(Agent::as_select())
        .into_boxed()
}

/// The statuses in use: `DISTINCT`, which the engine refuses.
pub(crate) fn statuses(org: i64) -> StatusesQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .select(tickets::status)
        .distinct()
        .order(tickets::status.asc())
        .into_boxed()
}

/// Tickets per agent: `GROUP BY`, which the engine refuses.
pub(crate) fn workload(org: i64) -> WorkloadQuery {
    diesel::sql_query(
        "SELECT agent_id, COUNT(*) AS tickets FROM tickets WHERE org_id = $1 \
         GROUP BY agent_id ORDER BY agent_id",
    )
    .into_boxed()
    .bind::<BigInt, _>(org)
}

/// The latest update: `MAX`, which the engine refuses.
pub(crate) fn last_activity(org: i64) -> LastActivityQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .select(diesel::dsl::max(tickets::updated_at))
        .into_boxed()
}

/// Tickets of a kind: an enum created after the engine started.
pub(crate) fn of_kind(org: i64, kind: TicketKind) -> OfKindQuery {
    tickets_with_kind::table
        .filter(tickets_with_kind::org_id.eq(org))
        .filter(tickets_with_kind::kind.eq(kind))
        .order(tickets_with_kind::id.asc())
        .select(TicketOfKind::as_select())
        .into_boxed()
}

/// A row of the comments join, as the tuple Diesel decodes.
pub type CommentRow = (i64, String, i64, String, DateTime<Utc>);

/// The join's rows as the struct the API serves.
pub fn comments_of(rows: Vec<CommentRow>) -> Vec<TicketComment> {
    rows.into_iter()
        .map(
            |(ticket_id, title, comment_id, body, created_at)| TicketComment {
                ticket_id,
                title,
                comment_id,
                body,
                created_at,
            },
        )
        .collect()
}

/// A row of the left join, as the tuple Diesel decodes.
pub type AuthorRow = (i64, String, Option<String>);

/// The left join's rows as the struct the API serves.
pub fn authors_of(rows: Vec<AuthorRow>) -> Vec<CommentAuthor> {
    rows.into_iter()
        .map(|(comment_id, body, agent)| CommentAuthor {
            comment_id,
            body,
            agent,
        })
        .collect()
}
