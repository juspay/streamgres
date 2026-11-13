//! The reads the ticket desk does, as one list: each is a named query
//! with a known shape, run through `load_cached` or straight on Postgres.
//!
//! A shape that matters to the adapter (an enum bound as an array, an
//! offset page, a join, a `bytea`, an `interval`, an operator the engine
//! refuses) is here so it is exercised the day it changes. Each says
//! whether the cache is expected to answer it ([`Lookup::expected`]); the
//! verify command holds every shape to it.

use chrono::{DateTime, TimeZone, Utc};
use diesel::dsl::{AsSelect, InnerJoinQuerySource, LeftJoinQuerySource};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::{AsQuery, BoxedSelectStatement, FromClause, QueryFragment, QueryId};
use diesel::query_dsl::methods::LoadQuery;
use diesel::sql_types::{BigInt, Int8, Interval, Nullable, Text, Timestamptz};
use diesel::{PgConnection, QueryResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use streamgres_diesel::{Cache, CacheAttempt, LoadCached};

use crate::models::{
    Agent, Attachment, CommentAuthor, Ticket, TicketComment, TicketKind, TicketOfKind,
    TicketPriority, TicketSla, TicketStatus, TicketStatusType, Workload,
};
use crate::schema::{agents, attachments, comments, tickets, tickets_with_kind};

/// Tickets per page of a queue.
pub const PAGE: i64 = 10;

/// Who is expected to answer a shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Expected {
    /// The cache, from the rows the engine keeps.
    Cache,
    /// Postgres: the engine refuses the shape, so `load_cached` falls
    /// back.
    Postgres,
}

/// Who answered a read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Served {
    Cache,
    Postgres,
}

/// Where a read goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// `load_cached`: the cache when it answers, Postgres otherwise.
    Cached,
    /// `.load`: Postgres.
    Postgres,
}

/// One read's connection and route, and what it found out.
pub struct Ctx<'a> {
    pub conn: &'a mut PgConnection,
    pub cache: &'a Cache,
    pub route: Route,
    /// Asks the cache directly as well and keeps its refusal.
    pub explain: bool,
    pub served: Served,
    pub refusal: Option<String>,
    /// SQL captured for query inspection, without bind values.
    pub sql: Option<String>,
}

impl<'a> Ctx<'a> {
    pub fn new(conn: &'a mut PgConnection, cache: &'a Cache, route: Route) -> Self {
        Ctx {
            conn,
            cache,
            route,
            explain: false,
            served: Served::Postgres,
            refusal: None,
            sql: None,
        }
    }
}

/// One read of the ticket desk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "lookup", rename_all = "snake_case")]
pub enum Lookup {
    Ticket {
        id: i64,
    },
    Queue {
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
            Lookup::Queue { .. } => "queue",
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
            Lookup::Ticket { .. } => "primary key, enum columns, timestamps",
            Lookup::Queue { .. } => "enum array bind, ORDER BY timestamp DESC, LIMIT, OFFSET",
            Lookup::QueueCount { .. } => "COUNT(*) with an enum array bind",
            Lookup::Comments { .. } => "inner join",
            Lookup::Attachment { .. } => "bytea",
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
            Lookup::OfKind { .. } => "enum type added after the engine started",
        }
    }

    /// Who is expected to answer the shape.
    pub fn expected(&self) -> Expected {
        match self {
            Lookup::Search { .. }
            | Lookup::Skilled { .. }
            | Lookup::Statuses { .. }
            | Lookup::Workload { .. }
            | Lookup::LastActivity { .. } => Expected::Postgres,
            _ => Expected::Cache,
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
            Lookup::Queue {
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

    /// Runs the lookup on `ctx`'s route; `ctx.served` says who answered.
    pub fn run(&self, ctx: &mut Ctx<'_>) -> QueryResult<Value> {
        match self {
            Lookup::Ticket { id } => json(load(ctx, || ticket(*id))?),
            Lookup::Queue {
                org,
                statuses,
                page,
            } => json(load(ctx, || queue(*org, statuses.clone(), *page))?),
            Lookup::QueueCount { org, statuses } => {
                json(load::<_, i64>(ctx, || queue_count(*org, statuses.clone()))?)
            }
            Lookup::Comments { ticket } => json(comments_of(load::<_, CommentRow>(ctx, || {
                ticket_comments(*ticket)
            })?)),
            Lookup::Attachment { id } => json(load(ctx, || attachment(*id))?),
            Lookup::Agents { org } => json(load(ctx, || org_agents(*org))?),
            Lookup::Sla { org } => json(
                load(ctx, || sla(*org))?
                    .into_iter()
                    .map(TicketSla::from_row)
                    .collect::<Vec<_>>(),
            ),
            Lookup::Search { org, pattern } => json(load(ctx, || search(*org, pattern.clone()))?),
            Lookup::Recent { org, limit } => json(load(ctx, || recent(*org, *limit))?),
            Lookup::OrgTicket { org, id } => json(load(ctx, || org_ticket(*org, *id))?),
            Lookup::Unassigned { org } => json(load(ctx, || unassigned(*org))?),
            Lookup::Escalated { org } => json(load(ctx, || escalated(*org))?),
            Lookup::OpenExcept { org, except } => {
                json(load(ctx, || open_except(*org, except.clone()))?)
            }
            Lookup::CreatedBetween { org, from, to } => {
                json(load(ctx, || created_between(*org, *from, *to))?)
            }
            Lookup::Many { ids } => json(load(ctx, || many(ids.clone()))?),
            Lookup::QueueEnd { org } => json(load(ctx, || queue_end(*org))?),
            Lookup::Nothing { org } => json(load(ctx, || nothing(*org))?),
            Lookup::AllTickets { org } => json(load(ctx, || all_tickets(*org))?),
            Lookup::CommentAuthors { ticket } => {
                json(authors_of(load::<_, AuthorRow>(ctx, || {
                    comment_authors(*ticket)
                })?))
            }
            Lookup::Skilled { org, skill } => json(load(ctx, || skilled(*org, skill.clone()))?),
            Lookup::Statuses { org } => json(load::<_, TicketStatus>(ctx, || statuses(*org))?),
            Lookup::Workload { org } => {
                // Raw SQL is not a Diesel query the adapter can render.
                ctx.served = Served::Postgres;
                ctx.sql = Some("SELECT agent_id, COUNT(*) AS tickets FROM tickets WHERE org_id = $1 GROUP BY agent_id ORDER BY agent_id".into());
                ctx.refusal = Some("raw sql_query: not handed to the cache".to_owned());
                json(RunQueryDsl::load::<Workload>(workload(*org), ctx.conn)?)
            }
            Lookup::LastActivity { org } => json(load::<_, Option<DateTime<Utc>>>(ctx, || {
                last_activity(*org)
            })?),
            Lookup::OfKind { org, kind } => json(load(ctx, || of_kind(*org, *kind))?),
        }
    }
}

/// The rows of `query()` on `ctx`'s route.
fn load<Q, U>(ctx: &mut Ctx<'_>, query: impl Fn() -> Q) -> QueryResult<Vec<U>>
where
    Q: LoadCached<'static, PgConnection, U> + LoadQuery<'static, PgConnection, U> + AsQuery,
    Q::Query: QueryFragment<Pg> + QueryId,
{
    if ctx.explain {
        ctx.sql = Some(
            streamgres_diesel::rendered(&query().as_query(), ctx.conn)?
                .0
                .to_string(),
        );
    }
    if ctx.route == Route::Cached {
        match query().inspect_cached(ctx.cache, ctx.conn)? {
            CacheAttempt::Hit(rows) => {
                ctx.served = Served::Cache;
                return Ok(rows);
            }
            CacheAttempt::Refused(reason) => ctx.refusal = Some(reason.to_string()),
            _ => ctx.refusal = Some("the adapter returned an unknown outcome".into()),
        }
    }
    ctx.served = Served::Postgres;
    RunQueryDsl::load(query(), ctx.conn)
}

/// When set, reads return `null` instead of their rows as JSON, so a
/// bench times the read and Diesel's decoding alone.
pub static ROWS_ONLY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The rows as JSON, the form the verify loop compares.
pub(crate) fn json<T: Serialize>(rows: T) -> QueryResult<Value> {
    if ROWS_ONLY.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(Value::Null);
    }
    serde_json::to_value(rows)
        .map_err(|error| diesel::result::Error::SerializationError(Box::new(error)))
}

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
