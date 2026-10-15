//! The reads the ticket desk does, as one list: each is a named query
//! with a known shape, runnable through either wrapper.
//!
//! This list is the point of the crate. The HTTP API serves it, the
//! scenario test drives every entry through both wrappers and both
//! routes, the verify loop compares every entry across the routes, and
//! the bench times it. A shape that matters to the wrapper (an enum
//! bound as an array, an offset page, a join, a `bytea`, a column the
//! engine cannot ship, an operator it refuses) is here so it is
//! exercised the day it changes, not the day a user finds it.
//!
//! Each shape says what it expects of the engine ([`Lookup::expected`]):
//! served from the engine's rows, refilled from Postgres on each change
//! because a column cannot be shipped, or refused and served live. The
//! scenario test holds every shape to it, so a shape that drops from
//! rows mode to refills fails a test instead of passing more slowly.

use chrono::{DateTime, TimeZone, Utc};
use diesel::dsl::{AsSelect, InnerJoinQuerySource, LeftJoinQuerySource};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::{BoxedSelectStatement, FromClause};
use diesel::sql_types::{BigInt, Int8, Interval, Nullable, Text, Timestamptz};
use diesel::{PgConnection, QueryResult};
use futures_util::future::{BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use streamgres_diesel::{AsyncSyncedConnection, SyncedConnection};

use crate::models::{
    Agent, Attachment, CommentAuthor, Ticket, TicketComment, TicketKind, TicketOfKind,
    TicketPriority, TicketSla, TicketStatus, TicketStatusType, Workload,
};
use crate::schema::{agents, attachments, comments, tickets, tickets_with_kind};

pub type SyncConn = SyncedConnection<PgConnection>;
pub type AsyncConn = AsyncSyncedConnection;

/// Tickets per page of a queue.
pub const PAGE: i64 = 10;

/// What a shape expects of the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Expected {
    /// Subscribed, served from the engine's rows, every change applied
    /// in place.
    Rows,
    /// Subscribed, but a projected column cannot be shipped, so each
    /// change refills the entry from Postgres.
    Refill,
    /// Outside the engine's subset: refused, served by Postgres every
    /// time.
    Live,
}

/// One read of the ticket desk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "lookup", rename_all = "snake_case")]
pub enum Lookup {
    /// One ticket by primary key.
    Ticket { id: i64 },
    /// An organization's tickets in some statuses, newest first, one
    /// page: an enum array bind, an order by timestamp, `LIMIT` and
    /// `OFFSET`.
    Queue {
        org: i64,
        statuses: Vec<TicketStatus>,
        page: i64,
    },
    /// How many tickets a queue has: a `COUNT(*)`.
    QueueCount {
        org: i64,
        statuses: Vec<TicketStatus>,
    },
    /// A ticket's comments with its title: an inner join, composed from
    /// the engine's rows per table.
    Comments { ticket: i64 },
    /// One attachment: a `bytea`, carried as hex by the engine.
    Attachment { id: i64 },
    /// An organization's agents: a text array column.
    Agents { org: i64 },
    /// Tickets with a service level: an `interval` column, which the
    /// engine cannot ship rows for, so the query is refilled from
    /// Postgres on every change instead.
    Sla { org: i64 },
    /// Tickets whose title matches a pattern: `ILIKE`, outside the
    /// engine's subset, so the read is refused and goes to Postgres
    /// every time.
    Search { org: i64, pattern: String },
    /// The organization's most recently updated tickets.
    Recent { org: i64, limit: i64 },
    /// One ticket by its organization and id: a two-column key.
    OrgTicket { org: i64, id: i64 },
    /// Tickets nobody holds: `IS NULL`.
    Unassigned { org: i64 },
    /// Tickets that are high priority or pending: an `OR` of two enum
    /// binds.
    Escalated { org: i64 },
    /// Tickets not closed, a few excluded by id: `<>` and `NOT IN`.
    OpenExcept { org: i64, except: Vec<i64> },
    /// Tickets created inside a window: two timestamp binds.
    CreatedBetween {
        org: i64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    /// Many tickets by id: an `IN` list a hundred long.
    Many { ids: Vec<i64> },
    /// A queue page far past the end: an empty result, subscribed, that
    /// a change elsewhere leaves empty.
    QueueEnd { org: i64 },
    /// A query that never has rows: subscribed, stays empty.
    Nothing { org: i64 },
    /// Every ticket of an organization: the largest result; under a
    /// small per-entry cap it is served but not kept.
    AllTickets { org: i64 },
    /// A ticket's comments with the name of whoever wrote each: a left
    /// join, `NULL` on the side with no agent.
    CommentAuthors { ticket: i64 },
    /// Agents with a skill: the array operator `@>`, refused.
    Skilled { org: i64, skill: String },
    /// The statuses in use: `DISTINCT`, refused.
    Statuses { org: i64 },
    /// Tickets per agent: `GROUP BY`, refused.
    Workload { org: i64 },
    /// When the organization was last active: `MAX`, an aggregate
    /// other than `COUNT(*)`, refused.
    LastActivity { org: i64 },
    /// Tickets of a kind, through the column and enum type `example
    /// migrate new-type` adds: before the migration the read fails
    /// (no such column); after it the bind is refused until the engine
    /// restarts, then served from rows. Not in the demo list.
    OfKind { org: i64, kind: TicketKind },
}

impl Lookup {
    /// The name the API, the bench and the stats page use.
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

    /// What the shape exercises in the wrapper, for the stats page and
    /// the scenario report.
    pub fn exercises(&self) -> &'static str {
        match self {
            Lookup::Ticket { .. } => "primary key, enum columns, timestamps",
            Lookup::Queue { .. } => "enum array bind, ORDER BY timestamp DESC, LIMIT, OFFSET",
            Lookup::QueueCount { .. } => "COUNT(*) with an enum array bind",
            Lookup::Comments { .. } => "inner join composed from engine rows",
            Lookup::Attachment { .. } => "bytea",
            Lookup::Agents { .. } => "text[] column",
            Lookup::Sla { .. } => "interval column: refill mode, not rows mode",
            Lookup::Search { .. } => "ILIKE: refused by the engine, served live",
            Lookup::Recent { .. } => "ORDER BY updated_at DESC LIMIT",
            Lookup::OrgTicket { .. } => "two-column key",
            Lookup::Unassigned { .. } => "IS NULL",
            Lookup::Escalated { .. } => "OR of two enum binds",
            Lookup::OpenExcept { .. } => "<> and NOT IN",
            Lookup::CreatedBetween { .. } => "timestamp range: two binds",
            Lookup::Many { .. } => "IN list of a hundred ids",
            Lookup::QueueEnd { .. } => "page past the end: empty, subscribed, stays empty",
            Lookup::Nothing { .. } => "never any rows: empty, subscribed, stays empty",
            Lookup::AllTickets { .. } => "largest result: not kept under a small per-entry cap",
            Lookup::CommentAuthors { .. } => "left join with an empty side",
            Lookup::Skilled { .. } => "array @>: refused, served live",
            Lookup::Statuses { .. } => "DISTINCT: refused, served live",
            Lookup::Workload { .. } => "GROUP BY: refused, served live",
            Lookup::LastActivity { .. } => "MAX: refused, served live",
            Lookup::OfKind { .. } => "enum type added after the engine started",
        }
    }

    /// What the shape expects of the engine.
    pub fn expected(&self) -> Expected {
        match self {
            Lookup::Sla { .. } => Expected::Refill,
            Lookup::Search { .. }
            | Lookup::Skilled { .. }
            | Lookup::Statuses { .. }
            | Lookup::Workload { .. }
            | Lookup::LastActivity { .. } => Expected::Live,
            _ => Expected::Rows,
        }
    }

    /// Whether the engine can follow the query at all.
    pub fn is_cacheable(&self) -> bool {
        self.expected() != Expected::Live
    }

    /// Whether a change made for the shape leaves its result as it is:
    /// the empty results, which must stay empty.
    pub fn is_static(&self) -> bool {
        matches!(self, Lookup::QueueEnd { .. } | Lookup::Nothing { .. })
    }

    /// The demo's lookups with their default arguments, in a fixed
    /// order: what the scenario test, the verify loop and the bench
    /// iterate. The shapes an operator refuses come last, so the table
    /// reads from the common case down.
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

    /// Runs the lookup on the blocking wrapper.
    pub fn run_sync(&self, conn: &mut SyncConn) -> QueryResult<Value> {
        match self {
            Lookup::Ticket { id } => json(sync_load(ticket(*id), conn)?),
            Lookup::Queue {
                org,
                statuses,
                page,
            } => json(sync_load(queue(*org, statuses.clone(), *page), conn)?),
            Lookup::QueueCount { org, statuses } => json(sync_load::<_, i64>(
                queue_count(*org, statuses.clone()),
                conn,
            )?),
            Lookup::Comments { ticket } => json(comments_of(sync_load::<_, CommentRow>(
                ticket_comments(*ticket),
                conn,
            )?)),
            Lookup::Attachment { id } => json(sync_load(attachment(*id), conn)?),
            Lookup::Agents { org } => json(sync_load(org_agents(*org), conn)?),
            Lookup::Sla { org } => json(
                sync_load(sla(*org), conn)?
                    .into_iter()
                    .map(TicketSla::from_row)
                    .collect::<Vec<_>>(),
            ),
            Lookup::Search { org, pattern } => {
                json(sync_load(search(*org, pattern.clone()), conn)?)
            }
            Lookup::Recent { org, limit } => json(sync_load(recent(*org, *limit), conn)?),
            Lookup::OrgTicket { org, id } => json(sync_load(org_ticket(*org, *id), conn)?),
            Lookup::Unassigned { org } => json(sync_load(unassigned(*org), conn)?),
            Lookup::Escalated { org } => json(sync_load(escalated(*org), conn)?),
            Lookup::OpenExcept { org, except } => {
                json(sync_load(open_except(*org, except.clone()), conn)?)
            }
            Lookup::CreatedBetween { org, from, to } => {
                json(sync_load(created_between(*org, *from, *to), conn)?)
            }
            Lookup::Many { ids } => json(sync_load(many(ids.clone()), conn)?),
            Lookup::QueueEnd { org } => json(sync_load(queue_end(*org), conn)?),
            Lookup::Nothing { org } => json(sync_load(nothing(*org), conn)?),
            Lookup::AllTickets { org } => json(sync_load(all_tickets(*org), conn)?),
            Lookup::CommentAuthors { ticket } => json(authors_of(sync_load::<_, AuthorRow>(
                comment_authors(*ticket),
                conn,
            )?)),
            Lookup::Skilled { org, skill } => json(sync_load(skilled(*org, skill.clone()), conn)?),
            Lookup::Statuses { org } => json(sync_load::<_, TicketStatus>(statuses(*org), conn)?),
            Lookup::Workload { org } => json(sync_load::<_, Workload>(workload(*org), conn)?),
            Lookup::LastActivity { org } => json(sync_load::<_, Option<DateTime<Utc>>>(
                last_activity(*org),
                conn,
            )?),
            Lookup::OfKind { org, kind } => json(sync_load(of_kind(*org, *kind), conn)?),
        }
    }

    /// Runs the lookup on the async wrapper.
    pub async fn run_async(&self, conn: &mut AsyncConn) -> QueryResult<Value> {
        match self {
            Lookup::Ticket { id } => json(async_load(ticket(*id), conn).await?),
            Lookup::Queue {
                org,
                statuses,
                page,
            } => json(async_load(queue(*org, statuses.clone(), *page), conn).await?),
            Lookup::QueueCount { org, statuses } => {
                json(async_load::<_, i64>(queue_count(*org, statuses.clone()), conn).await?)
            }
            Lookup::Comments { ticket } => json(comments_of(
                async_load::<_, CommentRow>(ticket_comments(*ticket), conn).await?,
            )),
            Lookup::Attachment { id } => json(async_load(attachment(*id), conn).await?),
            Lookup::Agents { org } => json(async_load(org_agents(*org), conn).await?),
            Lookup::Sla { org } => json(
                async_load(sla(*org), conn)
                    .await?
                    .into_iter()
                    .map(TicketSla::from_row)
                    .collect::<Vec<_>>(),
            ),
            Lookup::Search { org, pattern } => {
                json(async_load(search(*org, pattern.clone()), conn).await?)
            }
            Lookup::Recent { org, limit } => json(async_load(recent(*org, *limit), conn).await?),
            Lookup::OrgTicket { org, id } => json(async_load(org_ticket(*org, *id), conn).await?),
            Lookup::Unassigned { org } => json(async_load(unassigned(*org), conn).await?),
            Lookup::Escalated { org } => json(async_load(escalated(*org), conn).await?),
            Lookup::OpenExcept { org, except } => {
                json(async_load(open_except(*org, except.clone()), conn).await?)
            }
            Lookup::CreatedBetween { org, from, to } => {
                json(async_load(created_between(*org, *from, *to), conn).await?)
            }
            Lookup::Many { ids } => json(async_load(many(ids.clone()), conn).await?),
            Lookup::QueueEnd { org } => json(async_load(queue_end(*org), conn).await?),
            Lookup::Nothing { org } => json(async_load(nothing(*org), conn).await?),
            Lookup::AllTickets { org } => json(async_load(all_tickets(*org), conn).await?),
            Lookup::CommentAuthors { ticket } => json(authors_of(
                async_load::<_, AuthorRow>(comment_authors(*ticket), conn).await?,
            )),
            Lookup::Skilled { org, skill } => {
                json(async_load(skilled(*org, skill.clone()), conn).await?)
            }
            Lookup::Statuses { org } => {
                json(async_load::<_, TicketStatus>(statuses(*org), conn).await?)
            }
            Lookup::Workload { org } => {
                json(async_load::<_, Workload>(workload(*org), conn).await?)
            }
            Lookup::LastActivity { org } => {
                json(async_load::<_, Option<DateTime<Utc>>>(last_activity(*org), conn).await?)
            }
            Lookup::OfKind { org, kind } => json(async_load(of_kind(*org, *kind), conn).await?),
        }
    }
}

/// The rows as JSON, the form the API serves and the tests compare.
fn json<T: Serialize>(rows: T) -> QueryResult<Value> {
    serde_json::to_value(rows)
        .map_err(|error| diesel::result::Error::SerializationError(Box::new(error)))
}

/// Loads a query on the blocking wrapper.
fn sync_load<Q, U>(query: Q, conn: &mut SyncConn) -> QueryResult<Vec<U>>
where
    Q: diesel::query_dsl::methods::LoadQuery<'static, SyncConn, U>,
{
    diesel::RunQueryDsl::load(query, conn)
}

/// Boxed at a plain function rather than awaited in an `async fn`: the
/// compiler cannot prove the load future `Send` through the latter's
/// desugaring (the lifetime in diesel-async's `LoadQuery` bound comes
/// out higher-ranked), and can here.
fn async_load<'a, Q, U>(query: Q, conn: &'a mut AsyncConn) -> BoxFuture<'a, QueryResult<Vec<U>>>
where
    Q: diesel_async::methods::LoadQuery<'static, AsyncConn, U> + Send + 'static,
    U: Send + 'static,
{
    diesel_async::RunQueryDsl::load(query, conn).boxed()
}

type TicketQuery = tickets::BoxedQuery<'static, Pg, AsSelect<Ticket, Pg>>;
type CountQuery = tickets::BoxedQuery<'static, Pg, BigInt>;
type SlaQuery = tickets::BoxedQuery<'static, Pg, (Int8, Text, Nullable<Interval>)>;
type AgentsQuery = agents::BoxedQuery<'static, Pg, AsSelect<Agent, Pg>>;
type AttachmentQuery = attachments::BoxedQuery<'static, Pg, AsSelect<Attachment, Pg>>;
type StatusesQuery = tickets::BoxedQuery<'static, Pg, TicketStatusType>;
type LastActivityQuery = tickets::BoxedQuery<'static, Pg, Nullable<Timestamptz>>;
type OfKindQuery = tickets_with_kind::BoxedQuery<'static, Pg, AsSelect<TicketOfKind, Pg>>;
type CommentsQuery = BoxedSelectStatement<
    'static,
    (Int8, Text, Int8, Text, Timestamptz),
    FromClause<InnerJoinQuerySource<tickets::table, comments::table>>,
    Pg,
>;
type AuthorsQuery = BoxedSelectStatement<
    'static,
    (Int8, Text, Nullable<Text>),
    FromClause<LeftJoinQuerySource<comments::table, agents::table>>,
    Pg,
>;
type WorkloadQuery =
    diesel::query_builder::BoxedSqlQuery<'static, Pg, diesel::query_builder::SqlQuery>;

fn ticket(id: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::id.eq(id))
        .select(Ticket::as_select())
        .into_boxed()
}

fn queue(org: i64, statuses: Vec<TicketStatus>, page: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::status.eq_any(statuses))
        .order((tickets::created_at.desc(), tickets::id.asc()))
        .limit(PAGE)
        .offset(page * PAGE)
        .select(Ticket::as_select())
        .into_boxed()
}

fn queue_count(org: i64, statuses: Vec<TicketStatus>) -> CountQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::status.eq_any(statuses))
        .count()
        .into_boxed()
}

fn ticket_comments(ticket: i64) -> CommentsQuery {
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

fn attachment(id: i64) -> AttachmentQuery {
    attachments::table
        .filter(attachments::id.eq(id))
        .select(Attachment::as_select())
        .into_boxed()
}

fn org_agents(org: i64) -> AgentsQuery {
    agents::table
        .filter(agents::org_id.eq(org))
        .order(agents::id.asc())
        .select(Agent::as_select())
        .into_boxed()
}

fn sla(org: i64) -> SlaQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::sla.is_not_null())
        .order(tickets::id.asc())
        .select((tickets::id, tickets::title, tickets::sla))
        .into_boxed()
}

fn search(org: i64, pattern: String) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::title.ilike(pattern))
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

fn recent(org: i64, limit: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .order((tickets::updated_at.desc(), tickets::id.asc()))
        .limit(limit)
        .select(Ticket::as_select())
        .into_boxed()
}

fn org_ticket(org: i64, id: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::id.eq(id))
        .select(Ticket::as_select())
        .into_boxed()
}

fn unassigned(org: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::agent_id.is_null())
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

fn escalated(org: i64) -> TicketQuery {
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

fn open_except(org: i64, except: Vec<i64>) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::status.ne(TicketStatus::Closed))
        .filter(tickets::id.ne_all(except))
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

fn created_between(org: i64, from: DateTime<Utc>, to: DateTime<Utc>) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::created_at.ge(from))
        .filter(tickets::created_at.lt(to))
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

fn many(ids: Vec<i64>) -> TicketQuery {
    tickets::table
        .filter(tickets::id.eq_any(ids))
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

fn queue_end(org: i64) -> TicketQuery {
    queue(org, TicketStatus::ALL.to_vec(), 1000)
}

fn nothing(org: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .filter(tickets::id.lt(0))
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

fn all_tickets(org: i64) -> TicketQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .order(tickets::id.asc())
        .select(Ticket::as_select())
        .into_boxed()
}

fn comment_authors(ticket: i64) -> AuthorsQuery {
    comments::table
        .left_join(agents::table)
        .filter(comments::ticket_id.eq(ticket))
        .order(comments::id.asc())
        .select((comments::id, comments::body, agents::name.nullable()))
        .into_boxed()
}

fn skilled(org: i64, skill: String) -> AgentsQuery {
    agents::table
        .filter(agents::org_id.eq(org))
        .filter(agents::skills.contains(vec![skill]))
        .order(agents::id.asc())
        .select(Agent::as_select())
        .into_boxed()
}

fn statuses(org: i64) -> StatusesQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .select(tickets::status)
        .distinct()
        .order(tickets::status.asc())
        .into_boxed()
}

fn workload(org: i64) -> WorkloadQuery {
    diesel::sql_query(
        "SELECT agent_id, COUNT(*) AS tickets FROM tickets WHERE org_id = $1 \
         GROUP BY agent_id ORDER BY agent_id",
    )
    .into_boxed()
    .bind::<BigInt, _>(org)
}

fn last_activity(org: i64) -> LastActivityQuery {
    tickets::table
        .filter(tickets::org_id.eq(org))
        .select(diesel::dsl::max(tickets::updated_at))
        .into_boxed()
}

fn of_kind(org: i64, kind: TicketKind) -> OfKindQuery {
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
