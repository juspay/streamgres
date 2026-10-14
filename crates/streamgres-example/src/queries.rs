//! The reads the ticket desk does, as one list: each is a named query
//! with a known shape, runnable through either wrapper.
//!
//! This list is the point of the crate. The HTTP API serves it, the
//! scenario test drives every entry through both wrappers and both
//! routes, and the bench times it. A shape that matters to the wrapper
//! (an enum bound as an array, an offset page, a join, a `bytea`, a
//! column the engine cannot ship) is here so it is exercised the day it
//! changes, not the day a user finds it.

use chrono::{DateTime, Utc};
use diesel::dsl::{AsSelect, InnerJoinQuerySource};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::{BoxedSelectStatement, FromClause};
use diesel::sql_types::{BigInt, Int8, Interval, Nullable, Text, Timestamptz};
use diesel::{PgConnection, QueryResult};
use futures_util::future::{BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use streamgres_diesel::{AsyncSyncedConnection, SyncedConnection};

use crate::models::{Agent, Attachment, Ticket, TicketComment, TicketSla, TicketStatus};
use crate::schema::{agents, attachments, comments, tickets};

pub type SyncConn = SyncedConnection<PgConnection>;
pub type AsyncConn = AsyncSyncedConnection;

/// Tickets per page of a queue.
pub const PAGE: i64 = 10;

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
    QueueCount { org: i64, statuses: Vec<TicketStatus> },
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
}

impl Lookup {
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
        }
    }

    /// Whether the engine can follow the query at all.
    pub fn is_cacheable(&self) -> bool {
        !matches!(self, Lookup::Search { .. })
    }

    /// The demo's lookups with their default arguments, in a fixed
    /// order: what the scenario test and the bench iterate.
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
        ]
    }

    /// A demo lookup by name.
    pub fn named(name: &str) -> Option<Lookup> {
        Self::demo().into_iter().find(|lookup| lookup.name() == name)
    }

    pub fn run_sync(&self, conn: &mut SyncConn) -> QueryResult<Value> {
        match self {
            Lookup::Ticket { id } => json(sync_load(ticket(*id), conn)?),
            Lookup::Queue { org, statuses, page } => json(sync_load(queue(*org, statuses.clone(), *page), conn)?),
            Lookup::QueueCount { org, statuses } => json(sync_load::<_, i64>(queue_count(*org, statuses.clone()), conn)?),
            Lookup::Comments { ticket } => json(comments_of(sync_load::<_, CommentRow>(ticket_comments(*ticket), conn)?)),
            Lookup::Attachment { id } => json(sync_load(attachment(*id), conn)?),
            Lookup::Agents { org } => json(sync_load(org_agents(*org), conn)?),
            Lookup::Sla { org } => json(sync_load(sla(*org), conn)?.into_iter().map(TicketSla::from_row).collect::<Vec<_>>()),
            Lookup::Search { org, pattern } => json(sync_load(search(*org, pattern.clone()), conn)?),
            Lookup::Recent { org, limit } => json(sync_load(recent(*org, *limit), conn)?),
        }
    }

    pub async fn run_async(&self, conn: &mut AsyncConn) -> QueryResult<Value> {
        match self {
            Lookup::Ticket { id } => json(async_load(ticket(*id), conn).await?),
            Lookup::Queue { org, statuses, page } => json(async_load(queue(*org, statuses.clone(), *page), conn).await?),
            Lookup::QueueCount { org, statuses } => json(async_load::<_, i64>(queue_count(*org, statuses.clone()), conn).await?),
            Lookup::Comments { ticket } => json(comments_of(async_load::<_, CommentRow>(ticket_comments(*ticket), conn).await?)),
            Lookup::Attachment { id } => json(async_load(attachment(*id), conn).await?),
            Lookup::Agents { org } => json(async_load(org_agents(*org), conn).await?),
            Lookup::Sla { org } => json(
                async_load(sla(*org), conn)
                    .await?
                    .into_iter()
                    .map(TicketSla::from_row)
                    .collect::<Vec<_>>(),
            ),
            Lookup::Search { org, pattern } => json(async_load(search(*org, pattern.clone()), conn).await?),
            Lookup::Recent { org, limit } => json(async_load(recent(*org, *limit), conn).await?),
        }
    }
}

fn json<T: Serialize>(rows: T) -> QueryResult<Value> {
    serde_json::to_value(rows).map_err(|error| diesel::result::Error::SerializationError(Box::new(error)))
}

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
type CommentsQuery = BoxedSelectStatement<
    'static,
    (Int8, Text, Int8, Text, Timestamptz),
    FromClause<InnerJoinQuerySource<tickets::table, comments::table>>,
    Pg,
>;

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
        .select((tickets::id, tickets::title, comments::id, comments::body, comments::created_at))
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

/// A row of the comments join, as the tuple Diesel decodes.
pub type CommentRow = (i64, String, i64, String, DateTime<Utc>);

/// The join's rows as the struct the API serves.
pub fn comments_of(rows: Vec<CommentRow>) -> Vec<TicketComment> {
    rows.into_iter()
        .map(|(ticket_id, title, comment_id, body, created_at)| TicketComment {
            ticket_id,
            title,
            comment_id,
            body,
            created_at,
        })
        .collect()
}
