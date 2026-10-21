//! The rows the queries return, and the two Postgres enums.

use std::io::Write;

use chrono::{DateTime, Utc};
use diesel::data_types::PgInterval;
use diesel::deserialize::{self, FromSql, FromSqlRow};
use diesel::expression::AsExpression;
use diesel::pg::{Pg, PgValue};
use diesel::prelude::*;
use diesel::serialize::{self, IsNull, Output, ToSql};
use serde::{Deserialize, Serialize};

use crate::schema::{agents, attachments, comments, organizations, tickets};

#[derive(diesel::sql_types::SqlType, diesel::query_builder::QueryId)]
#[diesel(postgres_type(name = "ticket_status"))]
pub struct TicketStatusType;

#[derive(diesel::sql_types::SqlType, diesel::query_builder::QueryId)]
#[diesel(postgres_type(name = "ticket_priority"))]
pub struct TicketPriorityType;

/// The enum type a migration adds after the engine started
/// (`demo migrate new-type`): the engine learns it the first time a
/// read binds it.
#[derive(diesel::sql_types::SqlType, diesel::query_builder::QueryId)]
#[diesel(postgres_type(name = "ticket_kind"))]
pub struct TicketKindType;

/// A Postgres enum as a Rust enum: its labels on the wire, lower case
/// in JSON.
macro_rules! pg_enum {
    ($name:ident, $sql:ty, [$($variant:ident => $label:literal),+ $(,)?]) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsExpression, FromSqlRow, Serialize, Deserialize)]
        #[diesel(sql_type = $sql)]
        #[serde(rename_all = "lowercase")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const ALL: &'static [$name] = &[$($name::$variant),+];

            pub fn label(self) -> &'static str {
                match self {
                    $($name::$variant => $label),+
                }
            }

            pub fn parse(label: &str) -> Option<$name> {
                match label {
                    $($label => Some($name::$variant),)+
                    _ => None,
                }
            }
        }

        impl ToSql<$sql, Pg> for $name {
            fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
                out.write_all(self.label().as_bytes())?;
                Ok(IsNull::No)
            }
        }

        impl FromSql<$sql, Pg> for $name {
            fn from_sql(value: PgValue<'_>) -> deserialize::Result<Self> {
                let text = std::str::from_utf8(value.as_bytes())?;
                Self::parse(text).ok_or_else(|| format!("not a {}: {text}", stringify!($name)).into())
            }
        }
    };
}

pg_enum!(TicketStatus, TicketStatusType, [Open => "open", Pending => "pending", Closed => "closed"]);
pg_enum!(TicketPriority, TicketPriorityType, [Low => "low", Normal => "normal", High => "high"]);
pg_enum!(TicketKind, TicketKindType, [Bug => "bug", Task => "task", Question => "question"]);

impl TicketStatus {
    /// The next status the background writer moves a ticket to.
    pub fn next(self) -> TicketStatus {
        match self {
            TicketStatus::Open => TicketStatus::Pending,
            TicketStatus::Pending => TicketStatus::Closed,
            TicketStatus::Closed => TicketStatus::Open,
        }
    }
}

#[derive(Queryable, Selectable, Serialize, Debug, Clone, PartialEq)]
#[diesel(table_name = organizations)]
pub struct Organization {
    pub id: i64,
    pub slug: String,
    pub name: String,
}

#[derive(Queryable, Selectable, Serialize, Debug, Clone, PartialEq)]
#[diesel(table_name = agents)]
pub struct Agent {
    pub id: i64,
    pub org_id: i64,
    pub name: String,
    pub skills: Vec<String>,
}

#[derive(Queryable, Selectable, Serialize, Debug, Clone, PartialEq)]
#[diesel(table_name = tickets)]
pub struct Ticket {
    pub id: i64,
    pub org_id: i64,
    pub agent_id: Option<i64>,
    pub title: String,
    pub status: TicketStatus,
    pub priority: TicketPriority,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Queryable, Selectable, Serialize, Debug, Clone, PartialEq)]
#[diesel(table_name = comments)]
pub struct Comment {
    pub id: i64,
    pub ticket_id: i64,
    pub agent_id: Option<i64>,
    pub body: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Queryable, Selectable, Serialize, Debug, Clone, PartialEq)]
#[diesel(table_name = attachments)]
pub struct Attachment {
    pub id: i64,
    pub ticket_id: i64,
    pub name: String,
    #[serde(serialize_with = "hex")]
    pub data: Vec<u8>,
}

/// A ticket with the column `demo migrate new-type` adds.
#[derive(Queryable, Selectable, Serialize, Debug, Clone, PartialEq)]
#[diesel(table_name = crate::schema::tickets_with_kind)]
pub struct TicketOfKind {
    pub id: i64,
    pub org_id: i64,
    pub kind: Option<TicketKind>,
}

/// A comment with the name of the agent who wrote it, `None` where
/// nobody did: what the left join returns.
#[derive(Queryable, Serialize, Debug, Clone, PartialEq)]
pub struct CommentAuthor {
    pub comment_id: i64,
    pub body: String,
    pub agent: Option<String>,
}

/// How many tickets each agent holds, `None` for the unassigned: the
/// `GROUP BY` the engine refuses.
#[derive(QueryableByName, Serialize, Debug, Clone, PartialEq)]
pub struct Workload {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    pub agent_id: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub tickets: i64,
}

/// A ticket's title and its comment, as the join returns them.
#[derive(Queryable, Serialize, Debug, Clone, PartialEq)]
pub struct TicketComment {
    pub ticket_id: i64,
    pub title: String,
    pub comment_id: i64,
    pub body: String,
    pub created_at: DateTime<Utc>,
}

/// A ticket with its service-level agreement, an `interval`: the one
/// column the engine cannot ship rows for.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TicketSla {
    pub id: i64,
    pub title: String,
    /// The interval in seconds, ignoring its month part.
    pub sla_seconds: Option<i64>,
}

impl TicketSla {
    pub fn from_row((id, title, sla): (i64, String, Option<PgInterval>)) -> TicketSla {
        TicketSla {
            id,
            title,
            sla_seconds: sla.map(|sla| sla.microseconds / 1_000_000 + i64::from(sla.days) * 86_400),
        }
    }
}

fn hex<S: serde::Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    let text: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    serializer.serialize_str(&text)
}
