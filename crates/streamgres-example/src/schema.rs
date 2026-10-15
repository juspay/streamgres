//! The ticket desk's tables, as Diesel sees them.
//!
//! Five tables, chosen for the shapes they let a query take: an enum
//! column to bind and to `eq_any`, a text array, a `bytea`, an
//! `interval` (which the engine cannot ship rows for, so the query
//! falls back to refills), timestamps to order by, and a parent-child
//! pair to join. `tickets_with_kind` is the tickets table again, seen
//! through the column `example migrate new-type` adds.

diesel::table! {
    organizations (id) {
        id -> Int8,
        slug -> Text,
        name -> Text,
    }
}

diesel::table! {
    agents (id) {
        id -> Int8,
        org_id -> Int8,
        name -> Text,
        skills -> Array<Text>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::models::{TicketPriorityType, TicketStatusType};

    tickets (id) {
        id -> Int8,
        org_id -> Int8,
        agent_id -> Nullable<Int8>,
        title -> Text,
        status -> TicketStatusType,
        priority -> TicketPriorityType,
        sla -> Nullable<Interval>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    comments (id) {
        id -> Int8,
        ticket_id -> Int8,
        agent_id -> Nullable<Int8>,
        body -> Text,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    attachments (id) {
        id -> Int8,
        ticket_id -> Int8,
        name -> Text,
        data -> Bytea,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::models::TicketKindType;

    #[sql_name = "tickets"]
    tickets_with_kind (id) {
        id -> Int8,
        org_id -> Int8,
        kind -> Nullable<TicketKindType>,
    }
}

diesel::joinable!(agents -> organizations (org_id));
diesel::joinable!(comments -> agents (agent_id));
diesel::joinable!(tickets -> organizations (org_id));
diesel::joinable!(comments -> tickets (ticket_id));
diesel::joinable!(attachments -> tickets (ticket_id));
diesel::allow_tables_to_appear_in_same_query!(
    organizations,
    agents,
    tickets,
    comments,
    attachments
);
