//! The schema and a small, readable, deterministic dataset, at a scale.
//!
//! Three organizations, four agents each, sixty tickets each per unit of
//! scale with statuses and priorities spread evenly, comments on the
//! first third of each organization's tickets, an attachment on every
//! tenth, a service level on every third. Ids are fixed, so the demo's
//! URLs and the scenario test's expectations do not move between runs;
//! the first organization's tickets are `1..=60 * scale` at any scale.
//! Reseeding deletes and inserts rather than truncating, so an engine
//! already following the tables sees ordinary changes.
//!
//! `EXAMPLE_SCALE` (default 1) multiplies the tickets, comments and
//! attachments, so pages run deep and the cache's byte limits matter.

use diesel::QueryResult;
use diesel::connection::SimpleConnection;

/// Tickets per organization at scale 1.
pub const TICKETS_PER_ORG: i64 = 60;

/// Creates the types, tables and sequences if they do not exist. The
/// writer's rows take ids from the sequences, above the seed's.
pub const SCHEMA: &str = "
DO $$ BEGIN
    CREATE TYPE ticket_status AS ENUM ('open', 'pending', 'closed');
EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$;
DO $$ BEGIN
    CREATE TYPE ticket_priority AS ENUM ('low', 'normal', 'high');
EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$;
CREATE TABLE IF NOT EXISTS organizations (
    id bigint PRIMARY KEY,
    slug text NOT NULL UNIQUE,
    name text NOT NULL
);
CREATE TABLE IF NOT EXISTS agents (
    id bigint PRIMARY KEY,
    org_id bigint NOT NULL REFERENCES organizations (id),
    name text NOT NULL,
    skills text[] NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS tickets (
    id bigint PRIMARY KEY,
    org_id bigint NOT NULL REFERENCES organizations (id),
    agent_id bigint REFERENCES agents (id),
    title text NOT NULL,
    status ticket_status NOT NULL DEFAULT 'open',
    priority ticket_priority NOT NULL DEFAULT 'normal',
    sla interval,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS comments (
    id bigint PRIMARY KEY,
    ticket_id bigint NOT NULL REFERENCES tickets (id) ON DELETE CASCADE,
    agent_id bigint REFERENCES agents (id),
    body text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS attachments (
    id bigint PRIMARY KEY,
    ticket_id bigint NOT NULL REFERENCES tickets (id) ON DELETE CASCADE,
    name text NOT NULL,
    data bytea NOT NULL
);
CREATE SEQUENCE IF NOT EXISTS comments_id_seq START 100000;
CREATE SEQUENCE IF NOT EXISTS tickets_id_seq START 1000000;
";

/// Replaces the data; `{tickets}`, `{commented}` and the fixed instants
/// make it the same on every run at a scale.
const DATA: &str = "
BEGIN;
DELETE FROM attachments;
DELETE FROM comments;
DELETE FROM tickets;
DELETE FROM agents;
DELETE FROM organizations;

INSERT INTO organizations (id, slug, name) VALUES
    (1, 'acme', 'Acme Corporation'),
    (2, 'globex', 'Globex'),
    (3, 'initech', 'Initech');

INSERT INTO agents (id, org_id, name, skills)
SELECT (org - 1) * 4 + n,
       org,
       (ARRAY['Ada', 'Grace', 'Linus', 'Margaret'])[n],
       CASE n
           WHEN 1 THEN ARRAY['billing', 'refunds']
           WHEN 2 THEN ARRAY['login', 'sso']
           WHEN 3 THEN ARRAY['api']
           ELSE ARRAY['billing', 'api', 'login']
       END
FROM generate_series(1, 3) AS org, generate_series(1, 4) AS n;

INSERT INTO tickets (id, org_id, agent_id, title, status, priority, sla, created_at, updated_at)
SELECT (org - 1) * {tickets} + n,
       org,
       CASE WHEN n % 5 = 0 THEN NULL ELSE (org - 1) * 4 + 1 + (n % 4) END,
       (ARRAY['Cannot login after password reset', 'Invoice total is wrong', 'API returns 500 on export',
              'SSO redirect loops', 'Refund not received', 'Login page blank on Safari'])[1 + n % 6] || ' #' || n,
       (ARRAY['open', 'pending', 'closed'])[1 + n % 3]::ticket_status,
       (ARRAY['low', 'normal', 'high'])[1 + (n / 3) % 3]::ticket_priority,
       CASE WHEN n % 3 = 0 THEN (n % 2 + 1) * interval '4 hours' ELSE NULL END,
       timestamptz '2026-10-01 09:00:00+00' + (n * interval '37 minutes'),
       timestamptz '2026-10-01 09:00:00+00' + (n * interval '37 minutes') + interval '1 hour'
FROM generate_series(1, 3) AS org, generate_series(1, {tickets}) AS n;

INSERT INTO comments (id, ticket_id, agent_id, body, created_at)
SELECT ((org - 1) * {tickets} + n) * 10 + c,
       (org - 1) * {tickets} + n,
       CASE WHEN c = 1 THEN (org - 1) * 4 + 1 + (n % 4) ELSE NULL END,
       CASE WHEN c = 1 THEN 'Looking into it.' ELSE 'Any update?' END,
       timestamptz '2026-10-01 10:00:00+00' + (n * interval '37 minutes') + (c * interval '15 minutes')
FROM generate_series(1, 3) AS org, generate_series(1, {commented}) AS n, generate_series(1, 2) AS c;

INSERT INTO attachments (id, ticket_id, name, data)
SELECT ((org - 1) * {tickets} + n) / 10,
       (org - 1) * {tickets} + n,
       'screenshot-' || n || '.png',
       decode('89504e470d0a1a0a' || lpad(to_hex(n), 8, '0'), 'hex')
FROM generate_series(1, 3) AS org, generate_series(10, {tickets}, 10) AS n;
COMMIT;
";

/// The scale from `EXAMPLE_SCALE`: 1 unless set to a larger number.
pub fn scale() -> i64 {
    std::env::var("EXAMPLE_SCALE")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(1)
        .max(1)
}

/// Tickets per organization at the scale.
pub fn tickets_per_org(scale: i64) -> i64 {
    TICKETS_PER_ORG * scale
}

/// The data statement at a scale.
pub fn data(scale: i64) -> String {
    let tickets = tickets_per_org(scale);
    DATA.replace("{tickets}", &tickets.to_string())
        .replace("{commented}", &(tickets / 3).to_string())
}

/// What the seed loads at a scale, for the report.
pub fn summary(scale: i64) -> String {
    let tickets = tickets_per_org(scale);
    format!(
        "3 organizations, 12 agents, {} tickets, {} comments, {} attachments (scale {scale})",
        3 * tickets,
        3 * (tickets / 3) * 2,
        3 * (tickets / 10)
    )
}

/// Creates the schema and loads the data at `EXAMPLE_SCALE` on a
/// blocking connection.
pub fn run<C: SimpleConnection>(conn: &mut C) -> QueryResult<()> {
    conn.batch_execute(SCHEMA)?;
    conn.batch_execute(&data(scale()))
}
