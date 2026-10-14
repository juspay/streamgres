//! The schema and a small, readable, deterministic dataset.
//!
//! Three organizations, four agents each, sixty tickets each with
//! statuses and priorities spread evenly, comments on the first twenty
//! tickets of each, an attachment on every tenth, a service level on
//! every third. Ids are fixed, so the demo's URLs and the scenario
//! test's expectations do not move between runs. Reseeding deletes and
//! inserts rather than truncating, so an engine already following the
//! tables sees ordinary changes.

use diesel::QueryResult;
use diesel::connection::SimpleConnection;

/// Creates the types and tables if they do not exist.
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
";

/// Replaces the data. `base` is a fixed instant so timestamps are the
/// same on every run.
pub const DATA: &str = "
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
SELECT (org - 1) * 60 + n,
       org,
       CASE WHEN n % 5 = 0 THEN NULL ELSE (org - 1) * 4 + 1 + (n % 4) END,
       (ARRAY['Cannot login after password reset', 'Invoice total is wrong', 'API returns 500 on export',
              'SSO redirect loops', 'Refund not received', 'Login page blank on Safari'])[1 + n % 6] || ' #' || n,
       (ARRAY['open', 'pending', 'closed'])[1 + n % 3]::ticket_status,
       (ARRAY['low', 'normal', 'high'])[1 + (n / 3) % 3]::ticket_priority,
       CASE WHEN n % 3 = 0 THEN (n % 2 + 1) * interval '4 hours' ELSE NULL END,
       timestamptz '2026-10-01 09:00:00+00' + (n * interval '37 minutes'),
       timestamptz '2026-10-01 09:00:00+00' + (n * interval '37 minutes') + interval '1 hour'
FROM generate_series(1, 3) AS org, generate_series(1, 60) AS n;

INSERT INTO comments (id, ticket_id, agent_id, body, created_at)
SELECT ((org - 1) * 60 + n) * 10 + c,
       (org - 1) * 60 + n,
       CASE WHEN c = 1 THEN (org - 1) * 4 + 1 + (n % 4) ELSE NULL END,
       CASE WHEN c = 1 THEN 'Looking into it.' ELSE 'Any update?' END,
       timestamptz '2026-10-01 10:00:00+00' + (n * interval '37 minutes') + (c * interval '15 minutes')
FROM generate_series(1, 3) AS org, generate_series(1, 20) AS n, generate_series(1, 2) AS c;

INSERT INTO attachments (id, ticket_id, name, data)
SELECT ((org - 1) * 60 + n) / 10,
       (org - 1) * 60 + n,
       'screenshot-' || n || '.png',
       decode('89504e470d0a1a0a' || lpad(to_hex(n), 8, '0'), 'hex')
FROM generate_series(1, 3) AS org, generate_series(10, 60, 10) AS n;
COMMIT;
";

/// Creates the schema and loads the data on a blocking connection.
pub fn run<C: SimpleConnection>(conn: &mut C) -> QueryResult<()> {
    conn.batch_execute(SCHEMA)?;
    conn.batch_execute(DATA)
}
