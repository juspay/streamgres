//! A stream of changes, so the demo has something to keep fresh and the
//! verify loop something to catch.
//!
//! Every tick runs one operation drawn from the mix: a status moved on
//! (`status`), a comment added (`comment`), a ticket inserted into the
//! first organization's queue (`insert`), one of the writer's own
//! tickets deleted with its comments cascading (`delete`), one moved to
//! another organization so it leaves one subscription and enters
//! another (`move`), an agent renamed under the joins (`rename`), a
//! ticket's assignee flipped between an agent and `NULL` (`assign`),
//! several tickets touched in one transaction (`bulk`), a change rolled
//! back so nothing must reach the cache (`rollback`), and a comment
//! with a body too large for its row written then updated without
//! touching the body, so the engine sees a partial image (`toast`).
//!
//! Rows the writer creates take ids from the sequences, above the
//! seed's, and bodies marked `writer:`, so the seeded rows the demo's
//! URLs point at stay as they are apart from their status, assignee and
//! timestamps. The writer uses the plain pool, not the wrapper, so its
//! changes reach the cache the way another service's would: through the
//! engine, not the read-your-writes window.
//!
//! `EXAMPLE_WRITER_MIX` names the operations to draw from, comma
//! separated; the default is all of them.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use diesel::sql_query;
use diesel::sql_types::{BigInt, Text};
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use rand::Rng;
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use crate::seed;

/// The operations, in the order the counters report them.
pub const OPS: &[&str] = &[
    "status", "comment", "insert", "delete", "move", "rename", "assign", "bulk", "rollback",
    "toast",
];

/// How many of the writer's own tickets it keeps around before an
/// insert becomes a delete.
const KEEP: usize = 50;

/// How many times each operation ran, and how many failed.
#[derive(Default)]
pub struct Counts {
    done: [AtomicU64; 10],
    errors: AtomicU64,
}

impl Counts {
    /// The counters as JSON, by operation name.
    pub fn json(&self) -> Value {
        let mut map = serde_json::Map::new();
        for (index, name) in OPS.iter().enumerate() {
            map.insert(
                (*name).to_string(),
                json!(AtomicU64::load(&self.done[index], Ordering::Relaxed)),
            );
        }
        map.insert(
            "errors".to_string(),
            json!(AtomicU64::load(&self.errors, Ordering::Relaxed)),
        );
        Value::Object(map)
    }

    /// The total of every operation that ran.
    pub fn total(&self) -> u64 {
        self.done
            .iter()
            .map(|count| AtomicU64::load(count, Ordering::Relaxed))
            .sum()
    }

    /// How many operations failed.
    pub fn errors(&self) -> u64 {
        AtomicU64::load(&self.errors, Ordering::Relaxed)
    }

    /// How many of `op` ran.
    pub fn of(&self, op: &str) -> u64 {
        OPS.iter().position(|name| *name == op).map_or(0, |index| {
            AtomicU64::load(&self.done[index], Ordering::Relaxed)
        })
    }
}

/// The mix from `EXAMPLE_WRITER_MIX`, every operation when unset; a
/// name that is not an operation is reported and skipped.
pub fn mix_from_env() -> Vec<&'static str> {
    let Ok(list) = std::env::var("EXAMPLE_WRITER_MIX") else {
        return OPS.to_vec();
    };
    let mut mix = Vec::new();
    for name in list
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        match OPS.iter().find(|op| **op == name) {
            Some(op) => mix.push(*op),
            None => eprintln!("writer: no operation `{name}`; the operations are {OPS:?}"),
        }
    }
    if mix.is_empty() { OPS.to_vec() } else { mix }
}

/// A returned id.
#[derive(diesel::QueryableByName)]
struct Id {
    #[diesel(sql_type = BigInt)]
    id: i64,
}

/// What the writer remembers between ticks: its own tickets, and the
/// comment it is using for the partial-image case.
struct State {
    tick: u64,
    tickets_per_org: i64,
    created: VecDeque<i64>,
    toast: Option<i64>,
}

impl State {
    /// A seeded ticket of the first organization.
    fn seeded(&self) -> i64 {
        rand::rng().random_range(1..=self.tickets_per_org)
    }

    /// One of the writer's own tickets, if it has any.
    fn own(&self) -> Option<i64> {
        if self.created.is_empty() {
            return None;
        }
        let index = rand::rng().random_range(0..self.created.len());
        self.created.get(index).copied()
    }
}

/// Starts the writer: one operation from `mix` every `every`, counted
/// in `counts`.
pub fn spawn(
    plain: Pool<AsyncPgConnection>,
    every: Duration,
    mix: Vec<&'static str>,
    counts: std::sync::Arc<Counts>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut state = State {
            tick: 0,
            tickets_per_org: seed::tickets_per_org(seed::scale()),
            created: VecDeque::new(),
            toast: None,
        };
        loop {
            tokio::time::sleep(every).await;
            state.tick += 1;
            let op = mix[rand::rng().random_range(0..mix.len())];
            let Ok(mut conn) = plain.get().await else {
                counts.errors.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            match step(op, &mut conn, &mut state).await {
                Ok(()) => {
                    let index = OPS.iter().position(|name| *name == op).unwrap_or(0);
                    counts.done[index].fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    counts.errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("writer: {op}: {error}");
                }
            }
        }
    })
}

/// Runs one operation.
async fn step(
    op: &str,
    conn: &mut AsyncPgConnection,
    state: &mut State,
) -> Result<(), diesel::result::Error> {
    match op {
        "status" => {
            sql_query(
                "UPDATE tickets SET status = (CASE status
                    WHEN 'open' THEN 'pending' WHEN 'pending' THEN 'closed' ELSE 'open'
                 END)::ticket_status, updated_at = now() WHERE id = $1",
            )
            .bind::<BigInt, _>(state.seeded())
            .execute(conn)
            .await?;
        }
        "comment" => {
            sql_query(
                "INSERT INTO comments (id, ticket_id, body)
                 VALUES (nextval('comments_id_seq'), $1, 'writer: status changed')",
            )
            .bind::<BigInt, _>(state.seeded())
            .execute(conn)
            .await?;
        }
        "insert" => {
            if state.created.len() >= KEEP {
                return Box::pin(step("delete", conn, state)).await;
            }
            let row: Id = sql_query(
                "INSERT INTO tickets (id, org_id, title, status, priority)
                 VALUES (nextval('tickets_id_seq'), 1, 'writer: ticket ' || $1, 'open', 'normal')
                 RETURNING id",
            )
            .bind::<Text, _>(state.tick.to_string())
            .get_result(conn)
            .await?;
            state.created.push_back(row.id);
        }
        "delete" => {
            let Some(id) = state.created.pop_front() else {
                return Box::pin(step("insert", conn, state)).await;
            };
            sql_query("DELETE FROM tickets WHERE id = $1")
                .bind::<BigInt, _>(id)
                .execute(conn)
                .await?;
        }
        "move" => {
            let Some(id) = state.own() else {
                return Box::pin(step("insert", conn, state)).await;
            };
            sql_query(
                "UPDATE tickets SET org_id = CASE org_id WHEN 1 THEN 2 ELSE 1 END, updated_at = now()
                 WHERE id = $1",
            )
            .bind::<BigInt, _>(id)
            .execute(conn)
            .await?;
        }
        "rename" => {
            sql_query(
                "UPDATE agents SET name = regexp_replace(name, ' \\(v\\d+\\)$', '') || ' (v' || $1 || ')'
                 WHERE id = $2",
            )
            .bind::<Text, _>(state.tick.to_string())
            .bind::<BigInt, _>(1 + (state.tick % 4) as i64)
            .execute(conn)
            .await?;
        }
        "assign" => {
            let id = state.seeded();
            sql_query(
                "UPDATE tickets SET agent_id = CASE WHEN agent_id IS NULL THEN $2 ELSE NULL END,
                 updated_at = now() WHERE id = $1",
            )
            .bind::<BigInt, _>(id)
            .bind::<BigInt, _>(1 + id % 4)
            .execute(conn)
            .await?;
        }
        "bulk" => {
            sql_query("UPDATE tickets SET updated_at = now() WHERE org_id = 1 AND id % 7 = $1")
                .bind::<BigInt, _>((state.tick % 7) as i64)
                .execute(conn)
                .await?;
        }
        "rollback" => {
            let id = state.seeded();
            conn.batch_execute(&format!(
                "BEGIN; UPDATE tickets SET title = 'writer: rolled back' WHERE id = {id}; ROLLBACK"
            ))
            .await?;
        }
        "toast" => match state.toast {
            None => {
                let row: Id = sql_query(
                    "INSERT INTO comments (id, ticket_id, body)
                     VALUES (nextval('comments_id_seq'), $1, 'writer: toast ' || repeat('x', 4096))
                     RETURNING id",
                )
                .bind::<BigInt, _>(state.seeded())
                .get_result(conn)
                .await?;
                state.toast = Some(row.id);
            }
            Some(id) if state.tick % 5 == 0 => {
                sql_query("DELETE FROM comments WHERE id = $1")
                    .bind::<BigInt, _>(id)
                    .execute(conn)
                    .await?;
                state.toast = None;
            }
            Some(id) => {
                sql_query("UPDATE comments SET created_at = now() WHERE id = $1")
                    .bind::<BigInt, _>(id)
                    .execute(conn)
                    .await?;
            }
        },
        other => eprintln!("writer: no operation `{other}`"),
    }
    Ok(())
}
