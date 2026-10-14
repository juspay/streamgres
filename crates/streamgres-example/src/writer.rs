//! A trickle of changes, so the demo has something to keep fresh.
//!
//! Every tick moves one of Acme's tickets to its next status and bumps
//! its `updated_at`; every third tick adds a comment to it. It writes
//! through the plain pool, not the wrapper, so the cache learns of the
//! change the way it learns of any other service's: from the engine.

use std::time::Duration;

use diesel::sql_query;
use diesel::sql_types::BigInt;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::bb8::Pool;
use rand::Rng;
use tokio::task::JoinHandle;

/// Tickets of organization 1 have ids 1 to 60.
const TICKETS: i64 = 60;

pub fn spawn(plain: Pool<AsyncPgConnection>, every: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick: u64 = 0;
        loop {
            tokio::time::sleep(every).await;
            tick += 1;
            let id = rand::rng().random_range(1..=TICKETS);
            let Ok(mut conn) = plain.get().await else {
                continue;
            };
            let moved = sql_query(
                "UPDATE tickets SET status = (CASE status
                    WHEN 'open' THEN 'pending' WHEN 'pending' THEN 'closed' ELSE 'open'
                 END)::ticket_status, updated_at = now() WHERE id = $1",
            )
            .bind::<BigInt, _>(id)
            .execute(&mut conn)
            .await;
            if let Err(error) = moved {
                eprintln!("writer: {error}");
                continue;
            }
            if tick.is_multiple_of(3) {
                let _ = sql_query(
                    "INSERT INTO comments (id, ticket_id, body)
                     VALUES (nextval('comments_id_seq'), $1, 'Status changed by the background writer')",
                )
                .bind::<BigInt, _>(id)
                .execute(&mut conn)
                .await;
            }
        }
    })
}
