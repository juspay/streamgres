//! Who owns a replication slot, and the sweep of slots whose owner is
//! gone.
//!
//! Each process streams from a permanent slot of its own. It owns the slot
//! through a session advisory lock keyed by the slot's name, held on a
//! connection of its own for as long as the process runs ([`SlotOwner`]).
//! A process that ends without dropping its slot (killed, out of memory,
//! its machine gone) leaves the slot holding WAL, but its lock goes with
//! its connection. So any engine on the database can tell a slot whose
//! owner is gone: the slot is inactive and its lock can be taken. Such a
//! slot is dropped ([`sweep`]); a slot whose owner still holds the lock is
//! kept, however long its feed has been reconnecting.
//!
//! The lock is taken on a direct connection, never through a pooler that
//! could hand the session to someone else.

use std::time::Duration;

use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Config};

use super::StorageError;
use crate::log::{log_info, log_warn};

/// The first key of every slot lock, so these locks never meet an
/// application's own advisory locks keyed by one `bigint`.
const LOCK_SPACE: i32 = 0x5347_534c; // "SGSL"

/// How often a running engine sweeps for slots whose owner is gone.
pub const SWEEP_EVERY: Duration = Duration::from_secs(60);

/// This process's ownership of a slot: the connection holding its lock.
/// Dropping it closes the connection and gives the ownership up.
pub struct SlotOwner {
    config: Config,
    slot: String,
    client: Client,
}

impl SlotOwner {
    /// Takes ownership of `slot`, waiting for the lock if another session
    /// holds it.
    pub async fn claim(config: &Config, slot: &str) -> Result<Self, StorageError> {
        let client = lock(config, slot).await?;
        Ok(SlotOwner {
            config: config.clone(),
            slot: slot.to_owned(),
            client,
        })
    }

    /// The slot owned.
    pub fn slot(&self) -> &str {
        &self.slot
    }

    /// Takes the lock again when its connection was lost. A sweep may have
    /// dropped the slot meanwhile; the feed then finds it gone and stops.
    pub async fn keep(&mut self) -> Result<(), StorageError> {
        if !self.client.is_closed() {
            return Ok(());
        }
        log_warn!(
            "the connection owning slot {} was lost; taking it again",
            self.slot
        );
        self.client = lock(&self.config, &self.slot).await?;
        Ok(())
    }
}

/// A fresh connection holding the lock of `slot`.
async fn lock(config: &Config, slot: &str) -> Result<Client, StorageError> {
    let client = super::open(config, &tokio::runtime::Handle::current()).await?;
    client
        .execute(
            "SELECT pg_advisory_lock($1, hashtext($2))",
            &[&LOCK_SPACE, &slot],
        )
        .await?;
    Ok(client)
}

/// Drops every inactive slot named with `prefix` whose owner is gone,
/// other than `keep`; returns the slots dropped. A slot that turns active
/// between the check and the drop is kept.
pub async fn sweep(config: &Config, prefix: &str, keep: &str) -> Result<Vec<String>, StorageError> {
    let client = super::open(config, &tokio::runtime::Handle::current()).await?;
    let candidates = client
        .query(
            "SELECT slot_name::text FROM pg_replication_slots
             WHERE left(slot_name, length($1)) = $1 AND slot_name <> $2
               AND NOT temporary AND NOT active
               AND database = current_database()",
            &[&prefix, &keep],
        )
        .await?;
    let mut dropped = Vec::new();
    for row in candidates {
        let slot: String = row.get(0);
        let free: bool = client
            .query_one(
                "SELECT pg_try_advisory_lock($1, hashtext($2))",
                &[&LOCK_SPACE, &slot],
            )
            .await?
            .get(0);
        if !free {
            continue;
        }
        let outcome = client
            .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await;
        client
            .execute(
                "SELECT pg_advisory_unlock($1, hashtext($2))",
                &[&LOCK_SPACE, &slot],
            )
            .await?;
        match outcome {
            Ok(_) => {
                log_info!("dropped replication slot {slot}: its owner is gone");
                dropped.push(slot);
            }
            Err(error)
                if matches!(
                    error.code(),
                    Some(&SqlState::OBJECT_IN_USE) | Some(&SqlState::UNDEFINED_OBJECT)
                ) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(dropped)
}
