//! The change feed's replication slot over its life: whose it is, and who
//! drops it once nobody will stream from it again.
//!
//! Each start makes a permanent slot of its own, named [`SLOT_PREFIX`]
//! and a fresh UUID, and drops it when it stops. A process that dies
//! without stopping (killed, out of memory, its machine gone) leaves the
//! slot behind, and PostgreSQL keeps every byte of WAL since for it. So a
//! process **owns** its slot: before making it, it takes a session
//! advisory lock keyed on the slot's name ([`SlotOwner`]), on a connection
//! it holds for as long as it streams. The lock goes with that connection,
//! however the process ends. Any engine's **sweep** ([`sweep`], at its
//! start and every minute) drops a slot of the prefix that no walsender
//! streams from and whose lock it can take: its owner is gone. That works
//! on every PostgreSQL version, where an age-based rule needs 17's
//! `inactive_since`.
//!
//! Taking the lock before making the slot leaves no moment in which a
//! slot exists unowned while its process lives. A process whose lock
//! connection drops cannot tell whether a sweep dropped its slot since,
//! so it starts over on a fresh one ([`SlotOwner::take`]).
//!
//! What the sweep cannot help is a database no engine runs against any
//! more: the slot stays until someone drops it, or until PostgreSQL
//! invalidates it at `max_slot_wal_keep_size`, which is unlimited by
//! default ([`warn_if_unbounded`]).

use std::time::Duration;

use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls};

use super::StorageError;
use super::threads::SLOT_PREFIX;
use crate::log::{log_info, log_warn};
use crate::shutdown::Shutdown;

/// The first key of the session advisory lock a process holds on each slot
/// it owns; the second is `hashtext` of the slot's name.
pub const OWNER_LOCK: i32 = 0x5347_4f57; // "SGOW"

/// How often a running engine sweeps the slots of ended processes.
pub const SWEEP_EVERY: Duration = Duration::from_secs(60);

/// A process's hold on its slot: the owner lock, on a connection of its
/// own. Dropping it ends the connection, and the lock with it.
pub struct SlotOwner {
    _client: Client,
}

impl SlotOwner {
    /// Takes the owner lock of `slot`, on a connection of its own to
    /// `dsn` driven on `runtime`, before the slot is made. Should that
    /// connection end while `shutdown` is not requested, `shutdown`
    /// fails: a sweep may drop the slot from then on, so the process
    /// starts over on a fresh one rather than stream from a slot that may
    /// be gone.
    pub async fn take(
        dsn: &str,
        slot: &str,
        runtime: &tokio::runtime::Handle,
        shutdown: Shutdown,
    ) -> Result<SlotOwner, StorageError> {
        let config: tokio_postgres::Config = dsn.parse()?;
        let (client, connection) = config.connect(NoTls).await?;
        let name = slot.to_owned();
        runtime.spawn(async move {
            let ended = connection.await;
            if !shutdown.requested() {
                let why = match ended {
                    Ok(()) => "it closed".to_owned(),
                    Err(error) => error.to_string(),
                };
                log_warn!("the connection holding the owner lock of slot {name} ended: {why}");
                shutdown.fail(format!(
                    "the connection holding the owner lock of slot {name} ended ({why}); a sweep may drop the slot now, so the engine starts over on a fresh one"
                ));
            }
        });
        client
            .execute("SELECT pg_advisory_lock($1, hashtext($2))", &[&OWNER_LOCK, &slot])
            .await?;
        Ok(SlotOwner { _client: client })
    }
}

/// Drops the slots of ended processes on the database `dsn`: each slot of
/// [`SLOT_PREFIX`] that no walsender streams from and whose owner lock
/// this sweep can take, its owner being gone. With an `age` (PostgreSQL
/// 17 or later, an error on an older one), also each such slot inactive
/// for at least that long, whoever holds its lock. Returns how many it
/// dropped.
pub async fn sweep(dsn: &str, age: Duration) -> Result<usize, StorageError> {
    let client = connect(dsn).await?;
    let aged = if age.is_zero() {
        None
    } else {
        let version: i32 = client
            .query_one("SELECT current_setting('server_version_num')::int", &[])
            .await?
            .get(0);
        if version < 170_000 {
            return Err(StorageError(format!(
                "STREAMGRES_SLOT_CLEANUP_AGE_MS requires PostgreSQL 17 or later (server is {version}); PostgreSQL 16 has no inactive_since timestamp, so safe age-bounded cleanup is impossible (the owner-lock sweep needs no age, and runs on every version)"
            )));
        }
        Some(i64::try_from(age.as_millis()).unwrap_or(i64::MAX))
    };
    let candidates = match aged {
        None => {
            client
                .query(
                    "SELECT slot_name::text, false FROM pg_replication_slots
                      WHERE left(slot_name, length($1)) = $1 AND database = current_database()
                        AND NOT temporary AND NOT active",
                    &[&SLOT_PREFIX],
                )
                .await?
        }
        Some(age_ms) => {
            client
                .query(
                    "SELECT slot_name::text,
                            coalesce(inactive_since <= clock_timestamp() - $2::bigint * interval '1 millisecond', false)
                       FROM pg_replication_slots
                      WHERE left(slot_name, length($1)) = $1 AND database = current_database()
                        AND NOT temporary AND NOT active",
                    &[&SLOT_PREFIX, &age_ms],
                )
                .await?
        }
    };
    let mut dropped = 0;
    for row in candidates {
        let slot: String = row.get(0);
        let old: bool = row.get(1);
        let unowned: bool = client
            .query_one("SELECT pg_try_advisory_lock($1, hashtext($2))", &[&OWNER_LOCK, &slot])
            .await?
            .get(0);
        if unowned || old {
            match client
                .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
                .await
            {
                Ok(_) => {
                    dropped += 1;
                    if unowned {
                        log_info!("dropped replication slot {slot}: inactive, and its owner is gone");
                    } else {
                        log_info!("dropped replication slot {slot}: inactive for over {age:?}");
                    }
                }
                // A walsender may have claimed the slot after the candidate
                // query. PostgreSQL refuses to drop an active slot; leave it.
                Err(error) if error.code() == Some(&SqlState::OBJECT_IN_USE) => {
                    log_info!("replication slot {slot} became active during the sweep; keeping it");
                }
                // Another engine's sweep dropped it first.
                Err(error) if error.code() == Some(&SqlState::UNDEFINED_OBJECT) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if unowned {
            client
                .execute("SELECT pg_advisory_unlock($1, hashtext($2))", &[&OWNER_LOCK, &slot])
                .await?;
        }
    }
    Ok(dropped)
}

/// [`sweep`] every [`SWEEP_EVERY`] until `shutdown`; a sweep that fails is
/// logged and tried again at the next.
pub async fn sweep_every(dsn: String, age: Duration, shutdown: Shutdown) {
    loop {
        tokio::select! {
            () = tokio::time::sleep(SWEEP_EVERY) => {}
            () = shutdown.wait() => return,
        }
        if let Err(error) = sweep(&dsn, age).await {
            log_warn!("dropping the replication slots of ended processes: {error}");
        }
    }
}

/// Warns, once, when the server keeps WAL for a slot without bound
/// (`max_slot_wal_keep_size = -1`, PostgreSQL's default): a slot left by
/// a process that died, on a database no engine sweeps any more, would
/// then hold WAL until the disk fills. A failure to ask is logged and
/// otherwise ignored.
pub async fn warn_if_unbounded(dsn: &str) {
    let unbounded = async {
        let client = connect(dsn).await?;
        let size: String = client
            .query_one("SELECT current_setting('max_slot_wal_keep_size')", &[])
            .await?
            .get(0);
        Ok::<bool, StorageError>(size.trim() == "-1")
    };
    match unbounded.await {
        Ok(true) => log_warn!(
            "max_slot_wal_keep_size is -1 (unlimited): the slot of a process that died holds WAL until an engine on its database sweeps it, and with no engine left running nothing does; set max_slot_wal_keep_size (a few GB, say) so PostgreSQL invalidates such a slot before it fills the disk"
        ),
        Ok(false) => {}
        Err(error) => log_warn!("reading max_slot_wal_keep_size: {error}"),
    }
}

/// A connection to `dsn`, its driver spawned on the current runtime.
async fn connect(dsn: &str) -> Result<Client, StorageError> {
    let config: tokio_postgres::Config = dsn.parse()?;
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}
