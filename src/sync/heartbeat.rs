//! How current the engine is, measured end to end. The engine emits a
//! numbered heartbeat into the database's WAL every so often, as a
//! logical message on a prefix of its own slot's ([`prefix`]); the
//! change feed carries it, in order, behind every transaction committed
//! before it; and the S2S endpoint marks it applied once it has passed
//! on those transactions' changes, which the engine thread hands it
//! first. Everything committed before the newest
//! applied heartbeat was sent is applied, so the engine is behind by at
//! most that heartbeat's age ([`Freshness::lag`]): under one interval
//! while it keeps up, growing from the moment it stops, whatever stops
//! it (a transaction too large to decode quickly, a stalled engine
//! thread, a slow feed), and measured on the engine's own clock. The
//! S2S endpoint passes the lag on, so a backend can stop trusting its
//! cache when the engine falls behind.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The most heartbeats kept waiting to be applied. Past it the second
/// oldest is dropped, so the oldest still dates a lag with none applied,
/// and a heartbeat applied later is dated by an older one kept, which
/// can only overstate the lag.
const WAITING: usize = 1024;

/// Heartbeats sent and the newest applied.
#[derive(Debug, Default)]
pub struct Freshness {
    beats: Mutex<Beats>,
}

/// What [`Freshness`] guards.
#[derive(Debug, Default)]
struct Beats {
    /// The number the next heartbeat goes out under: numbers keep rising
    /// across reconnects, since a number at or below the newest applied
    /// is not waited on.
    next: u64,
    /// The newest heartbeat written.
    written: u64,
    /// Heartbeats sent and not yet applied, oldest first, with when
    /// each was sent; at most [`WAITING`].
    sent: VecDeque<(u64, Instant)>,
    /// The newest heartbeat applied.
    applied: u64,
    /// When the newest applied heartbeat was sent.
    fresh: Option<Instant>,
}

impl Freshness {
    /// The number for the next heartbeat, never one handed out before.
    pub fn number(&self) -> u64 {
        let mut beats = self.beats.lock().expect("heartbeats");
        beats.next += 1;
        beats.next
    }

    /// The newest heartbeat written, 0 before the first.
    pub fn written(&self) -> u64 {
        self.beats.lock().expect("heartbeats").written
    }

    /// Records heartbeat `n` as sent at `at`. It may have been
    /// applied already.
    pub fn sent(&self, n: u64, at: Instant) {
        let mut beats = self.beats.lock().expect("heartbeats");
        beats.written = beats.written.max(n);
        if n > beats.applied {
            if beats.sent.len() >= WAITING {
                beats.sent.remove(1);
            }
            beats.sent.push_back((n, at));
        } else if n == beats.applied {
            beats.fresh = beats.fresh.max(Some(at));
        }
    }

    /// Records heartbeat `n` as applied, every change committed before it
    /// passed on, and every heartbeat before it.
    pub fn applied(&self, n: u64) {
        let mut beats = self.beats.lock().expect("heartbeats");
        if n <= beats.applied {
            return;
        }
        beats.applied = n;
        while let Some(&(sent, at)) = beats.sent.front() {
            if sent > n {
                break;
            }
            beats.fresh = beats.fresh.max(Some(at));
            beats.sent.pop_front();
        }
    }

    /// How far behind the engine is at most: the age of the newest
    /// heartbeat it has applied, or of the first one sent while it has
    /// applied none; `None` until the first is sent.
    pub fn lag(&self) -> Option<Duration> {
        let beats = self.beats.lock().expect("heartbeats");
        beats
            .fresh
            .or_else(|| beats.sent.front().map(|&(_, at)| at))
            .map(|at| at.elapsed())
    }
}

/// The logical-message prefix of the heartbeats of the engine streaming
/// `slot`: its own, so the heartbeats of other engines on the database
/// are not taken for its.
pub fn prefix(slot: &str) -> String {
    format!("{slot}/heartbeat")
}

/// The statement writing one heartbeat on a server of version `version`
/// (`server_version_num`). A message outside a transaction writes no
/// commit record, so nothing would flush it, and logical decoding reads
/// only flushed WAL: on a quiet database the heartbeat would wait for
/// someone else's commit. PostgreSQL 17 flushes it when asked; before
/// that, taking a transaction id gives the statement a commit record,
/// whose flush covers the message.
pub fn statement(version: u32) -> &'static str {
    if version >= 170_000 {
        "SELECT pg_logical_emit_message(false, $1::text, $2::text, true)"
    } else {
        "SELECT pg_logical_emit_message(false, $1::text, $2::text), pg_current_xact_id()"
    }
}

/// Emits heartbeats on `client` every `every`, numbered by `freshness`,
/// until the connection fails; each is recorded in `freshness` once it
/// is in the WAL and flushed, as sent when it was asked for. A
/// non-transactional message is decoded at once, in WAL order, whatever
/// transactions are open.
pub async fn emit(
    client: &tokio_postgres::Client,
    prefix: &str,
    every: Duration,
    freshness: &Freshness,
) -> Result<(), tokio_postgres::Error> {
    let version: u32 = client
        .query_one("SELECT current_setting('server_version_num')::int4", &[])
        .await?
        .get::<_, i32>(0)
        .try_into()
        .unwrap_or(0);
    let heartbeat = client.prepare(statement(version)).await?;
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let n = freshness.number();
        let at = Instant::now();
        let content = n.to_string();
        client.execute(&heartbeat, &[&prefix, &content]).await?;
        freshness.sent(n, at);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The lag is unknown before the first heartbeat, the age of the
    /// first one while none is applied, then the age of the newest
    /// applied one; applying a heartbeat applies those before it, and
    /// one applied before it is recorded as sent still counts.
    #[test]
    fn the_lag_is_the_newest_applied_heartbeats_age() {
        let freshness = Freshness::default();
        assert_eq!(freshness.lag(), None);
        let long_ago = Instant::now() - Duration::from_secs(5);
        freshness.sent(1, long_ago);
        freshness.sent(2, Instant::now());
        assert!(freshness.lag().unwrap() >= Duration::from_secs(5));
        freshness.applied(1);
        assert!(
            freshness.lag().unwrap() >= Duration::from_secs(5),
            "2 is unapplied"
        );
        freshness.applied(2);
        assert!(freshness.lag().unwrap() < Duration::from_secs(1));
        freshness.sent(3, long_ago);
        freshness.sent(4, Instant::now());
        freshness.applied(4);
        assert!(
            freshness.lag().unwrap() < Duration::from_secs(1),
            "4 applies 3 too"
        );
        freshness.applied(3);
        assert!(
            freshness.lag().unwrap() < Duration::from_secs(1),
            "an old number moves nothing"
        );
        freshness.applied(5);
        let at = Instant::now();
        freshness.sent(5, at);
        assert!(
            freshness.lag().unwrap() < Duration::from_secs(1),
            "applied before recorded"
        );
        freshness.sent(4, long_ago);
        assert!(
            freshness.lag().unwrap() < Duration::from_secs(1),
            "an old number is not waited on"
        );
    }

    /// Numbers keep rising however often the emitter starts over, so a
    /// heartbeat after a reconnect is waited on; only the newest written
    /// is remembered as written.
    #[test]
    fn numbers_never_repeat() {
        let freshness = Freshness::default();
        let first: Vec<u64> = (0..3).map(|_| freshness.number()).collect();
        assert_eq!(first, vec![1, 2, 3]);
        for &n in &first {
            freshness.sent(n, Instant::now());
        }
        freshness.applied(3);
        let after = freshness.number();
        assert_eq!(after, 4, "a reconnect does not start over");
        freshness.sent(after, Instant::now());
        assert_eq!(freshness.beats.lock().unwrap().sent.len(), 1, "and is waited on");
        freshness.applied(after);
        assert!(freshness.beats.lock().unwrap().sent.is_empty());
        assert_eq!(freshness.written(), 4);
    }

    /// While none is applied the waiting heartbeats stay bounded, the
    /// oldest kept, so the lag still dates from it.
    #[test]
    fn waiting_heartbeats_are_bounded() {
        let freshness = Freshness::default();
        let long_ago = Instant::now() - Duration::from_secs(60);
        freshness.sent(freshness.number(), long_ago);
        for _ in 0..10 * WAITING {
            freshness.sent(freshness.number(), Instant::now());
        }
        let waiting = freshness.beats.lock().unwrap().sent.len();
        assert_eq!(waiting, WAITING);
        assert!(freshness.lag().unwrap() >= Duration::from_secs(60));
    }

    /// PostgreSQL 17 is asked to flush the message; an older server is
    /// given a commit record to flush it with.
    #[test]
    fn the_heartbeat_is_flushed_on_every_version() {
        assert!(statement(170_002).ends_with(", true)"));
        assert!(statement(160_004).contains("pg_current_xact_id()"));
    }
}
