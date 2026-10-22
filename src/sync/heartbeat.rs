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

/// Heartbeats sent and the newest applied.
#[derive(Debug, Default)]
pub struct Freshness {
    beats: Mutex<Beats>,
}

/// What [`Freshness`] guards.
#[derive(Debug, Default)]
struct Beats {
    /// Heartbeats sent and not yet applied, oldest first, with when
    /// each was sent.
    sent: VecDeque<(u64, Instant)>,
    /// The newest heartbeat applied.
    applied: u64,
    /// When the newest applied heartbeat was sent.
    fresh: Option<Instant>,
}

impl Freshness {
    /// Records heartbeat `n` as sent at `at`. It may have been
    /// applied already.
    pub fn sent(&self, n: u64, at: Instant) {
        let mut beats = self.beats.lock().expect("heartbeats");
        if n > beats.applied {
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

/// Emits heartbeats on `client` every `every`, numbered from 1, until the
/// connection fails; each is recorded in `freshness` once it is in the
/// WAL, as sent when it was asked for. A non-transactional message is
/// decoded at once, in WAL order, whatever transactions are open.
pub async fn emit(
    client: &tokio_postgres::Client,
    prefix: &str,
    every: Duration,
    freshness: &Freshness,
) -> Result<(), tokio_postgres::Error> {
    let mut next = 1u64;
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let at = Instant::now();
        let content = next.to_string();
        client
            .execute(
                "SELECT pg_logical_emit_message(false, $1::text, $2::text)",
                &[&prefix, &content],
            )
            .await?;
        freshness.sent(next, at);
        next += 1;
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
}
