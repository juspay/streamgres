//! What the wrapper does, for the application's own metrics and logs.
//!
//! Three outlets, so a cache that stops serving is noticed rather than
//! found out:
//!
//! - **[`Metrics`]**, a hook the application implements over its own
//!   telemetry and installs once ([`set_metrics`]). Every read the cache
//!   could serve reports where it was served; fallbacks, refusals, engine
//!   connections, lag, and subscriptions losing changes or ending report
//!   as they happen. Every method has an empty default.
//! - **Log lines** through the `log` facade, under the target
//!   `diesel_streamgres`: connections up and down, the engine falling
//!   behind and catching up, fallbacks, refusals and subscriptions that
//!   end. A line repeated under the same key is written at most once a
//!   minute, with a count of those held back.
//! - **[`history`]**: the latest events with their times, for reading
//!   back what happened around an incident.
//!
//! The service-level indicators these give: the share of eligible reads
//! served from the cache or the engine ([`Served`]), the engine's lag
//! against `STREAMGRES_S2S_MAX_LAG_MS`, the rate of fallbacks and
//! refusals, and, with shadow reads on ([`crate::shadow`]), the rate of
//! cached answers that disagree with Postgres.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

use crate::route::Served;

/// The application's metrics. Implement what it records; the rest stay
/// empty.
pub trait Metrics: Send + Sync {
    /// A read the cache could serve (not one routed to Postgres, in a
    /// transaction, or a write), and where it was served.
    fn served(&self, _served: Served) {}
    /// A read the cache could serve, sent to Postgres by the rollout share
    /// (`STREAMGRES_CACHE_SHARE`); also reported as served by Postgres.
    fn bypassed(&self) {}
    /// A query that went from the engine's rows to refills, and why.
    fn fallback(&self, _reason: &str) {}
    /// A query the engine, or this client, refused, and why.
    fn refused(&self, _reason: &str) {}
    /// Engine socket `index` came up (`true`) or went down (`false`).
    fn connection(&self, _index: usize, _up: bool) {}
    /// The engine's lag, as each socket reports it.
    fn lag(&self, _lag: Duration) {}
    /// A callback subscription lost changes (`reason`): its consumer
    /// reads afresh.
    fn missed(&self, _reason: &str) {}
    /// A callback subscription ended: the engine no longer follows it.
    fn ended(&self, _reason: &str) {}
    /// A shadow read's outcome ([`crate::shadow`]).
    fn shadow(&self, _outcome: crate::shadow::Outcome) {}
}

static METRICS: RwLock<Option<Arc<dyn Metrics>>> = RwLock::new(None);

/// Installs the application's metrics for every client and connection of
/// this process, replacing any installed before.
pub fn set_metrics(metrics: Arc<dyn Metrics>) {
    *METRICS.write().expect("metrics") = Some(metrics);
}

/// Calls `f` with the installed metrics, if any.
pub(crate) fn report(f: impl FnOnce(&dyn Metrics)) {
    let installed = METRICS.read().expect("metrics").clone();
    if let Some(metrics) = installed {
        f(metrics.as_ref());
    }
}

/// How many events [`history`] keeps.
pub const HISTORY: usize = 256;

static EVENTS: Mutex<VecDeque<(SystemTime, String)>> = Mutex::new(VecDeque::new());

/// The latest events (connections, lag, fallbacks, refusals, ended and
/// lossy subscriptions), oldest first, each with when it happened.
pub fn history() -> Vec<(SystemTime, String)> {
    EVENTS.lock().expect("events").iter().cloned().collect()
}

/// Records `event` in the history.
pub(crate) fn record(event: String) {
    let mut events = EVENTS.lock().expect("events");
    if events.len() >= HISTORY {
        events.pop_front();
    }
    events.push_back((SystemTime::now(), event));
}

/// How often a log line under one key is written at most.
const LOG_EVERY: Duration = Duration::from_secs(60);

/// Per key: when its line was last written, and how many were held back
/// since.
static WRITTEN: Mutex<Option<HashMap<String, (Instant, u64)>>> = Mutex::new(None);

/// Records `message` in the history and writes it at `level`, at most once
/// a minute under `key`; the next line written says how many were held
/// back.
pub(crate) fn note(level: log::Level, key: &str, message: String) {
    record(message.clone());
    let held_back = {
        let mut written = WRITTEN.lock().expect("written");
        let written = written.get_or_insert_with(HashMap::new);
        match written.get_mut(key) {
            Some((at, held)) if at.elapsed() < LOG_EVERY => {
                *held += 1;
                return;
            }
            Some((at, held)) => {
                *at = Instant::now();
                std::mem::take(held)
            }
            None => {
                if written.len() >= 1024 {
                    written.retain(|_, (at, _)| at.elapsed() < LOG_EVERY);
                }
                written.insert(key.to_string(), (Instant::now(), 0));
                0
            }
        }
    };
    if held_back > 0 {
        log::log!(target: "diesel_streamgres", level, "{message} ({held_back} more like it in the last minute)");
    } else {
        log::log!(target: "diesel_streamgres", level, "{message}");
    }
}

/// The key a reason is logged under: its first words, so reasons that
/// differ only in a query or a number share one.
pub(crate) fn reason_key(kind: &str, reason: &str) -> String {
    let head: String = reason
        .split(|c: char| c == ':' || c == '`' || c.is_ascii_digit())
        .next()
        .unwrap_or(reason)
        .chars()
        .take(80)
        .collect();
    format!("{kind}:{head}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Counts what is reported.
    #[derive(Default)]
    struct Counting {
        served: AtomicUsize,
        fallbacks: AtomicUsize,
    }

    impl Metrics for Counting {
        fn served(&self, _served: Served) {
            self.served.fetch_add(1, Ordering::Relaxed);
        }
        fn fallback(&self, _reason: &str) {
            self.fallbacks.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Installed metrics receive reports; methods left out stay empty.
    #[test]
    fn installed_metrics_receive_reports() {
        let counting = Arc::new(Counting::default());
        set_metrics(counting.clone());
        report(|metrics| metrics.served(Served::Cache));
        report(|metrics| metrics.fallback("a reason"));
        report(|metrics| metrics.lag(Duration::from_millis(5)));
        assert!(counting.served.load(Ordering::Relaxed) >= 1);
        assert!(counting.fallbacks.load(Ordering::Relaxed) >= 1);
    }

    /// Every note reaches the history, with its time, whatever the log
    /// holds back.
    #[test]
    fn notes_reach_the_history() {
        for n in 0..3 {
            note(log::Level::Warn, "test:history", format!("note {n} for the history test"));
        }
        let history = history();
        let ours: Vec<&String> = history
            .iter()
            .map(|(_, event)| event)
            .filter(|event| event.ends_with("for the history test"))
            .collect();
        assert_eq!(ours.len(), 3);
    }

    /// Reasons that differ only in a query or a number share a key.
    #[test]
    fn reasons_share_keys_by_their_first_words() {
        assert_eq!(
            reason_key("fallback", "types of `a.b` unavailable"),
            reason_key("fallback", "types of `c` unavailable")
        );
        assert_ne!(
            reason_key("fallback", "types of `a` unavailable"),
            reason_key("refused", "types of `a` unavailable")
        );
    }
}
