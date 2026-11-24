//! Bounded telemetry: recent activity for the page, and read counts and
//! latency samples per route.

use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;
use tokio::sync::broadcast;

pub fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
#[derive(Clone, Serialize)]
pub struct Event {
    pub id: u64,
    pub at: String,
    pub kind: String,
    pub message: String,
    pub detail: Value,
}
#[derive(Default)]
struct RouteStats {
    reads: u64,
    memory: u64,
    fallbacks: u64,
    reasons: BTreeMap<String, u64>,
    samples: VecDeque<f64>,
}
pub struct Telemetry {
    started: Instant,
    inner: Mutex<Inner>,
    pub events: broadcast::Sender<Event>,
}
struct Inner {
    next: u64,
    recent: VecDeque<Event>,
    routes: BTreeMap<String, RouteStats>,
    last_read_event: Option<Instant>,
}
impl Default for Telemetry {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            inner: Mutex::new(Inner {
                next: 1,
                recent: VecDeque::new(),
                routes: BTreeMap::new(),
                last_read_event: None,
            }),
            events: broadcast::channel(256).0,
        }
    }
}
impl Telemetry {
    pub fn emit(&self, kind: &str, message: String, detail: Value) {
        let event = {
            let mut inner = lock(&self.inner);
            if kind == "read" {
                if inner
                    .last_read_event
                    .is_some_and(|last| last.elapsed() < std::time::Duration::from_millis(50))
                {
                    return;
                }
                inner.last_read_event = Some(Instant::now());
            }
            let event = Event {
                id: inner.next,
                at: chrono::Utc::now().to_rfc3339(),
                kind: kind.into(),
                message,
                detail,
            };
            inner.next = inner.next.saturating_add(1);
            if inner.recent.len() >= 120 {
                inner.recent.pop_front();
            }
            inner.recent.push_back(event.clone());
            event
        };
        let _ = self.events.send(event);
    }
    /// Counts one read of route `key`: whether memory answered it, how
    /// long it took, and, for a read through Streamgres that Postgres
    /// answered, why.
    pub fn read(
        &self,
        key: &str,
        served: &str,
        through_streamgres: bool,
        micros: f64,
        reason: Option<&str>,
    ) {
        let mut inner = lock(&self.inner);
        let row = inner.routes.entry(key.to_owned()).or_default();
        row.reads = row.reads.saturating_add(1);
        if served == "cache" {
            row.memory = row.memory.saturating_add(1);
        }
        if through_streamgres && served == "postgres" {
            row.fallbacks = row.fallbacks.saturating_add(1);
            let mut key = reason
                .unwrap_or("not reported")
                .chars()
                .take(240)
                .collect::<String>();
            if row.reasons.len() >= 32 && !row.reasons.contains_key(&key) {
                key = "other reasons".into();
            }
            let count = row.reasons.entry(key).or_default();
            *count = count.saturating_add(1);
        }
        if row.samples.len() >= 1000 {
            row.samples.pop_front();
        }
        row.samples.push_back(micros);
    }
    pub fn snapshot(&self) -> Value {
        let inner = lock(&self.inner);
        let routes = inner.routes.iter().map(|(name, row)| {
            let mut samples: Vec<_> = row.samples.iter().copied().collect();
            samples.sort_by(f64::total_cmp);
            let percentile = |q: usize| samples.get(samples.len().saturating_sub(1) * q / 100).copied().unwrap_or(0.0);
            (name.clone(), json!({"reads": row.reads, "memory": row.memory, "fallbacks": row.fallbacks, "reasons": row.reasons, "p50_us": percentile(50), "p99_us": percentile(99), "sample_count": samples.len()}))
        }).collect::<serde_json::Map<String, Value>>();
        json!({"uptime_seconds": self.started.elapsed().as_secs(), "routes": routes, "events": inner.recent})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unsupported_and_failed_reads_are_not_postgres_fallbacks() {
        let telemetry = Telemetry::default();
        for served in ["unsupported", "failed", "cache", "postgres"] {
            telemetry.read("t", served, true, 1.0, None);
        }
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot["routes"]["t"]["reads"], 4);
        assert_eq!(snapshot["routes"]["t"]["memory"], 1);
        assert_eq!(snapshot["routes"]["t"]["fallbacks"], 1);
    }
}
