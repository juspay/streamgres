//! The metrics thread: every interval it reads what only sampling can
//! give (the process's resident set, each thread's CPU time by name and,
//! attributed to the core each thread was on, by core, the caches'
//! sizes) into the shared measurements, and every sixth sample it
//! writes the summary line, so a log alone says how the server is doing.
//! Nothing here runs on a thread that serves clients.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::connection::AppState;
use crate::log::{Level, log_error};
use crate::stats::Stats;

/// Start the sampler; `interval` zero starts nothing.
pub fn spawn(state: Arc<AppState>, interval: Duration) {
    if interval.is_zero() {
        return;
    }
    let spawned = std::thread::Builder::new()
        .name("xyne-sync-metrics".to_owned())
        .spawn(move || run(state, interval));
    if let Err(error) = spawned {
        log_error!("metrics thread: {error}");
    }
}

/// The sampler's loop.
fn run(state: Arc<AppState>, interval: Duration) {
    let stats: Arc<Stats> = state.stats.clone();
    let mut previous = stats.snapshot();
    let mut last_summary = Instant::now();
    let mut ticks = 0u64;
    let mut cores = CoreAccount::default();
    loop {
        std::thread::sleep(interval);
        ticks += 1;
        stats
            .process_rss_bytes
            .store(resident_bytes(), Ordering::Relaxed);
        stats
            .plan_cache_entries
            .store(state.plans.len() as u64, Ordering::Relaxed);
        stats
            .transform_cache_entries
            .store(state.transforms.len() as u64, Ordering::Relaxed);
        stats
            .warm_shapes
            .store(state.warm.len() as u64, Ordering::Relaxed);
        let samples = thread_samples();
        stats.publish_thread_cpu(thread_cpu_seconds(&samples));
        stats.publish_core_cpu(cores.account(&samples));
        if ticks % 6 == 0 {
            let (message, fields) = stats.summary_line(&previous, last_summary.elapsed());
            crate::log::event(Level::Info, message, fields);
            previous = stats.snapshot();
            stats.rotate_read_peak();
            last_summary = Instant::now();
        }
    }
}

/// The process's resident set in bytes: exact on Linux (`/proc`), the
/// peak so far elsewhere (`getrusage`).
pub fn resident_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            let mut fields = statm.split_whitespace();
            fields.next();
            if let Some(pages) = fields.next().and_then(|text| text.parse::<u64>().ok()) {
                // SAFETY: sysconf reads a constant.
                let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
                return pages * u64::try_from(page).unwrap_or(4096);
            }
        }
    }
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes the struct we hand it.
    let code = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if code != 0 {
        return 0;
    }
    // SAFETY: getrusage returned 0, so the struct is initialised.
    let usage = unsafe { usage.assume_init() };
    let maxrss = u64::try_from(usage.ru_maxrss).unwrap_or(0);
    if cfg!(target_os = "macos") {
        maxrss
    } else {
        maxrss * 1024
    }
}

/// One thread of the process as sampled: its id, its label
/// ([`thread_label`]), its CPU time so far in seconds, and the core it
/// last ran on (`processor` in `/proc/<tid>/stat`; `None` off Linux).
#[derive(Debug, Clone, PartialEq)]
pub struct ThreadSample {
    pub tid: u64,
    pub name: String,
    pub seconds: f64,
    pub core: Option<u32>,
}

/// Every thread of the process, sampled from `/proc` on Linux; nothing
/// elsewhere.
pub fn thread_samples() -> Vec<ThreadSample> {
    #[cfg_attr(not(target_os = "linux"), allow(unused_mut))]
    let mut samples = Vec::new();
    #[cfg(target_os = "linux")]
    {
        // SAFETY: sysconf reads a constant.
        let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        let ticks = if ticks > 0 { ticks as f64 } else { 100.0 };
        if let Ok(tasks) = std::fs::read_dir("/proc/self/task") {
            for task in tasks.flatten() {
                let path = task.path();
                let Some(tid) = task
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse::<u64>().ok())
                else {
                    continue;
                };
                let Ok(comm) = std::fs::read_to_string(path.join("comm")) else {
                    continue;
                };
                let Ok(stat) = std::fs::read_to_string(path.join("stat")) else {
                    continue;
                };
                let Some((seconds, core)) = parse_stat(&stat, ticks) else {
                    continue;
                };
                samples.push(ThreadSample {
                    tid,
                    name: thread_label(comm.trim()),
                    seconds,
                    core,
                });
            }
        }
    }
    samples
}

/// The CPU time and the core of a `/proc/<tid>/stat` line: `utime` and
/// `stime` (fields 14 and 15, in clock ticks of `ticks` a second)
/// summed, and `processor` (field 39), the fields taken after the last
/// `)` since the thread's name before it may hold spaces or parentheses.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_stat(stat: &str, ticks: f64) -> Option<(f64, Option<u32>)> {
    let after = stat.rsplit(')').next()?;
    let fields: Vec<&str> = after.split_whitespace().collect();
    let utime: f64 = fields.get(11)?.parse().ok()?;
    let stime: f64 = fields.get(12)?.parse().ok()?;
    let core = fields.get(36).and_then(|text| text.parse::<u32>().ok());
    Some(((utime + stime) / ticks, core))
}

/// CPU seconds so far by thread name (the server's own names, without
/// the `xyne-sync-` prefix and summed over a pool's threads), from
/// `samples`.
pub fn thread_cpu_seconds(samples: &[ThreadSample]) -> Vec<(String, f64)> {
    let mut by_name: std::collections::BTreeMap<String, f64> = std::collections::BTreeMap::new();
    for sample in samples {
        *by_name.entry(sample.name.clone()).or_insert(0.0) += sample.seconds;
    }
    by_name.into_iter().collect()
}

/// CPU time by core: each thread's CPU time since the last sample is
/// attributed to the core it was on at the sample, so a core's total is
/// what the process burned there, as closely as the sampling interval
/// tells a thread's moves apart (a shorter `XYNE_SYNC_METRICS_INTERVAL_MS`
/// sharpens it). A thread seen for the first time contributes nothing
/// yet; one gone is forgotten.
#[derive(Default)]
pub struct CoreAccount {
    last: std::collections::HashMap<u64, f64>,
    cores: std::collections::BTreeMap<u32, f64>,
}

impl CoreAccount {
    /// Take `samples` in and return the CPU seconds by core so far.
    pub fn account(&mut self, samples: &[ThreadSample]) -> Vec<(u32, f64)> {
        let mut seen = std::collections::HashMap::with_capacity(samples.len());
        for sample in samples {
            if let (Some(core), Some(&before)) = (sample.core, self.last.get(&sample.tid)) {
                *self.cores.entry(core).or_insert(0.0) += (sample.seconds - before).max(0.0);
            }
            seen.insert(sample.tid, sample.seconds);
        }
        self.last = seen;
        self.cores
            .iter()
            .map(|(&core, &seconds)| (core, seconds))
            .collect()
    }
}

/// A thread's label from its (15-character) kernel name.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn thread_label(comm: &str) -> String {
    let name = comm.strip_prefix("xyne-sync-").unwrap_or(comm);
    let name = match name {
        "engin" | "engine" => "engine",
        "reape" | "reaper" => "reaper",
        "serve" | "server" => "server",
        "metri" | "metrics" => "metrics",
        other if other.starts_with("group") => "groups",
        other if other.starts_with("reads") => "reads",
        other if other.starts_with("feed") => "feed",
        other if other.starts_with("log") => "log",
        other if other.starts_with("tokio") => "tokio",
        other => other,
    };
    name.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_names_fold_into_their_pools() {
        assert_eq!(thread_label("xyne-sync-engin"), "engine");
        assert_eq!(thread_label("xyne-sync-group"), "groups");
        assert_eq!(thread_label("xyne-sync-groups-3"), "groups");
        assert_eq!(thread_label("xyne-sync-reads"), "reads");
        assert_eq!(thread_label("xyne-sync-serve"), "server");
        assert_eq!(thread_label("xyne-sync-log"), "log");
        assert_eq!(thread_label("server"), "server");
    }

    #[test]
    fn the_resident_set_is_read() {
        assert!(resident_bytes() > 0);
    }

    /// A stat line's CPU time is `utime + stime` in ticks and its core the
    /// `processor` field, both read past the thread's name, which may
    /// hold spaces and parentheses.
    #[test]
    fn a_stat_line_gives_cpu_time_and_the_core() {
        let stat = "12345 (xyne-sync (eng) x) R 1 1 1 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 1 0 999 \
                    1000000 500 18446744073709551615 1 1 0 0 0 0 0 0 0 0 0 0 17 7 0 0 0 0 0 0 0 0 0 0 0 0 0";
        let (seconds, core) = parse_stat(stat, 100.0).expect("parsed");
        assert_eq!(seconds, 3.0);
        assert_eq!(core, Some(7));
        assert!(parse_stat("1 (x) R 1", 100.0).is_none());
    }

    /// CPU time since the last sample goes to the core the thread was
    /// sampled on; a first sample only sets the baseline, a thread gone is
    /// forgotten.
    #[test]
    fn cpu_time_is_attributed_to_the_core_a_thread_was_sampled_on() {
        let sample = |tid: u64, seconds: f64, core: u32| ThreadSample {
            tid,
            name: "engine".to_owned(),
            seconds,
            core: Some(core),
        };
        let mut account = CoreAccount::default();
        assert_eq!(account.account(&[sample(1, 10.0, 3)]), vec![]);
        assert_eq!(account.account(&[sample(1, 12.5, 3)]), vec![(3, 2.5)]);
        assert_eq!(
            account.account(&[sample(1, 13.0, 5), sample(2, 1.0, 5)]),
            vec![(3, 2.5), (5, 0.5)],
            "a move charges the new core; a new thread nothing yet"
        );
        assert_eq!(
            account.account(&[sample(2, 1.25, 5)]),
            vec![(3, 2.5), (5, 0.75)],
            "the thread gone is forgotten"
        );
        assert_eq!(
            thread_cpu_seconds(&[sample(1, 1.0, 0), sample(2, 2.0, 1)]),
            vec![("engine".to_owned(), 3.0)]
        );
    }
}
