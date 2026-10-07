//! The metrics thread: every interval it reads what only sampling can
//! give (the process's resident set, each thread's CPU time by name, the
//! caches' sizes) into the shared measurements, and every sixth sample it
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
        stats.publish_thread_cpu(thread_cpu_seconds());
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

/// CPU seconds so far by thread name (the server's own names, without
/// the `xyne-sync-` prefix and summed over a pool's threads); Linux
/// only, from `/proc`, nothing elsewhere.
pub fn thread_cpu_seconds() -> Vec<(String, f64)> {
    #[cfg_attr(not(target_os = "linux"), allow(unused_mut))]
    let mut by_name: std::collections::BTreeMap<String, f64> = std::collections::BTreeMap::new();
    #[cfg(target_os = "linux")]
    {
        // SAFETY: sysconf reads a constant.
        let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        let ticks = if ticks > 0 { ticks as f64 } else { 100.0 };
        if let Ok(tasks) = std::fs::read_dir("/proc/self/task") {
            for task in tasks.flatten() {
                let path = task.path();
                let Ok(comm) = std::fs::read_to_string(path.join("comm")) else {
                    continue;
                };
                let Ok(stat) = std::fs::read_to_string(path.join("stat")) else {
                    continue;
                };
                let Some(after) = stat.rsplit(')').next() else {
                    continue;
                };
                let fields: Vec<&str> = after.split_whitespace().collect();
                let (Some(utime), Some(stime)) = (fields.get(11), fields.get(12)) else {
                    continue;
                };
                let seconds = (utime.parse::<f64>().unwrap_or(0.0)
                    + stime.parse::<f64>().unwrap_or(0.0))
                    / ticks;
                let name = thread_label(comm.trim());
                *by_name.entry(name).or_insert(0.0) += seconds;
            }
        }
    }
    by_name.into_iter().collect()
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
}
