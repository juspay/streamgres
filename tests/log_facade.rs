//! The log in facade mode, in a process of its own since the facade's
//! logger is global: every line reaches the host's logger under the
//! target `streamgres`, with its fields, filtered by the facade's level,
//! and no log thread is started.

use std::sync::Mutex;

use streamgres::log::{Level, enabled, event, log, to_facade};

/// What the test logger received, as `(level, target, text)`.
static SEEN: Mutex<Vec<(log::Level, String, String)>> = Mutex::new(Vec::new());

/// A logger keeping every record.
struct Keeper;

impl log::Log for Keeper {
    /// Everything the facade lets through.
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    /// Kept for the test.
    fn log(&self, record: &log::Record<'_>) {
        SEEN.lock().unwrap().push((
            record.level(),
            record.target().to_owned(),
            record.args().to_string(),
        ));
    }

    /// Nothing buffered.
    fn flush(&self) {}
}

/// Lines go to the host's logger, at its level, and the log thread
/// never starts.
#[test]
fn lines_reach_the_hosts_logger() {
    static KEEPER: Keeper = Keeper;
    log::set_logger(&KEEPER).unwrap();
    log::set_max_level(log::LevelFilter::Info);
    to_facade();
    assert!(enabled(Level::Info) && !enabled(Level::Debug), "the facade's level decides");
    log(Level::Warn, format_args!("slot {} gone", "s1"));
    event(Level::Info, "transaction routed".to_owned(), vec![("writes", "3".to_owned())]);
    log(Level::Debug, format_args!("not written"));
    let seen = SEEN.lock().unwrap().clone();
    assert_eq!(
        seen,
        vec![
            (log::Level::Warn, "streamgres".to_owned(), "slot s1 gone".to_owned()),
            (log::Level::Info, "streamgres".to_owned(), "transaction routed writes=3".to_owned()),
        ]
    );
    let threads = std::fs::read_dir("/proc/self/task")
        .map(|tasks| {
            tasks
                .filter_map(Result::ok)
                .filter_map(|task| std::fs::read_to_string(task.path().join("comm")).ok())
                .any(|name| name.trim() == "streamgres-log")
        })
        .unwrap_or(false);
    assert!(!threads, "no log thread in facade mode");
}
