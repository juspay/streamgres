//! A log with four levels on stderr, the level set once from the
//! configuration; `log_error!`, `log_warn!`, `log_info!` and `log_debug!`
//! format like `println!`. A line is formatted in full and written with one
//! call, so a chatty level costs one syscall per line rather than one per
//! fragment (at `debug`, the engine thread logs every poke).

use std::io::Write;
use std::sync::atomic::{AtomicU8, Ordering};

/// A log level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Error,
    Warn,
    Info,
    Debug,
}

static LEVEL: AtomicU8 = AtomicU8::new(2);

/// Set the level below which lines are written.
pub fn set_level(level: Level) {
    LEVEL.store(rank(level), Ordering::Relaxed);
}

/// Whether `level` is written.
pub fn enabled(level: Level) -> bool {
    rank(level) <= LEVEL.load(Ordering::Relaxed)
}

/// Write one line at `level`.
pub fn log(level: Level, message: std::fmt::Arguments<'_>) {
    if !enabled(level) {
        return;
    }
    let now = chrono::Utc::now().format("%H:%M:%S%.3f");
    let tag = match level {
        Level::Error => "ERROR",
        Level::Warn => "WARN ",
        Level::Info => "INFO ",
        Level::Debug => "DEBUG",
    };
    let line = format!("{now} {tag} {message}\n");
    let _ = std::io::stderr().lock().write_all(line.as_bytes());
}

/// The numeric rank of a level, higher is chattier.
fn rank(level: Level) -> u8 {
    match level {
        Level::Error => 0,
        Level::Warn => 1,
        Level::Info => 2,
        Level::Debug => 3,
    }
}

macro_rules! log_error {
    ($($arg:tt)*) => {
        $crate::log::log($crate::log::Level::Error, format_args!($($arg)*))
    };
}
macro_rules! log_warn {
    ($($arg:tt)*) => {
        $crate::log::log($crate::log::Level::Warn, format_args!($($arg)*))
    };
}
macro_rules! log_info {
    ($($arg:tt)*) => {
        $crate::log::log($crate::log::Level::Info, format_args!($($arg)*))
    };
}
macro_rules! log_debug {
    ($($arg:tt)*) => {
        $crate::log::log($crate::log::Level::Debug, format_args!($($arg)*))
    };
}
pub(crate) use {log_debug, log_error, log_info, log_warn};
