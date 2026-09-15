//! A log with four levels on stderr, the level set once from the
//! configuration; `gw_error!`, `gw_warn!`, `gw_info!` and `gw_debug!`
//! format like `println!`.

use std::sync::atomic::{AtomicU8, Ordering};

use super::config::Level;

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
    eprintln!("{now} {tag} {message}");
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

macro_rules! gw_error {
    ($($arg:tt)*) => {
        $crate::gateway::log::log($crate::gateway::config::Level::Error, format_args!($($arg)*))
    };
}
macro_rules! gw_warn {
    ($($arg:tt)*) => {
        $crate::gateway::log::log($crate::gateway::config::Level::Warn, format_args!($($arg)*))
    };
}
macro_rules! gw_info {
    ($($arg:tt)*) => {
        $crate::gateway::log::log($crate::gateway::config::Level::Info, format_args!($($arg)*))
    };
}
macro_rules! gw_debug {
    ($($arg:tt)*) => {
        $crate::gateway::log::log($crate::gateway::config::Level::Debug, format_args!($($arg)*))
    };
}
pub(crate) use {gw_debug, gw_error, gw_info, gw_warn};
