//! How long a commit takes to reach the cached answer, two ways.
//!
//! - Another service's write: a ticket's title is set on a plain
//!   connection, and the reader polls `load_cached` until it shows it.
//! - The application's own write: the title is set on the reader's own
//!   connection, then `wait_for_writes` waits for the cache to apply it,
//!   and the next `load_cached` must show it on the first read.
//!
//! Both run under the background writer, so the feed is busy.

use std::time::{Duration, Instant};

use diesel::prelude::*;
use streamgres_diesel::{LoadCached, wait_for_writes};

use crate::models::Ticket;
use crate::schema::tickets;
use crate::stack::Stack;

/// The latencies of one way, and its failures.
#[derive(Debug, Default)]
pub struct Report {
    pub samples: Vec<Duration>,
    /// Reads the cache refused, served from Postgres instead.
    pub refused: usize,
    /// Read-your-writes reads that did not show the write.
    pub stale: usize,
    /// `wait_for_writes` calls that timed out.
    pub timeouts: usize,
}

impl Report {
    /// The latency at quantile `q`.
    pub fn at(&self, q: f64) -> Duration {
        let mut sorted = self.samples.clone();
        sorted.sort();
        let index = ((sorted.len().saturating_sub(1)) as f64 * q) as usize;
        sorted.get(index).copied().unwrap_or_default()
    }
}

/// The ticket the measurements write to: one the writer leaves alone.
const TICKET: i64 = 2;

fn title(conn: &mut PgConnection, stack: &Stack) -> QueryResult<Option<String>> {
    let rows: Option<Vec<Ticket>> = tickets::table
        .filter(tickets::id.eq(TICKET))
        .select(Ticket::as_select())
        .try_load_cached(&stack.cache, conn)?;
    Ok(rows.map(|rows| {
        rows.into_iter()
            .next()
            .map(|row| row.title)
            .unwrap_or_default()
    }))
}

fn set_title(conn: &mut PgConnection, value: &str) -> QueryResult<usize> {
    diesel::update(tickets::table.filter(tickets::id.eq(TICKET)))
        .set(tickets::title.eq(value))
        .execute(conn)
}

/// Another service's commits, `samples` of them.
pub fn from_elsewhere(stack: &Stack, samples: usize) -> Result<Report, String> {
    let mut writer = stack.connect()?;
    let mut reader = stack.connect()?;
    let mut report = Report::default();
    title(&mut reader, stack).map_err(|error| error.to_string())?;
    for n in 0..samples {
        let marker = format!("lag elsewhere {n}");
        set_title(&mut writer, &marker).map_err(|error| error.to_string())?;
        let committed = Instant::now();
        loop {
            match title(&mut reader, stack).map_err(|error| error.to_string())? {
                Some(seen) if seen == marker => {
                    report.samples.push(committed.elapsed());
                    break;
                }
                Some(_) => {}
                None => report.refused += 1,
            }
            if committed.elapsed() > Duration::from_secs(5) {
                return Err(format!("`{marker}` never reached the cache"));
            }
            std::thread::yield_now();
        }
    }
    Ok(report)
}

/// The reader's own commits, `samples` of them.
pub fn own_writes(stack: &Stack, samples: usize) -> Result<Report, String> {
    let mut conn = stack.connect()?;
    let mut report = Report::default();
    title(&mut conn, stack).map_err(|error| error.to_string())?;
    for n in 0..samples {
        let marker = format!("lag own {n}");
        set_title(&mut conn, &marker).map_err(|error| error.to_string())?;
        let committed = Instant::now();
        let caught_up = wait_for_writes(&stack.cache, &mut conn, Duration::from_secs(5))
            .map_err(|error| error.to_string())?;
        if !caught_up {
            report.timeouts += 1;
            continue;
        }
        match title(&mut conn, stack).map_err(|error| error.to_string())? {
            Some(seen) if seen == marker => report.samples.push(committed.elapsed()),
            Some(_) => report.stale += 1,
            None => report.refused += 1,
        }
    }
    Ok(report)
}
