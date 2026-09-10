//! Benchmark binary: measures the IVM engine with `std::time` alone — no
//! extra crates, and a fixed-seed xorshift PRNG so every run is
//! reproducible.
//!
//! ```bash
//! cargo run --release --bin bench
//! ```
//!
//! Four scenarios, each printed as a compact table:
//!
//! 1. **Routing scale** — `tickets` subscriptions drawn from a small
//!    filter vocabulary (so conditions are shared across subscriptions) at
//!    N = 100 / 1_000 / 10_000; 20_000 inserts and 20_000 updates routed
//!    through each. The claim under test: per-write routing cost tracks
//!    the number of *distinct* indexed conditions (flat in N), while
//!    delivery cost tracks the subscriptions actually impacted. Updates
//!    are routed twice — once through `search_impacted_queries` (routing
//!    only, no state change) and once through `incremental_update` (routing
//!    plus delivery) — to separate the two costs.
//! 2. **Registration** — cost per `register_query` at each N (measured in
//!    scenario 1's registration loop), and twin sharing: 1_000 identical
//!    registrations over existing data, served from the shared frame
//!    instead of storage.
//! 3. **Window** — one `ORDER BY points ASC LIMIT 50` subscription over
//!    100_000 storage rows under uniform writes (the published boundary
//!    rejects nearly everything) and window-targeted writes (every insert
//!    aims below the boundary, every delete hits a held row, so
//!    admissions, evictions, and refills all happen).
//! 4. **LEFT JOIN** — `tickets LEFT JOIN users ON assigned_to = users.id`
//!    with 1_000 identical subscriptions plus 100 distinct ones over 1_000
//!    users: ticket inserts, ticket reassignments, and user updates.
//!
//! Storage is a bench-local [`Storage`] implementation with hashed row
//! lookup: the crate's `MemoryStorage` finds rows by linear scan on every
//! `apply`, which would make loading 100_000 rows quadratic. Its `select`
//! scans and evaluates the same predicates, with one shortcut a real
//! database has too — a primary-key `IN` / `=` conjunct narrows the scan
//! to those keys. Every write is mirrored into storage *before* it is
//! routed, and only the engine call is timed.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::{Duration, Instant};

use jus_sync::ivm::{
    evaluate, IvmStats, MultiTableIVM, PgStorage, SingleTableIVM,
    Storage,
};
use jus_sync::model::ComparisonOperator::{EQ, GTE};
use jus_sync::model::*;

const STATUSES: [&str; 10] = [
    "NEW",
    "OPEN",
    "TODO",
    "IN_PROGRESS",
    "REVIEW",
    "BLOCKED",
    "DONE",
    "CLOSED",
    "ARCHIVED",
    "WONTFIX",
];
const PRIORITIES: [&str; 5] = ["LOWEST", "LOW", "MEDIUM", "HIGH", "URGENT"];
const TEAMS: u64 = 50;
const USERS: u64 = 1_000;
const POINTS_RANGE: u64 = 1_000;
const THRESHOLD_BASE: i64 = 900;
const THRESHOLD_STEP: i64 = 5;
const THRESHOLDS: u64 = 20;
const SEED: u64 = 0x9E37_79B9_7F4A_7C15;

const ROUTING_SIZES: [usize; 3] = [100, 1_000, 10_000];
const ROUTING_INSERTS: usize = 20_000;
const ROUTING_UPDATES: usize = 20_000;

const TWIN_ROWS: usize = 20_000;
const TWIN_COPIES: usize = 1_000;
const TWIN_STORAGE_SAMPLES: usize = 10;

const WINDOW_ROWS: usize = 100_000;
const WINDOW_LIMIT: u32 = 50;
const WINDOW_WRITES: usize = 10_000;
const WINDOW_POINTS_RANGE: u64 = 1_000_000;

const JOIN_TWINS: usize = 1_000;
const JOIN_DISTINCT: usize = 100;
const JOIN_INITIAL_TICKETS: usize = 2_000;
const JOIN_TICKET_INSERTS: usize = 10_000;
const JOIN_REASSIGNMENTS: usize = 5_000;
const JOIN_USER_UPDATES: usize = 5_000;

/// Xorshift64: a tiny deterministic PRNG so runs are reproducible without
/// a `rand` dependency.
struct XorShift64(u64);

impl XorShift64 {
    /// A generator seeded with `seed` (zero is nudged: it is xorshift's
    /// absorbing state).
    fn new(seed: u64) -> Self {
        XorShift64(seed.max(1))
    }

    /// The next 64 random bits.
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Uniform in `0..n` (`n > 0`); the modulo bias is negligible at these
    /// ranges.
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// A uniform index into a slice of `len` elements.
    fn index(&mut self, len: usize) -> usize {
        self.below(len as u64) as usize
    }
}

/// The `tickets(id, status, priority, assigned_to, points, team)` table.
fn tickets_table() -> DbTable {
    DbTable::new(
        "tickets",
        ["id"],
        vec![
            DbColumn::new("id", ValueType::Int),
            DbColumn::new("status", ValueType::String),
            DbColumn::new("priority", ValueType::String),
            DbColumn::new("assigned_to", ValueType::String),
            DbColumn::new("points", ValueType::Int),
            DbColumn::new("team", ValueType::Int),
        ],
    )
}

/// The `users(id, name, team)` table; `id` is the string `assigned_to`
/// references.
fn users_table() -> DbTable {
    DbTable::new(
        "users",
        ["id"],
        vec![
            DbColumn::new("id", ValueType::String),
            DbColumn::new("name", ValueType::String),
            DbColumn::new("team", ValueType::Int),
        ],
    )
}

/// The string id of user number `n`.
fn user_id(n: u64) -> String {
    format!("u{n}")
}

/// A single-column primary key `{"id": value}`.
fn key(id: impl Into<Value>) -> DataFrameKey {
    DataFrameKey::new(HashMap::from([("id".to_owned(), id.into())]))
}

/// A DELETE of one row of `table`.
fn delete(table: &DbTable, key: DataFrameKey) -> WriteQuery {
    WriteQuery::DELETE(DeleteQuery {
        table: table.name.clone(),
        pkey_value: key,
    })
}

/// A subscription query on `table` with the given filter, pkey-ordered,
/// unbounded.
fn unbounded(table: &DbTable, filter: Where) -> SingleTableReadQuery {
    SingleTableReadQuery::new(
        table.name.clone(),
        filter,
        OrderBy::new("id", Order::ASC),
        u32::MAX,
    )
}

/// A `tickets` row in the bench's compact form; `row()` renders the full
/// image the engine requires (every column, pkey included).
#[derive(Clone)]
struct Ticket {
    id: i64,
    status: usize,
    priority: usize,
    assigned_to: u64,
    points: i64,
    team: i64,
}

impl Ticket {
    /// A uniformly random ticket with the given id.
    fn random(rng: &mut XorShift64, id: i64) -> Self {
        Ticket {
            id,
            status: rng.index(STATUSES.len()),
            priority: rng.index(PRIORITIES.len()),
            assigned_to: rng.below(USERS),
            points: rng.below(POINTS_RANGE) as i64,
            team: rng.below(TEAMS) as i64,
        }
    }

    /// The full row image.
    fn row(&self) -> DataFrameRow {
        DataFrameRow {
            data: HashMap::from([
                ("id".to_owned(), Value::Int(self.id)),
                ("status".to_owned(), Value::from(STATUSES[self.status])),
                ("priority".to_owned(), Value::from(PRIORITIES[self.priority])),
                ("assigned_to".to_owned(), Value::from(user_id(self.assigned_to))),
                ("points".to_owned(), Value::Int(self.points)),
                ("team".to_owned(), Value::Int(self.team)),
            ]),
        }
    }

    /// An INSERT of this ticket.
    fn insert(&self) -> WriteQuery {
        WriteQuery::INSERT(InsertQuery {
            table: tickets_table().name,
            pkey_value: key(self.id),
            record: self.row(),
        })
    }

    /// An UPDATE carrying this ticket's full current image.
    fn update(&self) -> WriteQuery {
        WriteQuery::UPDATE(UpdateQuery {
            table: tickets_table().name,
            pkey_value: key(self.id),
            record: self.row(),
        })
    }
}

/// The full image of user `n` at name version `version`.
fn user_row(n: u64, version: u64) -> DataFrameRow {
    DataFrameRow {
        data: HashMap::from([
            ("id".to_owned(), Value::from(user_id(n))),
            ("name".to_owned(), Value::from(format!("user {n} v{version}"))),
            ("team".to_owned(), Value::Int((n % TEAMS) as i64)),
        ]),
    }
}

/// An INSERT of user `n`.
fn user_insert(n: u64, version: u64) -> WriteQuery {
    WriteQuery::INSERT(InsertQuery {
        table: users_table().name,
        pkey_value: key(user_id(n)),
        record: user_row(n, version),
    })
}

/// An UPDATE renaming user `n` to name version `version`.
fn user_update(n: u64, version: u64) -> WriteQuery {
    WriteQuery::UPDATE(UpdateQuery {
        table: users_table().name,
        pkey_value: key(user_id(n)),
        record: user_row(n, version),
    })
}

/// `status = <one of 10>`.
fn status_atom(rng: &mut XorShift64) -> Where {
    Where::condition("status", EQ, STATUSES[rng.index(STATUSES.len())])
}

/// `priority = <one of 5>`.
fn priority_atom(rng: &mut XorShift64) -> Where {
    Where::condition("priority", EQ, PRIORITIES[rng.index(PRIORITIES.len())])
}

/// `team = <one of 50>`.
fn team_atom(rng: &mut XorShift64) -> Where {
    Where::condition("team", EQ, rng.below(TEAMS) as i64)
}

/// `points >= <one of 20 thresholds in 900..=995>` — a "big tickets"
/// filter matching 0.5–10% of rows.
fn points_atom(rng: &mut XorShift64) -> Where {
    let threshold = THRESHOLD_BASE + THRESHOLD_STEP * rng.below(THRESHOLDS) as i64;
    Where::condition("points", GTE, threshold)
}

/// One subscription filter from the shared vocabulary. The mix leans
/// toward the narrow shapes real subscriptions have (one team's tickets
/// in one status), with a few broad ones, so that the average filter
/// matches about 1% of random rows — 85 distinct atoms in total no matter
/// how many subscriptions are drawn.
fn random_filter(rng: &mut XorShift64) -> Where {
    match rng.below(100) {
        0..=44 => Where::AND(vec![team_atom(rng), status_atom(rng)]),
        45..=64 => Where::AND(vec![team_atom(rng), priority_atom(rng)]),
        65..=74 => team_atom(rng),
        75..=81 => Where::AND(vec![status_atom(rng), priority_atom(rng)]),
        82..=91 => Where::AND(vec![status_atom(rng), points_atom(rng)]),
        92..=94 => points_atom(rng),
        _ => Where::OR(vec![team_atom(rng), team_atom(rng)]),
    }
}

/// The distinct main filters of the join scenario: 50 `team = T`, then the
/// 50 `status = S AND priority = P` combinations.
fn distinct_join_filter(index: usize) -> Where {
    if index < TEAMS as usize {
        Where::condition("team", EQ, index as i64)
    } else {
        let combination = index - TEAMS as usize;
        Where::AND(vec![
            Where::condition("status", EQ, STATUSES[combination % STATUSES.len()]),
            Where::condition("priority", EQ, PRIORITIES[combination / STATUSES.len()]),
        ])
    }
}

/// Distinct atoms and DNF disjuncts seen across a registration batch —
/// the generator-side count of what the table index has to hold.
#[derive(Default)]
struct Vocabulary {
    conditions: HashSet<Condition>,
    disjuncts: HashSet<Disjunct>,
}

impl Vocabulary {
    /// Record one filter's DNF.
    fn note(&mut self, filter: &Where) {
        for disjunct in filter.to_dnf() {
            for condition in &disjunct.conditions {
                self.conditions.insert(condition.clone());
            }
            self.disjuncts.insert(disjunct);
        }
    }
}

/// One table of the bench storage: rows in insertion order plus a
/// key → position index, so mirroring a write is O(1).
#[derive(Default)]
struct BenchTable {
    rows: Vec<(DataFrameKey, DataFrameRow)>,
    positions: HashMap<DataFrameKey, usize>,
}

/// The bench storage (see the module header).
#[derive(Default)]
struct BenchStorage {
    tables: RefCell<HashMap<TableName, BenchTable>>,
}

impl BenchStorage {
    /// Mirror one write: insert/update upsert by primary key, delete
    /// removes (swap-remove, index patched for the moved row).
    fn apply(&self, write: &WriteQuery) {
        let mut tables = self.tables.borrow_mut();
        let table = tables.entry(write.table().clone()).or_default();
        let key = write.pkey_value();
        match (write.new_row_image(), table.positions.get(key).copied()) {
            (Some(image), Some(position)) => table.rows[position].1 = image.clone(),
            (Some(image), None) => {
                table.positions.insert(key.clone(), table.rows.len());
                table.rows.push((key.clone(), image.clone()));
            }
            (None, Some(position)) => {
                table.positions.remove(key);
                table.rows.swap_remove(position);
                if position < table.rows.len() {
                    let moved = table.rows[position].0.clone();
                    table.positions.insert(moved, position);
                }
            }
            (None, None) => {}
        }
    }
}

/// Every primary-key `IN` / `=` conjunct of the filter — conditions on
/// `id` reachable through `AND`s only, so any one of them bounds the
/// result set.
fn pkey_restrictions(filter: &Where, out: &mut Vec<Vec<Value>>) {
    match filter {
        Where::Condition(condition) if condition.column == "id" => {
            match (&condition.comparison_operator, &condition.value) {
                (ComparisonOperator::IN, Value::List(values)) => out.push(values.clone()),
                (ComparisonOperator::IN, Value::Set(set)) => out.push(set.members()),
                (ComparisonOperator::EQ, value) => out.push(vec![value.clone()]),
                _ => {}
            }
        }
        Where::Condition(_) | Where::OR(_) => {}
        Where::AND(children) => {
            for child in children {
                pkey_restrictions(child, out);
            }
        }
    }
}

/// The `order_by` comparison of two rows for a limited query.
fn order_of(
    query: &SingleTableReadQuery,
    a: &(DataFrameKey, DataFrameRow),
    b: &(DataFrameKey, DataFrameRow),
) -> Ordering {
    let column = query.order_by.column.as_str();
    let ordering = match (a.1.data.get(column), b.1.data.get(column)) {
        (Some(x), Some(y)) => x.compare(y).unwrap_or(Ordering::Equal),
        _ => Ordering::Equal,
    };
    match query.order_by.direction {
        Order::ASC => ordering,
        Order::DESC => ordering.reverse(),
    }
}

impl Storage for BenchStorage {
    /// Scan the table (narrowed to the smallest primary-key conjunct when
    /// the filter has one), evaluate the filter on every candidate, and
    /// for a finite limit keep the best rows in `order_by` order.
    fn select(&self, query: &SingleTableReadQuery) -> Vec<(DataFrameKey, DataFrameRow)> {
        let tables = self.tables.borrow();
        let Some(table) = tables.get(&query.table) else {
            return Vec::new();
        };
        let mut restrictions = Vec::new();
        pkey_restrictions(&query.filter, &mut restrictions);
        let candidates: Vec<&(DataFrameKey, DataFrameRow)> =
            match restrictions.iter().min_by_key(|values| values.len()) {
                Some(values) => values
                    .iter()
                    .filter_map(|value| table.positions.get(&key(value.clone())))
                    .map(|&position| &table.rows[position])
                    .collect(),
                None => table.rows.iter().collect(),
            };
        let mut selected: Vec<(DataFrameKey, DataFrameRow)> = candidates
            .into_iter()
            .filter(|(_, row)| evaluate(&query.filter, &row.data, &mut 0))
            .cloned()
            .collect();
        if query.limit != u32::MAX {
            let limit = query.limit as usize;
            if selected.len() > limit {
                selected.select_nth_unstable_by(limit, |a, b| order_of(query, a, b));
                selected.truncate(limit);
            }
            selected.sort_by(|a, b| order_of(query, a, b));
        }
        selected
    }
}

/// The measured cost of one batch of writes: wall time of the engine
/// calls alone, the counter movement, and how many operations (or, for a
/// routing-only pass, impacted subscriptions) came back.
struct Run {
    label: String,
    writes: u64,
    elapsed: Duration,
    stats: IvmStats,
    returned: u64,
}

impl Run {
    /// Mean microseconds per write.
    fn micros_per_write(&self) -> f64 {
        self.elapsed.as_secs_f64() * 1e6 / self.writes as f64
    }

    /// Writes per second.
    fn writes_per_second(&self) -> f64 {
        self.writes as f64 / self.elapsed.as_secs_f64()
    }

    /// A counter averaged per write.
    fn per_write(&self, counter: u64) -> f64 {
        counter as f64 / self.writes as f64
    }
}

/// Route every write through `incremental_update`, timing the engine call
/// alone.
fn route_all(ivm: &mut SingleTableIVM, label: &str, writes: &[WriteQuery]) -> Run {
    let before = ivm.stats().clone();
    let mut returned = 0u64;
    let started = Instant::now();
    for write in writes {
        returned += ivm.incremental_update(write).len() as u64;
    }
    Run {
        label: label.to_owned(),
        writes: writes.len() as u64,
        elapsed: started.elapsed(),
        stats: ivm.stats().diff(&before),
        returned,
    }
}

/// Route every write through `search_impacted_queries` — routing only, no
/// frame or window change — timing the engine call alone.
fn search_all(ivm: &mut SingleTableIVM, label: &str, writes: &[WriteQuery]) -> Run {
    let before = ivm.stats().clone();
    let mut returned = 0u64;
    let started = Instant::now();
    for write in writes {
        returned += ivm.search_impacted_queries(write).len() as u64;
    }
    Run {
        label: label.to_owned(),
        writes: writes.len() as u64,
        elapsed: started.elapsed(),
        stats: ivm.stats().diff(&before),
        returned,
    }
}

/// Print `rows` under `header` as a right-aligned table.
fn print_table(header: &[&str], rows: &[Vec<String>]) {
    let widths: Vec<usize> = header
        .iter()
        .enumerate()
        .map(|(column, title)| {
            rows.iter()
                .map(|row| row[column].len())
                .chain([title.len()])
                .max()
                .unwrap_or(0)
        })
        .collect();
    let render = |cells: &[String]| {
        cells
            .iter()
            .zip(&widths)
            .map(|(cell, &width)| format!("{cell:>width$}"))
            .collect::<Vec<_>>()
            .join("  ")
    };
    let titles: Vec<String> = header.iter().map(|title| (*title).to_owned()).collect();
    println!("{}", render(&titles));
    println!(
        "{}",
        widths
            .iter()
            .map(|&width| "-".repeat(width))
            .collect::<Vec<_>>()
            .join("  ")
    );
    for row in rows {
        println!("{}", render(row));
    }
}

/// `x` with one decimal.
fn one(x: f64) -> String {
    format!("{x:.1}")
}

/// `x` with two decimals.
fn two(x: f64) -> String {
    format!("{x:.2}")
}

/// `x` rounded to an integer with thousands separators.
fn whole(x: f64) -> String {
    let digits = format!("{}", x.round() as i64);
    let mut out = String::new();
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            out.push('_');
        }
        out.push(ch);
    }
    out
}

/// Scenario 1 at one subscription count, plus scenario 2's per-N
/// registration cost.
struct RoutingReport {
    n: usize,
    vocabulary: Vocabulary,
    registration: Duration,
    registered: IvmStats,
    runs: Vec<Run>,
}

/// Register `n` vocabulary subscriptions on an empty engine, then route
/// the insert and update streams — the same streams for every `n`.
fn routing_scale(n: usize) -> RoutingReport {
    let mut subscription_rng = XorShift64::new(SEED);
    let mut data_rng = XorShift64::new(SEED ^ 1);
    let tickets_table = tickets_table();
    let mut ivm = SingleTableIVM::new(Rc::new(PgStorage));
    let filters: Vec<Where> = (0..n)
        .map(|_| random_filter(&mut subscription_rng))
        .collect();
    let mut vocabulary = Vocabulary::default();
    for filter in &filters {
        vocabulary.note(filter);
    }
    let started = Instant::now();
    for (index, filter) in filters.into_iter().enumerate() {
        ivm.register_query(format!("s{index}"), unbounded(&tickets_table, filter), None);
    }
    let registration = started.elapsed();
    let registered = ivm.stats().clone();

    let mut tickets: Vec<Ticket> = (0..ROUTING_INSERTS)
        .map(|id| Ticket::random(&mut data_rng, id as i64))
        .collect();
    let inserts: Vec<WriteQuery> = tickets.iter().map(Ticket::insert).collect();
    let insert_run = route_all(&mut ivm, "insert", &inserts);

    let updates: Vec<WriteQuery> = (0..ROUTING_UPDATES)
        .map(|_| {
            let ticket = &mut tickets[data_rng.index(ROUTING_INSERTS)];
            ticket.status = data_rng.index(STATUSES.len());
            ticket.points = data_rng.below(POINTS_RANGE) as i64;
            ticket.update()
        })
        .collect();
    let route_only = search_all(&mut ivm, "update (route only)", &updates);
    let update_run = route_all(&mut ivm, "update", &updates);

    RoutingReport {
        n,
        vocabulary,
        registration,
        registered,
        runs: vec![insert_run, route_only, update_run],
    }
}

/// Scenarios 1 and 2a: run [`routing_scale`] at every N and print the
/// registration and routing tables.
fn routing_and_registration() {
    println!("\n== 1. single-table routing scale ({ROUTING_INSERTS} inserts + {ROUTING_UPDATES} updates per N; storage: PgStorage stub) ==");
    let reports: Vec<RoutingReport> = ROUTING_SIZES.into_iter().map(routing_scale).collect();

    println!("\nregistration (scenario 2a):");
    let registration_rows: Vec<Vec<String>> = reports
        .iter()
        .map(|report| {
            vec![
                report.n.to_string(),
                report.vocabulary.conditions.len().to_string(),
                report.vocabulary.disjuncts.len().to_string(),
                report.registered.disjuncts_registered.to_string(),
                report.registered.conditions_indexed.to_string(),
                report.registered.snapshots_shared.to_string(),
                one(report.registration.as_secs_f64() * 1e3),
                one(report.registration.as_secs_f64() * 1e6 / report.n as f64),
            ]
        })
        .collect();
    print_table(
        &[
            "N subs",
            "distinct conds",
            "distinct disjuncts",
            "disjuncts_registered",
            "conditions_indexed",
            "snapshots_shared",
            "total ms",
            "us/register",
        ],
        &registration_rows,
    );

    println!("\nrouting (per-write averages from IvmStats::diff):");
    let routing_rows: Vec<Vec<String>> = reports
        .iter()
        .flat_map(|report| {
            report.runs.iter().map(|run| {
                vec![
                    report.n.to_string(),
                    run.label.clone(),
                    run.writes.to_string(),
                    one(run.micros_per_write()),
                    whole(run.writes_per_second()),
                    one(run.per_write(run.stats.columns_probed)),
                one(run.per_write(run.stats.conditions_evaluated)),
                    one(run.per_write(run.stats.disjunct_increments)),
                    one(run.per_write(run.stats.disjuncts_fired)),
                    one(run.per_write(run.stats.queries_impacted)),
                    one(run.per_write(run.stats.ops_add)),
                    one(run.per_write(run.stats.ops_delete)),
                ]
            })
        })
        .collect();
    print_table(
        &[
            "N subs",
            "phase",
            "writes",
            "us/write",
            "writes/s",
            "cols",
            "cond_match",
            "disj_incr",
            "disj_fired",
            "impacted",
            "ops_add",
            "ops_del",
        ],
        &routing_rows,
    );
}

/// Scenario 2b: the storage path (fresh engine, first registration of a
/// query over existing rows) against the twin path (identical
/// registrations served from the shared frame).
fn twin_sharing() {
    println!("\n== 2b. twin sharing ({TWIN_ROWS} tickets in storage; `team = 7` registered once, then {TWIN_COPIES} identical copies) ==");
    let mut rng = XorShift64::new(SEED ^ 2);
    let tickets_table = tickets_table();
    let storage = Rc::new(BenchStorage::default());
    for id in 0..TWIN_ROWS {
        storage.apply(&Ticket::random(&mut rng, id as i64).insert());
    }
    let query = unbounded(&tickets_table, Where::condition("team", EQ, 7));

    let mut storage_path = Duration::ZERO;
    let mut snapshot_rows = 0;
    for _ in 0..TWIN_STORAGE_SAMPLES {
        let mut fresh = SingleTableIVM::new(storage.clone() as Rc<dyn Storage>);
        let started = Instant::now();
        snapshot_rows = fresh.register_query("first", query.clone(), None).len();
        storage_path += started.elapsed();
    }

    let mut ivm = SingleTableIVM::new(storage.clone() as Rc<dyn Storage>);
    ivm.register_query("first", query.clone(), None);
    let started = Instant::now();
    for index in 0..TWIN_COPIES {
        ivm.register_query(format!("twin{index}"), query.clone(), None);
    }
    let twin_path = started.elapsed();

    print_table(
        &[
            "snapshot rows",
            "storage-path us/register",
            "twin-path us/register",
            "twins registered",
            "snapshots_shared",
            "held rows in frame",
        ],
        &[vec![
            snapshot_rows.to_string(),
            one(storage_path.as_secs_f64() * 1e6 / TWIN_STORAGE_SAMPLES as f64),
            one(twin_path.as_secs_f64() * 1e6 / TWIN_COPIES as f64),
            TWIN_COPIES.to_string(),
            ivm.stats().snapshots_shared.to_string(),
            ivm.rows_for("first").map_or(0, |rows| rows.len()).to_string(),
        ]],
    );
}

/// The bench's client-side view of the windowed subscription, rebuilt
/// from the operation stream exactly as a real client would — used to aim
/// the targeted workload at rows the window currently holds.
#[derive(Default)]
struct Mirror {
    held: Vec<(i64, i64)>,
}

impl Mirror {
    /// The integer `id` of a key.
    fn id_of(key: &DataFrameKey) -> i64 {
        match key.pkey_value.get("id") {
            Some(Value::Int(id)) => *id,
            _ => -1,
        }
    }

    /// The `points` of a row image.
    fn points_of(row: &DataFrameRow) -> i64 {
        match row.data.get("points") {
            Some(Value::Int(points)) => *points,
            _ => 0,
        }
    }

    /// Apply operations in emitted order; returns `(adds, deletes)`.
    fn apply(&mut self, ops: &[DataFrameOperation]) -> (u64, u64) {
        let mut adds = 0;
        let mut deletes = 0;
        for op in ops {
            match op {
                DataFrameOperation::Add(key, row) => {
                    adds += 1;
                    let id = Self::id_of(key);
                    let points = Self::points_of(row);
                    match self.held.iter_mut().find(|(held, _)| *held == id) {
                        Some(entry) => entry.1 = points,
                        None => self.held.push((id, points)),
                    }
                }
                DataFrameOperation::Delete(key, _) => {
                    deletes += 1;
                    let id = Self::id_of(key);
                    self.held.retain(|(held, _)| *held != id);
                }
            }
        }
        (adds, deletes)
    }

    /// The worst (largest) held `points` — the admission boundary.
    fn worst_points(&self) -> i64 {
        self.held.iter().map(|(_, points)| *points).max().unwrap_or(0)
    }

    /// A uniformly chosen held id, if any row is held.
    fn random_id(&self, rng: &mut XorShift64) -> Option<i64> {
        if self.held.is_empty() {
            None
        } else {
            Some(self.held[rng.index(self.held.len())].0)
        }
    }
}

/// Scenario 3 state: the engine, storage, the client mirror, and the ids
/// alive in storage.
struct WindowBench {
    ivm: SingleTableIVM,
    storage: Rc<BenchStorage>,
    rng: XorShift64,
    mirror: Mirror,
    alive: Vec<i64>,
    next_id: i64,
    targeted_range: u64,
}

impl WindowBench {
    /// One workload of [`WINDOW_WRITES`] writes, a coin flip between insert
    /// and delete each. Uniform: random points, random victim. Targeted:
    /// points drawn from twice the initial boundary (so about half are
    /// admitted at first), victims drawn from the rows the window holds.
    fn run(&mut self, label: &str, targeted: bool) -> (Run, u64, u64) {
        let tickets_table = tickets_table();
        let before = self.ivm.stats().clone();
        let mut elapsed = Duration::ZERO;
        let mut adds = 0;
        let mut deletes = 0;
        let mut returned = 0;
        for _ in 0..WINDOW_WRITES {
            let victim = if targeted {
                self.mirror.random_id(&mut self.rng)
            } else {
                Some(self.alive[self.rng.index(self.alive.len())])
            };
            let write = match victim.filter(|_| self.rng.below(2) == 1) {
                Some(id) => {
                    if let Some(position) = self.alive.iter().position(|&alive| alive == id) {
                        self.alive.swap_remove(position);
                    }
                    delete(&tickets_table, key(id))
                }
                None => {
                    let mut ticket = Ticket::random(&mut self.rng, self.next_id);
                    ticket.points = if targeted {
                        self.rng.below(self.targeted_range) as i64
                    } else {
                        self.rng.below(WINDOW_POINTS_RANGE) as i64
                    };
                    self.next_id += 1;
                    self.alive.push(ticket.id);
                    ticket.insert()
                }
            };
            self.storage.apply(&write);
            let started = Instant::now();
            let updates = self.ivm.incremental_update(&write);
            elapsed += started.elapsed();
            returned += updates.len() as u64;
            let ops: Vec<DataFrameOperation> = updates.into_iter().map(|update| update.op).collect();
            let (added, deleted) = self.mirror.apply(&ops);
            adds += added;
            deletes += deleted;
        }
        let run = Run {
            label: label.to_owned(),
            writes: WINDOW_WRITES as u64,
            elapsed,
            stats: self.ivm.stats().diff(&before),
            returned,
        };
        (run, adds, deletes)
    }
}

/// Scenario 3: one `ORDER BY points ASC LIMIT 50` subscription over
/// 100_000 rows, under uniform and window-targeted writes.
fn window() {
    println!("\n== 3. ORDER BY points ASC LIMIT {WINDOW_LIMIT} over {WINDOW_ROWS} storage rows ({WINDOW_WRITES} writes per workload, ~50/50 insert/delete) ==");
    let mut rng = XorShift64::new(SEED ^ 3);
    let tickets_table = tickets_table();
    let storage = Rc::new(BenchStorage::default());
    let mut alive = Vec::with_capacity(WINDOW_ROWS);
    for id in 0..WINDOW_ROWS {
        let mut ticket = Ticket::random(&mut rng, id as i64);
        ticket.points = rng.below(WINDOW_POINTS_RANGE) as i64;
        storage.apply(&ticket.insert());
        alive.push(ticket.id);
    }
    let mut ivm = SingleTableIVM::new(storage.clone() as Rc<dyn Storage>);
    let query = SingleTableReadQuery::new(
        tickets_table.name.clone(),
        Where::AND(vec![]),
        OrderBy::new("points", Order::ASC),
        WINDOW_LIMIT,
    );
    let started = Instant::now();
    let snapshot = ivm.register_query("w", query, None);
    let registration = started.elapsed();
    let mut mirror = Mirror::default();
    mirror.apply(&snapshot);
    let initial_boundary = mirror.worst_points();
    println!(
        "registration: {} snapshot rows (buffer = 2 x limit) in {} us; initial boundary points < {initial_boundary}",
        snapshot.len(),
        one(registration.as_secs_f64() * 1e6)
    );

    let mut bench = WindowBench {
        ivm,
        storage,
        rng,
        mirror,
        alive,
        next_id: WINDOW_ROWS as i64,
        targeted_range: (initial_boundary as u64) * 2 + 1,
    };
    let workloads = [
        bench.run("uniform", false),
        bench.run("targeted", true),
    ];
    let rows: Vec<Vec<String>> = workloads
        .iter()
        .map(|(run, adds, deletes)| {
            vec![
                run.label.clone(),
                run.writes.to_string(),
                two(run.micros_per_write()),
                whole(run.writes_per_second()),
                adds.to_string(),
                deletes.to_string(),
                run.stats.window_evictions.to_string(),
                run.stats.window_refills.to_string(),
                one(run.per_write(run.stats.columns_probed)),
                one(run.per_write(run.stats.conditions_evaluated)),
                one(run.per_write(run.stats.queries_impacted)),
                run.returned.to_string(),
            ]
        })
        .collect();
    print_table(
        &[
            "workload",
            "writes",
            "us/write",
            "writes/s",
            "client adds",
            "client deletes",
            "evictions",
            "refills",
            "cols",
            "cond_match",
            "impacted",
            "ops returned",
        ],
        &rows,
    );
    println!(
        "final: window holds {} rows, boundary points < {}",
        bench.mirror.held.len(),
        bench.mirror.worst_points()
    );
}

/// Scenario 4 state: the join layer, storage, and the bench's own copy of
/// the ticket and user rows it mutates.
struct JoinBench {
    ivm: MultiTableIVM,
    storage: Rc<BenchStorage>,
    rng: XorShift64,
    tickets: Vec<Ticket>,
    user_versions: Vec<u64>,
}

impl JoinBench {
    /// Route writes through the join layer, mirroring each into storage
    /// first and timing the engine call alone; returns the run plus the
    /// main-part / join-part operation split.
    fn route(&mut self, label: &str, writes: &[WriteQuery]) -> (Run, u64, u64) {
        let before = self.ivm.stats().clone();
        let mut main_ops = 0;
        let mut join_ops = 0;
        let mut elapsed = Duration::ZERO;
        for write in writes {
            self.storage.apply(write);
            let started = Instant::now();
            let updates = self.ivm.incremental_update(write);
            elapsed += started.elapsed();
            for update in &updates {
                if update.part.is_main() {
                    main_ops += 1;
                } else {
                    join_ops += 1;
                }
            }
        }
        let run = Run {
            label: label.to_owned(),
            writes: writes.len() as u64,
            elapsed,
            stats: self.ivm.stats().diff(&before),
            returned: main_ops + join_ops,
        };
        (run, main_ops, join_ops)
    }

    /// [`JOIN_TICKET_INSERTS`] fresh random tickets, assigned across all
    /// users.
    fn ticket_inserts(&mut self) -> (Run, u64, u64) {
        let mut writes = Vec::with_capacity(JOIN_TICKET_INSERTS);
        for _ in 0..JOIN_TICKET_INSERTS {
            let ticket = Ticket::random(&mut self.rng, self.tickets.len() as i64);
            writes.push(ticket.insert());
            self.tickets.push(ticket);
        }
        self.route("ticket insert", &writes)
    }

    /// [`JOIN_REASSIGNMENTS`] updates moving an existing ticket to another
    /// user, every other column unchanged.
    fn reassignments(&mut self) -> (Run, u64, u64) {
        let mut writes = Vec::with_capacity(JOIN_REASSIGNMENTS);
        for _ in 0..JOIN_REASSIGNMENTS {
            let index = self.rng.index(self.tickets.len());
            self.tickets[index].assigned_to = self.rng.below(USERS);
            writes.push(self.tickets[index].update());
        }
        self.route("ticket reassign", &writes)
    }

    /// [`JOIN_USER_UPDATES`] renames of random users — an in-place replace
    /// for every subscription holding the user.
    fn user_updates(&mut self) -> (Run, u64, u64) {
        let mut writes = Vec::with_capacity(JOIN_USER_UPDATES);
        for _ in 0..JOIN_USER_UPDATES {
            let user = self.rng.below(USERS);
            self.user_versions[user as usize] += 1;
            writes.push(user_update(user, self.user_versions[user as usize]));
        }
        self.route("user update", &writes)
    }
}

/// The join spec: `tickets WHERE <main> LEFT JOIN users ON assigned_to =
/// users.id`.
fn join_spec(main: Where) -> MultiTableReadQuery {
    MultiTableReadQuery {
        main_table: unbounded(&tickets_table(), main),
        left_joins: vec![Join::new(
            MultiTableReadQuery::single(unbounded(&users_table(), Where::AND(vec![]))),
            "assigned_to",
            "id",
        )],
        right_joins: Vec::new(),
    }
}

/// Scenario 4: the LEFT JOIN layer under ticket inserts, ticket
/// reassignments, and user updates.
fn left_join() {
    println!("\n== 4. tickets LEFT JOIN users ({JOIN_TWINS} identical `status = 'OPEN'` subscriptions + {JOIN_DISTINCT} distinct; {USERS} users, {JOIN_INITIAL_TICKETS} initial tickets) ==");
    let mut rng = XorShift64::new(SEED ^ 4);
    let storage = Rc::new(BenchStorage::default());
    let user_versions = vec![0u64; USERS as usize];
    for user in 0..USERS {
        storage.apply(&user_insert(user, 0));
    }
    let tickets: Vec<Ticket> = (0..JOIN_INITIAL_TICKETS)
        .map(|id| Ticket::random(&mut rng, id as i64))
        .collect();
    for ticket in &tickets {
        storage.apply(&ticket.insert());
    }
    let mut ivm = MultiTableIVM::new(storage.clone() as Rc<dyn Storage>);

    let started = Instant::now();
    let mut twin_ops = 0;
    for index in 0..JOIN_TWINS {
        twin_ops += ivm
            .register_query(
                format!("j{index}"),
                join_spec(Where::condition("status", EQ, "OPEN")),
            )
            .len();
    }
    let twins = started.elapsed();
    let after_twins = ivm.stats().clone();
    let started = Instant::now();
    for index in 0..JOIN_DISTINCT {
        ivm.register_query(format!("d{index}"), join_spec(distinct_join_filter(index)));
    }
    let distinct = started.elapsed();
    let after_distinct = ivm.stats().clone();

    println!("\nregistration:");
    print_table(
        &[
            "group",
            "subs",
            "us/register",
            "snapshot ops/sub",
            "snapshots_shared",
            "disjuncts_registered",
            "conditions_indexed",
        ],
        &[
            vec![
                "identical".to_owned(),
                JOIN_TWINS.to_string(),
                one(twins.as_secs_f64() * 1e6 / JOIN_TWINS as f64),
                one(twin_ops as f64 / JOIN_TWINS as f64),
                after_twins.snapshots_shared.to_string(),
                after_twins.disjuncts_registered.to_string(),
                after_twins.conditions_indexed.to_string(),
            ],
            vec![
                "distinct".to_owned(),
                JOIN_DISTINCT.to_string(),
                one(distinct.as_secs_f64() * 1e6 / JOIN_DISTINCT as f64),
                String::from("-"),
                (after_distinct.snapshots_shared - after_twins.snapshots_shared).to_string(),
                (after_distinct.disjuncts_registered - after_twins.disjuncts_registered).to_string(),
                (after_distinct.conditions_indexed - after_twins.conditions_indexed).to_string(),
            ],
        ],
    );

    let mut bench = JoinBench {
        ivm,
        storage,
        rng,
        tickets,
        user_versions,
    };
    let phases = [
        bench.ticket_inserts(),
        bench.reassignments(),
        bench.user_updates(),
    ];
    println!("\nrouting:");
    let rows: Vec<Vec<String>> = phases
        .iter()
        .map(|(run, main_ops, join_ops)| {
            vec![
                run.label.clone(),
                run.writes.to_string(),
                one(run.micros_per_write()),
                whole(run.writes_per_second()),
                one(run.per_write(run.stats.columns_probed)),
                one(run.per_write(run.stats.conditions_evaluated)),
                one(run.per_write(run.stats.disjunct_increments)),
                one(run.per_write(run.stats.queries_impacted)),
                one(*main_ops as f64 / run.writes as f64),
                one(*join_ops as f64 / run.writes as f64),
                run.stats.conditions_replaced.to_string(),
                one(run.per_write(run.stats.conditions_replaced)),
            ]
        })
        .collect();
    print_table(
        &[
            "phase",
            "writes",
            "us/write",
            "writes/s",
            "cols",
            "cond_match",
            "disj_incr",
            "impacted",
            "main ops",
            "join ops",
            "conditions_replaced",
            "replaced/write",
        ],
        &rows,
    );
}

/// Run the four scenarios in order.
fn main() {
    println!(
        "jus_sync bench: release build, single thread, xorshift seed {SEED:#x}, tables {} / {}",
        tickets_table().name,
        users_table().name
    );
    let started = Instant::now();
    routing_and_registration();
    twin_sharing();
    window();
    left_join();
    println!("\ntotal wall time: {}s", one(started.elapsed().as_secs_f64()));
}
