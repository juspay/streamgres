//! Print the DDL event-trigger stack the reference server installs, for a database
//! the reference server has never run against (a lab, a test database), so that the
//! server can hear of schema changes there:
//! `cargo run --example ddl_triggers -- <app> <shard> <publication>... | psql "$DSN"`.

use xyne_sync::sync::pg::ddl::trigger_stack_sql;

/// Print the stack for the app, shard and publications given.
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let shard = args.get(1).and_then(|text| text.parse::<u32>().ok());
    let (Some(app), Some(shard), publications) = (args.first(), shard, &args[2.min(args.len())..])
    else {
        eprintln!("usage: ddl_triggers <app> <shard> <publication>...");
        std::process::exit(2);
    };
    if publications.is_empty() {
        eprintln!("usage: ddl_triggers <app> <shard> <publication>...");
        std::process::exit(2);
    }
    let publications: Vec<&str> = publications.iter().map(String::as_str).collect();
    print!("{}", trigger_stack_sql(app, shard, &publications));
}
