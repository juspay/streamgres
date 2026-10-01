//! Print what the change feed delivers from `<dsn>` on `<slot>`, streaming
//! `<publication>` (both created when missing), schema
//! changes included as the DDL trigger whose messages carry `<prefix>`
//! announces them; the slot is first moved up to `<start>` when one is
//! given. A lab tool: `cargo run --example feed_probe -- <dsn> <slot>
//! <publication> <prefix> [start-lsn]`.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use xyne_sync::sync::pg::ddl::DdlSource;
use xyne_sync::sync::pg::threads::load_catalog_at;
use xyne_sync::sync::pg::{Feed, Transport};
use xyne_sync::sync::{Lsn, Transaction};

/// Stream until the feed ends, printing every transaction.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [dsn, slot, publication, prefix, ..] = args.as_slice() else {
        eprintln!("usage: feed_probe <dsn> <slot> <publication> <prefix> [start-lsn]");
        std::process::exit(2);
    };
    let schemas = vec!["public".to_owned()];
    let catalog = Arc::new(load_catalog_at(dsn, &schemas).await.expect("catalog"));
    let ddl = DdlSource {
        prefix: prefix.clone(),
        schemas,
    };
    let mut feed = Feed::with_ddl(catalog, ddl);
    Transport::prepare(dsn, slot, publication)
        .await
        .expect("prepare the feed");
    let transport = match args.get(4) {
        Some(start) => {
            let start = Lsn::parse(start).expect("an LSN like 0/1A2B");
            Transport::open_from(dsn, slot, publication, start).await
        }
        None => Transport::open(dsn, slot, publication).await,
    }
    .expect("open the feed");
    let (out, mut transactions) = mpsc::channel::<Transaction>(64);
    let printer = tokio::spawn(async move {
        while let Some(transaction) = transactions.recv().await {
            if transaction.is_mark() {
                continue;
            }
            println!(
                "commit {} writes={} schema={:?} catalog_tables={:?}",
                transaction.at,
                transaction.writes.len(),
                transaction.schema,
                transaction
                    .catalog
                    .as_ref()
                    .map(|catalog| catalog.tables().count())
            );
            for write in &transaction.writes {
                println!("  {write:?}");
            }
        }
    });
    let outcome = transport
        .stream(Duration::from_millis(200), &mut feed, &[], out)
        .await;
    println!("feed ended: {outcome:?}");
    let _ = printer.await;
}
