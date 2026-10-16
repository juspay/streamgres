//! The planner's sizes counted against estimated, on a PostgreSQL holding
//! production-shaped data: every `whereExists` query of the application's
//! registry the planner has to decide (as `streamgres_queries` builds
//! them, with the workspace backstop on the root), for a heavy, a median
//! and a light reader and channel picked from the data. Each query is
//! sized both ways through [`PgStorage`] — [`Storage::count`], the capped
//! `count(*)` the planner runs today, the wall time the median of three,
//! and [`Storage::estimate`], PostgreSQL's `EXPLAIN`s and statistics,
//! timed first asked (the estimate cache cleared) and asked again (from
//! the cache) — concurrent as `plan` sizes them; the planner then decides
//! on each and the two plans are compared. Printed: per query the times,
//! the two decisions' agreement and both sizes per node; then the totals.
//!
//! [`a_plan_laid_on_other_arguments`] measures what planning by name
//! costs the arguments it was not made for, and
//! [`planning_every_argument`] plans every query for every argument the
//! way the server does, counted by name against estimated per tree.
//!
//! Ignored by default (it needs a database with data, and is read-only on
//! it); run with
//! `STREAMGRES_PLAN_BENCH_DSN=postgres://… cargo test --release --test
//! plan_sizes -- --ignored --nocapture`. The database must be analyzed
//! (`ANALYZE`), have `wal_level = logical` (the storage reads on exported
//! snapshots) and a `public` schema of the application's tables.

#[path = "streamgres_queries/catalog.rs"]
#[allow(dead_code)]
mod catalog;
#[path = "streamgres_queries/zql.rs"]
#[allow(dead_code)]
mod zql;

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::future::join_all;
use streamgres::client::ast::Translated;
use streamgres::client::plan::{Count, PlanCache, Planner, Policy, Side, Sizes, plan};
use streamgres::model::ComparisonOperator::{GT, IN};
use streamgres::model::Order::DESC;
use streamgres::model::{Catalog, Lsn, MultiTableReadQuery};
use streamgres::sync::pg::PgStorage;
use streamgres::sync::pg::catalog::load_catalog;
use streamgres::sync::{Storage, StorageError};
use zql::{Q, eq, or, same, strings, zql};

/// Who and where a query is asked for.
#[derive(Debug, Clone)]
struct Params {
    label: &'static str,
    me: String,
    channel: String,
    desk: String,
}

/// The planner's production policy.
fn policy() -> Policy {
    Policy {
        limit: 100_000,
        preferred: Side::Parent,
        sizes: Sizes::Count,
    }
}

/// The queries the planner decides, by registry name, for `p`.
fn queries(p: &Params) -> Vec<(&'static str, Q)> {
    let me = p.me.as_str();
    let channel = p.channel.as_str();
    let mut out = Vec::new();
    out.push((
        "userVisibleChannels",
        zql("channels")
            .where_exists("participantsStatus", |s| {
                s.eq("isClosed", false).eq("isDeleted", false).eq("userId", me)
            })
            .related("channelStats", same),
    ));
    let mut browsable = zql("channels").eq("scopeType", "DEFAULT");
    let member = browsable.exists("participants", |m| m.eq("userId", me));
    out.push((
        "browsableChannels",
        browsable
            .filter(or(vec![eq("visibility", "PUBLIC"), member]))
            .related("participants", same),
    ));
    out.push((
        "searchChannelParticipants",
        zql("channel_participants")
            .eq("channelId", channel)
            .where_exists("user", same),
    ));
    out.push((
        "dmChannelsLatestMessagesPaginated",
        zql("channel_stats")
            .where_exists("channel", |ch| {
                ch.filter(or(vec![eq("scopeType", "DM"), eq("scopeType", "GROUP_DM")]))
            })
            .order_by("lastActivityAt", DESC)
            .order_by("channelId", DESC)
            .limit(10),
    ));
    out.push((
        "userConversationsPaginated",
        zql("conversations")
            .where_("replyCount", GT, 0)
            .where_exists("participants", |m| m.eq("userId", me))
            .order_by("lastActivityAt", DESC)
            .limit(10),
    ));
    out.push((
        "channelAndThreadMessagesV2",
        zql("messages")
            .eq("showInChannel", true)
            .where_exists("conversation", |c| c.eq("channelId", channel)),
    ));
    out.push((
        "channelConversations (channel-access rule)",
        zql("conversations")
            .eq("channelId", channel)
            .where_exists("channel", |ch| {
                let mut ch = ch.eq("id", channel);
                let member = ch.exists("participants", |m| m.eq("userId", me));
                ch.filter(or(vec![eq("visibility", "PUBLIC"), member]))
            })
            .order_by("createdAt", DESC)
            .limit(50),
    ));
    out.push((
        "channelTickets (member chain)",
        zql("tickets").eq("channelId", channel).where_exists("channel", |ch| {
            ch.eq("id", channel)
                .where_exists("participants", |m| m.eq("userId", me).eq("channelId", channel))
        }),
    ));
    out.push((
        "supportTicketsPageV4",
        zql("tickets")
            .eq("channelId", p.desk.as_str())
            .eq("isArchived", false)
            .where_exists("emails", |e| {
                e.where_("type", IN, strings(&["REPLY", "REPLY_ALL", "COMPOSE"]))
                    .eq("sentByUserId", me)
            })
            .order_by("lastEmailAt", DESC)
            .order_by("id", DESC)
            .limit(10),
    ));
    out.push((
        "ticketsForEmailChannelsV2",
        zql("tickets")
            .where_exists("conversation", |c| {
                c.where_exists("channel", |ch| ch.eq("type", "EMAIL"))
            })
            .order_by("createdAt", DESC),
    ));
    out.push((
        "userActivitiesPaginatedV2",
        zql("activities")
            .eq("userId", me)
            .eq("isRead", false)
            .where_exists("actor", |a| a.where_("userType", IN, strings(&["HUMAN"])))
            .order_by("updatedAt", DESC)
            .order_by("id", DESC)
            .limit(10),
    ));
    out.push((
        "userUnreadThreadActivities",
        zql("activities")
            .eq("userId", me)
            .eq("isRead", false)
            .eq("actionSource", "message")
            .where_exists("message", |m| {
                m.where_exists("conversation", |c| c.where_("replyCount", GT, 0))
            }),
    ));
    let mut canvases = zql("canvases");
    let participant = canvases.exists("participants", |m| m.eq("userId", me));
    out.push((
        "userCanvasesPaginated (visible)",
        canvases
            .filter(or(vec![eq("createdBy", me), eq("visibility", "PUBLIC"), participant]))
            .order_by("updatedAt", DESC)
            .order_by("id", DESC)
            .limit(20),
    ));
    let mut rule = zql("canvas_participants");
    let group = rule.exists("userGroup", |g| {
        g.where_exists("userGroupMappings", |m| m.eq("userId", me))
    });
    let in_channel = rule.exists("channel", |ch| {
        ch.where_exists("participants", |m| m.eq("userId", me))
    });
    let canvas = rule.exists("canvas", |c| {
        let mut c = c;
        let mine = c.exists("participants", |m| m.eq("userId", me));
        c.filter(or(vec![eq("createdBy", me), eq("visibility", "PUBLIC"), mine]))
    });
    out.push((
        "canvasParticipants (access rule)",
        rule.filter(or(vec![eq("userId", me), group, in_channel, canvas])),
    ));
    out
}

/// One way of sizing a tree: its answers in `wanted`'s order, how many
/// fell back to a count (estimates only), and its wall time.
struct Sized {
    answers: Vec<u64>,
    fallbacks: usize,
    wall: Duration,
}

/// Size `wanted` on `storage` the way `sizes` says, concurrently as the
/// server does; an estimate that cannot be made is counted.
async fn size(storage: &PgStorage, wanted: &[Count], sizes: Sizes) -> Result<Sized, StorageError> {
    let started = Instant::now();
    let answers = join_all(wanted.iter().map(|count| async move {
        match sizes {
            Sizes::Count | Sizes::Shadow => storage.count(&count.query, count.cap).await.map(|n| (n, false)),
            Sizes::Estimate => match storage.estimate(&count.query, count.cap).await {
                Ok(rows) => Ok((rows, false)),
                Err(_) => storage.count(&count.query, count.cap).await.map(|n| (n, true)),
            },
        }
    }))
    .await;
    let wall = started.elapsed();
    let mut sized = Sized {
        answers: Vec::with_capacity(answers.len()),
        fallbacks: 0,
        wall,
    };
    for answer in answers {
        let (rows, fell_back) = answer?;
        sized.answers.push(rows);
        sized.fallbacks += usize::from(fell_back);
    }
    Ok(sized)
}

/// The median-time sizing of three.
async fn median_of_three(
    storage: &PgStorage,
    wanted: &[Count],
    sizes: Sizes,
) -> Result<Sized, StorageError> {
    let mut runs = Vec::new();
    for _ in 0..3 {
        runs.push(size(storage, wanted, sizes).await?);
    }
    runs.sort_by_key(|run| run.wall);
    Ok(runs.swap_remove(1))
}

/// The planner for `spec` answered with `answers`.
fn answered(spec: &MultiTableReadQuery, wanted: &[Count], answers: &[u64]) -> Planner {
    let mut planner = Planner::new(spec.clone(), policy());
    for (count, rows) in wanted.iter().zip(answers) {
        planner.answer(count.node, count.narrowed, *rows);
    }
    planner
}

/// The plan `spec` gets from `answers`, and the planner's report of them.
fn decide(spec: &MultiTableReadQuery, wanted: &[Count], answers: &[u64]) -> (Result<MultiTableReadQuery, String>, String) {
    let planner = answered(spec, wanted, answers);
    let report = planner.report();
    (planner.decide(), report)
}

/// A hold for the table: the rows, or `over` the limit.
fn shown(hold: Option<u64>) -> String {
    hold.map_or("over".to_owned(), |rows| rows.to_string())
}

/// Text of the one row one column `sql` returns.
async fn one(client: &tokio_postgres::Client, sql: &str) -> String {
    let rows = client.simple_query(sql).await.expect(sql);
    rows.into_iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no row for {sql}"))
}

/// The heavy, median and light reader (by channel memberships), the
/// busiest, median and quietest channel (by conversations), and the desk
/// channel with the most tickets.
async fn params(client: &tokio_postgres::Client) -> Vec<Params> {
    let desk = one(
        client,
        r#"SELECT t."channelId" FROM tickets t JOIN channels c ON c.id = t."channelId"
           GROUP BY 1, c.type ORDER BY (c.type = 'EMAIL') DESC, count(*) DESC LIMIT 1"#,
    )
    .await;
    let mut out = Vec::new();
    for (label, at) in [("heavy", 0.0), ("median", 0.5), ("light", 1.0)] {
        out.push(Params {
            label,
            me: one(client, &user_at(at)).await,
            channel: one(client, &channel_at(at)).await,
            desk: desk.clone(),
        });
    }
    out
}

/// The reader at the quantile `at` of channel memberships, the busiest
/// first.
fn user_at(at: f64) -> String {
    format!(
        r#"SELECT "userId" FROM (SELECT "userId", count(*) n FROM channel_participants GROUP BY 1) s
           ORDER BY n DESC, 1 LIMIT 1 OFFSET (SELECT floor({at} * (count(DISTINCT "userId") - 1))::int FROM channel_participants)"#
    )
}

/// The channel at the quantile `at` of conversations, the busiest first.
fn channel_at(at: f64) -> String {
    format!(
        r#"SELECT "channelId" FROM (SELECT "channelId", count(*) n FROM conversations GROUP BY 1) s
           ORDER BY n DESC, 1 LIMIT 1 OFFSET (SELECT floor({at} * (count(DISTINCT "channelId") - 1))::int FROM conversations)"#
    )
}

/// The desk at the quantile `at` of tickets, the busiest first.
fn desk_at(at: f64) -> String {
    format!(
        r#"SELECT "channelId" FROM (SELECT "channelId", count(*) n FROM tickets GROUP BY 1) s
           ORDER BY n DESC, 1 LIMIT 1 OFFSET (SELECT floor({at} * (count(DISTINCT "channelId") - 1))::int FROM tickets)"#
    )
}

#[test]
#[ignore = "needs STREAMGRES_PLAN_BENCH_DSN, a database with data"]
fn counted_against_estimated() {
    let Ok(dsn) = std::env::var("STREAMGRES_PLAN_BENCH_DSN") else {
        eprintln!("STREAMGRES_PLAN_BENCH_DSN not set: skipped");
        return;
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(connection);
        let (catalog, _) = load_catalog(&client, &["public".to_owned()])
            .await
            .expect("catalog");
        let workspace = one(&client, r#"SELECT "workspaceId" FROM channels LIMIT 1"#).await;
        let catalog: Arc<Catalog> = Arc::new(catalog);
        let storage = PgStorage::connect(&dsn, catalog.clone())
            .await
            .expect("storage")
            .with_read_connections(64);
        storage.advance(Lsn(u64::MAX));
        let params = params(&client).await;

        let mut plans = 0;
        let mut same = 0;
        let mut count_total = Duration::ZERO;
        let mut estimate_total = Duration::ZERO;
        let mut count_times = Vec::new();
        let mut estimate_times = Vec::new();
        let mut warm_total = Duration::ZERO;
        let mut warm_times = Vec::new();
        let mut fallbacks = 0;
        let mut differing = Vec::new();
        let mut held = (0u64, 0u64);
        let mut worse = Vec::new();
        for p in &params {
            println!("\n== {} reader {} · channel {} · desk {}", p.label, p.me, p.channel, p.desk);
            println!(
                "{:<44} {:>10} {:>10} {:>8} {:>5} {:>13}  sizes (count | estimate)",
                "query", "count ms", "est ms", "cached", "same", "holds c/e"
            );
            for (name, q) in queries(p) {
                let q = if catalog
                    .table(q.table())
                    .is_some_and(|table| table.column("workspaceId").is_some())
                {
                    q.eq("workspaceId", workspace.as_str())
                } else {
                    q
                };
                let (spec, _) = q.build();
                if let Some(missing) = spec.tables().into_iter().find(|t| catalog.table(t.as_str()).is_none()) {
                    println!("{name:<44} skipped: no table `{missing}`");
                    continue;
                }
                storage.advance(Lsn(u64::MAX));
                let wanted = Planner::new(spec.clone(), policy()).counts();
                if wanted.is_empty() {
                    println!("{name:<44} nothing to size");
                    continue;
                }
                let counted = median_of_three(&storage, &wanted, Sizes::Count).await;
                storage.estimate_cache().clear();
                let estimated = size(&storage, &wanted, Sizes::Estimate).await;
                let warm = size(&storage, &wanted, Sizes::Estimate).await;
                let (counted, estimated, warm) = match (counted, estimated, warm) {
                    (Ok(c), Ok(e), Ok(w)) => (c, e, w),
                    (c, e, _) => {
                        println!("{name:<44} failed: {:?} / {:?}", c.err(), e.err());
                        continue;
                    }
                };
                assert_eq!(warm.answers, estimated.answers, "the cache answers as asked");
                let (by_count, count_report) = decide(&spec, &wanted, &counted.answers);
                let (by_estimate, estimate_report) = decide(&spec, &wanted, &estimated.answers);
                let agree = match (&by_count, &by_estimate) {
                    (Ok(a), Ok(b)) => a == b,
                    (Err(_), Err(_)) => true,
                    _ => false,
                };
                let judge = answered(&spec, &wanted, &counted.answers);
                let holds = match (&by_count, &by_estimate) {
                    (Ok(a), Ok(b)) => (judge.hold_of(a), judge.hold_of(b)),
                    _ => (None, None),
                };
                if let (Some(c), Some(e)) = holds {
                    held.0 += c;
                    held.1 += e;
                    if e > c {
                        worse.push(format!("{} / {name}: {c} -> {e}", p.label));
                    }
                }
                plans += 1;
                same += usize::from(agree);
                fallbacks += estimated.fallbacks;
                count_total += counted.wall;
                estimate_total += estimated.wall;
                warm_total += warm.wall;
                count_times.push(counted.wall);
                estimate_times.push(estimated.wall);
                warm_times.push(warm.wall);
                if !agree {
                    differing.push(format!("{} / {name}", p.label));
                }
                println!(
                    "{name:<44} {:>10.2} {:>10.2} {:>8.3} {:>5} {:>13}  {count_report} | {estimate_report}{}",
                    counted.wall.as_secs_f64() * 1000.0,
                    estimated.wall.as_secs_f64() * 1000.0,
                    warm.wall.as_secs_f64() * 1000.0,
                    if agree { "yes" } else { "NO" },
                    format!("{}/{}", shown(holds.0), shown(holds.1)),
                    if estimated.fallbacks > 0 {
                        format!(" ({} counted: no estimate)", estimated.fallbacks)
                    } else {
                        String::new()
                    }
                );
            }
        }
        let quantile = |times: &mut Vec<Duration>, q: f64| {
            times.sort();
            times
                .get(((times.len() as f64 - 1.0) * q).round() as usize)
                .copied()
                .unwrap_or_default()
                .as_secs_f64()
                * 1000.0
        };
        println!("\n== totals over {plans} plans");
        println!("same plan: {same}/{plans}; differing: {differing:?}");
        println!("estimates that fell back to a count: {fallbacks}");
        println!(
            "rows held on the counts: {} by the counts' plans, {} by the estimates' plans; held more: {worse:?}",
            held.0, held.1
        );
        println!(
            "count:    total {:>9.1} ms, p50 {:>8.2} ms, max {:>8.2} ms",
            count_total.as_secs_f64() * 1000.0,
            quantile(&mut count_times, 0.5),
            quantile(&mut count_times, 1.0)
        );
        println!(
            "estimate: total {:>9.1} ms, p50 {:>8.2} ms, max {:>8.2} ms (first asked)",
            estimate_total.as_secs_f64() * 1000.0,
            quantile(&mut estimate_times, 0.5),
            quantile(&mut estimate_times, 1.0)
        );
        println!(
            "cached:   total {:>9.1} ms, p50 {:>8.3} ms, max {:>8.3} ms (asked again)",
            warm_total.as_secs_f64() * 1000.0,
            quantile(&mut warm_times, 0.5),
            quantile(&mut warm_times, 1.0)
        );
    });
}

/// The quantiles a reader, channel and desk are picked at for
/// [`a_plan_laid_on_other_arguments`], the busiest first.
const SPREAD: [(&str, f64); 10] = [
    ("p0", 0.0),
    ("p0.1", 0.001),
    ("p1", 0.01),
    ("p5", 0.05),
    ("p10", 0.1),
    ("p25", 0.25),
    ("p50", 0.5),
    ("p75", 0.75),
    ("p90", 0.9),
    ("p100", 1.0),
];

/// What the name cache costs: each query planned on its true counts for
/// readers, channels and desks across [`SPREAD`], then every argument's
/// plan laid (as [`PlanCache::by_name`] lays it) onto every other
/// argument's tree and held on that tree's counts, against the plan the
/// tree's own counts make. Printed per query: how many of the laid plans
/// differ from the tree's own, the rows held when a uniformly drawn
/// argument asks first against the rows the trees' own plans hold, and
/// the worst first asker.
#[test]
#[ignore = "needs STREAMGRES_PLAN_BENCH_DSN, a database with data"]
fn a_plan_laid_on_other_arguments() {
    let Ok(dsn) = std::env::var("STREAMGRES_PLAN_BENCH_DSN") else {
        eprintln!("STREAMGRES_PLAN_BENCH_DSN not set: skipped");
        return;
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(connection);
        let (catalog, _) = load_catalog(&client, &["public".to_owned()])
            .await
            .expect("catalog");
        let workspace = one(&client, r#"SELECT "workspaceId" FROM channels LIMIT 1"#).await;
        let catalog: Arc<Catalog> = Arc::new(catalog);
        let storage = PgStorage::connect(&dsn, catalog.clone())
            .await
            .expect("storage")
            .with_read_connections(64);
        storage.advance(Lsn(u64::MAX));
        let mut spread = Vec::new();
        for (label, at) in SPREAD {
            spread.push(Params {
                label,
                me: one(&client, &user_at(at)).await,
                channel: one(&client, &channel_at(at)).await,
                desk: one(&client, &desk_at(at)).await,
            });
        }
        for p in &spread {
            println!("{:<5} reader {} · channel {} · desk {}", p.label, p.me, p.channel, p.desk);
        }
        let names: Vec<&str> = queries(&spread[0]).into_iter().map(|(name, _)| name).collect();
        let over = |hold: Option<u64>| hold.unwrap_or(u64::MAX);
        let (mut own_total, mut laid_total) = (0u64, 0u64);
        let (mut pairs, mut differ, mut worse, mut over_limit) = (0, 0, 0, 0);
        println!(
            "\n{:<44} {:>8} {:>8} {:>12} {:>14} {:>14}  own holds by argument; worst first asker",
            "query", "plans", "differ", "worse", "own rows", "laid rows"
        );
        for (at, name) in names.iter().enumerate() {
            // Per argument: its tree, the planner on its true counts, its own plan.
            let mut trees = Vec::new();
            for p in &spread {
                let (_, q) = queries(p).swap_remove(at);
                let q = if catalog
                    .table(q.table())
                    .is_some_and(|table| table.column("workspaceId").is_some())
                {
                    q.eq("workspaceId", workspace.as_str())
                } else {
                    q
                };
                let (spec, _) = q.build();
                storage.advance(Lsn(u64::MAX));
                let wanted = Planner::new(spec.clone(), policy()).counts();
                let counted = size(&storage, &wanted, Sizes::Count).await.expect("count");
                let judge = answered(&spec, &wanted, &counted.answers);
                let (own, _) = decide(&spec, &wanted, &counted.answers);
                trees.push((spec, judge, own));
            }
            if trees.iter().all(|(spec, ..)| Planner::new(spec.clone(), policy()).counts().is_empty()) {
                println!("{name:<44} nothing to size");
                continue;
            }
            let own_holds: Vec<Option<u64>> = trees
                .iter()
                .map(|(_, judge, own)| own.as_ref().ok().and_then(|own| judge.hold_of(own)))
                .collect();
            // The distinct plans: one is another when laid onto its tree it is that tree's plan.
            let lay = |known: &MultiTableReadQuery, spec: &MultiTableReadQuery| {
                let cache = PlanCache::new(Duration::from_secs(60), 10, Duration::from_secs(60));
                cache.remember(name, known);
                cache.by_name(name, spec)
            };
            let mut distinct: Vec<MultiTableReadQuery> = Vec::new();
            for (spec, _, own) in &trees {
                if let Ok(own) = own
                    && !distinct.iter().any(|known| lay(known, spec).as_ref() == Some(own))
                {
                    distinct.push(own.clone());
                }
            }
            let (mut name_differ, mut name_worse, mut name_over) = (0, 0, 0);
            let mut by_first = Vec::new();
            for (first, (_, _, planned)) in trees.iter().enumerate() {
                let Ok(planned) = planned else {
                    continue;
                };
                let cache = PlanCache::new(Duration::from_secs(60), 10, Duration::from_secs(60));
                cache.remember(name, planned);
                let mut held = 0u64;
                for (later, (spec, judge, own)) in trees.iter().enumerate() {
                    let laid = cache.by_name(name, spec).expect("same skeleton");
                    let hold = judge.hold_of(&laid);
                    let mine = own_holds[later];
                    pairs += 1;
                    if own.as_ref().ok() != Some(&laid) {
                        name_differ += 1;
                    }
                    if over(hold) > over(mine) {
                        name_worse += 1;
                    }
                    if hold.is_none() && mine.is_some() {
                        name_over += 1;
                    }
                    held = held.saturating_add(over(hold));
                }
                by_first.push((spread[first].label, held));
            }
            let own_rows: u64 = own_holds.iter().map(|&hold| over(hold)).fold(0, u64::saturating_add);
            let laid_mean = if by_first.is_empty() {
                0
            } else {
                by_first.iter().map(|(_, held)| *held).fold(0, u64::saturating_add) / by_first.len() as u64
            };
            let worst = by_first.iter().max_by_key(|(_, held)| *held).copied();
            own_total = own_total.saturating_add(own_rows);
            laid_total = laid_total.saturating_add(laid_mean);
            differ += name_differ;
            worse += name_worse;
            over_limit += name_over;
            let shown_rows = |rows: u64| if rows == u64::MAX { "over".to_owned() } else { rows.to_string() };
            println!(
                "{name:<44} {:>8} {:>8} {:>12} {:>14} {:>14}  [{}]; {}",
                distinct.len(),
                format!("{name_differ}/{}", by_first.len() * trees.len()),
                format!("{name_worse} ({name_over} over)"),
                shown_rows(own_rows),
                shown_rows(laid_mean),
                own_holds.iter().map(|&hold| shown(hold)).collect::<Vec<_>>().join(" "),
                worst.map_or(String::new(), |(label, held)| format!("{label} {}", shown_rows(held))),
            );
        }
        println!("\n== {pairs} (first asker, later asker) pairs over {} arguments", spread.len());
        println!("laid plan differs from the tree's own: {differ}; holds more: {worse}; over the limit where its own is not: {over_limit}");
        println!("rows held: {own_total} by every tree's own plan, {laid_total} by the name cache (mean over first askers)");
    });
}

/// Every query planned for every argument of [`SPREAD`] the way the
/// server plans them ([`plan`]), arguments arriving busiest first and
/// then quietest first: counted and remembered by name (the server's
/// default), against estimated per tree, first with the estimate cache
/// empty and then again from it. Printed per way: the wall time of all
/// the plans, the slowest plan, the statements run on PostgreSQL for
/// sizes (counts, or `EXPLAIN`s and column statistics newly remembered),
/// and the rows the plans hold measured on the true counts, against the
/// rows every tree's own counted plan holds.
#[test]
#[ignore = "needs STREAMGRES_PLAN_BENCH_DSN, a database with data"]
fn planning_every_argument() {
    let Ok(dsn) = std::env::var("STREAMGRES_PLAN_BENCH_DSN") else {
        eprintln!("STREAMGRES_PLAN_BENCH_DSN not set: skipped");
        return;
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(connection);
        let (catalog, _) = load_catalog(&client, &["public".to_owned()])
            .await
            .expect("catalog");
        let workspace = one(&client, r#"SELECT "workspaceId" FROM channels LIMIT 1"#).await;
        let catalog: Arc<Catalog> = Arc::new(catalog);
        let storage = PgStorage::connect(&dsn, catalog.clone())
            .await
            .expect("storage")
            .with_read_connections(64);
        storage.advance(Lsn(u64::MAX));
        let mut spread = Vec::new();
        for (label, at) in SPREAD {
            spread.push(Params {
                label,
                me: one(&client, &user_at(at)).await,
                channel: one(&client, &channel_at(at)).await,
                desk: one(&client, &desk_at(at)).await,
            });
        }
        // Every (name, tree), busiest arguments first, each with its judge
        // (the planner on its true counts) and what its own plan holds.
        let mut trees = Vec::new();
        for p in &spread {
            for (name, q) in queries(p) {
                let q = if catalog
                    .table(q.table())
                    .is_some_and(|table| table.column("workspaceId").is_some())
                {
                    q.eq("workspaceId", workspace.as_str())
                } else {
                    q
                };
                let (spec, _) = q.build();
                let wanted = Planner::new(spec.clone(), policy()).counts();
                if wanted.is_empty() {
                    continue;
                }
                let counted = size(&storage, &wanted, Sizes::Count).await.expect("count");
                let judge = answered(&spec, &wanted, &counted.answers);
                let own = decide(&spec, &wanted, &counted.answers)
                    .0
                    .ok()
                    .and_then(|own| judge.hold_of(&own));
                trees.push((name, spec, judge, own));
            }
        }
        let own: u64 = trees.iter().map(|(.., own)| own.unwrap_or(u64::MAX)).fold(0, u64::saturating_add);
        let shown = |rows: u64| if rows == u64::MAX { "over".to_owned() } else { rows.to_string() };
        println!("{} trees, {} queries x {} arguments; rows their own counted plans hold: {}", trees.len(), queries(&spread[0]).len(), spread.len(), shown(own));
        println!(
            "\n{:<36} {:>10} {:>10} {:>12} {:>12}",
            "way", "total ms", "max ms", "statements", "rows held"
        );
        let by_order = |reverse: bool| {
            let mut order: Vec<usize> = (0..trees.len()).collect();
            if reverse {
                order.reverse();
            }
            order
        };
        for (way, sizes, reverse, fresh) in [
            ("counted, by name, busiest first", Sizes::Count, false, true),
            ("counted, by name, quietest first", Sizes::Count, true, true),
            ("estimated per tree, cache empty", Sizes::Estimate, false, true),
            ("estimated per tree, from the cache", Sizes::Estimate, false, false),
        ] {
            if fresh {
                storage.estimate_cache().clear();
            }
            let cache = PlanCache::new(Duration::from_secs(600), 10_000, Duration::from_secs(86_400));
            let policy = Policy { sizes, ..policy() };
            let (mut total, mut slowest, mut held) = (Duration::ZERO, Duration::ZERO, 0u64);
            let mut statements = 0usize;
            for at in by_order(reverse) {
                let (name, spec, judge, _) = &trees[at];
                let before = storage.estimate_cache().len();
                let asked = Planner::new(spec.clone(), policy).counts().len();
                let named = cache.queries();
                let started = Instant::now();
                let planned = plan(
                    name,
                    Translated {
                        query: spec.clone(),
                        hidden: Default::default(),
                        uncursored: None,
                    },
                    policy,
                    &cache,
                    &storage,
                )
                .await;
                let took = started.elapsed();
                total += took;
                slowest = slowest.max(took);
                statements += match sizes {
                    Sizes::Estimate => storage.estimate_cache().len().saturating_sub(before),
                    _ => usize::from(cache.queries() != named) * asked,
                };
                held = held.saturating_add(
                    planned
                        .ok()
                        .and_then(|planned| judge.hold_of(&planned.query))
                        .unwrap_or(u64::MAX),
                );
            }
            println!(
                "{way:<36} {:>10.1} {:>10.2} {:>12} {:>12}",
                total.as_secs_f64() * 1000.0,
                slowest.as_secs_f64() * 1000.0,
                statements,
                shown(held)
            );
        }
        println!("estimate cache: {} answers kept", storage.estimate_cache().len());
    });
}
