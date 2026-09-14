//! One pinning test per gap named in `main.rs`, so each is visible in the
//! suite on its own and starts failing the moment the engine closes it;
//! plus the two rewrites that need no engine change (keyset cursors and the
//! empty `IN` list), shown to be exact.

use jus_sync::model::ComparisonOperator::{GT, NEQ};
use jus_sync::model::Order::{ASC, DESC};
use jus_sync::model::{Value, ValueType, Where};
use jus_sync::parser::parse_read;

use super::catalog::catalog;
use super::world::{ME, World, ops};
use super::zql::{eq, is_null, or, same, zql};

/// Closed gap N: `visibleTo IS NULL OR visibleTo = me` is the visibility
/// rule of every message query. `IS NULL` selects the everyone-visible
/// message, `!=` still drops `NULL` rows as SQL does, and the parser reads
/// both spellings while refusing `IS` with anything but `NULL`.
#[test]
fn n_is_null_selects_null_rows_and_comparisons_still_do_not() {
    let mut w = World::new();
    w.seed(
        "messages",
        &[
            ("messageId", "m-all".into()),
            ("conversationId", "c1".into()),
            ("visibleTo", Value::Null),
        ],
    );
    w.seed(
        "messages",
        &[
            ("messageId", "m-me".into()),
            ("conversationId", "c1".into()),
            ("visibleTo", ME.into()),
        ],
    );
    w.seed(
        "messages",
        &[
            ("messageId", "m-other".into()),
            ("conversationId", "c1".into()),
            ("visibleTo", "u-2".into()),
        ],
    );

    let visible = zql("messages")
        .eq("conversationId", "c1")
        .filter(or(vec![is_null("visibleTo"), eq("visibleTo", ME)]));
    assert_eq!(
        w.subscribe("q", &visible),
        ops(["q/main+m-all", "q/main+m-me"])
    );

    let not_hidden = zql("messages")
        .eq("conversationId", "c1")
        .where_("visibleTo", NEQ, "u-2");
    assert_eq!(w.subscribe("q2", &not_hidden), ops(["q2/main+m-me"]));
    assert_eq!(
        w.update(
            "messages",
            &[
                ("messageId", "m-all".into()),
                ("conversationId", "c1".into()),
                ("visibleTo", "u-2".into()),
            ],
        ),
        ops(["q/main-m-all"])
    );

    let parsed = parse_read("SELECT * FROM messages WHERE visibleTo IS NULL", &catalog())
        .expect("IS NULL parses");
    assert_eq!(parsed.filter, Where::is_null("visibleTo"));
    let parsed = parse_read("SELECT * FROM tickets WHERE rootId IS NOT NULL", &catalog())
        .expect("IS NOT NULL parses");
    assert_eq!(parsed.filter, Where::is_not_null("rootId"));
    let error = parse_read("SELECT * FROM messages WHERE visibleTo IS 'x'", &catalog())
        .expect_err("IS takes NULL only");
    assert!(error.message.contains("expected NULL after IS"), "{error}");
}

/// Gap L: the search queries match `name`, `title`, `xyneId` with `ILIKE`,
/// and two recording queries scan a JSON column with `LIKE`; the parser
/// refuses both spellings.
#[test]
fn l_like_and_ilike_are_refused() {
    let error = parse_read(
        "SELECT * FROM user_groups WHERE name ILIKE '%ops%'",
        &catalog(),
    )
    .expect_err("no ILIKE");
    assert!(
        error.message.contains("expected a comparison operator"),
        "{error}"
    );
    let error = parse_read(
        "SELECT * FROM calls WHERE recordingParticipants LIKE '%\"u-2\"%'",
        &catalog(),
    )
    .expect_err("no LIKE");
    assert!(
        error.message.contains("expected a comparison operator"),
        "{error}"
    );
}

/// Gap X: `visibility = PUBLIC OR EXISTS(participants WHERE userId = me)`
/// is the channel-access rule the ACL attaches to tickets, conversations,
/// messages, attachments and channels. The model has no `EXISTS` leaf, so
/// the disjunction cannot be one subscription; it can be two, unioned by
/// the client, which is what this pins: the public branch, the member
/// branch, and a channel in both showing up in both.
#[test]
fn x_exists_inside_or_needs_a_union_of_two_subscriptions() {
    let mut w = World::new();
    w.seed(
        "channels",
        &[("id", "c-pub".into()), ("visibility", "PUBLIC".into())],
    );
    w.seed(
        "channels",
        &[("id", "c-mine".into()), ("visibility", "PRIVATE".into())],
    );
    w.seed(
        "channels",
        &[("id", "c-both".into()), ("visibility", "PUBLIC".into())],
    );
    w.seed(
        "channels",
        &[("id", "c-other".into()), ("visibility", "PRIVATE".into())],
    );
    w.seed(
        "channel_participants",
        &[
            ("id", "p1".into()),
            ("channelId", "c-mine".into()),
            ("userId", ME.into()),
        ],
    );
    w.seed(
        "channel_participants",
        &[
            ("id", "p2".into()),
            ("channelId", "c-both".into()),
            ("userId", ME.into()),
        ],
    );
    w.seed(
        "channel_participants",
        &[
            ("id", "p3".into()),
            ("channelId", "c-other".into()),
            ("userId", "u-2".into()),
        ],
    );

    let public = zql("channels").eq("visibility", "PUBLIC");
    let member = zql("channels").where_exists("participants", |p| p.eq("userId", ME));
    assert_eq!(
        w.subscribe("public", &public),
        ops(["public/main+c-pub", "public/main+c-both"])
    );
    assert_eq!(
        w.subscribe("member", &member),
        ops([
            "member/main+c-mine",
            "member/main+c-both",
            "member/has:participants+p1",
            "member/has:participants+p2"
        ])
    );

    assert_eq!(
        w.delete("channel_participants", "p1"),
        ops(["member/main-c-mine", "member/has:participants-p1"])
    );
    assert_eq!(
        w.update(
            "channels",
            &[("id", "c-pub".into()), ("visibility", "PRIVATE".into())]
        ),
        ops(["public/main-c-pub"])
    );
}

/// Gap O: the parser takes one `ORDER BY` column and the builder drops the
/// tiebreak, so `ORDER BY createdAt DESC, id ASC` orders by `createdAt`
/// alone, and a related node's `ORDER BY … LIMIT 1` ships every related
/// row: the "latest RCA" relation delivers both RCAs.
#[test]
fn o_one_window_column_and_no_limit_below_the_root() {
    let error = parse_read(
        "SELECT * FROM tickets ORDER BY createdAt DESC, id ASC",
        &catalog(),
    )
    .expect_err("one column");
    assert!(
        error.message.contains("only one ORDER BY column"),
        "{error}"
    );
    let paged = zql("tickets")
        .order_by("createdAt", DESC)
        .order_by("id", ASC);
    assert_eq!(paged.dropped_order(), ["id"]);

    let mut w = World::new();
    w.seed("tickets", &[("id", "t1".into())]);
    w.seed(
        "rcas",
        &[
            ("id", "r-old".into()),
            ("ticketId", "t1".into()),
            ("createdAt", 100.into()),
        ],
    );
    w.seed(
        "rcas",
        &[
            ("id", "r-new".into()),
            ("ticketId", "t1".into()),
            ("createdAt", 200.into()),
        ],
    );
    let latest_rca = zql("tickets")
        .eq("id", "t1")
        .related("rcas", |r| r.order_by("createdAt", DESC).limit(1));
    assert_eq!(
        w.subscribe("q", &latest_rca),
        ops(["q/main+t1", "q/rcas+r-old", "q/rcas+r-new"])
    );
}

/// Gap J: the client's `json` columns land in the catalog as strings, so a
/// query can compare a whole document but not look inside it.
#[test]
fn j_json_columns_are_opaque_strings() {
    let catalog = catalog();
    let calls = catalog.table("calls").expect("calls");
    assert_eq!(
        calls
            .column("recordingParticipants")
            .expect("column")
            .r#type,
        ValueType::String
    );
    assert_eq!(
        calls.column("metadata").expect("column").r#type,
        ValueType::String
    );
}

/// Gap E: the client's `whereExists` filters the parent and returns nothing of
/// the child; the engine's RIGHT edge ships the matching child rows as a
/// part of their own (and every matching child, not just those under a
/// visible parent, since the child drives the edge).
#[test]
fn e_where_exists_ships_the_matching_child_rows() {
    let mut w = World::new();
    w.seed(
        "tickets",
        &[("id", "t1".into()), ("isArchived", false.into())],
    );
    w.seed(
        "tickets",
        &[("id", "t2".into()), ("isArchived", true.into())],
    );
    w.seed(
        "ticket_assignments",
        &[
            ("id", "a1".into()),
            ("ticketId", "t1".into()),
            ("userId", "u-2".into()),
        ],
    );
    w.seed(
        "ticket_assignments",
        &[
            ("id", "a2".into()),
            ("ticketId", "t2".into()),
            ("userId", "u-2".into()),
        ],
    );
    let reviewed_by = zql("tickets")
        .eq("isArchived", false)
        .where_exists("assignments", |a| a.eq("userId", "u-2"));
    assert_eq!(
        w.subscribe("q", &reviewed_by),
        ops(["q/main+t1", "q/has:assignments+a1", "q/has:assignments+a2"])
    );
}

/// Gaps S and B: `.one()` is a window of one row, and a window of `n`
/// keeps a doubled buffer, so the client receives up to `2n` rows (here
/// both conversations) and shows the best `n`; a better arrival evicts
/// the worst buffered row.
#[test]
fn s_and_b_one_is_a_window_with_a_buffer_of_two() {
    let mut w = World::new();
    w.seed(
        "conversations",
        &[
            ("conversationId", "c-old".into()),
            ("channelId", "ch".into()),
            ("createdAt", 100.into()),
        ],
    );
    w.seed(
        "conversations",
        &[
            ("conversationId", "c-new".into()),
            ("channelId", "ch".into()),
            ("createdAt", 200.into()),
        ],
    );
    w.seed(
        "conversations",
        &[
            ("conversationId", "c-oldest".into()),
            ("channelId", "ch".into()),
            ("createdAt", 50.into()),
        ],
    );
    let latest = zql("conversations")
        .eq("channelId", "ch")
        .order_by("createdAt", DESC)
        .one();
    assert_eq!(
        w.subscribe("q", &latest),
        ops(["q/main+c-new", "q/main+c-old"])
    );
    assert_eq!(
        w.insert(
            "conversations",
            &[
                ("conversationId", "c-newer".into()),
                ("channelId", "ch".into()),
                ("createdAt", 300.into())
            ]
        ),
        ops(["q/main+c-newer", "q/main-c-old"])
    );
}

/// Rewrite, exact: a keyset cursor `start({createdAt, id}, {inclusive:
/// false})` under `ORDER BY createdAt DESC, id ASC` is the predicate
/// `createdAt < X OR (createdAt = X AND id > Y)`; the inclusive form adds
/// the cursor row itself.
#[test]
fn keyset_cursor_is_the_where_it_means() {
    let mut w = World::new();
    for (id, created) in [("a", 300), ("b", 200), ("c", 200), ("d", 100)] {
        w.seed(
            "tickets",
            &[("id", id.into()), ("createdAt", created.into())],
        );
    }
    let after_b = zql("tickets")
        .order_by("createdAt", DESC)
        .order_by("id", ASC)
        .start(
            &[("createdAt", DESC, 200.into()), ("id", ASC, "b".into())],
            false,
        )
        .limit(10);
    assert_eq!(
        w.subscribe("after", &after_b),
        ops(["after/main+c", "after/main+d"])
    );
    let from_b = zql("tickets")
        .order_by("createdAt", DESC)
        .order_by("id", ASC)
        .start(
            &[("createdAt", DESC, 200.into()), ("id", ASC, "b".into())],
            true,
        )
        .limit(10);
    assert_eq!(
        w.subscribe("from", &from_b),
        ops(["from/main+b", "from/main+c", "from/main+d"])
    );
    let error = parse_read(
        "SELECT * FROM tickets WHERE createdAt < 200 OR (createdAt = 200 AND id > 'b') ORDER BY createdAt DESC LIMIT 10",
        &catalog(),
    );
    assert!(error.is_ok(), "{error:?}");
}

/// Rewrite, exact: the client cannot run `id IN []`, so the queries guard it
/// with a sentinel (`where('id', '__no_match__')`); the engine's empty
/// `IN` is simply false, and nothing ever routes to it.
#[test]
fn empty_in_list_is_false() {
    let mut w = World::new();
    w.seed("tickets", &[("id", "t1".into())]);
    assert_eq!(w.subscribe("q", &zql("tickets").in_("id", &[])), ops([]));
    assert_eq!(w.insert("tickets", &[("id", "t2".into())]), ops([]));
    let with_ids = zql("tickets").in_("id", &["t1", "t9"]);
    assert_eq!(w.subscribe("q2", &with_ids), ops(["q2/main+t1"]));
    assert_eq!(
        w.insert("tickets", &[("id", "t9".into())]),
        ops(["q2/main+t9"])
    );
}

/// Two clients on the same query (the dashboard opens `ticketRowById` per
/// card) share one tree: the second registration touches no storage and
/// is served the shared rows, a write reaches both, and dropping one
/// subscription leaves the other whole.
#[test]
fn identical_queries_share_one_tree() {
    let mut w = World::new();
    w.seed("tickets", &[("id", "t1".into()), ("boardId", "b1".into())]);
    let open = zql("tickets")
        .eq("boardId", "b1")
        .related("assignments", same);
    assert_eq!(w.subscribe("a", &open), ops(["a/main+t1"]));
    assert_eq!(w.subscribe("b", &open), ops(["b/main+t1"]));
    assert_eq!(
        w.insert(
            "ticket_assignments",
            &[
                ("id", "as1".into()),
                ("ticketId", "t1".into()),
                ("userId", ME.into())
            ]
        ),
        ops(["a/assignments+as1", "b/assignments+as1"])
    );
    w.unsubscribe("a");
    assert_eq!(
        w.insert("tickets", &[("id", "t2".into()), ("boardId", "b1".into())]),
        ops(["b/main+t2"])
    );
    assert_eq!(w.rows("b", "assignments"), 1);
}

/// The delta filter `defineQuery` adds for `lastUpdatedAt` is a plain
/// bound, the same on every table; and `.related` on the same row twice
/// (`referencesOut` and `referencesIn` both back to `tickets`) keeps the
/// two parts apart.
#[test]
fn delta_bound_and_self_joins_are_plain() {
    let mut w = World::new();
    w.seed(
        "channels",
        &[("id", "c1".into()), ("updatedAt", 100.into())],
    );
    w.seed(
        "channels",
        &[("id", "c2".into()), ("updatedAt", 300.into())],
    );
    assert_eq!(
        w.subscribe("delta", &zql("channels").where_("updatedAt", GT, 200)),
        ops(["delta/main+c2"])
    );

    w.seed("tickets", &[("id", "t1".into())]);
    w.seed("tickets", &[("id", "t2".into())]);
    w.seed(
        "ticket_reference_mappings",
        &[
            ("id", "r1".into()),
            ("sourceTicketId", "t1".into()),
            ("targetTicketId", "t2".into()),
        ],
    );
    let refs = zql("tickets")
        .eq("id", "t1")
        .related("referencesOut", |r| r.related("targetTicket", same))
        .related("referencesIn", |r| r.related("sourceTicket", same));
    assert_eq!(
        w.subscribe("refs", &refs),
        ops([
            "refs/main+t1",
            "refs/referencesOut+r1",
            "refs/referencesOut.targetTicket+t2"
        ])
    );
    assert_eq!(w.rows("refs", "referencesIn"), 0);
}
