//! The read queries of juspay/xyne-spaces, each rebuilt on the engine and
//! exercised end to end: the synced-query registry the dashboard subscribes
//! through (the backend's `queries.ts`, 283 queries at v1.316.4,
//! commit f80fd19) plus the access-control predicates `defineQuery` adds to
//! every one of them. One test per registry entry, named after it, in the
//! domain modules; the catalog is generated from the application's schema.
//!
//! Every test builds the closest query the model can express, registers it
//! over seeded storage, asserts the snapshot, then routes writes and asserts
//! the deltas. Where a query needs something the engine does not have, the
//! test keeps the expressible part and its doc names the gap by letter, so
//! the assertion shows the current behavior and flips when the gap closes.
//! `gaps.rs` pins each gap on its own.
//!
//! | Gap | What the queries use | What the engine has today |
//! |-----|----------------------|---------------------------|
//! | N | `IS NULL` / `IS NOT NULL` (74 + 9 sites: `visibleTo IS NULL`, `rootId IS NULL`, `userId IS NULL`, `deletedAt IS NULL`, …) | no `IS` operator; every comparison touching `NULL` is false, so a `NULL` row can only be excluded, never selected |
//! | L | `LIKE` / `ILIKE` (11 sites: searches over `name`, `title`, `xyneId`, and a JSON text) | no pattern operator; the parser refuses it |
//! | X | an existence test inside `OR` (canvas visibility, `browsableChannels`, `channelLinks`, `summaryTemplates`, `getUsers`, the channel-access ACL `visibility = PUBLIC OR EXISTS participants`, the calls ACL) | an existence test is a RIGHT edge, conjoined with the node's filter; `Where` has no `EXISTS` leaf |
//! | O | a second `ORDER BY` column (tiebreaks on `id`, 100+ sites) and `ORDER BY` / `LIMIT` inside `related` (`rcas` latest one, last 10 `conversations`) | one window column per query; a node below the root ships every matching row, unordered and uncapped |
//! | J | `json` columns (`metadata`, `recordingParticipants` searched with `LIKE`) | carried as opaque strings; no containment or path operators |
//! | E | `whereExists` returns no child rows | a RIGHT edge ships the matching child rows as their own part (more data, same parent set) |
//! | S | `.one()` is a singular result | `LIMIT 1`; the singular shape is the transport's |
//! | B | `LIMIT n` ships `n` rows | the window keeps a doubled buffer: up to `2n` rows are shipped and the client shows the best `n` |
//!
//! Keyset cursors (`.start(row, {inclusive})`, 34 sites) need no engine
//! change: the builder spells them as the `WHERE` they mean, and the tests
//! show the rewrite is exact. `{flip: true}` hints are planner advice with
//! no meaning here.

/// A row as `(column, value)` pairs with every value converted:
/// `row!["id" => "t1", "createdAt" => 100]`.
macro_rules! row {
    ($($column:expr => $value:expr),* $(,)?) => {
        &[$(($column, ::jus_sync::model::Value::from($value))),*][..]
    };
}

mod acl;
mod activities;
mod boards_forms;
mod calls;
mod canvases;
mod catalog;
mod channels;
mod conversations;
mod gaps;
mod people;
mod support_desk;
mod tickets;
mod world;
mod zql;
