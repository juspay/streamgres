# streamgres-diesel

Opt-in Diesel reads through an embedded Streamgres cache:

```rust,ignore
use streamgres_diesel::{LoadCached, Tracker};
conn.set_instrumentation(Tracker::new());
let rows: Vec<Order> = query.load_cached(&cache, &mut conn)?;
```

See the [library guide](../../docs/library-guide.md) for database setup,
consistency, instrumentation, resource budgets, and version support.
Diesel is pinned to 2.3.14.

`FirstCached` provides `first_cached`; `LoadCached` also provides
`get_result_cached`, `optional_cached`, and `inspect_cached` with a refusal
reason. Enable the `async` feature for `AsyncLoadCached` and
`wait_for_writes_async`. See the guide for its custom-bind fallback rule.
