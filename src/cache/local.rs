//! One bounded answer per thread. A writer publishes a new revision before
//! releasing the state lock, so a local answer cannot survive a visible
//! commit, eviction, schema change or stop. Freshness expires independently
//! of writes. Copies have thread-local Arc counters to avoid hot-key contention.
use super::{CachedRows, Reader};
use crate::parser::Bind;
use std::cell::RefCell;
use std::sync::Arc;
use std::time::Instant;
const MAX_BYTES: usize = 64 << 10;
struct Answer {
    identity: Arc<()>,
    revision: u64,
    expires: Instant,
    sql: Box<str>,
    binds: Vec<Bind>,
    reader: Reader,
    rows: CachedRows,
}
thread_local! { static ANSWER: RefCell<Option<Answer>> = const { RefCell::new(None) }; }
pub(super) struct Key<'a> {
    pub identity: &'a Arc<()>,
    pub revision: u64,
    pub sql: &'a str,
    pub binds: &'a [Bind],
    pub reader: &'a Reader,
}
pub(super) fn get(key: &Key<'_>) -> Option<CachedRows> {
    ANSWER.with(|answer| {
        let answer = answer.borrow();
        let answer = answer.as_ref()?;
        (Arc::ptr_eq(&answer.identity, key.identity)
            && answer.revision == key.revision
            && Instant::now() < answer.expires
            && &*answer.sql == key.sql
            && answer.binds == key.binds
            && answer.reader == *key.reader)
            .then(|| answer.rows.clone())
    })
}
pub(super) fn keep(key: Key<'_>, expires: Instant, rows: &CachedRows, budget: usize) {
    let Key {
        identity,
        revision,
        sql,
        binds,
        reader,
    } = key;
    let keys = sql.len().saturating_add(
        binds
            .iter()
            .map(|bind| 32 + bind.bytes.as_ref().map_or(0, Vec::len))
            .sum::<usize>(),
    );
    let Some(budget) = budget.min(MAX_BYTES).checked_sub(keys) else {
        return;
    };
    let Some(rows) = rows.local_copy(budget) else {
        return;
    };
    ANSWER.with(|answer| {
        *answer.borrow_mut() = Some(Answer {
            identity: identity.clone(),
            revision,
            expires,
            sql: sql.into(),
            binds: binds.to_vec(),
            reader: reader.clone(),
            rows,
        })
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::CatalogVersion;
    use crate::model::{Catalog, DbColumn, DbTable, ValueType};
    #[test]
    fn local_answers_require_identity_revision_reader_and_a_live_lease() {
        let catalog = Arc::new(Catalog::new(vec![DbTable::new(
            "t",
            ["id"],
            vec![DbColumn::new("id", ValueType::Int)],
        )]));
        let reader = Reader::new("1", "app", "", CatalogVersion::of(&catalog));
        let excluded = Reader::new("1", "app", "t", CatalogVersion::of(&catalog));
        let identity = Arc::new(());
        let other = Arc::new(());
        let select = crate::parser::parse_select("SELECT COUNT(*) FROM t", &[], &catalog).unwrap();
        let rows = crate::cache::rows::count(1, &select).unwrap();
        let make_key = || Key {
            identity: &identity,
            revision: 7,
            sql: "SELECT COUNT(*) FROM t",
            binds: &[],
            reader: &reader,
        };
        keep(
            make_key(),
            Instant::now() + std::time::Duration::from_secs(1),
            &rows,
            4096,
        );
        assert!(get(&make_key()).is_some());
        assert!(
            get(&Key {
                identity: &other,
                ..make_key()
            })
            .is_none()
        );
        assert!(
            get(&Key {
                revision: 8,
                ..make_key()
            })
            .is_none()
        );
        assert!(
            get(&Key {
                reader: &excluded,
                ..make_key()
            })
            .is_none()
        );
        assert!(
            get(&Key {
                sql: "SELECT id FROM t",
                ..make_key()
            })
            .is_none()
        );
        keep(make_key(), Instant::now(), &rows, 4096);
        assert!(
            get(&make_key()).is_none(),
            "freshness expires without another write"
        );
    }
}
