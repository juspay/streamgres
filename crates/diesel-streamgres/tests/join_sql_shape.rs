//! What Diesel renders for joins: the engine's parser is written against
//! these exact shapes, so a change in Diesel's rendering shows up here.
use diesel::pg::Pg;
use diesel::prelude::*;

diesel::table! { authors (id) { id -> Int4, name -> Text } }
diesel::table! { books (id) { id -> Int4, author_id -> Int4, title -> Text } }
diesel::table! { reviews (id) { id -> Int4, book_id -> Int4, stars -> Int4 } }
diesel::joinable!(books -> authors (author_id));
diesel::joinable!(reviews -> books (book_id));
diesel::allow_tables_to_appear_in_same_query!(authors, books, reviews);

/// Diesel renders an inner join, a left join, a nested join and an
/// explicit `ON` in the shapes the engine's parser reads.
#[test]
fn diesel_renders_joins_as_the_engine_expects() {
    let inner = authors::table
        .inner_join(books::table)
        .filter(authors::name.eq("x"))
        .select((authors::id, books::title))
        .order(authors::id.desc())
        .limit(5);
    assert_eq!(
        diesel::debug_query::<Pg, _>(&inner).to_string(),
        r#"SELECT "authors"."id", "books"."title" FROM ("authors" INNER JOIN "books" ON ("books"."author_id" = "authors"."id")) WHERE ("authors"."name" = $1) ORDER BY "authors"."id" DESC LIMIT $2 -- binds: ["x", 5]"#
    );
    let left = authors::table.left_join(books::table).select((authors::name, books::title.nullable()));
    assert_eq!(
        diesel::debug_query::<Pg, _>(&left).to_string(),
        r#"SELECT "authors"."name", "books"."title" FROM ("authors" LEFT OUTER JOIN "books" ON ("books"."author_id" = "authors"."id")) -- binds: []"#
    );
    let three = authors::table.inner_join(books::table.inner_join(reviews::table)).select((authors::name, reviews::stars));
    assert_eq!(
        diesel::debug_query::<Pg, _>(&three).to_string(),
        r#"SELECT "authors"."name", "reviews"."stars" FROM ("authors" INNER JOIN ("books" INNER JOIN "reviews" ON ("reviews"."book_id" = "books"."id")) ON ("books"."author_id" = "authors"."id")) -- binds: []"#
    );
    let explicit = authors::table.inner_join(books::table.on(books::author_id.eq(authors::id).and(books::title.eq("t")))).select(authors::id);
    assert_eq!(
        diesel::debug_query::<Pg, _>(&explicit).to_string(),
        r#"SELECT "authors"."id" FROM ("authors" INNER JOIN "books" ON (("books"."author_id" = "authors"."id") AND ("books"."title" = $1))) -- binds: ["t"]"#
    );
}
