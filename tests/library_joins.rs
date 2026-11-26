//! Library-only join translation compared with PostgreSQL before and after WAL updates.

use std::time::{Duration, Instant};
use streamgres::cache::{Rows, Served, Sql, Streamgres};
use tokio_postgres::types::{FromSql, Type};

#[derive(Debug, PartialEq, Eq)]
struct Answer {
    columns: Vec<String>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
}
struct Cell(Option<Vec<u8>>);
impl<'a> FromSql<'a> for Cell {
    fn from_sql(
        _: &Type,
        bytes: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self(Some(bytes.to_vec())))
    }
    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self(None))
    }
    fn accepts(_: &Type) -> bool {
        true
    }
}
fn answer(rows: Rows) -> Answer {
    Answer {
        columns: rows.columns().iter().map(|c| c.name().to_owned()).collect(),
        rows: rows
            .iter()
            .map(|r| {
                (0..rows.columns().len())
                    .map(|c| r.bytes(c).map(<[u8]>::to_vec))
                    .collect()
            })
            .collect(),
    }
}
async fn postgres(client: &tokio_postgres::Client, sql: &str) -> Answer {
    let statement = client.prepare(sql).await.expect(sql);
    let rows = client.query(&statement, &[]).await.expect(sql);
    Answer {
        columns: statement
            .columns()
            .iter()
            .map(|c| c.name().to_owned())
            .collect(),
        rows: rows
            .iter()
            .map(|r| (0..r.len()).map(|c| r.get::<_, Cell>(c).0).collect())
            .collect(),
    }
}
async fn matches_memory(lib: &Streamgres, client: &tokio_postgres::Client, sql: &str) {
    let expected = postgres(client, sql).await;
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let (rows, trace) = lib.load_and_sync(Sql::new(sql)).inspect().await;
        if trace.served == Served::Memory
            && let Ok(rows) = rows
        {
            let found = answer(rows);
            if found == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{sql}\nexpected {expected:?}\nfound {found:?}\n{trace:?}"
            );
        } else {
            assert!(Instant::now() < deadline, "{sql}: {trace:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn library_join_plans_match_postgres_and_follow_writes() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    // `departments.name` is ordered from memory, which needs a byte-order collation.
    client.batch_execute("DROP PUBLICATION IF EXISTS lib_joins_pub; DROP SCHEMA IF EXISTS lib_joins CASCADE; CREATE SCHEMA lib_joins;
        CREATE TABLE lib_joins.departments(id bigint PRIMARY KEY,org bigint,name text COLLATE \"C\");
        CREATE TABLE lib_joins.people(id bigint PRIMARY KEY,org bigint,dept bigint,manager bigint,active boolean,name text);
        CREATE TABLE lib_joins.notes(id bigint PRIMARY KEY,person_id bigint,kind text);
        CREATE TABLE lib_joins.keys_a(aid bigint PRIMARY KEY,k1 bigint,k2 bigint,av text);
        CREATE TABLE lib_joins.keys_b(bid bigint PRIMARY KEY,k1 bigint,k2 bigint,bv text);
        INSERT INTO lib_joins.departments VALUES(1,10,'sales'),(2,20,'engineering'),(3,30,'empty');
        INSERT INTO lib_joins.people VALUES(1,10,1,NULL,true,'alice'),(2,10,1,1,false,'bob'),(3,20,2,1,true,'carol'),(4,10,NULL,2,false,'dave'),(5,10,99,NULL,true,'eve');
        INSERT INTO lib_joins.notes VALUES(1,1,'a'),(2,1,'b'),(3,3,'c'),(4,99,'orphan'),(5,NULL,'null');
        INSERT INTO lib_joins.keys_a VALUES(1,1,1,'a'),(2,2,2,'b'),(3,NULL,1,'n');
        INSERT INTO lib_joins.keys_b VALUES(10,1,1,'x'),(11,3,3,'y'),(12,NULL,1,'n');
        CREATE PUBLICATION lib_joins_pub FOR TABLES IN SCHEMA lib_joins;").await.unwrap();
    let builder = Streamgres::connect(&dsn)
        .schemas(["lib_joins"])
        .publication("lib_joins_pub")
        .app("libjoins", 0)
        .max_lag(Duration::from_secs(5));
    client.batch_execute(&builder.setup_sql()).await.unwrap();
    let lib = builder.start().await.unwrap();
    let cases = [
        "SELECT p.id,d.name FROM lib_joins.people AS p JOIN lib_joins.departments AS d ON p.dept=d.id ORDER BY p.id",
        "SELECT p.* FROM lib_joins.people p JOIN lib_joins.departments d ON d.id=p.dept ORDER BY p.id",
        "SELECT e.id,m.name FROM lib_joins.people e LEFT JOIN lib_joins.people m ON e.manager=m.id ORDER BY e.id",
        "SELECT e.id,m.id FROM lib_joins.people e JOIN lib_joins.people m ON e.manager=m.id WHERE e.name='bob' AND m.name='alice' ORDER BY e.id",
        "SELECT p.id,d.id FROM lib_joins.people p RIGHT JOIN lib_joins.departments d ON p.dept=d.id ORDER BY d.id,p.id",
        "SELECT p.*,d.* FROM lib_joins.people p RIGHT JOIN lib_joins.departments d ON p.dept=d.id ORDER BY d.id,p.id",
        "SELECT d.id,p.id,n.id FROM lib_joins.departments d LEFT JOIN lib_joins.people p ON d.id=p.dept RIGHT JOIN lib_joins.notes n ON p.id=n.person_id ORDER BY n.id,p.id",
        "SELECT d.id,p.id,n.id FROM lib_joins.departments d RIGHT JOIN (lib_joins.people p JOIN lib_joins.notes n ON p.id=n.person_id) ON d.id=p.dept ORDER BY n.id,p.id",
        "SELECT p.id,d.name FROM lib_joins.people p JOIN lib_joins.departments d ON p.org=d.org AND p.dept=d.id ORDER BY p.id",
        "SELECT p.id,d.name FROM lib_joins.people p JOIN lib_joins.departments d ON (p.active=true AND (d.id=p.dept AND p.org=d.org)) ORDER BY p.id",
        "SELECT p.id,d.name FROM lib_joins.people p LEFT JOIN lib_joins.departments d ON d.name='sales' AND p.dept=d.id ORDER BY p.id",
        "SELECT d.id,p.id FROM lib_joins.departments d LEFT JOIN lib_joins.people p ON d.id=p.dept WHERE p.id IS NULL ORDER BY d.id",
        "SELECT d.id,p.id FROM lib_joins.departments d LEFT JOIN lib_joins.people p ON d.id=p.dept WHERE p.active=false ORDER BY d.id,p.id",
        "SELECT d.id,p.id FROM lib_joins.departments d LEFT JOIN lib_joins.people p ON d.id=p.dept WHERE p.id IS NOT NULL ORDER BY d.id,p.id",
        "SELECT d.id,p.id,n.id FROM lib_joins.departments d LEFT JOIN lib_joins.people p ON d.id=p.dept INNER JOIN lib_joins.notes n ON p.id=n.person_id ORDER BY d.id,p.id,n.id",
        "SELECT d.id,p.id,n.id FROM lib_joins.departments d LEFT JOIN (lib_joins.people p INNER JOIN lib_joins.notes n ON p.id=n.person_id) ON d.id=p.dept WHERE n.id IS NULL ORDER BY d.id,p.id,n.id",
        "SELECT p.id,d.name FROM lib_joins.people p JOIN lib_joins.departments d ON p.dept=d.id ORDER BY d.name,p.id LIMIT 2 OFFSET 1",
        "SELECT p.id,d.name FROM lib_joins.people p RIGHT JOIN lib_joins.departments d ON p.dept=d.id ORDER BY p.id,d.id LIMIT 3",
        "SELECT COUNT(*) FROM lib_joins.departments d LEFT JOIN lib_joins.people p ON d.id=p.dept WHERE p.id IS NULL",
        "SELECT * FROM lib_joins.keys_a a JOIN lib_joins.keys_b b USING(k1,k2) ORDER BY a.aid,b.bid",
        "SELECT * FROM lib_joins.keys_a a LEFT JOIN lib_joins.keys_b b USING(k1,k2) ORDER BY a.aid,b.bid",
        "SELECT * FROM lib_joins.keys_a a RIGHT JOIN lib_joins.keys_b b USING(k1,k2) ORDER BY a.aid,b.bid",
        "SELECT k1,k2,av,bv FROM lib_joins.keys_a LEFT JOIN lib_joins.keys_b USING(k1,k2) ORDER BY aid,bid",
        "SELECT * FROM lib_joins.keys_a a NATURAL JOIN lib_joins.keys_b b ORDER BY a.aid,b.bid",
        "SELECT * FROM lib_joins.keys_a a NATURAL LEFT JOIN lib_joins.keys_b b ORDER BY a.aid,b.bid",
        "SELECT * FROM lib_joins.keys_a a NATURAL RIGHT JOIN lib_joins.keys_b b ORDER BY a.aid,b.bid",
    ];
    let mut matrix = Vec::new();
    for first in ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN"] {
        for second in ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN"] {
            for key in ["p.id", "d.id"] {
                matrix.push(format!("SELECT d.id,p.id,n.id FROM lib_joins.departments d {first} lib_joins.people p ON d.id=p.dept {second} lib_joins.notes n ON {key}=n.person_id ORDER BY d.id,p.id,n.id"));
            }
            matrix.push(format!("SELECT d.id,p.id,n.id FROM lib_joins.departments d {first} (lib_joins.people p {second} lib_joins.notes n ON p.id=n.person_id) ON d.id=p.dept ORDER BY d.id,p.id,n.id"));
        }
    }
    for sql in cases {
        matches_memory(&lib, &client, sql).await;
    }
    for sql in &matrix {
        matches_memory(&lib, &client, sql).await;
    }
    // Two distinct aliases bind their filters independently, even though
    // the engine reads the same physical table for both occurrences.
    let sql=Sql::new("SELECT e.id,m.id FROM lib_joins.people e JOIN lib_joins.people m ON e.manager=m.id WHERE e.name=$1 AND m.name=$2 ORDER BY e.id").bind("bob").bind("alice");
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let (rows, trace) = lib.load_and_sync(sql.clone()).inspect().await;
        if trace.served == Served::Memory {
            assert_eq!(rows.unwrap().len(), 1);
            break;
        }
        assert!(Instant::now() < deadline, "{trace:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for (query, expected_sql) in [
        (Sql::new("SELECT d.id,p.id FROM lib_joins.departments d LEFT JOIN lib_joins.people p ON d.id=p.dept WHERE p.active=$1 ORDER BY d.id,p.id").bind(false),
            "SELECT d.id,p.id FROM lib_joins.departments d LEFT JOIN lib_joins.people p ON d.id=p.dept WHERE p.active=false ORDER BY d.id,p.id"),
        (Sql::new("SELECT d.id,p.id FROM lib_joins.departments d LEFT JOIN lib_joins.people p ON d.id=p.dept WHERE p.active=$1 ORDER BY d.id,p.id").bind(true),
            "SELECT d.id,p.id FROM lib_joins.departments d LEFT JOIN lib_joins.people p ON d.id=p.dept WHERE p.active=true ORDER BY d.id,p.id"),
        (Sql::new("SELECT p.id,d.name FROM lib_joins.people p JOIN lib_joins.departments d ON p.dept=d.id ORDER BY d.name,p.id LIMIT $1 OFFSET $2").bind(2i64).bind(1i64),
            "SELECT p.id,d.name FROM lib_joins.people p JOIN lib_joins.departments d ON p.dept=d.id ORDER BY d.name,p.id LIMIT 2 OFFSET 1"),
    ] {
        let expected=postgres(&client,expected_sql).await;
        let deadline=Instant::now()+Duration::from_secs(8);
        loop {
            let (rows,trace)=lib.load_and_sync(query.clone()).inspect().await;
            if trace.served==Served::Memory {
                assert_eq!(answer(rows.unwrap()),expected,"{}",query.text());break;
            }
            assert!(Instant::now()<deadline,"{trace:?}");tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    client
        .batch_execute(
            "UPDATE lib_joins.people SET org=30,dept=3,manager=2,active=false WHERE id=3;
        INSERT INTO lib_joins.notes VALUES(6,2,'d');
        DELETE FROM lib_joins.notes WHERE id=1;
        UPDATE lib_joins.keys_b SET k1=2,k2=2 WHERE bid=11;
        INSERT INTO lib_joins.keys_b VALUES(13,1,1,'z');",
        )
        .await
        .unwrap();
    for sql in cases {
        matches_memory(&lib, &client, sql).await;
    }
    for sql in &matrix {
        matches_memory(&lib, &client, sql).await;
    }
    lib.shutdown().await;
    // A global join window cannot be answered from a truncated root page.
    // If the complete candidate relation exceeds the budget, use Postgres.
    let lib = Streamgres::connect(&dsn)
        .schemas(["lib_joins"])
        .publication("lib_joins_pub")
        .app("libjoins", 0)
        .max_query_size(32 << 20, 2)
        .start()
        .await
        .unwrap();
    let sql = "SELECT p.id,d.name FROM lib_joins.people p JOIN lib_joins.departments d ON p.dept=d.id ORDER BY d.name,p.id LIMIT 1";
    let expected = postgres(&client, sql).await;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (rows, trace) = lib.load_and_sync(Sql::new(sql)).inspect().await;
        assert_eq!(answer(rows.unwrap()), expected);
        assert_ne!(
            trace.served,
            Served::Memory,
            "an incomplete global window must not be served"
        );
        if trace
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("budget"))
        {
            break;
        }
        assert!(Instant::now() < deadline, "{trace:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    lib.shutdown().await;
    // The trigger stack's DDL triggers would fire for every later test.
    client
        .batch_execute(
            "DROP PUBLICATION lib_joins_pub; DROP SCHEMA lib_joins CASCADE;
             DROP SCHEMA libjoins_0 CASCADE;",
        )
        .await
        .unwrap();
}
