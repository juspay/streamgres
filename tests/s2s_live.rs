//! The server-to-server endpoint spoken to directly, frame by frame,
//! with no client library in between: the greeting, a subscribe in rows
//! mode and in touch mode, the refusals, a change arriving as a delta
//! and as a touch, an unsubscribe that stops the frames, and a frame
//! the endpoint cannot read.
//!
//! Needs a running engine (`STREAMGRES_S2S_URL`) and the database it
//! streams (`STREAMGRES_PG_DSN`); skips itself otherwise. When the engine
//! was started with `STREAMGRES_S2S_TOKEN`, give the test the same one:
//! it then connects with it, and checks that a missing or wrong token is
//! answered 401.

#![cfg(feature = "server")]

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_postgres::{Client, NoTls};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const TABLE: &str = "live_s2s_items";

/// The socket to the endpoint.
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// The endpoint and the database, when both are given.
fn targets() -> Option<(String, String)> {
    let url = std::env::var("STREAMGRES_S2S_URL").ok();
    let dsn = std::env::var("STREAMGRES_PG_DSN").ok();
    match (url, dsn) {
        (Some(url), Some(dsn)) => Some((url, dsn)),
        _ => {
            eprintln!("STREAMGRES_S2S_URL or STREAMGRES_PG_DSN not set: s2s live scenario skipped");
            None
        }
    }
}

/// The upgrade request for `url`, with `token` as a bearer when given.
fn request(url: &str, token: Option<&str>) -> Request {
    let mut request = url.into_client_request().expect("a WebSocket URL");
    if let Some(token) = token {
        let value = format!("Bearer {token}").parse().expect("a header value");
        request.headers_mut().insert(AUTHORIZATION, value);
    }
    request
}

/// The token the engine was started with, if the test was given one.
fn token() -> Option<String> {
    std::env::var("STREAMGRES_S2S_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
}

/// Connects to the database on its own task.
async fn database(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// The table with three rows, `n` 1 to 3, created if the database lacks
/// it. The table stays: dropping it would stop the engine.
async fn seeded(client: &Client) {
    client
        .batch_execute(&format!(
            "CREATE TABLE IF NOT EXISTS {TABLE} (id bigint PRIMARY KEY, name text NOT NULL, n int NOT NULL);
             DELETE FROM {TABLE};
             INSERT INTO {TABLE} (id, name, n) VALUES (1, 'one', 1), (2, 'two', 2), (3, 'three', 3)"
        ))
        .await
        .expect("seed");
}

/// An `int4` bind as the endpoint takes it.
fn int4(value: i32) -> Value {
    json!({"oid": 23, "bytes": BASE64.encode(value.to_be_bytes())})
}

/// Sends one frame.
async fn send(socket: &mut Socket, frame: Value) {
    socket
        .send(Message::Text(frame.to_string().into()))
        .await
        .expect("send");
}

/// The next text frame within `wait` that is not a `lag`, which the
/// engine sends on its own every 500 ms; `None` on a close or when none
/// came.
async fn next(socket: &mut Socket, wait: Duration) -> Option<Value> {
    let deadline = Instant::now() + wait;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let frame = next_any(socket, left).await?;
        if tag(&frame) != "lag" {
            return Some(frame);
        }
    }
}

/// The next text frame within `wait`, a `lag` included; `None` on a
/// close or when none came.
async fn next_any(socket: &mut Socket, wait: Duration) -> Option<Value> {
    let deadline = Instant::now() + wait;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let frame = tokio::time::timeout(left, socket.next()).await.ok()??;
        match frame.expect("frame") {
            Message::Text(text) => return Some(serde_json::from_str(text.as_str()).expect("json")),
            Message::Close(_) => return None,
            _ => continue,
        }
    }
}

/// The next frame, which must arrive.
async fn expect(socket: &mut Socket, what: &str) -> Value {
    next(socket, Duration::from_secs(5))
        .await
        .unwrap_or_else(|| panic!("no frame within five seconds, waiting for {what}"))
}

/// The frame's tag.
fn tag(frame: &Value) -> &str {
    frame[0].as_str().unwrap_or("")
}

/// Subscribes `sql` under `id`, with `rows` asked for or not, retrying
/// under fresh ids while the engine has not yet heard of a table the
/// test just created. Returns the `accepted` body and the id it got.
async fn subscribe_once_known(
    socket: &mut Socket,
    id: &mut u64,
    sql: &str,
    binds: Vec<Value>,
    rows: bool,
) -> (u64, Value) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let this = *id;
        *id += 1;
        send(
            socket,
            json!(["subscribe", {"id": this, "sql": sql, "binds": binds, "rows": rows}]),
        )
        .await;
        let answer = expect(socket, "a verdict").await;
        assert_eq!(answer[1]["id"].as_u64(), Some(this), "{answer}");
        match tag(&answer) {
            "accepted" => return (this, answer[1].clone()),
            "refused"
                if answer[1]["reason"]
                    .as_str()
                    .is_some_and(|reason| reason.contains("unknown table")) =>
            {
                assert!(
                    Instant::now() < deadline,
                    "the engine never heard of {TABLE}: {answer}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            _ => panic!("{answer}"),
        }
    }
}

/// The ids of the rows in a `nodes` entry.
fn ids(node: &Value) -> BTreeSet<i64> {
    node["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row["id"].as_i64().expect("id"))
        .collect()
}

/// Reads frames for up to `wait`, returning them all.
async fn frames_for(socket: &mut Socket, wait: Duration) -> Vec<Value> {
    let deadline = Instant::now() + wait;
    let mut frames = Vec::new();
    while let Some(frame) = next(socket, deadline.saturating_duration_since(Instant::now())).await {
        frames.push(frame);
    }
    frames
}

/// The endpoint speaks its protocol against a live Postgres: `hello`
/// and `pong`; `accepted` with the rows and the query's shape in rows
/// mode, without them otherwise; `refused` with the id and the reason;
/// a commit's `delta` and `touched`; an unsubscribed id no longer told;
/// and `error` for a frame it cannot read.
#[tokio::test]
async fn the_endpoint_speaks_its_protocol() {
    let Some((url, dsn)) = targets() else {
        return;
    };
    let client = database(&dsn).await;
    seeded(&client).await;
    let (mut socket, _) = tokio_tungstenite::connect_async(request(&url, token().as_deref()))
        .await
        .expect("connect");
    let hello = expect(&mut socket, "hello").await;
    assert_eq!(tag(&hello), "hello");
    assert_eq!(hello[1]["protocol"], 1);
    let lag = next_any(&mut socket, Duration::from_secs(5))
        .await
        .expect("a lag frame");
    assert_eq!(tag(&lag), "lag", "the lag comes before any answer: {lag}");
    assert!(lag[1]["ms"].is_u64() || lag[1]["ms"].is_null(), "{lag}");
    assert!(
        hello[1]["database"]["system"].is_string() && hello[1]["database"]["name"].is_string(),
        "the engine names the database it streams: {hello}"
    );
    assert!(
        hello[1]["max_subscriptions"].is_u64() || hello[1]["max_subscriptions"].is_null(),
        "{hello}"
    );

    send(&mut socket, json!(["ping"])).await;
    assert_eq!(expect(&mut socket, "pong").await, json!(["pong"]));

    let mut id = 1;
    let sql =
        format!(r#"SELECT "id", "name", "n" FROM "{TABLE}" WHERE "n" > $1 ORDER BY "id" ASC"#);
    let (rows_id, accepted) =
        subscribe_once_known(&mut socket, &mut id, &sql, vec![int4(1)], true).await;
    let nodes = accepted["nodes"].as_array().expect("nodes");
    assert_eq!(nodes.len(), 1, "one table");
    assert_eq!(nodes[0]["table"].as_str(), Some(TABLE));
    assert_eq!(nodes[0]["key"], json!(["id"]));
    assert_eq!(nodes[0]["parent"], Value::Null);
    assert_eq!(
        ids(&nodes[0]),
        BTreeSet::from([2, 3]),
        "the rows with n above 1"
    );
    assert_eq!(
        accepted["columns"],
        json!([[TABLE, "id"], [TABLE, "name"], [TABLE, "n"]])
    );
    assert_eq!(accepted["names"], json!(["id", "name", "n"]));
    assert_eq!(accepted["count"], json!(false));
    assert_eq!(accepted["order"], json!([["id", "asc"]]));
    assert_eq!(accepted["limit"], Value::Null);
    assert_eq!(accepted["offset"], json!(0));

    let (touch_id, accepted) =
        subscribe_once_known(&mut socket, &mut id, &sql, vec![int4(0)], false).await;
    assert!(
        accepted.get("nodes").is_none(),
        "no rows asked for: {accepted}"
    );

    send(&mut socket, json!(["subscribe", {"id": id, "sql": format!(r#"SELECT "id" FROM "{TABLE}" WHERE "name" ILIKE $1"#), "binds": [{"oid": 25, "bytes": BASE64.encode("o%")}], "rows": true}])).await;
    let refused = expect(&mut socket, "a refusal").await;
    assert_eq!(tag(&refused), "refused", "{refused}");
    assert_eq!(refused[1]["id"].as_u64(), Some(id));
    assert!(
        refused[1]["reason"]
            .as_str()
            .is_some_and(|reason| !reason.is_empty())
    );
    id += 1;

    send(&mut socket, json!(["subscribe", {"id": id, "sql": r#"SELECT "id" FROM "no_such_table_anywhere""#, "binds": [], "rows": false}])).await;
    let refused = expect(&mut socket, "a refusal").await;
    assert_eq!(tag(&refused), "refused", "{refused}");
    assert!(
        refused[1]["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("unknown table")),
        "{refused}"
    );

    client
        .execute(
            &format!("INSERT INTO {TABLE} (id, name, n) VALUES (4, 'four', 4)"),
            &[],
        )
        .await
        .expect("insert");
    let mut delta = None;
    let mut touched = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while (delta.is_none() || touched.is_none()) && Instant::now() < deadline {
        let Some(frame) = next(&mut socket, Duration::from_secs(5)).await else {
            break;
        };
        match tag(&frame) {
            "delta" => delta = Some(frame),
            "touched" => touched = Some(frame),
            _ => panic!("{frame}"),
        }
    }
    let delta = delta.expect("the rows-mode subscription got a delta");
    assert_eq!(delta[1]["id"].as_u64(), Some(rows_id));
    let changes = delta[1]["changes"].as_array().expect("changes");
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0]["table"].as_str(), Some(TABLE));
    assert_eq!(
        changes[0]["put"],
        json!([{"id": 4, "name": "four", "n": 4}])
    );
    assert_eq!(changes[0]["del"], json!([]));
    let touched = touched.expect("the touch-mode subscription was touched");
    assert_eq!(touched[1]["ids"], json!([touch_id]));

    send(&mut socket, json!(["unsubscribe", {"id": rows_id}])).await;
    client
        .execute(
            &format!("UPDATE {TABLE} SET name = 'vier' WHERE id = 4"),
            &[],
        )
        .await
        .expect("update");
    let frames = frames_for(&mut socket, Duration::from_secs(2)).await;
    assert_eq!(frames.len(), 1, "only the touch: {frames:?}");
    assert_eq!(frames[0], json!(["touched", {"ids": [touch_id]}]));

    send(&mut socket, json!(["bogus"])).await;
    let error = expect(&mut socket, "an error").await;
    assert_eq!(tag(&error), "error", "{error}");

    client
        .execute(&format!("DELETE FROM {TABLE} WHERE id = 4"), &[])
        .await
        .expect("delete");
    assert_eq!(
        expect(&mut socket, "the touch for the delete").await,
        json!(["touched", {"ids": [touch_id]}])
    );
    send(&mut socket, json!(["unsubscribe", {"id": touch_id}])).await;
    client
        .execute(
            &format!("UPDATE {TABLE} SET name = 'uno' WHERE id = 1"),
            &[],
        )
        .await
        .expect("update");
    assert!(
        frames_for(&mut socket, Duration::from_secs(1))
            .await
            .is_empty(),
        "nothing is subscribed"
    );
    socket.close(None).await.expect("close");
}

/// An engine started with a token answers 401 to an upgrade without it
/// or with another one.
#[tokio::test]
async fn a_token_guards_the_endpoint() {
    let Some((url, _)) = targets() else {
        return;
    };
    let Some(token) = token() else {
        eprintln!("STREAMGRES_S2S_TOKEN not set: the token check is skipped");
        return;
    };
    let wrong = format!("{token}-not");
    for presented in [None, Some(wrong.as_str())] {
        match tokio_tungstenite::connect_async(request(&url, presented)).await {
            Err(Error::Http(response)) => assert_eq!(response.status(), 401),
            Err(other) => panic!("expected a 401, got {other}"),
            Ok(_) => panic!("the endpoint took {presented:?}"),
        }
    }
}
