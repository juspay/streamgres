//! The engine client against sockets that misbehave: one that takes
//! every subscribe and never answers, and one that drops each connection
//! the moment it is made. The reads need `DATABASE_URL`.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel_streamgres::cache::{Cache, Limits};
use diesel_streamgres::engine::{Config, EngineClient};
use diesel_streamgres::key::QueryKey;
use diesel_streamgres::{Consistency, Served, SyncedConnection};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

diesel::table! {
    sg_configs (id) {
        id -> Int4,
        key -> Text,
        value -> Text,
    }
}

/// What the fake does with a connection once the handshake is done.
#[derive(Clone, Copy)]
enum Answer {
    /// Greets, reads every frame, answers none.
    Never,
    /// Closes at once.
    Close,
}

/// What a fake server records: when each handshake completed, and every
/// frame received.
type Seen = (Arc<Mutex<Vec<Instant>>>, Arc<Mutex<Vec<serde_json::Value>>>);

/// A WebSocket server on its own thread that behaves one way.
struct Fake {
    url: String,
    accepts: Arc<Mutex<Vec<Instant>>>,
    frames: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl Fake {
    /// Binds a free port and serves until the test ends.
    fn start(answer: Answer) -> Fake {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let listener = runtime
            .block_on(TcpListener::bind("127.0.0.1:0"))
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let accepts = Arc::new(Mutex::new(Vec::new()));
        let frames = Arc::new(Mutex::new(Vec::new()));
        let seen = (Arc::clone(&accepts), Arc::clone(&frames));
        thread::spawn(move || runtime.block_on(serve(listener, answer, seen)));
        Fake {
            url: format!("ws://127.0.0.1:{port}/zero/s2s/v1"),
            accepts,
            frames,
        }
    }

    /// When each connection completed its handshake.
    fn accepts(&self) -> Vec<Instant> {
        self.accepts.lock().unwrap().clone()
    }

    /// Every frame received so far, in order.
    fn frames(&self) -> Vec<serde_json::Value> {
        self.frames.lock().unwrap().clone()
    }

    /// Waits until `count` frames have arrived.
    fn frames_after(&self, count: usize, wait: Duration) -> Vec<serde_json::Value> {
        let deadline = Instant::now() + wait;
        while self.frames.lock().unwrap().len() < count {
            assert!(Instant::now() < deadline, "only {:?}", self.frames());
            thread::sleep(Duration::from_millis(10));
        }
        self.frames()
    }
}

/// Accepts connections forever, each handled by `answer`.
async fn serve(listener: TcpListener, answer: Answer, seen: Seen) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut socket) = tokio_tungstenite::accept_async(stream).await else {
            continue;
        };
        seen.0.lock().unwrap().push(Instant::now());
        if let Answer::Close = answer {
            continue;
        }
        let frames = Arc::clone(&seen.1);
        tokio::spawn(async move {
            let hello = serde_json::json!(["hello", {"protocol": 1}]).to_string();
            if socket.send(Message::Text(hello.into())).await.is_err() {
                return;
            }
            while let Some(Ok(Message::Text(text))) = socket.next().await {
                if let Ok(frame) = serde_json::from_str(text.as_str()) {
                    frames.lock().unwrap().push(frame);
                }
            }
        });
    }
}

/// A client on the fake, connected, with a cache that keeps refusals
/// for the default 30 seconds.
fn client(fake: &Fake, verdict_timeout: Duration) -> (EngineClient, Arc<Cache>) {
    let engine = EngineClient::with_config(
        fake.url.clone(),
        Config {
            connections: 1,
            verdict_timeout,
            ..Config::default()
        },
    );
    let cache = Arc::new(Cache::with_limits(
        Limits::default(),
        Arc::new(engine.clone()),
    ));
    engine.attach(&cache);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !engine.is_connected() {
        assert!(Instant::now() < deadline, "the client did not connect");
        thread::sleep(Duration::from_millis(20));
    }
    (engine, cache)
}

fn url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres:///diesel_streamgres_test?host=/tmp".to_string())
}

/// A row to read, written outside the wrapper, and the wrapper to read
/// it through.
fn reader(cache: Arc<Cache>, key: &str) -> (SyncedConnection<PgConnection>, String) {
    let mut other = PgConnection::establish(&url()).expect("connect");
    other
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS sg_configs (
                id serial PRIMARY KEY, key text NOT NULL UNIQUE, value text NOT NULL)",
        )
        .expect("schema");
    let key = format!("silent-{}-{key}", std::process::id());
    diesel::insert_into(sg_configs::table)
        .values((sg_configs::key.eq(&key), sg_configs::value.eq("v")))
        .on_conflict(sg_configs::key)
        .do_update()
        .set(sg_configs::value.eq("v"))
        .execute(&mut other)
        .expect("put");
    let inner = PgConnection::establish(&url()).expect("connect");
    (
        SyncedConnection::with_cache(inner, cache, Consistency::Eventual),
        key,
    )
}

/// The frame's tag and the id in its body.
fn tagged(frame: &serde_json::Value) -> (&str, u64) {
    (
        frame[0].as_str().unwrap_or(""),
        frame[1]["id"].as_u64().unwrap_or(0),
    )
}

#[test]
fn a_hung_engine_is_given_up_on_in_time_and_asked_again() {
    let fake = Fake::start(Answer::Never);
    let (engine, cache) = client(&fake, Duration::from_millis(300));
    let (mut conn, key) = reader(Arc::clone(&cache), "hung");
    let query = || {
        sg_configs::table
            .filter(sg_configs::key.eq(&key))
            .select(sg_configs::value)
            .limit(1)
    };
    let query_key = QueryKey::of(&query(), conn.inner()).expect("key");

    let started = Instant::now();
    let value: Option<String> = query().first(&mut conn).optional().expect("read");
    let took = started.elapsed();
    assert_eq!(value.as_deref(), Some("v"));
    assert_eq!(conn.last_served(), Some(Served::Postgres), "served live");
    assert!(
        took >= Duration::from_millis(300),
        "waited the timeout: {took:?}"
    );
    assert!(took < Duration::from_secs(3), "not much longer: {took:?}");
    assert_eq!(cache.stats.refused.load(Ordering::Relaxed), 1);
    assert!(
        engine
            .refusals()
            .iter()
            .any(|reason| reason.contains("did not answer in time")),
        "{:?}",
        engine.refusals()
    );
    let frames = fake.frames_after(2, Duration::from_secs(2));
    assert_eq!(tagged(&frames[0]), ("subscribe", 1));
    assert_eq!(
        tagged(&frames[1]),
        ("unsubscribe", 1),
        "the wait withdrew its id"
    );
    assert_eq!(engine.subscribed(), 0);
    assert!(
        cache.refused_recently(&query_key).is_none(),
        "a timeout is not remembered as a refusal"
    );

    let value: Option<String> = query().first(&mut conn).optional().expect("read again");
    assert_eq!(value.as_deref(), Some("v"));
    assert_eq!(conn.last_served(), Some(Served::Postgres));
    assert_eq!(
        cache.stats.refused.load(Ordering::Relaxed),
        2,
        "asked again"
    );
    assert_eq!(cache.stats.refused_recently.load(Ordering::Relaxed), 0);
    let frames = fake.frames_after(4, Duration::from_secs(2));
    assert_eq!(tagged(&frames[2]), ("subscribe", 2));
    assert_eq!(tagged(&frames[3]), ("unsubscribe", 2));
}

/// An engine that greets and then goes silent (killed without a close,
/// a network gone without a reset) is taken for dead once the socket has
/// been quiet for the silence window, and dialled again.
#[test]
fn a_silent_engine_is_taken_for_dead() {
    let fake = Fake::start(Answer::Never);
    let engine = EngineClient::with_config(
        fake.url.clone(),
        Config {
            connections: 1,
            silence: Duration::from_millis(600),
            ..Config::default()
        },
    );
    thread::sleep(Duration::from_millis(2500));
    assert!(
        fake.accepts().len() >= 2,
        "{} connections: the silent one was not given up",
        fake.accepts().len()
    );
    assert!(
        fake.frames().iter().any(|frame| frame[0] == "ping"),
        "the quiet socket was pinged"
    );
    drop(engine);
}

#[test]
fn a_dropped_socket_is_tried_again_every_second() {
    let fake = Fake::start(Answer::Close);
    let engine = EngineClient::with_config(
        fake.url.clone(),
        Config {
            connections: 1,
            ..Config::default()
        },
    );
    thread::sleep(Duration::from_millis(3600));
    let accepts = fake.accepts();
    assert!(
        (3..=5).contains(&accepts.len()),
        "{} connections in 3.6 seconds",
        accepts.len()
    );
    for pair in accepts.windows(2) {
        let gap = pair[1] - pair[0];
        assert!(
            gap >= Duration::from_millis(900) && gap <= Duration::from_millis(2000),
            "a reconnect came after {gap:?}"
        );
    }
    assert!(!engine.is_connected(), "never up for long");
    drop(engine);
}
