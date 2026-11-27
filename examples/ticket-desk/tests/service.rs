//! The service over real HTTP against a live Postgres: every shape, writes
//! read back from memory, inspection, verification, load, and every lab
//! experiment.
//!
//! Set `TICKETDESK_TEST_DSN` to a disposable database with
//! `wal_level = logical`; the test resets the demo's data.

use std::time::{Duration, Instant};

use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
use serde_json::{Value, json};
use streamgres_demo::migrate::Step;
use streamgres_demo::seed;
use streamgres_demo::service::{self, Options};

async fn get(client: &reqwest::Client, base: &str, path: &str) -> Value {
    let response = client.get(format!("{base}{path}")).send().await.unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "GET {path}: {status} {body}");
    serde_json::from_str(&body).unwrap()
}

async fn post(client: &reqwest::Client, base: &str, path: &str, value: Value) -> Value {
    let response = client
        .post(format!("{base}{path}"))
        .json(&value)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "POST {path}: {status} {body}");
    serde_json::from_str(&body).unwrap()
}

/// Reads shape `name` until it is answered as `served`, for at most ten
/// seconds: a query's first reads go to Postgres while it loads.
async fn until_served(client: &reqwest::Client, base: &str, name: &str, served: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let answer = get(client, base, &format!("/api/query/{name}")).await;
        if answer["served"] == served {
            return answer;
        }
        assert!(
            Instant::now() < deadline,
            "{name} never served by {served}: {answer}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_desk_over_http() {
    let Ok(dsn) = std::env::var("TICKETDESK_TEST_DSN") else {
        eprintln!("TICKETDESK_TEST_DSN not set: live HTTP scenario skipped");
        return;
    };
    {
        let mut conn = AsyncPgConnection::establish(&dsn).await.unwrap();
        conn.batch_execute(seed::SCHEMA).await.unwrap();
        Step::Drop.run(&mut conn).await.unwrap();
    }
    let options = Options {
        listen: "127.0.0.1:0".parse().unwrap(),
        lab: true,
        pool_size: 4,
        writer_rate: 0,
        max_bytes: 256 << 20,
        max_lag_ms: 2000,
        seed: true,
        init: false,
    };
    let app = service::start(dsn.clone(), options).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(
        axum::serve(listener, service::router(app.clone()).into_make_service_with_connect_info::<std::net::SocketAddr>())
            .with_graceful_shutdown(async {
                let _ = stop_rx.await;
            })
            .into_future(),
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    assert!(
        client
            .get(&base)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .contains("Streamgres")
    );

    // Every shape answers, through Streamgres as directly, and each is
    // answered by whom it is expected to be once its query has loaded.
    let shapes = get(&client, &base, "/api/lookups").await;
    assert_eq!(shapes["count"], 25);
    for shape in shapes["shapes"].as_array().unwrap() {
        assert!(shape.get("error").is_none(), "{shape}");
        let name = shape["name"].as_str().unwrap();
        let expected = shape["expected"].as_str().unwrap();
        let answer = until_served(&client, &base, name, expected).await;
        if expected == "unsupported" {
            assert!(answer["data"].is_null(), "{answer}");
            continue;
        }
        if expected == "postgres" {
            assert!(
                answer["refusal"]
                    .as_str()
                    .is_some_and(|reason| !reason.is_empty()),
                "{answer}"
            );
        }
        let direct = get(&client, &base, &format!("/api/query/{name}?route=postgres")).await;
        assert_eq!(answer["data"], direct["data"], "{name}");
    }

    // Inspection sees the kept queries and who read them.
    let inspected = get(&client, &base, "/api/inspect/queue_cards").await;
    assert_eq!(inspected["state"], "Ready", "{inspected}");
    assert!(
        inspected["callers"]
            .as_array()
            .is_some_and(|callers| !callers.is_empty())
    );
    let refused = get(&client, &base, "/api/inspect/search").await;
    assert!(
        refused["state"]
            .as_str()
            .unwrap()
            .starts_with("Unsupported"),
        "{refused}"
    );
    let memory = get(&client, &base, "/api/memory").await;
    assert!(memory["queries"].as_array().unwrap().len() >= 4, "{memory}");
    let organizations = get(&client, &base, "/api/organizations").await;
    assert_eq!(organizations.as_array().unwrap().len(), 3);

    // A write is read back from memory.
    let created = post(
        &client,
        &base,
        "/tickets?wait=true",
        json!({"org_id": 1, "title": "HTTP integration ticket", "priority": "high"}),
    )
    .await;
    assert_eq!(created["committed"], true);
    assert_eq!(created["visible"], true, "{created}");
    let id = created["id"].as_i64().unwrap();
    let changed = post(
        &client,
        &base,
        &format!("/tickets/{id}/status?wait=true"),
        json!({"status": "pending"}),
    )
    .await;
    assert_eq!(changed["visible"], true, "{changed}");
    let comment = post(
        &client,
        &base,
        "/tickets/1/comments?wait=true",
        json!({"body": "from the test"}),
    )
    .await;
    assert_eq!(comment["visible"], true, "{comment}");
    let missing = client
        .post(format!("{base}/tickets/999999999/status"))
        .json(&json!({"status": "open"}))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);

    // Verification and load.
    let verified = post(&client, &base, "/api/verify", json!({})).await;
    assert_eq!(verified["mismatches"], 0, "{verified}");
    assert_eq!(verified["errors"], 0, "{verified}");
    let load = post(
        &client,
        &base,
        "/api/load",
        json!({"name": "queue_cards", "concurrency": 2, "seconds": 1}),
    )
    .await;
    assert!(load["cached"]["memory"].as_u64().unwrap() > 0, "{load}");
    assert_eq!(load["postgres"]["memory"], 0);

    // Every experiment completes.
    for scenario in [
        "widen",
        "new-type",
        "drop",
        "connection",
        "slot",
        "transaction",
    ] {
        let result = post(&client, &base, &format!("/api/lab/{scenario}"), json!({})).await;
        assert_eq!(result["ok"], true, "{scenario}: {result}");
    }

    let instance = get(&client, &base, "/api/metrics").await["engine"]["instance"].clone();
    let _ = stop_tx.send(());
    let _ = server.await;
    app.shutdown().await;
    let mut conn = AsyncPgConnection::establish(&dsn).await.unwrap();
    let remaining: i64 = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query(
            "SELECT count(*) AS count FROM pg_replication_slots WHERE slot_name = $1",
        )
        .bind::<diesel::sql_types::Text, _>(instance.as_str().unwrap_or_default()),
        &mut conn,
    )
    .await
    .map(|row: Count| row.count)
    .unwrap();
    assert_eq!(remaining, 0, "shutting down dropped the slot");

    // With no cache budget every read falls back. A committed value from
    // Postgres must not satisfy the promise to wait for memory visibility.
    let app = service::start(
        dsn.clone(),
        Options {
            listen: "127.0.0.1:0".parse().unwrap(),
            lab: false,
            pool_size: 2,
            writer_rate: 0,
            max_bytes: 0,
            max_lag_ms: 2000,
            seed: false,
            init: false,
        },
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, service::router(app.clone()).into_make_service_with_connect_info::<std::net::SocketAddr>()).into_future());
    let answer = post(
        &client,
        &base,
        "/tickets?wait=true",
        json!({"org_id":1, "title":"Fallback does not prove memory visibility"}),
    )
    .await;
    assert_eq!(answer["committed"], true, "{answer}");
    assert_eq!(answer["waited"], true, "{answer}");
    assert_eq!(answer["visible"], false, "{answer}");
    assert!(answer["visible_after_us"].as_f64().unwrap() >= 5_000_000.0);
    server.abort();
    let _ = server.await;
    app.shutdown().await;
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}
