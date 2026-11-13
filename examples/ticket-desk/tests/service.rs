//! Real HTTP and database checks. Use a disposable logical-WAL database.
use diesel::connection::SimpleConnection;
use diesel::{Connection, PgConnection};
use serde_json::{Value, json};
use std::time::Duration;
use streamgres_demo::{
    migrate::Step,
    seed,
    service::{self, Options},
};

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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_queries_writes_verification_and_recovery_are_real() {
    let Ok(dsn) = std::env::var("TICKETDESK_TEST_DSN") else {
        eprintln!("TICKETDESK_TEST_DSN not set: live HTTP scenario skipped");
        return;
    };
    let reset = dsn.clone();
    tokio::task::spawn_blocking(move || {
        let mut conn = PgConnection::establish(&reset).unwrap();
        conn.batch_execute(seed::SCHEMA).unwrap();
        Step::Drop.run(&mut conn).unwrap();
        conn.batch_execute("ALTER TABLE attachments DISABLE ROW LEVEL SECURITY; DROP SCHEMA IF EXISTS demo_tenant CASCADE; DROP FUNCTION IF EXISTS public.demo_set_tenant();").unwrap();
    }).await.unwrap();
    let options = Options {
        listen: "127.0.0.1:0".parse().unwrap(),
        lab: true,
        pool_size: 4,
        writer_rate: 0,
        max_bytes: 256 << 20,
        max_lag_ms: 2000,
        admit_after: 1,
        seed: true,
        init: false,
    };
    let app = service::start(dsn.clone(), options).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        axum::serve(listener, service::router(app.clone()))
            .with_graceful_shutdown(async {
                let _ = stop_rx.await;
            })
            .into_future(),
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(45))
        .build()
        .unwrap();
    let page = client.get(&base).send().await.unwrap();
    assert!(page.text().await.unwrap().contains("Fast reads."));
    let shapes = get(&client, &base, "/api/lookups").await;
    assert_eq!(shapes["count"], 23);
    for shape in shapes["shapes"].as_array().unwrap() {
        assert!(shape.get("error").is_none(), "{shape}");
        assert_eq!(shape["served"], shape["expected"], "{shape}");
    }
    for definition in shapes["shapes"].as_array().unwrap() {
        let shape = definition["name"].as_str().unwrap();
        let answer = get(&client, &base, &format!("/api/query/{shape}?adapter=async")).await;
        // Async custom bind metadata is intentionally delegated to Postgres.
        // Correctness and an explicit refusal matter even when sync can hit.
        if definition["expected"] == "postgres" {
            assert_eq!(answer["served"], "postgres", "{answer}");
        } else if answer["served"] == "postgres" {
            assert!(
                answer["refusal"]
                    .as_str()
                    .is_some_and(|reason| !reason.is_empty()),
                "{answer}"
            );
        } else {
            assert_eq!(answer["served"], "cache", "{answer}");
        }
        let direct = get(
            &client,
            &base,
            &format!("/api/query/{shape}?route=postgres&adapter=async"),
        )
        .await;
        assert_eq!(answer["data"], direct["data"]);
    }
    let refused = get(&client, &base, "/api/query/search").await;
    assert_eq!(refused["served"], "postgres");
    assert!(refused["refusal"].as_str().is_some_and(|s| !s.is_empty()));
    let created = post(
        &client,
        &base,
        "/tickets?wait=true",
        json!({"org_id":1,"title":"HTTP integration ticket","priority":"high"}),
    )
    .await;
    assert_eq!(created["committed"], true);
    assert_eq!(created["caught_up"], true);
    let id = created["id"].as_i64().unwrap();
    let changed = post(
        &client,
        &base,
        &format!("/tickets/{id}/status?wait=true"),
        json!({"status":"pending"}),
    )
    .await;
    assert_eq!(changed["caught_up"], true);
    assert_eq!(changed["observed"]["data"][0]["status"], "pending");
    post(
        &client,
        &base,
        &format!("/tickets/{id}/comments?wait=true"),
        json!({"body":"A comment written through the HTTP service."}),
    )
    .await;
    let comments = get(&client, &base, &format!("/tickets/{id}/comments")).await;
    assert_eq!(
        comments["data"][0]["body"],
        "A comment written through the HTTP service."
    );
    let forced = get(&client, &base, &format!("/tickets/{id}?route=postgres")).await;
    assert_eq!(forced["served"], "postgres");
    let invalid = client
        .post(format!("{base}/tickets"))
        .json(&json!({"org_id":1,"title":""}))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), reqwest::StatusCode::BAD_REQUEST);
    let missing_parent = client
        .post(format!("{base}/tickets"))
        .json(&json!({"org_id":99999999,"title":"Invalid organization"}))
        .send()
        .await
        .unwrap();
    assert_eq!(missing_parent.status(), reqwest::StatusCode::BAD_REQUEST);
    let missing_status = client
        .post(format!("{base}/tickets/99999999/status"))
        .json(&json!({"status":"open"}))
        .send()
        .await
        .unwrap();
    assert_eq!(missing_status.status(), reqwest::StatusCode::NOT_FOUND);
    let invalid_page = client
        .get(format!("{base}/orgs/1/queue?page=0"))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_page.status(), reqwest::StatusCode::BAD_REQUEST);
    let cross_origin = client
        .post(format!("{base}/tickets"))
        .header("Origin", "https://unrelated.invalid")
        .json(&json!({"org_id":1,"title":"Should not be written"}))
        .send()
        .await
        .unwrap();
    assert_eq!(cross_origin.status(), reqwest::StatusCode::BAD_REQUEST);
    // Simulate a barrier SQL failure after the INSERT has autocommitted. The
    // response must identify the committed row, rather than invite a retry.
    let setup_dsn = dsn.clone();
    tokio::task::spawn_blocking(move || {
        let mut conn = PgConnection::establish(&setup_dsn).unwrap();
        conn.batch_execute("CREATE OR REPLACE FUNCTION public.pg_current_wal_lsn() RETURNS pg_lsn LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'barrier failure probe'; END $$;
            CREATE OR REPLACE FUNCTION public.demo_barrier_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config('search_path', 'public,pg_catalog', false); RETURN NEW; END $$;
            CREATE TRIGGER demo_barrier_failure AFTER INSERT ON tickets FOR EACH ROW WHEN (NEW.title = 'Barrier failure probe') EXECUTE FUNCTION public.demo_barrier_failure();").unwrap();
    }).await.unwrap();
    let committed = post(
        &client,
        &base,
        "/tickets?wait=true",
        json!({"org_id":1,"title":"Barrier failure probe"}),
    )
    .await;
    let cleanup_dsn = dsn.clone();
    tokio::task::spawn_blocking(move || {
        let mut conn = PgConnection::establish(&cleanup_dsn).unwrap();
        conn.batch_execute("DROP TRIGGER demo_barrier_failure ON tickets; DROP FUNCTION public.demo_barrier_failure(); DROP FUNCTION public.pg_current_wal_lsn();").unwrap();
    }).await.unwrap();
    assert_eq!(committed["committed"], true);
    assert_eq!(committed["caught_up"], false);
    assert!(
        committed["wait_error"]
            .as_str()
            .unwrap()
            .contains("barrier failure probe")
    );
    let row = get(
        &client,
        &base,
        &format!("/tickets/{}?route=postgres", committed["id"]),
    )
    .await;
    assert_eq!(row["data"][0]["title"], "Barrier failure probe");
    // Continuous verification must execute the adapter requested by the UI.
    let before_async = get(&client, &base, "/api/metrics").await;
    post(
        &client,
        &base,
        "/api/verify/control",
        json!({"running":true,"adapter":"async"}),
    )
    .await;
    let mut completed = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let stats = get(&client, &base, "/api/verify").await;
        if stats["comparisons"].as_u64().unwrap() >= 23 && stats["busy"] == false {
            completed = true;
            break;
        }
    }
    let paused = post(
        &client,
        &base,
        "/api/verify/control",
        json!({"running":false,"adapter":"async"}),
    )
    .await;
    assert!(completed, "continuous pass did not finish");
    assert_eq!(paused["adapter"], "async");
    assert_eq!(paused["errors"], 0);
    let after_async = get(&client, &base, "/api/metrics").await;
    let count = |m: &Value| {
        m["service"]["routes"]["ticket:async:cached"]["reads"]
            .as_u64()
            .unwrap_or(0)
    };
    assert!(
        count(&after_async) > count(&before_async),
        "continuous verification must use async reads"
    );
    let blocked_fault = client
        .post(format!("{base}/api/lab/slot"))
        .header("Host", "remote.invalid")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(blocked_fault.status(), reqwest::StatusCode::BAD_REQUEST);
    post(&client, &base, "/api/writer", json!({"enabled":true,"rate":30,"operations":["status","comment","bulk","rollback","assign","rename"]})).await;
    for _ in 0..4 {
        let verify = post(&client, &base, "/api/verify", json!({})).await;
        assert_eq!(verify["mismatches"], 0, "{verify}");
        assert_eq!(verify["errors"], 0, "{verify}");
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    let metrics = get(&client, &base, "/api/metrics").await;
    assert!(
        metrics["writer"]["counts"]
            .as_object()
            .unwrap()
            .iter()
            .any(|(name, value)| name != "errors" && value.as_u64().unwrap_or(0) > 0)
    );
    let stream = client
        .get(format!("{base}/api/events"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        stream.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
    post(
        &client,
        &base,
        "/api/writer",
        json!({"enabled":false,"rate":0,"operations":["status"]}),
    )
    .await;
    let load = post(
        &client,
        &base,
        "/api/load",
        json!({"name":"ticket","concurrency":2,"seconds":1}),
    )
    .await;
    assert!(load["cached"]["requests"].as_u64().unwrap() > 0);
    assert_eq!(load["cached"]["errors"], 0);
    assert!(load["cached"]["memory"].as_u64().unwrap() > 0);
    assert!(load["postgres"]["requests"].as_u64().unwrap() > 0);
    for name in [
        "widen",
        "new-type",
        "drop",
        "connection",
        "slot",
        "rls",
        "session",
        "transaction",
    ] {
        let result = post(&client, &base, &format!("/api/lab/{name}"), json!({})).await;
        assert_eq!(result["ok"], true, "{result}");
        assert!(result["timeline"].as_array().unwrap().len() >= 2);
    }
    let verify = post(&client, &base, "/api/verify", json!({})).await;
    assert_eq!(verify["mismatches"], 0, "{verify}");
    assert_eq!(verify["errors"], 0, "{verify}");
    // Disconnect only this disposable demo's request pools. Idle sync Diesel
    // connections must be retired even when their transaction state is healthy.
    let disconnect_dsn = dsn.clone();
    tokio::task::spawn_blocking(move || {
        let mut conn = PgConnection::establish(&disconnect_dsn).unwrap();
        conn.batch_execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND application_name='ticket-desk-requests' AND pid<>pg_backend_pid()").unwrap();
    }).await.unwrap();
    for adapter in ["sync", "async"] {
        for route in ["cached", "postgres"] {
            let mut recovered = false;
            for _ in 0..30 {
                let response = client
                    .get(format!(
                        "{base}/api/query/search?adapter={adapter}&route={route}"
                    ))
                    .send()
                    .await
                    .unwrap();
                if response.status().is_success() {
                    let answer: Value = response.json().await.unwrap();
                    assert_eq!(answer["served"], "postgres");
                    assert!(answer["data"].is_array());
                    recovered = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(
                recovered,
                "{adapter} {route} pool remained poisoned after disconnect"
            );
        }
    }
    let end = app.clone();
    tokio::task::spawn_blocking(move || end.shutdown())
        .await
        .unwrap();
    let _ = stop_tx.send(());
    tokio::time::timeout(Duration::from_secs(8), server)
        .await
        .expect("SSE must not hold graceful shutdown open")
        .unwrap()
        .unwrap();
    drop(stream);
}
