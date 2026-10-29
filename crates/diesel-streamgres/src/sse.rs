//! Named query notifications as an HTTP event stream. Applications declare
//! their queries; this module shares watches, skips initial rows, turns
//! lost changes into refresh hints and closes the whole stream when any
//! query stops being followed. The application still authenticates the
//! request and chooses its workspace queries before calling `notifications`.

//!
//! With the `embedded` feature, start and check readiness once at startup,
//! then adapt each authorized request's body to your web framework:
//!
//! ```no_run
//! # #[cfg(feature = "embedded")]
//! use diesel_streamgres::{embedded, sse};
//! use futures_util::{Stream, StreamExt};
//! use std::{io, time::Duration};
//!
//! # #[cfg(feature = "embedded")]
//! async fn response_body() -> Result<impl Stream<Item = Result<String, io::Error>>, Box<dyn std::error::Error>> {
//!     let feed = sse::Feed::try_embedded(embedded::Config::new("postgres://localhost/app"))?;
//!     feed.ready(Duration::from_secs(30)).await?;
//!     let body = feed.notifications([
//!         sse::Query::new("payments", "SELECT id, status FROM public.payments"),
//!     ]).await?;
//!     // Serve with Content-Type: text/event-stream and Cache-Control: no-cache.
//!     Ok(body.map(Ok))
//! }
//! ```

use crate::{Change, SubscribeError, WatchError, engine::EngineClient};
use futures_util::{Stream, StreamExt, stream, stream::BoxStream};
use std::future::ready;
use std::time::Duration;

/// The response's content type.
pub const CONTENT_TYPE: &str = "text/event-stream";

/// A keep-alive interval most proxies' idle timeouts leave open.
pub const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// One event: an optional name (`event:`) and its data (`data:`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    name: Option<String>,
    data: String,
}

impl Event {
    /// An unnamed event (a client's `message`).
    pub fn new(data: impl Into<String>) -> Event {
        Event {
            name: None,
            data: data.into(),
        }
    }

    /// An event named `name`.
    pub fn named(name: impl Into<String>, data: impl Into<String>) -> Event {
        Event {
            name: Some(name.into()),
            data: data.into(),
        }
    }

    /// The event's lines: line breaks in the name are dropped, and each
    /// line of the data (ended by `\r\n`, `\r` or `\n`, as the format
    /// reads them) is a `data:` line of its own.
    pub fn render(&self) -> String {
        let mut text = String::new();
        if let Some(name) = &self.name {
            let name: String = name.chars().filter(|c| *c != '\n' && *c != '\r').collect();
            text.push_str("event: ");
            text.push_str(&name);
            text.push('\n');
        }
        for line in self.data.replace("\r\n", "\n").split(['\n', '\r']) {
            text.push_str("data: ");
            text.push_str(line);
            text.push('\n');
        }
        text.push('\n');
        text
    }
}

/// The body of a Server-Sent Events response carrying `events`: `:
/// connected` first, each event rendered, `: keep-alive` after each quiet
/// `keep_alive`. It ends when `events` does.
pub fn body<S>(events: S, keep_alive: Duration) -> impl Stream<Item = String> + Send + 'static
where
    S: Stream<Item = Event> + Send + 'static,
{
    // A zero timeout would repeatedly yield heartbeats without polling events.
    let keep_alive = if keep_alive.is_zero() {
        KEEP_ALIVE
    } else {
        keep_alive
    };
    futures_util::stream::unfold(
        (events.boxed(), true),
        move |(mut events, first)| async move {
            if first {
                return Some((": connected\n\n".to_string(), (events, false)));
            }
            let text = match tokio::time::timeout(keep_alive, events.next()).await {
                Err(_) => ": keep-alive\n\n".to_string(),
                Ok(Some(event)) => event.render(),
                Ok(None) => return None,
            };
            Some((text, (events, false)))
        },
    )
}

/// One application's notification: the event name and the SQL whose result
/// it invalidates. Rows present when the watch starts are not notifications;
/// the HTTP consumer refreshes on every connection instead.
#[derive(Debug, Clone)]
pub struct Query {
    name: String,
    sql: String,
    removals: bool,
}

impl Query {
    /// Notify when a row enters, changes or leaves the result.
    pub fn new(name: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sql: sql.into(),
            removals: true,
        }
    }
    /// Suppress removal hints only when the application guarantees a
    /// successor arrives. Deleting the final matching row then produces no
    /// notification. Most queries should keep removals enabled (the default).
    #[must_use]
    pub fn ignore_removals(mut self) -> Self {
        self.removals = false;
        self
    }
}

/// A reusable feed for a database. Clone it across HTTP workers; watches
/// of the same query share a subscription and stop with the last consumer.
#[derive(Clone)]
pub struct Feed {
    engine: EngineClient,
    database_url: String,
}

impl std::fmt::Debug for Feed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Feed").finish_non_exhaustive()
    }
}

impl Feed {
    /// Use an existing engine client, including one configured by the
    /// application for remote or embedded operation.
    pub fn new(engine: EngineClient, database_url: impl Into<String>) -> Self {
        Self {
            engine,
            database_url: database_url.into(),
        }
    }

    /// Run the engine in this process with default client settings.
    /// Thread creation failures panic; use [`Self::try_embedded`] to handle them.
    #[cfg(feature = "embedded")]
    pub fn embedded(config: crate::embedded::Config) -> Self {
        Self::try_embedded(config).expect("starting the embedded engine thread")
    }

    /// Start with explicit engine configuration and default client settings.
    /// Neither configuration reads environment variables.
    #[cfg(feature = "embedded")]
    pub fn try_embedded(
        config: crate::embedded::Config,
    ) -> Result<Self, crate::embedded::StartError> {
        Self::with_embedded_config(config, crate::engine::Config::default())
    }

    /// Start with explicit settings for both the engine and its client.
    #[cfg(feature = "embedded")]
    pub fn with_embedded_config(
        config: crate::embedded::Config,
        client: crate::engine::Config,
    ) -> Result<Self, crate::embedded::StartError> {
        let database_url = config.dsn.clone();
        Ok(Self::new(
            EngineClient::embedded(crate::embedded::Engine::try_start(config)?, client),
            database_url,
        ))
    }

    /// Load both configurations from the environment and include workspace
    /// schemas. Use [`Self::ready`] to check replication setup before serving.
    #[cfg(feature = "embedded")]
    pub fn embedded_from_env(schemas: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut config = crate::embedded::Config::from_env()?;
        for schema in schemas {
            if !config.schemas.contains(&schema) {
                config.schemas.push(schema);
            }
        }
        Self::with_embedded_config(config, crate::engine::Config::from_env())
            .map_err(|error| error.to_string())
    }

    /// Wait for engine startup and a client connection, within one deadline.
    /// A remote feed waits for a connection; an embedded feed also reports
    /// database and replication startup failures.
    pub async fn ready(&self, timeout: Duration) -> Result<(), SubscribeError> {
        self.engine.ready(timeout).await
    }

    /// Return invalidation hints, not a durable event log. Consumers must
    /// refresh authoritative data on connection and on every event; several
    /// changes may coalesce, and a reconnect may repeat hints.
    ///
    /// Return the complete SSE body: initial comment, named notifications
    /// and fifteen-second idle heartbeats. Dropping it drops every watch.
    /// A setup failure releases watches already opened for this request;
    /// a terminal failure later ends the entire body so a consumer can
    /// reconnect and reconcile all its state, never just part of it.
    pub async fn notifications(
        &self,
        queries: impl IntoIterator<Item = Query>,
    ) -> Result<BoxStream<'static, String>, SubscribeError> {
        let queries: Vec<_> = queries.into_iter().collect();
        if queries.is_empty()
            || queries
                .iter()
                .any(|q| q.name.is_empty() || q.name.contains(['\r', '\n']))
        {
            return Err(SubscribeError::InvalidInput(
                "notification queries need nonempty event names without line breaks".into(),
            ));
        }
        let mut streams = Vec::new();
        for query in queries {
            let watch = self
                .engine
                .watch_sql(&self.database_url, query.sql.clone())
                .await?;
            let ended = query.name.clone();
            streams.push(
                watch
                    .into_stream()
                    .filter_map(move |change| ready(notification(&query, change)))
                    .chain(stream::once(async move {
                        Err(format!("{ended} watcher ended"))
                    }))
                    .boxed(),
            );
        }
        Ok(body(merge(streams), KEEP_ALIVE).boxed())
    }
}

/// Interpret a watcher without exposing its row type to the HTTP adapter.
fn notification<T>(
    query: &Query,
    change: Result<Change<T>, WatchError>,
) -> Option<Result<Event, String>> {
    match change {
        Ok(Change::Present(_)) => None,
        Ok(Change::Removed(_)) if !query.removals => None,
        Err(WatchError::Failed(reason)) => Some(Err(format!("{}: {reason}", query.name))),
        Ok(_) | Err(WatchError::Missed(_)) => Some(Ok(Event::named(&query.name, &query.name))),
    }
}

/// A failed or ended watch terminates its siblings, even while they are
/// quiet. The library owns this contract so every HTTP adapter gets it.
fn merge(streams: Vec<BoxStream<'static, Result<Event, String>>>) -> impl Stream<Item = Event> {
    stream::select_all(streams)
        .take_while(|event| {
            if let Err(reason) = event {
                log::warn!(target: "diesel_streamgres", "notification stream ended: {reason}");
            }
            ready(event.is_ok())
        })
        .filter_map(|event| ready(event.ok()))
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn zero_heartbeat_interval_still_delivers_events_and_ends() {
        let events = stream::iter([Event::new("change")]);
        let text = body(events, Duration::ZERO).collect::<Vec<_>>().await;
        assert_eq!(text, [": connected\n\n", "data: change\n\n"]);
    }

    #[test]
    fn removals_invalidate_by_default_including_the_final_row() {
        let query = Query::new("config", "SELECT id FROM config LIMIT 1");
        assert_eq!(
            notification(&query, Ok(Change::Removed(1)))
                .unwrap()
                .unwrap(),
            Event::named("config", "config")
        );
    }

    use super::*;

    /// Named and unnamed events render as the format has them; data lines
    /// split, a name's line breaks dropped.
    #[test]
    fn events_render_as_lines() {
        assert_eq!(
            Event::named("config", "config").render(),
            "event: config\ndata: config\n\n"
        );
        assert_eq!(Event::new("a\nb").render(), "data: a\ndata: b\n\n");
        assert_eq!(
            Event::new("a\r\nb\rc").render(),
            "data: a\ndata: b\ndata: c\n\n"
        );
        assert_eq!(Event::named("x\ny", "1").render(), "event: xy\ndata: 1\n\n");
    }

    /// The body opens with a comment, carries each event, and ends with
    /// the events.
    #[tokio::test]
    async fn body_carries_events_then_ends() {
        let events = futures_util::stream::iter(vec![
            Event::named("config", "config"),
            Event::named("experiments", "experiments"),
        ]);
        let text: Vec<String> = body(events, KEEP_ALIVE).collect().await;
        assert_eq!(
            text,
            vec![
                ": connected\n\n",
                "event: config\ndata: config\n\n",
                "event: experiments\ndata: experiments\n\n",
            ]
        );
    }

    /// A quiet stream gets a keep-alive comment each interval.
    #[tokio::test]
    async fn quiet_streams_are_kept_alive() {
        let events = futures_util::stream::pending::<Event>();
        let text: Vec<String> = body(events, Duration::from_millis(20))
            .take(3)
            .collect()
            .await;
        assert_eq!(
            text,
            vec![": connected\n\n", ": keep-alive\n\n", ": keep-alive\n\n"]
        );
    }

    #[test]
    fn initial_rows_and_optional_removals_are_not_notifications() {
        let query = Query::new("orders", "SELECT id FROM orders").ignore_removals();
        assert!(notification(&query, Ok(Change::Present(1))).is_none());
        assert!(notification(&query, Ok(Change::Removed(1))).is_none());
        assert_eq!(
            notification(&query, Ok(Change::Added(1))).unwrap().unwrap(),
            Event::named("orders", "orders")
        );
        assert!(
            notification::<i32>(&query, Err(WatchError::Missed("lagged".into())))
                .unwrap()
                .is_ok()
        );
        assert!(
            notification::<i32>(&query, Err(WatchError::Failed("gone".into())))
                .unwrap()
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_failed_watch_ends_the_whole_body() {
        let events = merge(vec![
            stream::iter(vec![Err("failed".into())]).boxed(),
            stream::pending().boxed(),
        ]);
        let body = body(events, KEEP_ALIVE);
        futures_util::pin_mut!(body);
        assert_eq!(body.next().await.unwrap(), ": connected\n\n");
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), body.next())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn changes_before_failure_are_sent() {
        let events = merge(vec![
            stream::iter(vec![
                Ok(Event::new("changed")),
                Err("gone".into()),
                Ok(Event::new("too late")),
            ])
            .boxed(),
        ]);
        assert_eq!(
            events.collect::<Vec<_>>().await,
            vec![Event::new("changed")]
        );
    }
}
