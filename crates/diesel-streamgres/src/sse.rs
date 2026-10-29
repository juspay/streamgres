//! Changes as a Server-Sent Events body, for any web framework.
//!
//! [`body`] turns a stream of [`Event`]s into the text of a
//! `text/event-stream` response: a comment when the stream opens, each
//! event as its `event:` and `data:` lines, and a comment after every
//! quiet `keep_alive`, so proxies keep the connection and a client gone
//! away is noticed on the next write. The items are `String`s; a framework
//! wants them as its own bytes:
//!
//! ```ignore
//! let watch = engine.watch_sql(&database_url, "SELECT id, status FROM orders").await?;
//! let events = watch.into_stream().filter_map(|change| async move {
//!     match change {
//!         Ok(Change::Present(_)) => None,
//!         // A change, or changes missed: the client reads afresh.
//!         Ok(_) | Err(WatchError::Missed(_)) => Some(Event::named("orders", "changed")),
//!         // No longer followed: end the response, so the client reconnects.
//!         Err(WatchError::Failed(_)) => None,
//!     }
//! });
//! // actix-web
//! HttpResponse::Ok()
//!     .content_type(sse::CONTENT_TYPE)
//!     .streaming(sse::body(events, sse::KEEP_ALIVE).map(|text| Ok::<_, Infallible>(Bytes::from(text))))
//! ```
//!
//! What each event means, who may watch which query, and what a client
//! does when told are the application's.

use std::time::Duration;

use futures_util::{Stream, StreamExt};

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

#[cfg(test)]
mod tests {
    use super::*;

    /// Named and unnamed events render as the format has them; data lines
    /// split, a name's line breaks dropped.
    #[test]
    fn events_render_as_lines() {
        assert_eq!(Event::named("config", "config").render(), "event: config\ndata: config\n\n");
        assert_eq!(Event::new("a\nb").render(), "data: a\ndata: b\n\n");
        assert_eq!(Event::new("a\r\nb\rc").render(), "data: a\ndata: b\ndata: c\n\n");
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
        let text: Vec<String> = body(events, Duration::from_millis(20)).take(3).collect().await;
        assert_eq!(text, vec![": connected\n\n", ": keep-alive\n\n", ": keep-alive\n\n"]);
    }
}
