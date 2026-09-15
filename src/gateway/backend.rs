//! The gateway's calls to the application server, the way zero-cache
//! makes them: a `POST` of `["transform", [{id, name, args}]]` to the
//! query endpoint turns query names into ASTs, a `POST` of a client's push
//! body to the mutate endpoint runs its mutations; both carry the
//! connection's cookies and origin, its auth token when it has one, and
//! the `schema` and `appID` parameters the server library reads.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{Value as Json, json};

use super::config::Config;
use super::log::{gw_debug, gw_warn};

/// The HTTP client and where it posts.
pub struct Backend {
    http: reqwest::Client,
    query_url: String,
    mutate_url: String,
    forward_cookies: bool,
    upstream_schema: String,
    app_id: String,
}

/// What one connection carries along to the application server.
#[derive(Debug, Clone, Default)]
pub struct Identity {
    pub cookie: Option<String>,
    pub origin: Option<String>,
    pub token: Option<String>,
    pub query_url: Option<String>,
    pub query_headers: HashMap<String, String>,
    pub mutate_url: Option<String>,
    pub mutate_headers: HashMap<String, String>,
}

/// The query endpoint's answer.
#[derive(Debug)]
pub enum TransformOutcome {
    /// One result per requested query: `{id, name, ast}` or
    /// `{error, id, name, message?, details?}`.
    Queries(Vec<Json>),
    /// The endpoint could not answer at all.
    Failed {
        status: Option<u16>,
        message: String,
    },
}

/// The mutate endpoint's answer.
#[derive(Debug)]
pub enum PushOutcome {
    /// The endpoint's JSON body, as returned.
    Response(Json),
    /// The endpoint could not take the push.
    Failed {
        status: Option<u16>,
        preview: Option<String>,
        message: String,
    },
}

impl Backend {
    /// A client for the endpoints `config` names.
    pub fn new(config: &Config) -> Result<Backend, String> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|error| format!("http client: {error}"))?;
        Ok(Backend {
            http,
            query_url: config.query_url.clone(),
            mutate_url: config.mutate_url.clone(),
            forward_cookies: config.forward_cookies,
            upstream_schema: config.upstream_schema(),
            app_id: config.app_id.clone(),
        })
    }

    /// Turn query names into ASTs: `requests` are `{id, name, args}`
    /// objects. A 5xx or a network failure is retried a few times.
    pub async fn transform(&self, identity: &Identity, requests: Vec<Json>) -> TransformOutcome {
        let url = identity.query_url.as_deref().unwrap_or(&self.query_url);
        let body = json!(["transform", requests]);
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self
                .post(url, &identity.query_headers, identity, &body)
                .await
            {
                Ok((status, json)) if (200..300).contains(&status) => {
                    return match json {
                        Json::Object(object)
                            if object.get("kind") == Some(&json!("QueryResponse")) =>
                        {
                            match object.get("queries") {
                                Some(Json::Array(queries)) => {
                                    TransformOutcome::Queries(queries.clone())
                                }
                                _ => TransformOutcome::Failed {
                                    status: None,
                                    message: "QueryResponse without queries".to_owned(),
                                },
                            }
                        }
                        Json::Object(object) => TransformOutcome::Failed {
                            status: None,
                            message: object
                                .get("message")
                                .and_then(Json::as_str)
                                .unwrap_or("the query endpoint reported a failure")
                                .to_owned(),
                        },
                        Json::Array(items) if items.first() == Some(&json!("transformed")) => {
                            match items.get(1) {
                                Some(Json::Array(queries)) => {
                                    TransformOutcome::Queries(queries.clone())
                                }
                                _ => TransformOutcome::Failed {
                                    status: None,
                                    message: "transformed without queries".to_owned(),
                                },
                            }
                        }
                        Json::Array(items) if items.first() == Some(&json!("transformFailed")) => {
                            TransformOutcome::Failed {
                                status: None,
                                message: items
                                    .get(1)
                                    .and_then(|body| body.get("message"))
                                    .and_then(Json::as_str)
                                    .unwrap_or("the query endpoint reported a failure")
                                    .to_owned(),
                            }
                        }
                        Json::Array(items) => TransformOutcome::Queries(items),
                        other => TransformOutcome::Failed {
                            status: None,
                            message: format!("unexpected query response: {other}"),
                        },
                    };
                }
                Ok((status, json)) => {
                    if status >= 500 && attempt < 3 {
                        tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
                        continue;
                    }
                    return TransformOutcome::Failed {
                        status: Some(status),
                        message: format!(
                            "the query endpoint answered {status}: {}",
                            preview(&json.to_string())
                        ),
                    };
                }
                Err(message) => {
                    if attempt < 3 {
                        tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
                        continue;
                    }
                    return TransformOutcome::Failed {
                        status: None,
                        message,
                    };
                }
            }
        }
    }

    /// Forward one push body; never retried, the endpoint owns idempotency.
    pub async fn push(&self, identity: &Identity, body: &Json) -> PushOutcome {
        let url = identity.mutate_url.as_deref().unwrap_or(&self.mutate_url);
        match self
            .post(url, &identity.mutate_headers, identity, body)
            .await
        {
            Ok((status, json)) if (200..300).contains(&status) => PushOutcome::Response(json),
            Ok((status, json)) => PushOutcome::Failed {
                status: Some(status),
                preview: Some(preview(&json.to_string())),
                message: format!("the mutate endpoint answered {status}"),
            },
            Err(message) => PushOutcome::Failed {
                status: None,
                preview: None,
                message,
            },
        }
    }

    /// One `POST` with the identity's headers and the reserved
    /// parameters; the status and the body (as JSON when it parses, as a
    /// string otherwise).
    async fn post(
        &self,
        url: &str,
        extra: &HashMap<String, String>,
        identity: &Identity,
        body: &Json,
    ) -> Result<(u16, Json), String> {
        let mut url = reqwest::Url::parse(url)
            .map_err(|error| format!("bad endpoint URL `{url}`: {error}"))?;
        url.query_pairs_mut()
            .append_pair("schema", &self.upstream_schema)
            .append_pair("appID", &self.app_id);
        let mut request = self
            .http
            .post(url.clone())
            .header("Content-Type", "application/json");
        for (name, value) in extra {
            request = request.header(name, value);
        }
        if let Some(token) = &identity.token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        if let Some(cookie) = identity.cookie.as_ref().filter(|_| self.forward_cookies) {
            request = request.header("Cookie", cookie);
        }
        if let Some(origin) = &identity.origin {
            request = request.header("Origin", origin);
        }
        gw_debug!("POST {url}");
        let response = request
            .body(body.to_string())
            .send()
            .await
            .map_err(|error| format!("{url}: {error}"))?;
        let status = response.status().as_u16();
        let text = response
            .text()
            .await
            .map_err(|error| format!("{url}: reading the response: {error}"))?;
        if !(200..300).contains(&status) {
            gw_warn!("{url} answered {status}: {}", preview(&text));
        }
        Ok((
            status,
            serde_json::from_str(&text).unwrap_or(Json::String(text)),
        ))
    }
}

/// The first 512 characters of a body, for a log line or an error.
fn preview(text: &str) -> String {
    if text.len() <= 512 {
        return text.to_owned();
    }
    let mut end = 512;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &text[..end])
}
