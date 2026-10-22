//! The client side's calls to the application server, the way zero-cache
//! makes them: a `POST` of `["transform", [{id, name, args}]]` to the
//! query endpoint turns query names into ASTs, a `POST` of a client's push
//! body to the mutate endpoint runs its mutations; both carry the
//! connection's cookies and origin, its auth token when it has one, and
//! the `schema` and `appID` parameters the server library reads. Every
//! call is given the configured time to answer
//! (`STREAMGRES_BACKEND_TIMEOUT_MS`); one that ran out of it is reported as
//! failed and not made again, since a slow server is not helped by the
//! same question asked three times.

use std::fmt;
use std::time::Duration;

use serde_json::{Value as Json, json};

use super::config::Config;
use crate::log::{log_debug, log_warn};

/// The HTTP client and where it posts.
pub struct Backend {
    http: reqwest::Client,
    query_url: String,
    mutate_url: String,
    forward_cookies: bool,
    upstream_schema: String,
    app_id: String,
}

/// What one connection carries along to the application server: its
/// credentials and origin. The endpoints are the server's configuration,
/// never the client's.
#[derive(Debug, Clone, Default)]
pub struct Identity {
    pub cookie: Option<String>,
    pub origin: Option<String>,
    pub token: Option<String>,
}

impl Identity {
    /// The claim that binds a client group to its first claimant. Prefers the
    /// `sub` from the JWT token so that tabs with rotated tokens for the same
    /// user share a group. Falls back to the cookie pairs (sorted and
    /// deduplicated for stability) when there is no token or it carries no
    /// `sub`. Returns `None` when there is neither.
    pub fn stamp(&self) -> Option<String> {
        if let Some(sub) = self.token.as_deref().and_then(jwt_sub) {
            return Some(sub);
        }
        let cookie = self.cookie.as_deref().map(str::trim)?;
        let mut pairs: Vec<&str> = cookie
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        if pairs.is_empty() {
            return None;
        }
        pairs.sort_unstable();
        pairs.dedup();
        Some(pairs.join(";"))
    }
}

/// Extract the `sub` claim from a JWT without verifying the signature.
/// Returns `None` when the token is blank, malformed, or has no non-blank `sub`.
fn jwt_sub(token: &str) -> Option<String> {
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    let payload_b64 = token.splitn(3, '.').nth(1)?;
    use base64::Engine as _;
    let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload_b64))
        .ok()?;
    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;
    let sub = payload.get("sub").and_then(|v| v.as_str())?;
    if sub.is_empty() { None } else { Some(sub.to_owned()) }
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

/// A call that got no answer: what went wrong, and whether it was the
/// time that ran out (such a call is not made again).
#[derive(Debug)]
struct Unanswered {
    message: String,
    timed_out: bool,
}

impl fmt::Display for Unanswered {
    /// The message.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Backend {
    /// A client for the endpoints `config` names, each call given
    /// `config.backend_timeout` to answer (zero: as long as it takes).
    pub fn new(config: &Config) -> Result<Backend, String> {
        let mut http = reqwest::Client::builder();
        if !config.backend_timeout.is_zero() {
            http = http.timeout(config.backend_timeout);
        }
        let http = http
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
    /// objects. A 5xx or a network failure is retried a few times; a call
    /// that timed out is not.
    pub async fn transform(&self, identity: &Identity, requests: Vec<Json>) -> TransformOutcome {
        let body = json!(["transform", requests]);
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.post(&self.query_url, identity, &body).await {
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
                Err(unanswered) => {
                    if attempt < 3 && !unanswered.timed_out {
                        tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
                        continue;
                    }
                    return TransformOutcome::Failed {
                        status: None,
                        message: unanswered.message,
                    };
                }
            }
        }
    }

    /// Forward one push body; never retried, the endpoint owns idempotency.
    pub async fn push(&self, identity: &Identity, body: &Json) -> PushOutcome {
        match self.post(&self.mutate_url, identity, body).await {
            Ok((status, json)) if (200..300).contains(&status) => PushOutcome::Response(json),
            Ok((status, json)) => PushOutcome::Failed {
                status: Some(status),
                preview: Some(preview(&json.to_string())),
                message: format!("the mutate endpoint answered {status}"),
            },
            Err(unanswered) => PushOutcome::Failed {
                status: None,
                preview: None,
                message: unanswered.message,
            },
        }
    }

    /// One `POST` with the identity's headers and the reserved
    /// parameters; the status and the body (as JSON when it parses, as a
    /// string otherwise), or why there was no answer.
    async fn post(
        &self,
        url: &str,
        identity: &Identity,
        body: &Json,
    ) -> Result<(u16, Json), Unanswered> {
        let mut url = reqwest::Url::parse(url).map_err(|error| Unanswered {
            message: format!("bad endpoint URL `{url}`: {error}"),
            timed_out: false,
        })?;
        url.query_pairs_mut()
            .append_pair("schema", &self.upstream_schema)
            .append_pair("appID", &self.app_id);
        let mut request = self
            .http
            .post(url.clone())
            .header("Content-Type", "application/json");
        if let Some(token) = &identity.token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        if let Some(cookie) = identity.cookie.as_ref().filter(|_| self.forward_cookies) {
            request = request.header("Cookie", cookie);
        }
        if let Some(origin) = &identity.origin {
            request = request.header("Origin", origin);
        }
        log_debug!("POST {url}");
        let response = request
            .body(body.to_string())
            .send()
            .await
            .map_err(|error| unanswered(&url, "", &error))?;
        let status = response.status().as_u16();
        let text = response
            .text()
            .await
            .map_err(|error| unanswered(&url, "reading the response: ", &error))?;
        if !(200..300).contains(&status) {
            log_warn!("{url} answered {status}: {}", preview(&text));
        }
        Ok((
            status,
            serde_json::from_str(&text).unwrap_or(Json::String(text)),
        ))
    }
}

/// Why `url` gave no answer, a timeout named as one.
fn unanswered(url: &reqwest::Url, doing: &str, error: &reqwest::Error) -> Unanswered {
    let timed_out = error.is_timeout();
    let message = if timed_out {
        format!("{url}: {doing}no answer within the backend timeout")
    } else {
        format!("{url}: {doing}{error}")
    };
    Unanswered { message, timed_out }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make(token: Option<&str>, cookie: Option<&str>) -> Identity {
        Identity {
            token: token.map(str::to_owned),
            cookie: cookie.map(str::to_owned),
            ..Identity::default()
        }
    }

    // Two JWTs with the same `sub` ("user-123") but different `iat`.
    const JWT_V1: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9\
        .eyJzdWIiOiAidXNlci0xMjMiLCAiaWF0IjogMTcwMDAwMDAwMH0.fakesig";
    const JWT_V2: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9\
        .eyJzdWIiOiAidXNlci0xMjMiLCAiaWF0IjogMTcwMDAwMzYwMH0.fakesig";
    const JWT_OTHER: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9\
        .eyJzdWIiOiAidXNlci00NTYiLCAiaWF0IjogMTcwMDAwMDAwMH0.fakesig";
    // JWT with no `sub`.
    const JWT_NOSUB: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9\
        .eyJzdWIiOiBudWxsLCAiaWF0IjogMTcwMDAwMDAwMH0.fakesig";

    #[test]
    fn stamp_prefers_jwt_sub_and_falls_back_to_cookie() {
        // JWT sub is the stamp; two rotated tokens for the same user match.
        assert_eq!(make(Some(JWT_V1), None).stamp(), Some("user-123".to_owned()));
        assert_eq!(make(Some(JWT_V1), None).stamp(), make(Some(JWT_V2), None).stamp());
        assert_ne!(make(Some(JWT_V1), None).stamp(), make(Some(JWT_OTHER), None).stamp());

        // No token or no sub falls through to cookie.
        let cookie_stamp = make(None, Some("sid=1; uid=2")).stamp();
        assert_eq!(cookie_stamp, make(Some(""), Some("sid=1; uid=2")).stamp());
        assert_eq!(cookie_stamp, make(Some(JWT_NOSUB), Some("sid=1; uid=2")).stamp());
        // Same cookies in any order are the same stamp.
        assert_eq!(cookie_stamp, make(None, Some("uid=2; sid=1")).stamp());

        // No token and no cookie is no stamp.
        assert_eq!(make(None, None).stamp(), None);
        assert_eq!(make(None, Some("  ;  ")).stamp(), None);
    }
}
