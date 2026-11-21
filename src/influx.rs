//! Minimal InfluxDB v2 HTTP client: buckets, Flux queries, writes.

use crate::error::{Kind, fail};
use crate::window::default_shard_duration_secs;
use anyhow::Context as _;
use reqwest::blocking::{Client as Http, RequestBuilder, Response};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use tracing::{debug, warn};

pub struct Client {
    http: Http,
    base: String,
    token: String,
    pub org: String,
    pub write_retries: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct Bucket {
    pub id: String,
    pub name: String,
    pub org_id: String,
    /// 0 = infinite.
    pub retention_secs: i64,
    pub shard_group_secs: i64,
}

#[derive(Deserialize)]
struct RawBucket {
    id: String,
    name: String,
    #[serde(rename = "orgID")]
    org_id: String,
    #[serde(default, rename = "retentionRules")]
    retention_rules: Vec<RawRule>,
}

#[derive(Deserialize)]
struct RawRule {
    #[serde(default, rename = "everySeconds")]
    every_seconds: i64,
    #[serde(default, rename = "shardGroupDurationSeconds")]
    shard_group_duration_seconds: i64,
}

impl From<RawBucket> for Bucket {
    fn from(b: RawBucket) -> Self {
        let rule = b.retention_rules.first();
        let retention_secs = rule.map_or(0, |r| r.every_seconds);
        let shard = rule
            .map(|r| r.shard_group_duration_seconds)
            .filter(|s| *s > 0)
            .unwrap_or_else(|| default_shard_duration_secs(retention_secs));
        Bucket {
            id: b.id,
            name: b.name,
            org_id: b.org_id,
            retention_secs,
            shard_group_secs: shard,
        }
    }
}

/// A token whose permissions name a bucket by ID. Such tokens keep pointing at
/// the old bucket after a rename.
#[derive(Clone, Debug, Serialize)]
pub struct BucketToken {
    pub id: String,
    pub description: String,
    pub actions: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Dbrp {
    pub id: String,
    pub database: String,
    pub retention_policy: String,
}

fn body_message(resp: Response) -> String {
    let text = resp.text().unwrap_or_default();
    serde_json::from_str::<serde_json::Value>(&text)
        .ok()
        .and_then(|v| v.get("message").and_then(|m| m.as_str()).map(str::to_owned))
        .unwrap_or(text)
        .chars()
        .take(500)
        .collect()
}

fn connection_error(base: &str, e: reqwest::Error) -> anyhow::Error {
    if e.is_timeout() {
        return fail(Kind::Connection, format!("request to InfluxDB at {base} timed out: {e}"))
            .hint("rerun the same command to resume; if it repeats, use a smaller --window or a larger --timeout-secs")
            .into();
    }
    fail(
        Kind::Connection,
        format!("cannot reach InfluxDB at {base}: {e}"),
    )
    .hint("check --url / INFLUX_URL and that InfluxDB v2 is running; rerun to resume")
    .into()
}

impl Client {
    pub fn new(url: &str, token: &str, org: &str, timeout_secs: u64) -> anyhow::Result<Self> {
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(fail(
                Kind::Usage,
                format!("--url must start with http:// or https://, got `{url}`"),
            )
            .into());
        }
        if token.is_empty() {
            return Err(fail(Kind::Usage, "no token given")
                .hint("pass --token or set INFLUX_TOKEN")
                .into());
        }
        let http = Http::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(timeout_secs))
            .user_agent(concat!(
                "loxinflux-migrationshelper/",
                env!("CARGO_PKG_VERSION")
            ))
            .build()
            .context("building the HTTP client")?;
        Ok(Self {
            http,
            base: url.trim_end_matches('/').to_owned(),
            token: token.to_owned(),
            org: org.to_owned(),
            write_retries: 5,
        })
    }

    fn req(&self, method: Method, path: &str) -> RequestBuilder {
        self.http
            .request(method, format!("{}{}", self.base, path))
            .header("Authorization", format!("Token {}", self.token))
    }

    fn send(&self, rb: RequestBuilder, what: &str) -> anyhow::Result<Response> {
        let resp = rb.send().map_err(|e| connection_error(&self.base, e))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let msg = body_message(resp);
        let err = match status {
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => fail(Kind::Auth, format!("{what}: {status} {msg}"))
                .hint("the token needs read access to the source bucket and write access to the target bucket (an all-access or operator token is simplest)"),
            StatusCode::NOT_FOUND => fail(Kind::NotFound, format!("{what}: {status} {msg}")),
            _ => fail(Kind::Internal, format!("{what}: {status} {msg}")),
        };
        Err(err.into())
    }

    /// Returns the server version; fails with `connection` when unreachable.
    pub fn ping(&self) -> anyhow::Result<String> {
        let resp = self
            .http
            .get(format!("{}/health", self.base))
            .send()
            .map_err(|e| connection_error(&self.base, e))?;
        let v: serde_json::Value = resp.json().map_err(|_| {
            fail(
                Kind::Connection,
                format!("{} did not answer like InfluxDB v2 (/health)", self.base),
            )
            .hint("point --url at the InfluxDB v2 HTTP API, e.g. http://host:8086")
        })?;
        Ok(v.get("version")
            .and_then(|x| x.as_str())
            .unwrap_or("unknown")
            .to_owned())
    }

    pub fn org_id(&self) -> anyhow::Result<String> {
        let v: serde_json::Value = self
            .send(
                self.req(Method::GET, "/api/v2/orgs")
                    .query(&[("org", &self.org)]),
                "listing organisations",
            )?
            .json()?;
        v["orgs"][0]["id"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| {
                fail(
                    Kind::NotFound,
                    format!("organisation `{}` not found", self.org),
                )
                .hint("pass the organisation name with --org / INFLUX_ORG")
                .into()
            })
    }

    pub fn find_bucket(&self, name: &str) -> anyhow::Result<Option<Bucket>> {
        let rb = self
            .req(Method::GET, "/api/v2/buckets")
            .query(&[("name", name), ("org", &self.org)]);
        let resp = match self.send(rb, "looking up bucket") {
            Ok(r) => r,
            Err(e) if crate::error::classify(&e).0 == Kind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        #[derive(Deserialize)]
        struct List {
            #[serde(default)]
            buckets: Vec<RawBucket>,
        }
        let list: List = resp.json().context("parsing bucket list")?;
        Ok(list
            .buckets
            .into_iter()
            .find(|b| b.name == name)
            .map(Bucket::from))
    }

    pub fn bucket(&self, name: &str) -> anyhow::Result<Bucket> {
        self.find_bucket(name)?.ok_or_else(|| {
            fail(
                Kind::NotFound,
                format!("bucket `{name}` not found in organisation `{}`", self.org),
            )
            .into()
        })
    }

    /// Create a bucket with the given retention (0 = infinite) and shard group duration.
    pub fn create_bucket(
        &self,
        name: &str,
        org_id: &str,
        retention_secs: i64,
        shard_group_secs: i64,
        origin: &str,
    ) -> anyhow::Result<Bucket> {
        let body = json!({
            "orgID": org_id,
            "name": name,
            "description": format!("created by loxinflux-migrationshelper from {origin}"),
            "retentionRules": [{
                "type": "expire",
                "everySeconds": retention_secs,
                "shardGroupDurationSeconds": shard_group_secs,
            }],
        });
        let raw: RawBucket = self
            .send(
                self.req(Method::POST, "/api/v2/buckets").json(&body),
                "creating bucket",
            )?
            .json()?;
        Ok(raw.into())
    }

    pub fn rename_bucket(&self, id: &str, new_name: &str) -> anyhow::Result<()> {
        self.send(
            self.req(Method::PATCH, &format!("/api/v2/buckets/{id}"))
                .json(&json!({ "name": new_name })),
            "renaming bucket",
        )?;
        Ok(())
    }

    pub fn tokens_for_bucket(&self, bucket_id: &str) -> anyhow::Result<Vec<BucketToken>> {
        let v: serde_json::Value = self
            .send(
                self.req(Method::GET, "/api/v2/authorizations"),
                "listing tokens",
            )?
            .json()?;
        let mut out = Vec::new();
        for a in v["authorizations"].as_array().into_iter().flatten() {
            let actions: Vec<String> = a["permissions"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|p| p["resource"]["id"].as_str() == Some(bucket_id))
                .filter_map(|p| p["action"].as_str().map(str::to_owned))
                .collect();
            if !actions.is_empty() {
                out.push(BucketToken {
                    id: a["id"].as_str().unwrap_or_default().to_owned(),
                    description: a["description"].as_str().unwrap_or_default().to_owned(),
                    actions,
                });
            }
        }
        Ok(out)
    }

    pub fn dbrps_for_bucket(&self, org_id: &str, bucket_id: &str) -> anyhow::Result<Vec<Dbrp>> {
        let rb = self
            .req(Method::GET, "/api/v2/dbrps")
            .query(&[("orgID", org_id), ("bucketID", bucket_id)]);
        let v: serde_json::Value = self.send(rb, "listing DBRP mappings")?.json()?;
        Ok(v["content"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|d| Dbrp {
                id: d["id"].as_str().unwrap_or_default().to_owned(),
                database: d["database"].as_str().unwrap_or_default().to_owned(),
                retention_policy: d["retention_policy"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
            })
            .collect())
    }

    /// Run a Flux query and return the streaming annotated-CSV response.
    pub fn query(&self, flux: &str) -> anyhow::Result<Response> {
        debug!(flux, "flux query");
        let body = json!({
            "query": flux,
            "type": "flux",
            "dialect": {
                "header": true,
                "annotations": ["datatype", "group", "default"],
                "dateTimeFormat": "RFC3339Nano",
            },
        });
        self.send(
            self.req(Method::POST, "/api/v2/query")
                .query(&[("org", &self.org)])
                .header("Accept", "application/csv")
                .json(&body),
            "running Flux query",
        )
    }

    /// Go heap in use of the server (from /metrics), if exposed.
    pub fn heap_inuse_bytes(&self) -> Option<u64> {
        let text = self
            .http
            .get(format!("{}/metrics", self.base))
            .timeout(Duration::from_secs(5))
            .send()
            .ok()?
            .text()
            .ok()?;
        text.lines()
            .find(|l| l.starts_with("go_memstats_heap_inuse_bytes "))?
            .split_whitespace()
            .nth(1)?
            .parse::<f64>()
            .ok()
            .map(|v| v as u64)
    }

    /// Write a line protocol body (nanosecond precision), retrying transient
    /// failures with exponential backoff. `body` is cheap to clone.
    pub fn write_body(&self, bucket: &str, body: bytes::Bytes, gzip: bool) -> anyhow::Result<()> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut rb = self
                .req(Method::POST, "/api/v2/write")
                .query(&[
                    ("org", self.org.as_str()),
                    ("bucket", bucket),
                    ("precision", "ns"),
                ])
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(body.clone());
            if gzip {
                rb = rb.header("Content-Encoding", "gzip");
            }
            let (reason, wait) = match rb.send() {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) => {
                    let status = resp.status();
                    let wait = resp
                        .headers()
                        .get("Retry-After")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok());
                    let msg = body_message(resp);
                    match status {
                        StatusCode::TOO_MANY_REQUESTS
                        | StatusCode::INTERNAL_SERVER_ERROR
                        | StatusCode::BAD_GATEWAY
                        | StatusCode::SERVICE_UNAVAILABLE
                        | StatusCode::GATEWAY_TIMEOUT => (format!("{status} {msg}"), wait),
                        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                            return Err(fail(
                                Kind::Auth,
                                format!("writing to `{bucket}`: {status} {msg}"),
                            )
                            .hint("the token needs write access to the target bucket")
                            .into());
                        }
                        StatusCode::NOT_FOUND => {
                            return Err(fail(
                                Kind::NotFound,
                                format!("writing to `{bucket}`: {status} {msg}"),
                            )
                            .into());
                        }
                        _ => {
                            return Err(fail(Kind::WriteRejected, format!("InfluxDB rejected points in `{bucket}`: {status} {msg}"))
                                .hint("a `field type conflict` means the target already holds this field with another type; migrate into a fresh bucket")
                                .into());
                        }
                    }
                }
                Err(e) => (e.to_string(), None),
            };
            if attempt > self.write_retries {
                return Err(fail(
                    Kind::Connection,
                    format!("writing to `{bucket}` failed after {attempt} attempts: {reason}"),
                )
                .hint("InfluxDB is overloaded or unreachable; rerun the same command to resume")
                .into());
            }
            let secs = wait.unwrap_or(1 << (attempt - 1).min(5));
            warn!(attempt, retry_in_secs = secs, reason = %reason, "write failed, retrying");
            std::thread::sleep(Duration::from_secs(secs));
        }
    }
}
