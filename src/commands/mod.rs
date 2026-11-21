pub mod analyze;
pub mod convert_value;
pub mod cutover;
pub mod migrate;
pub mod verify;

use crate::cli::Global;
use crate::error::{Kind, fail};
use crate::influx::Client;
use tracing::info;

/// Build a client and make sure InfluxDB answers before doing real work.
pub fn connect(g: &Global) -> anyhow::Result<Client> {
    if g.org.is_empty() {
        return Err(fail(Kind::Usage, "no organisation given")
            .hint("pass --org or set INFLUX_ORG")
            .into());
    }
    let client = Client::new(&g.url, &g.token, &g.org, g.timeout_secs)?;
    let version = client.ping()?;
    info!(url = %g.url, influxdb_version = %version, org = %g.org, "connected");
    Ok(client)
}

/// Client for the target server (`--target-*`, defaulting to the source).
pub fn connect_target(g: &Global) -> anyhow::Result<Client> {
    if g.target_url.is_none() && g.target_token.is_none() && g.target_org.is_none() {
        return connect(g);
    }
    let url = g.target_url.as_deref().unwrap_or(&g.url);
    let token = g.target_token.as_deref().unwrap_or(&g.token);
    let org = g.target_org.as_deref().unwrap_or(&g.org);
    if org.is_empty() {
        return Err(fail(Kind::Usage, "no target organisation given")
            .hint("pass --target-org or --org")
            .into());
    }
    let client = Client::new(url, token, org, g.timeout_secs)?;
    let version = client.ping()?;
    info!(target_url = %url, influxdb_version = %version, org = %org, "connected to target");
    Ok(client)
}

/// Resource use, reported in every result.
#[derive(serde::Serialize, Default)]
pub struct Resources {
    pub peak_rss_mib: Option<u64>,
    pub memory_limit_mib: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub influx_heap_start_mib: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub influx_heap_peak_mib: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub influx_paused_secs: Option<u64>,
}

/// Quote an argument for a copy-paste shell command in `next_steps`.
pub fn sh(s: &str) -> String {
    if !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_./:=@".contains(c))
    {
        s.to_owned()
    } else {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
}

pub fn date(ns: i64) -> String {
    crate::window::fmt_time(ns)
}

pub fn human_duration(secs: i64) -> String {
    match secs {
        0 => "infinite".into(),
        s if s % 86_400 == 0 => format!("{}d", s / 86_400),
        s if s % 3600 == 0 => format!("{}h", s / 3600),
        s => format!("{s}s"),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn shell_quoting() {
        assert_eq!(super::sh("loxinflux"), "loxinflux");
        assert_eq!(super::sh("a b"), "'a b'");
        assert_eq!(super::sh("it's"), "'it'\\''s'");
    }
}
