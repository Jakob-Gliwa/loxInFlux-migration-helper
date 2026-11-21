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
