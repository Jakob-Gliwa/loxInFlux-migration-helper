//! Command line interface. Every option can also come from an environment
//! variable so that agents and scripts never need interactive input.

use crate::convert::Rules;
use crate::output::Format;
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "loxinflux-migration-helper",
    version,
    about = "Migrate an InfluxDB v2 bucket written by loxInFlux 0.3 to the float-only schema of loxInFlux 0.5",
    long_about = "Migrate an InfluxDB v2 bucket written by loxInFlux 0.3 to the float-only schema of loxInFlux 0.5.\n\n\
Typical sequence:\n  \
1. analyze   - what types are stored, where do conflicts hide (read-only)\n  \
2. migrate   - copy into a new bucket, converting values (resumable; --dry-run first)\n  \
3. verify    - compare written vs stored counts per window\n  \
4. cutover   - swap bucket names (check mode by default; --execute --yes to act)\n\n\
The source bucket is never modified. stdout carries one result (use --output json for machines),\n\
stderr carries logs. Exit codes: 0 ok, 1 internal, 2 usage, 3 connection, 4 auth, 5 not found,\n\
6 write rejected, 7 verify mismatch, 8 refused, 9 resource limit.",
    after_help = "Docs: https://github.com/Jakob-Gliwa/loxInFlux-migration-helper"
)]
pub struct Cli {
    #[command(flatten)]
    pub global: Global,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Args)]
pub struct Global {
    /// InfluxDB v2 base URL (the source; also the target unless --target-url is set).
    #[arg(
        long,
        env = "INFLUX_URL",
        default_value = "http://localhost:8086",
        global = true
    )]
    pub url: String,
    /// API token (all-access or read source + write target; cutover needs bucket write).
    #[arg(
        long,
        env = "INFLUX_TOKEN",
        hide_env_values = true,
        default_value = "",
        global = true
    )]
    pub token: String,
    /// Organisation name.
    #[arg(long, env = "INFLUX_ORG", default_value = "", global = true)]
    pub org: String,
    /// Target InfluxDB for migrate/verify, if it is another server. Default: --url.
    #[arg(long, env = "INFLUX_TARGET_URL", global = true)]
    pub target_url: Option<String>,
    /// Token for --target-url. Default: --token.
    #[arg(
        long,
        env = "INFLUX_TARGET_TOKEN",
        hide_env_values = true,
        global = true
    )]
    pub target_token: Option<String>,
    /// Organisation on --target-url. Default: --org.
    #[arg(long, env = "INFLUX_TARGET_ORG", global = true)]
    pub target_org: Option<String>,
    /// Result format on stdout.
    #[arg(
        long,
        env = "LMH_OUTPUT",
        value_enum,
        default_value = "human",
        global = true
    )]
    pub output: Format,
    /// Log format on stderr.
    #[arg(
        long,
        env = "LMH_LOG_FORMAT",
        value_enum,
        default_value = "text",
        global = true
    )]
    pub log_format: LogFormat,
    /// Log level (error, warn, info, debug, trace) or a tracing filter.
    #[arg(long, env = "LMH_LOG_LEVEL", default_value = "info", global = true)]
    pub log_level: String,
    /// HTTP timeout per request in seconds (one window's query must finish within it).
    #[arg(long, env = "LMH_TIMEOUT_SECS", default_value_t = 900, global = true)]
    pub timeout_secs: u64,
    /// Concurrent queries (windows read in parallel).
    #[arg(long, env = "LMH_PARALLEL", default_value_t = 2, global = true)]
    pub parallel: usize,
    /// Abort cleanly (exit 9) if this process uses more resident memory than this (Linux).
    #[arg(
        long,
        env = "LMH_MEMORY_LIMIT_MB",
        default_value_t = 512,
        global = true
    )]
    pub memory_limit_mb: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}

#[derive(Subcommand)]
pub enum Command {
    /// Read-only inventory: field types per window, conflicts, string values.
    Analyze(AnalyzeArgs),
    /// Copy a bucket (or a line protocol export) into a target bucket, converting values.
    Migrate(MigrateArgs),
    /// Compare the counts recorded by `migrate` with what the target bucket holds.
    Verify(VerifyArgs),
    /// Swap bucket names after a migration (check only unless --execute --yes).
    Cutover(CutoverArgs),
    /// Show how one value would be converted (offline, no InfluxDB needed).
    ConvertValue(ConvertValueArgs),
}

#[derive(Args)]
pub struct Range {
    /// Start of the time range (YYYY-MM-DD or RFC 3339), inclusive.
    #[arg(long)]
    pub start: String,
    /// End of the time range (YYYY-MM-DD, RFC 3339 or `now`), exclusive.
    #[arg(long, default_value = "now")]
    pub stop: String,
    /// Only these measurements (repeatable). Default: all.
    #[arg(long = "measurement", value_name = "NAME")]
    pub measurements: Vec<String>,
    /// Query window (e.g. 1d, 6h). Must divide one day; default is shard-safe (1d for day-based shards).
    #[arg(long)]
    pub window: Option<String>,
}

#[derive(Args)]
pub struct AnalyzeArgs {
    /// Bucket to inspect.
    #[arg(long)]
    pub bucket: String,
    #[command(flatten)]
    pub range: Range,
    /// Also read every string value and classify it (number, number with unit, text, empty, placeholder). Slower.
    #[arg(long)]
    pub classify_strings: bool,
    /// How many entries per list the human summary shows (JSON always has all).
    #[arg(long, default_value_t = 15)]
    pub top: usize,
}

#[derive(Args)]
pub struct RuleArgs {
    /// Keep booleans as booleans (default: true/false -> 1.0/0.0, like loxInFlux 0.5).
    #[arg(long)]
    pub keep_bools: bool,
    /// Do not turn "19.8°" or "523ppm" into numbers (loxInFlux: processing.parse_numeric_units = false).
    #[arg(long)]
    pub no_parse_units: bool,
    /// Keep empty strings instead of dropping them.
    #[arg(long)]
    pub keep_empty: bool,
    /// Keep Loxone placeholders such as "<v.1>" instead of dropping them.
    #[arg(long)]
    pub keep_placeholders: bool,
    /// Keep text in the original field instead of __text__<field> (risks new type conflicts).
    #[arg(long)]
    pub no_separate_text: bool,
}

impl RuleArgs {
    pub fn rules(&self) -> Rules {
        Rules {
            bool_to_float: !self.keep_bools,
            parse_numeric_units: !self.no_parse_units,
            drop_empty: !self.keep_empty,
            drop_placeholders: !self.keep_placeholders,
            separate_text_fields: !self.no_separate_text,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum OnReject {
    /// Stop at the first rejected batch (default).
    Abort,
    /// Log it, count it, continue; the run still ends with exit code 6.
    Skip,
}

#[derive(Args)]
pub struct MigrateArgs {
    /// Bucket to read from (never modified). Not needed with --from-lp.
    #[arg(long, required_unless_present = "from_lp")]
    pub source_bucket: Option<String>,
    /// Bucket to write to.
    #[arg(long)]
    pub target_bucket: String,
    /// Read line protocol, e.g. `influxd inspect export-lp ... --output-path -`, from a file (`-` = stdin).
    #[arg(long, value_name = "FILE", conflicts_with = "source_bucket")]
    pub from_lp: Option<PathBuf>,
    /// Start of the time range (API source).
    #[arg(long, required_unless_present = "from_lp")]
    pub start: Option<String>,
    /// End of the time range (API source).
    #[arg(long, default_value = "now")]
    pub stop: String,
    /// Only these measurements (repeatable). Others are NOT copied. Default: all.
    #[arg(long = "measurement", value_name = "NAME")]
    pub measurements: Vec<String>,
    /// Convert only points with this tag (repeatable, OR), e.g. application=loxInFlux. Other points are copied unchanged.
    #[arg(long = "convert-where-tag", value_name = "KEY=VALUE")]
    pub convert_where_tag: Vec<String>,
    /// Query window / verify granularity (e.g. 1d, 6h). Default is shard-safe.
    #[arg(long)]
    pub window: Option<String>,
    /// Create the target bucket (same retention and shard group duration as the source) if it is missing.
    #[arg(long)]
    pub create_target: bool,
    /// Read and convert, but write nothing. Uses its own state file.
    #[arg(long)]
    pub dry_run: bool,
    /// Checkpoint file. Default: ./lmh-state-<source>-to-<target>[-dry-run].jsonl
    #[arg(long)]
    pub state: Option<PathBuf>,
    /// Redo windows the state file marks as finished (within --start/--stop).
    #[arg(long)]
    pub redo: bool,
    /// Parallel write requests.
    #[arg(long, env = "LMH_WRITERS", default_value_t = 2)]
    pub writers: usize,
    /// Field values per write request.
    #[arg(long, default_value_t = 10_000)]
    pub batch_size: u64,
    /// Maximum uncompressed size of one write request, in MiB.
    #[arg(long, default_value_t = 4)]
    pub batch_mb: usize,
    /// Limit the write rate in field values per second (0 = unlimited).
    #[arg(long, env = "LMH_MAX_RATE", default_value_t = 0)]
    pub max_rate: u64,
    /// Send uncompressed (saves CPU on both sides when InfluxDB is local).
    #[arg(long)]
    pub no_gzip: bool,
    /// Pause writes while InfluxDB's Go heap is this many MiB above its value at start (0 = off).
    #[arg(long, env = "LMH_INFLUX_HEAP_HEADROOM_MB", default_value_t = 1024)]
    pub influx_heap_headroom_mb: u64,
    /// Give up (exit 9) if writes stay paused by the InfluxDB memory guard this long.
    #[arg(long, default_value_t = 600)]
    pub max_pause_secs: u64,
    /// What to do when InfluxDB rejects a batch.
    #[arg(long, value_enum, default_value = "abort")]
    pub on_reject: OnReject,
    #[command(flatten)]
    pub rules: RuleArgs,
}

#[derive(Args)]
pub struct VerifyArgs {
    /// State file written by `migrate`.
    #[arg(long)]
    pub state: PathBuf,
    /// How many mismatching measurements the human summary lists.
    #[arg(long, default_value_t = 15)]
    pub top: usize,
    /// Also compare every value: read source and target, replay the conversion,
    /// compare count and checksum per series and field. Reads both buckets fully.
    #[arg(long)]
    pub deep: bool,
    /// With --deep: only every Nth window gets the deep comparison (counts are checked for all).
    #[arg(long, default_value_t = 1)]
    pub sample_every: usize,
    /// With --deep: how both buckets are read. `flux` uses the query API; `export-lp`
    /// runs `influxd inspect export-lp` on the storage files (faster, needs access to
    /// the engine directory; windows that differ are re-checked through Flux).
    #[arg(long, env = "LMH_READ_VIA", value_enum, default_value = "flux")]
    pub read_via: ReadVia,
    /// export-lp: storage engine directory as the influxd command sees it.
    #[arg(
        long,
        env = "LMH_ENGINE_PATH",
        default_value = "/var/lib/influxdb2/engine"
    )]
    pub engine_path: String,
    /// export-lp: command that runs influxd, split on spaces
    /// (e.g. "docker exec influxdb influxd" when InfluxDB runs in a container).
    #[arg(long, env = "LMH_INFLUXD", default_value = "influxd")]
    pub influxd: String,
    /// export-lp: days read by one export call per bucket, aligned like shard
    /// groups. 0 = the larger shard group duration of the two buckets.
    #[arg(long, default_value_t = 0)]
    pub export_chunk_days: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ReadVia {
    Flux,
    ExportLp,
}

#[derive(Args)]
pub struct CutoverArgs {
    /// Current production bucket (will be renamed to --archive-name).
    #[arg(long)]
    pub source_bucket: String,
    /// Migrated bucket (will take over the source bucket's name).
    #[arg(long)]
    pub target_bucket: String,
    /// New name for the old bucket. Default: <source>_pre_migration
    #[arg(long)]
    pub archive_name: Option<String>,
    /// Actually rename. Without it, only checks and prints the plan.
    #[arg(long)]
    pub execute: bool,
    /// Confirm --execute (required; there is no interactive prompt).
    #[arg(long)]
    pub yes: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ValueType {
    String,
    Float,
    Integer,
    Unsigned,
    Boolean,
}

#[derive(Args)]
pub struct ConvertValueArgs {
    /// Field name, e.g. Default or AQ.
    #[arg(long, default_value = "Default")]
    pub field: String,
    /// The stored value, without line protocol quoting.
    #[arg(long, allow_hyphen_values = true)]
    pub value: String,
    /// Stored type.
    #[arg(long = "type", value_enum, default_value = "string")]
    pub value_type: ValueType,
    #[command(flatten)]
    pub rules: RuleArgs,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn from_lp_replaces_source_and_start() {
        let cli =
            Cli::try_parse_from(["x", "migrate", "--target-bucket", "t", "--from-lp", "a.lp"]);
        assert!(cli.is_ok());
        let cli = Cli::try_parse_from(["x", "migrate", "--target-bucket", "t"]);
        assert!(cli.is_err());
    }
}
