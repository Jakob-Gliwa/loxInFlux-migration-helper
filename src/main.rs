mod cli;
mod commands;
mod convert;
mod deep;
mod error;
mod fast;
mod fastcsv;
mod fastlp;
mod flux;
mod guard;
mod influx;
mod lp;
mod output;
mod pipeline;
mod state;
mod window;

use clap::Parser as _;
use cli::{Cli, Command, LogFormat};
use std::io::IsTerminal;
use tracing_subscriber::EnvFilter;

fn init_logging(format: LogFormat, level: &str) {
    let filter = EnvFilter::try_from_env("RUST_LOG")
        .or_else(|_| EnvFilter::try_new(level))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        // Colour only for humans at a terminal; agents and log files get plain text.
        .with_ansi(std::io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none());
    match format {
        LogFormat::Text => builder.init(),
        LogFormat::Json => builder
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .init(),
    }
}

/// Command line errors honour `--output json` too, so agents always get JSON.
fn parse_cli() -> Cli {
    match Cli::try_parse() {
        Ok(c) => c,
        Err(e) => {
            use clap::error::ErrorKind;
            if matches!(
                e.kind(),
                ErrorKind::DisplayHelp
                    | ErrorKind::DisplayVersion
                    | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            ) {
                e.exit();
            }
            let args: Vec<String> = std::env::args().collect();
            let json = args
                .windows(2)
                .any(|w| w[0] == "--output" && w[1] == "json")
                || args.iter().any(|a| a == "--output=json")
                || std::env::var("LMH_OUTPUT").is_ok_and(|v| v == "json");
            if !json {
                e.exit();
            }
            let detail = e.to_string();
            let first = detail
                .lines()
                .next()
                .unwrap_or("invalid command line")
                .trim_start_matches("error: ")
                .to_owned();
            let code = output::emit(
                output::Format::Json,
                "cli",
                Err((
                    error::Kind::Usage,
                    first,
                    Some("run with --help for the options".to_owned()),
                )),
            );
            std::process::exit(code);
        }
    }
}

fn main() {
    let cli = parse_cli();
    init_logging(cli.global.log_format, &cli.global.log_level);
    guard::install_signal_handler();
    let g = &cli.global;
    let (name, result) = match &cli.command {
        Command::Analyze(a) => ("analyze", commands::analyze::run(g, a)),
        Command::Migrate(a) => ("migrate", commands::migrate::run(g, a)),
        Command::Verify(a) => ("verify", commands::verify::run(g, a)),
        Command::Cutover(a) => ("cutover", commands::cutover::run(g, a)),
        Command::ConvertValue(a) => ("convert-value", commands::convert_value::run(a)),
    };
    let result = result.map_err(|e| {
        let (kind, hint) = error::classify(&e);
        let message = format!("{e:#}");
        tracing::error!(kind = %output::kind_name(kind), "{message}");
        (kind, message, hint)
    });
    let code = output::emit(g.output, name, result);
    std::process::exit(code);
}
