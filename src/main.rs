use clap::Parser;
use mot::{ScanOptions, collect_usage, parse_time_window, render_report, resolve_scope_root};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "mot",
    version = env!("MOT_VERSION_STRING"),
    disable_version_flag = true,
    about = "Fast CLI to aggregate LLM token usage from Codex and Claude Code metadata"
)]
struct Cli {
    /// Print runtime version in the form <tag> or <tag>+<commit>
    #[arg(
        short = 'v',
        short_alias = 'V',
        long = "version",
        action = clap::ArgAction::SetTrue,
        global = true
    )]
    version: bool,

    #[arg(
        long,
        help = "Count usage globally across all discovered Codex/Claude sessions on this host"
    )]
    global: bool,

    #[arg(
        long,
        value_name = "PATH",
        help = "Project root for scoped mode (defaults to current directory)"
    )]
    root: Option<PathBuf>,

    #[arg(long, help = "Emit JSON output instead of table output")]
    json: bool,

    #[arg(long, help = "Disable parallel parsing")]
    no_parallel: bool,

    #[arg(
        long,
        visible_alias = "since",
        value_name = "DURATION",
        help = "Only include usage in trailing window, e.g. 1d, 7d, 1m, 1y"
    )]
    window: Option<String>,

    #[arg(long, value_name = "PATH", hide = true)]
    codex_root: Option<PathBuf>,

    #[arg(long, value_name = "PATH", hide = true)]
    claude_root: Option<PathBuf>,
}

fn resolve_runtime_version() -> &'static str {
    option_env!("MOT_VERSION_STRING").unwrap_or(env!("CARGO_PKG_VERSION"))
}

fn cli_version_text() -> String {
    format!("mot {}", resolve_runtime_version())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if cli.version {
        println!("{}", cli_version_text());
        return Ok(());
    }

    let mut options = ScanOptions {
        global: cli.global,
        root: resolve_scope_root(cli.root),
        parallel: !cli.no_parallel,
        ..ScanOptions::default()
    };

    if let Some(window_spec) = cli.window {
        let parsed = parse_time_window(&window_spec)
            .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?;
        options.window = Some(parsed);
    }

    if let Some(codex_root) = cli.codex_root {
        options.codex_root = codex_root;
    }
    if let Some(claude_root) = cli.claude_root {
        options.claude_root = claude_root;
    }

    let report = collect_usage(&options);

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print!("{}", render_report(&report));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::Parser;

    #[test]
    fn cli_version_text_includes_binary_name() {
        assert!(super::cli_version_text().starts_with("mot "));
    }

    #[test]
    fn short_v_flag_is_accepted_for_version() {
        let parsed = Cli::try_parse_from(["mot", "-v"]).expect("parse -v");
        assert!(parsed.version);
    }

    #[test]
    fn long_version_flag_is_accepted_for_version() {
        let parsed = Cli::try_parse_from(["mot", "--version"]).expect("parse --version");
        assert!(parsed.version);
    }
}
