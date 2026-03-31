use clap::Parser;
use mot::{ScanOptions, collect_usage, parse_time_window, render_report, resolve_scope_root};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "mot",
    version,
    about = "Fast CLI to aggregate LLM token usage from Codex and Claude Code metadata"
)]
struct Cli {
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

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
