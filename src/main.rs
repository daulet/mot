use clap::Parser;
use mot::{
    ScanOptions, SessionSummary, build_topbar_snapshot, collect_usage, list_session_summaries,
    parse_time_window, render_report, resolve_scope_root, resolve_session_selection,
};
use std::io::{self, IsTerminal, Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};

const SESSION_PICKER_VISIBLE_ROWS: usize = 12;

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

    #[arg(
        long,
        help = "Emit 7-day menu bar JSON with daily totals and estimated cost"
    )]
    topbar_json: bool,

    #[arg(long, help = "Disable parallel parsing")]
    no_parallel: bool,

    #[arg(
        long,
        visible_alias = "since",
        value_name = "DURATION",
        help = "Only include usage in trailing window, e.g. 1d, 7d, 1m, 1y"
    )]
    window: Option<String>,

    #[arg(
        long,
        value_name = "ID_OR_PATH",
        help = "Only include a single local session by id, id prefix, file name, or JSONL/settings path"
    )]
    session: Option<String>,

    #[arg(
        long = "select-session",
        help = "Interactively choose a local session in the current scope before aggregating usage"
    )]
    select_session: bool,

    #[arg(
        long = "ssh-host",
        value_name = "HOST",
        help = "Aggregate usage from a remote VM over SSH; repeat to scan multiple hosts"
    )]
    ssh_hosts: Vec<String>,

    #[arg(long, value_name = "PATH", hide = true)]
    codex_root: Option<PathBuf>,

    #[arg(long, value_name = "PATH", hide = true)]
    claude_root: Option<PathBuf>,

    #[arg(long, value_name = "PATH", hide = true)]
    droid_root: Option<PathBuf>,
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

    if cli.session.is_some() && cli.select_session {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--session and --select-session cannot be used together",
        )
        .into());
    }

    let has_ssh_hosts = !cli.ssh_hosts.is_empty();

    let mut options = ScanOptions {
        global: cli.global,
        root: resolve_scope_root(cli.root),
        parallel: !cli.no_parallel,
        ssh_hosts: cli.ssh_hosts,
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
    if let Some(droid_root) = cli.droid_root {
        options.droid_root = droid_root;
    }

    if (cli.session.is_some() || cli.select_session) && has_ssh_hosts {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "session filtering is local-only and cannot be combined with --ssh-host",
        )
        .into());
    }

    if let Some(session) = &cli.session {
        options.selected_session = Some(
            resolve_session_selection(&options, session).map_err(|message| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
            })?,
        );
    } else if cli.select_session {
        let Some(session) = select_session_interactively(&options)? else {
            return Ok(());
        };
        options.selected_session = Some(session);
    }

    let report = collect_usage(&options);

    if cli.topbar_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&build_topbar_snapshot(&report, 7))?
        );
        return Ok(());
    }

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print!("{}", render_report(&report));
    Ok(())
}

fn select_session_interactively(
    options: &ScanOptions,
) -> Result<Option<SessionSummary>, Box<dyn std::error::Error>> {
    if !io::stdin().is_terminal() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--select-session requires an interactive stdin",
        )
        .into());
    }

    let mut sessions = list_session_summaries(options);
    if sessions.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("no local sessions found for {}", scope_label(options)),
        )
        .into());
    }

    if sessions.len() == 1 {
        eprintln!("Selected only matching session:");
        eprintln!(
            "{}",
            fit_terminal_line(
                &format_session_menu_row(1, &sessions[0]),
                terminal_width().saturating_sub(1).max(20),
            )
        );
        return Ok(sessions.pop());
    }

    select_session_with_arrows(&mut sessions, &scope_label(options))
}

fn select_session_with_arrows(
    sessions: &mut Vec<SessionSummary>,
    scope: &str,
) -> Result<Option<SessionSummary>, Box<dyn std::error::Error>> {
    let _raw_terminal = RawTerminal::enter()?;
    let mut selected = 0usize;
    let mut offset = 0usize;
    let mut stdin = io::stdin();
    let mut stderr = io::stderr();
    let mut needs_render = true;

    loop {
        if needs_render {
            clamp_session_picker_window(
                sessions.len(),
                selected,
                &mut offset,
                picker_visible_rows(),
            );
            render_session_picker(&mut stderr, sessions, scope, selected, offset)?;
            needs_render = false;
        }

        let mut byte = [0u8; 1];
        if stdin.read(&mut byte)? == 0 {
            continue;
        }

        match byte[0] {
            b'\r' | b'\n' => {
                clear_session_picker(&mut stderr)?;
                return Ok(Some(sessions.remove(selected)));
            }
            b'q' | b'Q' | 0x03 | 0x04 | 0x1b => {
                if byte[0] != 0x1b {
                    clear_session_picker(&mut stderr)?;
                    return Ok(None);
                }

                match read_escape_sequence(&mut stdin)? {
                    PickerKey::Up => selected = selected.saturating_sub(1),
                    PickerKey::Down => {
                        selected = (selected + 1).min(sessions.len().saturating_sub(1));
                    }
                    PickerKey::PageUp => selected = selected.saturating_sub(picker_visible_rows()),
                    PickerKey::PageDown => {
                        selected = (selected + picker_visible_rows())
                            .min(sessions.len().saturating_sub(1));
                    }
                    PickerKey::Home => selected = 0,
                    PickerKey::End => selected = sessions.len().saturating_sub(1),
                    PickerKey::UnknownEsc => {
                        clear_session_picker(&mut stderr)?;
                        return Ok(None);
                    }
                }
                needs_render = true;
            }
            b'k' => {
                selected = selected.saturating_sub(1);
                needs_render = true;
            }
            b'j' => {
                selected = (selected + 1).min(sessions.len().saturating_sub(1));
                needs_render = true;
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PickerKey {
    Up,
    Down,
    PageUp,
    PageDown,
    Home,
    End,
    UnknownEsc,
}

fn read_escape_sequence(stdin: &mut io::Stdin) -> io::Result<PickerKey> {
    let mut sequence = [0u8; 2];
    if stdin.read_exact(&mut sequence[..1]).is_err() {
        return Ok(PickerKey::UnknownEsc);
    }
    if sequence[0] != b'[' {
        return Ok(PickerKey::UnknownEsc);
    }
    if stdin.read_exact(&mut sequence[1..2]).is_err() {
        return Ok(PickerKey::UnknownEsc);
    }

    match sequence[1] {
        b'A' => Ok(PickerKey::Up),
        b'B' => Ok(PickerKey::Down),
        b'H' => Ok(PickerKey::Home),
        b'F' => Ok(PickerKey::End),
        b'1' | b'5' | b'6' => {
            let mut terminator = [0u8; 1];
            if stdin.read_exact(&mut terminator).is_err() {
                return Ok(PickerKey::UnknownEsc);
            }
            match (sequence[1], terminator[0]) {
                (b'1', b'~') => Ok(PickerKey::Home),
                (b'5', b'~') => Ok(PickerKey::PageUp),
                (b'6', b'~') => Ok(PickerKey::PageDown),
                _ => Ok(PickerKey::UnknownEsc),
            }
        }
        b'4' => {
            let mut terminator = [0u8; 1];
            if stdin.read_exact(&mut terminator).is_err() {
                return Ok(PickerKey::UnknownEsc);
            }
            if terminator[0] == b'~' {
                Ok(PickerKey::End)
            } else {
                Ok(PickerKey::UnknownEsc)
            }
        }
        _ => Ok(PickerKey::UnknownEsc),
    }
}

fn clamp_session_picker_window(
    total: usize,
    selected: usize,
    offset: &mut usize,
    visible_rows: usize,
) {
    let visible_rows = visible_rows.min(total);
    if selected < *offset {
        *offset = selected;
    } else if selected >= *offset + visible_rows {
        *offset = selected + 1 - visible_rows;
    }
}

fn render_session_picker(
    stderr: &mut io::Stderr,
    sessions: &[SessionSummary],
    scope: &str,
    selected: usize,
    offset: usize,
) -> io::Result<()> {
    let frame = build_session_picker_frame(
        sessions,
        scope,
        selected,
        offset,
        terminal_width(),
        terminal_height().unwrap_or(24),
    );

    write!(stderr, "\x1b[?25l\x1b[H\x1b[2J")?;
    for (idx, line) in frame.lines.iter().enumerate() {
        if idx > 0 {
            write!(stderr, "\r\n")?;
        }
        if idx == frame.selected_line {
            write!(stderr, "\x1b[7m{line}\x1b[0m")?;
        } else {
            write!(stderr, "{line}")?;
        }
    }

    stderr.flush()
}

struct PickerFrame {
    lines: Vec<String>,
    selected_line: usize,
}

fn build_session_picker_frame(
    sessions: &[SessionSummary],
    scope: &str,
    selected: usize,
    offset: usize,
    terminal_width: usize,
    terminal_height: usize,
) -> PickerFrame {
    let line_width = terminal_width.saturating_sub(1).max(1);
    let visible_rows = picker_visible_rows_for_height(terminal_height).min(sessions.len());
    let mut lines = Vec::with_capacity(visible_rows + 4);

    lines.push(fit_terminal_line(
        &format!("Select a session for {scope}"),
        line_width,
    ));
    lines.push(fit_terminal_line(
        "Up/Down or j/k to move, Enter to select, q/Esc to cancel.",
        line_width,
    ));
    lines.push(fit_terminal_line(&session_picker_header(), line_width));

    let selected_line = 3 + selected.saturating_sub(offset);
    for (idx, session) in sessions.iter().enumerate().skip(offset).take(visible_rows) {
        let prefix = if idx == selected { ">" } else { " " };
        lines.push(fit_terminal_line(
            &format!("{prefix} {}", format_session_menu_row(idx + 1, session)),
            line_width,
        ));
    }

    if sessions.len() > visible_rows {
        lines.push(fit_terminal_line(
            &format!(
                "Showing {}-{} of {}",
                offset + 1,
                offset + visible_rows,
                sessions.len()
            ),
            line_width,
        ));
    }

    PickerFrame {
        lines,
        selected_line,
    }
}

fn clear_session_picker(stderr: &mut io::Stderr) -> io::Result<()> {
    write!(stderr, "\x1b[?25h\x1b[H\x1b[2J")?;
    stderr.flush()
}

struct RawTerminal {
    original_state: String,
}

impl RawTerminal {
    fn enter() -> Result<Self, Box<dyn std::error::Error>> {
        let original_state = Command::new("stty")
            .arg("-g")
            .stdin(Stdio::inherit())
            .output()?;
        if !original_state.status.success() {
            return Err(std::io::Error::other("failed to read terminal state with stty").into());
        }

        let original_state = String::from_utf8(original_state.stdout)?.trim().to_string();
        let status = Command::new("stty")
            .args(["raw", "-echo", "min", "0", "time", "1"])
            .stdin(Stdio::inherit())
            .status()?;
        if !status.success() {
            return Err(std::io::Error::other("failed to enter raw terminal mode").into());
        }

        Ok(Self { original_state })
    }
}

impl Drop for RawTerminal {
    fn drop(&mut self) {
        let _ = Command::new("stty")
            .arg(&self.original_state)
            .stdin(Stdio::inherit())
            .status();
        let _ = write!(io::stderr(), "\x1b[?25h");
    }
}

fn scope_label(options: &ScanOptions) -> String {
    if options.global {
        "global scope".to_string()
    } else {
        options.root.display().to_string()
    }
}

fn format_session_menu_row(index: usize, session: &SessionSummary) -> String {
    let timestamp = session
        .started_at
        .as_deref()
        .or(session.updated_at.as_deref())
        .unwrap_or("unknown-time");
    let prompt = session.first_prompt.as_deref().unwrap_or("<no prompt>");
    format!(
        "{index:<4} {provider:<6} {timestamp:<20} {turns:>3} {turn_word:<5} {id:<12} {prompt}",
        provider = session.provider.label(),
        timestamp = compact_session_timestamp(timestamp),
        turns = session.turns,
        turn_word = if session.turns == 1 { "turn" } else { "turns" },
        id = short_session_id(&session.id),
    )
}

fn session_picker_header() -> String {
    format!(
        "  {:<4} {:<6} {:<20} {:>3} {:<5} {:<12} {}",
        "#", "src", "started", "n", "turns", "id", "first prompt"
    )
}

fn compact_session_timestamp(timestamp: &str) -> String {
    let timestamp = timestamp
        .strip_suffix('Z')
        .unwrap_or(timestamp)
        .split('.')
        .next()
        .unwrap_or(timestamp);
    timestamp.replace('T', " ").chars().take(19).collect()
}

fn short_session_id(id: &str) -> String {
    if id.chars().count() <= 12 {
        return id.to_string();
    }
    id.chars().take(12).collect()
}

fn terminal_width() -> usize {
    env_usize("COLUMNS")
        .or_else(stty_terminal_width)
        .unwrap_or(100)
        .max(2)
}

fn picker_visible_rows() -> usize {
    picker_visible_rows_for_height(terminal_height().unwrap_or(24))
}

fn picker_visible_rows_for_height(height: usize) -> usize {
    height
        .saturating_sub(4)
        .clamp(1, SESSION_PICKER_VISIBLE_ROWS)
}

fn terminal_height() -> Option<usize> {
    env_usize("LINES").or_else(stty_terminal_height)
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}

fn stty_terminal_width() -> Option<usize> {
    stty_terminal_size().map(|(_, columns)| columns)
}

fn stty_terminal_height() -> Option<usize> {
    stty_terminal_size().map(|(rows, _)| rows)
}

fn stty_terminal_size() -> Option<(usize, usize)> {
    let output = Command::new("stty")
        .arg("size")
        .stdin(Stdio::inherit())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let mut parts = text.split_whitespace();
    let rows = parts.next()?.parse().ok()?;
    let columns = parts.next()?.parse().ok()?;
    Some((rows, columns))
}

fn fit_terminal_line(line: &str, max_chars: usize) -> String {
    let mut out = String::with_capacity(max_chars.min(line.len()));
    let mut chars = line.chars();
    for _ in 0..max_chars {
        let Some(ch) = chars.next() else {
            return out;
        };
        out.push(ch);
    }
    if chars.next().is_some() && max_chars >= 3 {
        for _ in 0..3 {
            out.pop();
        }
        out.push_str("...");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{Cli, SessionSummary};
    use clap::Parser;
    use mot::SessionProvider;
    use std::path::PathBuf;

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

    #[test]
    fn ssh_host_flag_is_collected() {
        let parsed = Cli::try_parse_from(["mot", "--ssh-host", "hwvm"]).expect("parse ssh host");
        assert_eq!(parsed.ssh_hosts, vec!["hwvm"]);
    }

    #[test]
    fn session_filter_flag_is_collected() {
        let parsed = Cli::try_parse_from(["mot", "--session", "abc123"]).expect("parse session");
        assert_eq!(parsed.session.as_deref(), Some("abc123"));
    }

    #[test]
    fn select_session_flag_is_collected() {
        let parsed =
            Cli::try_parse_from(["mot", "--select-session"]).expect("parse select session");
        assert!(parsed.select_session);
    }

    #[test]
    fn picker_line_fitting_truncates_to_width() {
        let fitted = super::fit_terminal_line("abcdefghijklmnopqrstuvwxyz", 10);
        assert_eq!(fitted, "abcdefg...");
        assert_eq!(fitted.chars().count(), 10);
    }

    #[test]
    fn session_timestamp_is_compact_without_fractional_suffix() {
        assert_eq!(
            super::compact_session_timestamp("2026-04-15T00:16:33.788Z"),
            "2026-04-15 00:16:33"
        );
    }

    #[test]
    fn picker_frame_fits_width_and_height() {
        let sessions = (0..20)
            .map(|idx| SessionSummary {
                provider: SessionProvider::Codex,
                id: format!("session-{idx:02}-very-long-id"),
                path: PathBuf::from(format!("session-{idx}.jsonl")),
                cwd: None,
                started_at: Some("2026-04-15T00:16:33.788Z".to_string()),
                updated_at: None,
                turns: idx + 1,
                first_prompt: Some("a very long first prompt that should not wrap".to_string()),
            })
            .collect::<Vec<_>>();

        let frame = super::build_session_picker_frame(&sessions, "global scope", 4, 0, 40, 8);
        assert!(frame.lines.len() <= 8);
        assert_eq!(frame.selected_line, 7);
        assert!(frame.lines.iter().all(|line| line.chars().count() <= 39));
    }
}
