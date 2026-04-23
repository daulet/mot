use clap::Parser;
use mot::{
    ScanOptions, SessionSummary, TopBarSnapshot, collect_usage, list_session_summaries,
    parse_time_window, render_report, resolve_scope_root, resolve_session_selection,
};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use std::collections::HashMap;
use std::io::{self, IsTerminal, Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use time::{Date, Duration, Month, OffsetDateTime, Weekday};

const ACTIVITY_CALENDAR_WEEKS: usize = 53;
const ACTIVITY_CALENDAR_DAYS: usize = ACTIVITY_CALENDAR_WEEKS * 7;
const ACTIVITY_CALENDAR_HEIGHT: usize = 12;
const ACTIVITY_LABEL_WIDTH: usize = 4;
const ACTIVITY_CELL: &str = "■";
const WORDS_PER_TOKEN_ESTIMATE: f64 = 0.75;
const BOOK_TOKEN_COMPARISONS: &[BookTokenComparison] = &[
    BookTokenComparison {
        title: "The Harry Potter series",
        word_count: 1_083_594,
    },
    BookTokenComparison {
        title: "The Lord of the Rings",
        word_count: 481_103,
    },
    BookTokenComparison {
        title: "War and Peace",
        word_count: 587_287,
    },
    BookTokenComparison {
        title: "The Hobbit",
        word_count: 95_022,
    },
    BookTokenComparison {
        title: "Moby-Dick",
        word_count: 206_052,
    },
    BookTokenComparison {
        title: "Pride and Prejudice",
        word_count: 122_000,
    },
    BookTokenComparison {
        title: "The Great Gatsby",
        word_count: 47_000,
    },
    BookTokenComparison {
        title: "The Hunger Games trilogy",
        word_count: 301_583,
    },
    BookTokenComparison {
        title: "A Song of Ice and Fire books 1-5",
        word_count: 1_736_054,
    },
    BookTokenComparison {
        title: "The Chronicles of Narnia",
        word_count: 345_000,
    },
];
const SESSION_PICKER_VISIBLE_ROWS: usize = 12;

#[derive(Debug, Clone, Copy)]
struct BookTokenComparison {
    title: &'static str,
    word_count: u64,
}

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

    #[arg(long, help = "Hide the Ratatui activity calendar in table output")]
    no_activity_calendar: bool,

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
    let show_activity_calendar = !cli.no_activity_calendar;

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
            serde_json::to_string_pretty(&mot::build_topbar_snapshot(&report, 7))?
        );
        return Ok(());
    }

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print!("{}", render_report(&report));
    if show_activity_calendar {
        render_activity_calendar(&report)?;
    }
    Ok(())
}

fn render_activity_calendar(report: &mot::UsageReport) -> io::Result<()> {
    let mut stdout = io::stdout();
    if !stdout.is_terminal() {
        return Ok(());
    }

    let snapshot = mot::build_topbar_snapshot(report, ACTIVITY_CALENDAR_DAYS);
    let lines = activity_calendar_lines(&snapshot, terminal_width() as u16);

    writeln!(stdout)?;
    write!(stdout, "{}", activity_lines_to_ansi(&lines))?;
    stdout.flush()
}

fn activity_calendar_lines(snapshot: &TopBarSnapshot, width: u16) -> Vec<Line<'static>> {
    let weeks = activity_calendar_visible_weeks(width);
    let gap = activity_calendar_has_gap(width, weeks);
    let today = OffsetDateTime::now_utc().date();
    let start = activity_calendar_start_date(today, weeks);
    let day_totals = activity_day_totals(snapshot);
    let (active_days, total_tokens, max_tokens) =
        activity_calendar_totals(start, today, weeks, &day_totals);
    let streaks = activity_streaks(start, today, weeks, &day_totals);

    let mut lines = Vec::with_capacity(ACTIVITY_CALENDAR_HEIGHT);
    lines.push(Line::from(vec![
        Span::styled(
            "Activity calendar",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(
            " - last {weeks} weeks, {active_days} active days, {} tokens",
            format_calendar_count(total_tokens)
        )),
    ]));
    lines.push(activity_month_label_line(start, today, weeks, gap));
    for row in 0..7 {
        lines.push(activity_day_row(
            row,
            start,
            today,
            weeks,
            gap,
            &day_totals,
            max_tokens,
        ));
    }
    lines.push(activity_legend_line(max_tokens, gap));
    lines.push(activity_streak_line(streaks));
    lines.push(activity_book_scale_line(
        snapshot.total_tokens,
        OffsetDateTime::now_utc().unix_timestamp(),
    ));

    lines
}

fn activity_lines_to_ansi(lines: &[Line<'_>]) -> String {
    let mut out = String::new();
    for line in lines {
        for span in &line.spans {
            let styled = push_ansi_style(&mut out, span.style);
            out.push_str(span.content.as_ref());
            if styled {
                out.push_str("\x1b[0m");
            }
        }
        out.push('\n');
    }
    out
}

fn push_ansi_style(out: &mut String, style: Style) -> bool {
    let mut codes = Vec::new();
    if style.add_modifier.contains(Modifier::BOLD) {
        codes.push("1".to_string());
    }
    if let Some(fg) = style.fg
        && let Some(code) = ansi_fg_code(fg)
    {
        codes.push(code);
    }

    if codes.is_empty() {
        return false;
    }

    out.push_str("\x1b[");
    out.push_str(&codes.join(";"));
    out.push('m');
    true
}

fn ansi_fg_code(color: Color) -> Option<String> {
    match color {
        Color::Black => Some("30".to_string()),
        Color::Red => Some("31".to_string()),
        Color::Green => Some("32".to_string()),
        Color::Yellow => Some("33".to_string()),
        Color::Blue => Some("34".to_string()),
        Color::Magenta => Some("35".to_string()),
        Color::Cyan => Some("36".to_string()),
        Color::Gray => Some("37".to_string()),
        Color::DarkGray => Some("90".to_string()),
        Color::LightRed => Some("91".to_string()),
        Color::LightGreen => Some("92".to_string()),
        Color::LightYellow => Some("93".to_string()),
        Color::LightBlue => Some("94".to_string()),
        Color::LightMagenta => Some("95".to_string()),
        Color::LightCyan => Some("96".to_string()),
        Color::White => Some("97".to_string()),
        Color::Rgb(r, g, b) => Some(format!("38;2;{r};{g};{b}")),
        Color::Indexed(index) => Some(format!("38;5;{index}")),
        Color::Reset => None,
    }
}

fn activity_day_totals(snapshot: &TopBarSnapshot) -> HashMap<Date, u64> {
    snapshot
        .days
        .iter()
        .filter_map(|day| Some((date_from_day_key(&day.day)?, day.total_tokens)))
        .collect()
}

fn activity_calendar_totals(
    start: Date,
    today: Date,
    weeks: usize,
    day_totals: &HashMap<Date, u64>,
) -> (usize, u64, u64) {
    let mut active_days = 0usize;
    let mut total_tokens = 0u64;
    let mut max_tokens = 0u64;

    for offset in 0..weeks * 7 {
        let date = start + Duration::days(offset as i64);
        if date > today {
            continue;
        }

        let tokens = day_totals.get(&date).copied().unwrap_or(0);
        if tokens > 0 {
            active_days += 1;
            total_tokens += tokens;
            max_tokens = max_tokens.max(tokens);
        }
    }

    (active_days, total_tokens, max_tokens)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActivityStreaks {
    current: usize,
    longest: usize,
}

fn activity_streaks(
    start: Date,
    today: Date,
    weeks: usize,
    day_totals: &HashMap<Date, u64>,
) -> ActivityStreaks {
    let mut longest = 0usize;
    let mut running = 0usize;
    let mut current = 0usize;

    for offset in 0..weeks * 7 {
        let date = start + Duration::days(offset as i64);
        if date > today {
            break;
        }

        if day_totals.get(&date).copied().unwrap_or(0) > 0 {
            running += 1;
            longest = longest.max(running);
            if date == today {
                current = running;
            }
        } else {
            running = 0;
            if date == today {
                current = 0;
            }
        }
    }

    ActivityStreaks { current, longest }
}

fn activity_calendar_visible_weeks(width: u16) -> usize {
    let available = usize::from(width).saturating_sub(ACTIVITY_LABEL_WIDTH);
    let cell_width = if available >= ACTIVITY_CALENDAR_WEEKS * 2 {
        2
    } else {
        1
    };
    (available / cell_width).clamp(1, ACTIVITY_CALENDAR_WEEKS)
}

fn activity_calendar_has_gap(width: u16, weeks: usize) -> bool {
    usize::from(width).saturating_sub(ACTIVITY_LABEL_WIDTH) >= weeks * 2
}

fn activity_calendar_start_date(today: Date, weeks: usize) -> Date {
    let day_offset = (weeks.saturating_sub(1) * 7) + weekday_index_sunday_start(today.weekday());
    today - Duration::days(day_offset as i64)
}

fn activity_month_label_line(start: Date, today: Date, weeks: usize, gap: bool) -> Line<'static> {
    let cell_width = if gap { 2 } else { 1 };
    let mut chars = vec![' '; weeks * cell_width];

    for offset in 0..weeks * 7 {
        let date = start + Duration::days(offset as i64);
        if date > today {
            break;
        }
        if date.day() == 1 {
            let col = offset / 7;
            place_month_label(&mut chars, col * cell_width, month_abbrev(date.month()));
        }
    }

    let mut line = String::from("    ");
    line.extend(chars);
    Line::raw(line)
}

fn place_month_label(chars: &mut [char], position: usize, label: &str) {
    for (idx, ch) in label.chars().enumerate() {
        if let Some(slot) = chars.get_mut(position + idx) {
            *slot = ch;
        }
    }
}

fn activity_day_row(
    row: usize,
    start: Date,
    today: Date,
    weeks: usize,
    gap: bool,
    day_totals: &HashMap<Date, u64>,
    max_tokens: u64,
) -> Line<'static> {
    let mut spans = Vec::with_capacity(1 + weeks * if gap { 2 } else { 1 });
    spans.push(Span::raw(weekday_label(row)));

    for week in 0..weeks {
        let date = start + Duration::days((week * 7 + row) as i64);
        if date > today {
            spans.push(Span::raw(" "));
        } else {
            let tokens = day_totals.get(&date).copied().unwrap_or(0);
            spans.push(activity_cell_span(tokens, max_tokens));
        }
        if gap {
            spans.push(Span::raw(" "));
        }
    }

    Line::from(spans)
}

fn activity_cell_span(tokens: u64, max_tokens: u64) -> Span<'static> {
    if tokens == 0 {
        Span::styled(ACTIVITY_CELL, Style::default().fg(Color::Rgb(48, 54, 61)))
    } else {
        Span::styled(
            ACTIVITY_CELL,
            activity_level_style(activity_level_for_tokens(tokens, max_tokens)),
        )
    }
}

fn activity_legend_line(max_tokens: u64, gap: bool) -> Line<'static> {
    let mut spans = vec![Span::raw("    Less ")];
    for level in 1..=4 {
        spans.push(Span::styled(ACTIVITY_CELL, activity_level_style(level)));
        spans.push(Span::raw(if gap { "  " } else { " " }));
    }
    spans.push(Span::raw("More"));
    if max_tokens == 0 {
        spans.push(Span::raw(" (no timestamped activity)"));
    }
    Line::from(spans)
}

fn activity_streak_line(streaks: ActivityStreaks) -> Line<'static> {
    Line::from(vec![
        Span::raw("    Current streak  "),
        activity_stat_value_span(streaks.current),
        Span::raw(format!(" {:<4}", plural_days(streaks.current))),
        Span::raw("    Longest streak  "),
        activity_stat_value_span(streaks.longest),
        Span::raw(format!(" {}", plural_days(streaks.longest))),
    ])
}

fn plural_days(days: usize) -> &'static str {
    if days == 1 { "day" } else { "days" }
}

fn activity_stat_value_span(value: usize) -> Span<'static> {
    Span::styled(
        format!("{value:>4}"),
        Style::default()
            .fg(Color::Rgb(63, 185, 80))
            .add_modifier(Modifier::BOLD),
    )
}

fn activity_book_scale_line(total_tokens: u64, seed: i64) -> Line<'static> {
    let book = select_book_token_comparison(seed);
    let estimated_book_tokens = estimated_tokens_for_words(book.word_count);
    let ratio = if estimated_book_tokens == 0 {
        0.0
    } else {
        total_tokens as f64 / estimated_book_tokens as f64
    };

    Line::from(vec![
        Span::raw("    Book scale       "),
        Span::styled(
            format!("~{}", format_ratio(ratio)),
            Style::default()
                .fg(Color::Rgb(63, 185, 80))
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(" as many tokens as {}", book.title)),
    ])
}

fn select_book_token_comparison(seed: i64) -> &'static BookTokenComparison {
    let index = seed.unsigned_abs() as usize % BOOK_TOKEN_COMPARISONS.len();
    &BOOK_TOKEN_COMPARISONS[index]
}

fn estimated_tokens_for_words(word_count: u64) -> u64 {
    ((word_count as f64) / WORDS_PER_TOKEN_ESTIMATE).round() as u64
}

fn format_ratio(value: f64) -> String {
    if value >= 100.0 {
        format!("{value:.0}x")
    } else if value >= 10.0 {
        format!("{value:.1}x")
    } else if value >= 1.0 {
        format!("{value:.2}x")
    } else if value >= 0.01 {
        format!("{value:.2}x")
    } else if value > 0.0 {
        "<0.01x".to_string()
    } else {
        "0x".to_string()
    }
}

fn activity_level_for_tokens(tokens: u64, max_tokens: u64) -> u8 {
    if tokens == 0 || max_tokens == 0 {
        return 0;
    }

    let ratio = (tokens as f64).ln_1p() / (max_tokens as f64).ln_1p();
    if ratio < 0.25 {
        1
    } else if ratio < 0.5 {
        2
    } else if ratio < 0.75 {
        3
    } else {
        4
    }
}

fn activity_level_style(level: u8) -> Style {
    let color = match level {
        1 => Color::Rgb(155, 233, 168),
        2 => Color::Rgb(63, 185, 80),
        3 => Color::Rgb(35, 134, 54),
        _ => Color::Rgb(12, 84, 33),
    };
    Style::default().fg(color).add_modifier(Modifier::BOLD)
}

fn weekday_label(row: usize) -> &'static str {
    match row {
        1 => "Mon ",
        3 => "Wed ",
        5 => "Fri ",
        _ => "    ",
    }
}

fn weekday_index_sunday_start(weekday: Weekday) -> usize {
    match weekday {
        Weekday::Sunday => 0,
        Weekday::Monday => 1,
        Weekday::Tuesday => 2,
        Weekday::Wednesday => 3,
        Weekday::Thursday => 4,
        Weekday::Friday => 5,
        Weekday::Saturday => 6,
    }
}

fn month_abbrev(month: Month) -> &'static str {
    match month {
        Month::January => "Jan",
        Month::February => "Feb",
        Month::March => "Mar",
        Month::April => "Apr",
        Month::May => "May",
        Month::June => "Jun",
        Month::July => "Jul",
        Month::August => "Aug",
        Month::September => "Sep",
        Month::October => "Oct",
        Month::November => "Nov",
        Month::December => "Dec",
    }
}

fn date_from_day_key(day: &str) -> Option<Date> {
    let mut parts = day.split('-');
    let year = parts.next()?.parse().ok()?;
    let month = Month::try_from(parts.next()?.parse::<u8>().ok()?).ok()?;
    let day = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Date::from_calendar_date(year, month, day).ok()
}

fn format_calendar_count(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    let len = digits.len();
    for (i, ch) in digits.chars().enumerate() {
        if i != 0 && (len - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
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
    fn no_activity_calendar_flag_is_collected() {
        let parsed = Cli::try_parse_from(["mot", "--no-activity-calendar"])
            .expect("parse no activity calendar");
        assert!(parsed.no_activity_calendar);
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

    #[test]
    fn activity_calendar_start_aligns_to_sunday() {
        let today =
            time::Date::from_calendar_date(2026, time::Month::April, 22).expect("valid date");
        let start = super::activity_calendar_start_date(today, 2);

        assert_eq!(start.weekday(), time::Weekday::Sunday);
        assert_eq!(
            start,
            time::Date::from_calendar_date(2026, time::Month::April, 12).expect("valid date")
        );
    }

    #[test]
    fn activity_calendar_level_increases_with_log_scaled_usage() {
        assert_eq!(super::activity_level_for_tokens(0, 100), 0);
        assert_eq!(super::activity_level_for_tokens(1, 1_000_000), 1);
        assert_eq!(super::activity_level_for_tokens(100, 1_000_000), 2);
        assert_eq!(super::activity_level_for_tokens(10_000, 1_000_000), 3);
        assert_eq!(super::activity_level_for_tokens(1_000_000, 1_000_000), 4);
    }

    #[test]
    fn activity_calendar_cells_use_square_glyphs() {
        assert_eq!(super::activity_cell_span(0, 100).content.as_ref(), "■");
        assert_eq!(super::activity_cell_span(10, 100).content.as_ref(), "■");
    }

    #[test]
    fn activity_streaks_report_current_and_longest_runs() {
        let start =
            time::Date::from_calendar_date(2026, time::Month::April, 12).expect("valid date");
        let today =
            time::Date::from_calendar_date(2026, time::Month::April, 22).expect("valid date");
        let mut day_totals = std::collections::HashMap::new();
        day_totals.insert(start, 1);
        day_totals.insert(start + time::Duration::days(1), 1);
        day_totals.insert(start + time::Duration::days(2), 1);
        day_totals.insert(today - time::Duration::days(1), 1);
        day_totals.insert(today, 1);

        let streaks = super::activity_streaks(start, today, 2, &day_totals);

        assert_eq!(streaks.current, 2);
        assert_eq!(streaks.longest, 3);
    }

    #[test]
    fn activity_streaks_current_is_zero_when_today_is_inactive() {
        let start =
            time::Date::from_calendar_date(2026, time::Month::April, 12).expect("valid date");
        let today =
            time::Date::from_calendar_date(2026, time::Month::April, 22).expect("valid date");
        let mut day_totals = std::collections::HashMap::new();
        day_totals.insert(today - time::Duration::days(2), 1);
        day_totals.insert(today - time::Duration::days(1), 1);

        let streaks = super::activity_streaks(start, today, 2, &day_totals);

        assert_eq!(streaks.current, 0);
        assert_eq!(streaks.longest, 2);
    }

    #[test]
    fn activity_streak_line_aligns_and_highlights_values() {
        let line = super::activity_streak_line(super::ActivityStreaks {
            current: 2,
            longest: 13,
        });

        assert_eq!(line.spans[0].content.as_ref(), "    Current streak  ");
        assert_eq!(line.spans[1].content.as_ref(), "   2");
        assert_eq!(line.spans[4].content.as_ref(), "  13");
        assert!(
            line.spans[1]
                .style
                .add_modifier
                .contains(ratatui::style::Modifier::BOLD)
        );
        assert!(
            line.spans[4]
                .style
                .add_modifier
                .contains(ratatui::style::Modifier::BOLD)
        );
    }

    #[test]
    fn activity_book_scale_line_formats_ratio_under_stats() {
        assert_eq!(super::estimated_tokens_for_words(75), 100);

        let line = super::activity_book_scale_line(1_444_792, 0);
        let text = line
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();

        assert!(text.contains("Book scale"));
        assert!(text.contains("~1.00x"));
        assert!(text.contains("The Harry Potter series"));
        assert!(
            line.spans[1]
                .style
                .add_modifier
                .contains(ratatui::style::Modifier::BOLD)
        );
    }
}
