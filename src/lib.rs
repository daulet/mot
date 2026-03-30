use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;
use walkdir::WalkDir;

#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct TokenTotals {
    pub input: u64,
    pub output: u64,
    pub thinking: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

impl TokenTotals {
    fn max_assign(&mut self, other: &Self) {
        self.input = self.input.max(other.input);
        self.output = self.output.max(other.output);
        self.thinking = self.thinking.max(other.thinking);
        self.cache_read = self.cache_read.max(other.cache_read);
        self.cache_write = self.cache_write.max(other.cache_write);
    }

    fn is_zero(&self) -> bool {
        self.input == 0
            && self.output == 0
            && self.thinking == 0
            && self.cache_read == 0
            && self.cache_write == 0
    }
}

impl std::ops::AddAssign for TokenTotals {
    fn add_assign(&mut self, rhs: Self) {
        self.input += rhs.input;
        self.output += rhs.output;
        self.thinking += rhs.thinking;
        self.cache_read += rhs.cache_read;
        self.cache_write += rhs.cache_write;
    }
}

impl std::ops::Add for TokenTotals {
    type Output = TokenTotals;

    fn add(self, rhs: Self) -> Self::Output {
        let mut out = self;
        out += rhs;
        out
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProviderReport {
    pub files_scanned: usize,
    pub records_counted: usize,
    pub parse_errors: usize,
    pub totals: TokenTotals,
}

impl std::ops::AddAssign for ProviderReport {
    fn add_assign(&mut self, rhs: Self) {
        self.files_scanned += rhs.files_scanned;
        self.records_counted += rhs.records_counted;
        self.parse_errors += rhs.parse_errors;
        self.totals += rhs.totals;
    }
}

impl std::ops::Add for ProviderReport {
    type Output = ProviderReport;

    fn add(self, rhs: Self) -> Self::Output {
        let mut out = self;
        out += rhs;
        out
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ScopeReport {
    pub mode: &'static str,
    pub root: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsageReport {
    pub scope: ScopeReport,
    pub codex: ProviderReport,
    pub claude: ProviderReport,
    pub total: TokenTotals,
    pub duration_ms: u128,
}

#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub global: bool,
    pub root: PathBuf,
    pub codex_root: PathBuf,
    pub claude_root: PathBuf,
    pub parallel: bool,
}

impl Default for ScanOptions {
    fn default() -> Self {
        let cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let home = env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/"));

        Self {
            global: false,
            root: cwd,
            codex_root: home.join(".codex").join("sessions"),
            claude_root: home.join(".claude").join("projects"),
            parallel: true,
        }
    }
}

pub fn resolve_scope_root(root: Option<PathBuf>) -> PathBuf {
    match root {
        Some(path) if path.is_absolute() => path,
        Some(path) => env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path),
        None => env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
    }
}

pub fn collect_usage(options: &ScanOptions) -> UsageReport {
    let started = Instant::now();

    let codex_files = discover_jsonl_files(&options.codex_root);
    let claude_files = discover_jsonl_files(&options.claude_root);

    let (codex, claude) = if options.parallel {
        rayon::join(
            || scan_codex_files(&codex_files, options),
            || scan_claude_files(&claude_files, options),
        )
    } else {
        (
            scan_codex_files(&codex_files, options),
            scan_claude_files(&claude_files, options),
        )
    };

    UsageReport {
        scope: ScopeReport {
            mode: if options.global { "global" } else { "scoped" },
            root: if options.global {
                None
            } else {
                Some(options.root.clone())
            },
        },
        total: codex.totals + claude.totals,
        codex,
        claude,
        duration_ms: started.elapsed().as_millis(),
    }
}

pub fn render_report(report: &UsageReport) -> String {
    let mut out = String::new();
    match &report.scope.root {
        Some(root) => {
            out.push_str(&format!("Scope: {}\n", root.display()));
        }
        None => out.push_str("Scope: global\n"),
    }

    out.push_str(&format!(
        "Scanned: codex {} files, claude {} files in {} ms\n",
        report.codex.files_scanned, report.claude.files_scanned, report.duration_ms
    ));
    out.push_str(&format!(
        "Counted: codex {} sessions, claude {} assistant responses\n\n",
        report.codex.records_counted, report.claude.records_counted
    ));

    out.push_str(&format!(
        "{:<10} {:>14} {:>14} {:>14} {:>14} {:>14}\n",
        "Provider", "Input", "Output", "Thinking", "Cache Read", "Cache Write"
    ));

    push_row(&mut out, "Codex", &report.codex.totals);
    push_row(&mut out, "Claude", &report.claude.totals);
    push_row(&mut out, "Total", &report.total);

    if report.codex.parse_errors > 0 || report.claude.parse_errors > 0 {
        out.push_str(&format!(
            "\nParse warnings: codex {}, claude {}\n",
            report.codex.parse_errors, report.claude.parse_errors
        ));
    }

    out
}

fn push_row(out: &mut String, name: &str, totals: &TokenTotals) {
    out.push_str(&format!(
        "{:<10} {:>14} {:>14} {:>14} {:>14} {:>14}\n",
        name,
        format_u64(totals.input),
        format_u64(totals.output),
        format_u64(totals.thinking),
        format_u64(totals.cache_read),
        format_u64(totals.cache_write),
    ));
}

fn format_u64(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    let len = digits.len();
    for (i, ch) in digits.chars().enumerate() {
        if i != 0 && (len - i) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn discover_jsonl_files(root: &Path) -> Vec<PathBuf> {
    if !root.exists() {
        return Vec::new();
    }

    WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext == "jsonl")
        })
        .map(|entry| entry.into_path())
        .collect()
}

fn scan_codex_files(files: &[PathBuf], options: &ScanOptions) -> ProviderReport {
    if options.parallel {
        files
            .par_iter()
            .map(|path| scan_codex_file(path, options))
            .reduce(ProviderReport::default, |a, b| a + b)
    } else {
        files
            .iter()
            .map(|path| scan_codex_file(path, options))
            .fold(ProviderReport::default(), |acc, item| acc + item)
    }
}

fn scan_claude_files(files: &[PathBuf], options: &ScanOptions) -> ProviderReport {
    if options.parallel {
        files
            .par_iter()
            .map(|path| scan_claude_file(path, options))
            .reduce(ProviderReport::default, |a, b| a + b)
    } else {
        files
            .iter()
            .map(|path| scan_claude_file(path, options))
            .fold(ProviderReport::default(), |acc, item| acc + item)
    }
}

fn scan_codex_file(path: &Path, options: &ScanOptions) -> ProviderReport {
    let mut report = ProviderReport {
        files_scanned: 1,
        ..ProviderReport::default()
    };

    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => {
            report.parse_errors += 1;
            return report;
        }
    };

    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut line = String::new();
    let mut session_cwd: Option<PathBuf> = None;
    let mut max_totals = TokenTotals::default();
    let mut saw_usage = false;

    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                if line.contains("\"type\":\"session_meta\"") {
                    match serde_json::from_str::<CodexSessionMetaLine>(&line) {
                        Ok(parsed) => {
                            if parsed.kind == "session_meta"
                                && let Some(payload) = parsed.payload
                                && let Some(cwd) = payload.cwd
                            {
                                session_cwd = Some(PathBuf::from(cwd));
                            }
                        }
                        Err(_) => report.parse_errors += 1,
                    }
                }

                if line.contains("\"type\":\"event_msg\"")
                    && line.contains("\"type\":\"token_count\"")
                {
                    match serde_json::from_str::<CodexTokenCountLine>(&line) {
                        Ok(parsed) => {
                            if parsed.kind != "event_msg" {
                                continue;
                            }
                            let usage = parsed
                                .payload
                                .and_then(|payload| payload.info)
                                .and_then(|info| info.total_token_usage)
                                .map(TokenTotals::from);

                            if let Some(usage) = usage {
                                saw_usage = true;
                                max_totals.max_assign(&usage);
                            }
                        }
                        Err(_) => report.parse_errors += 1,
                    }
                }
            }
            Err(_) => {
                report.parse_errors += 1;
                break;
            }
        }
    }

    if !saw_usage {
        return report;
    }

    if !options.global {
        let Some(cwd) = session_cwd else {
            return report;
        };

        if !path_in_scope(&cwd, &options.root) {
            return report;
        }
    }

    report.records_counted = 1;
    report.totals = max_totals;
    report
}

fn scan_claude_file(path: &Path, options: &ScanOptions) -> ProviderReport {
    let mut report = ProviderReport {
        files_scanned: 1,
        ..ProviderReport::default()
    };

    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => {
            report.parse_errors += 1;
            return report;
        }
    };

    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut line = String::new();
    let mut by_request: HashMap<String, TokenTotals> = HashMap::new();
    let mut line_no: usize = 0;

    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                line_no += 1;

                if !line.contains("\"type\":\"assistant\"") || !line.contains("\"usage\":") {
                    continue;
                }

                match serde_json::from_str::<ClaudeUsageLine>(&line) {
                    Ok(parsed) => {
                        if parsed.kind.as_deref() != Some("assistant") {
                            continue;
                        }

                        if !options.global {
                            let Some(cwd) = parsed.cwd.as_deref() else {
                                continue;
                            };
                            if !path_in_scope(Path::new(cwd), &options.root) {
                                continue;
                            }
                        }

                        let (message_id, usage) = parsed
                            .message
                            .and_then(|message| message.usage.map(|usage| (message.id, usage)))
                            .unwrap_or((None, ClaudeUsage::default()));

                        let totals = usage.to_totals();
                        if totals.is_zero() {
                            continue;
                        }

                        let key = parsed
                            .request_id
                            .or(message_id)
                            .or(parsed.uuid)
                            .unwrap_or_else(|| format!("{}:{}", path.display(), line_no));

                        by_request
                            .entry(key)
                            .and_modify(|existing| existing.max_assign(&totals))
                            .or_insert(totals);
                    }
                    Err(_) => report.parse_errors += 1,
                }
            }
            Err(_) => {
                report.parse_errors += 1;
                break;
            }
        }
    }

    report.records_counted = by_request.len();
    report.totals = by_request
        .into_values()
        .fold(TokenTotals::default(), |acc, item| acc + item);
    report
}

fn path_in_scope(path: &Path, root: &Path) -> bool {
    if root.as_os_str().is_empty() {
        return true;
    }
    path.starts_with(root)
}

#[derive(Debug, Deserialize)]
struct CodexSessionMetaLine {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    payload: Option<CodexSessionMetaPayload>,
}

#[derive(Debug, Deserialize)]
struct CodexSessionMetaPayload {
    #[serde(default)]
    cwd: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CodexTokenCountLine {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    payload: Option<CodexTokenCountPayload>,
}

#[derive(Debug, Deserialize)]
struct CodexTokenCountPayload {
    #[serde(default)]
    info: Option<CodexTokenCountInfo>,
}

#[derive(Debug, Deserialize)]
struct CodexTokenCountInfo {
    #[serde(default)]
    total_token_usage: Option<CodexTotalTokenUsage>,
}

#[derive(Debug, Deserialize)]
struct CodexTotalTokenUsage {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    cached_input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
    #[serde(default)]
    reasoning_output_tokens: u64,
}

impl From<CodexTotalTokenUsage> for TokenTotals {
    fn from(value: CodexTotalTokenUsage) -> Self {
        Self {
            input: value.input_tokens,
            output: value.output_tokens,
            thinking: value.reasoning_output_tokens,
            cache_read: value.cached_input_tokens,
            cache_write: 0,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ClaudeUsageLine {
    #[serde(rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(rename = "requestId")]
    request_id: Option<String>,
    #[serde(default)]
    uuid: Option<String>,
    #[serde(default)]
    message: Option<ClaudeMessage>,
}

#[derive(Debug, Deserialize)]
struct ClaudeMessage {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    usage: Option<ClaudeUsage>,
}

#[derive(Debug, Default, Deserialize)]
struct ClaudeUsage {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
    #[serde(default)]
    cache_read_input_tokens: u64,
    #[serde(default)]
    cache_creation_input_tokens: u64,
    #[serde(default)]
    reasoning_output_tokens: u64,
    #[serde(default)]
    thinking_tokens: u64,
    #[serde(default)]
    output_tokens_details: Option<OutputTokenDetails>,
    #[serde(default)]
    thinking: Option<ThinkingUsage>,
}

#[derive(Debug, Default, Deserialize)]
struct OutputTokenDetails {
    #[serde(default)]
    reasoning_tokens: u64,
}

#[derive(Debug, Default, Deserialize)]
struct ThinkingUsage {
    #[serde(default)]
    tokens: u64,
    #[serde(default)]
    output_tokens: u64,
}

impl ClaudeUsage {
    fn to_totals(&self) -> TokenTotals {
        let thinking = self
            .reasoning_output_tokens
            .max(self.thinking_tokens)
            .max(
                self.output_tokens_details
                    .as_ref()
                    .map(|details| details.reasoning_tokens)
                    .unwrap_or(0),
            )
            .max(
                self.thinking
                    .as_ref()
                    .map(|thinking| thinking.tokens.max(thinking.output_tokens))
                    .unwrap_or(0),
            );

        TokenTotals {
            input: self.input_tokens,
            output: self.output_tokens,
            thinking,
            cache_read: self.cache_read_input_tokens,
            cache_write: self.cache_creation_input_tokens,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn codex_uses_max_total_usage_per_session() {
        let temp = tempdir().expect("create tempdir");
        let codex_root = temp.path().join("codex");
        fs::create_dir_all(&codex_root).expect("create codex root");

        let session = codex_root.join("session.jsonl");
        fs::write(
            &session,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"cwd\":\"/tmp/proj\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":10,\"cached_input_tokens\":2,\"output_tokens\":3,\"reasoning_output_tokens\":1}}}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":25,\"cached_input_tokens\":5,\"output_tokens\":7,\"reasoning_output_tokens\":2}}}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":25,\"cached_input_tokens\":5,\"output_tokens\":7,\"reasoning_output_tokens\":2}}}}\n"
            ),
        )
        .expect("write session");

        let options = ScanOptions {
            global: true,
            root: PathBuf::from("/tmp/proj"),
            codex_root: codex_root.clone(),
            claude_root: temp.path().join("missing"),
            parallel: false,
        };

        let report = collect_usage(&options);
        assert_eq!(report.codex.records_counted, 1);
        assert_eq!(report.codex.totals.input, 25);
        assert_eq!(report.codex.totals.cache_read, 5);
        assert_eq!(report.codex.totals.output, 7);
        assert_eq!(report.codex.totals.thinking, 2);
    }

    #[test]
    fn claude_deduplicates_repeated_request_ids() {
        let temp = tempdir().expect("create tempdir");
        let claude_root = temp.path().join("claude/projects/test");
        fs::create_dir_all(&claude_root).expect("create claude root");

        let session = claude_root.join("session.jsonl");
        fs::write(
            &session,
            concat!(
                "{\"type\":\"assistant\",\"cwd\":\"/tmp/proj\",\"requestId\":\"req1\",\"message\":{\"id\":\"m1\",\"usage\":{\"input_tokens\":10,\"output_tokens\":2,\"cache_read_input_tokens\":3,\"cache_creation_input_tokens\":4}}}\n",
                "{\"type\":\"assistant\",\"cwd\":\"/tmp/proj\",\"requestId\":\"req1\",\"message\":{\"id\":\"m1\",\"usage\":{\"input_tokens\":10,\"output_tokens\":2,\"cache_read_input_tokens\":3,\"cache_creation_input_tokens\":4}}}\n",
                "{\"type\":\"assistant\",\"cwd\":\"/tmp/proj\",\"requestId\":\"req2\",\"message\":{\"id\":\"m2\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"cache_read_input_tokens\":1,\"cache_creation_input_tokens\":1}}}\n"
            ),
        )
        .expect("write session");

        let options = ScanOptions {
            global: true,
            root: PathBuf::from("/tmp/proj"),
            codex_root: temp.path().join("missing"),
            claude_root: temp.path().join("claude/projects"),
            parallel: false,
        };

        let report = collect_usage(&options);
        assert_eq!(report.claude.records_counted, 2);
        assert_eq!(report.claude.totals.input, 11);
        assert_eq!(report.claude.totals.output, 3);
        assert_eq!(report.claude.totals.cache_read, 4);
        assert_eq!(report.claude.totals.cache_write, 5);
    }

    #[test]
    fn scoped_mode_filters_by_cwd_prefix() {
        let temp = tempdir().expect("create tempdir");

        let codex_root = temp.path().join("codex");
        fs::create_dir_all(&codex_root).expect("create codex root");
        fs::write(
            codex_root.join("session.jsonl"),
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"cwd\":\"/tmp/outside\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":50,\"cached_input_tokens\":10,\"output_tokens\":5,\"reasoning_output_tokens\":1}}}}\n"
            ),
        )
        .expect("write codex");

        let claude_root = temp.path().join("claude/projects/test");
        fs::create_dir_all(&claude_root).expect("create claude root");
        fs::write(
            claude_root.join("session.jsonl"),
            "{\"type\":\"assistant\",\"cwd\":\"/tmp/in-scope\",\"requestId\":\"req1\",\"message\":{\"id\":\"m1\",\"usage\":{\"input_tokens\":7,\"output_tokens\":1}}}\n",
        )
        .expect("write claude");

        let options = ScanOptions {
            global: false,
            root: PathBuf::from("/tmp/in-scope"),
            codex_root,
            claude_root: temp.path().join("claude/projects"),
            parallel: false,
        };

        let report = collect_usage(&options);
        assert_eq!(report.codex.records_counted, 0);
        assert_eq!(report.claude.records_counted, 1);
        assert_eq!(report.total.input, 7);
    }
}
