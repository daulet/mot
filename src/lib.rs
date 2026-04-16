use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use time::format_description::well_known::Rfc3339;
use time::{Date, Duration, OffsetDateTime};
use walkdir::WalkDir;

const UNKNOWN_MODEL: &str = "<unknown>";
const LOCAL_HOST_LABEL: &str = "local";
const REMOTE_SSH_CLEAR_FORWARDINGS: &str = "ClearAllForwardings=yes";
const REMOTE_SSH_CONNECT_TIMEOUT: &str = "ConnectTimeout=5";
const REMOTE_SSH_DISABLE_REMOTE_COMMAND: &str = "RemoteCommand=none";
const REMOTE_MOT_BINARY: &str = "mot";
const SESSION_PROMPT_SNIPPET_CHARS: usize = 96;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenTotals {
    pub input: u64,
    pub output: u64,
    pub thinking: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

impl TokenTotals {
    pub fn total_tokens(&self) -> u64 {
        self.input + self.output + self.thinking + self.cache_read + self.cache_write
    }

    fn max_assign(&mut self, other: &Self) {
        self.input = self.input.max(other.input);
        self.output = self.output.max(other.output);
        self.thinking = self.thinking.max(other.thinking);
        self.cache_read = self.cache_read.max(other.cache_read);
        self.cache_write = self.cache_write.max(other.cache_write);
    }

    fn delta_from_cumulative(self, previous: Self) -> Self {
        let decreased = self.input < previous.input
            || self.output < previous.output
            || self.thinking < previous.thinking
            || self.cache_read < previous.cache_read
            || self.cache_write < previous.cache_write;

        if decreased {
            return self;
        }

        Self {
            input: self.input - previous.input,
            output: self.output - previous.output,
            thinking: self.thinking - previous.thinking,
            cache_read: self.cache_read - previous.cache_read,
            cache_write: self.cache_write - previous.cache_write,
        }
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

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ModelPricing {
    pub input_per_mtok_usd: f64,
    pub output_per_mtok_usd: f64,
    pub cache_read_per_mtok_usd: f64,
    pub cache_write_per_mtok_usd: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelReport {
    pub model: String,
    pub records_counted: usize,
    pub totals: TokenTotals,
    pub estimated_cost_usd: Option<f64>,
    pub pricing: Option<ModelPricing>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DailyReport {
    pub day: String,
    pub records_counted: usize,
    pub totals: TokenTotals,
    pub estimated_cost_usd: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HostReport {
    pub host: String,
    pub records_counted: usize,
    pub totals: TokenTotals,
    pub estimated_cost_usd: f64,
}

#[derive(Debug, Clone, Default)]
struct ModelUsage {
    records_counted: usize,
    totals: TokenTotals,
}

impl ModelUsage {
    fn add_record(&mut self, totals: TokenTotals) {
        self.records_counted += 1;
        self.totals += totals;
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProviderReport {
    pub files_scanned: usize,
    pub records_counted: usize,
    pub parse_errors: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    pub totals: TokenTotals,
    pub estimated_cost_usd: f64,
    pub priced_totals: TokenTotals,
    pub unpriced_totals: TokenTotals,
    pub priced_records_counted: usize,
    pub unpriced_records_counted: usize,
    #[serde(default)]
    pub unpriced_models: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub daily: Vec<DailyReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub by_host: Vec<HostReport>,
    #[serde(default)]
    pub by_model: Vec<ModelReport>,
    #[serde(skip)]
    model_usage: HashMap<String, ModelUsage>,
    #[serde(skip)]
    day_model_usage: HashMap<String, HashMap<String, ModelUsage>>,
    #[serde(skip)]
    host_model_usage: HashMap<String, HashMap<String, ModelUsage>>,
}

impl std::ops::AddAssign for ProviderReport {
    fn add_assign(&mut self, rhs: Self) {
        self.files_scanned += rhs.files_scanned;
        self.records_counted += rhs.records_counted;
        self.parse_errors += rhs.parse_errors;
        self.warnings.extend(rhs.warnings);
        self.totals += rhs.totals;
        self.estimated_cost_usd += rhs.estimated_cost_usd;
        self.priced_totals += rhs.priced_totals;
        self.unpriced_totals += rhs.unpriced_totals;
        self.priced_records_counted += rhs.priced_records_counted;
        self.unpriced_records_counted += rhs.unpriced_records_counted;

        for model in rhs.unpriced_models {
            if !self.unpriced_models.contains(&model) {
                self.unpriced_models.push(model);
            }
        }

        for (model, usage) in rhs.model_usage {
            self.model_usage
                .entry(model)
                .and_modify(|existing| {
                    existing.records_counted += usage.records_counted;
                    existing.totals += usage.totals;
                })
                .or_insert(usage);
        }

        for (day, usage_by_model) in rhs.day_model_usage {
            let day_entry = self.day_model_usage.entry(day).or_default();
            for (model, usage) in usage_by_model {
                day_entry
                    .entry(model)
                    .and_modify(|existing| {
                        existing.records_counted += usage.records_counted;
                        existing.totals += usage.totals;
                    })
                    .or_insert(usage);
            }
        }

        for (host, usage_by_model) in rhs.host_model_usage {
            let host_entry = self.host_model_usage.entry(host).or_default();
            for (model, usage) in usage_by_model {
                host_entry
                    .entry(model)
                    .and_modify(|existing| {
                        existing.records_counted += usage.records_counted;
                        existing.totals += usage.totals;
                    })
                    .or_insert(usage);
            }
        }
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
    pub window: Option<String>,
    pub cutoff_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsageReport {
    pub scope: ScopeReport,
    pub codex: ProviderReport,
    pub claude: ProviderReport,
    pub droid: ProviderReport,
    pub by_host: Vec<HostReport>,
    pub total: TokenTotals,
    pub estimated_cost_usd: f64,
    pub priced_totals: TokenTotals,
    pub unpriced_totals: TokenTotals,
    pub unpriced_models: Vec<String>,
    pub duration_ms: u128,
}

#[derive(Debug, Clone, Deserialize)]
struct RemoteUsageReport {
    #[serde(default)]
    codex: ProviderReport,
    #[serde(default)]
    claude: ProviderReport,
    #[serde(default)]
    droid: ProviderReport,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopBarSnapshot {
    pub scope: ScopeReport,
    pub days: Vec<TopBarDay>,
    pub total: TokenTotals,
    pub total_tokens: u64,
    pub estimated_cost_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopBarDay {
    pub day: String,
    pub total: TokenTotals,
    pub total_tokens: u64,
    pub estimated_cost_usd: f64,
}

#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub global: bool,
    pub root: PathBuf,
    pub codex_root: PathBuf,
    pub claude_root: PathBuf,
    pub droid_root: PathBuf,
    pub parallel: bool,
    pub window: Option<TimeWindow>,
    pub ssh_hosts: Vec<String>,
    pub selected_session: Option<SessionSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimeWindow {
    pub spec: String,
    pub cutoff_unix_ms: i64,
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
            droid_root: home.join(".factory").join("sessions"),
            parallel: true,
            window: None,
            ssh_hosts: Vec::new(),
            selected_session: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SessionProvider {
    Codex,
    Claude,
    Droid,
}

impl SessionProvider {
    pub fn label(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Droid => "droid",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionSummary {
    pub provider: SessionProvider,
    pub id: String,
    pub path: PathBuf,
    pub cwd: Option<PathBuf>,
    pub started_at: Option<String>,
    pub updated_at: Option<String>,
    pub turns: usize,
    pub first_prompt: Option<String>,
}

pub fn parse_time_window(spec: &str) -> Result<TimeWindow, String> {
    parse_time_window_at(spec, now_unix_ms())
}

fn parse_time_window_at(spec: &str, now_unix_ms: i64) -> Result<TimeWindow, String> {
    let compact = normalize_window_spec(spec);
    if compact.is_empty() {
        return Err("time window is empty (examples: 1d, 7d, 1m, 1y)".to_string());
    }

    let split_at = compact
        .find(|ch: char| !ch.is_ascii_digit())
        .ok_or_else(|| "time window must end with a unit (examples: 1d, 7d, 1m, 1y)".to_string())?;

    if split_at == 0 {
        return Err("time window must start with a positive integer".to_string());
    }

    let (value_str, unit) = compact.split_at(split_at);
    let value = value_str
        .parse::<u64>()
        .map_err(|_| format!("invalid time window value: {value_str}"))?;
    if value == 0 {
        return Err("time window value must be greater than zero".to_string());
    }

    let seconds = match unit {
        "s" | "sec" | "secs" | "second" | "seconds" => value,
        "min" | "mins" | "minute" | "minutes" => value
            .checked_mul(60)
            .ok_or_else(|| "time window too large".to_string())?,
        "h" | "hr" | "hrs" | "hour" | "hours" => value
            .checked_mul(60 * 60)
            .ok_or_else(|| "time window too large".to_string())?,
        "d" | "day" | "days" => value
            .checked_mul(24 * 60 * 60)
            .ok_or_else(|| "time window too large".to_string())?,
        "w" | "wk" | "wks" | "week" | "weeks" => value
            .checked_mul(7 * 24 * 60 * 60)
            .ok_or_else(|| "time window too large".to_string())?,
        "m" | "mo" | "mon" | "month" | "months" => value
            .checked_mul(30 * 24 * 60 * 60)
            .ok_or_else(|| "time window too large".to_string())?,
        "y" | "yr" | "yrs" | "year" | "years" => value
            .checked_mul(365 * 24 * 60 * 60)
            .ok_or_else(|| "time window too large".to_string())?,
        _ => {
            return Err(format!(
                "unsupported time window unit: {unit} (use s|min|h|d|w|m|y)"
            ));
        }
    };

    let duration_ms = seconds
        .checked_mul(1_000)
        .and_then(|ms| i64::try_from(ms).ok())
        .ok_or_else(|| "time window too large".to_string())?;
    let cutoff_unix_ms = now_unix_ms.saturating_sub(duration_ms);

    Ok(TimeWindow {
        spec: compact,
        cutoff_unix_ms,
    })
}

fn normalize_window_spec(spec: &str) -> String {
    let mut out = String::with_capacity(spec.len());
    for ch in spec.chars() {
        if !ch.is_whitespace() {
            out.push(ch.to_ascii_lowercase());
        }
    }
    out
}

fn now_unix_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
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

    let (mut codex, mut claude, mut droid) = if options.parallel {
        let ((codex, claude), droid) = rayon::join(
            || rayon::join(|| scan_all_codex(options), || scan_all_claude(options)),
            || scan_all_droid(options),
        );
        (codex, claude, droid)
    } else {
        (
            scan_all_codex(options),
            scan_all_claude(options),
            scan_all_droid(options),
        )
    };

    finalize_provider_pricing(&mut codex);
    finalize_provider_pricing(&mut claude);
    finalize_provider_pricing_with(&mut droid, lookup_droid_pricing);

    let mut report = UsageReport {
        scope: ScopeReport {
            mode: if options.global { "global" } else { "scoped" },
            root: if options.global {
                None
            } else {
                Some(options.root.clone())
            },
            window: options.window.as_ref().map(|window| window.spec.clone()),
            cutoff_unix_ms: options.window.as_ref().map(|window| window.cutoff_unix_ms),
            session: options.selected_session.clone(),
        },
        total: TokenTotals::default(),
        codex,
        claude,
        droid,
        by_host: Vec::new(),
        estimated_cost_usd: 0.0,
        priced_totals: TokenTotals::default(),
        unpriced_totals: TokenTotals::default(),
        unpriced_models: Vec::new(),
        duration_ms: 0,
    };
    refresh_usage_report_rollups(&mut report);
    merge_remote_host_reports(&mut report, options);
    refresh_usage_report_rollups(&mut report);
    report.duration_ms = started.elapsed().as_millis();
    report
}

pub fn build_topbar_snapshot(report: &UsageReport, days: usize) -> TopBarSnapshot {
    let day_keys = recent_day_keys(days);
    build_topbar_snapshot_for_day_keys(report, &day_keys)
}

pub fn render_report(report: &UsageReport) -> String {
    let mut out = String::new();
    match &report.scope.root {
        Some(root) => {
            out.push_str(&format!("Scope: {}\n", root.display()));
        }
        None => out.push_str("Scope: global\n"),
    }
    if let Some(window) = &report.scope.window {
        out.push_str(&format!("Window: last {window}\n"));
    }
    if let Some(session) = &report.scope.session {
        out.push_str(&format!(
            "Session: {} {} ({} turns",
            session.provider.label(),
            session.id,
            session.turns
        ));
        if let Some(started_at) = &session.started_at {
            out.push_str(&format!(", started {started_at}"));
        }
        out.push_str(")\n");
        if let Some(first_prompt) = &session.first_prompt {
            out.push_str(&format!("First prompt: {first_prompt}\n"));
        }
    }

    out.push_str(&format!(
        "Scanned: codex {} files, claude {} files, droid {} files in {} ms\n",
        report.codex.files_scanned,
        report.claude.files_scanned,
        report.droid.files_scanned,
        report.duration_ms
    ));
    out.push_str(&format!(
        "Counted: codex {} sessions, claude {} assistant responses, droid {} sessions\n\n",
        report.codex.records_counted, report.claude.records_counted, report.droid.records_counted
    ));

    out.push_str(&format!(
        "{:<10} {:>14} {:>14} {:>14} {:>14} {:>14} {:>14}\n",
        "Provider", "Input", "Output", "Thinking", "Cache Read", "Cache Write", "Est. Cost"
    ));

    push_row(
        &mut out,
        "Codex",
        &report.codex.totals,
        report.codex.estimated_cost_usd,
    );
    push_row(
        &mut out,
        "Claude",
        &report.claude.totals,
        report.claude.estimated_cost_usd,
    );
    push_row(
        &mut out,
        "Droid",
        &report.droid.totals,
        report.droid.estimated_cost_usd,
    );
    push_row(&mut out, "Total", &report.total, report.estimated_cost_usd);

    if !report.by_host.is_empty() {
        out.push_str("\nBy host:\n");
        out.push_str(&format!(
            "{:<18} {:>14} {:>14} {:>14} {:>14} {:>14} {:>14}\n",
            "Host", "Input", "Output", "Thinking", "Cache Read", "Cache Write", "Est. Cost"
        ));

        for host in &report.by_host {
            push_host_row(&mut out, host);
        }
    }

    if !report.codex.by_model.is_empty()
        || !report.claude.by_model.is_empty()
        || !report.droid.by_model.is_empty()
    {
        out.push_str("\nBy model:\n");
        out.push_str(&format!(
            "{:<10} {:<28} {:>14} {:>14} {:>14} {:>14} {:>14} {:>14}\n",
            "Provider",
            "Model",
            "Input",
            "Output",
            "Thinking",
            "Cache Read",
            "Cache Write",
            "Est. Cost"
        ));

        for model in &report.codex.by_model {
            push_model_row(&mut out, "Codex", model);
        }
        for model in &report.claude.by_model {
            push_model_row(&mut out, "Claude", model);
        }
        for model in &report.droid.by_model {
            push_model_row(&mut out, "Droid", model);
        }
    }

    out.push_str(
        "\nPricing: Codex/Claude standard API (non-priority); Droid Factory Standard Tokens\n",
    );
    if !report.unpriced_totals.is_zero() {
        out.push_str(&format!(
            "Unpriced tokens: input {}, output {}, thinking {}, cache read {}, cache write {}\n",
            format_u64(report.unpriced_totals.input),
            format_u64(report.unpriced_totals.output),
            format_u64(report.unpriced_totals.thinking),
            format_u64(report.unpriced_totals.cache_read),
            format_u64(report.unpriced_totals.cache_write),
        ));
        if !report.unpriced_models.is_empty() {
            out.push_str(&format!(
                "Unpriced models: {}\n",
                report.unpriced_models.join(", ")
            ));
        }
    }

    if report.codex.parse_errors > 0
        || report.claude.parse_errors > 0
        || report.droid.parse_errors > 0
    {
        out.push_str(&format!(
            "\nParse warnings: codex {}, claude {}, droid {}\n",
            report.codex.parse_errors, report.claude.parse_errors, report.droid.parse_errors
        ));
    }

    if !report.codex.warnings.is_empty()
        || !report.claude.warnings.is_empty()
        || !report.droid.warnings.is_empty()
    {
        out.push_str("\nWarnings:\n");
        for warning in &report.codex.warnings {
            out.push_str(&format!("Codex: {warning}\n"));
        }
        for warning in &report.claude.warnings {
            out.push_str(&format!("Claude: {warning}\n"));
        }
        for warning in &report.droid.warnings {
            out.push_str(&format!("Droid: {warning}\n"));
        }
    }

    out
}

fn push_row(out: &mut String, name: &str, totals: &TokenTotals, estimated_cost_usd: f64) {
    out.push_str(&format!(
        "{:<10} {:>14} {:>14} {:>14} {:>14} {:>14} {:>14}\n",
        name,
        format_u64(totals.input),
        format_u64(totals.output),
        format_u64(totals.thinking),
        format_u64(totals.cache_read),
        format_u64(totals.cache_write),
        format_usd(estimated_cost_usd),
    ));
}

fn push_host_row(out: &mut String, host: &HostReport) {
    out.push_str(&format!(
        "{:<18} {:>14} {:>14} {:>14} {:>14} {:>14} {:>14}\n",
        truncate_model_name(&host.host, 18),
        format_u64(host.totals.input),
        format_u64(host.totals.output),
        format_u64(host.totals.thinking),
        format_u64(host.totals.cache_read),
        format_u64(host.totals.cache_write),
        format_usd(host.estimated_cost_usd),
    ));
}

fn push_model_row(out: &mut String, provider: &str, model: &ModelReport) {
    out.push_str(&format!(
        "{:<10} {:<28} {:>14} {:>14} {:>14} {:>14} {:>14} {:>14}\n",
        provider,
        truncate_model_name(&model.model, 28),
        format_u64(model.totals.input),
        format_u64(model.totals.output),
        format_u64(model.totals.thinking),
        format_u64(model.totals.cache_read),
        format_u64(model.totals.cache_write),
        format_optional_usd(model.estimated_cost_usd),
    ));
}

fn truncate_model_name(model: &str, max_len: usize) -> String {
    if model.chars().count() <= max_len {
        return model.to_string();
    }

    let keep = max_len.saturating_sub(3);
    let mut out = String::with_capacity(max_len);
    out.extend(model.chars().take(keep));
    out.push_str("...");
    out
}

fn format_optional_usd(value: Option<f64>) -> String {
    value.map(format_usd).unwrap_or_else(|| "n/a".to_string())
}

fn format_u64(value: u64) -> String {
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

fn format_usd(value: f64) -> String {
    if value >= 1.0 {
        format!("${value:.2}")
    } else if value >= 0.01 {
        format!("${value:.4}")
    } else {
        format!("${value:.6}")
    }
}

fn finalize_provider_pricing(report: &mut ProviderReport) {
    finalize_provider_pricing_with(report, lookup_model_pricing);
}

fn finalize_provider_pricing_with(
    report: &mut ProviderReport,
    lookup: fn(&str) -> Option<ModelPricing>,
) {
    report.estimated_cost_usd = 0.0;
    report.priced_totals = TokenTotals::default();
    report.unpriced_totals = TokenTotals::default();
    report.priced_records_counted = 0;
    report.unpriced_records_counted = 0;
    report.unpriced_models.clear();
    report.daily.clear();
    report.by_host.clear();
    report.by_model.clear();

    let mut by_model = Vec::with_capacity(report.model_usage.len());

    for (model, usage) in &report.model_usage {
        let pricing = lookup(model);
        let estimated_cost_usd = pricing.map(|rates| estimate_cost_usd(usage.totals, rates));

        match estimated_cost_usd {
            Some(cost) => {
                report.estimated_cost_usd += cost;
                report.priced_totals += usage.totals;
                report.priced_records_counted += usage.records_counted;
            }
            None => {
                report.unpriced_totals += usage.totals;
                report.unpriced_records_counted += usage.records_counted;
                if !report.unpriced_models.contains(model) {
                    report.unpriced_models.push(model.clone());
                }
            }
        }

        by_model.push(ModelReport {
            model: model.clone(),
            records_counted: usage.records_counted,
            totals: usage.totals,
            estimated_cost_usd,
            pricing,
        });
    }

    by_model.sort_by(model_report_sort);
    report.unpriced_models.sort();
    report.by_model = by_model;
    report.by_host = build_host_reports(&report.host_model_usage, lookup);
    report.daily = build_daily_reports(&report.day_model_usage, lookup);
}

fn model_report_sort(a: &ModelReport, b: &ModelReport) -> Ordering {
    match (a.estimated_cost_usd, b.estimated_cost_usd) {
        (Some(left), Some(right)) => right
            .partial_cmp(&left)
            .unwrap_or(Ordering::Equal)
            .then_with(|| b.totals.input.cmp(&a.totals.input))
            .then_with(|| a.model.cmp(&b.model)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => b
            .totals
            .input
            .cmp(&a.totals.input)
            .then_with(|| a.model.cmp(&b.model)),
    }
}

fn host_report_sort(a: &HostReport, b: &HostReport) -> Ordering {
    b.estimated_cost_usd
        .partial_cmp(&a.estimated_cost_usd)
        .unwrap_or(Ordering::Equal)
        .then_with(|| b.totals.input.cmp(&a.totals.input))
        .then_with(|| a.host.cmp(&b.host))
}

fn estimate_cost_usd(totals: TokenTotals, rates: ModelPricing) -> f64 {
    usd_from_tokens(totals.input, rates.input_per_mtok_usd)
        + usd_from_tokens(totals.output, rates.output_per_mtok_usd)
        + usd_from_tokens(totals.thinking, rates.output_per_mtok_usd)
        + usd_from_tokens(totals.cache_read, rates.cache_read_per_mtok_usd)
        + usd_from_tokens(totals.cache_write, rates.cache_write_per_mtok_usd)
}

fn build_daily_reports(
    day_model_usage: &HashMap<String, HashMap<String, ModelUsage>>,
    lookup: fn(&str) -> Option<ModelPricing>,
) -> Vec<DailyReport> {
    let mut daily = Vec::with_capacity(day_model_usage.len());

    for (day, usage_by_model) in day_model_usage {
        let mut records_counted = 0usize;
        let mut totals = TokenTotals::default();
        let mut estimated_cost_usd = 0.0;

        for (model, usage) in usage_by_model {
            records_counted += usage.records_counted;
            totals += usage.totals;
            if let Some(pricing) = lookup(model) {
                estimated_cost_usd += estimate_cost_usd(usage.totals, pricing);
            }
        }

        daily.push(DailyReport {
            day: day.clone(),
            records_counted,
            totals,
            estimated_cost_usd,
        });
    }

    daily.sort_by(|a, b| a.day.cmp(&b.day));
    daily
}

fn build_host_reports(
    host_model_usage: &HashMap<String, HashMap<String, ModelUsage>>,
    lookup: fn(&str) -> Option<ModelPricing>,
) -> Vec<HostReport> {
    let mut by_host = Vec::with_capacity(host_model_usage.len());

    for (host, usage_by_model) in host_model_usage {
        let mut records_counted = 0usize;
        let mut totals = TokenTotals::default();
        let mut estimated_cost_usd = 0.0;

        for (model, usage) in usage_by_model {
            records_counted += usage.records_counted;
            totals += usage.totals;
            if let Some(pricing) = lookup(model) {
                estimated_cost_usd += estimate_cost_usd(usage.totals, pricing);
            }
        }

        by_host.push(HostReport {
            host: host.clone(),
            records_counted,
            totals,
            estimated_cost_usd,
        });
    }

    by_host.sort_by(host_report_sort);
    by_host
}

fn combine_host_reports<'a>(
    providers: impl IntoIterator<Item = &'a ProviderReport>,
) -> Vec<HostReport> {
    let mut combined: HashMap<String, HostReport> = HashMap::new();

    for provider in providers {
        for host in &provider.by_host {
            combined
                .entry(host.host.clone())
                .and_modify(|existing| {
                    existing.records_counted += host.records_counted;
                    existing.totals += host.totals;
                    existing.estimated_cost_usd += host.estimated_cost_usd;
                })
                .or_insert_with(|| host.clone());
        }
    }

    let mut hosts: Vec<HostReport> = combined.into_values().collect();
    hosts.sort_by(host_report_sort);
    hosts
}

fn refresh_usage_report_rollups(report: &mut UsageReport) {
    report.total = report.codex.totals + report.claude.totals + report.droid.totals;
    report.priced_totals =
        report.codex.priced_totals + report.claude.priced_totals + report.droid.priced_totals;
    report.unpriced_totals =
        report.codex.unpriced_totals + report.claude.unpriced_totals + report.droid.unpriced_totals;
    report.estimated_cost_usd = report.codex.estimated_cost_usd
        + report.claude.estimated_cost_usd
        + report.droid.estimated_cost_usd;

    let mut unpriced_models = report.codex.unpriced_models.clone();
    for model in report
        .claude
        .unpriced_models
        .iter()
        .chain(&report.droid.unpriced_models)
    {
        if !unpriced_models.contains(model) {
            unpriced_models.push(model.clone());
        }
    }
    unpriced_models.sort();
    report.unpriced_models = unpriced_models;
    report.by_host = combine_host_reports([&report.codex, &report.claude, &report.droid]);
}

fn merge_remote_host_reports(report: &mut UsageReport, options: &ScanOptions) {
    if options.selected_session.is_some() {
        return;
    }

    let remote_reports = if options.parallel {
        options
            .ssh_hosts
            .par_iter()
            .map(|host| (host.clone(), fetch_remote_usage_report(host, options)))
            .collect::<Vec<_>>()
    } else {
        options
            .ssh_hosts
            .iter()
            .map(|host| (host.clone(), fetch_remote_usage_report(host, options)))
            .collect::<Vec<_>>()
    };

    for (host, result) in remote_reports {
        match result {
            Ok(mut remote) => {
                collapse_provider_to_host(&mut remote.codex, &host);
                collapse_provider_to_host(&mut remote.claude, &host);
                collapse_provider_to_host(&mut remote.droid, &host);
                merge_provider_report(&mut report.codex, remote.codex);
                merge_provider_report(&mut report.claude, remote.claude);
                merge_provider_report(&mut report.droid, remote.droid);
            }
            Err(warning) => {
                report.codex.warnings.push(warning);
            }
        }
    }
}

fn merge_provider_report(dst: &mut ProviderReport, mut src: ProviderReport) {
    let src_by_model = std::mem::take(&mut src.by_model);
    let src_daily = std::mem::take(&mut src.daily);
    let src_by_host = std::mem::take(&mut src.by_host);

    *dst += src;
    merge_model_reports(&mut dst.by_model, src_by_model);
    merge_daily_reports(&mut dst.daily, src_daily);
    merge_host_reports(&mut dst.by_host, src_by_host);
    dst.unpriced_models.sort();
}

fn merge_model_reports(dst: &mut Vec<ModelReport>, src: Vec<ModelReport>) {
    let mut index_by_model: HashMap<String, usize> = dst
        .iter()
        .enumerate()
        .map(|(idx, model)| (model.model.clone(), idx))
        .collect();

    for entry in src {
        if let Some(&idx) = index_by_model.get(&entry.model) {
            let existing = &mut dst[idx];
            existing.records_counted += entry.records_counted;
            existing.totals += entry.totals;
            existing.estimated_cost_usd =
                match (existing.estimated_cost_usd, entry.estimated_cost_usd) {
                    (Some(left), Some(right)) => Some(left + right),
                    (Some(left), None) => Some(left),
                    (None, Some(right)) => Some(right),
                    (None, None) => None,
                };
            if existing.pricing.is_none() {
                existing.pricing = entry.pricing;
            }
        } else {
            let next_index = dst.len();
            index_by_model.insert(entry.model.clone(), next_index);
            dst.push(entry);
        }
    }

    dst.sort_by(model_report_sort);
}

fn merge_daily_reports(dst: &mut Vec<DailyReport>, src: Vec<DailyReport>) {
    let mut index_by_day: HashMap<String, usize> = dst
        .iter()
        .enumerate()
        .map(|(idx, day)| (day.day.clone(), idx))
        .collect();

    for entry in src {
        if let Some(&idx) = index_by_day.get(&entry.day) {
            let existing = &mut dst[idx];
            existing.records_counted += entry.records_counted;
            existing.totals += entry.totals;
            existing.estimated_cost_usd += entry.estimated_cost_usd;
        } else {
            let next_index = dst.len();
            index_by_day.insert(entry.day.clone(), next_index);
            dst.push(entry);
        }
    }

    dst.sort_by(|a, b| a.day.cmp(&b.day));
}

fn merge_host_reports(dst: &mut Vec<HostReport>, src: Vec<HostReport>) {
    let mut index_by_host: HashMap<String, usize> = dst
        .iter()
        .enumerate()
        .map(|(idx, host)| (host.host.clone(), idx))
        .collect();

    for entry in src {
        if let Some(&idx) = index_by_host.get(&entry.host) {
            let existing = &mut dst[idx];
            existing.records_counted += entry.records_counted;
            existing.totals += entry.totals;
            existing.estimated_cost_usd += entry.estimated_cost_usd;
        } else {
            let next_index = dst.len();
            index_by_host.insert(entry.host.clone(), next_index);
            dst.push(entry);
        }
    }

    dst.sort_by(host_report_sort);
}

fn collapse_provider_to_host(report: &mut ProviderReport, host: &str) {
    let mut collapsed = HostReport {
        host: host.to_string(),
        ..HostReport::default()
    };

    if report.by_host.is_empty() {
        if report.records_counted > 0 || !report.totals.is_zero() {
            collapsed.records_counted = report.records_counted;
            collapsed.totals = report.totals;
            collapsed.estimated_cost_usd = report.estimated_cost_usd;
            report.by_host = vec![collapsed];
        }
        return;
    }

    for existing in &report.by_host {
        collapsed.records_counted += existing.records_counted;
        collapsed.totals += existing.totals;
        collapsed.estimated_cost_usd += existing.estimated_cost_usd;
    }
    report.by_host = vec![collapsed];
}

fn usd_from_tokens(tokens: u64, per_mtok_usd: f64) -> f64 {
    (tokens as f64) * per_mtok_usd / 1_000_000.0
}

#[derive(Debug, Clone, Copy)]
struct PricingRule {
    patterns: &'static [&'static str],
    pricing: ModelPricing,
}

const fn pricing(
    input_per_mtok_usd: f64,
    output_per_mtok_usd: f64,
    cache_read_per_mtok_usd: f64,
    cache_write_per_mtok_usd: f64,
) -> ModelPricing {
    ModelPricing {
        input_per_mtok_usd,
        output_per_mtok_usd,
        cache_read_per_mtok_usd,
        cache_write_per_mtok_usd,
    }
}

// API standard-rate pricing snapshots used for estimation.
// Source date: 2026-03-30.
// OpenAI: https://developers.openai.com/api/docs/pricing and
// model pages under https://developers.openai.com/api/docs/models/*
// Anthropic: https://docs.anthropic.com/en/docs/about-claude/pricing
//
// Anthropic cache write estimates use the default 5-minute cache write rate.
const OPENAI_PRICING_RULES: &[PricingRule] = &[
    PricingRule {
        patterns: &["gpt-5.4-pro"],
        pricing: pricing(30.0, 180.0, 0.0, 30.0),
    },
    PricingRule {
        patterns: &["gpt-5.4-mini"],
        pricing: pricing(0.75, 4.5, 0.075, 0.75),
    },
    PricingRule {
        patterns: &["gpt-5.4-nano"],
        pricing: pricing(0.20, 1.25, 0.02, 0.20),
    },
    PricingRule {
        patterns: &["gpt-5.4"],
        pricing: pricing(2.5, 15.0, 0.25, 2.5),
    },
    PricingRule {
        patterns: &["gpt-5.3-codex"],
        pricing: pricing(1.75, 14.0, 0.175, 1.75),
    },
    PricingRule {
        patterns: &["gpt-5.3-chat-latest"],
        pricing: pricing(1.75, 14.0, 0.175, 1.75),
    },
    PricingRule {
        patterns: &["gpt-5.2-pro"],
        pricing: pricing(21.0, 168.0, 0.0, 21.0),
    },
    PricingRule {
        patterns: &["gpt-5.2-codex", "gpt-5.2"],
        pricing: pricing(1.75, 14.0, 0.175, 1.75),
    },
    PricingRule {
        patterns: &["gpt-5.1-codex-mini"],
        pricing: pricing(0.25, 2.0, 0.025, 0.25),
    },
    PricingRule {
        patterns: &["gpt-5.1-codex", "gpt-5.1"],
        pricing: pricing(1.25, 10.0, 0.125, 1.25),
    },
    PricingRule {
        patterns: &["gpt-5-pro"],
        pricing: pricing(15.0, 120.0, 0.0, 15.0),
    },
    PricingRule {
        patterns: &["gpt-5-codex", "gpt-5"],
        pricing: pricing(1.25, 10.0, 0.125, 1.25),
    },
    PricingRule {
        patterns: &["gpt-5-mini"],
        pricing: pricing(0.25, 2.0, 0.025, 0.25),
    },
    PricingRule {
        patterns: &["gpt-5-nano"],
        pricing: pricing(0.05, 0.4, 0.005, 0.05),
    },
    PricingRule {
        patterns: &["codex-mini-latest"],
        pricing: pricing(1.5, 6.0, 0.375, 1.5),
    },
];

const ANTHROPIC_PRICING_RULES: &[PricingRule] = &[
    PricingRule {
        patterns: &["claude-opus-4-6"],
        pricing: pricing(5.0, 25.0, 0.5, 6.25),
    },
    PricingRule {
        patterns: &["claude-opus-4-5"],
        pricing: pricing(5.0, 25.0, 0.5, 6.25),
    },
    PricingRule {
        patterns: &["claude-opus-4-1"],
        pricing: pricing(15.0, 75.0, 1.5, 18.75),
    },
    PricingRule {
        patterns: &["claude-opus-4"],
        pricing: pricing(15.0, 75.0, 1.5, 18.75),
    },
    PricingRule {
        patterns: &["claude-sonnet-4-6"],
        pricing: pricing(3.0, 15.0, 0.3, 3.75),
    },
    PricingRule {
        patterns: &["claude-sonnet-4-5"],
        pricing: pricing(3.0, 15.0, 0.3, 3.75),
    },
    PricingRule {
        patterns: &["claude-sonnet-4"],
        pricing: pricing(3.0, 15.0, 0.3, 3.75),
    },
    PricingRule {
        patterns: &["claude-3-7-sonnet", "claude-sonnet-3-7"],
        pricing: pricing(3.0, 15.0, 0.3, 3.75),
    },
    PricingRule {
        patterns: &["claude-3-5-sonnet", "claude-sonnet-3-5"],
        pricing: pricing(3.0, 15.0, 0.3, 3.75),
    },
    PricingRule {
        patterns: &["claude-haiku-4-5", "claude-4-5-haiku"],
        pricing: pricing(1.0, 5.0, 0.1, 1.25),
    },
    PricingRule {
        patterns: &["claude-3-5-haiku", "claude-haiku-3-5"],
        pricing: pricing(0.8, 4.0, 0.08, 1.0),
    },
    PricingRule {
        patterns: &["claude-opus-3", "claude-3-opus"],
        pricing: pricing(15.0, 75.0, 1.5, 18.75),
    },
    PricingRule {
        patterns: &["claude-haiku-3", "claude-3-haiku"],
        pricing: pricing(0.25, 1.25, 0.03, 0.30),
    },
];

// Factory Droid pricing based on Standard Tokens with per-model multipliers.
// Source: https://docs.factory.ai/pricing (2026-03-30)
//
// All token types cost multiplier × $10/M tokens, except cached reads
// which cost multiplier × $1/M tokens (1/10th discount).
const fn droid_pricing(multiplier: f64) -> ModelPricing {
    pricing(
        multiplier * 10.0,
        multiplier * 10.0,
        multiplier * 1.0,
        multiplier * 10.0,
    )
}

const DROID_PRICING_RULES: &[PricingRule] = &[
    PricingRule {
        patterns: &["minimax-m2.5"],
        pricing: droid_pricing(0.12),
    },
    PricingRule {
        patterns: &["gemini-3-flash-preview"],
        pricing: droid_pricing(0.2),
    },
    PricingRule {
        patterns: &["glm-4.7"],
        pricing: droid_pricing(0.25),
    },
    PricingRule {
        patterns: &["kimi-k2.5"],
        pricing: droid_pricing(0.25),
    },
    PricingRule {
        patterns: &["claude-haiku-4-5", "claude-4-5-haiku"],
        pricing: droid_pricing(0.4),
    },
    PricingRule {
        patterns: &["glm-5"],
        pricing: droid_pricing(0.4),
    },
    PricingRule {
        patterns: &["gpt-5.2-codex", "gpt-5.2"],
        pricing: droid_pricing(0.7),
    },
    PricingRule {
        patterns: &["gpt-5.3-codex"],
        pricing: droid_pricing(0.7),
    },
    PricingRule {
        patterns: &["gemini-3.1-pro-preview"],
        pricing: droid_pricing(0.8),
    },
    PricingRule {
        patterns: &["gpt-5.4"],
        pricing: droid_pricing(1.0),
    },
    PricingRule {
        patterns: &["claude-sonnet-4-6"],
        pricing: droid_pricing(1.2),
    },
    PricingRule {
        patterns: &["claude-sonnet-4-5"],
        pricing: droid_pricing(1.2),
    },
    PricingRule {
        patterns: &["claude-opus-4-6-fast"],
        pricing: droid_pricing(12.0),
    },
    PricingRule {
        patterns: &["claude-opus-4-6"],
        pricing: droid_pricing(2.0),
    },
    PricingRule {
        patterns: &["claude-opus-4-5"],
        pricing: droid_pricing(2.0),
    },
];

fn lookup_droid_pricing(model: &str) -> Option<ModelPricing> {
    let normalized = model.trim().to_ascii_lowercase();
    if normalized.is_empty() || normalized == UNKNOWN_MODEL {
        return None;
    }

    for rule in DROID_PRICING_RULES {
        if rule
            .patterns
            .iter()
            .any(|pattern| model_rule_matches(&normalized, pattern))
        {
            return Some(rule.pricing);
        }
    }

    None
}

fn lookup_model_pricing(model: &str) -> Option<ModelPricing> {
    let normalized = model.trim().to_ascii_lowercase();
    if normalized.is_empty() || normalized == UNKNOWN_MODEL {
        return None;
    }

    for rule in OPENAI_PRICING_RULES {
        if rule
            .patterns
            .iter()
            .any(|pattern| model_rule_matches(&normalized, pattern))
        {
            return Some(rule.pricing);
        }
    }

    for rule in ANTHROPIC_PRICING_RULES {
        if rule
            .patterns
            .iter()
            .any(|pattern| model_rule_matches(&normalized, pattern))
        {
            return Some(rule.pricing);
        }
    }

    None
}

fn model_rule_matches(model: &str, pattern: &str) -> bool {
    if model == pattern {
        return true;
    }

    model
        .strip_prefix(pattern)
        .is_some_and(|suffix| suffix.starts_with('-') || suffix.starts_with('@'))
}

fn timestamp_in_window(timestamp: Option<&str>, window: Option<&TimeWindow>) -> bool {
    let Some(window) = window else {
        return true;
    };
    let Some(timestamp) = timestamp else {
        return false;
    };
    parse_rfc3339_unix_ms(timestamp)
        .map(|unix_ms| unix_ms >= window.cutoff_unix_ms)
        .unwrap_or(false)
}

fn parse_rfc3339_unix_ms(timestamp: &str) -> Option<i64> {
    let parsed = OffsetDateTime::parse(timestamp, &Rfc3339).ok()?;
    let unix_ms = parsed.unix_timestamp_nanos() / 1_000_000;
    i64::try_from(unix_ms).ok()
}

fn timestamp_day_key(timestamp: Option<&str>) -> Option<String> {
    let timestamp = timestamp?;
    let parsed = OffsetDateTime::parse(timestamp, &Rfc3339).ok()?;
    Some(day_key_from_date(parsed.date()))
}

fn day_key_from_date(date: Date) -> String {
    format!(
        "{:04}-{:02}-{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    )
}

fn recent_day_keys(days: usize) -> Vec<String> {
    recent_day_keys_at(days, now_unix_ms())
}

fn recent_day_keys_at(days: usize, now_unix_ms: i64) -> Vec<String> {
    let Ok(now) = OffsetDateTime::from_unix_timestamp_nanos(i128::from(now_unix_ms) * 1_000_000)
    else {
        return Vec::new();
    };
    let today = now.date();
    let mut day_keys = Vec::with_capacity(days);

    for offset in (0..days).rev() {
        day_keys.push(day_key_from_date(today - Duration::days(offset as i64)));
    }

    day_keys
}

fn build_topbar_snapshot_for_day_keys(report: &UsageReport, day_keys: &[String]) -> TopBarSnapshot {
    let mut days_by_key: HashMap<String, TopBarDay> = day_keys
        .iter()
        .cloned()
        .map(|day| {
            (
                day.clone(),
                TopBarDay {
                    day,
                    total: TokenTotals::default(),
                    total_tokens: 0,
                    estimated_cost_usd: 0.0,
                },
            )
        })
        .collect();

    for provider_day in report
        .codex
        .daily
        .iter()
        .chain(&report.claude.daily)
        .chain(&report.droid.daily)
    {
        let Some(day) = days_by_key.get_mut(&provider_day.day) else {
            continue;
        };
        day.total += provider_day.totals;
        day.total_tokens = day.total.total_tokens();
        day.estimated_cost_usd += provider_day.estimated_cost_usd;
    }

    let days: Vec<TopBarDay> = day_keys
        .iter()
        .filter_map(|day| days_by_key.remove(day))
        .collect();

    let mut total = TokenTotals::default();
    let mut estimated_cost_usd = 0.0;
    for day in &days {
        total += day.total;
        estimated_cost_usd += day.estimated_cost_usd;
    }

    TopBarSnapshot {
        scope: report.scope.clone(),
        total_tokens: total.total_tokens(),
        total,
        estimated_cost_usd,
        days,
    }
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

pub fn list_session_summaries(options: &ScanOptions) -> Vec<SessionSummary> {
    let mut sessions = Vec::new();

    for path in discover_jsonl_files(&options.codex_root) {
        if let Some(summary) = summarize_codex_session_file(&path) {
            sessions.push(summary);
        }
    }

    for path in discover_jsonl_files(&options.claude_root) {
        if let Some(summary) = summarize_claude_session_file(&path) {
            sessions.push(summary);
        }
    }

    for path in discover_settings_files(&options.droid_root) {
        if let Some(summary) = summarize_droid_session_file(&path) {
            sessions.push(summary);
        }
    }

    sessions.retain(|summary| session_summary_in_scope(summary, options));
    sessions.sort_by(session_summary_sort);
    sessions
}

pub fn resolve_session_selection(
    options: &ScanOptions,
    query: &str,
) -> Result<SessionSummary, String> {
    let query = query.trim();
    if query.is_empty() {
        return Err("session filter is empty".to_string());
    }

    if let Some(summary) = summarize_explicit_session_path(options, query) {
        return Ok(summary);
    }

    let sessions = list_session_summaries(options);
    let mut matches = sessions
        .into_iter()
        .filter(|summary| session_summary_matches(summary, query))
        .collect::<Vec<_>>();

    match matches.len() {
        0 => Err(format!(
            "no local session matched '{query}' in the current scope"
        )),
        1 => Ok(matches.remove(0)),
        _ => {
            matches.sort_by(session_summary_sort);
            let choices = matches
                .iter()
                .take(5)
                .map(|summary| {
                    format!(
                        "{} {} {}",
                        summary.provider.label(),
                        summary.id,
                        summary.path.display()
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            Err(format!(
                "session filter '{query}' matched {} sessions; pass a full id/path or use --select-session ({choices})",
                matches.len()
            ))
        }
    }
}

fn summarize_explicit_session_path(options: &ScanOptions, query: &str) -> Option<SessionSummary> {
    let path = PathBuf::from(query);
    let absolute_path = if path.is_absolute() {
        path
    } else {
        env::current_dir().ok()?.join(path)
    };

    if !absolute_path.exists() {
        return None;
    }

    if absolute_path
        .to_str()
        .is_some_and(|path| path.ends_with(".settings.json"))
    {
        return summarize_droid_session_file(&absolute_path);
    }

    if absolute_path.starts_with(&options.codex_root) {
        return summarize_codex_session_file(&absolute_path);
    }
    if absolute_path.starts_with(&options.claude_root) {
        return summarize_claude_session_file(&absolute_path);
    }
    if absolute_path.starts_with(&options.droid_root) {
        let settings_path = if absolute_path
            .to_str()
            .is_some_and(|path| path.ends_with(".jsonl"))
        {
            jsonl_path_to_settings(&absolute_path)
        } else {
            absolute_path.clone()
        };
        return summarize_droid_session_file(&settings_path);
    }

    summarize_codex_session_file(&absolute_path)
        .or_else(|| summarize_claude_session_file(&absolute_path))
        .or_else(|| summarize_droid_session_file(&absolute_path))
}

fn session_summary_in_scope(summary: &SessionSummary, options: &ScanOptions) -> bool {
    if options.global {
        return true;
    }

    summary
        .cwd
        .as_deref()
        .is_some_and(|cwd| path_in_scope(cwd, &options.root))
}

fn session_summary_matches(summary: &SessionSummary, query: &str) -> bool {
    let path_text = summary.path.to_string_lossy();
    let file_name = summary
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    let file_stem = summary
        .path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("");

    summary.id == query
        || (query.len() >= 8 && summary.id.starts_with(query))
        || file_name == query
        || file_stem == query
        || path_text == query
}

fn session_summary_sort(a: &SessionSummary, b: &SessionSummary) -> Ordering {
    b.updated_at
        .cmp(&a.updated_at)
        .then_with(|| b.started_at.cmp(&a.started_at))
        .then_with(|| a.provider.label().cmp(b.provider.label()))
        .then_with(|| a.id.cmp(&b.id))
}

fn summarize_codex_session_file(path: &Path) -> Option<SessionSummary> {
    let file = File::open(path).ok()?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut line = String::new();

    let mut saw_codex_marker = false;
    let mut id = session_id_from_path(path);
    let mut cwd: Option<PathBuf> = None;
    let mut started_at: Option<String> = None;
    let mut updated_at: Option<String> = None;
    let mut turns = 0usize;
    let mut first_prompt: Option<String> = None;

    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                let Ok(value) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };

                if let Some(timestamp) = value.get("timestamp").and_then(Value::as_str) {
                    started_at.get_or_insert_with(|| timestamp.to_string());
                    updated_at = Some(timestamp.to_string());
                }

                match value.get("type").and_then(Value::as_str) {
                    Some("session_meta") => {
                        saw_codex_marker = true;
                        if let Some(payload) = value.get("payload") {
                            if let Some(payload_id) = payload.get("id").and_then(Value::as_str) {
                                id = payload_id.to_string();
                            }
                            if let Some(timestamp) =
                                payload.get("timestamp").and_then(Value::as_str)
                            {
                                started_at = Some(timestamp.to_string());
                                updated_at.get_or_insert_with(|| timestamp.to_string());
                            }
                            if let Some(payload_cwd) = payload.get("cwd").and_then(Value::as_str) {
                                cwd = Some(PathBuf::from(payload_cwd));
                            }
                        }
                    }
                    Some("turn_context") => {
                        saw_codex_marker = true;
                        if let Some(payload_cwd) = value
                            .get("payload")
                            .and_then(|payload| payload.get("cwd"))
                            .and_then(Value::as_str)
                        {
                            cwd = Some(PathBuf::from(payload_cwd));
                        }
                    }
                    Some("response_item") => {
                        let Some(payload) = value.get("payload") else {
                            continue;
                        };
                        if payload.get("type").and_then(Value::as_str) != Some("message")
                            || payload.get("role").and_then(Value::as_str) != Some("user")
                        {
                            continue;
                        }
                        saw_codex_marker = true;
                        if let Some(prompt) =
                            extract_user_prompt_from_content(payload.get("content"))
                        {
                            turns += 1;
                            first_prompt.get_or_insert(prompt);
                        }
                    }
                    _ => {}
                }
            }
            Err(_) => break,
        }
    }

    if !saw_codex_marker {
        return None;
    }

    Some(SessionSummary {
        provider: SessionProvider::Codex,
        id,
        path: path.to_path_buf(),
        cwd,
        started_at,
        updated_at,
        turns,
        first_prompt,
    })
}

fn summarize_claude_session_file(path: &Path) -> Option<SessionSummary> {
    let file = File::open(path).ok()?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut line = String::new();

    let mut saw_claude_marker = false;
    let mut id = session_id_from_path(path);
    let mut cwd: Option<PathBuf> = None;
    let mut started_at: Option<String> = None;
    let mut updated_at: Option<String> = None;
    let mut turns = 0usize;
    let mut first_prompt: Option<String> = None;

    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                let Ok(value) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };

                if let Some(timestamp) = value.get("timestamp").and_then(Value::as_str) {
                    started_at.get_or_insert_with(|| timestamp.to_string());
                    updated_at = Some(timestamp.to_string());
                }
                if let Some(session_id) = value
                    .get("sessionId")
                    .or_else(|| value.get("session_id"))
                    .and_then(Value::as_str)
                {
                    id = session_id.to_string();
                }
                if let Some(line_cwd) = value.get("cwd").and_then(Value::as_str) {
                    cwd.get_or_insert_with(|| PathBuf::from(line_cwd));
                }

                let top_level_kind = value.get("type").and_then(Value::as_str);
                if matches!(top_level_kind, Some("user" | "assistant")) {
                    saw_claude_marker = true;
                }

                let message = value.get("message");
                let role = message
                    .and_then(|message| message.get("role"))
                    .and_then(Value::as_str)
                    .or(top_level_kind);
                if role != Some("user") {
                    continue;
                }

                let prompt = message
                    .and_then(|message| extract_user_prompt_from_content(message.get("content")))
                    .or_else(|| extract_user_prompt_from_content(value.get("content")));
                if let Some(prompt) = prompt {
                    turns += 1;
                    first_prompt.get_or_insert(prompt);
                }
            }
            Err(_) => break,
        }
    }

    if !saw_claude_marker {
        return None;
    }

    Some(SessionSummary {
        provider: SessionProvider::Claude,
        id,
        path: path.to_path_buf(),
        cwd,
        started_at,
        updated_at,
        turns,
        first_prompt,
    })
}

fn summarize_droid_session_file(settings_path: &Path) -> Option<SessionSummary> {
    let settings: DroidSettings = File::open(settings_path)
        .ok()
        .and_then(|file| serde_json::from_reader(BufReader::new(file)).ok())?;

    let jsonl_path = settings_path_to_jsonl(settings_path);
    let (cwd, turns, first_prompt, started_at) = summarize_droid_jsonl(&jsonl_path);

    Some(SessionSummary {
        provider: SessionProvider::Droid,
        id: droid_session_id_from_settings_path(settings_path),
        path: settings_path.to_path_buf(),
        cwd,
        started_at,
        updated_at: settings.provider_lock_timestamp,
        turns,
        first_prompt,
    })
}

fn summarize_droid_jsonl(
    jsonl_path: &Path,
) -> (Option<PathBuf>, usize, Option<String>, Option<String>) {
    let Some(file) = File::open(jsonl_path).ok() else {
        return (None, 0, None, None);
    };
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut line = String::new();
    let mut cwd: Option<PathBuf> = None;
    let mut turns = 0usize;
    let mut first_prompt: Option<String> = None;
    let mut started_at: Option<String> = None;

    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                let Ok(value) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if let Some(timestamp) = value.get("timestamp").and_then(Value::as_str) {
                    started_at.get_or_insert_with(|| timestamp.to_string());
                }
                if value.get("type").and_then(Value::as_str) == Some("session_start") {
                    if let Some(line_cwd) = value.get("cwd").and_then(Value::as_str) {
                        cwd = Some(PathBuf::from(line_cwd));
                    }
                }
                let role = value
                    .get("role")
                    .and_then(Value::as_str)
                    .or_else(|| value.get("type").and_then(Value::as_str));
                if role != Some("user") {
                    continue;
                }
                if let Some(prompt) = extract_user_prompt_from_content(value.get("content")) {
                    turns += 1;
                    first_prompt.get_or_insert(prompt);
                }
            }
            Err(_) => break,
        }
    }

    (cwd, turns, first_prompt, started_at)
}

fn extract_user_prompt_from_content(content: Option<&Value>) -> Option<String> {
    let text = extract_content_text(content?)?;
    let text = normalize_prompt_snippet(&text);
    if is_synthetic_prompt_text(&text) {
        None
    } else {
        Some(truncate_prompt_snippet(&text, SESSION_PROMPT_SNIPPET_CHARS))
    }
}

fn extract_content_text(content: &Value) -> Option<String> {
    match content {
        Value::String(text) => Some(text.clone()),
        Value::Array(items) => {
            let parts = items
                .iter()
                .filter_map(|item| {
                    item.get("text")
                        .and_then(Value::as_str)
                        .or_else(|| item.as_str())
                })
                .collect::<Vec<_>>();
            if parts.is_empty() {
                None
            } else {
                Some(parts.join("\n"))
            }
        }
        Value::Object(_) => content
            .get("text")
            .and_then(Value::as_str)
            .map(str::to_string),
        _ => None,
    }
}

fn normalize_prompt_snippet(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn is_synthetic_prompt_text(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with("<environment_context>")
        || trimmed.starts_with("<skill>")
        || trimmed.starts_with("<permissions instructions>")
}

fn truncate_prompt_snippet(text: &str, max_chars: usize) -> String {
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn session_id_from_path(path: &Path) -> String {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("<unknown>");
    stem.strip_prefix("rollout-").unwrap_or(stem).to_string()
}

fn droid_session_id_from_settings_path(settings_path: &Path) -> String {
    settings_path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_suffix(".settings.json"))
        .unwrap_or("<unknown>")
        .to_string()
}

fn jsonl_path_to_settings(jsonl_path: &Path) -> PathBuf {
    let name = jsonl_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    let settings_name = name.replace(".jsonl", ".settings.json");
    jsonl_path.with_file_name(settings_name)
}

fn scan_all_codex(options: &ScanOptions) -> ProviderReport {
    if let Some(session) = &options.selected_session {
        if session.provider != SessionProvider::Codex {
            return ProviderReport::default();
        }
        return scan_codex_files(std::slice::from_ref(&session.path), options);
    }

    let local_files = discover_jsonl_files(&options.codex_root);
    scan_codex_files(&local_files, options)
}

fn scan_all_claude(options: &ScanOptions) -> ProviderReport {
    if let Some(session) = &options.selected_session {
        if session.provider != SessionProvider::Claude {
            return ProviderReport::default();
        }
        return scan_claude_files(std::slice::from_ref(&session.path), options);
    }

    let local_files = discover_jsonl_files(&options.claude_root);
    scan_claude_files(&local_files, options)
}

fn scan_all_droid(options: &ScanOptions) -> ProviderReport {
    if let Some(session) = &options.selected_session {
        if session.provider != SessionProvider::Droid {
            return ProviderReport::default();
        }
        return scan_droid_files(std::slice::from_ref(&session.path), options);
    }

    let local_files = discover_settings_files(&options.droid_root);
    scan_droid_files(&local_files, options)
}

fn scan_codex_files(files: &[PathBuf], options: &ScanOptions) -> ProviderReport {
    scan_codex_files_with_host_label(files, LOCAL_HOST_LABEL, options)
}

fn scan_codex_files_with_host_label(
    files: &[PathBuf],
    host_label: &str,
    options: &ScanOptions,
) -> ProviderReport {
    if options.parallel {
        files
            .par_iter()
            .map(|path| scan_codex_file_with_host_label(path, host_label, options))
            .reduce(ProviderReport::default, |a, b| a + b)
    } else {
        files
            .iter()
            .map(|path| scan_codex_file_with_host_label(path, host_label, options))
            .fold(ProviderReport::default(), |acc, item| acc + item)
    }
}

fn scan_claude_files(files: &[PathBuf], options: &ScanOptions) -> ProviderReport {
    scan_claude_files_with_host_label(files, LOCAL_HOST_LABEL, options)
}

fn scan_claude_files_with_host_label(
    files: &[PathBuf],
    host_label: &str,
    options: &ScanOptions,
) -> ProviderReport {
    if options.parallel {
        files
            .par_iter()
            .map(|path| scan_claude_file_with_host_label(path, host_label, options))
            .reduce(ProviderReport::default, |a, b| a + b)
    } else {
        files
            .iter()
            .map(|path| scan_claude_file_with_host_label(path, host_label, options))
            .fold(ProviderReport::default(), |acc, item| acc + item)
    }
}

fn scan_codex_file_with_host_label(
    path: &Path,
    host_label: &str,
    options: &ScanOptions,
) -> ProviderReport {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => {
            return ProviderReport {
                files_scanned: 1,
                parse_errors: 1,
                ..ProviderReport::default()
            };
        }
    };

    let mut reader = BufReader::with_capacity(256 * 1024, file);
    parse_codex_reader(&mut reader, host_label, options)
}

fn parse_codex_reader<R: BufRead>(
    reader: &mut R,
    host_label: &str,
    options: &ScanOptions,
) -> ProviderReport {
    let mut report = ProviderReport {
        files_scanned: 1,
        ..ProviderReport::default()
    };

    let mut line = String::new();
    let mut session_cwd: Option<PathBuf> = None;
    let mut current_model: Option<String> = None;
    let mut previous_totals = TokenTotals::default();
    let mut session_totals = TokenTotals::default();
    let mut by_model: HashMap<String, ModelUsage> = HashMap::new();
    let mut day_model_usage: HashMap<String, HashMap<String, ModelUsage>> = HashMap::new();
    let mut host_model_usage: HashMap<String, HashMap<String, ModelUsage>> = HashMap::new();
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

                if line.contains("\"type\":\"turn_context\"") {
                    match serde_json::from_str::<CodexTurnContextLine>(&line) {
                        Ok(parsed) => {
                            if parsed.kind == "turn_context"
                                && let Some(payload) = parsed.payload
                            {
                                if let Some(cwd) = payload.cwd {
                                    session_cwd = Some(PathBuf::from(cwd));
                                }
                                if let Some(model) = payload.model {
                                    current_model = Some(model);
                                }
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
                                let delta = usage.delta_from_cumulative(previous_totals);
                                previous_totals = usage;
                                if delta.is_zero() {
                                    continue;
                                }
                                if !timestamp_in_window(
                                    parsed.timestamp.as_deref(),
                                    options.window.as_ref(),
                                ) {
                                    continue;
                                }

                                session_totals += delta;
                                let model = current_model
                                    .as_deref()
                                    .unwrap_or(UNKNOWN_MODEL)
                                    .to_string();
                                by_model.entry(model).or_default().add_record(delta);
                                host_model_usage
                                    .entry(host_label.to_string())
                                    .or_default()
                                    .entry(
                                        current_model
                                            .as_deref()
                                            .unwrap_or(UNKNOWN_MODEL)
                                            .to_string(),
                                    )
                                    .or_default()
                                    .add_record(delta);
                                if let Some(day_key) =
                                    timestamp_day_key(parsed.timestamp.as_deref())
                                {
                                    day_model_usage
                                        .entry(day_key)
                                        .or_default()
                                        .entry(
                                            current_model
                                                .as_deref()
                                                .unwrap_or(UNKNOWN_MODEL)
                                                .to_string(),
                                        )
                                        .or_default()
                                        .add_record(delta);
                                }
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

    if !options.global && options.selected_session.is_none() {
        let Some(cwd) = session_cwd else {
            return report;
        };

        if !path_in_scope(&cwd, &options.root) {
            return report;
        }
    }

    report.records_counted = 1;
    report.totals = session_totals;
    report.model_usage = by_model;
    report.day_model_usage = day_model_usage;
    report.host_model_usage = host_model_usage;
    report
}

fn scan_claude_file_with_host_label(
    path: &Path,
    host_label: &str,
    options: &ScanOptions,
) -> ProviderReport {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => {
            return ProviderReport {
                files_scanned: 1,
                parse_errors: 1,
                ..ProviderReport::default()
            };
        }
    };

    let mut reader = BufReader::with_capacity(256 * 1024, file);
    parse_claude_reader(
        &mut reader,
        &path.display().to_string(),
        host_label,
        options,
    )
}

fn parse_claude_reader<R: BufRead>(
    reader: &mut R,
    source_label: &str,
    host_label: &str,
    options: &ScanOptions,
) -> ProviderReport {
    let mut report = ProviderReport {
        files_scanned: 1,
        ..ProviderReport::default()
    };

    let mut line = String::new();
    let mut by_request: HashMap<String, ClaudeRequestUsage> = HashMap::new();
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

                        if !options.global && options.selected_session.is_none() {
                            let Some(cwd) = parsed.cwd.as_deref() else {
                                continue;
                            };
                            if !path_in_scope(Path::new(cwd), &options.root) {
                                continue;
                            }
                        }
                        if !timestamp_in_window(
                            parsed.timestamp.as_deref(),
                            options.window.as_ref(),
                        ) {
                            continue;
                        }

                        let (message_id, model, usage) = parsed
                            .message
                            .and_then(|message| {
                                message
                                    .usage
                                    .map(|usage| (message.id, message.model, usage))
                            })
                            .unwrap_or((None, None, ClaudeUsage::default()));

                        let totals = usage.to_totals();
                        if totals.is_zero() {
                            continue;
                        }

                        let day_key = timestamp_day_key(parsed.timestamp.as_deref());
                        let model = model.unwrap_or_else(|| UNKNOWN_MODEL.to_string());

                        let key = parsed
                            .request_id
                            .or(message_id)
                            .or(parsed.uuid)
                            .unwrap_or_else(|| format!("{source_label}:{line_no}"));

                        by_request
                            .entry(key)
                            .and_modify(|existing| {
                                existing.totals.max_assign(&totals);
                                if existing.model == UNKNOWN_MODEL && model != UNKNOWN_MODEL {
                                    existing.model = model.clone();
                                }
                                if existing.day_key.is_none() {
                                    existing.day_key = day_key.clone();
                                }
                            })
                            .or_insert(ClaudeRequestUsage {
                                day_key,
                                model,
                                totals,
                            });
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
    let mut by_model: HashMap<String, ModelUsage> = HashMap::new();
    let mut day_model_usage: HashMap<String, HashMap<String, ModelUsage>> = HashMap::new();
    let mut totals = TokenTotals::default();

    for usage in by_request.into_values() {
        totals += usage.totals;
        let day_key = usage.day_key;
        let model = usage.model;
        by_model
            .entry(model.clone())
            .or_default()
            .add_record(usage.totals);
        report
            .host_model_usage
            .entry(host_label.to_string())
            .or_default()
            .entry(model.clone())
            .or_default()
            .add_record(usage.totals);
        if let Some(day_key) = day_key {
            day_model_usage
                .entry(day_key)
                .or_default()
                .entry(model)
                .or_default()
                .add_record(usage.totals);
        }
    }

    report.totals = totals;
    report.model_usage = by_model;
    report.day_model_usage = day_model_usage;
    report
}

fn fetch_remote_usage_report(
    host: &str,
    options: &ScanOptions,
) -> Result<RemoteUsageReport, String> {
    let script = build_remote_mot_script(options);
    let remote_command = format!("sh -lc {}", shell_single_quote(&script));
    let output = Command::new("ssh")
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            REMOTE_SSH_CLEAR_FORWARDINGS,
            "-o",
            REMOTE_SSH_CONNECT_TIMEOUT,
            "-o",
            REMOTE_SSH_DISABLE_REMOTE_COMMAND,
            host,
            &remote_command,
        ])
        .output()
        .map_err(|err| format!("host {host}: failed to start ssh: {err}"))?;

    let stderr_text = String::from_utf8_lossy(&output.stderr);
    let stdout_text = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() {
        if let Some(warning) = classify_remote_mot_failure(host, &stderr_text, &stdout_text) {
            return Err(warning);
        }
        let mut details = Vec::new();
        if let Some(summary) = summarize_output(&stderr_text) {
            details.push(summary);
        }
        if let Some(summary) = summarize_output(&stdout_text) {
            details.push(summary);
        }
        if details.is_empty() {
            return Err(format!("host {host}: remote mot --json failed"));
        }
        return Err(format!(
            "host {host}: remote mot --json failed: {}",
            details.join(" | ")
        ));
    }

    serde_json::from_slice::<RemoteUsageReport>(&output.stdout).map_err(|err| {
        if let Some(warning) = classify_remote_mot_parse_failure(host, &stdout_text, &stderr_text) {
            return warning;
        }
        let mut details = Vec::new();
        if let Some(summary) = summarize_output(&stderr_text) {
            details.push(format!("stderr: {summary}"));
        }
        if let Some(summary) = summarize_output(&stdout_text) {
            details.push(format!("stdout: {summary}"));
        }
        if details.is_empty() {
            format!("host {host}: remote mot --json returned invalid JSON: {err}")
        } else {
            format!(
                "host {host}: remote mot --json returned invalid JSON: {err}; {}",
                details.join(" | ")
            )
        }
    })
}

fn classify_remote_mot_failure(host: &str, stderr: &str, stdout: &str) -> Option<String> {
    let combined = format!("{stderr}\n{stdout}");
    let combined_lower = combined.to_ascii_lowercase();

    if combined_lower.contains("mot not found in path")
        || combined_lower.contains(": mot: not found")
        || combined_lower.contains("command not found")
    {
        return Some(format!(
            "host {host}: remote mot is not installed or not in PATH; skipping host"
        ));
    }

    let unknown_json_flag = combined_lower.contains("--json")
        && (combined_lower.contains("unexpected argument")
            || combined_lower.contains("unknown argument")
            || combined_lower.contains("unrecognized option")
            || combined_lower.contains("invalid option"));
    if unknown_json_flag {
        return Some(format!(
            "host {host}: remote mot does not support --json (older version); skipping host"
        ));
    }

    None
}

fn classify_remote_mot_parse_failure(host: &str, stdout: &str, _stderr: &str) -> Option<String> {
    let trimmed = stdout.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return Some(format!(
            "host {host}: remote mot returned an incompatible JSON schema; skipping host (upgrade mot on remote)"
        ));
    }
    if !trimmed.is_empty() {
        return Some(format!(
            "host {host}: remote mot did not return JSON output; skipping host (ensure remote mot supports --json)"
        ));
    }
    None
}

fn build_remote_mot_script(options: &ScanOptions) -> String {
    let mut args = vec![REMOTE_MOT_BINARY.to_string(), "--json".to_string()];
    if options.global {
        args.push("--global".to_string());
    } else {
        args.push("--root".to_string());
        args.push(options.root.display().to_string());
    }
    if let Some(window) = &options.window {
        args.push("--window".to_string());
        args.push(window.spec.clone());
    }
    let command = args
        .iter()
        .map(|arg| shell_single_quote(arg))
        .collect::<Vec<_>>()
        .join(" ");

    format!(
        "if ! command -v {binary} >/dev/null 2>&1; then echo '{binary} not found in PATH' >&2; exit 127; fi; {command}",
        binary = REMOTE_MOT_BINARY,
    )
}

fn summarize_output(output: &str) -> Option<String> {
    let summary = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" | ");

    if summary.is_empty() {
        None
    } else {
        Some(truncate_warning_summary(&summary, 280))
    }
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn truncate_warning_summary(text: &str, max_chars: usize) -> String {
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
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
struct CodexTurnContextLine {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    payload: Option<CodexTurnContextPayload>,
}

#[derive(Debug, Deserialize)]
struct CodexTurnContextPayload {
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    model: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CodexTokenCountLine {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    timestamp: Option<String>,
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
    timestamp: Option<String>,
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
    model: Option<String>,
    #[serde(default)]
    usage: Option<ClaudeUsage>,
}

#[derive(Debug)]
struct ClaudeRequestUsage {
    day_key: Option<String>,
    model: String,
    totals: TokenTotals,
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

#[derive(Debug, Deserialize)]
struct DroidSessionStart {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    cwd: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DroidSettings {
    #[serde(default)]
    model: Option<String>,
    #[serde(rename = "tokenUsage")]
    token_usage: Option<DroidTokenUsage>,
    #[serde(rename = "providerLockTimestamp")]
    provider_lock_timestamp: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct DroidTokenUsage {
    #[serde(rename = "inputTokens", default)]
    input_tokens: u64,
    #[serde(rename = "outputTokens", default)]
    output_tokens: u64,
    #[serde(rename = "cacheCreationTokens", default)]
    cache_creation_tokens: u64,
    #[serde(rename = "cacheReadTokens", default)]
    cache_read_tokens: u64,
    #[serde(rename = "thinkingTokens", default)]
    thinking_tokens: u64,
}

impl DroidTokenUsage {
    fn to_totals(&self) -> TokenTotals {
        TokenTotals {
            input: self.input_tokens,
            output: self.output_tokens,
            thinking: self.thinking_tokens,
            cache_read: self.cache_read_tokens,
            cache_write: self.cache_creation_tokens,
        }
    }
}

fn discover_settings_files(root: &Path) -> Vec<PathBuf> {
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
                .to_str()
                .is_some_and(|s| s.ends_with(".settings.json"))
        })
        .map(|entry| entry.into_path())
        .collect()
}

fn scan_droid_files(files: &[PathBuf], options: &ScanOptions) -> ProviderReport {
    scan_droid_files_with_host_label(files, LOCAL_HOST_LABEL, options)
}

fn scan_droid_files_with_host_label(
    files: &[PathBuf],
    host_label: &str,
    options: &ScanOptions,
) -> ProviderReport {
    if options.parallel {
        files
            .par_iter()
            .map(|path| scan_droid_file_with_host_label(path, host_label, options))
            .reduce(ProviderReport::default, |a, b| a + b)
    } else {
        files
            .iter()
            .map(|path| scan_droid_file_with_host_label(path, host_label, options))
            .fold(ProviderReport::default(), |acc, item| acc + item)
    }
}

fn scan_droid_file_with_host_label(
    settings_path: &Path,
    host_label: &str,
    options: &ScanOptions,
) -> ProviderReport {
    let settings: DroidSettings = match File::open(settings_path)
        .ok()
        .and_then(|file| serde_json::from_reader(BufReader::new(file)).ok())
    {
        Some(s) => s,
        None => {
            return ProviderReport {
                files_scanned: 1,
                parse_errors: 1,
                ..ProviderReport::default()
            };
        }
    };

    let session_cwd = if options.global || options.selected_session.is_some() {
        None
    } else {
        let jsonl_path = settings_path_to_jsonl(settings_path);
        read_droid_session_cwd(&jsonl_path)
    };

    build_droid_report_from_settings(settings, host_label, session_cwd.as_deref(), options)
}

fn settings_path_to_jsonl(settings_path: &Path) -> PathBuf {
    let name = settings_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");
    let jsonl_name = name.replace(".settings.json", ".jsonl");
    settings_path.with_file_name(jsonl_name)
}

fn read_droid_session_cwd(jsonl_path: &Path) -> Option<String> {
    let file = File::open(jsonl_path).ok()?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let parsed: DroidSessionStart = serde_json::from_str(&line).ok()?;
    if parsed.kind == "session_start" {
        parsed.cwd
    } else {
        None
    }
}

fn build_droid_report_from_settings(
    settings: DroidSettings,
    host_label: &str,
    session_cwd: Option<&str>,
    options: &ScanOptions,
) -> ProviderReport {
    let DroidSettings {
        model,
        token_usage,
        provider_lock_timestamp,
    } = settings;

    let mut report = ProviderReport {
        files_scanned: 1,
        ..ProviderReport::default()
    };

    let usage = match token_usage {
        Some(u) => u,
        None => return report,
    };

    let totals = usage.to_totals();
    if totals.is_zero() {
        return report;
    }

    let timestamp = provider_lock_timestamp.as_deref();
    if !timestamp_in_window(timestamp, options.window.as_ref()) {
        return report;
    }

    if !options.global && options.selected_session.is_none() {
        match session_cwd {
            Some(cwd) if path_in_scope(Path::new(cwd), &options.root) => {}
            _ => return report,
        }
    }

    let model = model.unwrap_or_else(|| UNKNOWN_MODEL.to_string());
    report.records_counted = 1;
    report.totals = totals;
    report
        .model_usage
        .entry(model.clone())
        .or_default()
        .add_record(totals);
    report
        .host_model_usage
        .entry(host_label.to_string())
        .or_default()
        .entry(model.clone())
        .or_default()
        .add_record(totals);

    if let Some(day_key) = timestamp_day_key(timestamp) {
        report
            .day_model_usage
            .entry(day_key)
            .or_default()
            .entry(model)
            .or_default()
            .add_record(totals);
    }

    report
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
            droid_root: temp.path().join("missing"),
            parallel: false,
            window: None,
            ssh_hosts: Vec::new(),
            selected_session: None,
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
            droid_root: temp.path().join("missing"),
            parallel: false,
            window: None,
            ssh_hosts: Vec::new(),
            selected_session: None,
        };

        let report = collect_usage(&options);
        assert_eq!(report.claude.records_counted, 2);
        assert_eq!(report.claude.totals.input, 11);
        assert_eq!(report.claude.totals.output, 3);
        assert_eq!(report.claude.totals.cache_read, 4);
        assert_eq!(report.claude.totals.cache_write, 5);
    }

    #[test]
    fn estimates_cost_for_known_openai_and_anthropic_models() {
        let temp = tempdir().expect("create tempdir");

        let codex_root = temp.path().join("codex");
        fs::create_dir_all(&codex_root).expect("create codex root");
        fs::write(
            codex_root.join("session.jsonl"),
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"cwd\":\"/tmp/proj\"}}\n",
                "{\"type\":\"turn_context\",\"payload\":{\"cwd\":\"/tmp/proj\",\"model\":\"gpt-5.2-codex\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":100,\"cached_input_tokens\":30,\"output_tokens\":10,\"reasoning_output_tokens\":2}}}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":150,\"cached_input_tokens\":50,\"output_tokens\":20,\"reasoning_output_tokens\":4}}}}\n"
            ),
        )
        .expect("write codex session");

        let claude_root = temp.path().join("claude/projects/test");
        fs::create_dir_all(&claude_root).expect("create claude root");
        fs::write(
            claude_root.join("session.jsonl"),
            concat!(
                "{\"type\":\"assistant\",\"cwd\":\"/tmp/proj\",\"requestId\":\"req1\",\"message\":{\"model\":\"claude-sonnet-4-5-20250929\",\"id\":\"m1\",\"usage\":{\"input_tokens\":100,\"output_tokens\":20,\"cache_read_input_tokens\":50,\"cache_creation_input_tokens\":40}}}\n",
                "{\"type\":\"assistant\",\"cwd\":\"/tmp/proj\",\"requestId\":\"req1\",\"message\":{\"model\":\"claude-sonnet-4-5-20250929\",\"id\":\"m1\",\"usage\":{\"input_tokens\":100,\"output_tokens\":20,\"cache_read_input_tokens\":50,\"cache_creation_input_tokens\":40}}}\n"
            ),
        )
        .expect("write claude session");

        let options = ScanOptions {
            global: true,
            root: PathBuf::from("/tmp/proj"),
            codex_root,
            claude_root: temp.path().join("claude/projects"),
            droid_root: temp.path().join("missing"),
            parallel: false,
            window: None,
            ssh_hosts: Vec::new(),
            selected_session: None,
        };

        let report = collect_usage(&options);

        let expected_codex = (150.0 * 1.75 + 50.0 * 0.175 + 20.0 * 14.0 + 4.0 * 14.0) / 1_000_000.0;
        let expected_claude = (100.0 * 3.0 + 50.0 * 0.30 + 40.0 * 3.75 + 20.0 * 15.0) / 1_000_000.0;
        let expected_total = expected_codex + expected_claude;

        assert!((report.codex.estimated_cost_usd - expected_codex).abs() < 1e-12);
        assert!((report.claude.estimated_cost_usd - expected_claude).abs() < 1e-12);
        assert!((report.estimated_cost_usd - expected_total).abs() < 1e-12);
        assert!(report.unpriced_totals.is_zero());

        let rendered = render_report(&report);
        assert!(rendered.contains("By host:"));
        assert!(rendered.contains("local"));
        assert!(rendered.contains("By model:"));
        assert!(rendered.contains("gpt-5.2-codex"));
        assert!(rendered.contains("claude-sonnet-4-5-20250929"));
    }

    #[test]
    fn parse_time_window_supports_common_units() {
        let now = 1_800_000_000_000i64;

        let day = parse_time_window_at("1d", now).expect("parse 1d");
        assert_eq!(day.cutoff_unix_ms, now - 86_400_000);

        let week = parse_time_window_at("7d", now).expect("parse 7d");
        assert_eq!(week.cutoff_unix_ms, now - 7 * 86_400_000);

        let month = parse_time_window_at("1m", now).expect("parse 1m");
        assert_eq!(month.cutoff_unix_ms, now - 30 * 86_400_000);

        let year = parse_time_window_at("1y", now).expect("parse 1y");
        assert_eq!(year.cutoff_unix_ms, now - 365 * 86_400_000);
    }

    #[test]
    fn window_filters_records_by_timestamp() {
        let temp = tempdir().expect("create tempdir");

        let codex_root = temp.path().join("codex");
        fs::create_dir_all(&codex_root).expect("create codex root");
        fs::write(
            codex_root.join("session.jsonl"),
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"cwd\":\"/tmp/proj\"}}\n",
                "{\"type\":\"turn_context\",\"payload\":{\"cwd\":\"/tmp/proj\",\"model\":\"gpt-5.2-codex\"}}\n",
                "{\"timestamp\":\"2026-03-01T00:00:00Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":100,\"cached_input_tokens\":10,\"output_tokens\":10,\"reasoning_output_tokens\":2}}}}\n",
                "{\"timestamp\":\"2026-03-20T00:00:00Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":150,\"cached_input_tokens\":20,\"output_tokens\":20,\"reasoning_output_tokens\":4}}}}\n"
            ),
        )
        .expect("write codex session");

        let claude_root = temp.path().join("claude/projects/test");
        fs::create_dir_all(&claude_root).expect("create claude root");
        fs::write(
            claude_root.join("session.jsonl"),
            concat!(
                "{\"timestamp\":\"2026-03-02T00:00:00Z\",\"type\":\"assistant\",\"cwd\":\"/tmp/proj\",\"requestId\":\"req-old\",\"message\":{\"model\":\"claude-sonnet-4-5-20250929\",\"id\":\"m-old\",\"usage\":{\"input_tokens\":100,\"output_tokens\":10,\"cache_read_input_tokens\":5,\"cache_creation_input_tokens\":2}}}\n",
                "{\"timestamp\":\"2026-03-21T00:00:00Z\",\"type\":\"assistant\",\"cwd\":\"/tmp/proj\",\"requestId\":\"req-new\",\"message\":{\"model\":\"claude-sonnet-4-5-20250929\",\"id\":\"m-new\",\"usage\":{\"input_tokens\":30,\"output_tokens\":6,\"cache_read_input_tokens\":3,\"cache_creation_input_tokens\":1}}}\n"
            ),
        )
        .expect("write claude session");

        let cutoff_unix_ms = parse_rfc3339_unix_ms("2026-03-15T00:00:00Z").expect("parse cutoff");
        let options = ScanOptions {
            global: true,
            root: PathBuf::from("/tmp/proj"),
            codex_root,
            claude_root: temp.path().join("claude/projects"),
            droid_root: temp.path().join("missing"),
            parallel: false,
            window: Some(TimeWindow {
                spec: "7d".to_string(),
                cutoff_unix_ms,
            }),
            ssh_hosts: Vec::new(),
            selected_session: None,
        };

        let report = collect_usage(&options);
        assert_eq!(report.codex.totals.input, 50);
        assert_eq!(report.codex.totals.output, 10);
        assert_eq!(report.codex.totals.cache_read, 10);
        assert_eq!(report.claude.totals.input, 30);
        assert_eq!(report.claude.totals.output, 6);
        assert_eq!(report.claude.totals.cache_read, 3);
        assert_eq!(report.claude.totals.cache_write, 1);
        assert_eq!(report.total.input, 80);
        assert_eq!(report.total.output, 16);
    }

    #[test]
    fn topbar_snapshot_combines_daily_totals_and_pads_missing_days() {
        let temp = tempdir().expect("create tempdir");

        let codex_root = temp.path().join("codex");
        fs::create_dir_all(&codex_root).expect("create codex root");
        fs::write(
            codex_root.join("session.jsonl"),
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"cwd\":\"/tmp/proj\"}}\n",
                "{\"type\":\"turn_context\",\"payload\":{\"cwd\":\"/tmp/proj\",\"model\":\"gpt-5.2-codex\"}}\n",
                "{\"timestamp\":\"2026-03-19T00:00:00Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":100,\"cached_input_tokens\":10,\"output_tokens\":5,\"reasoning_output_tokens\":2}}}}\n",
                "{\"timestamp\":\"2026-03-20T00:00:00Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":150,\"cached_input_tokens\":10,\"output_tokens\":9,\"reasoning_output_tokens\":2}}}}\n"
            ),
        )
        .expect("write codex session");

        let claude_root = temp.path().join("claude/projects/test");
        fs::create_dir_all(&claude_root).expect("create claude root");
        fs::write(
            claude_root.join("session.jsonl"),
            "{\"timestamp\":\"2026-03-20T12:00:00Z\",\"type\":\"assistant\",\"cwd\":\"/tmp/proj\",\"requestId\":\"req1\",\"message\":{\"model\":\"claude-sonnet-4-5-20250929\",\"id\":\"m1\",\"usage\":{\"input_tokens\":50,\"output_tokens\":10,\"cache_read_input_tokens\":5,\"cache_creation_input_tokens\":2}}}\n",
        )
        .expect("write claude session");

        let options = ScanOptions {
            global: true,
            root: PathBuf::from("/tmp/proj"),
            codex_root,
            claude_root: temp.path().join("claude/projects"),
            droid_root: temp.path().join("missing"),
            parallel: false,
            window: None,
            ssh_hosts: Vec::new(),
            selected_session: None,
        };

        let report = collect_usage(&options);
        let day_keys = vec![
            "2026-03-19".to_string(),
            "2026-03-20".to_string(),
            "2026-03-21".to_string(),
        ];
        let snapshot = build_topbar_snapshot_for_day_keys(&report, &day_keys);

        assert_eq!(snapshot.days.len(), 3);
        assert_eq!(snapshot.days[0].total_tokens, 117);
        assert_eq!(snapshot.days[1].total_tokens, 121);
        assert_eq!(snapshot.days[2].total_tokens, 0);
        assert_eq!(snapshot.total_tokens, 238);
    }

    #[test]
    fn unknown_models_are_tracked_as_unpriced() {
        let temp = tempdir().expect("create tempdir");
        let codex_root = temp.path().join("codex");
        fs::create_dir_all(&codex_root).expect("create codex root");

        fs::write(
            codex_root.join("session.jsonl"),
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"cwd\":\"/tmp/proj\"}}\n",
                "{\"type\":\"turn_context\",\"payload\":{\"cwd\":\"/tmp/proj\",\"model\":\"mystery-model\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":10,\"cached_input_tokens\":3,\"output_tokens\":2,\"reasoning_output_tokens\":1}}}}\n"
            ),
        )
        .expect("write codex session");

        let options = ScanOptions {
            global: true,
            root: PathBuf::from("/tmp/proj"),
            codex_root,
            claude_root: temp.path().join("missing"),
            droid_root: temp.path().join("missing"),
            parallel: false,
            window: None,
            ssh_hosts: Vec::new(),
            selected_session: None,
        };

        let report = collect_usage(&options);
        assert_eq!(report.estimated_cost_usd, 0.0);
        assert_eq!(report.unpriced_totals.input, 10);
        assert_eq!(report.unpriced_totals.cache_read, 3);
        assert_eq!(report.unpriced_totals.output, 2);
        assert!(
            report
                .unpriced_models
                .contains(&"mystery-model".to_string())
        );
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
            droid_root: temp.path().join("missing"),
            parallel: false,
            window: None,
            ssh_hosts: Vec::new(),
            selected_session: None,
        };

        let report = collect_usage(&options);
        assert_eq!(report.codex.records_counted, 0);
        assert_eq!(report.claude.records_counted, 1);
        assert_eq!(report.total.input, 7);
        assert_eq!(report.by_host.len(), 1);
        assert_eq!(report.by_host[0].host, "local");
        assert_eq!(report.by_host[0].totals.input, 7);
    }

    #[test]
    fn session_summaries_include_turn_count_and_first_prompt() {
        let temp = tempdir().expect("create tempdir");
        let codex_root = temp.path().join("codex");
        fs::create_dir_all(&codex_root).expect("create codex root");

        fs::write(
            codex_root.join("in-scope.jsonl"),
            concat!(
                "{\"timestamp\":\"2026-03-20T00:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"s1\",\"timestamp\":\"2026-03-20T00:00:00Z\",\"cwd\":\"/tmp/proj\"}}\n",
                "{\"timestamp\":\"2026-03-20T00:00:01Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"<environment_context>ignored</environment_context>\"}]}}\n",
                "{\"timestamp\":\"2026-03-20T00:00:02Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"Build the usage dashboard\\nwith filters\"}]}}\n",
                "{\"timestamp\":\"2026-03-20T00:00:03Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]}}\n",
                "{\"timestamp\":\"2026-03-20T00:00:04Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"Add tests\"}]}}\n"
            ),
        )
        .expect("write codex session");

        fs::write(
            codex_root.join("outside.jsonl"),
            concat!(
                "{\"timestamp\":\"2026-03-21T00:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"s2\",\"timestamp\":\"2026-03-21T00:00:00Z\",\"cwd\":\"/tmp/other\"}}\n",
                "{\"timestamp\":\"2026-03-21T00:00:01Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"Outside\"}]}}\n"
            ),
        )
        .expect("write outside codex session");

        let options = ScanOptions {
            global: false,
            root: PathBuf::from("/tmp/proj"),
            codex_root,
            claude_root: temp.path().join("missing"),
            droid_root: temp.path().join("missing"),
            parallel: false,
            window: None,
            ssh_hosts: Vec::new(),
            selected_session: None,
        };

        let summaries = list_session_summaries(&options);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].provider, SessionProvider::Codex);
        assert_eq!(summaries[0].id, "s1");
        assert_eq!(summaries[0].turns, 2);
        assert_eq!(
            summaries[0].first_prompt.as_deref(),
            Some("Build the usage dashboard with filters")
        );
    }

    #[test]
    fn selected_codex_session_filters_other_sessions_and_providers() {
        let temp = tempdir().expect("create tempdir");

        let codex_root = temp.path().join("codex");
        fs::create_dir_all(&codex_root).expect("create codex root");
        fs::write(
            codex_root.join("first.jsonl"),
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"s1\",\"cwd\":\"/tmp/proj\"}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"First session\"}]}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":10,\"output_tokens\":1}}}}\n"
            ),
        )
        .expect("write first codex session");
        fs::write(
            codex_root.join("second.jsonl"),
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"s2\",\"cwd\":\"/tmp/proj\"}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"Second session\"}]}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":20,\"output_tokens\":2}}}}\n"
            ),
        )
        .expect("write second codex session");

        let claude_root = temp.path().join("claude/projects/test");
        fs::create_dir_all(&claude_root).expect("create claude root");
        fs::write(
            claude_root.join("claude.jsonl"),
            "{\"type\":\"assistant\",\"cwd\":\"/tmp/proj\",\"requestId\":\"req1\",\"message\":{\"id\":\"m1\",\"usage\":{\"input_tokens\":99,\"output_tokens\":9}}}\n",
        )
        .expect("write claude session");

        let mut options = ScanOptions {
            global: true,
            root: PathBuf::from("/tmp/proj"),
            codex_root,
            claude_root: temp.path().join("claude/projects"),
            droid_root: temp.path().join("missing"),
            parallel: false,
            window: None,
            ssh_hosts: Vec::new(),
            selected_session: None,
        };
        options.selected_session =
            Some(resolve_session_selection(&options, "s2").expect("resolve selected session"));

        let report = collect_usage(&options);
        assert_eq!(report.codex.records_counted, 1);
        assert_eq!(report.codex.totals.input, 20);
        assert_eq!(report.codex.totals.output, 2);
        assert_eq!(report.claude.records_counted, 0);
        assert_eq!(
            report
                .scope
                .session
                .as_ref()
                .map(|session| session.id.as_str()),
            Some("s2")
        );

        let rendered = render_report(&report);
        assert!(rendered.contains("Session: codex s2"));
        assert!(rendered.contains("First prompt: Second session"));
    }

    #[test]
    fn droid_pricing_uses_standard_token_multipliers() {
        let opus = lookup_droid_pricing("claude-opus-4-5-20251101").expect("opus 4.5 pricing");
        assert!((opus.input_per_mtok_usd - 20.0).abs() < 1e-12);
        assert!((opus.output_per_mtok_usd - 20.0).abs() < 1e-12);
        assert!((opus.cache_read_per_mtok_usd - 2.0).abs() < 1e-12);
        assert!((opus.cache_write_per_mtok_usd - 20.0).abs() < 1e-12);

        let sonnet =
            lookup_droid_pricing("claude-sonnet-4-5-20250929").expect("sonnet 4.5 pricing");
        assert!((sonnet.input_per_mtok_usd - 12.0).abs() < 1e-12);
        assert!((sonnet.cache_read_per_mtok_usd - 1.2).abs() < 1e-12);

        let haiku = lookup_droid_pricing("claude-haiku-4-5-20251001").expect("haiku 4.5 pricing");
        assert!((haiku.input_per_mtok_usd - 4.0).abs() < 1e-12);
        assert!((haiku.cache_read_per_mtok_usd - 0.4).abs() < 1e-12);

        let opus_fast =
            lookup_droid_pricing("claude-opus-4-6-fast").expect("opus 4.6 fast pricing");
        assert!((opus_fast.input_per_mtok_usd - 120.0).abs() < 1e-12);

        assert!(lookup_droid_pricing("mystery-model").is_none());
        assert!(lookup_droid_pricing("<unknown>").is_none());
    }

    #[test]
    fn droid_pricing_differs_from_api_pricing() {
        let droid = lookup_droid_pricing("claude-opus-4-5-20251101").expect("droid pricing");
        let api = lookup_model_pricing("claude-opus-4-5-20251101").expect("api pricing");

        assert_ne!(droid.input_per_mtok_usd, api.input_per_mtok_usd);
        assert_ne!(droid.output_per_mtok_usd, api.output_per_mtok_usd);
    }

    #[test]
    fn droid_scanner_reads_settings_and_jsonl() {
        let temp = tempdir().expect("create tempdir");
        let droid_root = temp.path().join("sessions");
        let session_dir = droid_root.join("-tmp-proj");
        fs::create_dir_all(&session_dir).expect("create session dir");

        fs::write(
            session_dir.join("abc.settings.json"),
            r#"{"model":"claude-opus-4-5-20251101","tokenUsage":{"inputTokens":100,"outputTokens":50,"cacheCreationTokens":20,"cacheReadTokens":500,"thinkingTokens":10},"providerLockTimestamp":"2026-03-20T12:00:00Z"}"#,
        )
        .expect("write settings");

        fs::write(
            session_dir.join("abc.jsonl"),
            "{\"type\":\"session_start\",\"cwd\":\"/tmp/proj\"}\n",
        )
        .expect("write jsonl");

        let options = ScanOptions {
            global: true,
            root: PathBuf::from("/tmp/proj"),
            codex_root: temp.path().join("missing"),
            claude_root: temp.path().join("missing"),
            droid_root,
            parallel: false,
            window: None,
            ssh_hosts: Vec::new(),
            selected_session: None,
        };

        let files = discover_settings_files(&options.droid_root);
        assert_eq!(files.len(), 1);

        let report = scan_droid_files(&files, &options);
        assert_eq!(report.records_counted, 1);
        assert_eq!(report.totals.input, 100);
        assert_eq!(report.totals.output, 50);
        assert_eq!(report.totals.cache_write, 20);
        assert_eq!(report.totals.cache_read, 500);
        assert_eq!(report.totals.thinking, 10);
        assert!(report.model_usage.contains_key("claude-opus-4-5-20251101"));
    }

    #[test]
    fn droid_scanner_filters_by_cwd_in_scoped_mode() {
        let temp = tempdir().expect("create tempdir");
        let droid_root = temp.path().join("sessions");
        let session_dir = droid_root.join("-tmp-proj");
        fs::create_dir_all(&session_dir).expect("create session dir");

        fs::write(
            session_dir.join("abc.settings.json"),
            r#"{"model":"claude-opus-4-5-20251101","tokenUsage":{"inputTokens":100,"outputTokens":50,"cacheCreationTokens":20,"cacheReadTokens":500,"thinkingTokens":10},"providerLockTimestamp":"2026-03-20T12:00:00Z"}"#,
        )
        .expect("write settings");

        fs::write(
            session_dir.join("abc.jsonl"),
            "{\"type\":\"session_start\",\"cwd\":\"/tmp/other\"}\n",
        )
        .expect("write jsonl");

        let files = discover_settings_files(&droid_root);
        let report = scan_droid_files(
            &files,
            &ScanOptions {
                global: false,
                root: PathBuf::from("/tmp/proj"),
                codex_root: temp.path().join("missing"),
                claude_root: temp.path().join("missing"),
                droid_root,
                parallel: false,
                window: None,
                ssh_hosts: Vec::new(),
                selected_session: None,
            },
        );

        assert_eq!(report.records_counted, 0);
        assert!(report.totals.is_zero());
    }

    #[test]
    fn droid_scanner_filters_by_time_window() {
        let temp = tempdir().expect("create tempdir");
        let droid_root = temp.path().join("sessions");
        let session_dir = droid_root.join("-tmp-proj");
        fs::create_dir_all(&session_dir).expect("create session dir");

        fs::write(
            session_dir.join("old.settings.json"),
            r#"{"model":"claude-opus-4-5-20251101","tokenUsage":{"inputTokens":100,"outputTokens":50,"cacheCreationTokens":0,"cacheReadTokens":0,"thinkingTokens":0},"providerLockTimestamp":"2026-03-01T00:00:00Z"}"#,
        )
        .expect("write old settings");
        fs::write(
            session_dir.join("old.jsonl"),
            "{\"type\":\"session_start\",\"cwd\":\"/tmp/proj\"}\n",
        )
        .expect("write old jsonl");

        fs::write(
            session_dir.join("new.settings.json"),
            r#"{"model":"claude-opus-4-5-20251101","tokenUsage":{"inputTokens":200,"outputTokens":30,"cacheCreationTokens":0,"cacheReadTokens":0,"thinkingTokens":0},"providerLockTimestamp":"2026-03-20T00:00:00Z"}"#,
        )
        .expect("write new settings");
        fs::write(
            session_dir.join("new.jsonl"),
            "{\"type\":\"session_start\",\"cwd\":\"/tmp/proj\"}\n",
        )
        .expect("write new jsonl");

        let cutoff_unix_ms = parse_rfc3339_unix_ms("2026-03-15T00:00:00Z").expect("parse cutoff");
        let files = discover_settings_files(&droid_root);
        let report = scan_droid_files(
            &files,
            &ScanOptions {
                global: true,
                root: PathBuf::from("/tmp/proj"),
                codex_root: temp.path().join("missing"),
                claude_root: temp.path().join("missing"),
                droid_root,
                parallel: false,
                window: Some(TimeWindow {
                    spec: "7d".to_string(),
                    cutoff_unix_ms,
                }),
                ssh_hosts: Vec::new(),
                selected_session: None,
            },
        );

        assert_eq!(report.records_counted, 1);
        assert_eq!(report.totals.input, 200);
        assert_eq!(report.totals.output, 30);
    }

    #[test]
    fn merge_provider_report_combines_model_daily_and_host_rows() {
        let mut local = ProviderReport {
            records_counted: 1,
            totals: TokenTotals {
                input: 10,
                output: 1,
                ..TokenTotals::default()
            },
            estimated_cost_usd: 1.0,
            priced_totals: TokenTotals {
                input: 10,
                output: 1,
                ..TokenTotals::default()
            },
            priced_records_counted: 1,
            by_model: vec![ModelReport {
                model: "m1".to_string(),
                records_counted: 1,
                totals: TokenTotals {
                    input: 10,
                    output: 1,
                    ..TokenTotals::default()
                },
                estimated_cost_usd: Some(1.0),
                pricing: None,
            }],
            daily: vec![DailyReport {
                day: "2026-03-20".to_string(),
                records_counted: 1,
                totals: TokenTotals {
                    input: 10,
                    output: 1,
                    ..TokenTotals::default()
                },
                estimated_cost_usd: 1.0,
            }],
            by_host: vec![HostReport {
                host: "local".to_string(),
                records_counted: 1,
                totals: TokenTotals {
                    input: 10,
                    output: 1,
                    ..TokenTotals::default()
                },
                estimated_cost_usd: 1.0,
            }],
            ..ProviderReport::default()
        };

        let remote = ProviderReport {
            records_counted: 2,
            totals: TokenTotals {
                input: 30,
                output: 3,
                ..TokenTotals::default()
            },
            estimated_cost_usd: 3.0,
            priced_totals: TokenTotals {
                input: 30,
                output: 3,
                ..TokenTotals::default()
            },
            priced_records_counted: 2,
            by_model: vec![
                ModelReport {
                    model: "m1".to_string(),
                    records_counted: 1,
                    totals: TokenTotals {
                        input: 20,
                        output: 2,
                        ..TokenTotals::default()
                    },
                    estimated_cost_usd: Some(2.0),
                    pricing: None,
                },
                ModelReport {
                    model: "m2".to_string(),
                    records_counted: 1,
                    totals: TokenTotals {
                        input: 10,
                        output: 1,
                        ..TokenTotals::default()
                    },
                    estimated_cost_usd: Some(1.0),
                    pricing: None,
                },
            ],
            daily: vec![DailyReport {
                day: "2026-03-20".to_string(),
                records_counted: 2,
                totals: TokenTotals {
                    input: 30,
                    output: 3,
                    ..TokenTotals::default()
                },
                estimated_cost_usd: 3.0,
            }],
            by_host: vec![HostReport {
                host: "vm-a".to_string(),
                records_counted: 2,
                totals: TokenTotals {
                    input: 30,
                    output: 3,
                    ..TokenTotals::default()
                },
                estimated_cost_usd: 3.0,
            }],
            ..ProviderReport::default()
        };

        merge_provider_report(&mut local, remote);
        assert_eq!(local.records_counted, 3);
        assert_eq!(local.totals.input, 40);
        assert_eq!(local.by_model.len(), 2);
        assert_eq!(
            local
                .by_model
                .iter()
                .find(|model| model.model == "m1")
                .map(|model| model.totals.input),
            Some(30)
        );
        assert_eq!(local.daily.len(), 1);
        assert_eq!(local.daily[0].totals.input, 40);
        assert_eq!(local.by_host.len(), 2);
    }

    #[test]
    fn collapse_provider_to_host_merges_all_rows_into_target_host() {
        let mut provider = ProviderReport {
            by_host: vec![
                HostReport {
                    host: "local".to_string(),
                    records_counted: 1,
                    totals: TokenTotals {
                        input: 10,
                        ..TokenTotals::default()
                    },
                    estimated_cost_usd: 1.0,
                },
                HostReport {
                    host: "other".to_string(),
                    records_counted: 2,
                    totals: TokenTotals {
                        input: 20,
                        ..TokenTotals::default()
                    },
                    estimated_cost_usd: 2.0,
                },
            ],
            ..ProviderReport::default()
        };

        collapse_provider_to_host(&mut provider, "vm-a");
        assert_eq!(provider.by_host.len(), 1);
        assert_eq!(provider.by_host[0].host, "vm-a");
        assert_eq!(provider.by_host[0].records_counted, 3);
        assert_eq!(provider.by_host[0].totals.input, 30);
        assert!((provider.by_host[0].estimated_cost_usd - 3.0).abs() < 1e-12);
    }

    #[test]
    fn remote_mot_script_contains_required_flags() {
        let script = build_remote_mot_script(&ScanOptions {
            global: false,
            root: PathBuf::from("/tmp/proj"),
            codex_root: PathBuf::from("missing"),
            claude_root: PathBuf::from("missing"),
            droid_root: PathBuf::from("missing"),
            parallel: false,
            window: Some(TimeWindow {
                spec: "7d".to_string(),
                cutoff_unix_ms: 0,
            }),
            ssh_hosts: Vec::new(),
            selected_session: None,
        });

        assert!(script.contains("command -v mot"));
        assert!(script.contains("'mot' '--json' '--root' '/tmp/proj' '--window' '7d'"));
    }

    #[test]
    fn summarize_output_compacts_and_truncates_lines() {
        let input = format!("{}\nsecond line\n", "a".repeat(500));
        let summary = summarize_output(&input).expect("summary");
        assert!(summary.ends_with("..."));
        assert!(summary.len() <= 283);
    }

    #[test]
    fn classify_remote_mot_failure_detects_missing_binary() {
        let warning = classify_remote_mot_failure("vm-a", "mot not found in PATH", "")
            .expect("expected warning");
        assert!(warning.contains("not installed"));
        assert!(warning.contains("skipping host"));
    }

    #[test]
    fn classify_remote_mot_failure_detects_old_json_flag() {
        let stderr = "error: unexpected argument '--json' found";
        let warning = classify_remote_mot_failure("vm-a", stderr, "").expect("expected warning");
        assert!(warning.contains("does not support --json"));
        assert!(warning.contains("skipping host"));
    }

    #[test]
    fn classify_remote_mot_parse_failure_detects_incompatible_schema() {
        let warning =
            classify_remote_mot_parse_failure("vm-a", "{\"scope\":{}}", "").expect("warning");
        assert!(warning.contains("incompatible JSON schema"));
        assert!(warning.contains("skipping host"));
    }
}
