use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;
use walkdir::WalkDir;

const UNKNOWN_MODEL: &str = "<unknown>";

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

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct ModelPricing {
    pub input_per_mtok_usd: f64,
    pub output_per_mtok_usd: f64,
    pub cache_read_per_mtok_usd: f64,
    pub cache_write_per_mtok_usd: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ModelReport {
    pub model: String,
    pub records_counted: usize,
    pub totals: TokenTotals,
    pub estimated_cost_usd: Option<f64>,
    pub pricing: Option<ModelPricing>,
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

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProviderReport {
    pub files_scanned: usize,
    pub records_counted: usize,
    pub parse_errors: usize,
    pub totals: TokenTotals,
    pub estimated_cost_usd: f64,
    pub priced_totals: TokenTotals,
    pub unpriced_totals: TokenTotals,
    pub priced_records_counted: usize,
    pub unpriced_records_counted: usize,
    pub unpriced_models: Vec<String>,
    pub by_model: Vec<ModelReport>,
    #[serde(skip)]
    model_usage: HashMap<String, ModelUsage>,
}

impl std::ops::AddAssign for ProviderReport {
    fn add_assign(&mut self, rhs: Self) {
        self.files_scanned += rhs.files_scanned;
        self.records_counted += rhs.records_counted;
        self.parse_errors += rhs.parse_errors;
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
    pub estimated_cost_usd: f64,
    pub priced_totals: TokenTotals,
    pub unpriced_totals: TokenTotals,
    pub unpriced_models: Vec<String>,
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

    let (mut codex, mut claude) = if options.parallel {
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

    finalize_provider_pricing(&mut codex);
    finalize_provider_pricing(&mut claude);

    let total = codex.totals + claude.totals;
    let priced_totals = codex.priced_totals + claude.priced_totals;
    let unpriced_totals = codex.unpriced_totals + claude.unpriced_totals;
    let estimated_cost_usd = codex.estimated_cost_usd + claude.estimated_cost_usd;
    let mut unpriced_models = codex.unpriced_models.clone();
    for model in &claude.unpriced_models {
        if !unpriced_models.contains(model) {
            unpriced_models.push(model.clone());
        }
    }
    unpriced_models.sort();

    UsageReport {
        scope: ScopeReport {
            mode: if options.global { "global" } else { "scoped" },
            root: if options.global {
                None
            } else {
                Some(options.root.clone())
            },
        },
        total,
        codex,
        claude,
        estimated_cost_usd,
        priced_totals,
        unpriced_totals,
        unpriced_models,
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
    push_row(&mut out, "Total", &report.total, report.estimated_cost_usd);

    if !report.codex.by_model.is_empty() || !report.claude.by_model.is_empty() {
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
    }

    out.push_str("\nPricing mode: standard API (non-priority)\n");
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

    if report.codex.parse_errors > 0 || report.claude.parse_errors > 0 {
        out.push_str(&format!(
            "\nParse warnings: codex {}, claude {}\n",
            report.codex.parse_errors, report.claude.parse_errors
        ));
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
    report.estimated_cost_usd = 0.0;
    report.priced_totals = TokenTotals::default();
    report.unpriced_totals = TokenTotals::default();
    report.priced_records_counted = 0;
    report.unpriced_records_counted = 0;
    report.unpriced_models.clear();
    report.by_model.clear();

    let mut by_model = Vec::with_capacity(report.model_usage.len());

    for (model, usage) in &report.model_usage {
        let pricing = lookup_model_pricing(model);
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

fn estimate_cost_usd(totals: TokenTotals, rates: ModelPricing) -> f64 {
    usd_from_tokens(totals.input, rates.input_per_mtok_usd)
        + usd_from_tokens(totals.output, rates.output_per_mtok_usd)
        + usd_from_tokens(totals.cache_read, rates.cache_read_per_mtok_usd)
        + usd_from_tokens(totals.cache_write, rates.cache_write_per_mtok_usd)
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
    let mut current_model: Option<String> = None;
    let mut previous_totals = TokenTotals::default();
    let mut session_totals = TokenTotals::default();
    let mut by_model: HashMap<String, ModelUsage> = HashMap::new();
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

                                session_totals += delta;
                                let model = current_model
                                    .as_deref()
                                    .unwrap_or(UNKNOWN_MODEL)
                                    .to_string();
                                by_model
                                    .entry(model)
                                    .or_insert_with(|| ModelUsage {
                                        records_counted: 1,
                                        totals: TokenTotals::default(),
                                    })
                                    .totals += delta;
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
    report.totals = session_totals;
    report.model_usage = by_model;
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

                        if !options.global {
                            let Some(cwd) = parsed.cwd.as_deref() else {
                                continue;
                            };
                            if !path_in_scope(Path::new(cwd), &options.root) {
                                continue;
                            }
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

                        let model = model.unwrap_or_else(|| UNKNOWN_MODEL.to_string());

                        let key = parsed
                            .request_id
                            .or(message_id)
                            .or(parsed.uuid)
                            .unwrap_or_else(|| format!("{}:{}", path.display(), line_no));

                        by_request
                            .entry(key)
                            .and_modify(|existing| {
                                existing.totals.max_assign(&totals);
                                if existing.model == UNKNOWN_MODEL && model != UNKNOWN_MODEL {
                                    existing.model = model.clone();
                                }
                            })
                            .or_insert(ClaudeRequestUsage { model, totals });
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
    let mut totals = TokenTotals::default();

    for usage in by_request.into_values() {
        totals += usage.totals;
        by_model
            .entry(usage.model)
            .or_default()
            .add_record(usage.totals);
    }

    report.totals = totals;
    report.model_usage = by_model;
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
    model: Option<String>,
    #[serde(default)]
    usage: Option<ClaudeUsage>,
}

#[derive(Debug)]
struct ClaudeRequestUsage {
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
            parallel: false,
        };

        let report = collect_usage(&options);

        let expected_codex = (150.0 * 1.75 + 50.0 * 0.175 + 20.0 * 14.0) / 1_000_000.0;
        let expected_claude = (100.0 * 3.0 + 50.0 * 0.30 + 40.0 * 3.75 + 20.0 * 15.0) / 1_000_000.0;
        let expected_total = expected_codex + expected_claude;

        assert!((report.codex.estimated_cost_usd - expected_codex).abs() < 1e-12);
        assert!((report.claude.estimated_cost_usd - expected_claude).abs() < 1e-12);
        assert!((report.estimated_cost_usd - expected_total).abs() < 1e-12);
        assert!(report.unpriced_totals.is_zero());

        let rendered = render_report(&report);
        assert!(rendered.contains("By model:"));
        assert!(rendered.contains("gpt-5.2-codex"));
        assert!(rendered.contains("claude-sonnet-4-5-20250929"));
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
            parallel: false,
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
            parallel: false,
        };

        let report = collect_usage(&options);
        assert_eq!(report.codex.records_counted, 0);
        assert_eq!(report.claude.records_counted, 1);
        assert_eq!(report.total.input, 7);
    }
}
