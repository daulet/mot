use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mot::{ScanOptions, collect_usage};
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
struct Fixture {
    scoped_root: PathBuf,
    codex_root: PathBuf,
    claude_root: PathBuf,
}

static FIXTURE: OnceLock<Fixture> = OnceLock::new();

fn fixture() -> &'static Fixture {
    FIXTURE.get_or_init(build_fixture)
}

fn build_fixture() -> Fixture {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();

    let base = std::env::temp_dir().join(format!("mot-bench-{unique}"));
    let scoped_root = base.join("workspace/scoped-project");
    let outside_root = base.join("workspace/outside-project");
    let codex_root = base.join(".codex/sessions");
    let claude_root = base.join(".claude/projects");

    fs::create_dir_all(&scoped_root).expect("create scoped root");
    fs::create_dir_all(&outside_root).expect("create outside root");

    let codex_bucket = codex_root.join("2026/03/30");
    fs::create_dir_all(&codex_bucket).expect("create codex bucket");

    let mut codex_total: u64 = 0;
    let mut claude_total: u64 = 0;

    for i in 0..600u64 {
        let cwd = if i % 2 == 0 {
            scoped_root.as_path()
        } else {
            outside_root.as_path()
        };

        let input_1 = 100 + i;
        let input_2 = 200 + i;
        codex_total += input_2;

        let file = codex_bucket.join(format!("rollout-{i}.jsonl"));
        let contents = format!(
            concat!(
                "{{\"type\":\"session_meta\",\"payload\":{{\"cwd\":\"{}\"}}}}\n",
                "{{\"type\":\"turn_context\",\"payload\":{{\"cwd\":\"{}\",\"model\":\"gpt-5.2-codex\"}}}}\n",
                "{{\"type\":\"event_msg\",\"payload\":{{\"type\":\"token_count\",\"info\":null}}}}\n",
                "{{\"type\":\"event_msg\",\"payload\":{{\"type\":\"token_count\",\"info\":{{\"total_token_usage\":{{\"input_tokens\":{},\"cached_input_tokens\":40,\"output_tokens\":20,\"reasoning_output_tokens\":5}}}}}}}}\n",
                "{{\"type\":\"event_msg\",\"payload\":{{\"type\":\"token_count\",\"info\":{{\"total_token_usage\":{{\"input_tokens\":{},\"cached_input_tokens\":60,\"output_tokens\":35,\"reasoning_output_tokens\":8}}}}}}}}\n"
            ),
            cwd.display(),
            cwd.display(),
            input_1,
            input_2,
        );
        fs::write(file, contents).expect("write codex fixture file");
    }

    for i in 0..600u64 {
        let cwd = if i % 2 == 0 {
            scoped_root.as_path()
        } else {
            outside_root.as_path()
        };

        let file_dir = claude_root.join(format!("project-{i}"));
        fs::create_dir_all(&file_dir).expect("create claude fixture project dir");

        let input = 40 + i;
        claude_total += input + 3;

        let req_main = format!("req-{i}-a");
        let req_secondary = format!("req-{i}-b");
        let msg_main = format!("msg-{i}-a");
        let msg_secondary = format!("msg-{i}-b");

        let file = file_dir.join(format!("session-{i}.jsonl"));
        let contents = format!(
            concat!(
                "{{\"type\":\"assistant\",\"cwd\":\"{}\",\"requestId\":\"{}\",\"message\":{{\"model\":\"claude-sonnet-4-5-20250929\",\"id\":\"{}\",\"usage\":{{\"input_tokens\":{},\"output_tokens\":11,\"cache_read_input_tokens\":9,\"cache_creation_input_tokens\":4}}}}}}\n",
                "{{\"type\":\"assistant\",\"cwd\":\"{}\",\"requestId\":\"{}\",\"message\":{{\"model\":\"claude-sonnet-4-5-20250929\",\"id\":\"{}\",\"usage\":{{\"input_tokens\":{},\"output_tokens\":11,\"cache_read_input_tokens\":9,\"cache_creation_input_tokens\":4}}}}}}\n",
                "{{\"type\":\"assistant\",\"cwd\":\"{}\",\"requestId\":\"{}\",\"message\":{{\"model\":\"claude-sonnet-4-5-20250929\",\"id\":\"{}\",\"usage\":{{\"input_tokens\":3,\"output_tokens\":2,\"cache_read_input_tokens\":1,\"cache_creation_input_tokens\":1}}}}}}\n"
            ),
            cwd.display(),
            req_main,
            msg_main,
            input,
            cwd.display(),
            req_main,
            msg_main,
            input,
            cwd.display(),
            req_secondary,
            msg_secondary,
        );
        fs::write(file, contents).expect("write claude fixture file");
    }

    assert!(codex_total > 0);
    assert!(claude_total > 0);

    Fixture {
        scoped_root,
        codex_root,
        claude_root,
    }
}

fn bench_collect_usage(c: &mut Criterion) {
    let fixture = fixture();

    let mut group = c.benchmark_group("collect_usage");

    for parallel in [true, false] {
        let scoped = ScanOptions {
            global: false,
            root: fixture.scoped_root.clone(),
            codex_root: fixture.codex_root.clone(),
            claude_root: fixture.claude_root.clone(),
            parallel,
            window: None,
        };

        let global = ScanOptions {
            global: true,
            root: fixture.scoped_root.clone(),
            codex_root: fixture.codex_root.clone(),
            claude_root: fixture.claude_root.clone(),
            parallel,
            window: None,
        };

        group.bench_with_input(
            BenchmarkId::new("scoped", if parallel { "parallel" } else { "single" }),
            &scoped,
            |b, opts| {
                b.iter(|| {
                    let report = collect_usage(black_box(opts));
                    black_box(report.total.input);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("global", if parallel { "parallel" } else { "single" }),
            &global,
            |b, opts| {
                b.iter(|| {
                    let report = collect_usage(black_box(opts));
                    black_box(report.total.input);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_collect_usage);
criterion_main!(benches);
