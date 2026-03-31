# mot

Small and fast Rust CLI to aggregate LLM token usage from local Codex and Claude Code metadata.

## What it counts

- Input tokens
- Output tokens
- Thinking/reasoning tokens (when exposed)
- Cache read tokens
- Cache write/create tokens
- Estimated API cost in USD (standard rates, non-priority)

## Default behavior

By default, `mot` scopes usage to your current working directory by matching session `cwd` prefixes.

## Installation

```bash
# Homebrew (macOS)
brew tap daulet/tap
brew install mot

# Or directly from tap
brew install daulet/tap/mot

# From source (latest main)
cargo install --git https://github.com/daulet/mot
```

## Releases

- Tags matching `v*` trigger automated release builds on GitHub Actions.
- Artifacts are published to GitHub Releases for:
  - `x86_64-unknown-linux-gnu`
  - `aarch64-apple-darwin`
  - `x86_64-apple-darwin`
- Homebrew formula updates are pushed to `daulet/homebrew-tap` automatically.
- Required secret for tap publishing: `HOMEBREW_TAP_TOKEN`.

## Usage

```bash
# Scoped to current directory
cargo run

# Global aggregation across host
cargo run -- --global

# JSON output
cargo run -- --json

# Last 7 days only
cargo run -- --window 7d

# Alias for --window
cargo run -- --since 1m

# Runtime version from git tag metadata
cargo run -- -v
```

## CLI flags

- `--global`: count all discovered sessions on host
- `--root <PATH>`: scope to a specific project path (default: current directory)
- `--json`: emit machine-readable JSON
- `--no-parallel`: disable parallel file parsing
- `--window <DURATION>` / `--since <DURATION>`: trailing time window (`1d`, `7d`, `1m`, `1y`, etc.)
- `-v, --version`: print runtime version resolved from git tags (`<tag>` or `<tag>+<commit>`)

## Pricing estimation

`mot` estimates cost per model, then aggregates by provider and total.
Text output includes a separate row per model used.

- OpenAI rates are matched by model id (including snapshots/suffixes) for:
  - GPT-5.4 family (`gpt-5.4`, `gpt-5.4-mini`, `gpt-5.4-nano`, `gpt-5.4-pro`)
  - GPT-5.3 Codex/chat latest (`gpt-5.3-codex`, `gpt-5.3-chat-latest`)
  - GPT-5.2 family (`gpt-5.2`, `gpt-5.2-codex`, `gpt-5.2-pro`)
  - GPT-5.1 family (`gpt-5.1`, `gpt-5.1-codex`, `gpt-5.1-codex-mini`)
  - GPT-5 family (`gpt-5`, `gpt-5-codex`, `gpt-5-mini`, `gpt-5-nano`, `gpt-5-pro`)
  - `codex-mini-latest`
- Anthropic rates are matched across Opus/Sonnet/Haiku families (including dated snapshots).
- Anthropic cache write cost uses 5-minute cache write pricing (default API behavior).
- Thinking tokens are treated as a subset of output tokens (reported separately, not double charged).
- Unknown model ids are reported under `unpriced_*` fields.

## Data sources

- Codex: `~/.codex/sessions/**/*.jsonl`
- Claude Code: `~/.claude/projects/**/*.jsonl`

## Notes on counting strategy

- Codex usage is read from cumulative `token_count` events and converted to deltas per session.
- Codex model attribution is read from `turn_context.payload.model`.
- Claude usage is read from assistant `message.usage`; repeated streamed events are deduped by request/message id.
- Claude model attribution is read from `message.model`.
- If `--window/--since` is set, only records with timestamps inside that window are counted.
