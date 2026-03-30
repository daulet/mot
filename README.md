# mot

Small and fast Rust CLI to aggregate LLM token usage from local Codex and Claude Code metadata.

## What it counts

- Input tokens
- Output tokens
- Thinking/reasoning tokens (when exposed)
- Cache read tokens
- Cache write/create tokens

## Default behavior

By default, `mot` scopes usage to your current working directory by matching session `cwd` prefixes.

## Usage

```bash
# Scoped to current directory
cargo run

# Global aggregation across host
cargo run -- --global

# JSON output
cargo run -- --json
```

## CLI flags

- `--global`: count all discovered sessions on host
- `--root <PATH>`: scope to a specific project path (default: current directory)
- `--json`: emit machine-readable JSON
- `--no-parallel`: disable parallel file parsing

## Data sources

- Codex: `~/.codex/sessions/**/*.jsonl`
- Claude Code: `~/.claude/projects/**/*.jsonl`

## Notes on counting strategy

- Codex usage is read from `event_msg` records where `payload.type == token_count`; per session, max cumulative totals are used.
- Claude usage is read from assistant `message.usage`; repeated streamed events are deduped by request/message id.
