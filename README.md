# mot

Count token usage and estimate project cost.

## Usage

```bash
mot # scoped to current directory
mot --global # aggregation across all local sessions
mot --window 7d # only count tokens in last week
mot --session 019d8e7f # only count one local session by id, id prefix, file name, or path
mot --select-session # choose a scoped local session with an arrow-key picker
mot --no-activity-calendar # hide the Ratatui activity calendar
mot --global --since 1m # all sessions, only count tokens in past month
mot --global --ssh-host vm-a --ssh-host vm-b # include remote VM sessions over SSH
```

Sample output:
```
Scanned: codex 1440 files, claude 339 files, droid 85 files in 273 ms
Counted: codex 26 sessions, claude 250 assistant responses, droid 85 sessions

Provider            Input         Output       Thinking     Cache Read    Cache Write      Est. Cost
Codex         143,995,252        785,532        467,276    135,731,584              0        $286.74
Claude             10,123            981              0     19,440,609        343,559         $10.01
Droid                 493         48,106          4,117     13,176,535        747,883         $16.83
Total         144,005,868        834,619        471,393    168,348,728      1,091,442        $313.58

By model:
Provider   Model                                 Input         Output       Thinking     Cache Read    Cache Write      Est. Cost
Codex      gpt-5.3-codex                   137,210,240        677,634        395,340    129,502,336              0        $272.27
Codex      gpt-5.2-codex                     6,785,012        107,898         71,936      6,229,248              0         $14.47
Claude     claude-opus-4-5-20251101              1,396            336              0     17,359,560        108,704          $9.37
Claude     claude-haiku-4-5-20251001             8,723            168              0      2,059,925        213,354        $0.4822
Claude     claude-opus-4-6                           4            477              0         21,124         21,501        $0.1569
Droid      claude-opus-4-5-20251101                493         48,106          4,117     13,176,535        747,883         $16.83
```

When stdout is an interactive terminal, table output also renders a GitHub-style
Ratatui activity calendar for the last 53 weeks. Squares are shaded by daily
token activity. Activity days come from the same timestamped usage records as
the daily rollups:

- Codex: token-count event deltas are assigned to each event timestamp, so a
  session that spans multiple days contributes to each active day.
- Claude: assistant responses are deduplicated by request/message id, then
  assigned to their response timestamp.
- Droid: Factory settings expose aggregate session token usage, so activity is
  assigned to `providerLockTimestamp`.

Table output also includes a randomly selected book-scale comparison. Book word
counts are converted to estimated tokens using the English rule of thumb that
one token is roughly three quarters of a word.

## Install

```bash
# Homebrew
brew install daulet/tap/mot

# From source (latest main)
cargo install --git https://github.com/daulet/mot
```

## Remote VMs

`mot` can aggregate sessions from remote hosts over SSH:

```bash
mot --global --ssh-host user@vm-a --ssh-host user@vm-b
```

Notes:

- Remote scanning reads the same default directories as local mode: `~/.codex/sessions`, `~/.claude/projects`, and `~/.factory/sessions`.
- It shells out to local `ssh` and runs `mot --json` on the remote host, so `mot` must be installed remotely and available in `PATH`.
- If a remote host has an older/incompatible `mot` (or missing `mot`), that host is skipped and surfaced in `Warnings` output.
- For cross-host aggregation, `--global` is usually the right choice because scoped mode still filters by the recorded session `cwd`.
