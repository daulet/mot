# mot

Count token usage and estimate project cost.

## Usage

```bash
mot # scoped to current directory
mot --global # aggregation across all local sessions
mot --window 7d # only count tokens in last week
mot --global --since 1m # all sessions, only count tokens in past month
```

Sample output:
```
Scanned: codex 1440 files, claude 339 files in 273 ms
Counted: codex 26 sessions, claude 250 assistant responses

Provider            Input         Output       Thinking     Cache Read    Cache Write      Est. Cost
Codex         143,995,252        785,532        467,276    135,731,584              0        $286.74
Claude             10,123            981              0     19,440,609        343,559         $10.01
Total         144,005,375        786,513        467,276    155,172,193        343,559        $296.76

By model:
Provider   Model                                 Input         Output       Thinking     Cache Read    Cache Write      Est. Cost
Codex      gpt-5.3-codex                   137,210,240        677,634        395,340    129,502,336              0        $272.27
Codex      gpt-5.2-codex                     6,785,012        107,898         71,936      6,229,248              0         $14.47
Claude     claude-opus-4-5-20251101              1,396            336              0     17,359,560        108,704          $9.37
Claude     claude-haiku-4-5-20251001             8,723            168              0      2,059,925        213,354        $0.4822
Claude     claude-opus-4-6                           4            477              0         21,124         21,501        $0.1569
```

## Install

```bash
# Homebrew (macOS)
brew install daulet/tap/mot

# From source (latest main)
cargo install --git https://github.com/daulet/mot
```
