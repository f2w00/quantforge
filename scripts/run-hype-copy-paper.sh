#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

config_path="${1:-strategies/hype-copy/hype-copy.toml}"
if [[ ! -f "$config_path" ]]; then
    echo "paper configuration not found: $config_path" >&2
    exit 1
fi

run_dir="runs/hype-copy-paper/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$run_dir"

stdout_path="$run_dir/stdout.log"
pid_path="$run_dir/pid"

nohup cargo run -p hype-copy --bin hype-copy-paper -- "$config_path" \
    >"$stdout_path" 2>&1 < /dev/null &
pid=$!
printf '%s\n' "$pid" >"$pid_path"

sleep 1
if ! kill -0 "$pid" 2>/dev/null; then
    echo "hype-copy paper failed to start; inspect $stdout_path" >&2
    exit 1
fi

echo "hype-copy paper started"
echo "PID: $pid"
echo "Run directory: $run_dir"
echo "Console log: $stdout_path"
echo "Strategy ledger: runs/hype-copy-paper/ledger.jsonl"
echo "Performance report: runs/hype-copy-paper/performance.md"
echo "Follow console log: tail -f $stdout_path"
echo "Stop: kill -TERM \$(cat $pid_path)"
