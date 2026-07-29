#!/usr/bin/env zsh
set -euo pipefail

REPO_ROOT="${0:A:h:h}"
cd "$REPO_ROOT"
bazel run //apps:tui -- --ticks 80 --seed 18374 --interval 0.20
print "\nPress Enter to close this Thyphon TUI terminal."
read
