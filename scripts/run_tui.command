#!/usr/bin/env zsh
set -euo pipefail

REPO_ROOT="${0:A:h:h}"
cd "$REPO_ROOT"

# Build quietly, then run the generated executable directly. `bazel run`
# writes its own progress before curses can take control of the alternate
# screen, which made the launcher look like a build log instead of a console.
BUILD_LOG="$(mktemp -t thyphon-bazel-build.XXXXXX)"
trap 'rm -f "$BUILD_LOG"' EXIT
if ! bazel build //apps:tui >"$BUILD_LOG" 2>&1; then
  clear
  print -u2 "Thyphon could not be built:"
  cat "$BUILD_LOG"
  print "\nPress Enter to close this Thyphon terminal."
  read
  exit 1
fi

clear
if "$REPO_ROOT/bazel-bin/apps/tui" --seed 18374 --interval 0.20; then
  status=0
else
  status=$?
fi
clear
if (( status != 0 )); then
  print -u2 "Thyphon TUI stopped with exit code $status."
else
  print "Thyphon TUI closed by operator."
fi
print "Press Enter to close this Thyphon terminal."
read
