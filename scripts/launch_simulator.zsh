#!/usr/bin/env zsh
set -euo pipefail

REPO_ROOT="${0:A:h:h}"
cd "$REPO_ROOT"

# This starts the deterministic SQLite simulator only. It neither reads nor
# controls Docker, PostgreSQL, Kafka or the distributed workers.
exec "$REPO_ROOT/scripts/run_tui.command"
