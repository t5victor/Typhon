#!/usr/bin/env zsh
set -euo pipefail

REPO_ROOT="${0:A:h:h}"
cd "$REPO_ROOT"

if ! docker info >/dev/null 2>&1; then
  print -u2 "Docker Desktop is not running. Start it, then run this script again."
  exit 1
fi

required_services=(postgres kafka api outbox-worker projection-worker)
running_services=("${(@f)$(docker compose ps --status running --services)}")
missing_services=()

for service in "${required_services[@]}"; do
  if (( ${running_services[(Ie)$service]} == 0 )); then
    missing_services+=("$service")
  fi
done

if (( ${#missing_services[@]} > 0 )); then
  print "Starting Thyphon services: ${missing_services[*]}"
  docker compose up -d --wait
else
  print "Thyphon services are already running."
fi

print "\nLive services:"
docker compose ps
print "\nOpening the Thyphon ASCII operations console in a new terminal..."

if [[ "$OSTYPE" == darwin* ]]; then
  open -a Terminal "$REPO_ROOT/scripts/run_tui.command"
else
  print -u2 "Open a terminal and run: cd ${(q)REPO_ROOT} && ./scripts/run_tui.command"
fi
