#!/usr/bin/env zsh
set -euo pipefail

REPO_ROOT="${0:A:h:h}"
cd "$REPO_ROOT"

if ! docker info >/dev/null 2>&1; then
  print -u2 "Docker Desktop is not running. Start it, then run this script again."
  exit 1
fi

required_services=(postgres kafka api outbox-worker projection-worker settlement-process-manager redrive-outbox-worker dead-letter-outbox-worker)
running_services=("${(@f)$(docker compose ps --status running --services)}")
missing_services=()

for service in "${required_services[@]}"; do
  if (( ${running_services[(Ie)$service]} == 0 )); then
    missing_services+=("$service")
  fi
done

if (( ${#missing_services[@]} > 0 )); then
  print "Starting Thyphon distributed services: ${missing_services[*]}"
  docker compose up -d --wait
else
  print "Thyphon distributed services are already running."
fi

print "\nDistributed runtime:"
docker compose ps
print "\nThe local simulator is separate: zsh scripts/launch_simulator.zsh"
