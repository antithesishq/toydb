#!/bin/sh
# The local loop: builds the coverage variant, runs the workload for a while,
# and collects logs, SDK output and coverage under runs/<timestamp>/.
#
#   scripts/local-run.sh [seconds]            # workload only (eventually off)
#   EVENTUALLY_AFTER=90 scripts/local-run.sh  # also run the eventually at 90 s
set -eu
cd "$(dirname "$0")/../config"
secs=${1:-300}
out=../runs/$(date +%Y%m%d-%H%M%S)
mkdir -p "$out"
export COMPOSE_FILE=docker-compose.yaml:docker-compose.coverage.yaml
if [ -n "${EVENTUALLY_AFTER:-}" ]; then
  export TOYDB_EVENTUALLY=on TOYDB_EVENTUALLY_AFTER=$EVENTUALLY_AFTER
fi
docker compose down -v --remove-orphans
docker compose up -d --build
docker compose exec -T driver /opt/antithesis/test/v1/main/first_start_workload
timeout 600 sh -c 'until docker compose exec -T driver grep -qs antithesis_setup /tmp/sdk.jsonl; do sleep 5; done'
echo "running the workload for ${secs}s"
sleep "$secs"
docker compose cp driver:/tmp/sdk.jsonl "$out/sdk.jsonl" || true
for n in 1 2 3 4 5; do
  docker compose cp "toydb$n:/coverage/toydb$n/sdk.jsonl" "$out/sdk-toydb$n.jsonl" 2>/dev/null || true
done
docker compose ps -a > "$out/ps.txt"
docker compose logs --no-color > "$out/compose.log"
# Clean shutdown check: a stopped node should exit 0 or by SIGTERM (143), not be killed (137).
docker compose stop
docker compose ps -a --format '{{.Name}} {{.State}} {{.ExitCode}}' >> "$out/ps.txt"
docker compose down -v --remove-orphans >/dev/null 2>&1
echo "evidence in $out"
