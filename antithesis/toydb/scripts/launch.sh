#!/bin/sh
# Builds, validates and launches the toyDB workload on Antithesis.
#
#   scripts/launch.sh <max-compute-hours> [extra snouty launch args...]
#
# Check the task's compute budget before each launch (see antithesis/README.md).
set -eu
hours=${1:?usage: launch.sh <max-compute-hours> [args...]}
shift
cd "$(dirname "$0")/../config"
PLUGIN_VERSION=${PLUGIN_VERSION:-0.0.9}
commit=$(git rev-parse --short HEAD)
docker compose build
snouty validate . --timeout 5m
SNOUTY_UNSTABLE_FEATURES=teams snouty launch \
  --launcher basic_test \
  --config . \
  --test-name toydb \
  --description "toyDB 5-node cluster: linearizable kv, bank, transactional inserts ($commit)" \
  --fault-tolerance high_availability \
  --fault-exclusions '^driver$' \
  --max-compute-hours "$hours" \
  --param attrs.workload=toydb \
  --param attrs.workload_commit="$commit" \
  --param attrs.plugin_version="$PLUGIN_VERSION" \
  "$@"
