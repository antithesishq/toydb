#!/bin/sh
# Writes this node's toyDB config from NODE_ID and NODE_COUNT, then execs the
# server so it receives signals directly.
set -eu
: "${NODE_ID:?}" "${NODE_COUNT:=5}"
[ -n "${ANTITHESIS_SDK_LOCAL_OUTPUT:-}" ] && mkdir -p "$(dirname "$ANTITHESIS_SDK_LOCAL_OUTPUT")"
cfg=/etc/toydb.yaml
{
  echo "id: ${NODE_ID}"
  echo "peers:"
  i=1
  while [ "$i" -le "$NODE_COUNT" ]; do
    [ "$i" != "$NODE_ID" ] && echo "  '$i': toydb$i:9701"
    i=$((i + 1))
  done
  echo "listen_sql: 0.0.0.0:9601"
  echo "listen_raft: 0.0.0.0:9701"
  echo "log_level: ${LOG_LEVEL:-INFO}"
  echo "data_dir: /data"
  echo "storage_raft: bitcask"
  echo "storage_sql: bitcask"
  echo "fsync: true"
  # Low thresholds, so restarts compact the Bitcask logs.
  echo "compact_threshold: 0.2"
  echo "compact_min_bytes: ${COMPACT_MIN_BYTES:-65536}"
} > "$cfg"
exec /usr/local/bin/toydb -c "$cfg"
