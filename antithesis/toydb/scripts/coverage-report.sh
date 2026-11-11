#!/bin/sh
# Reads the local loop's LLVM coverage of the toyDB nodes (after local-run.sh)
# and prints per-file coverage plus the toyDB functions that never ran.
set -eu
cd "$(dirname "$0")/../../.."
docker build -q --target build --build-arg COVERAGE=1 \
  -f antithesis/toydb/toydb/Dockerfile -t toydb-antithesis:coverage-build . >/dev/null
docker run --rm -v config_coverage:/coverage toydb-antithesis:coverage-build sh -c '
  set -e; (apt-get update -qq && apt-get install -y -qq jq) >/dev/null 2>&1
  bin=$(rustc --print sysroot)/lib/rustlib/x86_64-unknown-linux-gnu/bin
  $bin/llvm-profdata merge -sparse /coverage/*/*.profraw -o /tmp/all.profdata
  $bin/llvm-cov report /out/toydb -instr-profile=/tmp/all.profdata --ignore-filename-regex="(cargo/registry|rustc/|/tests?/)" | sed "s|/src/||"
  echo; echo "== functions never run (src/, excluding tests)"
  $bin/llvm-cov export /out/toydb -instr-profile=/tmp/all.profdata --format=text --ignore-filename-regex="(cargo/registry|rustc/)" \
    | jq -r ".data[0].functions[] | select(.count == 0) | select(.filenames[0] | test(\"/src/src/\")) | .name" \
    | while read -r f; do echo "$f"; done > /tmp/unrun
  $bin/llvm-cxxfilt < /tmp/unrun 2>/dev/null | sed -E "s/::h[0-9a-f]{16}$//" | grep -v "::tests::" | sort -u
'
