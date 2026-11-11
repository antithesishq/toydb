# The instrumented toyDB server. Build context: the toyDB repository root.
FROM rust:1-trixie AS build
WORKDIR /src
COPY . .
# COVERAGE=1 builds for local LLVM source coverage instead (the local loop).
ARG COVERAGE=0
# Antithesis symbolization needs debug info (source locations) in /symbols.
ENV CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=none
# Antithesis coverage instrumentation flags, from
# `snouty docs show /docs/reference/sdk/rust/instrumentation/`.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    if [ "$COVERAGE" = 1 ]; then \
      rustup component add llvm-tools-preview; \
      export RUSTFLAGS="-Cinstrument-coverage -Cllvm-args=-runtime-counter-relocation"; \
    fi \
 && cargo build --release --features antithesis-instrumentation \
      --bin toydb --bin toysql --bin toydump \
      --config 'build.target = "x86_64-unknown-linux-gnu"' \
      --config 'target.x86_64-unknown-linux-gnu.rustflags = ["-Ccodegen-units=1", "-Cpasses=sancov-module", "-Cllvm-args=-sanitizer-coverage-level=3", "-Cllvm-args=-sanitizer-coverage-trace-pc-guard", "-Cllvm-args=-sanitizer-coverage-pc-table", "-Clink-args=-Wl,--build-id"]' \
 && mkdir -p /out /symbols \
 && cp target/x86_64-unknown-linux-gnu/release/toydb target/x86_64-unknown-linux-gnu/release/toysql \
       target/x86_64-unknown-linux-gnu/release/toydump /out/ \
 && cp /out/* /symbols/

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl jq procps iproute2 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
COPY --from=build /symbols /symbols
COPY antithesis/toydb/toydb/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 755 /usr/local/bin/entrypoint.sh && mkdir -p /data
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
