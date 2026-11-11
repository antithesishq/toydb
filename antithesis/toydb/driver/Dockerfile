# Build context: the toyDB repository root (the driver depends on the toydb crate).
FROM rust:1-trixie AS build
WORKDIR /src
COPY . .
WORKDIR /src/antithesis/toydb/driver
# .cargo/config.toml here sets the Antithesis instrumentation flags and target.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/antithesis/toydb/driver/target \
    cargo build --release \
 && mkdir -p /out /symbols \
 && cp target/x86_64-unknown-linux-gnu/release/driver /out/ \
 && cp /out/driver /symbols/

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl jq procps iproute2 ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/driver /usr/local/bin/
COPY --from=build /symbols /symbols
COPY antithesis/toydb/driver/first_start_workload /opt/antithesis/test/v1/main/
RUN chmod 755 /opt/antithesis/test/v1/main/first_start_workload
CMD ["/usr/local/bin/driver"]
