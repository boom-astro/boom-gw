# Multi-stage build for the three boom-gw binaries
# (gw-clusterer, gw-consumer, gw-api). Mirrors BOOM proper's layout:
# Rust slim builder image, debian-slim runtime, system libs only for
# the rdkafka native-dep stack (libsasl2 + libkrb5).
FROM rust:1.90-slim-bookworm AS builder

WORKDIR /build
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-dev \
        libkrb5-dev \
        libssl-dev \
        pkg-config \
        cmake \
        clang \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests

RUN cargo build --release --locked --bins

FROM debian:bookworm-slim AS runtime

RUN apt-get update -qq && apt-get install -y --no-install-recommends \
        libsasl2-2 \
        libgssapi-krb5-2 \
        libssl3 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# cargo names binaries after the source-file stem, which uses
# underscores; rename them to hyphenated paths so the k8s manifests
# and ENTRYPOINT call them by the same name users see in --help.
COPY --from=builder /build/target/release/gw_clusterer /app/gw-clusterer
COPY --from=builder /build/target/release/gw_consumer  /app/gw-consumer
COPY --from=builder /build/target/release/gw_api       /app/gw-api
COPY --from=builder /build/target/release/gw_dump      /app/gw-dump

WORKDIR /app
