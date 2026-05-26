# Multi-stage build for the boom-gw binaries + bundled SPA. Three
# stages:
#   1. `web-builder`  — npm install + `npm run build` → web/dist
#   2. `builder`      — cargo build --release for every binary
#   3. `runtime`      — debian-slim with libs the rdkafka native stack
#                       needs (libsasl2 + libkrb5 + libssl), the four
#                       boom-gw binaries, and the static SPA bundle.
#
# gw-api's `--static-dir` (or `BOOM_GW_STATIC_DIR`) points at
# /app/web/dist so the API and the SPA are served same-origin. This
# matters for the session cookie + the OIDC redirect URI — see
# src/oidc.rs and src/session.rs.

FROM node:22-slim AS web-builder

WORKDIR /web
# Copy package metadata first so `npm ci` caches when only source
# changes. The repo ships package-lock.json under web/.
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

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

# Bundled SPA. gw-api serves it as the catch-all behind /api/* when
# BOOM_GW_STATIC_DIR=/app/web/dist (set in the k8s manifest).
COPY --from=web-builder /web/dist /app/web/dist

WORKDIR /app
