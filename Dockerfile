FROM rust:1.93-bookworm AS chef

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' ca-certificates)" ge 20250419~deb12u2 \
    && test ! -e /usr/share/ca-certificates/mozilla/emSign_ECC_Root_CA_-_G3.crt \
    && test ! -e /usr/share/ca-certificates/mozilla/AAA_Certificate_Services.crt \
    && rm -rf /var/lib/apt/lists/*

ARG CARGO_FEATURES=""
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

# Install only the packages required to compile the workspace. Upgrading the
# entire base image adds network and package churn without changing the Rust
# build inputs.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libclang-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked --version 0.1.71

FROM chef AS planner
WORKDIR /app
RUN --mount=target=. \
    cargo chef prepare --recipe-path /recipe.json

FROM chef AS builder
WORKDIR /app
COPY --from=planner /recipe.json recipe.json
COPY .cargo /app/.cargo
# The source bind mount in the final build shadows /app, including
# /app/target. Keep cargo-chef's artifacts outside that mount so the final
# cargo build can reuse them instead of compiling the dependency graph twice.
ENV CARGO_TARGET_DIR=/app-target
RUN --mount=type=secret,id=github_token \
    set -eu; \
    if [ -s /run/secrets/github_token ]; then \
        GITHUB_TOKEN="$(cat /run/secrets/github_token)"; \
        export GIT_CONFIG_COUNT=1; \
        export GIT_CONFIG_KEY_0="url.https://x-access-token:${GITHUB_TOKEN}@github.com/.insteadOf"; \
        export GIT_CONFIG_VALUE_0="https://github.com/"; \
    fi; \
    cargo chef cook ${CARGO_FEATURES:+--features $CARGO_FEATURES} --release \
        --recipe-path recipe.json --package rollup-node --bin rollup-node
RUN --mount=target=. \
    --mount=type=secret,id=github_token \
    set -eu; \
    if [ -s /run/secrets/github_token ]; then \
        GITHUB_TOKEN="$(cat /run/secrets/github_token)"; \
        export GIT_CONFIG_COUNT=1; \
        export GIT_CONFIG_KEY_0="url.https://x-access-token:${GITHUB_TOKEN}@github.com/.insteadOf"; \
        export GIT_CONFIG_VALUE_0="https://github.com/"; \
    fi; \
    cargo build --locked ${CARGO_FEATURES:+--features $CARGO_FEATURES} --release --package rollup-node --bin rollup-node

# Release

# Preserve the deployed runtime libraries.
FROM docker.io/dogeos69/rollup-node@sha256:4b73115c4eec04544d2e457992e982b58c3b4d112b844e456e2b0bf668be8cd3

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' ca-certificates)" ge 20250419~deb12u2 \
    && test ! -e /usr/share/ca-certificates/mozilla/emSign_ECC_Root_CA_-_G3.crt \
    && test ! -e /usr/share/ca-certificates/mozilla/AAA_Certificate_Services.crt \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app-target/release/rollup-node /bin/

EXPOSE 30303 30303/udp 9001 8545 8546 6669

ENTRYPOINT ["rollup-node"]
