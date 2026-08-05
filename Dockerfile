FROM rust:1.93-bookworm AS chef

ARG CARGO_FEATURES=""
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

# Install basic packages
RUN apt-get update && apt-get -y upgrade && apt-get install -y libclang-dev pkg-config
RUN cargo install cargo-chef --locked --version  0.1.71

FROM chef AS planner
WORKDIR /app
RUN --mount=target=. \
    cargo chef prepare --recipe-path /recipe.json

FROM chef AS builder
WORKDIR /app
COPY --from=planner /recipe.json recipe.json
COPY .cargo /app/.cargo
RUN --mount=type=secret,id=github_token \
    set -eu; \
    if [ -s /run/secrets/github_token ]; then \
        GITHUB_TOKEN="$(cat /run/secrets/github_token)"; \
        export GIT_CONFIG_COUNT=1; \
        export GIT_CONFIG_KEY_0="url.https://x-access-token:${GITHUB_TOKEN}@github.com/.insteadOf"; \
        export GIT_CONFIG_VALUE_0="https://github.com/"; \
    fi; \
    cargo chef cook --release --recipe-path recipe.json --package rollup-node --bin rollup-node
RUN --mount=target=. \
    --mount=type=secret,id=github_token \
    set -eu; \
    if [ -s /run/secrets/github_token ]; then \
        GITHUB_TOKEN="$(cat /run/secrets/github_token)"; \
        export GIT_CONFIG_COUNT=1; \
        export GIT_CONFIG_KEY_0="url.https://x-access-token:${GITHUB_TOKEN}@github.com/.insteadOf"; \
        export GIT_CONFIG_VALUE_0="https://github.com/"; \
    fi; \
    cargo build ${CARGO_FEATURES:+--features $CARGO_FEATURES} --release --package rollup-node --bin rollup-node --target-dir=/app-target

# Release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl sqlite3 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app-target/release/rollup-node /bin/

EXPOSE 30303 30303/udp 9001 8545 8546 6669

ENTRYPOINT ["rollup-node"]
