#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
verify_script="${script_dir}/verify-buildx-capabilities.sh"
mock_docker="${script_dir}/fixtures/mock-buildx.sh"

assert_rejected() {
    local name=$1
    local expected_error=$2
    shift 2
    local output

    if output=$("$@" 2>&1); then
        echo "rejection case unexpectedly passed: $name" >&2
        exit 1
    fi
    grep -F "$expected_error" <<<"$output" >/dev/null || {
        echo "rejection case emitted the wrong error: $name" >&2
        echo "$output" >&2
        exit 1
    }
}

DOCKER_COMMAND="$mock_docker" "$verify_script" >/dev/null

assert_rejected wrong-version "expected Docker Buildx 0.36.1, got 0.23.0" \
    env DOCKER_COMMAND="$mock_docker" MOCK_BUILDX_VERSION=v0.23.0 "$verify_script"

assert_rejected missing-metadata-file "Docker Buildx 0.36.1 lacks imagetools create --metadata-file" \
    env DOCKER_COMMAND="$mock_docker" MOCK_BUILDX_HAS_METADATA_FILE=false "$verify_script"

echo "Buildx capability cases passed"
