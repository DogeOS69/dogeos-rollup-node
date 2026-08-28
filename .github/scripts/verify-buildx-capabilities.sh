#!/usr/bin/env bash

set -euo pipefail

docker_command=${DOCKER_COMMAND:-docker}
expected_version=${EXPECTED_BUILDX_VERSION:-0.36.1}

version_output=$("$docker_command" buildx version)
printf '%s\n' "$version_output"

actual_version=$(
    awk '
        NR == 1 {
            version = $2
            sub(/^v/, "", version)
            print version
        }
    ' <<<"$version_output"
)
if [[ "$actual_version" != "$expected_version" ]]; then
    echo "expected Docker Buildx ${expected_version}, got ${actual_version:-unknown}" >&2
    exit 1
fi

create_help=$("$docker_command" buildx imagetools create --help)
if ! grep -Fq -- '--metadata-file' <<<"$create_help"; then
    echo "Docker Buildx ${expected_version} lacks imagetools create --metadata-file" >&2
    exit 1
fi

echo "Docker Buildx ${expected_version} release capabilities verified"
