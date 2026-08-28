#!/usr/bin/env bash

set -euo pipefail

case "$*" in
    "buildx version")
        echo "github.com/docker/buildx ${MOCK_BUILDX_VERSION:-v0.36.1} deadbeef"
        ;;
    "buildx imagetools create --help")
        echo "Usage: docker buildx imagetools create [OPTIONS] [SOURCE...]"
        if [[ "${MOCK_BUILDX_HAS_METADATA_FILE:-true}" == "true" ]]; then
            echo "      --metadata-file string   Write create result metadata to a file"
        fi
        ;;
    *)
        echo "unexpected mock Docker command: $*" >&2
        exit 1
        ;;
esac
