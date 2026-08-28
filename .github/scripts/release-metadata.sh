#!/usr/bin/env bash

set -euo pipefail

: "${EVENT_NAME:?EVENT_NAME is required}"
: "${REF_TYPE:?REF_TYPE is required}"
: "${REF_NAME:?REF_NAME is required}"
: "${GITHUB_REF:?GITHUB_REF is required}"
: "${GITHUB_SHA:?GITHUB_SHA is required}"
: "${WORKSPACE_VERSION:?WORKSPACE_VERSION is required}"
: "${IMAGE_NAME:?IMAGE_NAME is required}"

MANUAL_TAG=${MANUAL_TAG:-}
BUILD_ARM64=${BUILD_ARM64:-false}

if [[ ! "$GITHUB_SHA" =~ ^[0-9a-f]{40}$ ]]; then
    echo "GITHUB_SHA must be a full lowercase hexadecimal commit ID" >&2
    exit 1
fi

if [[ ! "$WORKSPACE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$ ]]; then
    echo "WORKSPACE_VERSION must be one Docker-tag-compatible semantic version without build metadata" >&2
    exit 1
fi

version_major=$(cut -d. -f1 <<<"$WORKSPACE_VERSION")
version_minor=$(cut -d. -f2 <<<"$WORKSPACE_VERSION")
version_patch=$(cut -d. -f3 <<<"$WORKSPACE_VERSION")
vcs_ref=${GITHUB_SHA:0:8}

tags=()
environment=ephemeral
multi_arch=false

if [[ "$EVENT_NAME" == "workflow_dispatch" ]]; then
    if [[ ! "$MANUAL_TAG" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
        echo "manual image tag is not a valid Docker tag" >&2
        exit 1
    fi
    if [[ "$BUILD_ARM64" != "true" && "$BUILD_ARM64" != "false" ]]; then
        echo "BUILD_ARM64 must be true or false" >&2
        exit 1
    fi

    tags+=("${IMAGE_NAME}:${MANUAL_TAG}")
    environment=manual
    [[ "$BUILD_ARM64" == "true" ]] && multi_arch=true
elif [[ "$REF_TYPE" == "tag" ]]; then
    tag_name=$REF_NAME
    version=${tag_name#v}

    if [[ "$version" =~ -(alpha|beta|rc) ]]; then
        environment=devnet
        tags+=(
            "${IMAGE_NAME}:${tag_name}"
            "${IMAGE_NAME}:${version}"
            "${IMAGE_NAME}:${version}-${vcs_ref}"
        )
    else
        environment=testnet
        tags+=(
            "${IMAGE_NAME}:${tag_name}"
            "${IMAGE_NAME}:${WORKSPACE_VERSION}"
            "${IMAGE_NAME}:${WORKSPACE_VERSION}-${vcs_ref}"
            "${IMAGE_NAME}:${version_major}.${version_minor}"
            "${IMAGE_NAME}:${version_major}"
            "${IMAGE_NAME}:latest-testnet"
        )
    fi
    multi_arch=true
elif [[ "$GITHUB_REF" == "refs/heads/main" ]]; then
    environment=testnet
    tags+=("${IMAGE_NAME}:latest-testnet" "${IMAGE_NAME}:${vcs_ref}")
elif [[ "$GITHUB_REF" == "refs/heads/develop" ]]; then
    environment=devnet
    tags+=("${IMAGE_NAME}:latest-devnet" "${IMAGE_NAME}:${vcs_ref}")
else
    tags+=("${IMAGE_NAME}:${vcs_ref}")
fi

tags_json=$(printf '%s\n' "${tags[@]}" | jq -R . | jq -sc .)

if [[ "$multi_arch" == "true" ]]; then
    matrix_json='{"include":[{"arch":"amd64","platform":"linux/amd64","runner":"blacksmith-8vcpu-ubuntu-2404"},{"arch":"arm64","platform":"linux/arm64","runner":"blacksmith-8vcpu-ubuntu-2404-arm"}]}'
    expected_platforms_json='["linux/amd64","linux/arm64"]'
    matrix_count=2
else
    matrix_json='{"include":[{"arch":"amd64","platform":"linux/amd64","runner":"blacksmith-8vcpu-ubuntu-2404"}]}'
    expected_platforms_json='["linux/amd64"]'
    matrix_count=1
fi

jq -cn \
    --argjson matrix "$matrix_json" \
    --argjson tags "$tags_json" \
    --argjson expected_platforms "$expected_platforms_json" \
    --arg environment "$environment" \
    --arg workspace_version "$WORKSPACE_VERSION" \
    --arg version_major "$version_major" \
    --arg version_minor "$version_minor" \
    --arg version_patch "$version_patch" \
    --arg vcs_ref "$vcs_ref" \
    --arg ref_name "$REF_NAME" \
    --argjson matrix_count "$matrix_count" \
    '{
        matrix: $matrix,
        tags: $tags,
        expected_platforms: $expected_platforms,
        matrix_count: $matrix_count,
        environment: $environment,
        workspace_version: $workspace_version,
        version_major: $version_major,
        version_minor: $version_minor,
        version_patch: $version_patch,
        vcs_ref: $vcs_ref,
        ref_name: $ref_name
    }'
