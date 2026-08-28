#!/usr/bin/env bash

set -euo pipefail

: "${MOCK_FIXTURE_DIR:?MOCK_FIXTURE_DIR is required}"
: "${MOCK_LOG:?MOCK_LOG is required}"

source_amd64=sha256:1111111111111111111111111111111111111111111111111111111111111111
source_arm64=sha256:2222222222222222222222222222222222222222222222222222222222222222
attestation_amd64=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
attestation_arm64=sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd

candidate_raw=$(<"${MOCK_FIXTURE_DIR}/candidate.json")
candidate_checksum=$(printf '%s' "$candidate_raw" | sha256sum)
candidate_digest="sha256:${candidate_checksum%% *}"
metadata_digest=$candidate_digest
tag_digest=$candidate_digest
[[ ${MOCK_METADATA_DIGEST_MISMATCH:-false} == true ]] && metadata_digest=sha256:0000000000000000000000000000000000000000000000000000000000000000
[[ ${MOCK_TAG_DIGEST_MISMATCH:-false} == true ]] && tag_digest=sha256:9999999999999999999999999999999999999999999999999999999999999999

if [[ "$1" != buildx || "$2" != imagetools ]]; then
    echo "unexpected mock docker command: $*" >&2
    exit 1
fi

operation=$3
shift 3

if [[ "$operation" == inspect ]]; then
    raw=false
    if [[ ${1:-} == --raw ]]; then
        raw=true
        shift
    fi
    ref=$1

    if [[ "$raw" == false ]]; then
        printf 'Name: %s\nMediaType: application/vnd.oci.image.index.v1+json\nDigest: %s\n' "$ref" "$tag_digest"
        exit 0
    fi

    case "$ref" in
        *@"$source_amd64") cat "${MOCK_FIXTURE_DIR}/source-amd64.json" ;;
        *@"$source_arm64") cat "${MOCK_FIXTURE_DIR}/source-arm64.json" ;;
        *@"$attestation_amd64") cat "${MOCK_FIXTURE_DIR}/attestation-amd64.json" ;;
        *@"$attestation_arm64") cat "${MOCK_FIXTURE_DIR}/attestation-arm64.json" ;;
        *:*)
            if [[ ${MOCK_RAW_MISMATCH:-false} == true ]]; then
                cat "${MOCK_FIXTURE_DIR}/candidate-invalid.json"
            else
                printf '%s' "$candidate_raw"
            fi
            ;;
        *) echo "unknown mock inspect ref: $ref" >&2; exit 1 ;;
    esac
    exit 0
fi

if [[ "$operation" != create ]]; then
    echo "unexpected mock imagetools operation: $operation" >&2
    exit 1
fi

mode=publish
prefer_index=false
metadata_file=
sources=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)
            mode=dry-run
            shift
            ;;
        --prefer-index=true)
            prefer_index=true
            shift
            ;;
        --metadata-file)
            metadata_file=$2
            shift 2
            ;;
        --tag)
            shift 2
            ;;
        *@sha256:*)
            sources+=("$1")
            shift
            ;;
        *)
            echo "unexpected mock create argument: $1" >&2
            exit 1
            ;;
    esac
done

printf 'create:%s:prefer=%s:%s\n' "$mode" "$prefer_index" "${sources[*]}" >>"$MOCK_LOG"

if [[ "$mode" == dry-run ]]; then
    if [[ ${MOCK_INVALID_CANDIDATE:-false} == true ]]; then
        cat "${MOCK_FIXTURE_DIR}/candidate-invalid.json"
    else
        printf '%s\n' "$candidate_raw"
    fi
    exit 0
fi

if [[ -z "$metadata_file" ]]; then
    echo "mock publish requires --metadata-file" >&2
    exit 1
fi

jq -n --arg digest "$metadata_digest" '{
    "containerimage.descriptor": {
        mediaType: "application/vnd.oci.image.index.v1+json",
        digest: $digest,
        size: 1234
    },
    "image.name": "docker.io/dogeos69/rollup-node"
}' >"$metadata_file"
