#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
fixture_dir="${script_dir}/fixtures"
publisher="${script_dir}/publish-image-index.sh"
image=dogeos69/rollup-node
source_amd64=1111111111111111111111111111111111111111111111111111111111111111
source_arm64=2222222222222222222222222222222222222222222222222222222222222222
expected_sources="${image}@sha256:${source_amd64} ${image}@sha256:${source_arm64}"

test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT
mkdir -p "${test_dir}/bin" "${test_dir}/digests"
ln -s "${fixture_dir}/mock-docker.sh" "${test_dir}/bin/docker"
touch "${test_dir}/digests/amd64-${source_amd64}"
touch "${test_dir}/digests/arm64-${source_arm64}"

run_publisher() {
    PATH="${test_dir}/bin:${PATH}" \
    MOCK_FIXTURE_DIR=$fixture_dir \
    MOCK_LOG="${test_dir}/docker.log" \
    IMAGE_NAME=$image \
    DIGEST_DIR="${test_dir}/digests" \
    EXPECTED_PLATFORMS_JSON='["linux/amd64","linux/arm64"]' \
    DOCKER_TAGS_JSON='["dogeos69/rollup-node:test-one","dogeos69/rollup-node:test-two"]' \
    EXPECTED_DIGEST_COUNT=2 \
        "$@" "$publisher"
}

assert_create_order() {
    local expected
    expected=$(printf 'create:dry-run:prefer=true:%s\ncreate:publish:prefer=true:%s' "$expected_sources" "$expected_sources")
    if [[ $(<"${test_dir}/docker.log") != "$expected" ]]; then
        echo "dry-run and publish did not use the same ordered source list" >&2
        cat "${test_dir}/docker.log" >&2
        exit 1
    fi
}

: >"${test_dir}/docker.log"
run_publisher env >/dev/null
assert_create_order

: >"${test_dir}/docker.log"
if run_publisher env MOCK_INVALID_CANDIDATE=true >"${test_dir}/invalid.out" 2>&1; then
    echo "invalid candidate was published" >&2
    exit 1
fi
grep -F "candidate index is missing the validated linux/arm64 runtime or attestation" "${test_dir}/invalid.out" >/dev/null
if grep -q '^create:publish:' "${test_dir}/docker.log"; then
    echo "publish ran after invalid dry-run candidate" >&2
    exit 1
fi

: >"${test_dir}/docker.log"
if run_publisher env MOCK_METADATA_DIGEST_MISMATCH=true >"${test_dir}/metadata-mismatch.out" 2>&1; then
    echo "metadata digest mismatch was accepted" >&2
    exit 1
fi
grep -F "published metadata does not match validated candidate" "${test_dir}/metadata-mismatch.out" >/dev/null
assert_create_order

: >"${test_dir}/docker.log"
if run_publisher env MOCK_TAG_DIGEST_MISMATCH=true >"${test_dir}/tag-mismatch.out" 2>&1; then
    echo "final tag digest mismatch was accepted" >&2
    exit 1
fi
grep -F "instead of validated candidate" "${test_dir}/tag-mismatch.out" >/dev/null
assert_create_order

: >"${test_dir}/docker.log"
if run_publisher env MOCK_RAW_MISMATCH=true >"${test_dir}/raw-mismatch.out" 2>&1; then
    echo "final raw-byte mismatch was accepted" >&2
    exit 1
fi
grep -F "published index bytes do not match validated candidate" "${test_dir}/raw-mismatch.out" >/dev/null
assert_create_order

echo "image-index publisher fixture cases passed"
