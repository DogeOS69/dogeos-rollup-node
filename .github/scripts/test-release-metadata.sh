#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
metadata_script="${script_dir}/release-metadata.sh"
image=dogeos69/rollup-node
sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
amd64_matrix='{"include":[{"arch":"amd64","platform":"linux/amd64","runner":"blacksmith-8vcpu-ubuntu-2404"}]}'
multi_arch_matrix='{"include":[{"arch":"amd64","platform":"linux/amd64","runner":"blacksmith-8vcpu-ubuntu-2404"},{"arch":"arm64","platform":"linux/arm64","runner":"blacksmith-8vcpu-ubuntu-2404-arm"}]}'

run_case() {
    local event_name=$1
    local ref_type=$2
    local ref_name=$3
    local github_ref=$4
    local workspace_version=$5
    local manual_tag=$6
    local build_arm64=$7

    EVENT_NAME=$event_name \
    REF_TYPE=$ref_type \
    REF_NAME=$ref_name \
    GITHUB_REF=$github_ref \
    GITHUB_SHA=$sha \
    WORKSPACE_VERSION=$workspace_version \
    IMAGE_NAME=$image \
    MANUAL_TAG=$manual_tag \
    BUILD_ARM64=$build_arm64 \
        "$metadata_script"
}

assert_case() {
    local name=$1
    local actual=$2
    local expected_tags=$3
    local expected_environment=$4
    local expected_platforms=$5
    local expected_patch=$6
    local expected_matrix=$7

    jq -e \
        --argjson tags "$expected_tags" \
        --arg environment "$expected_environment" \
        --argjson platforms "$expected_platforms" \
        --arg patch "$expected_patch" \
        --argjson matrix "$expected_matrix" \
        '.tags == $tags
         and .environment == $environment
         and .expected_platforms == $platforms
         and .matrix == $matrix
         and .matrix_count == ($platforms | length)
         and .version_patch == $patch
         and .vcs_ref == "aaaaaaaa"' \
        <<<"$actual" >/dev/null || {
        echo "metadata case failed: $name" >&2
        jq . <<<"$actual" >&2
        exit 1
    }
}

actual=$(run_case pull_request branch 123/merge refs/pull/123/merge 0.3.0-beta.1 '' false)
assert_case pr "$actual" '["dogeos69/rollup-node:aaaaaaaa"]' ephemeral '["linux/amd64"]' 0-beta "$amd64_matrix"

actual=$(run_case push branch main refs/heads/main 0.3.0-beta.1 '' false)
assert_case main "$actual" '["dogeos69/rollup-node:latest-testnet","dogeos69/rollup-node:aaaaaaaa"]' testnet '["linux/amd64"]' 0-beta "$amd64_matrix"

actual=$(run_case push branch develop refs/heads/develop 0.3.0-beta.1 '' false)
assert_case develop "$actual" '["dogeos69/rollup-node:latest-devnet","dogeos69/rollup-node:aaaaaaaa"]' devnet '["linux/amd64"]' 0-beta "$amd64_matrix"

actual=$(run_case workflow_dispatch branch main refs/heads/main 0.3.0-beta.1 canary-amd64 false)
assert_case manual-amd64 "$actual" '["dogeos69/rollup-node:canary-amd64"]' manual '["linux/amd64"]' 0-beta "$amd64_matrix"

actual=$(run_case workflow_dispatch branch main refs/heads/main 0.3.0-beta.1 canary-multi true)
assert_case manual-multi "$actual" '["dogeos69/rollup-node:canary-multi"]' manual '["linux/amd64","linux/arm64"]' 0-beta "$multi_arch_matrix"

actual=$(run_case push tag v0.3.0-beta.1 refs/tags/v0.3.0-beta.1 0.3.0-beta.1 '' false)
assert_case prerelease "$actual" '["dogeos69/rollup-node:v0.3.0-beta.1","dogeos69/rollup-node:0.3.0-beta.1","dogeos69/rollup-node:0.3.0-beta.1-aaaaaaaa"]' devnet '["linux/amd64","linux/arm64"]' 0-beta "$multi_arch_matrix"

actual=$(run_case push tag v0.3.0 refs/tags/v0.3.0 0.3.0 '' false)
assert_case stable "$actual" '["dogeos69/rollup-node:v0.3.0","dogeos69/rollup-node:0.3.0","dogeos69/rollup-node:0.3.0-aaaaaaaa","dogeos69/rollup-node:0.3","dogeos69/rollup-node:0","dogeos69/rollup-node:latest-testnet"]' testnet '["linux/amd64","linux/arm64"]' 0 "$multi_arch_matrix"

if run_case workflow_dispatch branch main refs/heads/main 0.3.0-beta.1 'bad tag' true >/dev/null 2>&1; then
    echo "invalid manual tag was accepted" >&2
    exit 1
fi

if run_case workflow_dispatch branch main refs/heads/main 0.3.0-beta.1 valid-tag maybe >/dev/null 2>&1; then
    echo "invalid BUILD_ARM64 value was accepted" >&2
    exit 1
fi

if run_case pull_request branch 123/merge refs/pull/123/merge $'0.3.0\nmatrix={"include":[]}' '' false >/dev/null 2>&1; then
    echo "multiline workspace version was accepted" >&2
    exit 1
fi

echo "release metadata cases passed"
