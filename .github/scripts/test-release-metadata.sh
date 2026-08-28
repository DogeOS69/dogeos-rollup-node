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
    local expected_ref_name=$8

    jq -e \
        --argjson tags "$expected_tags" \
        --arg environment "$expected_environment" \
        --argjson platforms "$expected_platforms" \
        --arg patch "$expected_patch" \
        --argjson matrix "$expected_matrix" \
        --arg ref_name "$expected_ref_name" \
        '.tags == $tags
         and .environment == $environment
         and .expected_platforms == $platforms
         and .matrix == $matrix
         and .matrix_count == ($platforms | length)
         and .version_patch == $patch
         and .vcs_ref == "aaaaaaaa"
         and .ref_name == $ref_name' \
        <<<"$actual" >/dev/null || {
        echo "metadata case failed: $name" >&2
        jq . <<<"$actual" >&2
        exit 1
    }
}

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

actual=$(run_case pull_request branch 123/merge refs/pull/123/merge 0.3.0-beta.1 '' false)
assert_case pr "$actual" '["dogeos69/rollup-node:aaaaaaaa"]' ephemeral '["linux/amd64"]' 0-beta "$amd64_matrix" 123/merge

actual=$(run_case push branch main refs/heads/main 0.3.0-beta.1 '' false)
assert_case main "$actual" '["dogeos69/rollup-node:latest-testnet","dogeos69/rollup-node:aaaaaaaa"]' testnet '["linux/amd64"]' 0-beta "$amd64_matrix" main

actual=$(run_case push branch develop refs/heads/develop 0.3.0-beta.1 '' false)
assert_case develop "$actual" '["dogeos69/rollup-node:latest-devnet","dogeos69/rollup-node:aaaaaaaa"]' devnet '["linux/amd64"]' 0-beta "$amd64_matrix" develop

actual=$(run_case workflow_dispatch branch main refs/heads/main 0.3.0-beta.1 canary-amd64 false)
assert_case manual-amd64 "$actual" '["dogeos69/rollup-node:canary-amd64"]' manual '["linux/amd64"]' 0-beta "$amd64_matrix" main

actual=$(run_case workflow_dispatch branch main refs/heads/main 0.3.0-beta.1 canary-multi true)
assert_case manual-multi "$actual" '["dogeos69/rollup-node:canary-multi"]' manual '["linux/amd64","linux/arm64"]' 0-beta "$multi_arch_matrix" main

actual=$(run_case push tag v0.3.0-beta.1 refs/tags/v0.3.0-beta.1 0.3.0-beta.1 '' false)
assert_case prerelease "$actual" '["dogeos69/rollup-node:v0.3.0-beta.1","dogeos69/rollup-node:0.3.0-beta.1","dogeos69/rollup-node:0.3.0-beta.1-aaaaaaaa"]' devnet '["linux/amd64","linux/arm64"]' 0-beta "$multi_arch_matrix" v0.3.0-beta.1

actual=$(run_case push tag v0.3.0-alpha.2 refs/tags/v0.3.0-alpha.2 0.3.0-alpha.2 '' false)
assert_case alpha "$actual" '["dogeos69/rollup-node:v0.3.0-alpha.2","dogeos69/rollup-node:0.3.0-alpha.2","dogeos69/rollup-node:0.3.0-alpha.2-aaaaaaaa"]' devnet '["linux/amd64","linux/arm64"]' 0-alpha "$multi_arch_matrix" v0.3.0-alpha.2

actual=$(run_case push tag v0.3.0-rc.1 refs/tags/v0.3.0-rc.1 0.3.0-rc.1 '' false)
assert_case rc "$actual" '["dogeos69/rollup-node:v0.3.0-rc.1","dogeos69/rollup-node:0.3.0-rc.1","dogeos69/rollup-node:0.3.0-rc.1-aaaaaaaa"]' devnet '["linux/amd64","linux/arm64"]' 0-rc "$multi_arch_matrix" v0.3.0-rc.1

actual=$(run_case push tag v0.3.0 refs/tags/v0.3.0 0.3.0 '' false)
assert_case stable "$actual" '["dogeos69/rollup-node:v0.3.0","dogeos69/rollup-node:0.3.0","dogeos69/rollup-node:0.3.0-aaaaaaaa","dogeos69/rollup-node:0.3","dogeos69/rollup-node:0","dogeos69/rollup-node:latest-testnet"]' testnet '["linux/amd64","linux/arm64"]' 0 "$multi_arch_matrix" v0.3.0

assert_rejected invalid-manual-tag "manual image tag is not a valid Docker tag" \
    run_case workflow_dispatch branch main refs/heads/main 0.3.0-beta.1 'bad tag' true
assert_rejected invalid-arm-boolean "BUILD_ARM64 must be true or false" \
    run_case workflow_dispatch branch main refs/heads/main 0.3.0-beta.1 valid-tag maybe
assert_rejected multiline-version "WORKSPACE_VERSION must be one Docker-tag-compatible semantic version without build metadata" \
    run_case pull_request branch 123/merge refs/pull/123/merge $'0.3.0\nmatrix={"include":[]}' '' false
assert_rejected build-metadata "WORKSPACE_VERSION must be one Docker-tag-compatible semantic version without build metadata" \
    run_case push tag v0.3.0+build.5 refs/tags/v0.3.0+build.5 0.3.0+build.5 '' false

original_sha=$sha
sha=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
assert_rejected uppercase-sha "GITHUB_SHA must be a full lowercase hexadecimal commit ID" \
    run_case push branch main refs/heads/main 0.3.0-beta.1 '' false
sha=$original_sha

echo "release metadata cases passed"
