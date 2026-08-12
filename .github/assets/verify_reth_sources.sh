#!/usr/bin/env sh
set -eu

metadata_file=$(mktemp)
trap 'rm -f "$metadata_file"' EXIT

cargo metadata --locked --offline --format-version 1 > "$metadata_file"

components_source="git+https://github.com/DogeOS69/dogeos-reth.git?rev=c5198f722a4fcbd47e8c0a10fe8f9835a801c2d2#c5198f722a4fcbd47e8c0a10fe8f9835a801c2d2"
reth_source="git+https://github.com/DogeOS69/reth.git?rev=ae160090003d9b04be0521e9e4760558798cdf40#ae160090003d9b04be0521e9e4760558798cdf40"
revm_source="git+https://github.com/DogeOS69/dogeos-revm.git?branch=dogeos#dcf087684f255131c96c0d20f3291eef9198e990"
official_reth_pattern='^git\+https://github\.com/paradigmxyz/reth(\.git)?[?#]'

jq -e --arg source "$components_source" '
    [.packages[] | select(.name == "dogeos-reth-node") | .source]
    == [$source]
' "$metadata_file" > /dev/null

jq -e --arg source "$reth_source" '
    [.packages[] | select(.name == "reth-node-builder") | .source]
    == [$source]
' "$metadata_file" > /dev/null

jq -e --arg source "$revm_source" '
    [.packages[] | select(.name == "revm-scroll") | .source]
    == [$source]
' "$metadata_file" > /dev/null

# Registry compatibility crates and official transitive revisions are allowed.
# Other Git sources and path overrides are rejected.
jq -e --arg source "$reth_source" --arg official "$official_reth_pattern" '
    [.packages[]
     | select(.name | test("^reth(-|$)"))
     | select(.source == null or (.source | startswith("git+")))
     | .source] as $sources
    | ($sources | length > 0) and
      all($sources[];
          if . == null then false else . == $source or test($official) end)
' "$metadata_file" > /dev/null

# Reject stale component/REVM revisions and the retired behavioral Reth forks.
jq -e --arg components "$components_source" --arg reth "$reth_source" --arg revm "$revm_source" '
    [.packages[].source
     | select(. != null)
     | select(
         (contains("github.com/DogeOS69/dogeos-reth.git") and . != $components) or
         (contains("github.com/DogeOS69/reth.git") and . != $reth) or
         (contains("github.com/DogeOS69/dogeos-revm") and . != $revm) or
         contains("github.com/DogeOS69/dogeos-reth2.git") or
         contains("github.com/DogeOS69/scroll-reth.git") or
         contains("github.com/scroll-tech/reth.git")
       )]
    | length == 0
' "$metadata_file" > /dev/null

echo "dependency graph verified: qualified DogeOS component, Reth, and REVM revisions"
