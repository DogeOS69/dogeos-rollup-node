#!/usr/bin/env sh
set -eu

metadata_file=$(mktemp)
trap 'rm -f "$metadata_file"' EXIT

cargo metadata --locked --offline --format-version 1 > "$metadata_file"

components_source="git+https://github.com/DogeOS69/dogeos-reth.git?rev=cd92aada272e0c083d9a68794a46f94665d53a29#cd92aada272e0c083d9a68794a46f94665d53a29"
reth_source="git+https://github.com/DogeOS69/reth.git?rev=ae160090003d9b04be0521e9e4760558798cdf40#ae160090003d9b04be0521e9e4760558798cdf40"
official_reth_pattern='^git\+https://github\.com/paradigmxyz/reth(\.git)?[?#]'

jq -e --arg source "$components_source" '
    [.packages[] | select(.name == "dogeos-reth-node") | .source]
    == [$source]
' "$metadata_file" > /dev/null

jq -e --arg source "$reth_source" '
    [.packages[] | select(.name == "reth-node-builder") | .source]
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

# Reject stale component revisions and the retired behavioral Reth forks.
jq -e --arg components "$components_source" --arg reth "$reth_source" '
    [.packages[].source
     | select(. != null)
     | select(
         (contains("github.com/DogeOS69/dogeos-reth.git") and . != $components) or
         (contains("github.com/DogeOS69/reth.git") and . != $reth) or
         contains("github.com/DogeOS69/dogeos-reth2.git") or
         contains("github.com/DogeOS69/scroll-reth.git") or
         contains("github.com/scroll-tech/reth.git")
       )]
    | length == 0
' "$metadata_file" > /dev/null

echo "dependency graph verified: qualified DogeOS components and Reth revisions"
