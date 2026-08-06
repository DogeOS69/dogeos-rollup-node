#!/usr/bin/env sh
set -eu

metadata_file=$(mktemp)
trap 'rm -f "$metadata_file"' EXIT

cargo metadata --locked --offline --format-version 1 > "$metadata_file"

reth_source="git+https://github.com/paradigmxyz/reth.git?rev=eb4c15e5e36d8776d46629beae4c0a69af7ab04f#eb4c15e5e36d8776d46629beae4c0a69af7ab04f"
official_reth_pattern='^git\+https://github\.com/paradigmxyz/reth(\.git)?[?#]'

jq -e --arg source "$reth_source" '
    [.packages[] | select(.name == "reth-node-builder") | .source]
    == [$source]
' "$metadata_file" > /dev/null

# Registry compatibility crates and other official transitive revisions are
# allowed. Vendored, path-overridden, and fork-sourced Reth crates are not.
jq -e --arg pattern "$official_reth_pattern" '
    [.packages[]
     | select(.name | test("^reth(-|$)"))
     | select(.source == null or (.source | startswith("git+")))
     | .source] as $sources
    | ($sources | length > 0) and
      all($sources[];
          if . == null then false else test($pattern) end)
' "$metadata_file" > /dev/null

jq -e '
    [.packages[].source
     | select(. != null)
     | select(
         contains("github.com/DogeOS69/dogeos-reth.git") or
         contains("github.com/scroll-tech/reth.git")
       )]
    | length == 0
' "$metadata_file" > /dev/null

echo "dependency graph verified: official paradigmxyz Reth 2"
