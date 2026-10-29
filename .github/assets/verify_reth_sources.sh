#!/usr/bin/env sh
set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)

# Canonical reviewed sources; every guarded package family must resolve to
# these exact strings (see verify_reth_sources.jq for the enforced rules).
components_source="git+https://github.com/DogeOS69/dogeos-reth.git?rev=f493cb9f561482f48d6b0711434880b84e991f04#f493cb9f561482f48d6b0711434880b84e991f04"
reth_source="git+https://github.com/DogeOS69/reth.git?tag=v2.0.0-dogeos-20261023.1#42b56304227691327c44415262d23ff680a44ffb"
revm_source="git+https://github.com/DogeOS69/dogeos-revm.git?rev=a36ef169a4b4df77f0cf9ff450721a0f46a91e42#a36ef169a4b4df77f0cf9ff450721a0f46a91e42"
official_reth_source="git+https://github.com/paradigmxyz/reth?rev=b25f32a977b489f9b84254c7811a2a5a25a81369#b25f32a977b489f9b84254c7811a2a5a25a81369"

# The predicate runs against a metadata file when one is provided (used by the
# fixtures in verify_reth_sources_test.sh); CI and local runs generate the
# metadata from the locked workspace.
if [ "$#" -gt 0 ]; then
    metadata_file="$1"
else
    metadata_file=$(mktemp)
    trap 'rm -f "$metadata_file"' EXIT
    cargo metadata --locked --offline --format-version 1 > "$metadata_file"
fi

violations=$(jq -r \
    --arg components "$components_source" \
    --arg reth "$reth_source" \
    --arg revm "$revm_source" \
    --arg official "$official_reth_source" \
    -f "$script_dir/verify_reth_sources.jq" \
    "$metadata_file")

if [ -n "$violations" ]; then
    printf '%s\n' "dependency source verification failed:" "$violations" >&2
    exit 1
fi

echo "dependency graph verified: qualified DogeOS component, Reth, and REVM revisions"
