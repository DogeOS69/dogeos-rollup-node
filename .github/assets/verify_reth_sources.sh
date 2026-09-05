#!/usr/bin/env sh
set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)

# Canonical reviewed sources; every guarded package family must resolve to
# these exact strings (see verify_reth_sources.jq for the enforced rules).
components_source="git+https://github.com/DogeOS69/dogeos-reth.git?rev=7105d8d468622d32ef507dd7a547e492d2359451#7105d8d468622d32ef507dd7a547e492d2359451"
reth_source="git+https://github.com/DogeOS69/reth.git?rev=972366a0bfc11cf6a0d5dc79d5e779cd81e32232#972366a0bfc11cf6a0d5dc79d5e779cd81e32232"
revm_source="git+https://github.com/DogeOS69/dogeos-revm.git?rev=dcf087684f255131c96c0d20f3291eef9198e990#dcf087684f255131c96c0d20f3291eef9198e990"
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
