#!/usr/bin/env sh
# Fixtures for verify_reth_sources.sh: the unmodified dependency graph must
# pass, and every mutated copy of it must be rejected. Requires a fetched
# workspace (cargo metadata --locked --offline must succeed).
set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
verify="$script_dir/verify_reth_sources.sh"

workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT

metadata_file="$workdir/metadata.json"
cargo metadata --locked --offline --format-version 1 > "$metadata_file"

# Positive control: the checked-in graph must pass through the file entry point.
"$verify" "$metadata_file" > /dev/null
echo "ok: guard accepts the checked-in dependency graph"

fixture="$workdir/fixture.json"
canonical_metadata="$workdir/metadata.canonical.json"
jq -S . "$metadata_file" > "$canonical_metadata"

expect_reject() {
    description="$1"
    mutation="$2"
    jq "$mutation" "$metadata_file" > "$fixture"
    # A fixture that does not change the metadata would trivially pass the
    # guard; treat it as a broken fixture rather than a rejection.
    if jq -S . "$fixture" | cmp -s - "$canonical_metadata"; then
        echo "FAIL: fixture mutation for $description left the metadata unchanged" >&2
        exit 1
    fi
    if "$verify" "$fixture" > /dev/null 2>&1; then
        echo "FAIL: guard accepted $description" >&2
        exit 1
    fi
    echo "ok: guard rejects $description"
}

expect_reject "a null-source DogeOS component" \
    '.packages |= map(if .name == "dogeos-reth-evm" then .source = null else . end)'

expect_reject "a path-source DogeOS component" \
    '.packages |= map(if .name == "dogeos-chainspec" then .source = "path+file:///tmp/dogeos-chainspec" else . end)'

expect_reject "the obsolete c5198f7 DogeOS component source alongside the good anchors" \
    '.packages += [first(.packages[] | select(.name == "dogeos-chainspec")) | .source = "git+https://github.com/DogeOS69/dogeos-reth.git?rev=c5198f722a4fcbd47e8c0a10fe8f9835a801c2d2#c5198f722a4fcbd47e8c0a10fe8f9835a801c2d2"]'

expect_reject "the immediately retired 18adb117 DogeOS component source alongside the good anchors" \
    '.packages += [first(.packages[] | select(.name == "dogeos-chainspec")) | .source = "git+https://github.com/DogeOS69/dogeos-reth.git?rev=18adb1176636b4f3bdc828a15c4622f60d2e5ec7#18adb1176636b4f3bdc828a15c4622f60d2e5ec7"]'

expect_reject "the immediately retired 81c8b33e DogeOS component source alongside the good anchors" \
    '.packages += [first(.packages[] | select(.name == "dogeos-chainspec")) | .source = "git+https://github.com/DogeOS69/dogeos-reth.git?rev=81c8b33ea958fd03173bc37094b97ddebeed1441#81c8b33ea958fd03173bc37094b97ddebeed1441"]'

expect_reject "the obsolete PR #3 ae160090 clean-Reth source alongside the good anchors" \
    '.packages += [first(.packages[] | select(.name == "reth-node-builder")) | .source = "git+https://github.com/DogeOS69/reth.git?rev=ae160090003d9b04be0521e9e4760558798cdf40#ae160090003d9b04be0521e9e4760558798cdf40"]'

expect_reject "the immediately retired f851224e clean-Reth source alongside the good anchors" \
    '.packages += [first(.packages[] | select(.name == "reth-node-builder")) | .source = "git+https://github.com/DogeOS69/reth.git?rev=f851224ee9aaf21c76a14e844cbd12d9756f5f3b#f851224ee9aaf21c76a14e844cbd12d9756f5f3b"]'

expect_reject "a retired dogeos-reth2 source without the .git suffix" \
    '.packages += [first(.packages[] | select(.name == "reth-node-builder")) | .source = "git+https://github.com/DogeOS69/dogeos-reth2?rev=0000000000000000000000000000000000000000#0000000000000000000000000000000000000000"]'

expect_reject "a retired scroll-reth source with alternate casing" \
    '.packages += [first(.packages[] | select(.name == "reth-node-builder")) | .source = "git+https://github.com/DogeOS69/Scroll-Reth.git?branch=dev#0000000000000000000000000000000000000000"]'

expect_reject "a retired scroll-tech/reth source without the .git suffix" \
    '.packages += [first(.packages[] | select(.name == "reth-node-builder")) | .source = "git+https://github.com/scroll-tech/reth?branch=scroll#0000000000000000000000000000000000000000"]'

expect_reject "a duplicate lowercase-organization DogeOS REVM source" \
    '.packages += [first(.packages[] | select(.name == "revm-scroll")) | .source = "git+https://github.com/dogeos69/dogeos-revm?rev=dcf087684f255131c96c0d20f3291eef9198e990#dcf087684f255131c96c0d20f3291eef9198e990"]'

expect_reject "the immediately retired branch-form DogeOS REVM source alongside the good anchors" \
    '.packages += [first(.packages[] | select(.name == "revm-scroll")) | .source = "git+https://github.com/DogeOS69/dogeos-revm.git?branch=dogeos#dcf087684f255131c96c0d20f3291eef9198e990"]'

expect_reject "a wrong DogeOS REVM revision" \
    '.packages |= map(if .name == "revm-scroll" then .source = "git+https://github.com/DogeOS69/dogeos-revm.git?rev=1111111111111111111111111111111111111111#1111111111111111111111111111111111111111" else . end)'

# The reviewed official Reth subtree only ever entered the graph through the
# removed Foundry/Tempo subtree, so the clean graph no longer contains a
# paradigmxyz/reth package to mutate. Inject a wrong official-Reth source onto a
# non-anchor, non-reth-* package so the rejection is attributed specifically to the
# repository-level official-Reth rule rather than the reth-* naming rule.
expect_reject "a wrong official Reth revision alongside the good anchors" \
    '.packages += [first(.packages[] | select(.name == "alloy-node-bindings")) | .source = "git+https://github.com/paradigmxyz/reth?rev=2222222222222222222222222222222222222222#2222222222222222222222222222222222222222"]'

echo "source guard fixtures passed"
