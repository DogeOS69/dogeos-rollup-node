#!/usr/bin/env bash

set -euo pipefail

: "${IMAGE_NAME:?IMAGE_NAME is required}"
: "${DIGEST_DIR:?DIGEST_DIR is required}"
: "${EXPECTED_PLATFORMS_JSON:?EXPECTED_PLATFORMS_JSON is required}"
: "${DOCKER_TAGS_JSON:?DOCKER_TAGS_JSON is required}"
: "${EXPECTED_DIGEST_COUNT:?EXPECTED_DIGEST_COUNT is required}"

declare -A runtime_by_arch=()
declare -A attestation_by_arch=()
sources=()

inspect_raw() {
    local ref=$1
    local _

    for _ in {1..5}; do
        if docker buildx imagetools inspect --raw "$ref" 2>/dev/null; then
            return 0
        fi
        sleep 2
    done

    echo "unable to inspect $ref" >&2
    return 1
}

validate_attestation() {
    local attestation_digest=$1
    local runtime_digest=$2
    local raw

    raw=$(inspect_raw "${IMAGE_NAME}@${attestation_digest}")
    jq -e --arg runtime_digest "$runtime_digest" '
        .mediaType == "application/vnd.oci.image.manifest.v1+json"
        and .artifactType == "application/vnd.docker.attestation.manifest.v1+json"
        and .subject.mediaType == "application/vnd.oci.image.manifest.v1+json"
        and .subject.digest == $runtime_digest
        and .config.mediaType == "application/vnd.oci.empty.v1+json"
        and (.layers | length) == 1
        and .layers[0].mediaType == "application/vnd.in-toto+json"
        and .layers[0].annotations["in-toto.io/predicate-type"] == "https://slsa.dev/provenance/v1"
    ' <<<"$raw" >/dev/null || {
        echo "invalid provenance attestation ${attestation_digest}" >&2
        return 1
    }
}

mapfile -t marker_files < <(find "$DIGEST_DIR" -maxdepth 1 -type f -printf '%f\n' | sort)
if [[ ${#marker_files[@]} -ne $EXPECTED_DIGEST_COUNT ]]; then
    echo "expected $EXPECTED_DIGEST_COUNT digest markers, found ${#marker_files[@]}" >&2
    exit 1
fi

for marker in "${marker_files[@]}"; do
    if [[ ! "$marker" =~ ^(amd64|arm64)-([0-9a-f]{64})$ ]]; then
        echo "invalid digest marker: $marker" >&2
        exit 1
    fi

    arch=${BASH_REMATCH[1]}
    source_digest="sha256:${BASH_REMATCH[2]}"
    source_ref="${IMAGE_NAME}@${source_digest}"

    if [[ -n ${runtime_by_arch[$arch]:-} ]]; then
        echo "duplicate digest marker for $arch" >&2
        exit 1
    fi

    raw=$(inspect_raw "$source_ref")
    runtime_digest=$(jq -er --arg arch "$arch" '
        select(.mediaType == "application/vnd.oci.image.index.v1+json")
        | [.manifests[] | select(.platform.os == "linux" and .platform.architecture == $arch)]
        | select(length == 1)
        | .[0].digest
    ' <<<"$raw") || {
        echo "source $source_ref does not contain exactly one linux/$arch runtime manifest" >&2
        exit 1
    }

    jq -e --arg arch "$arch" --arg runtime_digest "$runtime_digest" '
        all(.manifests[];
            (.platform.os == "linux"
             and .platform.architecture == $arch
             and .digest == $runtime_digest
             and .mediaType == "application/vnd.oci.image.manifest.v1+json")
            or
            (.platform.os == "unknown"
             and .platform.architecture == "unknown"
             and .mediaType == "application/vnd.oci.image.manifest.v1+json"
             and .annotations["vnd.docker.reference.type"] == "attestation-manifest"
             and .annotations["vnd.docker.reference.digest"] == $runtime_digest)
        )
        and ([.manifests[] | select(.annotations["vnd.docker.reference.type"] == "attestation-manifest")] | length) == 1
    ' <<<"$raw" >/dev/null || {
        echo "source $source_ref contains an unexpected descriptor" >&2
        exit 1
    }

    attestation_digest=$(jq -er '.manifests[] | select(.annotations["vnd.docker.reference.type"] == "attestation-manifest") | .digest' <<<"$raw")
    validate_attestation "$attestation_digest" "$runtime_digest"

    runtime_by_arch[$arch]=$runtime_digest
    attestation_by_arch[$arch]=$attestation_digest
    sources+=("$source_ref")
done

mapfile -t expected_platforms < <(jq -er '.[]' <<<"$EXPECTED_PLATFORMS_JSON")
if [[ ${#expected_platforms[@]} -ne $EXPECTED_DIGEST_COUNT ]]; then
    echo "expected platform count does not match digest count" >&2
    exit 1
fi

for platform in "${expected_platforms[@]}"; do
    arch=${platform#linux/}
    if [[ "$platform" != "linux/$arch" || -z ${runtime_by_arch[$arch]:-} ]]; then
        echo "missing validated runtime for $platform" >&2
        exit 1
    fi
done

mapfile -t tags < <(jq -er '.[]' <<<"$DOCKER_TAGS_JSON")
if [[ ${#tags[@]} -eq 0 ]]; then
    echo "no final image tags supplied" >&2
    exit 1
fi

tag_args=()
for tag in "${tags[@]}"; do
    if [[ ! "$tag" =~ ^${IMAGE_NAME}:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
        echo "invalid final image tag: $tag" >&2
        exit 1
    fi
    tag_args+=(--tag "$tag")
done

docker buildx imagetools create "${tag_args[@]}" "${sources[@]}"

common_digest=
final_raw=
for tag in "${tags[@]}"; do
    digest=$(docker buildx imagetools inspect "$tag" | awk '$1 == "Digest:" { print $2; exit }')
    if [[ ! "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
        echo "could not resolve final digest for $tag" >&2
        exit 1
    fi
    if [[ -z "$common_digest" ]]; then
        common_digest=$digest
        final_raw=$(inspect_raw "$tag")
    elif [[ "$digest" != "$common_digest" ]]; then
        echo "final tags do not share one index digest" >&2
        exit 1
    fi
done

jq -e 'select(.mediaType == "application/vnd.oci.image.index.v1+json")' <<<"$final_raw" >/dev/null || {
    echo "final image is not an OCI index" >&2
    exit 1
}

expected_descriptor_count=$((EXPECTED_DIGEST_COUNT * 2))
if [[ $(jq '.manifests | length' <<<"$final_raw") -ne $expected_descriptor_count ]]; then
    echo "final index contains an unexpected descriptor count" >&2
    exit 1
fi

for platform in "${expected_platforms[@]}"; do
    arch=${platform#linux/}
    runtime_digest=${runtime_by_arch[$arch]}

    jq -e --arg arch "$arch" --arg runtime_digest "$runtime_digest" '
        ([.manifests[]
          | select(.platform.os == "linux"
                   and .platform.architecture == $arch
                   and .mediaType == "application/vnd.oci.image.manifest.v1+json"
                   and .digest == $runtime_digest)] | length) == 1
        and
        ([.manifests[]
          | select(.platform.os == "unknown"
                   and .platform.architecture == "unknown"
                   and .mediaType == "application/vnd.oci.image.manifest.v1+json"
                   and .annotations["vnd.docker.reference.type"] == "attestation-manifest"
                   and .annotations["vnd.docker.reference.digest"] == $runtime_digest)] | length) == 1
    ' <<<"$final_raw" >/dev/null || {
        echo "final index is missing the validated linux/$arch runtime or attestation" >&2
        exit 1
    }

    attestation_digest=$(jq -er --arg runtime_digest "$runtime_digest" '
        .manifests[]
        | select(.annotations["vnd.docker.reference.type"] == "attestation-manifest"
                 and .annotations["vnd.docker.reference.digest"] == $runtime_digest)
        | .digest
    ' <<<"$final_raw")
    if [[ "$attestation_digest" != "${attestation_by_arch[$arch]}" ]]; then
        echo "final index replaced the validated linux/$arch attestation descriptor" >&2
        exit 1
    fi
    validate_attestation "$attestation_digest" "$runtime_digest"
done

echo "Published ${tags[*]} at ${common_digest}"
