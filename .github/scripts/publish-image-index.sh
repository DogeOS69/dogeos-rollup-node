#!/usr/bin/env bash

set -euo pipefail

: "${IMAGE_NAME:?IMAGE_NAME is required}"
: "${DIGEST_DIR:?DIGEST_DIR is required}"
: "${EXPECTED_PLATFORMS_JSON:?EXPECTED_PLATFORMS_JSON is required}"
: "${DOCKER_TAGS_JSON:?DOCKER_TAGS_JSON is required}"
: "${EXPECTED_DIGEST_COUNT:?EXPECTED_DIGEST_COUNT is required}"

if [[ ! "$EXPECTED_DIGEST_COUNT" =~ ^[12]$ ]]; then
    echo "EXPECTED_DIGEST_COUNT must be 1 or 2" >&2
    exit 1
fi

declare -A runtime_by_arch=()
declare -A attestation_by_arch=()
sources=()

inspect_raw() {
    local ref=$1
    local stderr_file output last_error=
    local _

    stderr_file=$(mktemp)
    for _ in {1..5}; do
        if output=$(docker buildx imagetools inspect --raw "$ref" 2>"$stderr_file"); then
            rm -f "$stderr_file"
            printf '%s' "$output"
            return 0
        fi
        last_error=$(<"$stderr_file")
        sleep 2
    done

    echo "unable to inspect $ref after 5 attempts" >&2
    [[ -n "$last_error" ]] && echo "$last_error" >&2
    rm -f "$stderr_file"
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

validate_index() {
    local raw=$1
    local description=$2
    local arch platform runtime_digest attestation_digest
    local expected_descriptor_count

    jq -e 'select(.mediaType == "application/vnd.oci.image.index.v1+json")' <<<"$raw" >/dev/null || {
        echo "$description is not an OCI index" >&2
        return 1
    }

    expected_descriptor_count=$((EXPECTED_DIGEST_COUNT * 2))
    if [[ $(jq '.manifests | length' <<<"$raw") -ne $expected_descriptor_count ]]; then
        echo "$description contains an unexpected descriptor count" >&2
        return 1
    fi

    for platform in "${expected_platforms[@]}"; do
        arch=${platform#linux/}
        runtime_digest=${runtime_by_arch[$arch]}
        attestation_digest=${attestation_by_arch[$arch]}

        jq -e \
            --arg arch "$arch" \
            --arg runtime_digest "$runtime_digest" \
            --arg attestation_digest "$attestation_digest" '
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
                       and .digest == $attestation_digest
                       and .annotations["vnd.docker.reference.type"] == "attestation-manifest"
                       and .annotations["vnd.docker.reference.digest"] == $runtime_digest)] | length) == 1
        ' <<<"$raw" >/dev/null || {
            echo "$description is missing the validated linux/$arch runtime or attestation" >&2
            return 1
        }

        validate_attestation "$attestation_digest" "$runtime_digest"
    done
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

jq -e 'type == "array" and length > 0 and all(.[]; type == "string")' <<<"$EXPECTED_PLATFORMS_JSON" >/dev/null
mapfile -t expected_platforms < <(jq -r '.[]' <<<"$EXPECTED_PLATFORMS_JSON")
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

jq -e 'type == "array" and length > 0 and all(.[]; type == "string")' <<<"$DOCKER_TAGS_JSON" >/dev/null
mapfile -t tags < <(jq -r '.[]' <<<"$DOCKER_TAGS_JSON")

tag_args=()
tag_prefix="${IMAGE_NAME}:"
for tag in "${tags[@]}"; do
    if [[ "$tag" != "$tag_prefix"* ]]; then
        echo "final image tag uses the wrong repository: $tag" >&2
        exit 1
    fi
    tag_value=${tag#"$tag_prefix"}
    if [[ ! "$tag_value" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
        echo "invalid final image tag: $tag" >&2
        exit 1
    fi
    tag_args+=(--tag "$tag")
done

# Validate and hash the exact index bytes before any release tag is updated.
candidate_raw=$(docker buildx imagetools create --dry-run --prefer-index=true "${sources[@]}")
validate_index "$candidate_raw" "candidate index"
candidate_checksum=$(printf '%s' "$candidate_raw" | sha256sum)
candidate_digest="sha256:${candidate_checksum%% *}"

metadata_file=$(mktemp)
trap 'rm -f "$metadata_file"' EXIT
docker buildx imagetools create \
    --prefer-index=true \
    --metadata-file "$metadata_file" \
    "${tag_args[@]}" \
    "${sources[@]}"

published_digest=$(jq -er --arg candidate_digest "$candidate_digest" '
    .["containerimage.descriptor"]
    | select(.mediaType == "application/vnd.oci.image.index.v1+json")
    | select(.digest == $candidate_digest)
    | .digest
' "$metadata_file") || {
    echo "published metadata does not match validated candidate $candidate_digest" >&2
    exit 1
}

final_raw=
for tag in "${tags[@]}"; do
    inspect_output=$(docker buildx imagetools inspect "$tag")
    digest=$(awk '$1 == "Digest:" { digest = $2 } END { print digest }' <<<"$inspect_output")
    if [[ "$digest" != "$published_digest" ]]; then
        echo "$tag resolved to $digest instead of validated candidate $published_digest" >&2
        exit 1
    fi
    [[ -z "$final_raw" ]] && final_raw=$(inspect_raw "$tag")
done

final_checksum=$(printf '%s' "$final_raw" | sha256sum)
final_digest="sha256:${final_checksum%% *}"
if [[ "$final_digest" != "$candidate_digest" ]]; then
    echo "published index bytes do not match validated candidate $candidate_digest" >&2
    exit 1
fi
validate_index "$final_raw" "published index"

echo "Published ${tags[*]} at ${candidate_digest}"
