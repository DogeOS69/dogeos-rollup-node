#!/usr/bin/env bash
#
# Install the exact `anvil` binary the L1 integration tests are pinned to.
#
# The rollup-node L1 sync/reorg fixtures launch an external `anvil` process and
# refuse to run against any other build (see `crates/node/src/test_utils/fixture.rs`).
# This script fetches the reviewed Foundry `v1.5.0` release, verifies its archive
# against a hard-coded SHA-256 before extracting, installs only the `anvil`
# executable, and confirms the installed version and commit.
#
# Usage:
#   .github/assets/install_anvil.sh [DEST_DIR]
#
# DEST_DIR defaults to "$PWD/.anvil-bin". The resulting executable path is printed
# on the final line so callers can wire it into `ANVIL_BIN`, e.g.:
#   ANVIL_BIN="$(.github/assets/install_anvil.sh /opt/anvil | tail -n1)"
#
# Deliberately does NOT use foundryup or a mutable release channel: the version and
# checksums are pinned so CI and local runs resolve the identical binary.
set -euo pipefail

# --- Pinned release --------------------------------------------------------------
# The commit is the definitive identity. The immutable `v1.5.0` archive reports
# version `1.5.0-v1.5.0`; a `foundryup stable` build reports `1.5.0-stable`; both
# share this commit, so the version string is matched by prefix only.
readonly FOUNDRY_VERSION="v1.5.0"
readonly REQUIRED_VERSION_PREFIX="1.5.0"
readonly REQUIRED_COMMIT="1c57854462289b2e71ee7654cd6666217ed86ffd"
readonly BASE_URL="https://github.com/foundry-rs/foundry/releases/download/${FOUNDRY_VERSION}"

# Official Linux archive digests for ${FOUNDRY_VERSION}.
readonly SHA256_AMD64="5cd98f9092bcc28be087939491f786b2bf3ed55e492996a409e29519b8ab4dc8"
readonly SHA256_ARM64="8138e1615568bfcca5999773830892d93a569370eb0ae4b7dd97db46e2af47f9"

readonly DEST_DIR="${1:-$PWD/.anvil-bin}"

err() { printf 'install_anvil: %s\n' "$*" >&2; }

# --- Platform selection ----------------------------------------------------------
os="$(uname -s)"
if [ "${os}" != "Linux" ]; then
    err "unsupported OS '${os}'; only Linux archives are pinned. Install Foundry ${FOUNDRY_VERSION} manually and set ANVIL_BIN."
    exit 1
fi

arch="$(uname -m)"
case "${arch}" in
    x86_64 | amd64)
        archive="foundry_${FOUNDRY_VERSION}_linux_amd64.tar.gz"
        expected_sha="${SHA256_AMD64}"
        ;;
    aarch64 | arm64)
        archive="foundry_${FOUNDRY_VERSION}_linux_arm64.tar.gz"
        expected_sha="${SHA256_ARM64}"
        ;;
    *)
        err "unsupported architecture '${arch}'; only x86_64 and aarch64 are pinned."
        exit 1
        ;;
esac

# --- sha256 helper ---------------------------------------------------------------
if command -v sha256sum >/dev/null 2>&1; then
    sha256_of() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
    sha256_of() { shasum -a 256 "$1" | awk '{print $1}'; }
else
    err "neither sha256sum nor shasum is available; cannot verify the archive."
    exit 1
fi

# --- Download --------------------------------------------------------------------
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

archive_path="${tmp_dir}/${archive}"
url="${BASE_URL}/${archive}"
err "downloading ${url}"
curl --proto '=https' --tlsv1.2 -fsSL "${url}" -o "${archive_path}"

# --- Verify checksum BEFORE extracting -------------------------------------------
actual_sha="$(sha256_of "${archive_path}")"
if [ "${actual_sha}" != "${expected_sha}" ]; then
    err "checksum mismatch for ${archive}"
    err "  expected: ${expected_sha}"
    err "  actual:   ${actual_sha}"
    exit 1
fi
err "checksum verified: ${expected_sha}"

# --- Extract only the anvil executable -------------------------------------------
mkdir -p "${DEST_DIR}"
tar -xzf "${archive_path}" -C "${DEST_DIR}" anvil
anvil_bin="${DEST_DIR}/anvil"
chmod +x "${anvil_bin}"

# --- Verify the installed binary -------------------------------------------------
version_output="$("${anvil_bin}" --version)"
installed_version="$(printf '%s\n' "${version_output}" | sed -n 's/^anvil Version:[[:space:]]*//p' | head -n1)"
installed_commit="$(printf '%s\n' "${version_output}" | sed -n 's/^Commit SHA:[[:space:]]*//p' | head -n1)"

case "${installed_version}" in
    "${REQUIRED_VERSION_PREFIX}"*) ;;
    *)
        err "installed anvil does not match the pinned release"
        err "  expected version ${REQUIRED_VERSION_PREFIX}* commit ${REQUIRED_COMMIT}"
        err "  found    version ${installed_version:-<none>} commit ${installed_commit:-<none>}"
        exit 1
        ;;
esac
if [ "${installed_commit}" != "${REQUIRED_COMMIT}" ]; then
    err "installed anvil does not match the pinned release"
    err "  expected version ${REQUIRED_VERSION_PREFIX}* commit ${REQUIRED_COMMIT}"
    err "  found    version ${installed_version:-<none>} commit ${installed_commit:-<none>}"
    exit 1
fi

err "installed anvil ${installed_version} (${REQUIRED_COMMIT}) at ${anvil_bin}"
# Final stdout line: the executable path, for ANVIL_BIN wiring.
printf '%s\n' "${anvil_bin}"
