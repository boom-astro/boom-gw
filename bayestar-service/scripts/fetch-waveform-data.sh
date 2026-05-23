#!/usr/bin/env bash
#
# Download the SEOBNRv4_ROM and related waveform HDF5 data files that
# LALSuite needs to evaluate the BAYESTAR `o2-uberbank` waveform.
#
# Source: https://git.ligo.org/waveforms/software/lalsuite-waveform-data
# These files are not bundled with the conda-forge `lalsimulation-data`
# package and have to be fetched separately. The download is roughly
# one to two gigabytes total.
#
# Usage:
#     ./scripts/fetch-waveform-data.sh [target-dir]
#
# By default the target is $LAL_DATA_PATH (if set) or
# $HOME/.local/share/lal-waveform-data. Re-running the script is idempotent;
# files that already exist and pass the HDF5 magic-byte check are skipped.
#
# When the script finishes it prints the `export LAL_DATA_PATH=…` line
# to add to your shell profile, your systemd unit, or your container
# Dockerfile.

set -euo pipefail

REPO_URL="${LALSUITE_WAVEFORM_DATA_URL:-https://git.ligo.org/waveforms/software/lalsuite-waveform-data}"
BRANCH="${LALSUITE_WAVEFORM_DATA_BRANCH:-main}"
TARGET_DIR="${1:-${LAL_DATA_PATH:-$HOME/.local/share/lal-waveform-data}}"

# The full file set as of the upstream `main` branch. BAYESTAR's
# `o2-uberbank` configuration uses several of these depending on the
# coincidence's mass range, so we fetch them all rather than guessing.
FILES=(
    NRHybSur3dq8_lal_v1.0.h5
    NRSur3dq8Remnant_v1.0.h5
    NRSur7dq4Remnant_v1.0.h5
    NRSur7dq4_v1.0.h5
    SEOBNRv4HMROM_v1.0.hdf5
    SEOBNRv4ROM_v3.0.hdf5
    SEOBNRv4T_surrogate_v2.0.0.hdf5
    SEOBNRv5HMROM_v1.0.hdf5
    SEOBNRv5ROM_v1.0.hdf5
)

mkdir -p "$TARGET_DIR"

# Check whether the given path is a valid HDF5 file by sniffing the
# 4-byte magic prefix (0x89 'H' 'D' 'F').
is_hdf5() {
    local f="$1"
    [ -f "$f" ] && [ -s "$f" ] || return 1
    local magic
    magic=$(head -c 4 "$f" | od -An -tx1 | tr -d ' \n')
    [ "$magic" = "89484446" ]
}

# Pretty-print the size in MiB if we can; fall back to "?".
human_size() {
    local f="$1"
    if [ -f "$f" ]; then
        local bytes
        bytes=$(wc -c <"$f" | tr -d ' ')
        awk -v b="$bytes" 'BEGIN { printf "%.1f MiB", b/1048576 }'
    else
        echo "?"
    fi
}

echo "Target: $TARGET_DIR"
echo "Source: $REPO_URL (branch $BRANCH)"
echo

for name in "${FILES[@]}"; do
    out="$TARGET_DIR/$name"
    if is_hdf5 "$out"; then
        echo "[skip]    $name  ($(human_size "$out") already present)"
        continue
    fi

    url="$REPO_URL/-/raw/$BRANCH/waveform_data/$name?inline=false"
    echo "[fetch]   $name"
    # `--fail-with-body` makes curl exit non-zero on a 4xx/5xx and still
    # show the response body for diagnostics. `--retry 3 --retry-delay 5`
    # rides through transient blips at git.ligo.org without manual
    # intervention. `--continue-at -` resumes a partial download if a
    # previous attempt was interrupted mid-byte.
    curl \
        --fail-with-body \
        --location \
        --retry 3 \
        --retry-delay 5 \
        --retry-connrefused \
        --progress-bar \
        --continue-at - \
        --output "$out.part" \
        "$url"

    if is_hdf5 "$out.part"; then
        mv "$out.part" "$out"
        echo "[done]    $name  ($(human_size "$out"))"
    else
        echo "Downloaded file $out.part is not a valid HDF5 — aborting." >&2
        echo "First 64 bytes:" >&2
        head -c 64 "$out.part" | od -An -c | head -3 >&2
        exit 1
    fi
done

cat <<EOF

All ${#FILES[@]} waveform data files are in:
  $TARGET_DIR

Total: $(du -sh "$TARGET_DIR" | awk '{print $1}')

Add the following to the environment that runs bayestar-service (your
shell profile, the service's systemd unit, or its Dockerfile):

  export LAL_DATA_PATH="$TARGET_DIR\${LAL_DATA_PATH:+:\$LAL_DATA_PATH}"

After that, bayestar-service can be run with the default
\`o2-uberbank\` waveform instead of the fallback
\`TaylorF2threePointFivePN\`.
EOF
