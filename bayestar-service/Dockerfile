# bayestar-service container image.
#
# Base: micromamba/conda-forge. `ligo.skymap` pulls in the full
# LALSuite stack and its pip wheel is fragile; conda-forge ships
# pre-built binaries that just work. This is the same path the
# `bayestar-real` GitHub Actions job uses, so the runtime
# environment in CI matches production.
#
# What this image does NOT include:
#   * The SEOBNRv4_ROM / NR* HDF5 waveform data files (1-2 GB).
#     They're not needed for the default
#     `TaylorF2threePointFivePN` waveform that the service falls
#     back on. To enable the `o2-uberbank` path in production,
#     mount the data as a PVC at $LAL_DATA_PATH (see
#     `scripts/fetch-waveform-data.sh` for a downloader). The
#     container reads LAL_DATA_PATH at runtime, no rebuild needed.
#   * Real BAYESTAR is the default; pass `BAYESTAR_STUB=1` (or
#     `--stub`) for the canned-FITS path used by integration tests.

FROM mambaorg/micromamba:1.5.10-bookworm-slim

# micromamba's default user; everything in /opt/conda is owned by
# this UID. Switch to root briefly for system-level installs.
USER root

# Install the conda environment in /opt/conda directly (the base
# env). This avoids an `activate` dance at container start —
# `python` and `bayestar-service` resolve straight to the conda env's
# binaries via /opt/conda/bin on PATH.
COPY --chown=$MAMBA_USER:$MAMBA_USER bayestar-env.yaml /tmp/env.yaml
RUN micromamba install -y -n base -f /tmp/env.yaml && \
    micromamba clean --all --yes && \
    rm /tmp/env.yaml

# Copy the package source and install it into the same conda env.
# `pip install --no-deps` is safe here because micromamba already
# resolved every dep in the conda env above.
COPY --chown=$MAMBA_USER:$MAMBA_USER pyproject.toml /app/pyproject.toml
COPY --chown=$MAMBA_USER:$MAMBA_USER README.md      /app/README.md
COPY --chown=$MAMBA_USER:$MAMBA_USER src            /app/src
RUN /opt/conda/bin/pip install --no-deps /app

USER $MAMBA_USER
WORKDIR /app

# Activate the base env on every command so PATH / LAL env vars are
# set. Without this, librdkafka loaded by confluent-kafka won't find
# the conda-shipped libsasl.
ENV PATH=/opt/conda/bin:$PATH

# Sensible production defaults; override via the k8s manifest.
ENV BAYESTAR_BOOTSTRAP_SERVERS=broker:29092 \
    BAYESTAR_GROUP_ID=bayestar-service \
    BAYESTAR_AUTO_OFFSET_RESET=earliest \
    BAYESTAR_LOG_LEVEL=info

ENTRYPOINT ["python", "-m", "bayestar_service.main"]
