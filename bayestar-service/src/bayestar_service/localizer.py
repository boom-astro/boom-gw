"""BAYESTAR adapter.

Isolated from the Kafka loop so the localization path is independently
testable against captured coinc.xml fixtures without standing up a
broker.
"""

from __future__ import annotations

import io
import logging
import time
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class Localization:
    """Result of a single BAYESTAR call."""

    fits_bytes: bytes
    elapsed_ms: int


def localize(
    coinc_xml_bytes: bytes,
    *,
    f_low: float = 15.0,
    waveform: str = "TaylorF2threePointFivePN",
) -> Localization:
    """Run BAYESTAR against a coinc.xml document and return MOC FITS bytes.

    ``coinc_xml_bytes`` is the raw LIGO_LW XML payload exactly as it
    arrived on the GraceDB Kafka topic, including the PSD and per-IFO
    SNR series ``<Array>`` elements that BAYESTAR requires. The
    function returns the HEALPix MOC FITS as bytes, suitable for
    base64-encoding into a [`LocalizeResult`].

    The default ``waveform="TaylorF2threePointFivePN"`` works against a
    stock conda-forge install. The production-grade ``"o2-uberbank"``
    preset requires SEOBNRv4_ROM HDF5 data files that conda-forge does
    not ship; install ``lalsuite-extra`` separately and set
    ``LAL_DATA_PATH`` to enable that path.

    BAYESTAR is imported lazily so that the rest of the service
    (message parsing, Kafka glue) is importable even when LALSuite is
    not installed — e.g. inside a CI job that only exercises the
    contract tests.
    """
    # Lazy imports keep ligo.skymap / lalsuite off the import path for
    # callers (and tests) that do not actually run BAYESTAR.
    from ligo.skymap.bayestar import localize as bayestar_localize
    from ligo.skymap.io import events
    from ligo.skymap.io.fits import write_sky_map

    start = time.monotonic()
    coinc_io = io.BytesIO(coinc_xml_bytes)
    psd_io = io.BytesIO(coinc_xml_bytes)
    # The ligo.skymap reader expects coinc.xml and PSD both as file-like
    # objects; in our case both are the same document.
    by_id = events.ligolw.open(coinc_io, psd_io, None)
    if not by_id:
        raise RuntimeError("coinc.xml contained no coinc_inspiral events")
    (event,) = by_id.values()
    sky_map = bayestar_localize(event, waveform=waveform, f_low=f_low)

    buf = io.BytesIO()
    write_sky_map(buf, sky_map, moc=True)
    elapsed_ms = int((time.monotonic() - start) * 1000)
    log.debug("bayestar localize finished in %d ms", elapsed_ms)
    return Localization(fits_bytes=buf.getvalue(), elapsed_ms=elapsed_ms)
