"""Real-BAYESTAR test gated behind ``@pytest.mark.bayestar``.

Skipped automatically when ``ligo.skymap`` is not importable so plain
``pytest`` keeps working without LALSuite. The CI ``bayestar-real`` job
installs ``ligo.skymap`` and runs ``pytest -m bayestar`` to exercise this
path; the contract-only job does not.
"""

from __future__ import annotations

import pathlib

import pytest

ligo_skymap = pytest.importorskip(
    "ligo.skymap",
    reason="ligo.skymap not installed; this test runs in the bayestar-real CI job",
)


# Mark the test as "bayestar" so a default pytest run does not pick it up
# unless the runner passes -m bayestar.
pytestmark = pytest.mark.bayestar

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "sample_coinc.xml"

# FITS files begin with "SIMPLE  =                    T" followed by
# more header. Sniffing the first eight bytes is enough to verify the
# byte stream is at least pretending to be FITS.
FITS_MAGIC_PREFIX = b"SIMPLE  "


def test_localize_against_captured_gracedb_test_event():
    """End-to-end BAYESTAR run on a real captured coinc.xml.

    The fixture is one of the MBTA messages we captured from
    gracedb-test.ligo.org during the SCITokens / Kafka spike. It has
    the per-IFO PSD and SNR ``<Array>`` elements BAYESTAR requires.
    The test uses the default ``TaylorF2threePointFivePN`` waveform so
    it does not depend on the SEOBNRv4_ROM HDF5 data files (which live
    in a separate `lalsuite-waveform-data` repo). Use
    ``bayestar-service/scripts/fetch-waveform-data.sh`` and re-run with
    the ``o2-uberbank`` waveform to exercise the production-grade path.
    """
    from bayestar_service.localizer import localize

    coinc_xml = FIXTURE.read_bytes()
    assert coinc_xml.startswith(b"<?xml"), "fixture is not XML"

    loc = localize(coinc_xml)
    assert loc.elapsed_ms > 0
    assert loc.fits_bytes.startswith(FITS_MAGIC_PREFIX), (
        "BAYESTAR output does not start with FITS magic bytes; "
        f"first 16 bytes: {loc.fits_bytes[:16]!r}"
    )
    # A real MOC FITS is well above 10 KB; a near-empty file would
    # mean BAYESTAR fell out without writing anything useful.
    assert len(loc.fits_bytes) > 10_000, (
        f"unreasonably small FITS payload ({len(loc.fits_bytes)} bytes)"
    )
