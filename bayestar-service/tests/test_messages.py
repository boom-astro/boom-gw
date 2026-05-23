"""Round-trip contract tests.

These exercise the JSON wire format the Python service sees from the
Rust side. The shape is locked in on the Rust side by
``tests::request_json_shape_matches_python_expectations``; this test
verifies the Python side accepts it.
"""

from __future__ import annotations

import base64
import json

from bayestar_service.messages import LocalizeRequest, LocalizeResult


def test_request_round_trips_through_rust_shape():
    # Identical to what `LocalizeRequest::from_coinc_xml` in the Rust
    # crate emits for the same inputs.
    payload = json.dumps(
        {
            "request_id": "req-abc",
            "superevent_id": "S000001",
            "graceid": "G42",
            "pipeline": "gstlal",
            "coinc_xml": base64.b64encode(b"<x/>").decode("ascii"),
        }
    ).encode("utf-8")

    req = LocalizeRequest.from_bytes(payload)
    assert req.request_id == "req-abc"
    assert req.superevent_id == "S000001"
    assert req.graceid == "G42"
    assert req.pipeline == "gstlal"
    assert req.coinc_xml_bytes() == b"<x/>"


def test_ok_result_emits_rust_compatible_shape():
    req = LocalizeRequest(
        request_id="req-1",
        superevent_id="S0",
        graceid="G7",
        pipeline="mbta",
        coinc_xml="",
    )
    fits = b"SIMPLE  =                    T"
    result = LocalizeResult.ok(req, skymap_fits_bytes=fits, elapsed_ms=137)
    obj = json.loads(result.to_bytes())
    assert obj["status"] == "ok"
    assert obj["request_id"] == "req-1"
    assert obj["elapsed_ms"] == 137
    assert base64.b64decode(obj["skymap_fits"]) == fits
    # No error_message field on success — it is dropped from the wire
    # form rather than serialized as null.
    assert "error_message" not in obj


def test_error_result_emits_rust_compatible_shape():
    req = LocalizeRequest(
        request_id="req-2",
        superevent_id="S1",
        graceid="G8",
        pipeline="pycbc",
        coinc_xml="",
    )
    result = LocalizeResult.error(req, error_message="PSDs missing", elapsed_ms=12)
    obj = json.loads(result.to_bytes())
    assert obj["status"] == "error"
    assert obj["error_message"] == "PSDs missing"
    assert "skymap_fits" not in obj
