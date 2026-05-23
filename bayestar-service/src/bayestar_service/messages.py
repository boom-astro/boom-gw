"""JSON contract between boom-gw (Rust) and bayestar-service (Python).

Field names mirror the `LocalizeRequest` / `LocalizeResult` types in
``boom_gw::localizer`` (see ``boom-gw/src/localizer.rs``). The Rust
test suite enforces the field set, so any drift here surfaces as a
Rust test failure before it can hit production.
"""

from __future__ import annotations

import base64
import json
from dataclasses import asdict, dataclass
from typing import Optional


@dataclass
class LocalizeRequest:
    request_id: str
    superevent_id: str
    graceid: str
    pipeline: str
    coinc_xml: str  # base64-encoded LIGO_LW coinc.xml bytes

    @classmethod
    def from_bytes(cls, payload: bytes) -> LocalizeRequest:
        obj = json.loads(payload)
        return cls(
            request_id=obj["request_id"],
            superevent_id=obj["superevent_id"],
            graceid=obj["graceid"],
            pipeline=obj["pipeline"],
            coinc_xml=obj["coinc_xml"],
        )

    def coinc_xml_bytes(self) -> bytes:
        return base64.b64decode(self.coinc_xml)


@dataclass
class LocalizeResult:
    request_id: str
    superevent_id: str
    graceid: str
    status: str  # "ok" or "error"
    elapsed_ms: int
    skymap_fits: Optional[str] = None  # base64-encoded HEALPix MOC FITS
    error_message: Optional[str] = None

    @classmethod
    def ok(
        cls,
        request: LocalizeRequest,
        skymap_fits_bytes: bytes,
        elapsed_ms: int,
    ) -> LocalizeResult:
        return cls(
            request_id=request.request_id,
            superevent_id=request.superevent_id,
            graceid=request.graceid,
            status="ok",
            elapsed_ms=elapsed_ms,
            skymap_fits=base64.b64encode(skymap_fits_bytes).decode("ascii"),
        )

    @classmethod
    def error(
        cls,
        request: LocalizeRequest,
        error_message: str,
        elapsed_ms: int,
    ) -> LocalizeResult:
        return cls(
            request_id=request.request_id,
            superevent_id=request.superevent_id,
            graceid=request.graceid,
            status="error",
            elapsed_ms=elapsed_ms,
            error_message=error_message,
        )

    def to_bytes(self) -> bytes:
        # Drop None-valued fields so the Rust side sees the absent shape
        # rather than `"skymap_fits": null`. Either is valid serde JSON,
        # but skipping keeps the wire form tidy.
        obj = {k: v for k, v in asdict(self).items() if v is not None}
        return json.dumps(obj).encode("utf-8")
