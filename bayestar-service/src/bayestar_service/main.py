"""Kafka-driven BAYESTAR service entrypoint.

Subscribes to a request topic from boom-gw, runs BAYESTAR on each
incoming coinc.xml, and publishes the FITS sky map to a result topic.
The service is single-threaded by design: BAYESTAR releases the GIL
during its C/C++ calls, but running multiple localizations
concurrently in one process produces nondeterministic ordering and
makes correlation IDs harder to reason about. Scale by running
multiple instances against the same Kafka consumer group.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from typing import Optional

from confluent_kafka import Consumer, KafkaError, Producer

from .localizer import Localization, localize
from .messages import LocalizeRequest, LocalizeResult

DEFAULT_REQUEST_TOPIC = "boom-gw.localize-request"
DEFAULT_RESULT_TOPIC = "boom-gw.localize-result"

# Deterministic byte string the --stub localizer returns instead of a real
# FITS. The Rust integration test asserts on this exact value to confirm a
# full Kafka round-trip and rules out coincidental success.
STUB_FITS_BYTES = b"STUB_FITS_PAYLOAD_for_integration_tests"

log = logging.getLogger("bayestar_service")


def build_consumer(args: argparse.Namespace) -> Consumer:
    config = {
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": args.auto_offset_reset,
        "enable.auto.commit": True,
        # The service does its own offset management implicitly via
        # auto-commit. If a localization fails for a transient reason
        # the message is committed anyway; boom-gw will reissue when
        # the next event arrives. That is the conservative choice for
        # an exploratory service.
    }
    return Consumer(config)


def build_producer(args: argparse.Namespace) -> Producer:
    return Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "acks": "1",
            "compression.type": "zstd",
            # Real BAYESTAR FITS files are ~500-800 KB; the wrapping
            # JSON envelope base64-expands the bytes by ~4/3, pushing
            # the per-message size past Kafka's 1 MB default. 16 MB
            # is generous and matches the broker-side bump in
            # docker-compose.yaml / the NRP kafka manifest.
            "message.max.bytes": 16 * 1024 * 1024,
        }
    )


def _stub_localize(_coinc_xml_bytes: bytes) -> Localization:
    """Bypass BAYESTAR. Used by the --stub integration-test mode."""
    return Localization(fits_bytes=STUB_FITS_BYTES, elapsed_ms=1)


def make_handler(stub: bool):
    """Build the per-message handler. ``stub=True`` skips the real BAYESTAR
    call so the Kafka round-trip can be exercised in CI without LALSuite."""
    localize_fn = _stub_localize if stub else localize

    def handle(request: LocalizeRequest) -> LocalizeResult:
        start = time.monotonic()
        try:
            loc = localize_fn(request.coinc_xml_bytes())
            return LocalizeResult.ok(
                request, skymap_fits_bytes=loc.fits_bytes, elapsed_ms=loc.elapsed_ms
            )
        except Exception as exc:  # noqa: BLE001 — surface all errors
            elapsed_ms = int((time.monotonic() - start) * 1000)
            log.exception(
                "bayestar failed for superevent=%s graceid=%s",
                request.superevent_id,
                request.graceid,
            )
            return LocalizeResult.error(request, error_message=str(exc), elapsed_ms=elapsed_ms)

    return handle


def run(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    consumer = build_consumer(args)
    producer = build_producer(args)
    handler = make_handler(stub=args.stub)
    if args.stub:
        log.warning("--stub is set: BAYESTAR will NOT run; canned FITS bytes will be returned")

    consumer.subscribe([args.request_topic])
    log.info(
        "subscribed to request_topic=%s, publishing to result_topic=%s, group_id=%s",
        args.request_topic,
        args.result_topic,
        args.group_id,
    )

    stop = {"flag": False}

    def _handle_signal(_sig, _frame) -> None:
        log.info("signal received, draining and stopping")
        stop["flag"] = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        while not stop["flag"]:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.warning("kafka error: %s", msg.error())
                continue
            payload = msg.value()
            if payload is None:
                continue
            try:
                request = LocalizeRequest.from_bytes(payload)
            except Exception:
                log.exception("failed to parse request envelope; skipping")
                continue
            log.info(
                "handling superevent=%s graceid=%s request_id=%s",
                request.superevent_id,
                request.graceid,
                request.request_id,
            )
            result = handler(request)
            producer.produce(
                topic=args.result_topic,
                key=result.superevent_id.encode("utf-8"),
                value=result.to_bytes(),
            )
            producer.poll(0)
            log.info(
                "%s superevent=%s in %d ms",
                result.status,
                result.superevent_id,
                result.elapsed_ms,
            )
    finally:
        log.info("flushing producer")
        producer.flush(timeout=10)
        consumer.close()

    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka-driven BAYESTAR sky-map localization service for boom-gw."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("BAYESTAR_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap broker list.",
    )
    parser.add_argument(
        "--group-id",
        default=os.environ.get("BAYESTAR_GROUP_ID", "bayestar-service"),
        help="Kafka consumer group ID.",
    )
    parser.add_argument(
        "--request-topic",
        default=os.environ.get("BAYESTAR_REQUEST_TOPIC", DEFAULT_REQUEST_TOPIC),
    )
    parser.add_argument(
        "--result-topic",
        default=os.environ.get("BAYESTAR_RESULT_TOPIC", DEFAULT_RESULT_TOPIC),
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("BAYESTAR_LOG_LEVEL", "info"),
        choices=["debug", "info", "warning", "error"],
    )
    parser.add_argument(
        "--auto-offset-reset",
        default=os.environ.get("BAYESTAR_AUTO_OFFSET_RESET", "latest"),
        choices=["latest", "earliest"],
        help="Kafka consumer offset reset policy when no committed offset exists.",
    )
    parser.add_argument(
        "--stub",
        action="store_true",
        default=os.environ.get("BAYESTAR_STUB", "").lower() in ("1", "true", "yes"),
        help="Skip BAYESTAR and return canned FITS bytes; intended for "
        "integration tests where LALSuite is not installed.",
    )
    return parser.parse_args(argv)


def main() -> int:  # pragma: no cover — entrypoint
    return run(parse_args())


if __name__ == "__main__":
    sys.exit(main())
