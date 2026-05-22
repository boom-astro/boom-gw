# boom-gw

Gravitational-wave alert ingestion, superevent clustering, and downstream
publishing on top of the LIGO/Virgo/KAGRA GraceDB Kafka topics.

`boom-gw` is the GW counterpart to [BOOM](https://github.com/boom-astro/boom),
the Rust alert broker for optical surveys. It consumes pipeline events from
the GraceDB Kafka spine (`gstlal`, `mbta`, `pycbc`, `spiir`, `aframe`,
`cwb`, `mly`), parses the embedded `coinc.xml` payload with a native Rust
parser, clusters the resulting events into superevents using the same
window/SNR-preferred policy as [sgn-llai](https://git.ligo.org/computing/sgn-llai),
publishes the resulting superevent stream onto a downstream Kafka topic,
and persists open-superevent state in Redis so the clusterer is
restart-safe.

The crate is intentionally framework-independent. It runs as a standalone
service with `cargo run --bin gw_clusterer`, and is also exposed from
`boom` itself under the `gw` Cargo feature when both repositories sit
side by side.

## Architecture

```
GraceDB Kafka topics                                 boom-gw
─────────────────────                                ───────
gracedb-test.gstlal     ┐                            ┌─ src/envelope.rs   JSON envelope
gracedb-test.mbta       │   OAUTHBEARER (SCITokens)  │  src/event.rs      GwEvent
gracedb-test.pycbc      ├──────────────────────────► │  ligo-lw/          coinc.xml parser
gracedb-test.spiir      │                            │  src/clustering.rs SupereventCreator
gracedb-test.aframe     │                            │  src/state.rs      Redis state
gracedb-test.cwb        │                            └─ src/publisher.rs  emit to Kafka
gracedb-test.mly        ┘                                              │
                                                                       ▼
                                                                  downstream
                                                                  consumers
                                                                  (alert
                                                                  service,
                                                                  MMA
                                                                  correlator,
                                                                  ...)
```

The clustering layer carries the same semantics as `sgn-llai`'s
`SupereventCreator`: a 5-second window centred on the GPS time of the
first event opening the superevent, bisect-based candidate lookup with
the closest `t_0` winning on ties, and an SNR-preferred policy that
upgrades the preferred slot whenever a higher-SNR event arrives within
the window. Lower-SNR arrivals are still recorded in the `g_events` list
but do not replace the preferred event. A side-by-side parity check
against a faithful re-implementation of the `sgn-llai` algorithm on a
25-event corpus from `gracedb-test` produces an empty diff; see the
**Comparison with sgn-llai** section below.

## Prerequisites

* Rust 1.75 or newer.
* For live consumption: a valid LIGO/IGWN SCITokens bearer token. The
  standard acquisition path is `htgettoken` against `vault.ligo.org`. The
  package is pip-installable.
* For Redis-backed state: a Redis or Valkey instance reachable from the
  consumer.
* For publishing: a target Kafka cluster. Authentication on the publish
  side is plain `SASL_PLAINTEXT` or no auth in the current implementation;
  OAUTHBEARER for the publish side is on the roadmap below.

## Build

```bash
git clone https://github.com/boom-astro/boom-gw
cd boom-gw
cargo build --release
```

The workspace builds the library, the embedded `ligo-lw` crate, and the
three binaries below in one pass.

## Acquire a SCITokens bearer token

Outside of `htgettoken`, no token-acquisition path is implemented in this
crate, and you do not want one: token issuance is owned by the LIGO
vault and you should not duplicate that machinery here. The Python
`ligo.gracedb.kafka.GraceDbKafkaConsumer` client follows the same
convention — both read tokens out of standard WLCG discovery locations.

```bash
# Once per session: vault flow opens a browser for OIDC the first time
# and uses a cached refresh token thereafter.
pip install htgettoken
htgettoken -a vault.ligo.org -i igwn \
    --audience=kafka-dev.ligo.org \
    --scopes=kafka.consume \
    -o $BEARER_TOKEN_FILE
```

The resulting file is what `--token-file` expects. The default discovery
order is `$BEARER_TOKEN_FILE`, then `$XDG_RUNTIME_DIR/bt_u<uid>`, then
`/tmp/bt_u<uid>`.

**macOS gotcha.** `htgettoken` self-disables the OIDC flow when none of
stdout / stderr / stdin is a TTY. If you call it from a non-interactive
shell (e.g. inside a Makefile or a CI runner), wrap the invocation with
`script -q /dev/null htgettoken ...` so that it sees a pseudo-TTY and
runs the browser-based device-code flow.

For querying the GraceDB REST API (separate from the Kafka stream), the
audience is `gracedb-test.ligo.org` and the scope is `gracedb.read`. The
deployed server's `SCITOKEN_SCOPE` env var must allow whatever scope
your token carries; in practice the GraceDB administrator publishes this
list in the deployment notes.

## Run the binaries

### gw_consumer — read events and print

```bash
cargo run --release --bin gw_consumer -- \
    --bootstrap-servers kafka-dev.ligo.org:9092 \
    --topics gracedb-test.gstlal,gracedb-test.mbta,gracedb-test.pycbc,gracedb-test.spiir,gracedb-test.aframe,gracedb-test.cwb,gracedb-test.mly \
    --token-file $BEARER_TOKEN_FILE \
    --group-id "$USER-boom-spike" \
    --auto-offset-reset earliest \
    --max-events 50
```

One line per decoded `GwEvent` is printed: producer timestamp, graceid,
pipeline, message type, IFOs, network SNR, FAR, chirp mass, GPS end
time. Use `Ctrl-C` to stop in the unbounded mode.

### gw_dump — capture envelopes to disk

```bash
cargo run --release --bin gw_dump -- \
    --topics gracedb-test.gstlal,gracedb-test.mbta \
    --token-file $BEARER_TOKEN_FILE \
    --group-id "$USER-boom-dump" \
    --max-messages 25 \
    --out-dir /tmp/gw-payloads
```

For each received message, the raw JSON envelope is written to
`/tmp/gw-payloads/msg_NNNN.json` and the decoded `coinc.xml` to
`/tmp/gw-payloads/msg_NNNN.xml`. These are the input format the
clusterer accepts in offline replay mode.

### gw_clusterer — superevent clustering

Live mode, with optional Kafka publish and Redis state:

```bash
cargo run --release --bin gw_clusterer -- \
    --bootstrap-servers kafka-dev.ligo.org:9092 \
    --topics gracedb-test.gstlal,gracedb-test.mbta \
    --token-file $BEARER_TOKEN_FILE \
    --group-id "$USER-boom-clusterer" \
    --window-secs 5.0 \
    --publish-servers downstream-kafka.example.org:9092 \
    --publish-topic boom-gw.superevents \
    --redis-url redis://localhost:6379/ \
    --redis-prefix gw:clusterer:default
```

Offline replay mode (no auth needed):

```bash
cargo run --release --bin gw_clusterer -- \
    --replay-dir /tmp/gw-payloads \
    --out-jsonl /tmp/boom-clustering.jsonl
```

In replay mode the clusterer reads `*.json` envelope files written by
`gw_dump`, sorts them by `_producer_timestamp`, deduplicates by graceid,
and runs them through the clustering layer in the same order they would
have arrived live. The optional `--out-jsonl` writes one line per
processed event with the resulting superevent assignment, suitable for
diffing against any other clustering implementation.

## Topic-name caveat

The seven `DEFAULT_PIPELINE_TOPICS` constants (`gstlal`, `mbta`, ...) are
the bare pipeline names, **not** the topic names on the wire. The actual
topic names on `kafka-dev.ligo.org` are namespaced by the GraceDB
instance: `gracedb-dev.gstlal`, `gracedb-test.gstlal`,
`gracedb.mbta`, and so on. Always pass the namespaced form on the
command line via `--topics`. The Python `GraceDbKafkaConsumer` client
derives the namespace from its `service_url` parameter; we make it
explicit here to avoid coupling the crate to any one GraceDB instance.

## Restart safety via Redis

When `--redis-url` is set, the clusterer:

1. Loads the persisted open-superevent state from
   `{prefix}:open` (hash) and `{prefix}:t0_index` (sorted set) at
   startup, restoring the in-memory `SupereventCreator` exactly as it
   was when the previous process stopped.
2. After every processed event, atomically replaces the stored state in
   a MULTI/EXEC pipeline so the hash and the sorted set never disagree.
3. Recovers the next id sequence number from the maximum suffix on the
   loaded ids, so newly opened superevents do not collide with any that
   were already on disk.

The state is small (a few hundred open windows in steady state), so
save-after-every-event is fine in the current implementation. A
checkpoint-on-interval mode is on the roadmap if profile data ever shows
the per-event overhead matters.

## Test

```bash
cargo test         # 25 boom-gw tests + 7 ligo-lw tests
```

The ligo-lw integration tests in `ligo-lw/tests/basic.rs` cover the
parser's behaviour on minimal hand-written fixtures. The boom-gw unit
tests cover envelope parsing, the GwEvent extraction path, the JWT
claim decode, the OAUTHBEARER context, the SupereventCreator policy
edge cases, the publisher's JSON shape, and the Redis state's
next-sequence recovery logic.

The end-to-end Redis path is exercised by the `gw_clusterer` binary
itself in replay mode against a local Redis (Homebrew's `redis-server`
or a Docker container will do).

## Comparison with sgn-llai

The clustering layer is a direct port of `sgn-llai`'s
`SupereventCreator` semantics, validated by replaying the same captured
event stream through both the Rust implementation and a faithful Python
re-implementation of the bisect-based algorithm. On a 25-event corpus
drawn from `gracedb-test`'s MBTA topic, the two outputs agree on every
event (16 superevent creations, 8 preferred-event updates, 1 lower-SNR
skip; by-event diff is empty). The Python harness used for that check
lives in this repository's `comparison/` directory once we add it; for
now, the BOOM-side JSONL produced by `gw_clusterer --out-jsonl` is the
canonical artefact for any future side-by-side diff.

## Roadmap

* Live publish-side OAUTHBEARER auth so the output Kafka cluster can be
  the same kind of LIGO-managed broker as the input.
* `--instance gracedb-test` flag that namespaces the default topic list
  automatically.
* A `gracedb-supervisor` companion binary that consumes the published
  superevent stream and writes events to GraceDB asynchronously,
  closing the "GraceDB as optional archival consumer" picture.
* MMA correlator binary that consumes the superevent topic alongside
  GRB notices, neutrino alerts, and optical alerts to produce
  multi-messenger associations (the RAVEN-equivalent role).
* Native LIGO_LW `Array` element parsing in `ligo-lw` so PSD payloads
  and sky-map metadata are available to downstream consumers.

## License

MIT.
