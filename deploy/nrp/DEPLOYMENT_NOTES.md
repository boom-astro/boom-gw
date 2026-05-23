# Deployment notes & troubleshooting (NRP Nautilus)

## 1. Kafka data volume — `emptyDir`, not CephFS

The Kafka broker manifest in `k8s/ingestion/kafka.yaml` deliberately
backs `/var/lib/kafka/data` with an `emptyDir` rather than a CephFS
PVC. This mirrors the workaround documented in
[boom-deploy-nrp](https://github.com/boom-astro/boom-deploy-nrp/blob/main/DEPLOYMENT_NOTES.md#1-kafka-metadata-latency-the-5s-timeout-bug):

- CephFS metadata operations (creating directories and per-partition
  index files) can take 8-12 s under load.
- rdkafka's default `message.timeout.ms` is 5 s, so the producer
  hits its deadline before the broker finishes creating
  partitions on first publish — the publish errors out and the
  cluster never warms up.
- Backing the volume with `emptyDir` puts Kafka's data on local
  NVMe; partition creation drops to <100 ms.

Trade-off: if the broker pod restarts, the data is gone. boom-gw is
designed to tolerate this — Redis holds the open superevent
windows separately, and the durable archive lives in MongoDB. The
only thing lost across a broker restart is unconsumed
localize-request / localize-result / public-alert messages, which is
fine since the clusterer re-issues those when the next event arrives
on the upstream GraceDB topic.

## 2. SCITokens for GraceDB Kafka

The clusterer reads the seven LVK pipeline topics from
`kafka-dev.ligo.org:9092` using SASL/OAUTHBEARER with a SCITokens
bearer JWT. The token is held in the `BOOM_GW_SCITOKEN` field of
the `boom-gw-secrets` Secret and mounted into the clusterer pod at
`/run/secrets/scitoken`. The clusterer is started with
`--token-file /run/secrets/scitoken`.

To rotate the token:

```bash
htgettoken -a vault.ligo.org -i igwn   # produces /tmp/bt_u<uid>
# Update BOOM_GW_SCITOKEN in k8s/secrets.yaml with the new JWT
make deploy-secrets
# Force the clusterer pod to pick up the new token
kubectl rollout restart deployment/gw-clusterer -n umn-babamul
```

SCITokens expire after ~10 hours by default. For a long-lived
deployment, set up a CronJob that runs `htgettoken` against vault
and updates the Secret on a schedule — that piece is **not** in
this repo yet.

## 3. Image registry

The Rust binaries and the Python bayestar-service ship as two
separate images, each with its own `Dockerfile`:

- `ghcr.io/boom-astro/boom-gw:latest` — built from the repo root's
  `Dockerfile`. Contains `gw-clusterer`, `gw-api`, `gw-consumer`,
  `gw-dump`. Build:
  ```bash
  docker build -t ghcr.io/boom-astro/boom-gw:latest .
  ```
- `ghcr.io/boom-astro/boom-gw-bayestar:latest` — built from
  `bayestar-service/Dockerfile` (micromamba base, conda-forge
  `ligo.skymap`). Build from the `bayestar-service/` directory so
  the `src/` and `pyproject.toml` resolve correctly:
  ```bash
  docker build -t ghcr.io/boom-astro/boom-gw-bayestar:latest bayestar-service/
  ```
  Image is ~2-3 GB because of LALSuite; the build takes a few
  minutes the first time. The waveform HDF5 files needed for the
  `o2-uberbank` path are **not** baked in — mount them at
  `$LAL_DATA_PATH` as a PVC if you need that waveform.

Until either image is published to ghcr, NRP cannot pull them. As
a stopgap, the `bayestar-service` Deployment manifest sets
`BAYESTAR_STUB=1` so the canned-FITS path exercises the wiring
without LALSuite. The clusterer / API have no equivalent stub —
the operator needs to publish the Rust image first.

## 4. MongoDB sizing

The PVC starts at 10 Gi. Each ingested event with its full
`CoincInspiralEvent` row is ~5-30 KB; each superevent doc with its
FITS payload attached is ~100-800 KB. At O4 rates that comfortably
fits in 10 Gi for several months. If you intend to keep FITS
attached to every superevent forever, plan on bumping the PVC.

## 5. HA story (or lack thereof)

boom-gw scales via the Kafka-consumer-group mechanism documented in
`../../docs/deployment.md`. On NRP that translates to: bump the
`replicas` on the `gw-clusterer` Deployment, and rdkafka will
redistribute partitions on assignment. The localizer-result consumer
and the alert-publisher producer use per-process group IDs derived
from `$HOSTNAME` (the pod UID), so scaling clusterers does not
produce duplicate localize requests or duplicate alerts. There is no
leader election, no Redis-fenced lock, no DLQ — same trade-off as
boom-deploy-nrp.

## 6. API authentication

The gw-api Deployment validates `Authorization: Bearer <scitoken>`
on every request except `GET /api/health`. The policy mirrors
GraceDB's server (`gracedb/api/v2/auth.py`):

- **Issuer allowlist** (env `BOOM_GW_AUTH_ISSUERS`): defaults to
  `https://cilogon.org/igwn`, `https://test.cilogon.org/igwn`,
  `https://osdf.igwn.org/cit`. Override only if your deployment
  trusts a different IDP.
- **Audience** (env `BOOM_GW_AUTH_AUDIENCES`): defaults to
  `ANY,boom-gw`. Every IGWN token Michael Coughlin's environment
  mints today carries `aud="ANY"`, so the default accepts every
  IGWN user with an active token. Narrow it to `boom-gw` only once
  the LIGO IDP has been configured to mint a boom-gw-specific
  audience.
- **Required scope** (env `BOOM_GW_AUTH_SCOPE`): defaults to
  `gracedb.read`. GraceDB enforces this single scope on every
  endpoint; we do the same.
- **Alert-publisher allowlist** (env `BOOM_GW_ALERT_PUBLISHERS`):
  comma-separated `sub` claim values permitted to POST public
  alerts. The clusterer's service account belongs here; human
  users with personal tokens do not. **An empty list means "anyone
  authenticated can publish"** — the binary warns at startup.
  Populate via `boom-gw-secrets` (see `k8s/secrets.example.yaml`).
- **Dev mode** (env `BOOM_GW_API_AUTH_DEV_MODE=1`): skips signature
  validation while still enforcing iss/aud/exp/scope. Useful for
  ingress smoke tests when the CILogon JWKS endpoint is
  unreachable from the cluster. Never enable in production.

Token signatures are verified against the OIDC JWKS published by
each allowlisted issuer (`{iss}/.well-known/openid-configuration`
→ `jwks_uri` → keys). The cache is warmed at startup and refreshes
on `kid` cache misses; the TTL is one hour.

A typical end-user request from a developer machine:

```sh
# Once per ~10 h:
htgettoken -a vault.ligo.org -i igwn
TOKEN=$(cat /tmp/bt_u$(id -u))

curl -H "Authorization: Bearer $TOKEN" \
     https://boom-gw-api.nrp-nautilus.io/api/superevents
```

For service-to-service callers (e.g. the clusterer needing to
publish alerts), the principal needs a SCITokens credential whose
`sub` is on the alert-publisher allowlist. Operations: pick a
service account, register it with vault.ligo.org, add its
principal name to `BOOM_GW_ALERT_PUBLISHERS`, and have the
clusterer pod mount its credential alongside the existing
`BOOM_GW_SCITOKEN`.

## 7. Skymap storage backend (mongo vs S3)

The FITS sky maps don't live inline on the superevent docs — they
go to a [`SkymapStorage`](../../src/storage/skymap.rs) with two
backends. The choice is a Day-0 decision; both writer
(`gw-clusterer`) and reader (`gw-api`) must agree.

### Mongo backend (default)

```yaml
BOOM_GW_SKYMAP_STORAGE: mongo
```

FITS bytes are stored as native BSON Binary in a dedicated
`skymaps` collection on the same database as the rest of the
archive, keyed by `superevent_id`. WiredTiger compresses on disk;
the application does not bother. Mirror of what BOOM proper's
`MongoCutoutStorage` does for cutouts (`utils/cutouts.rs`).

* Pros: simplest operationally — one database to back up, no
  extra services, no extra credentials.
* Cons: bytes count against mongo's 16 MB document cap (real
  BAYESTAR FITS are ~800 KB so plenty of headroom for the
  foreseeable future). Storage and replication carry the FITS
  bytes on every mongo write.

This is the default in both Deployments and the right choice
unless you have a specific reason to go to S3.

### S3 backend

```yaml
BOOM_GW_SKYMAP_STORAGE: s3
BOOM_GW_S3_BUCKET:        boom-gw-skymaps          # required
BOOM_GW_S3_ENDPOINT_URL:  http://minio:9000        # required for in-cluster MinIO / rustfs / Wasabi
BOOM_GW_S3_ACCESS_KEY:    <credential>             # required
BOOM_GW_S3_SECRET_KEY:    <credential>             # required
BOOM_GW_S3_REGION:        us-east-1                # used by the AWS SDK signature; arbitrary for MinIO
BOOM_GW_S3_KEY_PREFIX:    boom-gw                  # objects land at {prefix}/skymaps/{id}.json
BOOM_GW_S3_CACHE_REDIS_URL: redis://valkey:6379/   # optional, in front of S3 reads
```

FITS bytes go to an S3-compatible object store at
`{key_prefix}/skymaps/{superevent_id}.json` (base64 inside a small
JSON envelope, optionally zstd-compressed). Mirrors BOOM proper's
`S3CutoutStorage`. The bucket is auto-created on startup via
`head_bucket` + `create_bucket` — no separate provisioning Job is
needed.

* Pros: separates blob growth from the mongo working set; lets
  multiple boom-gw instances share a single object store; tunable
  with bucket-level lifecycle policies (e.g. archive to Glacier
  after 90 days).
* Cons: one more service to operate; reads are slower than mongo
  (mitigated by the optional Valkey-backed `SkymapCache` —
  configured via `BOOM_GW_S3_CACHE_REDIS_URL`, defaults to a 30 s
  TTL). Real AWS S3 also costs money per GET; pin
  `BOOM_GW_S3_CACHE_REDIS_URL` if so.

### Three endpoint options on NRP

1. **In-cluster MinIO** — `make deploy-storage` brings up
   `k8s/storage/minio.yaml` with a 50 Gi CephFS-backed PVC. Set
   `BOOM_GW_S3_ENDPOINT_URL=http://minio:9000` in the Secret.
   Uses the same access/secret credentials as the application
   (MINIO_ROOT_USER/MINIO_ROOT_PASSWORD bind to
   BOOM_GW_S3_ACCESS_KEY/BOOM_GW_S3_SECRET_KEY).

2. **NRP Ceph RadosGW** — most NRP namespaces have access to the
   cluster's RadosGW S3 endpoint. Set
   `BOOM_GW_S3_ENDPOINT_URL=https://rook-ceph-rgw-...nrp-nautilus.io`
   and skip `k8s/storage/`.

3. **External AWS S3 / Wasabi / Backblaze** — leave
   `BOOM_GW_S3_ENDPOINT_URL` blank (AWS) or set to the provider's
   endpoint. Be careful about egress costs from NRP.

### Switching mid-deployment

Switching mongo → S3 (or vice versa) does **not** migrate
existing skymap data automatically. The clean path:

1. Drain the clusterer (`kubectl scale deploy/gw-clusterer --replicas=0`).
2. Update the Secret + Deployment envs for the new backend; redeploy.
3. Re-run `gw-clusterer` against the kafka retention window so it
   re-emits the localize requests and re-attaches sky maps to the
   new backend.

For a one-shot migration without re-localizing, write a small
script that reads from the old backend and upserts into the new
one — the `SkymapStorage` API is `upsert(SkymapBlob)` /
`get(superevent_id)`. We have not bundled one because the
operator probably wants to also rebuild the mongo summary
fields, which is repo-specific.

## 8. Resource limits

The defaults in the manifests are conservative — small enough to
fit on any NRP node, large enough that the stub-mode round trip
runs comfortably. Once real BAYESTAR is in the image, bump
`bayestar-service` to `4` CPU and `8` Gi memory minimum (a full
PSD-laden event can spike to ~3 GB resident).
