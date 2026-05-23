# boom-gw NRP Nautilus deployment

Kubernetes manifests for running boom-gw on the [NRP](https://nrp.ai/)
cluster. The layout deliberately mirrors
[boom-deploy-nrp](https://github.com/boom-astro/boom-deploy-nrp) so
the same operational mental model applies to both stacks.

## Deployment overview

The stack runs in the `umn-babamul` namespace alongside the BOOM
filter sandbox. Resources are labelled `stack=boom-gw` so they do
not collide with the BOOM proper resources in the same namespace.

- **API endpoint:** `https://boom-gw-api.nrp-nautilus.io`
- **Container image:** `ghcr.io/boom-astro/boom-gw:latest` — build
  from this repository's `Dockerfile` and push.
- **Bayestar microservice image:**
  `ghcr.io/boom-astro/boom-gw-bayestar:latest` (built from
  `bayestar-service/`). Until that image is published the
  manifest ships with `BAYESTAR_STUB=1` so the canned-FITS path
  exercises the wiring end-to-end.

## Manifest layout

| Folder / file               | Contents                                                                       | When to deploy                                                          |
|-----------------------------|--------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| `k8s/secrets.example.yaml`  | Template for the boom-gw Secret (mongo password, SCITokens JWT, S3 creds)      | One-time setup (or when rotating credentials) via `make deploy-secrets` |
| `k8s/core/`                 | MongoDB, gw-api Deployment + Service + Ingress                                 | Required — minimal stack for serving the archive over HTTP              |
| `k8s/ingestion/`            | Kafka broker, Valkey, gw-clusterer Deployment, bayestar-service Deployment     | When you want live ingestion + clustering + localization                |
| `k8s/storage/`              | In-cluster MinIO Deployment + PVC (50 Gi) + Service                            | Only when `BOOM_GW_SKYMAP_STORAGE=s3` and you want the bucket in-cluster |
| `k8s/observability/`        | OTel Collector + Prometheus + sidecar exporters (mongo / valkey / kafka)       | When you want metrics                                                   |

## Getting started

1. **Namespace.** Targets `umn-babamul` by default (override with
   `make deploy NS=other-namespace`).

2. **Secrets (one-time).** Copy the template, fill in real values,
   then apply explicitly:
   ```bash
   cp k8s/secrets.example.yaml k8s/secrets.yaml
   # edit k8s/secrets.yaml — supply mongo password and SCITokens JWT
   make deploy-secrets
   ```
   Re-run when credentials rotate. The other targets never apply
   secrets automatically.

3. **Deploy the minimum (mongo + API):**
   ```bash
   make deploy-core
   ```
   The API will come up serving the (empty) archive at the ingress
   host above.

4. **Add the ingestion stack** (Kafka + Valkey + clusterer +
   bayestar-service):
   ```bash
   make deploy-ingestion
   ```
   The clusterer reads from `kafka-dev.ligo.org:9092` over SASL +
   SCITokens; the JWT in the Secret is mounted to
   `/run/secrets/scitoken` and passed as `--token-file`.

5. **Add observability:**
   ```bash
   make deploy-observability
   ```

6. **(Optional) In-cluster S3 for skymap storage:**
   ```bash
   # Set BOOM_GW_SKYMAP_STORAGE=s3 + the BOOM_GW_S3_* fields in
   # secrets.yaml first, then:
   make deploy-storage
   ```
   See [DEPLOYMENT_NOTES.md §7](DEPLOYMENT_NOTES.md) for when to
   choose mongo vs s3 backend.

7. **Everything at once** (omits `storage` because that's opt-in):
   ```bash
   make deploy-all
   ```

8. **Status:** `make status` (`kubectl get pods,svc -n umn-babamul`).

### Tear-down

```bash
make delete-core            # tears down mongo + gw-api (PVC data lost)
make delete-ingestion       # tears down kafka + valkey + clusterer + bayestar
make delete-observability   # tears down otel + prometheus + exporters
```

### Without `make`

Each target is a thin wrapper around `kubectl apply -f`:

```bash
# secrets (one-time)
kubectl apply -n umn-babamul -f k8s/secrets.yaml

# core
kubectl apply -n umn-babamul -f k8s/core/mongodb.yaml
kubectl apply -n umn-babamul -f k8s/core/gw-api.yaml

# ingestion
kubectl apply -n umn-babamul -f k8s/ingestion/

# observability
kubectl apply -n umn-babamul -f k8s/observability/
```

_Note: `kubectl apply -f <dir>` does not recurse and picks up every
`.yaml` it sees — including the secrets template if you have not
renamed it. Don't `apply` the `k8s/` root._

## Key documentation

- [Deployment notes & troubleshooting](DEPLOYMENT_NOTES.md): the
  CephFS-vs-Kafka-latency gotcha and how this repo works around it.
- The boom-gw application stack is documented in
  [`../../docs/deployment.md`](../../docs/deployment.md): metric
  names, env var defaults, and the consumer-group HA pattern.
