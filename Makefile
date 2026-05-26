# Boom-GW dev-loop ergonomics.
#
# Modeled on SkyPortal's `make db_clear && make db_init && make run`
# + `make load_demo_data` workflow — the goal is that an operator
# coming back to the project after a few weeks can `make stack_up`,
# get a populated mongo + minio + gw-api + vite, and have something
# realistic to look at.
#
# Dev defaults baked into the recipes match docker-compose.yaml and
# the live process args (see `ps aux | grep gw_api`). Override on
# the command line if you have a different stack:
#     make run BOOM_GW_MONGO_URI=mongodb://...
#
# The recipes intentionally avoid `cargo test` — keep this file
# focused on dev-loop ergonomics. CI is the right place for tests.

SHELL := /bin/bash
.DEFAULT_GOAL := help

MONGO_URI ?= mongodb://mongoadmin:devpassword@localhost:27017/admin?authSource=admin
API_BIND ?= 127.0.0.1:8080
API_URL ?= http://127.0.0.1:8080
S3_BUCKET ?= boom-gw-skymaps
S3_ENDPOINT ?= http://127.0.0.1:9000
S3_ACCESS_KEY ?= boomgw
S3_SECRET_KEY ?= boomgwsecret
SKYMAP_STORAGE ?= s3
# Stable dev session secret so cookies survive `make run` restarts.
# Override in prod; in CI the binary auto-generates one when dev-mode
# is on and BOOM_GW_SESSION_SECRET is unset.
SESSION_SECRET ?= dev-only-session-secret-do-not-use-in-prod

CARGO ?= cargo

.PHONY: help
help:
	@echo "Boom-GW dev targets:"
	@echo "  make db_init         docker-compose up the data plane (mongo, valkey, kafka, minio)"
	@echo "  make db_clear        wipe the demo-managed mongo collections (uses load_demo_data --wipe-only)"
	@echo "  make db_down         docker-compose down (stops everything; volumes persist)"
	@echo "  make load_demo_data  wipe + reseed a comprehensive demo dataset"
	@echo "  make run             cargo run gw_api against the local dev stack"
	@echo "  make web             vite dev server for the React frontend (web/) on :5173"
	@echo "  make stack_up        db_init + load_demo_data, then prints the recommended terminal split"
	@echo "  make build           cargo build --release for every binary"
	@echo
	@echo "Override defaults by setting env vars, e.g. make run API_BIND=0.0.0.0:8080"

# ---------------- docker-compose data plane ----------------

# Bring up mongo + valkey + kafka + otel + prometheus + (with the
# s3 profile) MinIO, then bootstrap the `boom-gw-skymaps` bucket
# via `mc mb`. Without the bucket-create step, `gw_api` aborts at
# startup with `s3 bucket setup failed: head_bucket failed:
# dispatch failure` because the bucket doesn't exist yet. The
# `--ignore-existing` flag makes this idempotent so a second
# `make db_init` run is a no-op.
.PHONY: db_init
db_init:
	docker compose --profile s3 up -d mongo valkey broker minio
	@echo "Waiting for MinIO to accept connections..."
	@for i in $$(seq 1 30); do \
	  if curl -fsS http://localhost:9000/minio/health/ready > /dev/null 2>&1; then \
	    echo "MinIO ready"; break; \
	  fi; sleep 1; \
	done
	@docker run --rm --network host --entrypoint sh minio/mc:latest -c " \
	  mc alias set local http://localhost:9000 $(S3_ACCESS_KEY) $(S3_SECRET_KEY) > /dev/null && \
	  mc mb --ignore-existing local/$(S3_BUCKET) && \
	  echo 'Bucket $(S3_BUCKET) ready'"

.PHONY: db_down
db_down:
	docker compose --profile s3 --profile app down

# ---------------- demo data ----------------

# Wipe the demo-managed collections. Same env defaults the gw_api
# binary uses, so the wipe always targets the right database.
.PHONY: db_clear
db_clear:
	$(CARGO) run --bin load_demo_data -- \
	  --mongo-uri '$(MONGO_URI)' \
	  --skymap-storage $(SKYMAP_STORAGE) \
	  --s3-bucket $(S3_BUCKET) \
	  --s3-endpoint-url $(S3_ENDPOINT) \
	  --s3-access-key $(S3_ACCESS_KEY) \
	  --s3-secret-key $(S3_SECRET_KEY) \
	  --wipe-only

# Full reseed: wipes + writes the curated demo dataset (5
# superevents, GRBs, BOOM transients, FRB / neutrino alerts, an
# IceCube LVK Nu Track Search, cross-matches, annotations,
# alerts; with synthetic MOC FITS skymaps where appropriate).
#
# IMPORTANT: requires gw_api to be running at $(API_URL). All
# external-alert ingest (BOOM, FRB, neutrino, LVK search) goes
# through the live REST surface (POST /api/{boom,frb,neutrino}-
# alerts, POST /api/superevents/{id}/icecube-lvk-searches) so
# the loader exercises the same handlers operators do. Run
# `make run` in another terminal first.
.PHONY: load_demo_data
load_demo_data:
	$(CARGO) run --bin load_demo_data -- \
	  --mongo-uri '$(MONGO_URI)' \
	  --skymap-storage $(SKYMAP_STORAGE) \
	  --s3-bucket $(S3_BUCKET) \
	  --s3-endpoint-url $(S3_ENDPOINT) \
	  --s3-access-key $(S3_ACCESS_KEY) \
	  --s3-secret-key $(S3_SECRET_KEY) \
	  --api-url $(API_URL)

# ---------------- backend ----------------

# Run the HTTP API in dev mode. The flag set matches the currently
# running process: dev-mode auth so we don't need a CILogon JWKS
# fetch, S3 skymap storage pointed at the local MinIO.
.PHONY: run
run:
	BOOM_GW_API_AUTH_DEV_MODE=true \
	BOOM_GW_SESSION_SECRET='$(SESSION_SECRET)' \
	  $(CARGO) run --bin gw_api -- \
	  --mongo-uri '$(MONGO_URI)' \
	  --bind $(API_BIND) \
	  --skymap-storage $(SKYMAP_STORAGE) \
	  --s3-bucket $(S3_BUCKET) \
	  --s3-endpoint-url $(S3_ENDPOINT) \
	  --s3-access-key $(S3_ACCESS_KEY) \
	  --s3-secret-key $(S3_SECRET_KEY)

# ---------------- frontend ----------------

# Vite dev server (port 5173 by default). Proxies /api/* to the
# gw_api on :8080 — see web/vite.config.ts.
.PHONY: web
web:
	cd web && npm run dev

# ---------------- one-shot bring-up ----------------

# Bring up infra + print the recommended launch sequence. Make
# can't run interactive processes in parallel cleanly, so we
# leave the long-running `gw_api` + `vite` + `load_demo_data` to
# the operator's other terminals. The ordering matters now that
# `load_demo_data` POSTs through the live API: gw_api has to be
# up before the loader can run.
.PHONY: stack_up
stack_up: db_init
	@echo
	@echo "  Infra is up. Open three terminals and run:"
	@echo "    Terminal 2:  make run             # gw_api on $(API_BIND)"
	@echo "    Terminal 3:  make load_demo_data  # POSTs demo data to gw_api"
	@echo "    Terminal 4:  make web             # vite dev server on :5173"
	@echo
	@echo "  Then open http://localhost:5173"

# ---------------- builds ----------------

.PHONY: build
build:
	$(CARGO) build --release
