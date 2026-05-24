#!/usr/bin/env bash
# Smoke test the gw-api + SPA bundle wiring against a running
# gw-api instance. Defaults assume the binary is bound at
# http://127.0.0.1:8090 with --static-dir web/dist.
#
# Exits non-zero on any failure. Doesn't mint a real JWT — the
# authenticated request path is covered by integration_auth.rs;
# this only verifies the static-serve + public-route plumbing
# that the integration tests can't exercise.

set -euo pipefail

BASE="${BASE:-http://127.0.0.1:8090}"

expect_status() {
  local url="$1" want="$2" desc="$3"
  local got
  got=$(curl -s -o /dev/null -w '%{http_code}' "$url")
  if [[ "$got" != "$want" ]]; then
    echo "FAIL $desc — $url returned $got, expected $want" >&2
    exit 1
  fi
  echo "OK   $desc — $url $got"
}

expect_contains() {
  local url="$1" needle="$2" desc="$3"
  if ! curl -fsS "$url" | grep -q "$needle"; then
    echo "FAIL $desc — $url did not contain '$needle'" >&2
    exit 1
  fi
  echo "OK   $desc — $url contains '$needle'"
}

# 1. Public route works without auth.
expect_status "$BASE/api/health" 200 "GET /api/health (public)"

# 2. /api/* routes outside the public list require auth.
expect_status "$BASE/api/superevents" 401 "GET /api/superevents w/o token"

# 3. SPA index is served when --static-dir is set.
expect_status "$BASE/" 200 "GET / (SPA index)"
expect_contains "$BASE/" "<div id=\"root\">" "SPA index has root div"

# 4. SPA deep links fall back to index.html (so reloads work).
expect_status "$BASE/superevents/S250101a" 200 "GET /superevents/<id> (deep link)"
expect_contains "$BASE/superevents/S250101a" "<div id=\"root\">" "deep link serves index"

# 5. Hashed Vite asset exists.
asset=$(curl -fsS "$BASE/" | grep -oE 'assets/index-[A-Za-z0-9]+\.js' | head -1)
if [[ -z "$asset" ]]; then
  echo "FAIL no hashed bundle reference in /" >&2
  exit 1
fi
expect_status "$BASE/$asset" 200 "GET /$asset (Vite bundle)"

echo
echo "All smoke checks passed."
