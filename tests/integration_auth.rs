//! End-to-end HTTP test for the boom-gw auth middleware.
//!
//! Marked `#[ignore]` because it needs a real MongoDB (we exercise
//! the full app stack with `auth_middleware` wrapped on top of the
//! API routes). Run alongside the other integration tests in the
//! `integration-kafka` GA job:
//!
//! ```sh
//! BOOM_GW_MONGO_URI=mongodb://localhost:27017 \
//!   cargo test --test integration_auth -- --ignored
//! ```
//!
//! Scenarios covered (each runs as its own actix app to avoid
//! state leak between scenarios):
//!
//! 1. **No Authorization header** → 401 on a protected route.
//! 2. **Valid token, public route** (`/api/health`) → 200 without
//!    a token (sanity-checks the public-route allowlist).
//! 3. **Valid HS256-signed token with `gracedb.read`** → 200 on GETs.
//! 4. **Token missing `gracedb.read`** → 401.
//! 5. **Expired token** → 401 even though the signature is valid.
//! 6. **Alert-publish allowlist: non-allowlisted principal** → 403
//!    even with a valid token.
//! 7. **Alert-publish allowlist: allowlisted principal** → 201.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use actix_web::body::MessageBody;
use actix_web::middleware::from_fn;
use actix_web::{test, web, App};
use boom_gw::{
    api, api::MaybeAlertPublisher, auth_middleware, Archive, ArchiveConfig, AuthConfig, JwksCache,
    Superevent,
};
use igwn_ligolw::CoincInspiralEvent;
use jsonwebtoken::{encode, Algorithm, DecodingKey, EncodingKey, Header};
use serde_json::{json, Value};

const TEST_ISSUER: &str = "https://test.cilogon.org/igwn";
const TEST_KID: &str = "test-key-1";
const TEST_SECRET: &[u8] = b"integration-auth-test-shared-secret";

fn mongo_uri() -> String {
    std::env::var("BOOM_GW_MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into())
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Mint an HS256-signed token. In production we use RS256 with the
/// CILogon JWKS; the validation code is algorithm-agnostic (it reads
/// the `alg` from the JWT header), so an HMAC test exercises the
/// same code path with one fewer fixture file.
fn mint_token(claims: Value) -> String {
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(TEST_KID.into());
    encode(&header, &claims, &EncodingKey::from_secret(TEST_SECRET)).unwrap()
}

/// Build a config with the test issuer/key + a single allowlisted
/// publisher. `alert_publishers` is empty so the "any authenticated
/// user can publish" branch is exercised; specific tests override.
fn baseline_auth(alert_publishers: Vec<&str>) -> AuthConfig {
    AuthConfig {
        issuers: vec![TEST_ISSUER.into()],
        audiences: vec!["ANY".into(), "boom-gw".into()],
        required_scope: "gracedb.read".into(),
        alert_publishers: alert_publishers.into_iter().map(String::from).collect(),
        dev_mode: false,
    }
}

async fn fresh_jwks() -> JwksCache {
    let jwks = JwksCache::new();
    jwks.insert_test_key(TEST_ISSUER, TEST_KID, DecodingKey::from_secret(TEST_SECRET))
        .await;
    jwks
}

async fn build_archive() -> (Archive, String) {
    let uri = mongo_uri();
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut cfg = ArchiveConfig::new(&uri);
    cfg.database = format!("boom_gw_auth_test_{pid}_{nanos}");
    let raw = mongodb::Client::with_uri_str(&uri).await.unwrap();
    raw.database(&cfg.database).drop().await.unwrap();
    let archive = Archive::connect(cfg.clone()).await.unwrap();
    (archive, cfg.database)
}

async fn drop_database(database: &str) {
    let client = mongodb::Client::with_uri_str(&mongo_uri()).await.unwrap();
    client.database(database).drop().await.unwrap();
}

fn dummy_event(graceid: &str) -> boom_gw::GwEvent {
    let coinc = CoincInspiralEvent {
        coinc_event_id: graceid.into(),
        ifos: "H1,L1".into(),
        combined_far: 1e-12,
        snr: 15.0,
        mass: None,
        mchirp: None,
        end_time: 1_400_000_000.0,
        sngls: vec![],
    };
    boom_gw::GwEvent {
        pipeline: "gstlal".into(),
        graceid: graceid.into(),
        producer_timestamp: 1_700_000_000.0,
        message_type: "new".into(),
        submitter: "ci".into(),
        end_time: 1_400_000_000.0,
        ifos: "H1,L1".into(),
        snr: 15.0,
        far: 1e-12,
        mchirp: None,
        total_mass: None,
        coinc,
    }
}

/// Build the actix app exactly as `run_server` does — with the auth
/// middleware mounted on top of the API routes. The helper takes
/// `archive` and `auth` so each test can pick its own allowlist /
/// principal set.
fn make_app(
    archive: Archive,
    auth: AuthConfig,
    jwks: JwksCache,
) -> App<
    impl actix_web::dev::ServiceFactory<
        actix_web::dev::ServiceRequest,
        Config = (),
        Response = actix_web::dev::ServiceResponse<impl MessageBody>,
        Error = actix_web::Error,
        InitError = (),
    >,
> {
    App::new()
        .app_data(web::Data::new(archive))
        .app_data(web::Data::new(MaybeAlertPublisher(None)))
        .app_data(web::Data::new(auth))
        .app_data(web::Data::new(jwks))
        .wrap(from_fn(auth_middleware))
        .configure(api::configure)
}

#[actix_web::test]
#[ignore]
async fn health_is_public() {
    let (archive, db_name) = build_archive().await;
    let app =
        test::init_service(make_app(archive, baseline_auth(vec![]), fresh_jwks().await)).await;

    let req = test::TestRequest::get().uri("/api/health").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);

    drop_database(&db_name).await;
}

#[actix_web::test]
#[ignore]
async fn missing_token_returns_401() {
    let (archive, db_name) = build_archive().await;
    let app =
        test::init_service(make_app(archive, baseline_auth(vec![]), fresh_jwks().await)).await;

    let req = test::TestRequest::get().uri("/api/events").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 401);

    drop_database(&db_name).await;
}

#[actix_web::test]
#[ignore]
async fn valid_token_grants_read_access() {
    let (archive, db_name) = build_archive().await;
    let app = test::init_service(make_app(
        archive.clone(),
        baseline_auth(vec![]),
        fresh_jwks().await,
    ))
    .await;

    let token = mint_token(json!({
        "iss": TEST_ISSUER,
        "sub": "michael.coughlin@ligo.org",
        "aud": "ANY",
        "scope": "gracedb.read read:/ligo",
        "exp": now_unix() + 3600,
    }));

    let req = test::TestRequest::get()
        .uri("/api/events")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    let body: Value = test::read_body_json(resp).await;
    assert_eq!(body["message"], "success");

    drop_database(&db_name).await;
}

#[actix_web::test]
#[ignore]
async fn token_without_required_scope_is_401() {
    let (archive, db_name) = build_archive().await;
    let app =
        test::init_service(make_app(archive, baseline_auth(vec![]), fresh_jwks().await)).await;

    // No `gracedb.read` in the scope list — only the WLCG-path-style
    // scopes that Michael's personal token also carries.
    let token = mint_token(json!({
        "iss": TEST_ISSUER,
        "sub": "michael.coughlin@ligo.org",
        "aud": "ANY",
        "scope": "read:/ligo read:/shared",
        "exp": now_unix() + 3600,
    }));

    let req = test::TestRequest::get()
        .uri("/api/events")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 401);

    drop_database(&db_name).await;
}

#[actix_web::test]
#[ignore]
async fn expired_token_is_401() {
    let (archive, db_name) = build_archive().await;
    let app =
        test::init_service(make_app(archive, baseline_auth(vec![]), fresh_jwks().await)).await;

    let token = mint_token(json!({
        "iss": TEST_ISSUER,
        "sub": "x",
        "aud": "ANY",
        "scope": "gracedb.read",
        "exp": now_unix() - 60,
    }));

    let req = test::TestRequest::get()
        .uri("/api/events")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 401);

    drop_database(&db_name).await;
}

#[actix_web::test]
#[ignore]
async fn alert_publish_allowlist_blocks_non_member() {
    let (archive, db_name) = build_archive().await;
    // Seed a superevent so the alert handler can find it.
    let ev = dummy_event("G_auth_test");
    archive.record_event(&ev).await.unwrap();
    let superevent_id = "S_auth_001".to_string();
    let s = Superevent {
        id: superevent_id.clone(),
        t_0: 1_400_000_000.0,
        t_start: 1_399_999_997.5,
        t_end: 1_400_000_002.5,
        preferred_event: ev.clone(),
        g_events: vec![ev],
        skymap: None,
    };
    archive.upsert_superevent(&s).await.unwrap();

    let app = test::init_service(make_app(
        archive,
        baseline_auth(vec!["boom-gw-clusterer@ligo.org"]),
        fresh_jwks().await,
    ))
    .await;

    // Authenticated as a human user — NOT on the allowlist.
    let token = mint_token(json!({
        "iss": TEST_ISSUER,
        "sub": "michael.coughlin@ligo.org",
        "aud": "ANY",
        "scope": "gracedb.read",
        "exp": now_unix() + 3600,
    }));

    let req = test::TestRequest::post()
        .uri(&format!("/api/superevents/{superevent_id}/alerts"))
        .insert_header(("Authorization", format!("Bearer {token}")))
        .set_json(json!({"alert_type": "PRELIMINARY", "dry_run": true}))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 403);

    drop_database(&db_name).await;
}

#[actix_web::test]
#[ignore]
async fn alert_publish_allowlist_permits_member() {
    let (archive, db_name) = build_archive().await;
    let ev = dummy_event("G_auth_test_2");
    archive.record_event(&ev).await.unwrap();
    let superevent_id = "S_auth_002".to_string();
    let s = Superevent {
        id: superevent_id.clone(),
        t_0: 1_400_000_000.0,
        t_start: 1_399_999_997.5,
        t_end: 1_400_000_002.5,
        preferred_event: ev.clone(),
        g_events: vec![ev],
        skymap: None,
    };
    archive.upsert_superevent(&s).await.unwrap();

    let mut auth = baseline_auth(vec![]);
    auth.alert_publishers = HashSet::from(["boom-gw-clusterer@ligo.org".to_string()]);
    let app = test::init_service(make_app(archive, auth, fresh_jwks().await)).await;

    let token = mint_token(json!({
        "iss": TEST_ISSUER,
        "sub": "boom-gw-clusterer@ligo.org",
        "aud": "ANY",
        "scope": "gracedb.read",
        "exp": now_unix() + 3600,
    }));

    let req = test::TestRequest::post()
        .uri(&format!("/api/superevents/{superevent_id}/alerts"))
        .insert_header(("Authorization", format!("Bearer {token}")))
        .set_json(json!({"alert_type": "PRELIMINARY", "dry_run": true}))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 201);
    let body: Value = test::read_body_json(resp).await;
    assert_eq!(body["data"]["audit"]["published"], false);
    assert_eq!(body["data"]["alert"]["alert_type"], "PRELIMINARY");

    drop_database(&db_name).await;
}
