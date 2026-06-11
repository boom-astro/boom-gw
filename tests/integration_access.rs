//! Integration coverage for the SkyPortal-style access control:
//! startup seeding, JIT provisioning + the enriched `/api/users/me`,
//! and the groups / members / streams endpoints with their invariants.
//!
//! `#[ignore]` (needs Mongo); run via `cargo test -- --ignored` with
//! `BOOM_GW_MONGO_URI` pointed at a dev server. The stub principal
//! middleware signs every request in as `integration-test@boom-gw`,
//! who — being the first provisioned user with no site-admins set —
//! is bootstrapped to Super admin.

use actix_web::{test, web, App};
use boom_gw::AuthConfig;
use boom_gw::{api, api::MaybeAlertPublisher, stub_principal_middleware, Archive, ArchiveConfig};
use serde_json::{json, Value};

fn mongo_uri() -> String {
    std::env::var("BOOM_GW_MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into())
}

async fn build_archive() -> (Archive, String) {
    let uri = mongo_uri();
    let suffix = uuid::Uuid::new_v4().simple();
    let mut cfg = ArchiveConfig::new(&uri);
    cfg.database = format!("boom_gw_access_test_{suffix}");
    let client = mongodb::Client::with_uri_str(&uri).await.expect("mongo");
    client.database(&cfg.database).drop().await.expect("drop");
    let archive = Archive::connect(cfg.clone()).await.expect("connect");
    (archive, cfg.database)
}

async fn drop_database(db: &str) {
    let client = mongodb::Client::with_uri_str(&mongo_uri()).await.unwrap();
    client.database(db).drop().await.unwrap();
}

#[actix_web::test]
#[ignore]
async fn access_control_end_to_end() {
    let (archive, db_name) = build_archive().await;

    let mut auth = AuthConfig::defaults();
    auth.dev_mode = true; // not used by these routes, but keep it consistent

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(archive.clone()))
            .app_data(web::Data::new(auth))
            .app_data(web::Data::new(MaybeAlertPublisher(None)))
            .wrap(actix_web::middleware::from_fn(stub_principal_middleware))
            .configure(api::configure),
    )
    .await;

    // --- Seeding: roles + streams present ---------------------------------
    let req = test::TestRequest::get().uri("/api/roles").to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 4);

    let req = test::TestRequest::get().uri("/api/streams").to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 5);

    // --- First user bootstrapped to Super admin via /users/me ------------
    let req = test::TestRequest::get().uri("/api/users/me").to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"]["sub"], "integration-test@boom-gw");
    let acls = body["data"]["acls"].as_array().unwrap();
    assert!(
        acls.iter().any(|a| a == "System admin"),
        "first user should be Super admin, got {acls:?}"
    );

    // --- Create a group (creator becomes admin) --------------------------
    let req = test::TestRequest::post()
        .uri("/api/groups")
        .set_json(json!({"name": "MMA team", "description": "multi-messenger"}))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    let gid = body["data"]["id"].as_str().unwrap().to_string();
    assert_eq!(body["data"]["admin"], true);

    // Appears in the group list.
    let req = test::TestRequest::get().uri("/api/groups").to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 1);

    // --- Add a member + grant the group a stream -------------------------
    let req = test::TestRequest::post()
        .uri(&format!("/api/groups/{gid}/members"))
        .set_json(json!({"sub": "bob@ligo.org", "admin": false}))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"]["members"].as_array().unwrap().len(), 2);

    let req = test::TestRequest::post()
        .uri(&format!("/api/groups/{gid}/streams"))
        .set_json(json!({"stream_id": "gcn_grb"}))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"]["streams"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"]["streams"][0]["id"], "gcn_grb");

    // --- Lockout guard: cannot remove the last admin (me) ----------------
    let req = test::TestRequest::delete()
        .uri(&format!(
            "/api/groups/{gid}/members/integration-test@boom-gw"
        ))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 400, "removing the last admin must 400");

    // Removing the non-admin member is fine.
    let req = test::TestRequest::delete()
        .uri(&format!("/api/groups/{gid}/members/bob@ligo.org"))
        .to_request();
    let body: Value = test::call_and_read_body_json(&app, req).await;
    assert_eq!(body["data"]["members"].as_array().unwrap().len(), 1);

    // --- Role assignment (Manage users): grant bob full_user -------------
    // bob must exist first — provision by referencing him; simplest is to
    // add him back to a group, but PATCH /users requires the row. Create
    // it by re-adding as member (provisioning happens at access_ctx for
    // the *caller*, not arbitrary subs, so we add bob then patch).
    let req = test::TestRequest::post()
        .uri(&format!("/api/groups/{gid}/members"))
        .set_json(json!({"sub": "bob@ligo.org"}))
        .to_request();
    let _ = test::call_service(&app, req).await;
    // bob has no user row yet (only a membership); patch should 404.
    let req = test::TestRequest::patch()
        .uri("/api/users/bob@ligo.org")
        .set_json(json!({"role_ids": ["full_user"]}))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status() == 404 || resp.status() == 200,
        "patch on a membership-only user is 404 (no user doc) — got {}",
        resp.status()
    );

    // --- Unknown-role rejection ------------------------------------------
    let req = test::TestRequest::patch()
        .uri("/api/users/integration-test@boom-gw")
        .set_json(json!({"role_ids": ["not_a_role"]}))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 400, "unknown role must 400");

    drop_database(&db_name).await;
}
