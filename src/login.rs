//! HTTP handlers for the session-cookie login flow.
//!
//! Three routes today:
//!
//! * `GET /api/auth/me` — current principal, populated by the auth
//!   middleware from either a Bearer token or a session cookie.
//! * `POST /api/auth/logout` — clears the session cookie.
//! * `POST /api/auth/dev-login` — dev-mode only: mint a session for
//!   an arbitrary `sub`. The real CILogon login + callback land
//!   here in a follow-up.
//!
//! Each handler is intentionally tiny — the heavy lifting lives in
//! [`crate::session`]. This module is the actix-glue.

use actix_web::{web, HttpMessage, HttpRequest, HttpResponse, Responder};
use serde::Deserialize;
use serde_json::json;
use tracing::info;

use crate::archive::Archive;
use crate::auth::{AuthConfig, Principal, DEFAULT_REQUIRED_SCOPE};
use crate::oidc::OidcConfig;
use crate::session::{build_session_cookie, clear_session_cookie, mint_session, SessionConfig};

#[derive(Debug, Deserialize)]
pub struct DevLoginBody {
    /// Principal `sub` to mint a session for. Free-form; matches
    /// what CILogon would produce (`"cough052@ligo.org"` etc).
    pub sub: String,
}

/// Mint a session cookie for an arbitrary `sub`. Only enabled when
/// `BOOM_GW_API_AUTH_DEV_MODE=1` — guarded inside the handler so
/// turning dev-mode off in prod automatically disables it without
/// the operator having to remove the route.
pub async fn dev_login(
    auth: web::Data<AuthConfig>,
    archive: web::Data<Archive>,
    session: web::Data<SessionConfig>,
    body: web::Json<DevLoginBody>,
) -> impl Responder {
    if !auth.dev_mode {
        return HttpResponse::NotFound().json(json!({
            "message": "dev login disabled",
            "data": null,
        }));
    }
    let principal = Principal {
        sub: body.sub.clone(),
        iss: "dev-login".into(),
        scopes: vec![DEFAULT_REQUIRED_SCOPE.to_string()],
    };
    // JIT-provision so the access-control layer has a user row (and the
    // site-admin / first-user bootstrap applies).
    if let Err(e) =
        crate::access::provision_user(&archive, &auth.site_admins, &principal.sub, None, None).await
    {
        return HttpResponse::InternalServerError().json(json!({
            "message": format!("provision failed: {e}"),
            "data": null,
        }));
    }
    let token = match mint_session(&session, &principal) {
        Ok(t) => t,
        Err(e) => {
            return HttpResponse::InternalServerError().json(json!({
                "message": format!("mint failed: {e}"),
                "data": null,
            }))
        }
    };
    info!(sub = %principal.sub, "dev-login: minted session");
    HttpResponse::Ok()
        .cookie(build_session_cookie(token, &session))
        .json(json!({
            "message": "success",
            "data": me_body(&principal),
        }))
}

/// Return the current principal, or `data: null` when the caller
/// is anonymous. Always 200 — the SPA hydrates from this on mount,
/// and a 401 here would pollute logs (and burn through 401
/// retry/auto-redirect interceptors) for every signed-out
/// pageload.
pub async fn me(req: HttpRequest) -> impl Responder {
    let principal = req.extensions().get::<Principal>().cloned();
    let data = principal
        .as_ref()
        .map(me_body)
        .unwrap_or(serde_json::Value::Null);
    HttpResponse::Ok().json(json!({
        "message": "success",
        "data": data,
    }))
}

/// Clear the session cookie. Always 200 even if there was no
/// session, so the SPA logout button is idempotent.
pub async fn logout(session: web::Data<SessionConfig>) -> impl Responder {
    HttpResponse::Ok()
        .cookie(clear_session_cookie(&session))
        .json(json!({
            "message": "success",
            "data": null,
        }))
}

fn me_body(p: &Principal) -> serde_json::Value {
    json!({
        "sub": p.sub,
        "iss": p.iss,
        "scopes": p.scopes,
    })
}

/// Public config endpoint — the SPA reads this on the login page to
/// decide which sign-in path to show.
pub async fn config(
    auth: web::Data<AuthConfig>,
    oidc: Option<web::Data<OidcConfig>>,
) -> impl Responder {
    HttpResponse::Ok().json(json!({
        "message": "success",
        "data": {
            "dev_mode": auth.dev_mode,
            "oidc_enabled": oidc.is_some(),
            "oidc_login_url": oidc.as_ref().map(|_| "/api/auth/login"),
        },
    }))
}
