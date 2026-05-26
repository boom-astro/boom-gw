//! SCITokens bearer-token authentication for the boom-gw HTTP API.
//!
//! Mirrors GraceDB's policy as closely as practical
//! (`/Users/mcoughlin/Code/LIGO/server/gracedb/api/v2/auth.py`):
//!
//! * **Issuer allowlist** — only tokens minted by configured issuers
//!   (default: prod / test CILogon + OSDF IGWN) are accepted.
//! * **Audience overlap** — token `aud` must overlap with the
//!   configured audience list (default `["ANY", "boom-gw"]`).
//! * **Scope** — token must carry a configured scope (default
//!   `gracedb.read`). GraceDB enforces a single scope across every
//!   endpoint; we do the same and reserve per-action authorization
//!   for an in-process allowlist on sensitive endpoints.
//! * **Alert-publish allowlist** — the one bit of per-endpoint
//!   authorization. The clusterer is permitted to publish public
//!   alerts; humans are not, unless explicitly listed. The list lives
//!   in config (`BOOM_GW_ALERT_PUBLISHERS`).
//! * **Public routes** — `/api/health`, plus everything outside the
//!   `/api/*` scope so the static SPA bundle (when served by gw-api
//!   via `--static-dir`) can be fetched unauthenticated.
//!
//! Validation is full: signature against the issuer's JWKS (fetched
//! via OIDC `.well-known/openid-configuration`, cached), `iss`, `aud`,
//! `exp`, `nbf`, and `scope` all enforced. A development-only escape
//! hatch (`BOOM_GW_API_AUTH_DEV_MODE=1`) skips the signature check so
//! local smoke tests can use unsigned JWTs.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use actix_web::body::{BoxBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::{Error, HttpMessage, HttpResponse};
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Default audience list. The literal `"ANY"` is the SCITokens 2.0
/// sentinel meaning "any audience accepts this," which is what every
/// IGWN-issued user token carries today. We also accept `"boom-gw"`
/// for forward-compat with audience-narrowed tokens minted
/// specifically for us.
pub const DEFAULT_AUDIENCES: &[&str] = &["ANY", "boom-gw"];

/// Default issuer allowlist, matching GraceDB's
/// `SCITOKEN_ISSUERS` default.
pub const DEFAULT_ISSUERS: &[&str] = &[
    "https://cilogon.org/igwn",
    "https://test.cilogon.org/igwn",
    "https://osdf.igwn.org/cit",
];

/// Default required scope, matching GraceDB's `SCITOKEN_SCOPE`.
/// GraceDB enforces this single scope on every endpoint regardless of
/// HTTP method, so we do the same.
pub const DEFAULT_REQUIRED_SCOPE: &str = "gracedb.read";

/// How long the JWKS cache trusts a fetched key set before
/// re-fetching. CILogon documents short-lived rotation, so an hour is
/// the upper bound; we re-fetch on demand when a kid miss happens.
const JWKS_CACHE_TTL: Duration = Duration::from_secs(3600);

/// Identity extracted from a validated token and attached to the
/// actix request via `req.extensions().insert(Principal(...))`.
#[derive(Debug, Clone)]
pub struct Principal {
    /// `sub` claim — typically `"michael.coughlin@ligo.org"`.
    pub sub: String,
    /// `iss` claim — which IDP minted the token.
    pub iss: String,
    /// All scopes the token carries, parsed from the space-separated
    /// `scope` claim (or array if the issuer used the alternate form).
    pub scopes: Vec<String>,
}

impl Principal {
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope)
    }
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub issuers: Vec<String>,
    pub audiences: Vec<String>,
    pub required_scope: String,
    /// Principals (`sub` values) allowed to POST to
    /// `/api/superevents/{id}/alerts`. Anything else with a valid
    /// token can still read and annotate.
    pub alert_publishers: HashSet<String>,
    /// When `true`, skip signature validation and trust the JWT
    /// body. `iss`/`aud`/`exp`/`scope` are still checked. Intended
    /// for local development and integration tests where JWKS is
    /// unreachable.
    pub dev_mode: bool,
}

impl AuthConfig {
    pub fn defaults() -> Self {
        Self {
            issuers: DEFAULT_ISSUERS.iter().map(|s| s.to_string()).collect(),
            audiences: DEFAULT_AUDIENCES.iter().map(|s| s.to_string()).collect(),
            required_scope: DEFAULT_REQUIRED_SCOPE.to_string(),
            alert_publishers: HashSet::new(),
            dev_mode: false,
        }
    }
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing Authorization header")]
    Missing,
    #[error("malformed Authorization header: {0}")]
    Malformed(&'static str),
    #[error("jwt decode failed: {0}")]
    Decode(#[from] jsonwebtoken::errors::Error),
    #[error("issuer {0:?} is not in the allowlist")]
    UnknownIssuer(String),
    #[error("audience {actual:?} does not overlap with {allowed:?}")]
    AudienceMismatch {
        actual: Vec<String>,
        allowed: Vec<String>,
    },
    #[error("required scope {required:?} not present (token scopes: {present:?})")]
    MissingScope {
        required: String,
        present: Vec<String>,
    },
    #[error("token expired")]
    Expired,
    #[error("token not yet valid (nbf)")]
    NotYetValid,
    #[error("no signing key for kid={0:?}")]
    NoKey(Option<String>),
    #[error("jwks fetch failed for {issuer}: {source}")]
    Jwks {
        issuer: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Full claim set we parse out of the JWT. Field names match
/// SCITokens 2.0 / WLCG conventions.
#[derive(Debug, Clone, Deserialize)]
struct Claims {
    pub iss: String,
    #[serde(default)]
    pub sub: Option<String>,
    /// Audience can be a single string OR an array of strings per
    /// JWT spec. CILogon's IGWN tokens currently emit a string (the
    /// literal `"ANY"`), but we tolerate both forms.
    #[serde(default)]
    pub aud: Option<StringOrVec>,
    #[serde(default)]
    pub scope: Option<String>,
    pub exp: i64,
    #[serde(default)]
    pub nbf: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum StringOrVec {
    One(String),
    Many(Vec<String>),
}

impl StringOrVec {
    fn into_vec(self) -> Vec<String> {
        match self {
            StringOrVec::One(s) => vec![s],
            StringOrVec::Many(v) => v,
        }
    }
}

/// Per-issuer JWKS entry — the parsed keys plus the time we last
/// fetched them.
#[derive(Clone)]
struct JwksEntry {
    keys: HashMap<String, DecodingKey>,
    fetched_at: SystemTime,
}

/// Cache of JWKS keyed by issuer URL. Cheap to clone — wraps an
/// `Arc<RwLock<...>>`.
#[derive(Clone, Default)]
pub struct JwksCache {
    inner: Arc<RwLock<HashMap<String, JwksEntry>>>,
}

impl JwksCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-warm the cache. Called once at startup with every issuer
    /// in the allowlist so the first inbound request doesn't pay a
    /// JWKS fetch on the hot path.
    pub async fn warm(&self, issuers: &[String]) -> Result<(), AuthError> {
        for iss in issuers {
            self.refresh(iss).await?;
        }
        Ok(())
    }

    /// Fetch (or re-fetch) the JWKS for `issuer` and replace the
    /// cached entry. OIDC discovery: GET `{iss}/.well-known/openid-configuration`,
    /// pull `jwks_uri`, GET that, parse `keys`.
    pub async fn refresh(&self, issuer: &str) -> Result<(), AuthError> {
        let discovery_url = format!("{}/.well-known/openid-configuration", issuer);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| AuthError::Jwks {
                issuer: issuer.into(),
                source: Box::new(e),
            })?;
        let discovery: OidcDiscovery = client
            .get(&discovery_url)
            .send()
            .await
            .map_err(|e| AuthError::Jwks {
                issuer: issuer.into(),
                source: Box::new(e),
            })?
            .error_for_status()
            .map_err(|e| AuthError::Jwks {
                issuer: issuer.into(),
                source: Box::new(e),
            })?
            .json()
            .await
            .map_err(|e| AuthError::Jwks {
                issuer: issuer.into(),
                source: Box::new(e),
            })?;
        let jwks: Jwks = client
            .get(&discovery.jwks_uri)
            .send()
            .await
            .map_err(|e| AuthError::Jwks {
                issuer: issuer.into(),
                source: Box::new(e),
            })?
            .error_for_status()
            .map_err(|e| AuthError::Jwks {
                issuer: issuer.into(),
                source: Box::new(e),
            })?
            .json()
            .await
            .map_err(|e| AuthError::Jwks {
                issuer: issuer.into(),
                source: Box::new(e),
            })?;

        let mut keys = HashMap::new();
        for jwk in jwks.keys {
            if let (Some(kid), Some(n), Some(e)) = (jwk.kid, jwk.n, jwk.e) {
                if let Ok(key) = DecodingKey::from_rsa_components(&n, &e) {
                    keys.insert(kid, key);
                }
            }
        }
        debug!(issuer, key_count = keys.len(), "refreshed JWKS for issuer");

        let mut w = self.inner.write().await;
        w.insert(
            issuer.to_string(),
            JwksEntry {
                keys,
                fetched_at: SystemTime::now(),
            },
        );
        Ok(())
    }

    /// Directly install a key for `(issuer, kid)`. Used by tests
    /// (both unit and integration) to avoid running a real HTTP
    /// server. The naming carries the warning — production code
    /// uses [`Self::refresh`] / [`Self::warm`].
    pub async fn insert_test_key(&self, issuer: &str, kid: &str, key: DecodingKey) {
        let mut w = self.inner.write().await;
        let entry = w.entry(issuer.to_string()).or_insert_with(|| JwksEntry {
            keys: HashMap::new(),
            fetched_at: SystemTime::now(),
        });
        entry.keys.insert(kid.to_string(), key);
    }

    pub(crate) async fn get(&self, issuer: &str, kid: &Option<String>) -> Option<DecodingKey> {
        let r = self.inner.read().await;
        let entry = r.get(issuer)?;
        match kid {
            Some(k) => entry.keys.get(k).cloned(),
            // A token without a `kid` only matches if the issuer
            // happens to publish exactly one key.
            None => {
                if entry.keys.len() == 1 {
                    entry.keys.values().next().cloned()
                } else {
                    None
                }
            }
        }
    }

    async fn is_stale(&self, issuer: &str) -> bool {
        let r = self.inner.read().await;
        match r.get(issuer) {
            Some(entry) => entry
                .fetched_at
                .elapsed()
                .map(|d| d > JWKS_CACHE_TTL)
                .unwrap_or(true),
            None => true,
        }
    }
}

#[derive(Deserialize)]
struct OidcDiscovery {
    jwks_uri: String,
}

#[derive(Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Deserialize)]
struct Jwk {
    #[serde(default)]
    kid: Option<String>,
    #[serde(default)]
    n: Option<String>,
    #[serde(default)]
    e: Option<String>,
}

/// Validate a bearer token against the configured policy. Returns
/// the extracted [`Principal`] on success.
pub async fn validate_token(
    token: &str,
    config: &AuthConfig,
    jwks: &JwksCache,
) -> Result<Principal, AuthError> {
    let header = decode_header(token)?;

    // Decode-only pass to read claims for iss/aud/scope checks. We
    // do *not* trust these yet — signature is verified below before
    // we accept the principal.
    let unverified: Claims = {
        let mut v = Validation::new(header.alg);
        v.insecure_disable_signature_validation();
        // We will check exp / nbf / aud / iss ourselves.
        v.validate_exp = false;
        v.validate_nbf = false;
        v.validate_aud = false;
        v.required_spec_claims = HashSet::new();
        decode::<Claims>(token, &DecodingKey::from_secret(&[]), &v)?.claims
    };

    if !config.issuers.iter().any(|i| i == &unverified.iss) {
        return Err(AuthError::UnknownIssuer(unverified.iss.clone()));
    }

    let token_aud: Vec<String> = unverified
        .aud
        .clone()
        .map(StringOrVec::into_vec)
        .unwrap_or_default();
    let aud_overlaps = token_aud
        .iter()
        .any(|t| config.audiences.iter().any(|a| a == t));
    if !aud_overlaps {
        return Err(AuthError::AudienceMismatch {
            actual: token_aud,
            allowed: config.audiences.clone(),
        });
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    if unverified.exp < now {
        return Err(AuthError::Expired);
    }
    if let Some(nbf) = unverified.nbf {
        if nbf > now {
            return Err(AuthError::NotYetValid);
        }
    }

    let scopes: Vec<String> = unverified
        .scope
        .as_deref()
        .unwrap_or("")
        .split_ascii_whitespace()
        .map(|s| s.to_string())
        .collect();
    if !scopes.iter().any(|s| s == &config.required_scope) {
        return Err(AuthError::MissingScope {
            required: config.required_scope.clone(),
            present: scopes,
        });
    }

    if !config.dev_mode {
        if jwks.is_stale(&unverified.iss).await {
            // Best-effort refresh on staleness; a failure here only
            // matters if no key currently matches.
            let _ = jwks.refresh(&unverified.iss).await;
        }
        let key = match jwks.get(&unverified.iss, &header.kid).await {
            Some(k) => k,
            None => {
                // Kid miss — refresh once and retry. Handles the case
                // where the issuer rotated keys mid-cache-window.
                jwks.refresh(&unverified.iss).await?;
                jwks.get(&unverified.iss, &header.kid)
                    .await
                    .ok_or_else(|| AuthError::NoKey(header.kid.clone()))?
            }
        };
        let mut validation = Validation::new(header.alg);
        validation.validate_exp = false;
        validation.validate_aud = false;
        validation.required_spec_claims = HashSet::new();
        // The decode call below performs the signature check. We
        // ignore its claim re-parsing since we already have the
        // unverified ones in scope.
        decode::<Claims>(token, &key, &validation)?;
    }

    Ok(Principal {
        sub: unverified.sub.unwrap_or_else(|| "anonymous".to_string()),
        iss: unverified.iss,
        scopes,
    })
}

/// Actix middleware. Opportunistically extracts the caller's
/// [`Principal`] from either an `Authorization: Bearer` header or
/// the `boom_gw_session` cookie, and inserts it into request
/// extensions for downstream handlers to consume.
///
/// **Anonymous access is allowed by default.** This mirrors
/// GraceDB's posture: most reads are public, and the handlers that
/// need an authenticated caller gate themselves with
/// [`require_principal`]. The middleware only returns 401 when a
/// credential *is* present but fails validation — letting bad
/// credentials silently pass would let an expired/tampered cookie
/// look identical to anonymous browsing.
///
/// Bearer wins over cookie when both are present: an explicit
/// CLI/curl token should never be ambushed by a stale browser
/// cookie sharing the same UA.
///
/// Returns `ServiceResponse<BoxBody>` so both the short-circuit
/// (401) path and the downstream-handler path produce a uniform
/// response type.
pub async fn auth_middleware<B: MessageBody + 'static>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let config = req.app_data::<actix_web::web::Data<AuthConfig>>().cloned();
    let jwks = req.app_data::<actix_web::web::Data<JwksCache>>().cloned();
    let session_cfg = req
        .app_data::<actix_web::web::Data<crate::session::SessionConfig>>()
        .cloned();
    let (config, jwks) = match (config, jwks) {
        (Some(c), Some(j)) => (c, j),
        _ => {
            warn!("auth middleware mounted without AuthConfig + JwksCache app_data");
            // Without config we cannot validate anything; let the
            // request through anonymously. Private handlers will
            // still 401 via require_principal.
            return next.call(req).await.map(|r| r.map_into_boxed_body());
        }
    };

    let bearer = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer ").map(|t| t.trim().to_string()));

    if let Some(token) = bearer {
        return match validate_token(&token, config.get_ref(), jwks.get_ref()).await {
            Ok(principal) => {
                req.extensions_mut().insert(principal);
                next.call(req).await.map(|r| r.map_into_boxed_body())
            }
            Err(e) => {
                debug!("auth rejected (bearer): {e}");
                Ok(req.into_response(unauthorized(&e.to_string())))
            }
        };
    }

    if let (Some(session_cfg), Some(token)) = (session_cfg, crate::session::cookie_token(&req)) {
        return match crate::session::verify_session(session_cfg.get_ref(), &token) {
            Ok(principal) => {
                req.extensions_mut().insert(principal);
                next.call(req).await.map(|r| r.map_into_boxed_body())
            }
            Err(e) => {
                debug!("auth rejected (cookie): {e}");
                Ok(req.into_response(unauthorized(&e.to_string())))
            }
        };
    }

    // No credential at all — anonymous. Let private handlers do
    // their own require_principal check.
    next.call(req).await.map(|r| r.map_into_boxed_body())
}

/// Reject the request with 401 unless the caller authenticated
/// successfully. Use this at the top of any handler that should
/// require a signed-in principal — most writes, plus the few reads
/// that surface non-public data (raw G-events, audit logs).
///
/// Returns `Some(401)` when the caller is anonymous; `None` when a
/// [`Principal`] is in the request extensions (i.e. the middleware
/// validated a Bearer or session cookie).
pub fn require_principal(req: &actix_web::HttpRequest) -> Option<HttpResponse> {
    if req.extensions().get::<Principal>().is_none() {
        Some(unauthorized("sign in required"))
    } else {
        None
    }
}

/// Test-only middleware that drops a synthetic [`Principal`] into
/// every request's extensions, so handlers gated by
/// [`require_principal`] behave as if the caller signed in.
///
/// Wire into an integration-test app with
/// `App::new().wrap(actix_web::middleware::from_fn(stub_principal_middleware))`
/// instead of the real [`auth_middleware`] when the test only cares
/// about the data-flow path (the auth-policy path has its own
/// dedicated test in `tests/integration_auth.rs`).
pub async fn stub_principal_middleware<B: MessageBody + 'static>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    req.extensions_mut().insert(Principal {
        sub: "integration-test@boom-gw".into(),
        iss: "integration-test".into(),
        scopes: vec![DEFAULT_REQUIRED_SCOPE.to_string()],
    });
    next.call(req).await.map(|r| r.map_into_boxed_body())
}

fn unauthorized(detail: &str) -> HttpResponse {
    HttpResponse::Unauthorized().json(json!({
        "message": format!("unauthorized: {detail}"),
        "data": null,
    }))
}

/// Forbidden response (403) used by the alert-publisher allowlist
/// check. Distinct from 401 — the caller authenticated successfully
/// but is not on the allowlist for this operation.
pub fn forbidden(detail: &str) -> HttpResponse {
    HttpResponse::Forbidden().json(json!({
        "message": format!("forbidden: {detail}"),
        "data": null,
    }))
}

/// Helper for handlers that need to gate on the alert-publisher
/// allowlist. Pulls the principal out of request extensions and
/// returns `Some(response)` if the caller is not permitted, or
/// `None` if they may proceed.
pub fn require_alert_publisher(
    req: &actix_web::HttpRequest,
    config: &AuthConfig,
) -> Option<HttpResponse> {
    let principal = req.extensions().get::<Principal>().cloned();
    let sub = principal.as_ref().map(|p| p.sub.as_str()).unwrap_or("");
    if config.alert_publishers.is_empty() || config.alert_publishers.contains(sub) {
        // Empty allowlist = anyone authenticated can publish (dev
        // default). Otherwise enforce membership.
        if config.alert_publishers.is_empty() {
            debug!(
                principal = sub,
                "alert publisher allowlist empty; permitting any authenticated principal"
            );
        }
        None
    } else {
        Some(forbidden(&format!(
            "principal {sub:?} is not on the alert-publisher allowlist"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

    /// Mint a test token and the matching DecodingKey, both signed
    /// with HS256 against a fixed secret. We use a symmetric key for
    /// tests so we don't need to ship an RSA fixture; production uses
    /// RS256 via the JWKS path.
    fn make_token(claims: serde_json::Value, kid: &str) -> (String, DecodingKey) {
        let secret = b"unit-test-shared-secret";
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(kid.into());
        let token = encode(&header, &claims, &EncodingKey::from_secret(secret)).unwrap();
        (token, DecodingKey::from_secret(secret))
    }

    fn baseline_config() -> AuthConfig {
        AuthConfig {
            issuers: vec!["https://cilogon.org/igwn".into()],
            audiences: vec!["ANY".into(), "boom-gw".into()],
            required_scope: "gracedb.read".into(),
            alert_publishers: HashSet::new(),
            dev_mode: false,
        }
    }

    #[tokio::test]
    async fn validates_a_good_token() {
        let exp = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64)
            + 3600;
        let claims = json!({
            "iss": "https://cilogon.org/igwn",
            "sub": "michael.coughlin@ligo.org",
            "aud": "ANY",
            "scope": "gracedb.read read:/ligo",
            "exp": exp,
        });
        let (token, key) = make_token(claims, "test-key-1");
        let jwks = JwksCache::new();
        jwks.insert_test_key("https://cilogon.org/igwn", "test-key-1", key)
            .await;
        let principal = validate_token(&token, &baseline_config(), &jwks)
            .await
            .expect("valid token");
        assert_eq!(principal.sub, "michael.coughlin@ligo.org");
        assert!(principal.has_scope("gracedb.read"));
    }

    #[tokio::test]
    async fn rejects_unknown_issuer() {
        let exp = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64)
            + 3600;
        let claims = json!({
            "iss": "https://impostor.example.com/",
            "sub": "x",
            "aud": "ANY",
            "scope": "gracedb.read",
            "exp": exp,
        });
        let (token, key) = make_token(claims, "k");
        let jwks = JwksCache::new();
        jwks.insert_test_key("https://impostor.example.com/", "k", key)
            .await;
        let err = validate_token(&token, &baseline_config(), &jwks)
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::UnknownIssuer(_)));
    }

    #[tokio::test]
    async fn rejects_audience_mismatch() {
        let exp = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64)
            + 3600;
        let claims = json!({
            "iss": "https://cilogon.org/igwn",
            "sub": "x",
            "aud": "https://gracedb.ligo.org/api/",
            "scope": "gracedb.read",
            "exp": exp,
        });
        let (token, key) = make_token(claims, "k");
        let jwks = JwksCache::new();
        jwks.insert_test_key("https://cilogon.org/igwn", "k", key)
            .await;
        let err = validate_token(&token, &baseline_config(), &jwks)
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::AudienceMismatch { .. }));
    }

    #[tokio::test]
    async fn rejects_expired_token() {
        let exp = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64)
            - 60;
        let claims = json!({
            "iss": "https://cilogon.org/igwn",
            "sub": "x",
            "aud": "ANY",
            "scope": "gracedb.read",
            "exp": exp,
        });
        let (token, key) = make_token(claims, "k");
        let jwks = JwksCache::new();
        jwks.insert_test_key("https://cilogon.org/igwn", "k", key)
            .await;
        let err = validate_token(&token, &baseline_config(), &jwks)
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::Expired));
    }

    #[tokio::test]
    async fn rejects_missing_scope() {
        let exp = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64)
            + 3600;
        let claims = json!({
            "iss": "https://cilogon.org/igwn",
            "sub": "x",
            "aud": "ANY",
            "scope": "read:/ligo dqsegdb.read",
            "exp": exp,
        });
        let (token, key) = make_token(claims, "k");
        let jwks = JwksCache::new();
        jwks.insert_test_key("https://cilogon.org/igwn", "k", key)
            .await;
        let err = validate_token(&token, &baseline_config(), &jwks)
            .await
            .unwrap_err();
        assert!(
            matches!(err, AuthError::MissingScope { .. }),
            "expected MissingScope, got {err:?}"
        );
    }

    #[tokio::test]
    async fn accepts_audience_array_form() {
        let exp = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64)
            + 3600;
        let claims = json!({
            "iss": "https://cilogon.org/igwn",
            "sub": "x",
            "aud": ["https://gracedb.ligo.org/api/", "boom-gw"],
            "scope": "gracedb.read",
            "exp": exp,
        });
        let (token, key) = make_token(claims, "k");
        let jwks = JwksCache::new();
        jwks.insert_test_key("https://cilogon.org/igwn", "k", key)
            .await;
        let principal = validate_token(&token, &baseline_config(), &jwks)
            .await
            .expect("valid token with array aud");
        assert_eq!(principal.sub, "x");
    }

    #[tokio::test]
    async fn dev_mode_skips_signature_but_still_checks_claims() {
        let exp = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64)
            + 3600;
        let claims = json!({
            "iss": "https://cilogon.org/igwn",
            "sub": "dev",
            "aud": "ANY",
            "scope": "gracedb.read",
            "exp": exp,
        });
        let (token, _key) = make_token(claims, "any-kid");
        // Empty JWKS — would fail in non-dev mode.
        let jwks = JwksCache::new();
        let mut config = baseline_config();
        config.dev_mode = true;
        let principal = validate_token(&token, &config, &jwks)
            .await
            .expect("dev mode");
        assert_eq!(principal.sub, "dev");
    }

    #[tokio::test]
    async fn dev_mode_still_rejects_expired() {
        let exp = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64)
            - 60;
        let claims = json!({
            "iss": "https://cilogon.org/igwn",
            "sub": "dev",
            "aud": "ANY",
            "scope": "gracedb.read",
            "exp": exp,
        });
        let (token, _) = make_token(claims, "k");
        let jwks = JwksCache::new();
        let mut config = baseline_config();
        config.dev_mode = true;
        let err = validate_token(&token, &config, &jwks).await.unwrap_err();
        assert!(matches!(err, AuthError::Expired));
    }
}
