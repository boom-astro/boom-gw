//! CILogon OIDC authorization-code flow (with PKCE).
//!
//! Implements the click-through "Sign in with LIGO.org" path. The
//! user hits `GET /api/auth/login`; we redirect them through
//! CILogon, which delegates to the LIGO.org IdP (Shibboleth); when
//! they come back to `GET /api/auth/callback` we exchange the code
//! for an ID token, verify it against CILogon's JWKS, mint a local
//! session cookie via [`crate::session`], and 302 them at the SPA.
//!
//! No new server-side state. The PKCE `code_verifier` + state nonce
//! live in a short-lived HS256-signed cookie (`boom_gw_oidc_state`)
//! so we don't need a session store across the redirect roundtrip.
//! Same pattern oauth2-proxy uses.
//!
//! The CILogon OIDC client is registered out of band at
//! <https://cilogon.org/oauth2/register>; the resulting client_id +
//! client_secret are fed in via `BOOM_GW_OIDC_*` env vars. When
//! these are unset the routes return 404 and the SPA falls back to
//! dev-login (if enabled) or to nothing.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use actix_web::cookie::{Cookie, SameSite};
use actix_web::http::header::LOCATION;
use actix_web::{web, HttpRequest, HttpResponse, Responder};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use jsonwebtoken::{
    decode, decode_header, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::auth::{JwksCache, Principal, DEFAULT_REQUIRED_SCOPE};
use crate::session::{build_session_cookie, mint_session, SessionConfig};

/// Cookie that carries the PKCE verifier + state nonce across the
/// CILogon redirect. Short TTL — five minutes is plenty for a
/// human typing a password on the IdP page.
pub const STATE_COOKIE: &str = "boom_gw_oidc_state";

const STATE_COOKIE_TTL: Duration = Duration::from_secs(300);

const STATE_JWT_ISSUER: &str = "boom-gw/oidc-state";

#[derive(Debug, Clone)]
pub struct OidcConfig {
    pub client_id: String,
    pub client_secret: String,
    /// Where CILogon redirects the browser after the IdP login.
    /// Must exactly match what was registered with CILogon.
    pub redirect_uri: String,
    /// OIDC issuer URL — root for `.well-known/openid-configuration`.
    /// Defaults to `https://cilogon.org`.
    pub issuer: String,
    /// `selected_idp` query parameter sent to CILogon. Skips the
    /// CILogon institution picker and goes straight to LIGO.org.
    /// Set to empty string to let the user choose.
    pub selected_idp: String,
    /// Scopes requested. CILogon needs at minimum `openid`; we ask
    /// for `email profile` so we can populate the principal's
    /// display name, and `org.cilogon.userinfo` to receive the
    /// `eppn` claim that mirrors the SCITokens `sub`.
    pub scopes: String,
    /// Where to send the user after a successful login. Almost
    /// always the SPA root.
    pub post_login_redirect: String,
    /// Set the `Secure` flag on the state cookie. Mirrors
    /// [`SessionConfig::secure`]; both flow over the same channel.
    pub secure_cookie: bool,
}

impl OidcConfig {
    /// Pull config from env vars. Returns `None` when the required
    /// pair (client_id + client_secret) is missing, so the binary
    /// can disable the routes cleanly.
    pub fn from_env() -> Option<Self> {
        let client_id = std::env::var("BOOM_GW_OIDC_CLIENT_ID").ok()?;
        let client_secret = std::env::var("BOOM_GW_OIDC_CLIENT_SECRET").ok()?;
        let redirect_uri = std::env::var("BOOM_GW_OIDC_REDIRECT_URI")
            .unwrap_or_else(|_| "http://localhost:5173/api/auth/callback".to_string());
        let issuer = std::env::var("BOOM_GW_OIDC_ISSUER")
            .unwrap_or_else(|_| "https://cilogon.org".to_string());
        let selected_idp = std::env::var("BOOM_GW_OIDC_SELECTED_IDP")
            .unwrap_or_else(|_| "https://login.ligo.org/idp/shibboleth".to_string());
        let scopes = std::env::var("BOOM_GW_OIDC_SCOPES")
            .unwrap_or_else(|_| "openid email profile org.cilogon.userinfo".to_string());
        let post_login_redirect =
            std::env::var("BOOM_GW_WEB_BASE_URL").unwrap_or_else(|_| "/superevents".to_string());
        let secure_cookie = std::env::var("BOOM_GW_SESSION_SECURE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        Some(OidcConfig {
            client_id,
            client_secret,
            redirect_uri,
            issuer,
            selected_idp,
            scopes,
            post_login_redirect,
            secure_cookie,
        })
    }
}

/// OIDC discovery document fields we care about. Cached per
/// [`OidcConfig`] in [`DiscoveryCache`]. The `jwks_uri` is also
/// in the discovery doc but we get there via [`JwksCache::refresh`]
/// which fetches discovery itself — no point caching it twice.
#[derive(Debug, Clone, Deserialize)]
struct Discovery {
    authorization_endpoint: String,
    token_endpoint: String,
}

/// Tiny one-issuer cache so `/api/auth/login` and `/api/auth/callback`
/// each pay at most one discovery fetch per process lifetime.
#[derive(Default, Clone)]
pub struct DiscoveryCache {
    inner: Arc<RwLock<Option<Discovery>>>,
}

impl DiscoveryCache {
    pub fn new() -> Self {
        Self::default()
    }

    async fn get(&self, issuer: &str) -> Result<Discovery, OidcError> {
        if let Some(d) = self.inner.read().await.clone() {
            return Ok(d);
        }
        let url = format!("{}/.well-known/openid-configuration", issuer);
        let d: Discovery = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        *self.inner.write().await = Some(d.clone());
        Ok(d)
    }
}

#[derive(Debug, Error)]
pub enum OidcError {
    #[error("OIDC is not configured")]
    NotConfigured,
    #[error("OIDC state cookie missing or tampered")]
    BadState,
    #[error("state mismatch between cookie and callback param")]
    StateMismatch,
    #[error("token exchange failed: HTTP {status}: {body}")]
    TokenExchange { status: u16, body: String },
    #[error("id_token verification failed: {0}")]
    IdToken(String),
    #[error("CILogon ID token has no usable principal claim (sub/eppn/email)")]
    NoPrincipal,
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("jwt: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
}

/// State cookie body — what `/api/auth/login` writes and
/// `/api/auth/callback` reads back. Signed with the same HS256
/// secret as the session cookie.
#[derive(Debug, Serialize, Deserialize)]
struct StateClaims {
    iss: String,
    /// CSRF nonce we also pass to CILogon via `?state=`.
    csrf: String,
    /// PKCE code_verifier — the secret half of the PKCE pair.
    pkce_verifier: String,
    exp: i64,
    iat: i64,
}

/// Generate a 32-byte URL-safe random string. Suitable for both
/// the OAuth state nonce and the PKCE verifier (RFC 7636 requires
/// the verifier to be 43–128 chars from the URL-safe alphabet; 32
/// random bytes base64url-encoded land at 43 chars).
fn random_url_safe() -> String {
    let mut buf = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut buf);
    URL_SAFE_NO_PAD.encode(buf)
}

/// PKCE S256 challenge: `base64url(sha256(verifier))`.
fn pkce_challenge(verifier: &str) -> String {
    let digest = Sha256::digest(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(digest)
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn mint_state_cookie(secret: &[u8], csrf: &str, pkce_verifier: &str) -> Result<String, OidcError> {
    let now = now_secs();
    let claims = StateClaims {
        iss: STATE_JWT_ISSUER.into(),
        csrf: csrf.into(),
        pkce_verifier: pkce_verifier.into(),
        exp: now + STATE_COOKIE_TTL.as_secs() as i64,
        iat: now,
    };
    Ok(encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret),
    )?)
}

fn verify_state_cookie(secret: &[u8], token: &str) -> Result<StateClaims, OidcError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_issuer(&[STATE_JWT_ISSUER]);
    validation.validate_aud = false;
    validation.validate_exp = true;
    let data = decode::<StateClaims>(token, &DecodingKey::from_secret(secret), &validation)
        .map_err(|_| OidcError::BadState)?;
    Ok(data.claims)
}

fn build_state_cookie<'a>(value: String, secure: bool) -> Cookie<'a> {
    let mut c = Cookie::build(STATE_COOKIE, value)
        .http_only(true)
        .same_site(SameSite::Lax)
        .path("/")
        .max_age(actix_web::cookie::time::Duration::seconds(
            STATE_COOKIE_TTL.as_secs() as i64,
        ))
        .finish();
    if secure {
        c.set_secure(true);
    }
    c
}

fn clear_state_cookie<'a>(secure: bool) -> Cookie<'a> {
    let mut c = Cookie::build(STATE_COOKIE, "")
        .http_only(true)
        .same_site(SameSite::Lax)
        .path("/")
        .max_age(actix_web::cookie::time::Duration::seconds(0))
        .finish();
    if secure {
        c.set_secure(true);
    }
    c
}

fn url_encode_query(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(k, v)| format!("{}={}", percent(k), percent(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// RFC 3986 unreserved chars stay literal; everything else becomes
/// `%XX`. We avoid pulling the whole `url` crate in for one call.
fn percent(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char);
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

/// `GET /api/auth/login` — kick off the OIDC dance.
pub async fn login(
    oidc: Option<web::Data<OidcConfig>>,
    session: web::Data<SessionConfig>,
    discovery: web::Data<DiscoveryCache>,
) -> impl Responder {
    let oidc = match oidc {
        Some(o) => o,
        None => {
            return HttpResponse::NotFound()
                .body("OIDC sign-in is not configured on this gw-api instance");
        }
    };
    let d = match discovery.get(&oidc.issuer).await {
        Ok(d) => d,
        Err(e) => {
            warn!("OIDC discovery failed: {e}");
            return HttpResponse::BadGateway().body(format!("OIDC discovery failed: {e}"));
        }
    };
    let csrf = random_url_safe();
    let pkce_verifier = random_url_safe();
    let challenge = pkce_challenge(&pkce_verifier);
    let state_jwt = match mint_state_cookie(&session.secret, &csrf, &pkce_verifier) {
        Ok(t) => t,
        Err(e) => {
            return HttpResponse::InternalServerError().body(format!("state mint failed: {e}"))
        }
    };
    let query = url_encode_query(&[
        ("response_type", "code"),
        ("client_id", &oidc.client_id),
        ("redirect_uri", &oidc.redirect_uri),
        ("scope", &oidc.scopes),
        ("state", &csrf),
        ("code_challenge", &challenge),
        ("code_challenge_method", "S256"),
        ("selected_idp", &oidc.selected_idp),
    ]);
    let authorize_url = format!("{}?{}", d.authorization_endpoint, query);
    debug!(target: "oidc", "redirecting to authorize: {}", authorize_url);
    HttpResponse::Found()
        .cookie(build_state_cookie(state_jwt, oidc.secure_cookie))
        .insert_header((LOCATION, authorize_url))
        .finish()
}

#[derive(Debug, Deserialize)]
pub struct CallbackQuery {
    pub code: Option<String>,
    pub state: Option<String>,
    pub error: Option<String>,
    pub error_description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    id_token: String,
}

#[derive(Debug, Deserialize)]
struct IdTokenClaims {
    iss: String,
    aud: serde_json::Value,
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    email: Option<String>,
    /// CILogon-IGWN populates this with `<user>@ligo.org` —
    /// matches the SCITokens `sub` exactly. When present we use
    /// it in preference to the OpenID `sub` (which is an opaque
    /// CILogon identifier).
    #[serde(default, rename = "eppn", alias = "eduPersonPrincipalName")]
    eppn: Option<String>,
    exp: i64,
}

/// `GET /api/auth/callback` — finish the OIDC dance.
pub async fn callback(
    req: HttpRequest,
    oidc: Option<web::Data<OidcConfig>>,
    session: web::Data<SessionConfig>,
    discovery: web::Data<DiscoveryCache>,
    jwks: web::Data<JwksCache>,
    query: web::Query<CallbackQuery>,
) -> impl Responder {
    let oidc = match oidc {
        Some(o) => o,
        None => {
            return HttpResponse::NotFound()
                .body("OIDC sign-in is not configured on this gw-api instance");
        }
    };
    if let Some(err) = &query.error {
        return HttpResponse::BadRequest().body(format!(
            "OIDC error: {err} — {}",
            query
                .error_description
                .as_deref()
                .unwrap_or("(no description)")
        ));
    }
    let code = match &query.code {
        Some(c) => c.clone(),
        None => return HttpResponse::BadRequest().body("missing code"),
    };
    let csrf = match &query.state {
        Some(s) => s.clone(),
        None => return HttpResponse::BadRequest().body("missing state"),
    };
    let cookie_value = match req.cookie(STATE_COOKIE) {
        Some(c) => c.value().to_string(),
        None => return HttpResponse::BadRequest().body("missing state cookie"),
    };
    let state_claims = match verify_state_cookie(&session.secret, &cookie_value) {
        Ok(c) => c,
        Err(e) => return HttpResponse::BadRequest().body(format!("bad state cookie: {e}")),
    };
    if state_claims.csrf != csrf {
        return HttpResponse::BadRequest().body(OidcError::StateMismatch.to_string());
    }

    let discovery = match discovery.get(&oidc.issuer).await {
        Ok(d) => d,
        Err(e) => return HttpResponse::BadGateway().body(format!("OIDC discovery failed: {e}")),
    };

    // Exchange code for tokens. CILogon accepts client_id+secret
    // in either Basic Auth or the form body; we use the form body
    // because it's what most reference clients do.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let form = [
        ("grant_type", "authorization_code"),
        ("code", code.as_str()),
        ("redirect_uri", oidc.redirect_uri.as_str()),
        ("client_id", oidc.client_id.as_str()),
        ("client_secret", oidc.client_secret.as_str()),
        ("code_verifier", state_claims.pkce_verifier.as_str()),
    ];
    let token_resp = match client
        .post(&discovery.token_endpoint)
        .form(&form)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return HttpResponse::BadGateway().body(format!("token endpoint: {e}")),
    };
    let status = token_resp.status();
    if !status.is_success() {
        let body = token_resp.text().await.unwrap_or_default();
        return HttpResponse::BadGateway().body(
            OidcError::TokenExchange {
                status: status.as_u16(),
                body,
            }
            .to_string(),
        );
    }
    let token_resp: TokenResponse = match token_resp.json().await {
        Ok(t) => t,
        Err(e) => return HttpResponse::BadGateway().body(format!("token decode: {e}")),
    };

    // Verify the ID token. We do not reuse `validate_token` —
    // that path is the SCITokens 2.0 access-token policy, with
    // its own issuer allowlist and scope check. ID tokens have
    // different rules (iss = `https://cilogon.org`, no scope
    // claim, audience = our client_id).
    let principal = match verify_id_token(&token_resp.id_token, &oidc, &jwks).await {
        Ok(p) => p,
        Err(e) => {
            warn!("id_token verify failed: {e}");
            return HttpResponse::Unauthorized().body(format!("id_token rejected: {e}"));
        }
    };

    let session_jwt = match mint_session(&session, &principal) {
        Ok(t) => t,
        Err(e) => return HttpResponse::InternalServerError().body(format!("session mint: {e}")),
    };
    info!(sub = %principal.sub, "OIDC login: minted session");
    HttpResponse::Found()
        .cookie(build_session_cookie(session_jwt, &session))
        .cookie(clear_state_cookie(oidc.secure_cookie))
        .insert_header((LOCATION, oidc.post_login_redirect.clone()))
        .finish()
}

async fn verify_id_token(
    token: &str,
    oidc: &OidcConfig,
    jwks: &JwksCache,
) -> Result<Principal, OidcError> {
    // Fetch the issuer's JWKS via the shared cache. The cache's
    // discovery code reads `{iss}/.well-known/openid-configuration`,
    // which for CILogon publishes a JWKS distinct from the SCITokens
    // /igwn endpoint — so this populates a new cache entry rather
    // than colliding with the existing one.
    jwks.refresh(&oidc.issuer)
        .await
        .map_err(|e| OidcError::IdToken(format!("jwks refresh: {e}")))?;
    let header = decode_header(token)?;
    let key = jwks
        .get(&oidc.issuer, &header.kid)
        .await
        .ok_or_else(|| OidcError::IdToken(format!("no signing key for kid={:?}", header.kid)))?;

    let mut validation = Validation::new(header.alg);
    validation.set_issuer(&[&oidc.issuer]);
    validation.set_audience(&[&oidc.client_id]);
    validation.validate_exp = true;
    let data = decode::<IdTokenClaims>(token, &key, &validation)
        .map_err(|e| OidcError::IdToken(format!("decode: {e}")))?;

    let _ = data.claims.iss;
    let _ = data.claims.aud;
    let _ = data.claims.exp;

    let sub = data
        .claims
        .eppn
        .or(data.claims.email)
        .or(data.claims.sub)
        .ok_or(OidcError::NoPrincipal)?;
    Ok(Principal {
        sub,
        iss: oidc.issuer.clone(),
        scopes: vec![DEFAULT_REQUIRED_SCOPE.to_string()],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::JwksCache;

    #[test]
    fn pkce_challenge_matches_rfc7636_example() {
        // RFC 7636 §4 example: verifier =
        //   "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        // expected challenge =
        //   "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        let v = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        assert_eq!(
            pkce_challenge(v),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn state_cookie_round_trips() {
        let secret = b"unit-test-session-secret-xxxxxxxx";
        let token = mint_state_cookie(secret, "csrf-abc", "pkce-xyz").unwrap();
        let claims = verify_state_cookie(secret, &token).unwrap();
        assert_eq!(claims.csrf, "csrf-abc");
        assert_eq!(claims.pkce_verifier, "pkce-xyz");
    }

    #[test]
    fn state_cookie_rejects_wrong_secret() {
        let token = mint_state_cookie(b"a-secret-1234567890abcdef-bytes!", "x", "y").unwrap();
        let err = verify_state_cookie(b"a-different-secret-bytes-32!!!!a", &token).unwrap_err();
        assert!(matches!(err, OidcError::BadState));
    }

    #[test]
    fn percent_encodes_reserved_chars() {
        assert_eq!(percent("hello world"), "hello%20world");
        assert_eq!(percent("a/b?c=d&e=f"), "a%2Fb%3Fc%3Dd%26e%3Df");
        assert_eq!(percent("https://x.y/z"), "https%3A%2F%2Fx.y%2Fz");
    }

    /// JwksCache is constructible without a network — sanity check
    /// that the OIDC module compiles against it.
    #[tokio::test]
    async fn jwks_cache_constructs() {
        let _ = JwksCache::new();
    }
}
