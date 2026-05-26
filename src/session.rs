//! Local session-cookie auth.
//!
//! Sits next to the SCITokens Bearer path in [`crate::auth`]. The
//! Bearer path stays for CLI clients and CI; humans get here via an
//! interactive login (CILogon OIDC, eventually — for now a dev-only
//! shortcut), and the server issues an HS256-signed JWT in an
//! HttpOnly cookie. Every subsequent request authenticates by cookie
//! and the middleware skips the JWKS fetch.
//!
//! The JWT is signed with a server-local HMAC secret
//! (`BOOM_GW_SESSION_SECRET`), so it is *not* an IGWN-issued token —
//! it is our own session credential, minted only after we have
//! independently verified the user's identity (today via dev-login,
//! tomorrow via CILogon callback). Putting it in JWT form means the
//! same [`crate::auth::Principal`] extraction path serves both
//! the Bearer code path and the cookie code path.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use actix_web::cookie::{Cookie, SameSite};
use actix_web::dev::ServiceRequest;
use actix_web::HttpRequest;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use crate::auth::Principal;

/// Cookie name we mint and read.
pub const SESSION_COOKIE: &str = "boom_gw_session";

/// Session JWT issuer string. Distinct from CILogon's `iss` so we
/// can never confuse our local tokens with an IGWN-issued one.
pub const SESSION_ISSUER: &str = "boom-gw/session";

#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// HMAC secret used to sign + verify the session JWT. Must be
    /// >= 32 bytes for HS256 (jsonwebtoken enforces nothing here, but
    /// shorter secrets weaken the integrity guarantee).
    pub secret: Vec<u8>,
    /// Cookie lifetime. Matches the JWT `exp` claim.
    pub max_age: Duration,
    /// Set the `Secure` flag (only send over HTTPS). True for prod,
    /// false for local dev where vite serves over plain HTTP.
    pub secure: bool,
}

impl SessionConfig {
    pub fn new(secret: impl Into<Vec<u8>>, max_age: Duration, secure: bool) -> Self {
        Self {
            secret: secret.into(),
            max_age,
            secure,
        }
    }
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("session token is malformed or has invalid signature: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error("session token expired")]
    Expired,
    #[error("session token has wrong issuer: {0}")]
    BadIssuer(String),
}

/// Session JWT claims. Smaller than the SCITokens shape — we only
/// need to round-trip the principal back to ourselves.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionClaims {
    pub iss: String,
    pub sub: String,
    pub scope: String,
    pub exp: i64,
    pub iat: i64,
    /// Which upstream issuer originally vouched for this principal.
    /// `"dev-login"` for the dev shortcut; populated from the CILogon
    /// `iss` claim once we wire the real flow.
    #[serde(default)]
    pub upstream_iss: String,
}

/// Mint a session JWT for the given principal.
pub fn mint_session(config: &SessionConfig, principal: &Principal) -> Result<String, SessionError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let exp = now + config.max_age.as_secs() as i64;
    let claims = SessionClaims {
        iss: SESSION_ISSUER.to_string(),
        sub: principal.sub.clone(),
        scope: principal.scopes.join(" "),
        exp,
        iat: now,
        upstream_iss: principal.iss.clone(),
    };
    let token = encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(&config.secret),
    )?;
    Ok(token)
}

/// Verify a session JWT and produce the [`Principal`]. The token
/// must be HS256-signed with our secret and carry [`SESSION_ISSUER`]
/// in the `iss` claim — both checks defend against a leaked IGWN
/// token being replayed in the cookie slot.
pub fn verify_session(config: &SessionConfig, token: &str) -> Result<Principal, SessionError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_issuer(&[SESSION_ISSUER]);
    validation.validate_exp = true;
    // We didn't set `aud` on the session token; don't require one.
    validation.validate_aud = false;
    let data = decode::<SessionClaims>(
        token,
        &DecodingKey::from_secret(&config.secret),
        &validation,
    )
    .map_err(|e| match e.kind() {
        jsonwebtoken::errors::ErrorKind::ExpiredSignature => SessionError::Expired,
        _ => SessionError::Jwt(e),
    })?;
    if data.claims.iss != SESSION_ISSUER {
        return Err(SessionError::BadIssuer(data.claims.iss));
    }
    let scopes = data
        .claims
        .scope
        .split_ascii_whitespace()
        .map(|s| s.to_string())
        .collect();
    Ok(Principal {
        sub: data.claims.sub,
        iss: data.claims.upstream_iss,
        scopes,
    })
}

/// Build the Set-Cookie payload for a freshly-minted session.
pub fn build_session_cookie<'a>(token: String, config: &SessionConfig) -> Cookie<'a> {
    let mut c = Cookie::build(SESSION_COOKIE, token)
        .http_only(true)
        .same_site(SameSite::Lax)
        .path("/")
        .max_age(actix_web::cookie::time::Duration::seconds(
            config.max_age.as_secs() as i64,
        ))
        .finish();
    if config.secure {
        c.set_secure(true);
    }
    c
}

/// Build a Set-Cookie payload that clears the session.
pub fn clear_session_cookie<'a>(config: &SessionConfig) -> Cookie<'a> {
    let mut c = Cookie::build(SESSION_COOKIE, "")
        .http_only(true)
        .same_site(SameSite::Lax)
        .path("/")
        .max_age(actix_web::cookie::time::Duration::seconds(0))
        .finish();
    if config.secure {
        c.set_secure(true);
    }
    c
}

/// Pull the session token out of the request cookie jar, if any.
pub fn cookie_token(req: &ServiceRequest) -> Option<String> {
    req.cookie(SESSION_COOKIE).map(|c| c.value().to_string())
}

/// Same as [`cookie_token`], for [`HttpRequest`]. Handlers receive
/// `HttpRequest`, middleware receives `ServiceRequest`; we need both.
pub fn cookie_token_http(req: &HttpRequest) -> Option<String> {
    req.cookie(SESSION_COOKIE).map(|c| c.value().to_string())
}

/// Generate a random 32-byte session secret. Used at startup when
/// `BOOM_GW_SESSION_SECRET` is unset *and* dev-mode is on; the value
/// changes on every restart, which is fine for local dev but means
/// every restart logs the user out. Logs a warning so this is not
/// silent.
pub fn generate_dev_secret() -> Vec<u8> {
    use rand::RngCore;
    let mut secret = vec![0u8; 32];
    rand::thread_rng().fill_bytes(&mut secret);
    warn!("BOOM_GW_SESSION_SECRET unset; generated an ephemeral dev secret. Sessions reset on every restart.");
    secret
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal() -> Principal {
        Principal {
            sub: "cough052@ligo.org".into(),
            iss: "https://cilogon.org/igwn".into(),
            scopes: vec!["gracedb.read".into(), "boom-gw.write".into()],
        }
    }

    fn config() -> SessionConfig {
        SessionConfig::new(
            b"unit-test-secret-bytes-1234567890" as &[u8],
            Duration::from_secs(3600),
            false,
        )
    }

    #[test]
    fn round_trip_preserves_principal() {
        let cfg = config();
        let token = mint_session(&cfg, &principal()).unwrap();
        let back = verify_session(&cfg, &token).unwrap();
        assert_eq!(back.sub, "cough052@ligo.org");
        assert_eq!(back.iss, "https://cilogon.org/igwn");
        assert!(back.scopes.contains(&"gracedb.read".to_string()));
        assert!(back.scopes.contains(&"boom-gw.write".to_string()));
    }

    #[test]
    fn wrong_secret_is_rejected() {
        let cfg = config();
        let token = mint_session(&cfg, &principal()).unwrap();
        let mut bad = cfg.clone();
        bad.secret = b"different-secret-different-32byts".to_vec();
        let err = verify_session(&bad, &token).unwrap_err();
        assert!(matches!(err, SessionError::Jwt(_)), "got {err:?}");
    }

    #[test]
    fn expired_token_is_rejected() {
        // Mint a JWT whose `exp` is in the past by going through
        // jsonwebtoken directly — the public mint_session can't
        // back-date.
        let cfg = config();
        let claims = SessionClaims {
            iss: SESSION_ISSUER.into(),
            sub: "x".into(),
            scope: "".into(),
            exp: 1, // 1970-01-01
            iat: 0,
            upstream_iss: "".into(),
        };
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(&cfg.secret),
        )
        .unwrap();
        let err = verify_session(&cfg, &token).unwrap_err();
        assert!(matches!(err, SessionError::Expired), "got {err:?}");
    }
}
