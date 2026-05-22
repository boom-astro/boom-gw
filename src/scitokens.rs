//! SCITokens bearer-token sources for OAUTHBEARER Kafka authentication.
//!
//! The GraceDB Kafka brokers (e.g. `kafka-dev.ligo.org:9092`) accept SCITokens
//! JWTs as OAuth bearer credentials. Tokens are short-lived (typically one
//! hour) and are normally acquired out-of-band by a tool such as `htgettoken`
//! against the LIGO vault. This module provides a [`TokenSource`] abstraction
//! that the Kafka consumer's OAUTHBEARER context calls into whenever rdkafka
//! asks for a fresh token, plus the JWT claim extraction needed to populate
//! `rdkafka::client::OAuthToken`.
//!
//! Discovery follows the WLCG token-discovery convention:
//!
//! 1. The path in `$BEARER_TOKEN_FILE` if set.
//! 2. The path in `$XDG_RUNTIME_DIR/bt_u${UID}` if `XDG_RUNTIME_DIR` is set.
//! 3. `/tmp/bt_u${UID}` as a fallback.
//!
//! Callers who want a different source can use [`EnvTokenSource`] or
//! implement [`TokenSource`] themselves.

use std::fs;
use std::path::{Path, PathBuf};

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use base64::Engine as _;
use serde::Deserialize;
use thiserror::Error;

/// Decoded SCITokens claim set, limited to the fields rdkafka needs to
/// populate `OAuthToken` (`sub` for the principal, `exp` for the lifetime).
#[derive(Debug, Clone, Deserialize)]
pub struct Claims {
    /// Subject — used as the OAUTHBEARER principal name.
    #[serde(default)]
    pub sub: Option<String>,
    /// Expiration time, seconds since the Unix epoch.
    pub exp: i64,
    /// Issuer URL, captured for diagnostics.
    #[serde(default)]
    pub iss: Option<String>,
    /// Scope string, captured for diagnostics.
    #[serde(default)]
    pub scope: Option<String>,
}

#[derive(Debug, Error)]
pub enum TokenError {
    #[error("failed to read token file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("environment variable {0} is not set")]
    EnvMissing(String),
    #[error("token is empty after trimming whitespace")]
    EmptyToken,
    #[error("malformed JWT: expected 3 dot-separated segments, found {0}")]
    BadJwtShape(usize),
    #[error("failed to base64-decode JWT payload: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("failed to parse JWT payload as JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("token discovery failed: none of BEARER_TOKEN_FILE, $XDG_RUNTIME_DIR/bt_u<uid>, or /tmp/bt_u<uid> is readable")]
    NotDiscovered,
}

/// A source of SCITokens bearer tokens. The consumer's OAUTHBEARER context
/// calls [`Self::current_token`] every time rdkafka requests a refresh.
pub trait TokenSource: Send + Sync {
    fn current_token(&self) -> Result<String, TokenError>;
}

/// Reads the token from a file on every call. The file is expected to
/// contain a single JWT, optionally surrounded by whitespace.
#[derive(Debug, Clone)]
pub struct FileTokenSource {
    pub path: PathBuf,
}

impl FileTokenSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Apply WLCG discovery rules to find an existing token file.
    pub fn discover() -> Result<Self, TokenError> {
        if let Ok(p) = std::env::var("BEARER_TOKEN_FILE") {
            let path = PathBuf::from(p);
            if path.exists() {
                return Ok(Self { path });
            }
        }
        let uid = user_uid_string();
        if let Ok(xdg) = std::env::var("XDG_RUNTIME_DIR") {
            let path = Path::new(&xdg).join(format!("bt_u{uid}"));
            if path.exists() {
                return Ok(Self { path });
            }
        }
        let fallback = PathBuf::from(format!("/tmp/bt_u{uid}"));
        if fallback.exists() {
            return Ok(Self { path: fallback });
        }
        Err(TokenError::NotDiscovered)
    }
}

impl TokenSource for FileTokenSource {
    fn current_token(&self) -> Result<String, TokenError> {
        let contents = fs::read_to_string(&self.path).map_err(|source| TokenError::Read {
            path: self.path.clone(),
            source,
        })?;
        let trimmed = contents.trim();
        if trimmed.is_empty() {
            return Err(TokenError::EmptyToken);
        }
        Ok(trimmed.to_string())
    }
}

/// Reads the token from an environment variable on every call. Useful for
/// tests and for short-lived debug sessions where a token has already been
/// exported.
#[derive(Debug, Clone)]
pub struct EnvTokenSource {
    pub var: String,
}

impl EnvTokenSource {
    pub fn new(var: impl Into<String>) -> Self {
        Self { var: var.into() }
    }
}

impl TokenSource for EnvTokenSource {
    fn current_token(&self) -> Result<String, TokenError> {
        let value = std::env::var(&self.var).map_err(|_| TokenError::EnvMissing(self.var.clone()))?;
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(TokenError::EmptyToken);
        }
        Ok(trimmed.to_string())
    }
}

/// Decode the unverified claim set out of a JWT. We deliberately do not
/// verify the signature: the broker is the party that verifies, and we need
/// only the `sub` and `exp` claims to populate rdkafka's `OAuthToken`.
pub fn decode_claims(token: &str) -> Result<Claims, TokenError> {
    let segments: Vec<&str> = token.split('.').collect();
    if segments.len() != 3 {
        return Err(TokenError::BadJwtShape(segments.len()));
    }
    let payload = B64URL.decode(segments[1].as_bytes())?;
    let claims: Claims = serde_json::from_slice(&payload)?;
    Ok(claims)
}

#[cfg(unix)]
fn user_uid_string() -> String {
    // Read uid via libc without adding a dependency. SAFETY: getuid is
    // documented as always succeeding and being thread-safe.
    unsafe { libc::getuid() }.to_string()
}

#[cfg(not(unix))]
fn user_uid_string() -> String {
    // Non-unix fallback: BEARER_TOKEN_FILE is required.
    "0".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a fake JWT for testing the decode path. The header and signature
    /// are arbitrary base64url strings since we never verify; only the
    /// payload is meaningful.
    fn make_jwt(payload_json: &str) -> String {
        let header = B64URL.encode(b"{\"alg\":\"RS256\",\"typ\":\"JWT\"}");
        let payload = B64URL.encode(payload_json.as_bytes());
        let sig = B64URL.encode(b"unverified");
        format!("{header}.{payload}.{sig}")
    }

    #[test]
    fn decodes_minimum_claims() {
        let jwt = make_jwt(r#"{"sub":"alice","exp":1800000000}"#);
        let claims = decode_claims(&jwt).unwrap();
        assert_eq!(claims.sub.as_deref(), Some("alice"));
        assert_eq!(claims.exp, 1_800_000_000);
    }

    #[test]
    fn decodes_full_scitokens_shape() {
        let jwt = make_jwt(
            r#"{"sub":"https://login.ligo.org/cilogon/alice",
                 "iss":"https://cilogon.org/igwn",
                 "scope":"read:/gracedb",
                 "exp":1800000000,
                 "aud":"kafka-dev.ligo.org"}"#,
        );
        let claims = decode_claims(&jwt).unwrap();
        assert_eq!(claims.iss.as_deref(), Some("https://cilogon.org/igwn"));
        assert_eq!(claims.scope.as_deref(), Some("read:/gracedb"));
    }

    #[test]
    fn rejects_malformed_jwt_shape() {
        let err = decode_claims("not-a-jwt").unwrap_err();
        match err {
            TokenError::BadJwtShape(n) => assert_eq!(n, 1),
            other => panic!("expected BadJwtShape, got {other:?}"),
        }
    }

    #[test]
    fn env_token_source_reads_var() {
        let var = "BOOM_TEST_SCITOKEN_VAR";
        let jwt = make_jwt(r#"{"sub":"x","exp":1}"#);
        // Safety: tests run single-threaded by default for env-mutating tests.
        std::env::set_var(var, &jwt);
        let src = EnvTokenSource::new(var);
        assert_eq!(src.current_token().unwrap(), jwt);
        std::env::remove_var(var);
        assert!(matches!(
            src.current_token().unwrap_err(),
            TokenError::EnvMissing(_)
        ));
    }

    #[test]
    fn file_token_source_reads_and_trims() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bearer_token");
        let jwt = make_jwt(r#"{"sub":"x","exp":1}"#);
        std::fs::write(&path, format!("\n{jwt}\n  ")).unwrap();
        let src = FileTokenSource::new(&path);
        assert_eq!(src.current_token().unwrap(), jwt);
    }

    #[test]
    fn file_token_source_errors_on_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bearer_token");
        std::fs::write(&path, "   \n\n   ").unwrap();
        let src = FileTokenSource::new(&path);
        assert!(matches!(
            src.current_token().unwrap_err(),
            TokenError::EmptyToken
        ));
    }
}
