// Login screen. Reads `/api/auth/config` on mount to decide which
// sign-in paths are available:
//
// * `oidc_enabled` → show "Sign in with LIGO.org" (CILogon → LIGO
//   Shibboleth → back to us with a session cookie).
// * `dev_mode` → show the dev-login form so local work doesn't
//   need a CILogon client registered.
//
// Both can be true simultaneously (a dev gw-api with OIDC also
// configured), in which case both controls render.

import { useEffect, useState } from "react";
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Container,
  Paper,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import { useNavigate } from "react-router-dom";
import { AuthServerConfig, getAuthConfig } from "../api";
import { doDevLogin } from "../ducks/auth";
import { useAppDispatch } from "../store";

export function LoginPage() {
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const [cfg, setCfg] = useState<AuthServerConfig | null>(null);
  const [cfgError, setCfgError] = useState<string | null>(null);
  const [sub, setSub] = useState("cough052@ligo.org");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    getAuthConfig()
      .then(setCfg)
      .catch((e) => setCfgError(e instanceof Error ? e.message : String(e)));
  }, []);

  async function onDevLogin() {
    setError(null);
    setBusy(true);
    try {
      const result = await dispatch(doDevLogin(sub.trim())).unwrap();
      if (result) navigate("/superevents", { replace: true });
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(
        msg.includes("404")
          ? "Dev login is disabled. Start gw-api with --auth-dev-mode."
          : msg,
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <Box
      sx={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        bgcolor: "background.default",
      }}
    >
      <Container maxWidth="sm">
        <Paper sx={{ p: 4 }}>
          <Typography variant="h5" gutterBottom>
            boom-gw
          </Typography>
          <Typography variant="body2" sx={{ mb: 3, color: "text.secondary" }}>
            Sign in with your LIGO.org credentials.
          </Typography>

          {cfgError && (
            <Alert severity="warning" sx={{ mb: 2 }}>
              Could not reach gw-api: {cfgError}
            </Alert>
          )}

          {!cfg && !cfgError && (
            <Box sx={{ display: "flex", justifyContent: "center", py: 3 }}>
              <CircularProgress size={24} />
            </Box>
          )}

          {cfg && (
            <Stack spacing={2}>
              <Button
                variant="contained"
                fullWidth
                size="large"
                onClick={() => {
                  if (cfg.oidc_login_url)
                    window.location.href = cfg.oidc_login_url;
                }}
                disabled={!cfg.oidc_enabled}
              >
                {cfg.oidc_enabled
                  ? "Sign in with LIGO.org"
                  : "Sign in with LIGO.org (not configured)"}
              </Button>
              {!cfg.oidc_enabled && (
                <Typography
                  variant="caption"
                  color="text.secondary"
                  sx={{ textAlign: "center" }}
                >
                  Register a CILogon OIDC client at cilogon.org/oauth2/register
                  and set BOOM_GW_OIDC_CLIENT_ID + BOOM_GW_OIDC_CLIENT_SECRET to
                  enable.
                </Typography>
              )}

              {cfg.dev_mode && (
                <>
                  <Typography
                    variant="caption"
                    color="text.secondary"
                    sx={{ textAlign: "center" }}
                  >
                    ─── or, in dev ───
                  </Typography>
                  <TextField
                    label="Dev login: sub"
                    value={sub}
                    onChange={(e) => {
                      setError(null);
                      setSub(e.target.value);
                    }}
                    size="small"
                    fullWidth
                    spellCheck={false}
                    helperText="Mints a session cookie for this principal."
                  />
                  {error && <Alert severity="error">{error}</Alert>}
                  <Button
                    variant="outlined"
                    onClick={onDevLogin}
                    disabled={!sub.trim() || busy}
                  >
                    {busy ? "Signing in…" : "Dev sign-in"}
                  </Button>
                </>
              )}
            </Stack>
          )}
        </Paper>
      </Container>
    </Box>
  );
}
