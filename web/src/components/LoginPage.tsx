// SCITokens login. There's no IdP redirect dance — the LIGO/IGWN
// flow expects the operator to run `htgettoken -a vault.ligo.org
// -i igwn` in their terminal and paste the resulting JWT. We
// decode it client-side just to surface the principal / expiry;
// the real validation happens server-side on every request.

import { useMemo, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Container,
  Link,
  Paper,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import { useNavigate } from "react-router-dom";
import { decodeClaims } from "../api";
import { setToken } from "../ducks/auth";
import { useAppDispatch } from "../store";

export function LoginPage() {
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const [value, setValue] = useState("");
  const [error, setError] = useState<string | null>(null);

  const preview = useMemo(() => {
    const trimmed = value.trim();
    if (!trimmed) return null;
    return decodeClaims(trimmed);
  }, [value]);

  function onSubmit() {
    const trimmed = value.trim();
    const claims = decodeClaims(trimmed);
    if (!claims) {
      setError("That doesn't parse as a JWT. Did you paste the full token?");
      return;
    }
    if (claims.exp && claims.exp * 1000 < Date.now()) {
      setError("Token is expired — mint a fresh one with htgettoken.");
      return;
    }
    dispatch(setToken(trimmed));
    navigate("/superevents", { replace: true });
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
            Paste a SCITokens bearer JWT to sign in. Get one with:
          </Typography>
          <Box
            component="pre"
            sx={{
              bgcolor: "rgba(255,255,255,0.04)",
              p: 1.5,
              borderRadius: 1,
              fontSize: 13,
              mb: 3,
              overflowX: "auto",
            }}
          >
            htgettoken -a vault.ligo.org -i igwn{"\n"}
            cat $BEARER_TOKEN_FILE
          </Box>
          <Stack spacing={2}>
            <TextField
              label="Bearer token"
              multiline
              minRows={5}
              fullWidth
              value={value}
              onChange={(e) => {
                setError(null);
                setValue(e.target.value);
              }}
              spellCheck={false}
              autoFocus
            />
            {preview && (
              <Alert severity="info" variant="outlined">
                {preview.sub && (
                  <div>
                    <strong>sub:</strong> {preview.sub}
                  </div>
                )}
                {preview.iss && (
                  <div>
                    <strong>iss:</strong> {preview.iss}
                  </div>
                )}
                {preview.scope && (
                  <div>
                    <strong>scope:</strong> {preview.scope}
                  </div>
                )}
                {preview.exp && (
                  <div>
                    <strong>exp:</strong>{" "}
                    {new Date(preview.exp * 1000).toLocaleString()}
                  </div>
                )}
              </Alert>
            )}
            {error && <Alert severity="error">{error}</Alert>}
            <Button
              variant="contained"
              onClick={onSubmit}
              disabled={!value.trim()}
            >
              Sign in
            </Button>
            <Typography variant="caption" color="text.secondary">
              Token is stored in localStorage and sent as a Bearer header. See{" "}
              <Link
                href="https://computing.docs.ligo.org/guide/auth/tokens/"
                target="_blank"
                rel="noopener"
              >
                IGWN token docs
              </Link>
              .
            </Typography>
          </Stack>
        </Paper>
      </Container>
    </Box>
  );
}
