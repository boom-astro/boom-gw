// Persistent shell: app bar with principal + logout, then the
// matched child route. Modeled on SkyPortal's MainNav drawer pattern
// but we only have one section today, so a top bar is enough.

import {
  AppBar,
  Box,
  Button,
  Container,
  Toolbar,
  Tooltip,
  Typography,
} from "@mui/material";
import { Outlet, useNavigate } from "react-router-dom";
import { logout } from "../ducks/auth";
import { useAppDispatch, useAppSelector } from "../store";

export function Layout() {
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const claims = useAppSelector((s) => s.auth.claims);

  const expiresAt = claims?.exp ? new Date(claims.exp * 1000) : null;
  const expiresSoon =
    expiresAt && expiresAt.getTime() - Date.now() < 15 * 60 * 1000;

  return (
    <Box sx={{ minHeight: "100vh", bgcolor: "background.default" }}>
      <AppBar position="sticky" color="default" elevation={0}>
        <Toolbar>
          <Typography
            variant="h6"
            sx={{ cursor: "pointer", flexGrow: 0, mr: 3 }}
            onClick={() => navigate("/superevents")}
          >
            boom-gw
          </Typography>
          <Button color="inherit" onClick={() => navigate("/superevents")}>
            Superevents
          </Button>
          <Box sx={{ flexGrow: 1 }} />
          {claims?.sub && (
            <Tooltip
              title={
                expiresAt
                  ? `Token expires ${expiresAt.toLocaleString()}`
                  : "Token"
              }
            >
              <Typography
                variant="body2"
                sx={{
                  mr: 2,
                  color: expiresSoon ? "warning.main" : "text.secondary",
                }}
              >
                {claims.sub}
              </Typography>
            </Tooltip>
          )}
          <Button
            color="inherit"
            onClick={() => {
              dispatch(logout());
              navigate("/login", { replace: true });
            }}
          >
            Log out
          </Button>
        </Toolbar>
      </AppBar>
      <Container maxWidth={false} sx={{ py: 3 }}>
        <Outlet />
      </Container>
    </Box>
  );
}
