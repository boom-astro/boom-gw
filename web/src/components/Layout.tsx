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
import { doLogout } from "../ducks/auth";
import { useAppDispatch, useAppSelector } from "../store";

export function Layout() {
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const principal = useAppSelector((s) => s.auth.principal);

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
          <Button color="inherit" onClick={() => navigate("/external-streams")}>
            External streams
          </Button>
          <Box sx={{ flexGrow: 1 }} />
          {principal?.sub ? (
            <>
              <Tooltip
                title={
                  principal.iss
                    ? `Signed in via ${principal.iss}`
                    : "Signed in"
                }
              >
                <Typography
                  variant="body2"
                  sx={{ mr: 2, color: "text.secondary" }}
                >
                  {principal.sub}
                </Typography>
              </Tooltip>
              <Button
                color="inherit"
                onClick={async () => {
                  await dispatch(doLogout()).unwrap();
                  navigate("/superevents", { replace: true });
                }}
              >
                Log out
              </Button>
            </>
          ) : (
            <Button
              color="inherit"
              variant="outlined"
              size="small"
              onClick={() => navigate("/login")}
            >
              Sign in
            </Button>
          )}
        </Toolbar>
      </AppBar>
      <Container maxWidth={false} sx={{ py: 3 }}>
        <Outlet />
      </Container>
    </Box>
  );
}
