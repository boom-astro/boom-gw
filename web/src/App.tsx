// Top-level router. Anonymous users see all routes — public pages
// render fully, private sections (raw G-events, audit logs) replace
// their data with a "sign in to view" placeholder. This mirrors the
// GraceDB posture: anyone can browse superevents and external alerts,
// but the constituent G-events stay private until you sign in.
//
// On mount we call `/api/auth/me` to detect a live session cookie;
// while that's pending we render a spinner. The auth status drives
// header chrome (Sign-in button vs. principal + logout) and the
// per-section gates inside SuperEventPage.

import { useEffect } from "react";
import { Box, CircularProgress } from "@mui/material";
import { Navigate, Route, Routes } from "react-router-dom";
import { useAppDispatch, useAppSelector } from "./store";
import { loadMe } from "./ducks/auth";
import { Layout } from "./components/Layout";
import { LoginPage } from "./components/LoginPage";
import { SuperEventsPage } from "./components/SuperEventsPage";
import { SuperEventPage } from "./components/SuperEventPage";
import { ExternalStreamsPage } from "./components/ExternalStreamsPage";
import { GrbTriggerPage } from "./components/GrbTriggerPage";
import { SystemHealthPage } from "./components/SystemHealthPage";
import { ScienceFiltersPage } from "./components/ScienceFiltersPage";
import { GroupsPage } from "./components/GroupsPage";
import { GroupDetailPage } from "./components/GroupDetailPage";
import { AdminUsersPage } from "./components/AdminUsersPage";
import { AdminStreamsPage } from "./components/AdminStreamsPage";
import { RequireAcl } from "./components/RequireAcl";
import { ACL_MANAGE_USERS, ACL_MANAGE_STREAMS } from "./types/access";

export function App() {
  const dispatch = useAppDispatch();
  const status = useAppSelector((s) => s.auth.status);

  useEffect(() => {
    if (status === "idle") {
      dispatch(loadMe());
    }
  }, [status, dispatch]);

  if (status === "idle" || status === "loading") {
    return (
      <Box
        sx={{
          minHeight: "100vh",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route element={<Layout />}>
        <Route index element={<Navigate to="/superevents" replace />} />
        <Route path="/superevents" element={<SuperEventsPage />} />
        <Route path="/superevents/:id" element={<SuperEventPage />} />
        <Route path="/external-streams" element={<ExternalStreamsPage />} />
        <Route path="/science-filters" element={<ScienceFiltersPage />} />
        <Route path="/groups" element={<GroupsPage />} />
        <Route path="/groups/:id" element={<GroupDetailPage />} />
        <Route
          path="/admin/users"
          element={
            <RequireAcl acl={ACL_MANAGE_USERS}>
              <AdminUsersPage />
            </RequireAcl>
          }
        />
        <Route
          path="/admin/streams"
          element={
            <RequireAcl acl={ACL_MANAGE_STREAMS}>
              <AdminStreamsPage />
            </RequireAcl>
          }
        />
        <Route path="/system-health" element={<SystemHealthPage />} />
        <Route path="/grb-triggers/:triggerId" element={<GrbTriggerPage />} />
        <Route path="*" element={<Navigate to="/superevents" replace />} />
      </Route>
    </Routes>
  );
}
