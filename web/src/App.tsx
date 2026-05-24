// Top-level router. If we don't have a bearer token, every route
// falls through to <LoginPage/>. Once a token is set we drop into
// the Layout which handles the header + nested routes.

import { Navigate, Route, Routes } from "react-router-dom";
import { useAppSelector } from "./store";
import { Layout } from "./components/Layout";
import { LoginPage } from "./components/LoginPage";
import { SuperEventsPage } from "./components/SuperEventsPage";
import { SuperEventPage } from "./components/SuperEventPage";
import { ExternalStreamsPage } from "./components/ExternalStreamsPage";

export function App() {
  const token = useAppSelector((s) => s.auth.token);

  if (!token) {
    return (
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route path="*" element={<Navigate to="/login" replace />} />
      </Routes>
    );
  }

  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Navigate to="/superevents" replace />} />
        <Route path="/superevents" element={<SuperEventsPage />} />
        <Route path="/superevents/:id" element={<SuperEventPage />} />
        <Route path="/external-streams" element={<ExternalStreamsPage />} />
        <Route path="*" element={<Navigate to="/superevents" replace />} />
      </Route>
    </Routes>
  );
}
