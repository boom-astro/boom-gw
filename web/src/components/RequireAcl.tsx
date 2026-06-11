// Route guard: render children only if the current user holds `acl`
// (or the System admin wildcard); otherwise redirect to /superevents.
// The App-level spinner gate ensures `me` is loaded before this runs,
// so there's no auth flicker.

import { ReactNode } from "react";
import { Navigate } from "react-router-dom";
import { useHasAcl } from "../hooks/access";

export function RequireAcl({
  acl,
  children,
}: {
  acl: string;
  children: ReactNode;
}) {
  return useHasAcl(acl) ? <>{children}</> : <Navigate to="/superevents" replace />;
}
