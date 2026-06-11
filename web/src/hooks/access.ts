// Authorization hooks over the enriched auth profile (`s.auth.me`).
// Kept separate from the auth duck so they can import the store's typed
// selector without creating an import cycle (store.ts imports the duck).
//
// All hooks guard with `?? []` because the API omits empty arrays
// (serde `skip_serializing_if`).

import { useAppSelector } from "../store";
import { ACL_SYSTEM_ADMIN } from "../types/access";

export const useMe = () => useAppSelector((s) => s.auth.me);

export const useMyAcls = (): string[] =>
  useAppSelector((s) => s.auth.me?.acls ?? []);

/// True if the user holds `acl` — or the `System admin` wildcard.
export const useHasAcl = (acl: string): boolean =>
  useAppSelector((s) => {
    const acls = s.auth.me?.acls ?? [];
    return acls.includes(ACL_SYSTEM_ADMIN) || acls.includes(acl);
  });

export const useMyGroups = () => useAppSelector((s) => s.auth.me?.groups ?? []);

export const useMyStreams = () =>
  useAppSelector((s) => s.auth.me?.streams ?? []);

/// True if the user administers the given group (or is a System admin).
export const useIsGroupAdmin = (groupId: string): boolean =>
  useAppSelector((s) => {
    const me = s.auth.me;
    if (!me) return false;
    if ((me.acls ?? []).includes(ACL_SYSTEM_ADMIN)) return true;
    return (me.groups ?? []).some((g) => g.id === groupId && g.admin);
  });
