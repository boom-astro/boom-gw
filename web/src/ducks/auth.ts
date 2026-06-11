// Auth slice: tracks the enriched current-user profile (`/api/users/me`)
// — identity plus ACLs, group memberships, and accessible streams.
// The credential is an HttpOnly session cookie set by gw-api; the SPA
// never touches the token directly.
//
// We keep a thin `principal` ({sub, iss, scopes}) derived from the
// profile for back-compat with call sites that only need the sub, and
// the full `me` for authorization (ACL gating, group/stream pickers).

import {
  createAsyncThunk,
  createSlice,
  PayloadAction,
} from "@reduxjs/toolkit";
import {
  devLogin,
  getMyProfile,
  logout as apiLogout,
  Principal,
} from "../api";
import type { Me } from "../types/api";

type Status = "idle" | "loading" | "authenticated" | "anonymous" | "error";

interface AuthState {
  /** Thin principal, derived from `me` — back-compat. */
  principal: Principal | null;
  /** Enriched profile: acls, groups, streams. */
  me: Me | null;
  status: Status;
  error: string | null;
}

const initialState: AuthState = {
  principal: null,
  me: null,
  status: "idle",
  error: null,
};

function principalOf(me: Me | null): Principal | null {
  if (!me) return null;
  return { sub: me.sub, iss: me.iss ?? "", scopes: me.scopes ?? [] };
}

export const loadMe = createAsyncThunk("auth/loadMe", async () => {
  return await getMyProfile();
});

export const doDevLogin = createAsyncThunk(
  "auth/devLogin",
  async (sub: string) => {
    // Mint the cookie, then hydrate the enriched profile — the
    // dev-login response itself is the thin principal only.
    await devLogin(sub);
    return await getMyProfile();
  },
);

export const doLogout = createAsyncThunk("auth/logout", async () => {
  await apiLogout();
});

const slice = createSlice({
  name: "auth",
  initialState,
  reducers: {
    setPrincipal(state, action: PayloadAction<Principal | null>) {
      state.principal = action.payload;
      state.status = action.payload ? "authenticated" : "anonymous";
    },
  },
  extraReducers: (b) => {
    b.addCase(loadMe.pending, (s) => {
      s.status = "loading";
    });
    b.addCase(loadMe.fulfilled, (s, a) => {
      s.me = a.payload;
      s.principal = principalOf(a.payload);
      s.status = a.payload ? "authenticated" : "anonymous";
      s.error = null;
    });
    b.addCase(loadMe.rejected, (s, a) => {
      s.me = null;
      s.principal = null;
      s.status = "error";
      s.error = a.error.message ?? "unknown error";
    });
    b.addCase(doDevLogin.fulfilled, (s, a) => {
      s.me = a.payload;
      s.principal = principalOf(a.payload);
      s.status = a.payload ? "authenticated" : "anonymous";
      s.error = null;
    });
    b.addCase(doDevLogin.rejected, (s, a) => {
      s.status = "error";
      s.error = a.error.message ?? "login failed";
    });
    b.addCase(doLogout.fulfilled, (s) => {
      s.principal = null;
      s.me = null;
      s.status = "anonymous";
    });
  },
});

export const { setPrincipal } = slice.actions;
export default slice.reducer;
