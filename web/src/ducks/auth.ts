// Auth slice: tracks the authenticated principal returned by
// `/api/auth/me`. The actual credential is an HttpOnly session
// cookie set by gw-api — the browser handles it, the SPA never
// touches the token directly.

import {
  createAsyncThunk,
  createSlice,
  PayloadAction,
} from "@reduxjs/toolkit";
import { devLogin, getMe, logout as apiLogout, Principal } from "../api";

type Status = "idle" | "loading" | "authenticated" | "anonymous" | "error";

interface AuthState {
  principal: Principal | null;
  status: Status;
  error: string | null;
}

const initialState: AuthState = {
  principal: null,
  status: "idle",
  error: null,
};

export const loadMe = createAsyncThunk("auth/loadMe", async () => {
  return await getMe();
});

export const doDevLogin = createAsyncThunk(
  "auth/devLogin",
  async (sub: string) => {
    return await devLogin(sub);
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
      s.principal = a.payload;
      s.status = a.payload ? "authenticated" : "anonymous";
      s.error = null;
    });
    b.addCase(loadMe.rejected, (s, a) => {
      s.principal = null;
      s.status = "error";
      s.error = a.error.message ?? "unknown error";
    });
    b.addCase(doDevLogin.fulfilled, (s, a) => {
      s.principal = a.payload;
      s.status = "authenticated";
      s.error = null;
    });
    b.addCase(doDevLogin.rejected, (s, a) => {
      s.status = "error";
      s.error = a.error.message ?? "login failed";
    });
    b.addCase(doLogout.fulfilled, (s) => {
      s.principal = null;
      s.status = "anonymous";
    });
  },
});

export const { setPrincipal } = slice.actions;
export default slice.reducer;
