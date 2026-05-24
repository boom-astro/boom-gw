// Auth slice: holds the SCITokens bearer JWT and the decoded claims
// for header display + `exp` warnings. The token itself is mirrored
// to localStorage by api.ts so it survives reloads; this slice is
// the source of truth at runtime.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import {
  decodeClaims,
  getStoredToken,
  setStoredToken,
  TokenClaims,
} from "../api";

interface AuthState {
  token: string | null;
  claims: TokenClaims | null;
}

function initialState(): AuthState {
  const token = getStoredToken();
  return {
    token,
    claims: token ? decodeClaims(token) : null,
  };
}

const slice = createSlice({
  name: "auth",
  initialState: initialState(),
  reducers: {
    setToken(state, action: PayloadAction<string | null>) {
      const token = action.payload;
      setStoredToken(token);
      state.token = token;
      state.claims = token ? decodeClaims(token) : null;
    },
    logout(state) {
      setStoredToken(null);
      state.token = null;
      state.claims = null;
    },
  },
});

export const { setToken, logout } = slice.actions;
export default slice.reducer;
