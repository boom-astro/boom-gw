// Thin axios wrapper for the boom-gw API.
//
// Authentication is via an HttpOnly session cookie (`boom_gw_session`)
// minted by `/api/auth/dev-login` (or the CILogon callback, once that
// lands). Browsers attach the cookie automatically on same-origin
// requests, and Vite proxies `/api/*` to gw-api, so axios needs
// nothing more than `withCredentials: true` to make the cross-origin
// case work in prod splits.
//
// The base URL is empty by default — Vite's dev server proxies
// `/api/*` to gw-api on :8080, and in production gw-api serves the
// SPA bundle so the API is same-origin. Override with
// `VITE_API_BASE_URL` for split-deployment scenarios.

import axios, { AxiosError, AxiosInstance } from "axios";

const baseURL = import.meta.env.VITE_API_BASE_URL || "";
export const http: AxiosInstance = axios.create({
  baseURL,
  withCredentials: true,
});

export interface Principal {
  sub: string;
  iss: string;
  scopes: string[];
}

interface Envelope<T> {
  message: string;
  data: T;
}

export async function getMe(): Promise<Principal | null> {
  try {
    const { data } = await http.get<Envelope<Principal>>("/api/auth/me");
    return data.data;
  } catch (err) {
    const axErr = err as AxiosError;
    if (axErr.response?.status === 401) return null;
    throw err;
  }
}

/// Dev-mode shortcut: mint a session cookie for `sub` without going
/// through CILogon. Backend gates this on `BOOM_GW_API_AUTH_DEV_MODE`
/// so it's a no-op in prod (returns 404).
export async function devLogin(sub: string): Promise<Principal> {
  const { data } = await http.post<Envelope<Principal>>("/api/auth/dev-login", {
    sub,
  });
  return data.data;
}

export async function logout(): Promise<void> {
  await http.post("/api/auth/logout");
}

export interface AuthServerConfig {
  dev_mode: boolean;
  oidc_enabled: boolean;
  oidc_login_url: string | null;
}

export async function getAuthConfig(): Promise<AuthServerConfig> {
  const { data } = await http.get<Envelope<AuthServerConfig>>("/api/auth/config");
  return data.data;
}
