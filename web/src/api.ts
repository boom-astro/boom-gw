// Thin axios wrapper that attaches the SCITokens bearer JWT (kept
// in localStorage by the LoginPage) to every request. Modeled on
// SkyPortal's `static/js/API.js` but using axios + Bearer auth
// instead of cookie-session credentials.
//
// The base URL is empty by default — Vite's dev server proxies
// `/api/*` to gw-api on :8080, and in production gw-api serves the
// SPA bundle so the API is same-origin. Override with
// `VITE_API_BASE_URL` for split-deployment scenarios.

import axios, { AxiosError, AxiosInstance } from "axios";

const STORAGE_KEY = "boom-gw.token";

export function getStoredToken(): string | null {
  return localStorage.getItem(STORAGE_KEY);
}

export function setStoredToken(token: string | null): void {
  if (token) {
    localStorage.setItem(STORAGE_KEY, token);
  } else {
    localStorage.removeItem(STORAGE_KEY);
  }
}

/// Lightweight JWT-claims peek (NOT signature verification — the
/// server enforces that). Used by the UI for `exp` warnings and to
/// show the principal name in the header.
export interface TokenClaims {
  sub?: string;
  iss?: string;
  scope?: string;
  exp?: number;
  iat?: number;
}

export function decodeClaims(token: string): TokenClaims | null {
  const parts = token.split(".");
  if (parts.length < 2) return null;
  try {
    const payload = atob(parts[1].replace(/-/g, "+").replace(/_/g, "/"));
    return JSON.parse(payload) as TokenClaims;
  } catch {
    return null;
  }
}

const baseURL = import.meta.env.VITE_API_BASE_URL || "";
export const http: AxiosInstance = axios.create({
  baseURL,
});

http.interceptors.request.use((cfg) => {
  const token = getStoredToken();
  if (token) {
    cfg.headers = cfg.headers ?? {};
    cfg.headers.Authorization = `Bearer ${token}`;
  }
  return cfg;
});

// 401 means the token is missing / expired / rejected. Wipe it so
// the next render trips the LoginPage redirect. We don't auto-
// retry — the user has to mint a fresh token (htgettoken) before
// reloading.
http.interceptors.response.use(
  (r) => r,
  (err: AxiosError) => {
    if (err.response?.status === 401) {
      setStoredToken(null);
    }
    return Promise.reject(err);
  }
);
