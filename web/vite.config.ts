import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In dev: Vite serves on :5173 and proxies /api/* (and the
// public-route /api/superevents/{id}/skymap raw FITS endpoint) to
// the locally-running gw-api on :8080 so the browser sees same-origin
// and no CORS dance is needed.
//
// In prod: `npm run build` produces web/dist/ which gw-api serves
// directly via actix-files (see src/api.rs static fallback). Same
// same-origin story; the API and the SPA share a host.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:8080",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: true,
  },
});
