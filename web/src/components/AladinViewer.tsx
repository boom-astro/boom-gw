// Aladin Lite v3 wrapper. The CDS CDN script is loaded by
// index.html so the global `A` is defined by the time React mounts
// (we poll briefly in case it's still in flight).
//
// We don't try to render the raw BAYESTAR probability density map
// here — Aladin Lite v3 only handles plain MOC FITS, not the
// multi-order UNIQ+PROBDENSITY shape. Instead, gw-clusterer
// precomputes credible-region MOCs (50% / 90%) when the skymap
// arrives, and we ask the API for those via
// `/api/superevents/{id}/contour?level=N`. Two contours are drawn:
// the 90% region as a translucent fill, and the 50% region as a
// brighter outline.
//
// Auth: gw-api requires a Bearer token, but Aladin's own URL loader
// can't attach headers. We fetch the contour FITS ourselves with
// the token, then hand Aladin a same-origin `blob:` URL.

import { useEffect, useRef, useState } from "react";
import { Box, Typography } from "@mui/material";
import { getStoredToken } from "../api";

declare global {
  interface Window {
    A?: AladinNamespace;
  }
}

interface AladinNamespace {
  init?: Promise<void>;
  aladin: (el: HTMLElement, options?: Record<string, unknown>) => AladinInstance;
  MOCFromURL?: (
    url: string,
    options?: Record<string, unknown>,
    successCallback?: (moc: unknown) => void,
    errorCallback?: (err: unknown) => void,
  ) => unknown;
}

interface AladinInstance {
  addMOC: (moc: unknown) => void;
  gotoRaDec: (ra: number, dec: number) => void;
}

interface Props {
  /**
   * URL template for the contour endpoint. The string `{level}` is
   * substituted with the integer percent (e.g. 50, 90). Example:
   * `/api/superevents/S000123/contour?level={level}`.
   */
  contourUrlTemplate: string;
  height?: number | string;
}

type Status =
  | { kind: "waiting-script" }
  | { kind: "initializing" }
  | { kind: "fetching" }
  | { kind: "rendering" }
  | { kind: "ready" }
  | { kind: "error"; message: string };

interface ContourLayer {
  level: number;
  options: Record<string, unknown>;
}

// 90% region as a translucent fill, 50% as a more saturated outline.
// Colors picked for visibility against the DSS2 starfield.
const CONTOUR_LAYERS: ContourLayer[] = [
  {
    level: 90,
    options: {
      opacity: 0.35,
      color: "#84CDFF",
      lineWidth: 1.0,
      fill: true,
    },
  },
  {
    level: 50,
    options: {
      opacity: 0.9,
      color: "#FFB347",
      lineWidth: 1.8,
      fill: false,
    },
  },
];

async function waitForAladin(timeoutMs = 10000): Promise<AladinNamespace> {
  const deadline = Date.now() + timeoutMs;
  while (!window.A) {
    if (Date.now() > deadline) {
      throw new Error(
        "Aladin Lite did not load. Check that the CDN script in index.html is reachable.",
      );
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  return window.A;
}

async function fetchAuthedBlob(url: string): Promise<string> {
  const token = getStoredToken();
  const res = await fetch(url, {
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
  if (!res.ok) {
    throw new Error(`${url} → HTTP ${res.status}`);
  }
  const blob = await res.blob();
  return URL.createObjectURL(blob);
}

export function AladinViewer({ contourUrlTemplate, height = 600 }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const aladinRef = useRef<AladinInstance | null>(null);
  const [status, setStatus] = useState<Status>({ kind: "waiting-script" });

  useEffect(() => {
    let cancelled = false;
    const blobUrls: string[] = [];

    async function mount() {
      try {
        const A = await waitForAladin();
        if (cancelled || !ref.current) return;

        setStatus({ kind: "initializing" });
        if (A.init) await A.init;
        if (cancelled || !ref.current) return;

        const aladin = A.aladin(ref.current, {
          survey: "P/DSS2/color",
          fov: 180,
          projection: "AIT",
          cooFrame: "equatorial",
          showCooGridControl: true,
          showProjectionControl: true,
          showFullscreenControl: true,
          showLayersControl: true,
          showFrame: true,
          showReticle: false,
        });
        aladinRef.current = aladin;

        if (!A.MOCFromURL) {
          throw new Error(
            "A.MOCFromURL missing — Aladin Lite CDN script may be the wrong version.",
          );
        }

        setStatus({ kind: "fetching" });
        const blobByLevel = await Promise.all(
          CONTOUR_LAYERS.map(async (layer) => ({
            layer,
            blobUrl: await fetchAuthedBlob(
              contourUrlTemplate.replace("{level}", String(layer.level)),
            ),
          })),
        );
        blobByLevel.forEach(({ blobUrl }) => blobUrls.push(blobUrl));
        if (cancelled) return;

        setStatus({ kind: "rendering" });
        for (const { layer, blobUrl } of blobByLevel) {
          // MOCFromURL is callback-based in some builds and sync in
          // others — handle both. Synchronous return value gets used
          // directly; the callbacks are wired as a fallback.
          const moc = await new Promise<unknown>((resolve, reject) => {
            let resolved = false;
            const safeResolve = (m: unknown) => {
              if (resolved) return;
              resolved = true;
              resolve(m);
            };
            const safeReject = (e: unknown) => {
              if (resolved) return;
              resolved = true;
              reject(e);
            };
            const ret = A.MOCFromURL!(
              blobUrl,
              layer.options,
              (m) => safeResolve(m),
              (e) => safeReject(e),
            );
            if (ret) safeResolve(ret);
            // Defensive timeout so a silent failure doesn't hang
            // the viewer forever.
            setTimeout(() => safeResolve(ret), 10000);
          });
          aladin.addMOC(moc);
        }
        if (!cancelled) setStatus({ kind: "ready" });
      } catch (e) {
        const message = e instanceof Error ? e.message : String(e);
        console.error("[AladinViewer] mount failed:", e);
        if (!cancelled) setStatus({ kind: "error", message });
      }
    }
    mount();
    return () => {
      cancelled = true;
      for (const u of blobUrls) URL.revokeObjectURL(u);
    };
  }, [contourUrlTemplate]);

  return (
    <Box sx={{ position: "relative", width: "100%", height }}>
      <Box
        ref={ref}
        sx={{
          width: "100%",
          height: "100%",
          bgcolor: "#000",
          borderRadius: 1,
          overflow: "hidden",
        }}
      />
      {status.kind !== "ready" && (
        <Box
          sx={{
            position: "absolute",
            inset: 0,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            pointerEvents: "none",
            color: status.kind === "error" ? "error.main" : "text.secondary",
            bgcolor: "rgba(0,0,0,0.4)",
            borderRadius: 1,
          }}
        >
          <Typography variant="body2">
            {status.kind === "waiting-script" && "Loading Aladin Lite…"}
            {status.kind === "initializing" && "Initializing viewer…"}
            {status.kind === "fetching" && "Fetching credible-region contours…"}
            {status.kind === "rendering" && "Rendering contours…"}
            {status.kind === "error" && `Sky map error: ${status.message}`}
          </Typography>
        </Box>
      )}
    </Box>
  );
}
