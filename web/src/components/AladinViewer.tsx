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
// Auth: gw-api validates a session cookie. Aladin's own URL loader
// won't send credentials, so we fetch the contour FITS ourselves
// with `credentials: include` and hand Aladin a same-origin `blob:`
// URL.

import { useEffect, useRef, useState } from "react";
import { Box, Typography } from "@mui/material";

declare global {
  interface Window {
    A?: AladinNamespace;
  }
}

interface AladinNamespace {
  init?: Promise<void>;
  aladin: (
    el: HTMLElement,
    options?: Record<string, unknown>,
  ) => AladinInstance;
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
  setFoV?: (degrees: number) => void;
}

/// One additional MOC FITS to overlay on top of the GW credible
/// regions. Color + opacity are independent so callers (e.g. the
/// Localization tab) can give each GRB its own visual identity.
export interface ExtraMocOverlay {
  /// Unique identifier for the overlay — used as a React key and as
  /// the Aladin layer name. Typically `"{instrument}/{trigger_id}"`.
  id: string;
  /// Absolute URL the AladinViewer fetches (with the bearer token
  /// attached) and feeds to `A.MOCFromURL`.
  url: string;
  /// Aladin MOC styling options. See A.MOCFromURL second arg.
  options: Record<string, unknown>;
  /// Optional human-readable label shown in the legend.
  label?: string;
}

interface Props {
  /**
   * URL template for the GW credible-region contour endpoint. The
   * string `{level}` is substituted with the integer percent (e.g.
   * 50, 90). Example:
   *   `/api/superevents/S000123/contour?level={level}`.
   * Pass `null` for callers that don't have a GW skymap to overlay
   * (e.g. the per-GRB trigger drill-down) — the viewer will skip
   * the GW layers and render only `extraMocs`.
   */
  contourUrlTemplate: string | null;
  /**
   * Additional MOCs to overlay alongside the GW credible regions —
   * typically the canonical GRB MOC for each cross-matched
   * trigger. Each is rendered after the GW layers so they draw on
   * top.
   */
  extraMocs?: ExtraMocOverlay[];
  /**
   * Layers to render. When omitted, everything renders. When
   * provided, only IDs in the set are drawn. IDs:
   *   - GW contours: `"gw-50"`, `"gw-90"`
   *   - extras: whatever `ExtraMocOverlay.id` is set to
   * The viewer re-mounts whenever the set membership changes.
   */
  visibleLayerIds?: Set<string>;
  /**
   * Optional initial center for the view. When provided, the
   * viewer pans to `(ra, dec)` after the GW contours load — so
   * the operator doesn't have to chase the contour across the
   * sky after each remount. Typically sourced from
   * `SupereventDoc.skymap_summary.{center_ra, center_dec}`.
   */
  initialCenter?: { ra: number; dec: number };
  /**
   * Initial field-of-view (degrees) applied alongside
   * `initialCenter`. Defaults to 30° — wide enough to show a
   * BNS-scale localization with margin, tight enough that the
   * contour fills most of the frame.
   */
  initialFovDeg?: number;
  height?: number | string;
}

/// Stable IDs for the GW credible-region layers, surfaced as a
/// const so callers (the LocalizationTab legend) and the viewer
/// agree on the same string identifiers.
export const GW_LAYER_IDS = {
  cr90: "gw-90",
  cr50: "gw-50",
} as const;

type Status =
  | { kind: "waiting-script" }
  | { kind: "initializing" }
  | { kind: "fetching" }
  | { kind: "rendering" }
  | { kind: "ready" }
  | { kind: "error"; message: string };

interface ContourLayer {
  id: string;
  level: number;
  options: Record<string, unknown>;
}

// 90% region as a translucent fill, 50% as a more saturated outline.
// Colors picked for visibility against the DSS2 starfield.
const CONTOUR_LAYERS: ContourLayer[] = [
  {
    id: GW_LAYER_IDS.cr90,
    level: 90,
    options: {
      opacity: 0.35,
      color: "#84CDFF",
      lineWidth: 1.0,
      fill: true,
    },
  },
  {
    id: GW_LAYER_IDS.cr50,
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
  const res = await fetch(url, { credentials: "include" });
  if (!res.ok) {
    throw new Error(`${url} → HTTP ${res.status}`);
  }
  const blob = await res.blob();
  return URL.createObjectURL(blob);
}

export function AladinViewer({
  contourUrlTemplate,
  extraMocs,
  visibleLayerIds,
  initialCenter,
  initialFovDeg = 30,
  height = 600,
}: Props) {
  // Membership test that treats "no set passed" as "everything
  // visible" — so existing callers that don't yet wire the legend
  // get the original behavior.
  const isVisible = (id: string) =>
    visibleLayerIds == null || visibleLayerIds.has(id);
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
          // If the caller passed an initial center, open at the
          // narrower FoV right away — otherwise stay on the
          // full-sky default. Avoids the visible "zoom-in" jump
          // after the contours arrive.
          fov: initialCenter ? initialFovDeg : 180,
          target: initialCenter
            ? `${initialCenter.ra} ${initialCenter.dec}`
            : undefined,
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
        // Re-issue gotoRaDec defensively — some Aladin Lite v3
        // builds ignore `target` when it's passed as a string but
        // accept the imperative call. Either way we end up at
        // the same position.
        if (initialCenter) {
          aladin.gotoRaDec(initialCenter.ra, initialCenter.dec);
          if (aladin.setFoV) aladin.setFoV(initialFovDeg);
        }

        if (!A.MOCFromURL) {
          throw new Error(
            "A.MOCFromURL missing — Aladin Lite CDN script may be the wrong version.",
          );
        }

        setStatus({ kind: "fetching" });
        // Skip the GW credible-region layers entirely when the
        // caller didn't pass a contour template — used by the
        // per-GRB trigger drill-down, which only has external
        // MOCs to overlay (the `extraMocs` list below).
        const visibleContours = contourUrlTemplate
          ? CONTOUR_LAYERS.filter((l) => isVisible(l.id))
          : [];
        const tmpl = contourUrlTemplate;
        const blobByLevel = await Promise.all(
          visibleContours.map(async (layer) => ({
            layer,
            blobUrl: await fetchAuthedBlob(
              tmpl!.replace("{level}", String(layer.level)),
            ),
          })),
        );
        blobByLevel.forEach(({ blobUrl }) => blobUrls.push(blobUrl));
        if (cancelled) return;

        setStatus({ kind: "rendering" });

        // Same callback-or-sync trick the original loop used —
        // factored out so the GW contours and the GRB extras can
        // both use it without duplicating the timeout dance.
        const renderMoc = async (
          blobUrl: string,
          options: Record<string, unknown>,
        ) => {
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
              options,
              (m) => safeResolve(m),
              (e) => safeReject(e),
            );
            if (ret) safeResolve(ret);
            setTimeout(() => safeResolve(ret), 10000);
          });
          aladin.addMOC(moc);
        };

        for (const { layer, blobUrl } of blobByLevel) {
          await renderMoc(blobUrl, layer.options);
        }

        // Extra MOCs (typically GRB error regions for each cross-
        // matched trigger). Fetched the same way as the GW
        // contours — authed blob URL → A.MOCFromURL → addMOC.
        // Failures here log a warning and skip just the bad
        // overlay rather than crashing the entire viewer.
        const visibleExtras = (extraMocs ?? []).filter((m) => isVisible(m.id));
        for (const extra of visibleExtras) {
          try {
            const blobUrl = await fetchAuthedBlob(extra.url);
            blobUrls.push(blobUrl);
            if (cancelled) return;
            await renderMoc(blobUrl, extra.options);
          } catch (e) {
            console.warn(
              `[AladinViewer] failed to load extra MOC ${extra.id}:`,
              e,
            );
          }
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
    // Re-mount the viewer when either the set of extras or the
    // visibility filter changes. We key on the joined ID lists
    // (stable strings) instead of the array/set identity so React
    // doesn't loop forever when the parent recomputes references
    // each render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    contourUrlTemplate,
    (extraMocs ?? []).map((m) => m.id).join(","),
    visibleLayerIds ? Array.from(visibleLayerIds).sort().join(",") : "__all__",
    // Remount when the initial center changes (e.g. navigating
    // between superevents). Without this, the viewer keeps the
    // previous superevent's center.
    initialCenter ? `${initialCenter.ra},${initialCenter.dec}` : "default",
    initialFovDeg,
  ]);

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
