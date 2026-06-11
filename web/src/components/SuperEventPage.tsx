// Detail page for one superevent. Matches the SkyPortal GcnEventPage
// shape — tabs on top of a sticky properties drawer — but the
// resources we have are different (g-events instead of localizations,
// localize requests/results instead of observation plans).
//
// Tabs:
//   * Overview — properties + g-events list
//   * Localization — Aladin Lite skymap + localize req/result table
//   * Annotations — AnnotationsPanel
//   * Alerts — AlertsPanel

import { useEffect, useMemo, useState } from "react";
import {
  Alert,
  Box,
  Chip,
  CircularProgress,
  Divider,
  Drawer,
  Grid,
  IconButton,
  Paper,
  Stack,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Tabs,
  Tooltip,
  Typography,
} from "@mui/material";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import { useNavigate, useParams } from "react-router-dom";
import {
  clear,
  fetchLocalizeRequests,
  fetchLocalizeResults,
  fetchSuperevent,
  fetchSupereventEvents,
} from "../ducks/superevent";
import { useAppDispatch, useAppSelector } from "../store";
import { fetchCrossMatches } from "../ducks/crossMatches";
import { fetchIceCubeLvkSearches } from "../ducks/icecubeLvkSearches";
import { AladinViewer, GW_LAYER_IDS } from "./AladinViewer";
import { AnnotationsPanel } from "./AnnotationsPanel";
import { AlertsPanel } from "./AlertsPanel";
import { CrossMatchesPanel } from "./CrossMatchesPanel";
import { IceCubeLvkSearchesPanel } from "./IceCubeLvkSearchesPanel";

const DRAWER_WIDTH = 360;

export function SuperEventPage() {
  const { id = "" } = useParams();
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const [tab, setTab] = useState(0);
  const [propsOpen, setPropsOpen] = useState(true);
  const { doc, events, localizeRequests, localizeResults, loading, error } =
    useAppSelector((s) => s.superevent);
  const isAuthenticated = useAppSelector(
    (s) => s.auth.status === "authenticated",
  );

  useEffect(() => {
    if (!id) return;
    dispatch(fetchSuperevent(id));
    dispatch(fetchIceCubeLvkSearches(id));
    // /api/events, /api/localize-requests, /api/localize-results
    // require auth — skip the fetch for anonymous visitors so we
    // don't fill the console with 401s. The corresponding sections
    // render a "sign in to view" placeholder instead.
    if (isAuthenticated) {
      dispatch(fetchLocalizeRequests(id));
      dispatch(fetchLocalizeResults(id));
    }
    return () => {
      dispatch(clear());
    };
  }, [dispatch, id, isAuthenticated]);

  useEffect(() => {
    if (doc?.g_event_graceids && isAuthenticated) {
      dispatch(fetchSupereventEvents(doc.g_event_graceids));
    }
  }, [dispatch, doc?.g_event_graceids, isAuthenticated]);

  if (!id) {
    return (
      <Alert severity="error" onClick={() => navigate("/superevents")}>
        Missing superevent id
      </Alert>
    );
  }

  return (
    <Box sx={{ display: "flex", gap: 2 }}>
      <Box sx={{ flexGrow: 1, minWidth: 0 }}>
        {error && <Alert severity="error">{error}</Alert>}
        <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 2 }}>
          <Typography variant="h5">
            <code>{id}</code>
          </Typography>
          {loading && <CircularProgress size={16} />}
          <Box sx={{ flexGrow: 1 }} />
          <Tooltip title="Toggle properties">
            <IconButton onClick={() => setPropsOpen((v) => !v)}>
              <InfoOutlinedIcon />
            </IconButton>
          </Tooltip>
        </Stack>
        <Paper sx={{ mb: 2 }}>
          <Tabs
            value={tab}
            onChange={(_, v) => setTab(v)}
            variant="scrollable"
            scrollButtons="auto"
          >
            <Tab label="Overview" />
            <Tab label="Localization" />
            <Tab label="Annotations" />
            <Tab label="Alerts" />
            <Tab label="Cross-matches" />
            <Tab label="Nu searches" />
          </Tabs>
        </Paper>
        {tab === 0 && (
          <OverviewTab
            events={events}
            graceids={doc?.g_event_graceids ?? []}
            isAuthenticated={isAuthenticated}
          />
        )}
        {tab === 1 && (
          <LocalizationTab
            supereventId={id}
            hasSkymap={Boolean(doc?.skymap_summary)}
            skymapCenter={
              doc?.skymap_summary?.center_ra != null &&
              doc?.skymap_summary?.center_dec != null
                ? {
                    ra: doc.skymap_summary.center_ra,
                    dec: doc.skymap_summary.center_dec,
                  }
                : undefined
            }
            requests={localizeRequests}
            results={localizeResults}
            isAuthenticated={isAuthenticated}
          />
        )}
        {tab === 2 && <AnnotationsPanel supereventId={id} />}
        {tab === 3 && <AlertsPanel supereventId={id} />}
        {tab === 4 && <CrossMatchesPanel supereventId={id} />}
        {tab === 5 && <IceCubeLvkSearchesPanel supereventId={id} />}
      </Box>

      <Drawer
        variant="persistent"
        anchor="right"
        open={propsOpen}
        sx={{
          width: propsOpen ? DRAWER_WIDTH : 0,
          flexShrink: 0,
          "& .MuiDrawer-paper": {
            width: DRAWER_WIDTH,
            position: "relative",
            border: "none",
            bgcolor: "transparent",
          },
        }}
      >
        <PropertiesPanel id={id} />
      </Drawer>
    </Box>
  );
}

function PropertiesPanel({ id }: { id: string }) {
  const doc = useAppSelector((s) => s.superevent.doc);
  if (!doc) return <Paper sx={{ p: 2 }}>Loading…</Paper>;
  const rows: Array<[string, React.ReactNode]> = [
    ["ID", <code key="id">{id}</code>],
    ["Preferred GraceID", <code key="g">{doc.preferred_graceid}</code>],
    ["Preferred SNR", doc.preferred_snr.toFixed(3)],
    ["t₀ (GPS)", doc.t_0.toFixed(3)],
    ["t_start", doc.t_start.toFixed(3)],
    ["t_end", doc.t_end.toFixed(3)],
    ["G-events", doc.g_event_graceids.length],
    [
      "Skymap",
      doc.skymap_summary ? (
        <Chip
          size="small"
          color="success"
          label={`${(doc.skymap_summary.bytes_size / 1024).toFixed(0)} KB · ${doc.skymap_summary.elapsed_ms} ms`}
        />
      ) : (
        <Chip size="small" label="none" />
      ),
    ],
  ];
  return (
    <Paper sx={{ p: 2 }}>
      <Typography variant="subtitle2" gutterBottom>
        Properties
      </Typography>
      <Divider sx={{ mb: 1 }} />
      <Stack spacing={0.5}>
        {rows.map(([k, v]) => (
          <Box
            key={k}
            sx={{ display: "flex", justifyContent: "space-between", gap: 1 }}
          >
            <Typography variant="caption" color="text.secondary">
              {k}
            </Typography>
            <Typography variant="body2" sx={{ textAlign: "right" }}>
              {v}
            </Typography>
          </Box>
        ))}
      </Stack>
    </Paper>
  );
}

function OverviewTab({
  events,
  graceids,
  isAuthenticated,
}: {
  events: import("../types/api").EventDoc[];
  graceids: string[];
  isAuthenticated: boolean;
}) {
  if (graceids.length === 0) {
    return <Typography color="text.secondary">No g-events linked.</Typography>;
  }
  return (
    <Paper>
      {!isAuthenticated && (
        <Box sx={{ p: 2 }}>
          <Alert severity="info">
            Sign in to see G-event details (pipeline, SNR, FAR, M_chirp).
            Anonymous visitors can see the constituent GraceIDs but not the
            per-event data.
          </Alert>
        </Box>
      )}
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>GraceID</TableCell>
            <TableCell>Pipeline</TableCell>
            <TableCell>IFOs</TableCell>
            <TableCell align="right">SNR</TableCell>
            <TableCell align="right">FAR</TableCell>
            <TableCell align="right">M_chirp</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {graceids.map((g) => {
            const e = events.find((ev) => ev._id === g);
            return (
              <TableRow key={g}>
                <TableCell>
                  <code>{g}</code>
                </TableCell>
                <TableCell>{e?.pipeline ?? "—"}</TableCell>
                <TableCell>{e?.ifos ?? "—"}</TableCell>
                <TableCell align="right">{e?.snr?.toFixed(2) ?? "—"}</TableCell>
                <TableCell align="right">
                  {e?.far !== undefined ? e.far.toExponential(2) : "—"}
                </TableCell>
                <TableCell align="right">
                  {e?.mchirp !== undefined && e.mchirp !== null
                    ? e.mchirp.toFixed(2)
                    : "—"}
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </Paper>
  );
}

/// Outer-join `LocalizeRequestDoc` and `LocalizeResultDoc` on the
/// shared `_id` (which is `request_id` in both schemas — see
/// `archive.rs::LocalizeRequestDoc/LocalizeResultDoc`). Requests
/// without a matching result render as "pending"; results without
/// a request are rare but possible (operator-driven backfill) and
/// still render with what we have.
interface LocalizeJobRow {
  _id: string;
  graceid: string;
  pipeline: string | null;
  status: "ok" | "error" | null;
  elapsed_ms: number | null;
  skymap_fits_bytes: number | null;
}

function joinLocalizeJobs(
  requests: import("../types/api").LocalizeRequestDoc[],
  results: import("../types/api").LocalizeResultDoc[],
): LocalizeJobRow[] {
  const byId = new Map<string, LocalizeJobRow>();
  for (const r of requests) {
    byId.set(r._id, {
      _id: r._id,
      graceid: r.graceid,
      pipeline: r.pipeline,
      status: null,
      elapsed_ms: null,
      skymap_fits_bytes: null,
    });
  }
  for (const r of results) {
    const prev = byId.get(r._id);
    byId.set(r._id, {
      _id: r._id,
      graceid: r.graceid,
      pipeline: prev?.pipeline ?? null,
      status: r.status,
      elapsed_ms: r.elapsed_ms,
      skymap_fits_bytes: r.skymap_fits_bytes ?? null,
    });
  }
  return Array.from(byId.values());
}

// Deterministic per-trigger color palette. Hashing the trigger
// label into a hue keeps the same GRB the same color across
// renders, but does NOT collide more than once per ~20 distinct
// triggers — fine for the operator's at-a-glance use.
function grbOverlayColor(seed: string): string {
  let h = 0;
  for (let i = 0; i < seed.length; i++)
    h = (h * 31 + seed.charCodeAt(i)) & 0xffff;
  const hue = h % 360;
  return `hsl(${hue}, 80%, 60%)`;
}

function LocalizationTab({
  supereventId,
  hasSkymap,
  skymapCenter,
  requests,
  results,
  isAuthenticated,
}: {
  supereventId: string;
  hasSkymap: boolean;
  skymapCenter?: { ra: number; dec: number };
  requests: import("../types/api").LocalizeRequestDoc[];
  results: import("../types/api").LocalizeResultDoc[];
  isAuthenticated: boolean;
}) {
  // Load cross-matches if the user lands here directly. The
  // CrossMatchesPanel also fetches them; the duck dedups so
  // double-fetches are harmless.
  const dispatch = useAppDispatch();
  const crossMatches = useAppSelector(
    (s) => s.crossMatches.bySuperevent[supereventId] ?? [],
  );
  useEffect(() => {
    dispatch(fetchCrossMatches(supereventId));
  }, [dispatch, supereventId]);

  // Overlay exactly the matches the operator has committed
  // (starred via the cross-matches table). The cross-matches table
  // is where significance gets evaluated — sorted by joint FAR,
  // p_value shown per row — and the skymap reflects only the
  // operator's decisions. We previously also auto-overlaid any
  // `p_value < 0.05` match, but with `associated: bool` (no
  // tristate) the rule couldn't distinguish "operator rejected"
  // from "operator hasn't looked yet" — so de-starring a
  // significant match silently left it on the map.
  const overlayMatches = useMemo(
    () => crossMatches.filter((m) => m.associated === true),
    [crossMatches],
  );

  const extraMocs = useMemo(
    () =>
      overlayMatches.map((m) => ({
        id: `${m.instrument}/${m.trigger_id}`,
        url: `/api/grb-triggers/${encodeURIComponent(m.instrument)}/${encodeURIComponent(m.trigger_id)}/skymap`,
        label: `${m.instrument} ${m.trigger_id}${m.associated ? " ⭐" : ""}`,
        color: grbOverlayColor(`${m.instrument}/${m.trigger_id}`),
        options: {
          color: grbOverlayColor(`${m.instrument}/${m.trigger_id}`),
          // Associated overlays are slightly more opaque so the
          // operator can pick them out at a glance.
          opacity: m.associated ? 0.7 : 0.4,
          lineWidth: m.associated ? 2.0 : 1.5,
          fill: false,
        },
      })),
    [overlayMatches],
  );

  // Visibility state. Default = everything visible. Clicking a
  // legend chip toggles that layer. We keep the set as a fresh
  // object on every update so React's referential-equality check
  // triggers a re-render (and the AladinViewer re-mounts via its
  // dependency-array key).
  const [hidden, setHidden] = useState<Set<string>>(() => new Set());
  // Reset visibility when the cross-match set changes — otherwise
  // a freshly-added GRB inherits "hidden" if its id collides with
  // a prior layer the user toggled off.
  const allKnownIds = useMemo(() => {
    const s = new Set<string>([GW_LAYER_IDS.cr90, GW_LAYER_IDS.cr50]);
    extraMocs.forEach((m) => s.add(m.id));
    return s;
  }, [extraMocs]);
  useEffect(() => {
    setHidden((prev) => {
      const next = new Set<string>();
      for (const id of prev) if (allKnownIds.has(id)) next.add(id);
      return next.size === prev.size ? prev : next;
    });
  }, [allKnownIds]);

  const visibleLayerIds = useMemo(() => {
    const s = new Set(allKnownIds);
    hidden.forEach((id) => s.delete(id));
    return s;
  }, [allKnownIds, hidden]);

  const toggle = (id: string) =>
    setHidden((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  // Reusable legend chip: shows a color swatch + label, with a
  // strikethrough when the layer is hidden. The whole row is
  // clickable.
  const LegendChip = ({
    id,
    label,
    swatch,
  }: {
    id: string;
    label: string;
    swatch: React.ReactNode;
  }) => {
    const isHidden = hidden.has(id);
    return (
      <Stack
        direction="row"
        spacing={0.5}
        alignItems="center"
        onClick={() => toggle(id)}
        sx={{
          cursor: "pointer",
          userSelect: "none",
          opacity: isHidden ? 0.4 : 1,
          "&:hover": { opacity: isHidden ? 0.6 : 0.85 },
        }}
      >
        {swatch}
        <Typography
          variant="caption"
          sx={{
            textDecoration: isHidden ? "line-through" : "none",
          }}
        >
          {label}
        </Typography>
      </Stack>
    );
  };

  return (
    <Grid container spacing={2}>
      <Grid size={{ xs: 12, md: 7 }}>
        <Paper sx={{ p: 1 }}>
          {hasSkymap ? (
            <>
              <AladinViewer
                contourUrlTemplate={`/api/superevents/${supereventId}/contour?level={level}`}
                extraMocs={extraMocs}
                visibleLayerIds={visibleLayerIds}
                initialCenter={skymapCenter}
                height={520}
              />
              <Stack
                direction="row"
                spacing={1.5}
                sx={{ mt: 1, flexWrap: "wrap", alignItems: "center" }}
              >
                <Typography variant="caption" color="text.secondary">
                  Layers (click to toggle):
                </Typography>
                <LegendChip
                  id={GW_LAYER_IDS.cr90}
                  label="GW 90% CR"
                  swatch={
                    <Box
                      sx={{
                        width: 12,
                        height: 12,
                        bgcolor: "#84CDFF",
                        opacity: 0.6,
                        borderRadius: 0.5,
                      }}
                    />
                  }
                />
                <LegendChip
                  id={GW_LAYER_IDS.cr50}
                  label="GW 50% CR"
                  swatch={
                    <Box
                      sx={{
                        width: 12,
                        height: 12,
                        border: "2px solid #FFB347",
                        borderRadius: 0.5,
                      }}
                    />
                  }
                />
                {extraMocs.map((m) => (
                  <LegendChip
                    key={m.id}
                    id={m.id}
                    label={m.label}
                    swatch={
                      <Box
                        sx={{
                          width: 12,
                          height: 12,
                          border: `2px solid ${m.color}`,
                          borderRadius: 0.5,
                        }}
                      />
                    }
                  />
                ))}
              </Stack>
            </>
          ) : (
            <Box
              sx={{
                height: 520,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                color: "text.secondary",
              }}
            >
              No skymap attached yet.
            </Box>
          )}
        </Paper>
      </Grid>
      <Grid size={{ xs: 12, md: 5 }}>
        <Stack spacing={2}>
          {!isAuthenticated && (
            <Alert severity="info">
              Localize-request and localize-result audit logs require sign-in.
            </Alert>
          )}
          <Paper>
            <Typography variant="subtitle2" sx={{ p: 1.5, pb: 1 }}>
              Localization jobs
            </Typography>
            <Typography
              variant="caption"
              sx={{
                display: "block",
                px: 1.5,
                pb: 1.5,
                color: "text.secondary",
              }}
            >
              Audit trail of the gw_clusterer → localizer Kafka loop, joined on
              request_id. "pending" rows are requests we haven't seen a result
              for yet (worker not running, or the skymap arrived via another
              path — e.g. operator backfill).
            </Typography>
            <Divider />
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>GraceID</TableCell>
                  <TableCell>Pipeline</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell align="right">Elapsed</TableCell>
                  <TableCell align="right">FITS bytes</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {joinLocalizeJobs(requests, results).map((r) => (
                  <TableRow key={r._id}>
                    <TableCell>
                      <code>{r.graceid}</code>
                    </TableCell>
                    <TableCell>{r.pipeline ?? "—"}</TableCell>
                    <TableCell>
                      {r.status ? (
                        <Chip
                          size="small"
                          color={r.status === "ok" ? "success" : "error"}
                          label={r.status}
                        />
                      ) : (
                        <Chip size="small" label="pending" />
                      )}
                    </TableCell>
                    <TableCell align="right">
                      {r.elapsed_ms != null ? `${r.elapsed_ms} ms` : "—"}
                    </TableCell>
                    <TableCell align="right">
                      {r.skymap_fits_bytes ?? "—"}
                    </TableCell>
                  </TableRow>
                ))}
                {requests.length === 0 && results.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={5} sx={{ color: "text.secondary" }}>
                      No localization jobs recorded for this superevent.
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </Paper>
        </Stack>
      </Grid>
    </Grid>
  );
}
