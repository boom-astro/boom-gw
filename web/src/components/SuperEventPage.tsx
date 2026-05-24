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

import { useEffect, useState } from "react";
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
import { AladinViewer } from "./AladinViewer";
import { AnnotationsPanel } from "./AnnotationsPanel";
import { AlertsPanel } from "./AlertsPanel";
import { CrossMatchesPanel } from "./CrossMatchesPanel";

const DRAWER_WIDTH = 360;

export function SuperEventPage() {
  const { id = "" } = useParams();
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const [tab, setTab] = useState(0);
  const [propsOpen, setPropsOpen] = useState(true);
  const { doc, events, localizeRequests, localizeResults, loading, error } =
    useAppSelector((s) => s.superevent);

  useEffect(() => {
    if (!id) return;
    dispatch(fetchSuperevent(id));
    dispatch(fetchLocalizeRequests(id));
    dispatch(fetchLocalizeResults(id));
    return () => {
      dispatch(clear());
    };
  }, [dispatch, id]);

  useEffect(() => {
    if (doc?.g_event_graceids) {
      dispatch(fetchSupereventEvents(doc.g_event_graceids));
    }
  }, [dispatch, doc?.g_event_graceids]);

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
          </Tabs>
        </Paper>
        {tab === 0 && (
          <OverviewTab events={events} graceids={doc?.g_event_graceids ?? []} />
        )}
        {tab === 1 && (
          <LocalizationTab
            supereventId={id}
            hasSkymap={Boolean(doc?.skymap_summary)}
            requests={localizeRequests}
            results={localizeResults}
          />
        )}
        {tab === 2 && <AnnotationsPanel supereventId={id} />}
        {tab === 3 && <AlertsPanel supereventId={id} />}
        {tab === 4 && <CrossMatchesPanel supereventId={id} />}
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
}: {
  events: import("../types/api").EventDoc[];
  graceids: string[];
}) {
  if (graceids.length === 0) {
    return <Typography color="text.secondary">No g-events linked.</Typography>;
  }
  return (
    <Paper>
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
                <TableCell align="right">
                  {e?.snr?.toFixed(2) ?? "—"}
                </TableCell>
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

function LocalizationTab({
  supereventId,
  hasSkymap,
  requests,
  results,
}: {
  supereventId: string;
  hasSkymap: boolean;
  requests: import("../types/api").LocalizeRequestDoc[];
  results: import("../types/api").LocalizeResultDoc[];
}) {
  return (
    <Grid container spacing={2}>
      <Grid size={{ xs: 12, md: 7 }}>
        <Paper sx={{ p: 1 }}>
          {hasSkymap ? (
            <AladinViewer
              contourUrlTemplate={`/api/superevents/${supereventId}/contour?level={level}`}
              height={520}
            />
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
          <Paper>
            <Typography variant="subtitle2" sx={{ p: 1.5, pb: 1 }}>
              Localize requests
            </Typography>
            <Divider />
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>GraceID</TableCell>
                  <TableCell>Pipeline</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {requests.map((r) => (
                  <TableRow key={r._id}>
                    <TableCell>
                      <code>{r.graceid}</code>
                    </TableCell>
                    <TableCell>{r.pipeline}</TableCell>
                  </TableRow>
                ))}
                {requests.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={2}>None</TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </Paper>
          <Paper>
            <Typography variant="subtitle2" sx={{ p: 1.5, pb: 1 }}>
              Localize results
            </Typography>
            <Divider />
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>GraceID</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell align="right">Elapsed</TableCell>
                  <TableCell align="right">FITS bytes</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {results.map((r) => (
                  <TableRow key={r._id}>
                    <TableCell>
                      <code>{r.graceid}</code>
                    </TableCell>
                    <TableCell>
                      <Chip
                        size="small"
                        color={r.status === "ok" ? "success" : "error"}
                        label={r.status}
                      />
                    </TableCell>
                    <TableCell align="right">{r.elapsed_ms} ms</TableCell>
                    <TableCell align="right">
                      {r.skymap_fits_bytes ?? "—"}
                    </TableCell>
                  </TableRow>
                ))}
                {results.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={4}>None</TableCell>
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
