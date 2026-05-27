// Drill-down for one GRB trigger_id. Fermi-GBM emits FLT (autonomous
// flight, seconds), GND (ground refinement, minutes), and FIN (final,
// hours) updates for the same burst; this page shows all of them as
// a single chain so operators can see the localization tighten over
// time. The Aladin viewer overlays the highest-priority stage's MOC
// FITS (FIN > GND > FLT) — what the SPA's list rows already display
// as "best".
//
// Data shape: one `GrbTriggerSummary` from
// `/api/grb-trigger-summaries/{trigger_id}` with `stages` already
// sorted FIN-first by the server-side aggregation.

import { useEffect } from "react";
import {
  Alert,
  Box,
  Chip,
  CircularProgress,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import { useNavigate, useParams } from "react-router-dom";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import { fetchGrbTriggerSummary } from "../ducks/grbTriggerSummaries";
import { useAppDispatch, useAppSelector } from "../store";
import { AladinViewer, GW_LAYER_IDS } from "./AladinViewer";

dayjs.extend(utc);

function fmtGps(t: number | null | undefined): string {
  if (t == null || !Number.isFinite(t) || t === 0) return "—";
  const unix = t + 315964800 - 18;
  return dayjs.unix(unix).utc().format("YYYY-MM-DD HH:mm:ss[Z]");
}

/// Strip the `-FIN`/`-GND`/`-FLT`/`-SUBTHRESH` suffix off the
/// instrument label for the per-stage badge — keeps the column
/// readable when every row is "Fermi-GBM-something".
function stageBadge(instrument: string): string {
  const m = instrument.match(/-(FIN|GND|FLT|SUBTHRESH)$/);
  return m ? m[1] : instrument;
}

export function GrbTriggerPage() {
  const { triggerId = "" } = useParams();
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const summary = useAppSelector(
    (s) => s.grbTriggerSummaries.byId[triggerId],
  );
  const loading = useAppSelector(
    (s) => s.grbTriggerSummaries.detailLoading,
  );
  const error = useAppSelector((s) => s.grbTriggerSummaries.error);

  useEffect(() => {
    if (triggerId) dispatch(fetchGrbTriggerSummary(triggerId));
  }, [dispatch, triggerId]);

  if (!triggerId) {
    return <Alert severity="error">Missing trigger_id in URL</Alert>;
  }

  return (
    <Stack spacing={2}>
      <Box>
        <Typography
          variant="caption"
          color="text.secondary"
          sx={{ cursor: "pointer" }}
          onClick={() => navigate("/external-streams")}
        >
          ← External streams
        </Typography>
        <Typography variant="h5">
          GRB trigger <code>{triggerId}</code>
        </Typography>
        {loading && <CircularProgress size={16} sx={{ ml: 1 }} />}
        {error && <Alert severity="error">{error}</Alert>}
        {summary && (
          <Typography variant="body2" color="text.secondary">
            Best stage: <strong>{summary.best_instrument}</strong> ·{" "}
            {summary.stage_count} stage{summary.stage_count === 1 ? "" : "s"} ·
            trigger_time {fmtGps(summary.trigger_time)}
          </Typography>
        )}
      </Box>

      {summary && (
        <Paper sx={{ p: 1 }}>
          <Typography variant="subtitle2" sx={{ p: 1 }}>
            Refinement chain
          </Typography>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Stage</TableCell>
                <TableCell>Instrument</TableCell>
                <TableCell>Trigger time (UTC)</TableCell>
                <TableCell align="right">RA (°)</TableCell>
                <TableCell align="right">Dec (°)</TableCell>
                <TableCell align="right">Err radius (°)</TableCell>
                <TableCell align="right">Significance</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {summary.stages.map((s, i) => (
                <TableRow
                  key={s.instrument + "@" + s.trigger_time + "/" + i}
                >
                  <TableCell>
                    <Chip
                      size="small"
                      label={stageBadge(s.instrument)}
                      color={
                        s.instrument === summary.best_instrument
                          ? "primary"
                          : "default"
                      }
                    />
                  </TableCell>
                  <TableCell>{s.instrument}</TableCell>
                  <TableCell>{fmtGps(s.trigger_time)}</TableCell>
                  <TableCell align="right">
                    {s.position?.ra != null
                      ? s.position.ra.toFixed(2)
                      : "—"}
                  </TableCell>
                  <TableCell align="right">
                    {s.position?.dec != null
                      ? s.position.dec.toFixed(2)
                      : "—"}
                  </TableCell>
                  <TableCell align="right">
                    {s.error_radius_deg != null
                      ? s.error_radius_deg.toFixed(2)
                      : "—"}
                  </TableCell>
                  <TableCell align="right">
                    {s.significance != null && s.significance > 0
                      ? s.significance.toFixed(2)
                      : "—"}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Paper>
      )}

      {summary && summary.ra != null && summary.dec != null && (
        <Paper sx={{ p: 1 }}>
          <Typography variant="subtitle2" sx={{ p: 1 }}>
            Sky map ({summary.best_instrument})
          </Typography>
          <AladinViewer
            // GRB triggers don't have GW credible-region contours;
            // we pass an empty template and let the per-trigger MOC
            // overlay carry the visualization.
            contourUrlTemplate={null}
            extraMocs={[
              {
                id: `${summary.best_instrument}/${triggerId}`,
                url: `/api/grb-triggers/${encodeURIComponent(summary.best_instrument)}/${encodeURIComponent(triggerId)}/skymap`,
                label: `${summary.best_instrument} ${triggerId}`,
                options: {
                  color: "hsl(0, 80%, 60%)",
                  opacity: 0.6,
                  lineWidth: 2,
                  fill: false,
                },
              },
            ]}
            visibleLayerIds={
              new Set<string>([
                GW_LAYER_IDS.cr90,
                GW_LAYER_IDS.cr50,
                `${summary.best_instrument}/${triggerId}`,
              ])
            }
            initialCenter={{ ra: summary.ra, dec: summary.dec }}
            height={520}
          />
        </Paper>
      )}
    </Stack>
  );
}
