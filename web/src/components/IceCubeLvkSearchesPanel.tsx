// "ν searches" panel — IceCube LVK Coincident Neutrino Track
// Search results attached to a specific superevent. Each search
// is itself the cross-match result (IceCube ran the search
// against the GW localization), so this view summarizes the
// upstream pipeline's findings rather than offering a re-run.

import {
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
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import { useAppSelector } from "../store";

dayjs.extend(utc);

function fmtGps(t: number | null | undefined): string {
  if (t == null || !Number.isFinite(t) || t === 0) return "—";
  const unix = t + 315964800 - 18;
  return dayjs.unix(unix).utc().format("YYYY-MM-DD HH:mm:ss[Z]");
}

function fmtPval(p: number | null | undefined): string {
  if (p == null || !Number.isFinite(p)) return "—";
  // p-values span many decades; show 3 sig figs in scientific
  // notation when they're small so the operator can see the
  // significance at a glance.
  return p < 0.01 ? p.toExponential(2) : p.toFixed(3);
}

export function IceCubeLvkSearchesPanel({
  supereventId,
}: {
  supereventId: string;
}) {
  const items = useAppSelector(
    (s) => s.icecubeLvk.bySuperevent[supereventId] ?? [],
  );
  const loading = useAppSelector((s) => s.icecubeLvk.loading);

  if (loading && items.length === 0) {
    return (
      <Paper sx={{ p: 3, display: "flex", justifyContent: "center" }}>
        <CircularProgress size={20} />
      </Paper>
    );
  }
  if (items.length === 0) {
    return (
      <Paper sx={{ p: 3 }}>
        <Typography variant="body2" color="text.secondary">
          No IceCube LVK Nu Track Search results have been received for this
          superevent yet. The gw-gcn-consumer subscribes to{" "}
          <code>gcn.notices.icecube.lvk_nu_track_search</code> by default;
          alerts will appear here automatically when IceCube publishes a
          search against this superevent.
        </Typography>
      </Paper>
    );
  }

  return (
    <Stack spacing={2}>
      {items.map((s) => (
        <Paper key={s._id.alert_time_gps} sx={{ p: 2 }}>
          <Stack
            direction="row"
            spacing={2}
            alignItems="center"
            sx={{ mb: 1.5 }}
          >
            <Typography variant="subtitle1">
              IceCube LVK Nu Track Search
            </Typography>
            <Chip
              size="small"
              label={`${s.n_events_coincident} coincident track${
                s.n_events_coincident === 1 ? "" : "s"
              }`}
              color={s.n_events_coincident > 0 ? "primary" : "default"}
              variant={s.n_events_coincident > 0 ? "filled" : "outlined"}
            />
            <Box sx={{ flexGrow: 1 }} />
            <Typography variant="caption" color="text.secondary">
              alert {fmtGps(s.alert_time)}
            </Typography>
          </Stack>

          <Stack direction="row" spacing={4} sx={{ mb: 2 }}>
            <Stat label="p (generic)" value={fmtPval(s.pval_generic)} />
            <Stat label="p (Bayesian)" value={fmtPval(s.pval_bayesian)} />
            <Stat
              label="livetime"
              value={
                s.observation_livetime != null
                  ? `${s.observation_livetime.toFixed(0)} s`
                  : "—"
              }
            />
            <Stat
              label="window"
              value={
                s.observation_start != null && s.observation_stop != null
                  ? `${fmtGps(s.observation_start)} → ${fmtGps(s.observation_stop)}`
                  : "—"
              }
            />
            {s.most_probable_direction && (
              <Stat
                label="most prob. direction"
                value={`(${s.most_probable_direction.ra.toFixed(2)}, ${s.most_probable_direction.dec.toFixed(2)})°`}
              />
            )}
          </Stack>

          {s.coincident_events.length > 0 && (
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Event ID</TableCell>
                  <TableCell align="right">Δt (s)</TableCell>
                  <TableCell align="right">RA (°)</TableCell>
                  <TableCell align="right">Dec (°)</TableCell>
                  <TableCell align="right">Err (°)</TableCell>
                  <TableCell align="right">p (generic)</TableCell>
                  <TableCell align="right">p (Bayesian)</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {s.coincident_events.map((e) => (
                  <TableRow key={e.id}>
                    <TableCell>
                      <code>{e.id}</code>
                    </TableCell>
                    <TableCell align="right">{e.event_dt.toFixed(2)}</TableCell>
                    <TableCell align="right">
                      {e.localization?.ra != null
                        ? e.localization.ra.toFixed(2)
                        : "—"}
                    </TableCell>
                    <TableCell align="right">
                      {e.localization?.dec != null
                        ? e.localization.dec.toFixed(2)
                        : "—"}
                    </TableCell>
                    <TableCell align="right">
                      {e.localization?.uncertainty_arcsec != null
                        ? (e.localization.uncertainty_arcsec / 3600).toFixed(2)
                        : "—"}
                    </TableCell>
                    <TableCell align="right">
                      {fmtPval(e.event_pval_generic)}
                    </TableCell>
                    <TableCell align="right">
                      {fmtPval(e.event_pval_bayesian)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}

          {(s.flux_sensitivity_range || s.sensitive_energy_range) && (
            <Box sx={{ mt: 2 }}>
              <Typography variant="caption" color="text.secondary">
                {s.flux_sensitivity_range &&
                  `Flux sensitivity (E²dN/dE, 90% region): ${s.flux_sensitivity_range[0].toExponential(2)}–${s.flux_sensitivity_range[1].toExponential(2)} GeV cm⁻²`}
                {s.sensitive_energy_range &&
                  ` · sensitive E range: ${s.sensitive_energy_range[0].toExponential(1)}–${s.sensitive_energy_range[1].toExponential(1)} GeV`}
              </Typography>
            </Box>
          )}
        </Paper>
      ))}
    </Stack>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <Box>
      <Typography variant="caption" color="text.secondary" display="block">
        {label}
      </Typography>
      <Typography variant="body2">{value}</Typography>
    </Box>
  );
}
