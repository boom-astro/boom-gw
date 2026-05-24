// Cross-matches tab for a superevent. Shows the list of GW × GRB
// cross-match results computed against this superevent, plus a
// small inline form to trigger a new one by (instrument,
// trigger_id) — useful for ad-hoc operator queries.
//
// The math (RAVEN spatial integral + joint FAR) runs server-side
// in gw-api; this panel only renders the results.

import { useEffect, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import {
  clearError,
  createCrossMatch,
  fetchCrossMatches,
} from "../ducks/crossMatches";
import { useAppDispatch, useAppSelector } from "../store";
import type { CrossMatchDoc } from "../types/api";

function fmtComputed(raw: CrossMatchDoc["computed_at"]): string {
  if (typeof raw === "string") return raw;
  const inner = raw?.$date;
  if (!inner) return "";
  if (typeof inner === "string") return inner;
  const ms = inner.$numberLong ? Number(inner.$numberLong) : 0;
  return new Date(ms).toISOString();
}

function fmtFar(joint: number | null | undefined): string {
  if (joint == null || !Number.isFinite(joint)) return "—";
  // Display sufficient digits to span the realistic range
  // (1e-10/yr for golden coincidences, 1e+2/yr for chance).
  if (joint === 0) return "0";
  return joint.toExponential(2);
}

interface Props {
  supereventId: string;
}

export function CrossMatchesPanel({ supereventId }: Props) {
  const dispatch = useAppDispatch();
  const items = useAppSelector(
    (s) => s.crossMatches.bySuperevent[supereventId] ?? [],
  );
  const loading = useAppSelector((s) => s.crossMatches.loading);
  const computing = useAppSelector((s) => s.crossMatches.computing);
  const error = useAppSelector((s) => s.crossMatches.error);

  const [instrument, setInstrument] = useState("Fermi-GBM-FIN");
  const [triggerId, setTriggerId] = useState("");

  useEffect(() => {
    dispatch(fetchCrossMatches(supereventId));
  }, [dispatch, supereventId]);

  async function onCompute() {
    if (!instrument.trim() || !triggerId.trim()) return;
    await dispatch(
      createCrossMatch({
        supereventId,
        instrument: instrument.trim(),
        triggerId: triggerId.trim(),
      }),
    );
  }

  return (
    <Stack spacing={2}>
      <Paper sx={{ p: 2 }}>
        <Typography variant="subtitle2" gutterBottom>
          Run a cross-match
        </Typography>
        <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: "block" }}>
          The GRB trigger must already exist in the archive
          (POST /api/grb-triggers, or via the live GCN consumer).
        </Typography>
        <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap">
          <TextField
            size="small"
            label="Instrument"
            value={instrument}
            onChange={(e) => setInstrument(e.target.value)}
            sx={{ minWidth: 180 }}
          />
          <TextField
            size="small"
            label="Trigger ID"
            value={triggerId}
            onChange={(e) => setTriggerId(e.target.value)}
            sx={{ minWidth: 200 }}
          />
          <Button
            variant="contained"
            onClick={onCompute}
            disabled={computing || !triggerId.trim() || !instrument.trim()}
            startIcon={computing ? <CircularProgress size={14} /> : null}
          >
            {computing ? "Computing…" : "Compute"}
          </Button>
        </Stack>
        {error && (
          <Alert
            severity="error"
            sx={{ mt: 2 }}
            onClose={() => dispatch(clearError())}
          >
            {error}
          </Alert>
        )}
      </Paper>

      <Paper>
        <Stack
          direction="row"
          alignItems="center"
          spacing={1}
          sx={{ px: 2, pt: 1.5 }}
        >
          <Typography variant="subtitle2">Cross-matches</Typography>
          {loading && <CircularProgress size={14} />}
          <Box sx={{ flexGrow: 1 }} />
          <Typography variant="caption" color="text.secondary">
            {items.length} result{items.length === 1 ? "" : "s"}
          </Typography>
        </Stack>
        <Divider sx={{ mt: 1 }} />
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Trigger</TableCell>
              <TableCell align="right">Δt (s)</TableCell>
              <TableCell align="right">Spatial overlap</TableCell>
              <TableCell>CR membership</TableCell>
              <TableCell align="right">
                <Tooltip
                  title="Empirical p-value from N random sky rotations of the GRB cone. Lower = more significant."
                >
                  <span>p-value</span>
                </Tooltip>
              </TableCell>
              <TableCell align="right">
                <Tooltip title="Bias-corrected joint FAR using the empirical p-value (RAVEN remapped formula). Falls back to the classical RAVEN FAR when no p-value was computed.">
                  <span>Joint FAR / yr</span>
                </Tooltip>
              </TableCell>
              <TableCell>Computed</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {items.map((m) => (
              <TableRow key={`${m.instrument}/${m.trigger_id}`}>
                <TableCell>
                  <Tooltip title={m.instrument}>
                    <code>{m.trigger_id}</code>
                  </Tooltip>
                  <Typography
                    variant="caption"
                    color="text.secondary"
                    sx={{ display: "block" }}
                  >
                    {m.instrument}
                  </Typography>
                </TableCell>
                <TableCell align="right">
                  {m.time_offset_sec.toFixed(2)}
                </TableCell>
                <TableCell align="right">
                  {m.spatial_overlap.toExponential(2)}
                </TableCell>
                <TableCell>
                  <Stack direction="row" spacing={0.5}>
                    {m.in_50cr && (
                      <Chip size="small" color="warning" label="50% CR" />
                    )}
                    {m.in_90cr && (
                      <Chip size="small" color="info" label="90% CR" />
                    )}
                    {!m.in_50cr && !m.in_90cr && (
                      <Chip size="small" label="outside" />
                    )}
                  </Stack>
                </TableCell>
                <TableCell align="right">
                  {m.p_value != null ? (
                    <Tooltip
                      title={
                        m.p_value_trials
                          ? `${m.p_value_trials} Monte Carlo rotation trials`
                          : "empirical p-value"
                      }
                    >
                      <span>{m.p_value.toExponential(2)}</span>
                    </Tooltip>
                  ) : (
                    "—"
                  )}
                </TableCell>
                <TableCell align="right">
                  {fmtFar(
                    m.joint_far_remapped_per_year ?? m.joint_far_per_year,
                  )}
                </TableCell>
                <TableCell>
                  <Typography variant="caption" color="text.secondary">
                    {fmtComputed(m.computed_at)}
                  </Typography>
                </TableCell>
              </TableRow>
            ))}
            {items.length === 0 && !loading && (
              <TableRow>
                <TableCell colSpan={7}>
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ py: 3, textAlign: "center" }}
                  >
                    No cross-matches yet. Compute one above, or wait for the
                    GCN consumer to populate them automatically.
                  </Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </Paper>
    </Stack>
  );
}
