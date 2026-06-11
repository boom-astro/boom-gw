// Cross-matches tab for a superevent. The operator-facing flow:
//
//   1. Click "Scan ±window" — the server pulls every GRB trigger
//      and BOOM optical alert with arrival time inside the window,
//      computes spatial overlap + Monte Carlo p-value + remapped
//      joint FAR for each, persists them, and returns the list
//      sorted by significance.
//   2. The table renders the ranked candidates. Each row has a
//      star toggle that flips the `associated` flag, the
//      operator's commitment that the match is real. Aladin
//      overlays on the Localization tab respect the same flag.

import { useEffect, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  Paper,
  Select,
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
import { Link as RouterLink } from "react-router-dom";
import StarBorderIcon from "@mui/icons-material/StarBorder";
import StarIcon from "@mui/icons-material/Star";
import {
  clearError,
  fetchCrossMatches,
  fetchFilteredCrossMatches,
  scanCrossMatches,
  setCrossMatchAssociated,
} from "../ducks/crossMatches";
import { fetchScienceFilters } from "../ducks/scienceFilters";
import { useAppDispatch, useAppSelector } from "../store";
import type { CrossMatchDoc, FilteredCrossMatch } from "../types/api";

// Map an instrument label to a coarse messenger category. Drives
// the small color-coded chip in the trigger column so a scan
// result with a dozen rows is scannable at a glance.
type MessengerCategory = "gamma" | "frb" | "neutrino" | "optical" | "unknown";

interface MessengerStyle {
  category: MessengerCategory;
  label: string;
  // MUI chip color tokens. Each is picked so the four messengers
  // don't collide with the warning/info reds used elsewhere on
  // this table for CR membership / associated stars.
  color: "warning" | "secondary" | "info" | "success" | "default";
}

function messengerStyle(instrument: string): MessengerStyle {
  // GRB instruments — Fermi/Swift/SVOM/Einstein Probe families.
  if (
    instrument.startsWith("Fermi-") ||
    instrument.startsWith("Swift-") ||
    instrument.startsWith("SVOM-") ||
    instrument.startsWith("Einstein") ||
    instrument.startsWith("BurstCube")
  ) {
    return { category: "gamma", label: "γ", color: "warning" };
  }
  // FRB instruments emit the labels CHIME_INSTRUMENT_LABEL /
  // DSA110_INSTRUMENT_LABEL on the Rust side; match the literal
  // strings so a future radio survey doesn't accidentally route
  // through the GRB / neutrino chips.
  if (instrument === "CHIME-FRB" || instrument === "DSA110-FRB") {
    return { category: "frb", label: "FRB", color: "secondary" };
  }
  // Neutrino instruments — IceCube + KM3NeT.
  if (instrument === "IceCube" || instrument === "KM3NeT") {
    return { category: "neutrino", label: "ν", color: "info" };
  }
  // BOOM is the optical-transient label used by the cross-match
  // adapter in crate::boom::BOOM_INSTRUMENT_LABEL.
  if (instrument === "BOOM") {
    return { category: "optical", label: "opt", color: "success" };
  }
  return { category: "unknown", label: "?", color: "default" };
}

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
  if (joint === 0) return "0";
  return joint.toExponential(2);
}

interface Props {
  supereventId: string;
}

export function CrossMatchesPanel({ supereventId }: Props) {
  const dispatch = useAppDispatch();
  const allItems = useAppSelector(
    (s) => s.crossMatches.bySuperevent[supereventId] ?? [],
  );
  const filteredItems = useAppSelector(
    (s) => s.crossMatches.filteredBySuperevent[supereventId] ?? [],
  );
  const loading = useAppSelector((s) => s.crossMatches.loading);
  const filtering = useAppSelector((s) => s.crossMatches.filtering);
  const scanning = useAppSelector((s) => s.crossMatches.scanning);
  const error = useAppSelector((s) => s.crossMatches.error);
  const filters = useAppSelector((s) => s.scienceFilters.items);

  const [timeWindowSec, setTimeWindowSec] = useState(10);
  const [pValueTrials, setPValueTrials] = useState(200);
  // "" → no filter (objective view). Otherwise a science-filter id.
  const [filterId, setFilterId] = useState("");

  // The rows shown: the filtered (tier-tagged) set when a filter is
  // active, otherwise the full objective list. Both are
  // `CrossMatchDoc`-shaped so the table renders identically; only the
  // confidence column differs.
  const showingFiltered = filterId !== "";
  const items: FilteredCrossMatch[] = showingFiltered
    ? filteredItems
    : allItems;

  useEffect(() => {
    dispatch(fetchCrossMatches(supereventId));
    dispatch(fetchScienceFilters());
  }, [dispatch, supereventId]);

  // Re-apply the active filter whenever it changes or a scan rewrites
  // the underlying metrics.
  useEffect(() => {
    if (filterId !== "") {
      dispatch(fetchFilteredCrossMatches({ supereventId, filterId }));
    }
  }, [dispatch, supereventId, filterId, allItems]);

  async function onScan() {
    await dispatch(
      scanCrossMatches({ supereventId, timeWindowSec, pValueTrials }),
    );
  }

  function onToggleAssociated(m: CrossMatchDoc) {
    dispatch(
      setCrossMatchAssociated({
        supereventId,
        instrument: m.instrument,
        triggerId: m.trigger_id,
        associated: !(m.associated ?? false),
      }),
    );
  }

  return (
    <Stack spacing={2}>
      <Paper sx={{ p: 2 }}>
        <Typography variant="subtitle2" gutterBottom>
          Scan for coincident external events
        </Typography>
        <Typography
          variant="caption"
          color="text.secondary"
          sx={{ mb: 1.5, display: "block" }}
        >
          Computes a cross-match against every ingested GRB trigger and BOOM
          optical alert with arrival time inside the window. Persisted results
          land in the table below ranked by remapped joint FAR. Star a row to
          commit it as an association.
        </Typography>
        <Stack
          direction="row"
          spacing={1.5}
          alignItems="center"
          flexWrap="wrap"
        >
          <TextField
            size="small"
            label="Time window (± sec)"
            type="number"
            value={timeWindowSec}
            onChange={(e) => setTimeWindowSec(Number(e.target.value) || 0)}
            sx={{ width: 180 }}
          />
          <TextField
            size="small"
            label="p-value trials"
            type="number"
            value={pValueTrials}
            onChange={(e) => setPValueTrials(Number(e.target.value) || 0)}
            sx={{ width: 150 }}
          />
          <Button
            variant="contained"
            onClick={onScan}
            disabled={scanning || timeWindowSec <= 0}
            startIcon={scanning ? <CircularProgress size={14} /> : null}
          >
            {scanning ? "Scanning…" : `Scan ±${timeWindowSec}s`}
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
          <Typography variant="subtitle2">Candidates</Typography>
          {(loading || filtering) && <CircularProgress size={14} />}
          <Box sx={{ flexGrow: 1 }} />
          <FormControl size="small" sx={{ minWidth: 220 }}>
            <InputLabel id="science-filter-label">Science filter</InputLabel>
            <Select
              labelId="science-filter-label"
              label="Science filter"
              value={filterId}
              onChange={(e) => setFilterId(e.target.value)}
            >
              <MenuItem value="">
                <em>None — all candidates</em>
              </MenuItem>
              {filters.map((f) => (
                <MenuItem key={f._id} value={f._id}>
                  {f.name}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <Tooltip title="Manage science filters">
            <Button
              size="small"
              component={RouterLink}
              to="/science-filters"
              sx={{ whiteSpace: "nowrap" }}
            >
              Edit filters
            </Button>
          </Tooltip>
          <Typography variant="caption" color="text.secondary">
            {items.length} result{items.length === 1 ? "" : "s"}
            {showingFiltered ? " passing" : ""}
            {items.some((m) => m.associated)
              ? ` · ${items.filter((m) => m.associated).length} associated`
              : ""}
          </Typography>
        </Stack>
        <Divider sx={{ mt: 1 }} />
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell padding="checkbox" />
              <TableCell>Trigger</TableCell>
              <TableCell align="right">Δt (s)</TableCell>
              <TableCell align="right">Spatial overlap</TableCell>
              <TableCell>CR membership</TableCell>
              <TableCell align="right">
                <Tooltip title="Empirical p-value from the random-rotation Monte Carlo. Lower = more significant.">
                  <span>p-value</span>
                </Tooltip>
              </TableCell>
              <TableCell align="right">
                <Tooltip title="Bias-corrected joint FAR using the empirical p-value. Sort key for this table (best first).">
                  <span>Joint FAR / yr</span>
                </Tooltip>
              </TableCell>
              {showingFiltered && (
                <TableCell>
                  <Tooltip title="Confidence tier this match earns under the selected filter.">
                    <span>Confidence</span>
                  </Tooltip>
                </TableCell>
              )}
              <TableCell>Computed</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {items.map((m) => (
              <TableRow
                key={`${m.instrument}/${m.trigger_id}`}
                sx={{
                  bgcolor: m.associated
                    ? "rgba(255, 235, 100, 0.07)"
                    : undefined,
                }}
              >
                <TableCell padding="checkbox">
                  <Tooltip
                    title={
                      m.associated
                        ? "Un-associate this match"
                        : "Mark as an association"
                    }
                  >
                    <IconButton
                      size="small"
                      onClick={() => onToggleAssociated(m)}
                    >
                      {m.associated ? (
                        <StarIcon
                          fontSize="small"
                          sx={{ color: "warning.main" }}
                        />
                      ) : (
                        <StarBorderIcon fontSize="small" />
                      )}
                    </IconButton>
                  </Tooltip>
                </TableCell>
                <TableCell>
                  <Stack direction="row" spacing={1} alignItems="center">
                    {(() => {
                      const s = messengerStyle(m.instrument);
                      return (
                        <Tooltip
                          title={`${s.category} messenger (${m.instrument})`}
                        >
                          <Chip
                            size="small"
                            label={s.label}
                            color={s.color}
                            variant="outlined"
                            sx={{
                              minWidth: 38,
                              fontWeight: 600,
                              fontFamily: "monospace",
                            }}
                          />
                        </Tooltip>
                      );
                    })()}
                    <Box>
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
                    </Box>
                  </Stack>
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
                {showingFiltered && (
                  <TableCell>
                    {m.confidence_tier ? (
                      <Chip
                        size="small"
                        color="info"
                        label={m.confidence_tier}
                        sx={{ fontWeight: 600 }}
                      />
                    ) : (
                      <Typography variant="caption" color="text.secondary">
                        —
                      </Typography>
                    )}
                  </TableCell>
                )}
                <TableCell>
                  <Typography variant="caption" color="text.secondary">
                    {fmtComputed(m.computed_at)}
                  </Typography>
                </TableCell>
              </TableRow>
            ))}
            {items.length === 0 && !loading && !filtering && (
              <TableRow>
                <TableCell colSpan={showingFiltered ? 9 : 8}>
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ py: 3, textAlign: "center" }}
                  >
                    {showingFiltered
                      ? "No cross-matches pass this filter. Loosen its cuts or pick a different filter."
                      : `No cross-matches yet. Click Scan to compute matches
                         against every external event near this superevent's
                         time of arrival.`}
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
