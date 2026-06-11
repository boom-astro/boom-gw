// Science-filter builder + manager. Modeled on SkyPortal's filter
// editor: each filter is a saved set of cuts over the objective
// cross-match metrics plus named confidence tiers. Two users running
// two filters see two different sets of associations from the same
// stored metrics.
//
// Filters are owner-scoped; a `group` label shares one read-only with
// everyone (only the owner edits/deletes). The cuts here map 1:1 to
// the Rust `FilterCuts`, and the tiers to `ConfidenceTier`.

import { useEffect, useMemo, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  FormControl,
  FormControlLabel,
  IconButton,
  InputLabel,
  MenuItem,
  OutlinedInput,
  Paper,
  Select,
  Stack,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from "@mui/icons-material/Delete";
import EditIcon from "@mui/icons-material/Edit";
import {
  clearError,
  createScienceFilter,
  deleteScienceFilter,
  fetchScienceFilters,
  updateScienceFilter,
  type FilterPayload,
} from "../ducks/scienceFilters";
import { useAppDispatch, useAppSelector } from "../store";
import { useMyGroups } from "../hooks/access";
import type { ConfidenceTier, ScienceFilterDoc } from "../types/api";

// A blank editor form. `num` fields are kept as strings so the inputs
// can be cleared without snapping to 0; we parse on submit.
interface FormState {
  name: string;
  groupId: string;
  streamIds: string[];
  active: boolean;
  instruments: string;
  timeWindowSec: string;
  spatialOverlapMin: string;
  pValueMax: string;
  jointFarRemappedMax: string;
  requireIn90cr: boolean;
  tiers: ConfidenceTier[];
}

const EMPTY_FORM: FormState = {
  name: "",
  groupId: "",
  streamIds: [],
  active: false,
  instruments: "",
  timeWindowSec: "",
  spatialOverlapMin: "",
  pValueMax: "",
  jointFarRemappedMax: "",
  requireIn90cr: false,
  tiers: [],
};

function parseOptNum(s: string): number | undefined {
  const t = s.trim();
  if (t === "") return undefined;
  const n = Number(t);
  return Number.isFinite(n) ? n : undefined;
}

function formToPayload(f: FormState): FilterPayload {
  const instruments = f.instruments
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  return {
    name: f.name.trim(),
    group_id: f.groupId === "" ? null : f.groupId,
    stream_ids: f.streamIds,
    active: f.active,
    cuts: {
      instruments,
      time_window_sec: parseOptNum(f.timeWindowSec) ?? null,
      spatial_overlap_min: parseOptNum(f.spatialOverlapMin) ?? null,
      p_value_max: parseOptNum(f.pValueMax) ?? null,
      joint_far_remapped_max_per_year:
        parseOptNum(f.jointFarRemappedMax) ?? null,
      require_in_90cr: f.requireIn90cr ? true : null,
    },
    // Sort most-significant first; the server re-sorts too, but this
    // keeps the optimistic local copy consistent.
    confidence_tiers: [...f.tiers].sort(
      (a, b) =>
        a.joint_far_remapped_max_per_year - b.joint_far_remapped_max_per_year,
    ),
  };
}

function docToForm(d: ScienceFilterDoc): FormState {
  const c = d.cuts ?? {};
  const numStr = (n?: number | null) => (n == null ? "" : String(n));
  return {
    name: d.name,
    groupId: d.group_id ?? "",
    streamIds: d.stream_ids ?? [],
    active: d.active ?? false,
    instruments: (c.instruments ?? []).join(", "),
    timeWindowSec: numStr(c.time_window_sec),
    spatialOverlapMin: numStr(c.spatial_overlap_min),
    pValueMax: numStr(c.p_value_max),
    jointFarRemappedMax: numStr(c.joint_far_remapped_max_per_year),
    requireIn90cr: c.require_in_90cr === true,
    tiers: d.confidence_tiers ?? [],
  };
}

function cutsSummary(d: ScienceFilterDoc): string[] {
  const c = d.cuts ?? {};
  const out: string[] = [];
  if (c.instruments && c.instruments.length)
    out.push(c.instruments.join(" / "));
  if (c.time_window_sec != null) out.push(`±${c.time_window_sec}s`);
  if (c.spatial_overlap_min != null)
    out.push(`overlap ≥ ${c.spatial_overlap_min}`);
  if (c.p_value_max != null) out.push(`p ≤ ${c.p_value_max}`);
  if (c.joint_far_remapped_max_per_year != null)
    out.push(`FAR ≤ ${c.joint_far_remapped_max_per_year}/yr`);
  if (c.require_in_90cr) out.push("in 90% CR");
  return out;
}

export function ScienceFiltersPage() {
  const dispatch = useAppDispatch();
  const items = useAppSelector((s) => s.scienceFilters.items);
  const loading = useAppSelector((s) => s.scienceFilters.loading);
  const saving = useAppSelector((s) => s.scienceFilters.saving);
  const error = useAppSelector((s) => s.scienceFilters.error);
  const mySub = useAppSelector((s) => s.auth.principal?.sub ?? null);
  const myGroups = useMyGroups();

  const [open, setOpen] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [form, setForm] = useState<FormState>(EMPTY_FORM);

  useEffect(() => {
    dispatch(fetchScienceFilters());
  }, [dispatch]);

  // Group-name lookup for the table; streams available to the group
  // chosen in the editor (filter streams must be a subset).
  const groupName = (id?: string | null) =>
    id ? (myGroups.find((g) => g.id === id)?.name ?? id) : null;
  const selectedGroupStreams =
    myGroups.find((g) => g.id === form.groupId)?.streams ?? [];

  const isOwner = useMemo(
    () => (d: ScienceFilterDoc) => mySub != null && d.owner === mySub,
    [mySub],
  );

  function openNew() {
    setEditingId(null);
    setForm(EMPTY_FORM);
    setOpen(true);
  }

  function openEdit(d: ScienceFilterDoc) {
    setEditingId(d._id);
    setForm(docToForm(d));
    setOpen(true);
  }

  async function onSave() {
    const payload = formToPayload(form);
    if (!payload.name) return;
    if (editingId) {
      await dispatch(updateScienceFilter({ id: editingId, patch: payload }));
    } else {
      await dispatch(createScienceFilter(payload));
    }
    setOpen(false);
  }

  function onDelete(d: ScienceFilterDoc) {
    if (window.confirm(`Delete filter "${d.name}"?`)) {
      dispatch(deleteScienceFilter(d._id));
    }
  }

  function setTier(idx: number, patch: Partial<ConfidenceTier>) {
    setForm((f) => {
      const tiers = f.tiers.slice();
      tiers[idx] = { ...tiers[idx], ...patch };
      return { ...f, tiers };
    });
  }

  return (
    <Stack spacing={2}>
      <Stack direction="row" alignItems="center" spacing={2}>
        <Typography variant="h5">Science filters</Typography>
        {loading && <CircularProgress size={18} />}
        <Box sx={{ flexGrow: 1 }} />
        <Button variant="contained" startIcon={<AddIcon />} onClick={openNew}>
          New filter
        </Button>
      </Stack>
      <Typography variant="body2" color="text.secondary">
        A filter is a saved set of cuts over the cross-match metrics plus
        named confidence tiers. Apply one on a superevent's Cross-matches
        tab to see only the associations that pass it, tagged by tier.
        Filters are private unless you give them a group label.
      </Typography>

      {error && (
        <Alert severity="error" onClose={() => dispatch(clearError())}>
          {error}
        </Alert>
      )}

      <Paper>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Cuts</TableCell>
              <TableCell>Tiers</TableCell>
              <TableCell>Owner / group</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {items.map((d) => {
              const owned = isOwner(d);
              return (
                <TableRow key={d._id}>
                  <TableCell>
                    <Typography variant="body2" sx={{ fontWeight: 600 }}>
                      {d.name}
                    </Typography>
                    {d.active && (
                      <Chip
                        size="small"
                        color="success"
                        variant="outlined"
                        label="active"
                        sx={{ mt: 0.5 }}
                      />
                    )}
                  </TableCell>
                  <TableCell>
                    <Stack direction="row" spacing={0.5} flexWrap="wrap" useFlexGap>
                      {cutsSummary(d).map((c) => (
                        <Chip key={c} size="small" variant="outlined" label={c} />
                      ))}
                      {cutsSummary(d).length === 0 && (
                        <Typography variant="caption" color="text.secondary">
                          no cuts (matches everything)
                        </Typography>
                      )}
                    </Stack>
                  </TableCell>
                  <TableCell>
                    <Stack direction="row" spacing={0.5} flexWrap="wrap" useFlexGap>
                      {(d.confidence_tiers ?? []).map((t) => (
                        <Tooltip
                          key={t.name}
                          title={`remapped FAR ≤ ${t.joint_far_remapped_max_per_year}/yr`}
                        >
                          <Chip size="small" color="info" label={t.name} />
                        </Tooltip>
                      ))}
                    </Stack>
                  </TableCell>
                  <TableCell>
                    <Typography variant="caption" color="text.secondary">
                      {d.owner}
                    </Typography>
                    {d.group_id && (
                      <Chip
                        size="small"
                        variant="outlined"
                        label={groupName(d.group_id)}
                        sx={{ ml: 0.5 }}
                      />
                    )}
                  </TableCell>
                  <TableCell align="right">
                    <Tooltip title={owned ? "Edit" : "Only the owner can edit"}>
                      <span>
                        <IconButton
                          size="small"
                          aria-label="Edit filter"
                          disabled={!owned}
                          onClick={() => openEdit(d)}
                        >
                          <EditIcon fontSize="small" />
                        </IconButton>
                      </span>
                    </Tooltip>
                    <Tooltip
                      title={owned ? "Delete" : "Only the owner can delete"}
                    >
                      <span>
                        <IconButton
                          size="small"
                          aria-label="Delete filter"
                          disabled={!owned}
                          onClick={() => onDelete(d)}
                        >
                          <DeleteIcon fontSize="small" />
                        </IconButton>
                      </span>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              );
            })}
            {items.length === 0 && !loading && (
              <TableRow>
                <TableCell colSpan={5}>
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ py: 3, textAlign: "center" }}
                  >
                    No science filters yet. Create one to start surfacing
                    associations at your own confidence thresholds.
                  </Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </Paper>

      <Dialog open={open} onClose={() => setOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>{editingId ? "Edit filter" : "New filter"}</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2} sx={{ mt: 0.5 }}>
            <TextField
              label="Name"
              size="small"
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              required
              fullWidth
            />
            <FormControl size="small" fullWidth>
              <InputLabel id="filter-group-label">Group</InputLabel>
              <Select
                labelId="filter-group-label"
                label="Group"
                value={form.groupId}
                onChange={(e) =>
                  // Switching groups invalidates previously-picked
                  // streams, so reset them.
                  setForm({ ...form, groupId: e.target.value, streamIds: [] })
                }
              >
                <MenuItem value="">
                  <em>Private (no group)</em>
                </MenuItem>
                {myGroups.map((g) => (
                  <MenuItem key={g.id} value={g.id}>
                    {g.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <FormControl size="small" fullWidth disabled={form.groupId === ""}>
              <InputLabel id="filter-streams-label">Streams</InputLabel>
              <Select
                labelId="filter-streams-label"
                multiple
                value={form.streamIds}
                input={<OutlinedInput label="Streams" />}
                renderValue={(sel) => (
                  <Stack direction="row" spacing={0.5} flexWrap="wrap" useFlexGap>
                    {(sel as string[]).map((id) => (
                      <Chip
                        key={id}
                        size="small"
                        label={
                          selectedGroupStreams.find((s) => s.id === id)?.name ??
                          id
                        }
                      />
                    ))}
                  </Stack>
                )}
                onChange={(e) => {
                  const value = e.target.value;
                  setForm({
                    ...form,
                    streamIds:
                      typeof value === "string" ? value.split(",") : value,
                  });
                }}
              >
                {selectedGroupStreams.map((s) => (
                  <MenuItem key={s.id} value={s.id}>
                    {s.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <Divider textAlign="left">
              <Typography variant="caption" color="text.secondary">
                Cuts (leave blank to skip)
              </Typography>
            </Divider>
            <TextField
              label="Instruments (comma-separated)"
              size="small"
              placeholder="Fermi-GBM, Swift-BAT, IceCube"
              value={form.instruments}
              onChange={(e) =>
                setForm({ ...form, instruments: e.target.value })
              }
              fullWidth
            />
            <Stack direction="row" spacing={1.5} flexWrap="wrap" useFlexGap>
              <TextField
                label="Time window (± sec)"
                size="small"
                type="number"
                value={form.timeWindowSec}
                onChange={(e) =>
                  setForm({ ...form, timeWindowSec: e.target.value })
                }
                sx={{ width: 170 }}
              />
              <TextField
                label="Min spatial overlap"
                size="small"
                type="number"
                value={form.spatialOverlapMin}
                onChange={(e) =>
                  setForm({ ...form, spatialOverlapMin: e.target.value })
                }
                sx={{ width: 170 }}
              />
              <TextField
                label="Max p-value"
                size="small"
                type="number"
                value={form.pValueMax}
                onChange={(e) =>
                  setForm({ ...form, pValueMax: e.target.value })
                }
                sx={{ width: 150 }}
              />
              <TextField
                label="Max joint FAR / yr"
                size="small"
                type="number"
                value={form.jointFarRemappedMax}
                onChange={(e) =>
                  setForm({ ...form, jointFarRemappedMax: e.target.value })
                }
                sx={{ width: 170 }}
              />
            </Stack>
            <FormControlLabel
              control={
                <Switch
                  checked={form.requireIn90cr}
                  onChange={(e) =>
                    setForm({ ...form, requireIn90cr: e.target.checked })
                  }
                />
              }
              label="Require external position in GW 90% credible region"
            />

            <Divider textAlign="left">
              <Typography variant="caption" color="text.secondary">
                Confidence tiers
              </Typography>
            </Divider>
            {form.tiers.map((t, idx) => (
              <Stack key={idx} direction="row" spacing={1} alignItems="center">
                <TextField
                  label="Tier name"
                  size="small"
                  value={t.name}
                  onChange={(e) => setTier(idx, { name: e.target.value })}
                  sx={{ width: 160 }}
                />
                <TextField
                  label="Max FAR / yr"
                  size="small"
                  type="number"
                  value={t.joint_far_remapped_max_per_year}
                  onChange={(e) =>
                    setTier(idx, {
                      joint_far_remapped_max_per_year:
                        Number(e.target.value) || 0,
                    })
                  }
                  sx={{ width: 150 }}
                />
                <IconButton
                  size="small"
                  onClick={() =>
                    setForm((f) => ({
                      ...f,
                      tiers: f.tiers.filter((_, i) => i !== idx),
                    }))
                  }
                >
                  <DeleteIcon fontSize="small" />
                </IconButton>
              </Stack>
            ))}
            <Button
              size="small"
              startIcon={<AddIcon />}
              onClick={() =>
                setForm((f) => ({
                  ...f,
                  tiers: [
                    ...f.tiers,
                    { name: "", joint_far_remapped_max_per_year: 1 },
                  ],
                }))
              }
              sx={{ alignSelf: "flex-start" }}
            >
              Add tier
            </Button>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            onClick={onSave}
            disabled={saving || form.name.trim() === ""}
            startIcon={saving ? <CircularProgress size={14} /> : null}
          >
            {editingId ? "Save" : "Create"}
          </Button>
        </DialogActions>
      </Dialog>
    </Stack>
  );
}
