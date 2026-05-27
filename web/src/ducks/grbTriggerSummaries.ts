// One-row-per-trigger_id list view for GRBs. Backed by
// `/api/grb-trigger-summaries` which runs a mongo aggregation that
// collapses the FLT/GND/FIN/SUBTHRESH stages Fermi-GBM emits for
// the same burst into a single row, picking the best-refined
// localization for the row's RA/Dec.
//
// Pagination is server-side, same shape as `superevents.ts`:
// page/skip → /api/grb-trigger-summaries?skip=...&limit=...,
// total → /api/grb-trigger-summaries/count.
//
// For per-trigger detail (the drill-down page that shows every
// stage), use `fetchGrbTriggerSummary(triggerId)` — that returns
// a single `GrbTriggerSummary` with the `stages` array populated.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type { ApiEnvelope, GrbTriggerSummary } from "../types/api";

export interface GrbTriggerSummariesQuery {
  limit?: number;
  skip?: number;
  instrument?: string;
}

interface State {
  items: GrbTriggerSummary[];
  total: number | null;
  loading: boolean;
  countLoading: boolean;
  /** Single-trigger drill-down cache, keyed by trigger_id. */
  byId: Record<string, GrbTriggerSummary | undefined>;
  detailLoading: boolean;
  error: string | null;
}

const initialState: State = {
  items: [],
  total: null,
  loading: false,
  countLoading: false,
  byId: {},
  detailLoading: false,
  error: null,
};

export const fetchGrbTriggerSummaries = createAsyncThunk<
  GrbTriggerSummary[],
  GrbTriggerSummariesQuery | undefined
>("grbTriggerSummaries/fetchList", async (query) => {
  const params = query ?? {};
  const { data } = await http.get<ApiEnvelope<GrbTriggerSummary[]>>(
    "/api/grb-trigger-summaries",
    { params },
  );
  return data.data;
});

export const fetchGrbTriggerSummariesCount = createAsyncThunk<
  number,
  GrbTriggerSummariesQuery | undefined
>("grbTriggerSummaries/fetchCount", async (query) => {
  const params = query ?? {};
  const { data } = await http.get<ApiEnvelope<{ count: number }>>(
    "/api/grb-trigger-summaries/count",
    { params },
  );
  return data.data.count;
});

export const fetchGrbTriggerSummary = createAsyncThunk<
  GrbTriggerSummary,
  string
>("grbTriggerSummaries/fetchOne", async (triggerId) => {
  const { data } = await http.get<ApiEnvelope<GrbTriggerSummary>>(
    `/api/grb-trigger-summaries/${encodeURIComponent(triggerId)}`,
  );
  return data.data;
});

const slice = createSlice({
  name: "grbTriggerSummaries",
  initialState,
  reducers: {},
  extraReducers: (b) => {
    b.addCase(fetchGrbTriggerSummaries.pending, (s) => {
      s.loading = true;
      s.error = null;
    });
    b.addCase(fetchGrbTriggerSummaries.fulfilled, (s, a) => {
      s.loading = false;
      s.items = a.payload;
    });
    b.addCase(fetchGrbTriggerSummaries.rejected, (s, a) => {
      s.loading = false;
      s.error = a.error.message ?? "Failed to load GRB trigger summaries";
    });
    b.addCase(fetchGrbTriggerSummariesCount.pending, (s) => {
      s.countLoading = true;
    });
    b.addCase(fetchGrbTriggerSummariesCount.fulfilled, (s, a) => {
      s.countLoading = false;
      s.total = a.payload;
    });
    b.addCase(fetchGrbTriggerSummariesCount.rejected, (s) => {
      s.countLoading = false;
    });
    b.addCase(fetchGrbTriggerSummary.pending, (s) => {
      s.detailLoading = true;
      s.error = null;
    });
    b.addCase(fetchGrbTriggerSummary.fulfilled, (s, a) => {
      s.detailLoading = false;
      s.byId[a.payload._id] = a.payload;
    });
    b.addCase(fetchGrbTriggerSummary.rejected, (s, a) => {
      s.detailLoading = false;
      s.error = a.error.message ?? "Failed to load trigger detail";
    });
  },
});

export default slice.reducer;
