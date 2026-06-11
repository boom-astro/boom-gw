// GW × GRB cross-matches for a single superevent. The list is
// read on tab-mount; new cross-matches can be triggered from the
// UI by POSTing (instrument, trigger_id) — gw-api fetches the
// skymap, runs the RAVEN integral, and persists the result.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type {
  ApiEnvelope,
  CrossMatchDoc,
  FilteredCrossMatch,
} from "../types/api";

interface CrossMatchesState {
  bySuperevent: Record<string, CrossMatchDoc[]>;
  /** Results of the last science-filtered view, keyed by superevent.
   *  Carries the per-row `confidence_tier` tag the unfiltered list
   *  lacks. Kept separate so toggling a filter off restores the
   *  full list without a refetch. */
  filteredBySuperevent: Record<string, FilteredCrossMatch[]>;
  loading: boolean;
  filtering: boolean;
  computing: boolean;
  scanning: boolean;
  error: string | null;
}

const initialState: CrossMatchesState = {
  bySuperevent: {},
  filteredBySuperevent: {},
  loading: false,
  filtering: false,
  computing: false,
  scanning: false,
  error: null,
};

export const fetchCrossMatches = createAsyncThunk<
  { supereventId: string; items: CrossMatchDoc[] },
  string
>("crossMatches/fetch", async (supereventId) => {
  const { data } = await http.get<ApiEnvelope<CrossMatchDoc[]>>(
    `/api/superevents/${supereventId}/cross-matches`,
    { params: { limit: 200 } },
  );
  return { supereventId, items: data.data };
});

/// Fetch the cross-matches passing a saved science filter, each
/// tagged with its confidence tier. Backs the filter selector on the
/// cross-matches panel.
export const fetchFilteredCrossMatches = createAsyncThunk<
  { supereventId: string; items: FilteredCrossMatch[] },
  { supereventId: string; filterId: string },
  { rejectValue: string }
>(
  "crossMatches/fetchFiltered",
  async ({ supereventId, filterId }, { rejectWithValue }) => {
    try {
      const { data } = await http.get<ApiEnvelope<FilteredCrossMatch[]>>(
        `/api/superevents/${supereventId}/cross-matches`,
        { params: { limit: 200, filter_id: filterId } },
      );
      return { supereventId, items: data.data };
    } catch (e) {
      const ax = e as { response?: { data?: { message?: string } } };
      return rejectWithValue(ax.response?.data?.message ?? (e as Error).message);
    }
  },
);

export interface CrossMatchRequest {
  supereventId: string;
  instrument: string;
  triggerId: string;
}

export const createCrossMatch = createAsyncThunk<
  { supereventId: string; item: CrossMatchDoc },
  CrossMatchRequest,
  { rejectValue: string }
>(
  "crossMatches/create",
  async ({ supereventId, instrument, triggerId }, { rejectWithValue }) => {
    try {
      const { data } = await http.post<ApiEnvelope<CrossMatchDoc>>(
        `/api/superevents/${supereventId}/cross-matches`,
        { instrument, trigger_id: triggerId },
      );
      return { supereventId, item: data.data };
    } catch (e) {
      // axios errors include the response body for 4xx/5xx, which
      // is where gw-api's `{message, data}` envelope lives. Surface
      // that as the UI error string.
      const ax = e as { response?: { data?: { message?: string } } };
      const msg = ax.response?.data?.message ?? (e as Error).message;
      return rejectWithValue(msg);
    }
  },
);

export interface ScanCrossMatchesRequest {
  supereventId: string;
  timeWindowSec: number;
  pValueTrials: number;
}

/// Scan every external event with `t ∈ [t_0 ± window]` and persist
/// a cross-match for each. Replaces the prior list on the
/// superevent with the freshly-sorted results.
export const scanCrossMatches = createAsyncThunk<
  { supereventId: string; items: CrossMatchDoc[] },
  ScanCrossMatchesRequest,
  { rejectValue: string }
>(
  "crossMatches/scan",
  async (
    { supereventId, timeWindowSec, pValueTrials },
    { rejectWithValue },
  ) => {
    try {
      const { data } = await http.post<ApiEnvelope<CrossMatchDoc[]>>(
        `/api/superevents/${supereventId}/scan-cross-matches`,
        { time_window_sec: timeWindowSec, p_value_trials: pValueTrials },
      );
      return { supereventId, items: data.data };
    } catch (e) {
      const ax = e as { response?: { data?: { message?: string } } };
      const msg = ax.response?.data?.message ?? (e as Error).message;
      return rejectWithValue(msg);
    }
  },
);

export interface AssociateRequest {
  supereventId: string;
  instrument: string;
  triggerId: string;
  associated: boolean;
}

/// Flip the `associated` flag on one cross-match. Optimistically
/// updates the local store so the star toggle is instant; the
/// server response (which echoes the full doc) reconciles.
export const setCrossMatchAssociated = createAsyncThunk<
  { supereventId: string; item: CrossMatchDoc },
  AssociateRequest,
  { rejectValue: string }
>(
  "crossMatches/setAssociated",
  async (
    { supereventId, instrument, triggerId, associated },
    { rejectWithValue },
  ) => {
    try {
      const { data } = await http.patch<ApiEnvelope<CrossMatchDoc>>(
        `/api/superevents/${supereventId}/cross-matches/${encodeURIComponent(instrument)}/${encodeURIComponent(triggerId)}`,
        { associated },
      );
      return { supereventId, item: data.data };
    } catch (e) {
      const ax = e as { response?: { data?: { message?: string } } };
      const msg = ax.response?.data?.message ?? (e as Error).message;
      return rejectWithValue(msg);
    }
  },
);

const slice = createSlice({
  name: "crossMatches",
  initialState,
  reducers: {
    clearError(state) {
      state.error = null;
    },
  },
  extraReducers: (b) => {
    b.addCase(fetchCrossMatches.pending, (state) => {
      state.loading = true;
      state.error = null;
    });
    b.addCase(fetchCrossMatches.fulfilled, (state, action) => {
      state.loading = false;
      state.bySuperevent[action.payload.supereventId] = action.payload.items;
    });
    b.addCase(fetchCrossMatches.rejected, (state, action) => {
      state.loading = false;
      state.error = action.error.message ?? "Failed to load cross-matches";
    });
    b.addCase(fetchFilteredCrossMatches.pending, (state) => {
      state.filtering = true;
      state.error = null;
    });
    b.addCase(fetchFilteredCrossMatches.fulfilled, (state, action) => {
      state.filtering = false;
      state.filteredBySuperevent[action.payload.supereventId] =
        action.payload.items;
    });
    b.addCase(fetchFilteredCrossMatches.rejected, (state, action) => {
      state.filtering = false;
      state.error = action.payload ?? "Failed to apply filter";
    });
    b.addCase(createCrossMatch.pending, (state) => {
      state.computing = true;
      state.error = null;
    });
    b.addCase(createCrossMatch.fulfilled, (state, action) => {
      state.computing = false;
      const { supereventId, item } = action.payload;
      const existing = state.bySuperevent[supereventId] ?? [];
      // De-dup by composite key — the API upserts, so a fresh
      // result for the same trigger replaces the prior one.
      const idx = existing.findIndex(
        (m) =>
          m.instrument === item.instrument && m.trigger_id === item.trigger_id,
      );
      if (idx >= 0) {
        const next = existing.slice();
        next[idx] = item;
        state.bySuperevent[supereventId] = next;
      } else {
        state.bySuperevent[supereventId] = [item, ...existing];
      }
    });
    b.addCase(createCrossMatch.rejected, (state, action) => {
      state.computing = false;
      state.error = action.payload ?? "Cross-match request failed";
    });
    b.addCase(scanCrossMatches.pending, (state) => {
      state.scanning = true;
      state.error = null;
    });
    b.addCase(scanCrossMatches.fulfilled, (state, action) => {
      state.scanning = false;
      state.bySuperevent[action.payload.supereventId] = action.payload.items;
    });
    b.addCase(scanCrossMatches.rejected, (state, action) => {
      state.scanning = false;
      state.error = action.payload ?? "Cross-match scan failed";
    });
    b.addCase(setCrossMatchAssociated.fulfilled, (state, action) => {
      const { supereventId, item } = action.payload;
      const existing = state.bySuperevent[supereventId] ?? [];
      const idx = existing.findIndex(
        (m) =>
          m.instrument === item.instrument && m.trigger_id === item.trigger_id,
      );
      if (idx >= 0) {
        const next = existing.slice();
        next[idx] = item;
        state.bySuperevent[supereventId] = next;
      }
    });
    b.addCase(setCrossMatchAssociated.rejected, (state, action) => {
      state.error = action.payload ?? "Association update failed";
    });
  },
});

export const { clearError } = slice.actions;
export default slice.reducer;
