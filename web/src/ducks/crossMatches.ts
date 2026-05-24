// GW × GRB cross-matches for a single superevent. The list is
// read on tab-mount; new cross-matches can be triggered from the
// UI by POSTing (instrument, trigger_id) — gw-api fetches the
// skymap, runs the RAVEN integral, and persists the result.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type { ApiEnvelope, CrossMatchDoc } from "../types/api";

interface CrossMatchesState {
  bySuperevent: Record<string, CrossMatchDoc[]>;
  loading: boolean;
  computing: boolean;
  error: string | null;
}

const initialState: CrossMatchesState = {
  bySuperevent: {},
  loading: false,
  computing: false,
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
  },
});

export const { clearError } = slice.actions;
export default slice.reducer;
