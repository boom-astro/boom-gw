// Outgoing GCN alerts assembled by gw-api for a superevent. List
// is read-only here; the "publish" / "assemble" POST is gated by
// the AlertConfig allowlist on the server, so the Phase 2 form
// will only succeed for principals in that list.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type { AlertDoc, ApiEnvelope } from "../types/api";

interface AlertsState {
  bySuperevent: Record<string, AlertDoc[]>;
  loading: boolean;
  error: string | null;
}

const initialState: AlertsState = {
  bySuperevent: {},
  loading: false,
  error: null,
};

export const fetchAlerts = createAsyncThunk<
  { supereventId: string; items: AlertDoc[] },
  string
>("alerts/fetch", async (supereventId) => {
  const { data } = await http.get<ApiEnvelope<AlertDoc[]>>(
    `/api/superevents/${supereventId}/alerts`,
    { params: { limit: 200 } },
  );
  return { supereventId, items: data.data };
});

const slice = createSlice({
  name: "alerts",
  initialState,
  reducers: {},
  extraReducers: (b) => {
    b.addCase(fetchAlerts.pending, (state) => {
      state.loading = true;
      state.error = null;
    });
    b.addCase(fetchAlerts.fulfilled, (state, action) => {
      state.loading = false;
      state.bySuperevent[action.payload.supereventId] = action.payload.items;
    });
    b.addCase(fetchAlerts.rejected, (state, action) => {
      state.loading = false;
      state.error = action.error.message ?? "Failed to load alerts";
    });
  },
});

export default slice.reducer;
