// Redux duck for the External Streams page: GRB triggers from
// [`/api/grb-triggers`] and BOOM cross-matched optical alerts from
// [`/api/boom-alerts`]. Held as separate per-source caches because
// pagination, filters, and refresh cadence will differ per source
// once we add more upstreams (Swift, SVOM, ATels …).

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type { ApiEnvelope, BoomAlertDoc, GrbTriggerDoc } from "../types/api";

interface ExternalAlertsState {
  grbTriggers: GrbTriggerDoc[];
  boomAlerts: BoomAlertDoc[];
  loading: boolean;
  error: string | null;
}

const initialState: ExternalAlertsState = {
  grbTriggers: [],
  boomAlerts: [],
  loading: false,
  error: null,
};

export const fetchGrbTriggers = createAsyncThunk<
  GrbTriggerDoc[],
  { limit?: number; instrument?: string } | undefined
>("externalAlerts/fetchGrbTriggers", async (q) => {
  const { data } = await http.get<ApiEnvelope<GrbTriggerDoc[]>>(
    "/api/grb-triggers",
    { params: q ?? { limit: 500 } },
  );
  return data.data;
});

export const fetchBoomAlerts = createAsyncThunk<
  BoomAlertDoc[],
  { limit?: number } | undefined
>("externalAlerts/fetchBoomAlerts", async (q) => {
  // The route may legitimately not exist yet in older API
  // deployments — treat 404 as an empty list rather than a hard
  // error so the page still renders the GRB side.
  try {
    const { data } = await http.get<ApiEnvelope<BoomAlertDoc[]>>(
      "/api/boom-alerts",
      { params: q ?? { limit: 500 } },
    );
    return data.data;
  } catch (e) {
    const ax = e as { response?: { status?: number } };
    if (ax.response?.status === 404) return [];
    throw e;
  }
});

const slice = createSlice({
  name: "externalAlerts",
  initialState,
  reducers: {},
  extraReducers: (b) => {
    b.addCase(fetchGrbTriggers.pending, (state) => {
      state.loading = true;
      state.error = null;
    });
    b.addCase(fetchGrbTriggers.fulfilled, (state, action) => {
      state.loading = false;
      state.grbTriggers = action.payload;
    });
    b.addCase(fetchGrbTriggers.rejected, (state, action) => {
      state.loading = false;
      state.error = action.error.message ?? "Failed to load GRB triggers";
    });
    b.addCase(fetchBoomAlerts.fulfilled, (state, action) => {
      state.boomAlerts = action.payload;
    });
    b.addCase(fetchBoomAlerts.rejected, (state, action) => {
      state.error = action.error.message ?? "Failed to load BOOM alerts";
    });
  },
});

export default slice.reducer;
