// /api/health/dashboard duck. Backs the System Health page.
// Polled on a 10s interval while the page is mounted; no caching
// across navigations so a tab-switch always sees fresh numbers.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type { ApiEnvelope, HealthDashboard } from "../types/api";

interface State {
  data: HealthDashboard | null;
  loading: boolean;
  error: string | null;
  lastFetchedAt: number | null;
}

const initialState: State = {
  data: null,
  loading: false,
  error: null,
  lastFetchedAt: null,
};

export const fetchHealthDashboard = createAsyncThunk<HealthDashboard>(
  "health/fetchDashboard",
  async () => {
    const { data } = await http.get<ApiEnvelope<HealthDashboard>>(
      "/api/health/dashboard",
    );
    return data.data;
  },
);

const slice = createSlice({
  name: "health",
  initialState,
  reducers: {},
  extraReducers: (b) => {
    b.addCase(fetchHealthDashboard.pending, (s) => {
      s.loading = true;
      // Don't clear `data` — keep the last good payload visible
      // while a refresh is in flight so the page doesn't flicker.
      s.error = null;
    });
    b.addCase(fetchHealthDashboard.fulfilled, (s, a) => {
      s.loading = false;
      s.data = a.payload;
      s.lastFetchedAt = Date.now();
    });
    b.addCase(fetchHealthDashboard.rejected, (s, a) => {
      s.loading = false;
      s.error = a.error.message ?? "Failed to load health dashboard";
    });
  },
});

export default slice.reducer;
