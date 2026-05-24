// List view for /api/superevents. Mirrors the SkyPortal "GcnEvents"
// page in shape (a paginated table) but the backing query is much
// simpler: ?limit=N&skip=M returns SupereventDoc[] wrapped in
// ApiEnvelope.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type { ApiEnvelope, SupereventDoc } from "../types/api";

export interface SupereventsQuery {
  limit?: number;
  skip?: number;
}

interface SupereventsState {
  items: SupereventDoc[];
  loading: boolean;
  error: string | null;
  query: SupereventsQuery;
}

const initialState: SupereventsState = {
  items: [],
  loading: false,
  error: null,
  query: { limit: 50, skip: 0 },
};

export const fetchSuperevents = createAsyncThunk<
  SupereventDoc[],
  SupereventsQuery | undefined
>("superevents/fetch", async (query) => {
  const params = query ?? {};
  const { data } = await http.get<ApiEnvelope<SupereventDoc[]>>(
    "/api/superevents",
    { params },
  );
  return data.data;
});

const slice = createSlice({
  name: "superevents",
  initialState,
  reducers: {
    setQuery(state, action: { payload: SupereventsQuery }) {
      state.query = { ...state.query, ...action.payload };
    },
  },
  extraReducers: (b) => {
    b.addCase(fetchSuperevents.pending, (state) => {
      state.loading = true;
      state.error = null;
    });
    b.addCase(fetchSuperevents.fulfilled, (state, action) => {
      state.loading = false;
      state.items = action.payload;
    });
    b.addCase(fetchSuperevents.rejected, (state, action) => {
      state.loading = false;
      state.error = action.error.message ?? "Failed to load superevents";
    });
  },
});

export const { setQuery } = slice.actions;
export default slice.reducer;
