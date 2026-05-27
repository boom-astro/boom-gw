// List view for /api/superevents. Server-side pagination: each
// page change fires a fresh fetch with `?skip=...&limit=...`, and
// the total count comes from a separate `/api/superevents/count`
// call so MUI's TablePagination can render "X of N" + disable the
// "next" button on the last page. The backend caps `limit` at 500
// (see MAX_LIMIT in src/api.rs); the page size dropdown stays well
// under that.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type { ApiEnvelope, SupereventDoc } from "../types/api";

export interface SupereventsQuery {
  limit?: number;
  skip?: number;
}

interface SupereventsState {
  items: SupereventDoc[];
  /// Total matching the current filter, populated by `fetchSupereventsCount`.
  /// `null` until the first count call resolves.
  total: number | null;
  loading: boolean;
  countLoading: boolean;
  error: string | null;
}

const initialState: SupereventsState = {
  items: [],
  total: null,
  loading: false,
  countLoading: false,
  error: null,
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

export const fetchSupereventsCount = createAsyncThunk<number>(
  "superevents/count",
  async () => {
    const { data } = await http.get<ApiEnvelope<{ count: number }>>(
      "/api/superevents/count",
    );
    return data.data.count;
  },
);

const slice = createSlice({
  name: "superevents",
  initialState,
  reducers: {},
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
    b.addCase(fetchSupereventsCount.pending, (state) => {
      state.countLoading = true;
    });
    b.addCase(fetchSupereventsCount.fulfilled, (state, action) => {
      state.countLoading = false;
      state.total = action.payload;
    });
    b.addCase(fetchSupereventsCount.rejected, (state) => {
      state.countLoading = false;
      // Leave the previous total in place; not worth surfacing this
      // as a page-blocking error.
    });
  },
});

export default slice.reducer;
