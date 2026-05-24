// Redux duck for IceCube LVK Nu Track Search results attached to
// a specific superevent. Distinct from `externalAlerts` because
// each search is a coincidence-search result keyed on a
// superevent — not a free-standing trigger to cross-match.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type { ApiEnvelope, IceCubeLvkSearchDoc } from "../types/api";

interface State {
  bySuperevent: Record<string, IceCubeLvkSearchDoc[]>;
  loading: boolean;
  error: string | null;
}

const initialState: State = {
  bySuperevent: {},
  loading: false,
  error: null,
};

export const fetchIceCubeLvkSearches = createAsyncThunk<
  { supereventId: string; items: IceCubeLvkSearchDoc[] },
  string
>("icecubeLvk/fetchSearches", async (supereventId) => {
  try {
    const { data } = await http.get<ApiEnvelope<IceCubeLvkSearchDoc[]>>(
      `/api/superevents/${encodeURIComponent(supereventId)}/icecube-lvk-searches`,
    );
    return { supereventId, items: data.data };
  } catch (e) {
    const ax = e as { response?: { status?: number } };
    if (ax.response?.status === 404) return { supereventId, items: [] };
    throw e;
  }
});

const slice = createSlice({
  name: "icecubeLvk",
  initialState,
  reducers: {},
  extraReducers: (b) => {
    b.addCase(fetchIceCubeLvkSearches.pending, (state) => {
      state.loading = true;
      state.error = null;
    });
    b.addCase(fetchIceCubeLvkSearches.fulfilled, (state, action) => {
      state.loading = false;
      state.bySuperevent[action.payload.supereventId] = action.payload.items;
    });
    b.addCase(fetchIceCubeLvkSearches.rejected, (state, action) => {
      state.loading = false;
      state.error =
        action.error.message ?? "Failed to load IceCube LVK searches";
    });
  },
});

export default slice.reducer;
