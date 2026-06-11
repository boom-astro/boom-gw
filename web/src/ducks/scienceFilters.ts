// Saved science filters — the per-user cut sets that decide which
// GW × external cross-matches count as associations, and at what
// confidence. CRUD against `/api/science-filters`; the filtered
// cross-match view (see `crossMatches.ts`) consumes the selected
// filter's id.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type {
  ApiEnvelope,
  ConfidenceTier,
  FilterCuts,
  ScienceFilterDoc,
} from "../types/api";

interface ScienceFiltersState {
  items: ScienceFilterDoc[];
  loading: boolean;
  saving: boolean;
  error: string | null;
}

const initialState: ScienceFiltersState = {
  items: [],
  loading: false,
  saving: false,
  error: null,
};

function errMessage(e: unknown): string {
  const ax = e as { response?: { data?: { message?: string } } };
  return ax.response?.data?.message ?? (e as Error).message;
}

export const fetchScienceFilters = createAsyncThunk<ScienceFilterDoc[]>(
  "scienceFilters/fetch",
  async () => {
    const { data } =
      await http.get<ApiEnvelope<ScienceFilterDoc[]>>("/api/science-filters");
    return data.data;
  },
);

export interface FilterPayload {
  name: string;
  group_id?: string | null;
  stream_ids?: string[];
  active?: boolean;
  cuts: FilterCuts;
  confidence_tiers: ConfidenceTier[];
}

export const createScienceFilter = createAsyncThunk<
  ScienceFilterDoc,
  FilterPayload,
  { rejectValue: string }
>("scienceFilters/create", async (payload, { rejectWithValue }) => {
  try {
    const { data } = await http.post<ApiEnvelope<ScienceFilterDoc>>(
      "/api/science-filters",
      payload,
    );
    return data.data;
  } catch (e) {
    return rejectWithValue(errMessage(e));
  }
});

export const updateScienceFilter = createAsyncThunk<
  ScienceFilterDoc,
  { id: string; patch: Partial<FilterPayload> },
  { rejectValue: string }
>("scienceFilters/update", async ({ id, patch }, { rejectWithValue }) => {
  try {
    const { data } = await http.patch<ApiEnvelope<ScienceFilterDoc>>(
      `/api/science-filters/${encodeURIComponent(id)}`,
      patch,
    );
    return data.data;
  } catch (e) {
    return rejectWithValue(errMessage(e));
  }
});

export const deleteScienceFilter = createAsyncThunk<
  string,
  string,
  { rejectValue: string }
>("scienceFilters/delete", async (id, { rejectWithValue }) => {
  try {
    await http.delete(`/api/science-filters/${encodeURIComponent(id)}`);
    return id;
  } catch (e) {
    return rejectWithValue(errMessage(e));
  }
});

function upsert(items: ScienceFilterDoc[], doc: ScienceFilterDoc) {
  const idx = items.findIndex((f) => f._id === doc._id);
  if (idx >= 0) {
    items[idx] = doc;
  } else {
    items.unshift(doc);
  }
}

const slice = createSlice({
  name: "scienceFilters",
  initialState,
  reducers: {
    clearError(state) {
      state.error = null;
    },
  },
  extraReducers: (b) => {
    b.addCase(fetchScienceFilters.pending, (s) => {
      s.loading = true;
      s.error = null;
    });
    b.addCase(fetchScienceFilters.fulfilled, (s, a) => {
      s.loading = false;
      s.items = a.payload;
    });
    b.addCase(fetchScienceFilters.rejected, (s, a) => {
      s.loading = false;
      s.error = a.error.message ?? "Failed to load science filters";
    });
    b.addCase(createScienceFilter.pending, (s) => {
      s.saving = true;
      s.error = null;
    });
    b.addCase(createScienceFilter.fulfilled, (s, a) => {
      s.saving = false;
      upsert(s.items, a.payload);
    });
    b.addCase(createScienceFilter.rejected, (s, a) => {
      s.saving = false;
      s.error = a.payload ?? "Failed to create filter";
    });
    b.addCase(updateScienceFilter.pending, (s) => {
      s.saving = true;
      s.error = null;
    });
    b.addCase(updateScienceFilter.fulfilled, (s, a) => {
      s.saving = false;
      upsert(s.items, a.payload);
    });
    b.addCase(updateScienceFilter.rejected, (s, a) => {
      s.saving = false;
      s.error = a.payload ?? "Failed to update filter";
    });
    b.addCase(deleteScienceFilter.fulfilled, (s, a) => {
      s.items = s.items.filter((f) => f._id !== a.payload);
    });
    b.addCase(deleteScienceFilter.rejected, (s, a) => {
      s.error = a.payload ?? "Failed to delete filter";
    });
  },
});

export const { clearError } = slice.actions;
export default slice.reducer;
