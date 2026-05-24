// Single-superevent detail. Holds the SupereventDoc plus the linked
// EventDoc rows fetched on demand. The skymap blob itself is NOT
// pulled into Redux — it's a 700+ KB FITS file, so AladinViewer
// streams it straight from /api/superevents/{id}/skymap via Aladin.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type {
  ApiEnvelope,
  EventDoc,
  LocalizeRequestDoc,
  LocalizeResultDoc,
  SupereventDoc,
} from "../types/api";

interface SupereventState {
  doc: SupereventDoc | null;
  events: EventDoc[];
  localizeRequests: LocalizeRequestDoc[];
  localizeResults: LocalizeResultDoc[];
  loading: boolean;
  error: string | null;
}

const initialState: SupereventState = {
  doc: null,
  events: [],
  localizeRequests: [],
  localizeResults: [],
  loading: false,
  error: null,
};

export const fetchSuperevent = createAsyncThunk<SupereventDoc, string>(
  "superevent/fetch",
  async (id) => {
    const { data } = await http.get<ApiEnvelope<SupereventDoc>>(
      `/api/superevents/${id}`,
    );
    return data.data;
  },
);

// The events list isn't keyed by superevent_id on the wire today —
// we filter client-side using SupereventDoc.g_event_graceids. Once
// the API exposes a `?superevent_id=` filter this should switch.
export const fetchSupereventEvents = createAsyncThunk<EventDoc[], string[]>(
  "superevent/fetchEvents",
  async (graceids) => {
    if (graceids.length === 0) return [];
    const { data } = await http.get<ApiEnvelope<EventDoc[]>>("/api/events", {
      params: { limit: 200 },
    });
    return data.data.filter((e) => graceids.includes(e._id));
  },
);

export const fetchLocalizeRequests = createAsyncThunk<
  LocalizeRequestDoc[],
  string
>("superevent/fetchLocalizeRequests", async (supereventId) => {
  const { data } = await http.get<ApiEnvelope<LocalizeRequestDoc[]>>(
    "/api/localize-requests",
    { params: { superevent_id: supereventId, limit: 50 } },
  );
  return data.data;
});

export const fetchLocalizeResults = createAsyncThunk<
  LocalizeResultDoc[],
  string
>("superevent/fetchLocalizeResults", async (supereventId) => {
  const { data } = await http.get<ApiEnvelope<LocalizeResultDoc[]>>(
    "/api/localize-results",
    { params: { superevent_id: supereventId, limit: 50 } },
  );
  return data.data;
});

const slice = createSlice({
  name: "superevent",
  initialState,
  reducers: {
    clear(state) {
      state.doc = null;
      state.events = [];
      state.localizeRequests = [];
      state.localizeResults = [];
      state.error = null;
    },
  },
  extraReducers: (b) => {
    b.addCase(fetchSuperevent.pending, (state) => {
      state.loading = true;
      state.error = null;
    });
    b.addCase(fetchSuperevent.fulfilled, (state, action) => {
      state.loading = false;
      state.doc = action.payload;
    });
    b.addCase(fetchSuperevent.rejected, (state, action) => {
      state.loading = false;
      state.error = action.error.message ?? "Failed to load superevent";
    });
    b.addCase(fetchSupereventEvents.fulfilled, (state, action) => {
      state.events = action.payload;
    });
    b.addCase(fetchLocalizeRequests.fulfilled, (state, action) => {
      state.localizeRequests = action.payload;
    });
    b.addCase(fetchLocalizeResults.fulfilled, (state, action) => {
      state.localizeResults = action.payload;
    });
  },
});

export const { clear } = slice.actions;
export default slice.reducer;
