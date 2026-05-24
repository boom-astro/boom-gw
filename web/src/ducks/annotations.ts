// Annotations attached to a superevent. SkyPortal models comments
// + annotations separately; we collapse them into a single
// kind-tagged stream since gw-api does too.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type { AnnotationDoc, ApiEnvelope } from "../types/api";

interface AnnotationsState {
  bySuperevent: Record<string, AnnotationDoc[]>;
  loading: boolean;
  error: string | null;
}

const initialState: AnnotationsState = {
  bySuperevent: {},
  loading: false,
  error: null,
};

export const fetchAnnotations = createAsyncThunk<
  { supereventId: string; items: AnnotationDoc[] },
  string
>("annotations/fetch", async (supereventId) => {
  const { data } = await http.get<ApiEnvelope<AnnotationDoc[]>>(
    `/api/superevents/${supereventId}/annotations`,
    { params: { limit: 200 } },
  );
  return { supereventId, items: data.data };
});

export interface CreateAnnotationInput {
  supereventId: string;
  kind: string;
  author: string;
  payload: unknown;
}

export const createAnnotation = createAsyncThunk<
  { supereventId: string; item: AnnotationDoc },
  CreateAnnotationInput
>("annotations/create", async ({ supereventId, kind, author, payload }) => {
  const { data } = await http.post<ApiEnvelope<AnnotationDoc>>(
    `/api/superevents/${supereventId}/annotations`,
    { kind, author, payload },
  );
  return { supereventId, item: data.data };
});

const slice = createSlice({
  name: "annotations",
  initialState,
  reducers: {},
  extraReducers: (b) => {
    b.addCase(fetchAnnotations.pending, (state) => {
      state.loading = true;
      state.error = null;
    });
    b.addCase(fetchAnnotations.fulfilled, (state, action) => {
      state.loading = false;
      state.bySuperevent[action.payload.supereventId] = action.payload.items;
    });
    b.addCase(fetchAnnotations.rejected, (state, action) => {
      state.loading = false;
      state.error = action.error.message ?? "Failed to load annotations";
    });
    b.addCase(createAnnotation.fulfilled, (state, action) => {
      const { supereventId, item } = action.payload;
      const existing = state.bySuperevent[supereventId] ?? [];
      state.bySuperevent[supereventId] = [item, ...existing];
    });
  },
});

export default slice.reducer;
