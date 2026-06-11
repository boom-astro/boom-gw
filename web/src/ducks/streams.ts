// Stream catalog + admin operations. `/api/streams`.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type { ApiEnvelope, StreamDoc } from "../types/api";

interface StreamsState {
  items: StreamDoc[];
  loading: boolean;
  saving: boolean;
  error: string | null;
}

const initialState: StreamsState = {
  items: [],
  loading: false,
  saving: false,
  error: null,
};

function errMessage(e: unknown): string {
  const ax = e as { response?: { data?: { message?: string } } };
  return ax.response?.data?.message ?? (e as Error).message;
}

export const fetchStreams = createAsyncThunk<StreamDoc[]>(
  "streams/fetch",
  async () => {
    const { data } = await http.get<ApiEnvelope<StreamDoc[]>>("/api/streams");
    return data.data;
  },
);

export const createStream = createAsyncThunk<
  StreamDoc,
  { id: string; name: string; description?: string },
  { rejectValue: string }
>("streams/create", async (payload, { rejectWithValue }) => {
  try {
    const { data } = await http.post<ApiEnvelope<StreamDoc>>(
      "/api/streams",
      payload,
    );
    return data.data;
  } catch (e) {
    return rejectWithValue(errMessage(e));
  }
});

export const grantStreamAccess = createAsyncThunk<
  { streamId: string; sub: string },
  { streamId: string; sub: string },
  { rejectValue: string }
>("streams/grant", async ({ streamId, sub }, { rejectWithValue }) => {
  try {
    await http.post(`/api/streams/${encodeURIComponent(streamId)}/users`, {
      sub,
    });
    return { streamId, sub };
  } catch (e) {
    return rejectWithValue(errMessage(e));
  }
});

const slice = createSlice({
  name: "streams",
  initialState,
  reducers: {
    clearError(state) {
      state.error = null;
    },
  },
  extraReducers: (b) => {
    b.addCase(fetchStreams.pending, (s) => {
      s.loading = true;
      s.error = null;
    });
    b.addCase(fetchStreams.fulfilled, (s, a) => {
      s.loading = false;
      s.items = a.payload;
    });
    b.addCase(fetchStreams.rejected, (s, a) => {
      s.loading = false;
      s.error = a.error.message ?? "Failed to load streams";
    });
    b.addCase(createStream.fulfilled, (s, a) => {
      const idx = s.items.findIndex((x) => x._id === a.payload._id);
      if (idx >= 0) s.items[idx] = a.payload;
      else s.items.push(a.payload);
    });
    b.addCase(createStream.rejected, (s, a) => {
      s.error = a.payload ?? "Failed to create stream";
    });
    b.addCase(grantStreamAccess.rejected, (s, a) => {
      s.error = a.payload ?? "Failed to grant stream access";
    });
  },
});

export const { clearError } = slice.actions;
export default slice.reducer;
