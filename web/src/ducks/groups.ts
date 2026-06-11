// Groups + membership + group-stream access. CRUD against
// `/api/groups`. Mirrors the scienceFilters duck conventions.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type { ApiEnvelope, GroupDoc } from "../types/api";

interface GroupsState {
  items: GroupDoc[];
  current: GroupDoc | null;
  loading: boolean;
  saving: boolean;
  error: string | null;
}

const initialState: GroupsState = {
  items: [],
  current: null,
  loading: false,
  saving: false,
  error: null,
};

function errMessage(e: unknown): string {
  const ax = e as { response?: { data?: { message?: string } } };
  return ax.response?.data?.message ?? (e as Error).message;
}

export const fetchGroups = createAsyncThunk<GroupDoc[]>(
  "groups/fetch",
  async () => {
    const { data } = await http.get<ApiEnvelope<GroupDoc[]>>("/api/groups");
    return data.data;
  },
);

export const fetchGroup = createAsyncThunk<GroupDoc, string>(
  "groups/fetchOne",
  async (id) => {
    const { data } = await http.get<ApiEnvelope<GroupDoc>>(
      `/api/groups/${encodeURIComponent(id)}`,
    );
    return data.data;
  },
);

export const createGroup = createAsyncThunk<
  GroupDoc,
  { name: string; description?: string },
  { rejectValue: string }
>("groups/create", async (payload, { rejectWithValue }) => {
  try {
    const { data } = await http.post<ApiEnvelope<GroupDoc>>(
      "/api/groups",
      payload,
    );
    return data.data;
  } catch (e) {
    return rejectWithValue(errMessage(e));
  }
});

export const deleteGroup = createAsyncThunk<
  string,
  string,
  { rejectValue: string }
>("groups/delete", async (id, { rejectWithValue }) => {
  try {
    await http.delete(`/api/groups/${encodeURIComponent(id)}`);
    return id;
  } catch (e) {
    return rejectWithValue(errMessage(e));
  }
});

export const addGroupMember = createAsyncThunk<
  GroupDoc,
  { groupId: string; sub: string; admin: boolean },
  { rejectValue: string }
>("groups/addMember", async ({ groupId, sub, admin }, { rejectWithValue }) => {
  try {
    const { data } = await http.post<ApiEnvelope<GroupDoc>>(
      `/api/groups/${encodeURIComponent(groupId)}/members`,
      { sub, admin },
    );
    return data.data;
  } catch (e) {
    return rejectWithValue(errMessage(e));
  }
});

export const removeGroupMember = createAsyncThunk<
  GroupDoc,
  { groupId: string; sub: string },
  { rejectValue: string }
>("groups/removeMember", async ({ groupId, sub }, { rejectWithValue }) => {
  try {
    const { data } = await http.delete<ApiEnvelope<GroupDoc>>(
      `/api/groups/${encodeURIComponent(groupId)}/members/${encodeURIComponent(sub)}`,
    );
    return data.data;
  } catch (e) {
    return rejectWithValue(errMessage(e));
  }
});

export const addGroupStream = createAsyncThunk<
  GroupDoc,
  { groupId: string; streamId: string },
  { rejectValue: string }
>("groups/addStream", async ({ groupId, streamId }, { rejectWithValue }) => {
  try {
    const { data } = await http.post<ApiEnvelope<GroupDoc>>(
      `/api/groups/${encodeURIComponent(groupId)}/streams`,
      { stream_id: streamId },
    );
    return data.data;
  } catch (e) {
    return rejectWithValue(errMessage(e));
  }
});

export const removeGroupStream = createAsyncThunk<
  GroupDoc,
  { groupId: string; streamId: string },
  { rejectValue: string }
>("groups/removeStream", async ({ groupId, streamId }, { rejectWithValue }) => {
  try {
    const { data } = await http.delete<ApiEnvelope<GroupDoc>>(
      `/api/groups/${encodeURIComponent(groupId)}/streams/${encodeURIComponent(streamId)}`,
    );
    return data.data;
  } catch (e) {
    return rejectWithValue(errMessage(e));
  }
});

const slice = createSlice({
  name: "groups",
  initialState,
  reducers: {
    clearError(state) {
      state.error = null;
    },
  },
  extraReducers: (b) => {
    b.addCase(fetchGroups.pending, (s) => {
      s.loading = true;
      s.error = null;
    });
    b.addCase(fetchGroups.fulfilled, (s, a) => {
      s.loading = false;
      s.items = a.payload;
    });
    b.addCase(fetchGroups.rejected, (s, a) => {
      s.loading = false;
      s.error = a.error.message ?? "Failed to load groups";
    });
    b.addCase(fetchGroup.fulfilled, (s, a) => {
      s.current = a.payload;
    });
    b.addCase(createGroup.fulfilled, (s, a) => {
      s.items.unshift(a.payload);
    });
    b.addCase(createGroup.rejected, (s, a) => {
      s.error = a.payload ?? "Failed to create group";
    });
    b.addCase(deleteGroup.fulfilled, (s, a) => {
      s.items = s.items.filter((g) => g.id !== a.payload);
      if (s.current?.id === a.payload) s.current = null;
    });
    // Member / stream mutations all echo the refreshed group.
    for (const thunk of [
      addGroupMember,
      removeGroupMember,
      addGroupStream,
      removeGroupStream,
    ]) {
      b.addCase(thunk.fulfilled, (s, a) => {
        s.current = a.payload;
      });
      b.addCase(thunk.rejected, (s, a) => {
        s.error = (a.payload as string) ?? "Group update failed";
      });
    }
  },
});

export const { clearError } = slice.actions;
export default slice.reducer;
