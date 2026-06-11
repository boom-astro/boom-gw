// User roster + role assignment. `/api/users`. Roster reads are open
// to any signed-in user (member/role pickers); role assignment is
// gated server-side on `Manage users`.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type { ApiEnvelope, UserDoc } from "../types/api";

interface UsersState {
  items: UserDoc[];
  loading: boolean;
  saving: boolean;
  error: string | null;
}

const initialState: UsersState = {
  items: [],
  loading: false,
  saving: false,
  error: null,
};

function errMessage(e: unknown): string {
  const ax = e as { response?: { data?: { message?: string } } };
  return ax.response?.data?.message ?? (e as Error).message;
}

export const fetchUsers = createAsyncThunk<UserDoc[]>(
  "users/fetch",
  async () => {
    const { data } = await http.get<ApiEnvelope<UserDoc[]>>("/api/users");
    return data.data;
  },
);

export const assignRoles = createAsyncThunk<
  UserDoc,
  { sub: string; roles: string[] },
  { rejectValue: string }
>("users/assignRoles", async ({ sub, roles }, { rejectWithValue }) => {
  try {
    const { data } = await http.patch<ApiEnvelope<UserDoc>>(
      `/api/users/${encodeURIComponent(sub)}`,
      { role_ids: roles },
    );
    return data.data;
  } catch (e) {
    return rejectWithValue(errMessage(e));
  }
});

const slice = createSlice({
  name: "users",
  initialState,
  reducers: {
    clearError(state) {
      state.error = null;
    },
  },
  extraReducers: (b) => {
    b.addCase(fetchUsers.pending, (s) => {
      s.loading = true;
      s.error = null;
    });
    b.addCase(fetchUsers.fulfilled, (s, a) => {
      s.loading = false;
      s.items = a.payload;
    });
    b.addCase(fetchUsers.rejected, (s, a) => {
      s.loading = false;
      s.error = a.error.message ?? "Failed to load users";
    });
    b.addCase(assignRoles.fulfilled, (s, a) => {
      // The PATCH echoes the full UserDoc (with `_id` as sub on the
      // wire); normalize to the roster shape on `sub`.
      const updated = a.payload as UserDoc & { _id?: string };
      const sub = updated.sub ?? updated._id;
      const idx = s.items.findIndex((u) => u.sub === sub);
      if (idx >= 0) s.items[idx] = { ...s.items[idx], roles: updated.roles };
    });
    b.addCase(assignRoles.rejected, (s, a) => {
      s.error = a.payload ?? "Failed to assign roles";
    });
  },
});

export const { clearError } = slice.actions;
export default slice.reducer;
