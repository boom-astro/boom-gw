// Read-only access metadata: the role catalog and the ACL vocabulary.
// `/api/roles`, `/api/acls`.

import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { http } from "../api";
import type { ApiEnvelope, RoleDoc } from "../types/api";

interface AccessMetaState {
  roles: RoleDoc[];
  acls: string[];
  loading: boolean;
  error: string | null;
}

const initialState: AccessMetaState = {
  roles: [],
  acls: [],
  loading: false,
  error: null,
};

export const fetchRoles = createAsyncThunk<RoleDoc[]>(
  "accessMeta/fetchRoles",
  async () => {
    const { data } = await http.get<ApiEnvelope<RoleDoc[]>>("/api/roles");
    return data.data;
  },
);

export const fetchAcls = createAsyncThunk<string[]>(
  "accessMeta/fetchAcls",
  async () => {
    const { data } = await http.get<ApiEnvelope<string[]>>("/api/acls");
    return data.data;
  },
);

const slice = createSlice({
  name: "accessMeta",
  initialState,
  reducers: {},
  extraReducers: (b) => {
    b.addCase(fetchRoles.fulfilled, (s, a) => {
      s.roles = a.payload;
    });
    b.addCase(fetchAcls.fulfilled, (s, a) => {
      s.acls = a.payload;
    });
  },
});

export default slice.reducer;
