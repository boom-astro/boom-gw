// Admin: user roster + role assignment. Gated by Manage users (route
// wrapped in RequireAcl). Each row has a multi-select of roles.

import { useEffect } from "react";
import {
  Alert,
  Box,
  Chip,
  CircularProgress,
  FormControl,
  InputLabel,
  MenuItem,
  OutlinedInput,
  Paper,
  Select,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import { assignRoles, clearError, fetchUsers } from "../ducks/users";
import { fetchRoles } from "../ducks/accessMeta";
import { useAppDispatch, useAppSelector } from "../store";

export function AdminUsersPage() {
  const dispatch = useAppDispatch();
  const users = useAppSelector((s) => s.users.items);
  const roles = useAppSelector((s) => s.accessMeta.roles);
  const loading = useAppSelector((s) => s.users.loading);
  const error = useAppSelector((s) => s.users.error);

  useEffect(() => {
    dispatch(fetchUsers());
    dispatch(fetchRoles());
  }, [dispatch]);

  return (
    <Stack spacing={2}>
      <Stack direction="row" alignItems="center" spacing={2}>
        <Typography variant="h5">Users</Typography>
        {loading && <CircularProgress size={18} />}
        <Box sx={{ flexGrow: 1 }} />
      </Stack>
      <Typography variant="body2" color="text.secondary">
        Assign roles to grant ACLs. Roles bundle permissions; "Super admin"
        is the wildcard.
      </Typography>

      {error && (
        <Alert severity="error" onClose={() => dispatch(clearError())}>
          {error}
        </Alert>
      )}

      <Paper>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>User</TableCell>
              <TableCell>Roles</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {users.map((u) => (
              <TableRow key={u.sub}>
                <TableCell>
                  <Typography variant="body2">
                    {u.display_name || u.sub}
                  </Typography>
                  {u.display_name && (
                    <Typography variant="caption" color="text.secondary">
                      {u.sub}
                    </Typography>
                  )}
                </TableCell>
                <TableCell>
                  <FormControl size="small" sx={{ minWidth: 260 }}>
                    <InputLabel id={`roles-${u.sub}`}>Roles</InputLabel>
                    <Select
                      labelId={`roles-${u.sub}`}
                      multiple
                      value={u.roles ?? []}
                      input={<OutlinedInput label="Roles" />}
                      renderValue={(sel) => (
                        <Stack direction="row" spacing={0.5} flexWrap="wrap" useFlexGap>
                          {(sel as string[]).map((r) => (
                            <Chip key={r} size="small" label={r} />
                          ))}
                        </Stack>
                      )}
                      onChange={(e) => {
                        const value = e.target.value;
                        const roleIds =
                          typeof value === "string" ? value.split(",") : value;
                        dispatch(assignRoles({ sub: u.sub, roles: roleIds }));
                      }}
                    >
                      {roles.map((r) => (
                        <MenuItem key={r._id} value={r._id}>
                          {r.name}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </TableCell>
              </TableRow>
            ))}
            {users.length === 0 && !loading && (
              <TableRow>
                <TableCell colSpan={2}>
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ py: 3, textAlign: "center" }}
                  >
                    No users provisioned yet.
                  </Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </Paper>
    </Stack>
  );
}
