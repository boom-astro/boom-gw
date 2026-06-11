// Groups landing page: the groups you belong to (or all, with Manage
// groups), plus a create dialog. Row click opens the detail page.

import { useEffect, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from "@mui/icons-material/Delete";
import { useNavigate } from "react-router-dom";
import {
  clearError,
  createGroup,
  deleteGroup,
  fetchGroups,
} from "../ducks/groups";
import { useAppDispatch, useAppSelector } from "../store";
import { useHasAcl } from "../hooks/access";
import { ACL_MANAGE_GROUPS } from "../types/access";

export function GroupsPage() {
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const groups = useAppSelector((s) => s.groups.items);
  const loading = useAppSelector((s) => s.groups.loading);
  const error = useAppSelector((s) => s.groups.error);
  const canManage = useHasAcl(ACL_MANAGE_GROUPS);

  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");

  useEffect(() => {
    dispatch(fetchGroups());
  }, [dispatch]);

  async function onCreate() {
    if (!name.trim()) return;
    const res = await dispatch(createGroup({ name: name.trim(), description }));
    if (createGroup.fulfilled.match(res)) {
      setOpen(false);
      setName("");
      setDescription("");
      navigate(`/groups/${res.payload.id}`);
    }
  }

  return (
    <Stack spacing={2}>
      <Stack direction="row" alignItems="center" spacing={2}>
        <Typography variant="h5">Groups</Typography>
        {loading && <CircularProgress size={18} />}
        <Box sx={{ flexGrow: 1 }} />
        {canManage && (
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => setOpen(true)}
          >
            New group
          </Button>
        )}
      </Stack>
      <Typography variant="body2" color="text.secondary">
        Groups are the unit of sharing: a science filter shared with a group is
        visible to its members, and a filter can only draw from the group's
        streams.
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
              <TableCell>Name</TableCell>
              <TableCell>Description</TableCell>
              <TableCell>Role</TableCell>
              <TableCell align="right">Open</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {groups.map((g) => (
              <TableRow
                key={g.id}
                hover
                sx={{ cursor: "pointer" }}
                onClick={() => navigate(`/groups/${g.id}`)}
              >
                <TableCell>
                  <Typography variant="body2" sx={{ fontWeight: 600 }}>
                    {g.name}
                  </Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="caption" color="text.secondary">
                    {g.description}
                  </Typography>
                </TableCell>
                <TableCell>
                  {g.admin ? (
                    <Chip size="small" color="primary" label="admin" />
                  ) : (
                    <Chip size="small" variant="outlined" label="member" />
                  )}
                </TableCell>
                <TableCell align="right" onClick={(e) => e.stopPropagation()}>
                  {(g.admin || canManage) && (
                    <Tooltip title="Delete group">
                      <IconButton
                        size="small"
                        aria-label="Delete group"
                        onClick={() => {
                          if (window.confirm(`Delete group "${g.name}"?`)) {
                            dispatch(deleteGroup(g.id));
                          }
                        }}
                      >
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  )}
                </TableCell>
              </TableRow>
            ))}
            {groups.length === 0 && !loading && (
              <TableRow>
                <TableCell colSpan={4}>
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ py: 3, textAlign: "center" }}
                  >
                    You're not in any groups yet.
                    {canManage ? " Create one to get started." : ""}
                  </Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </Paper>

      <Dialog
        open={open}
        onClose={() => setOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>New group</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2} sx={{ mt: 0.5 }}>
            <TextField
              label="Name"
              size="small"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              fullWidth
            />
            <TextField
              label="Description"
              size="small"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              fullWidth
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            onClick={onCreate}
            disabled={name.trim() === ""}
          >
            Create
          </Button>
        </DialogActions>
      </Dialog>
    </Stack>
  );
}
