// Group detail: members (with admin toggle + remove) and the group's
// stream access. Mutating controls are gated by group-admin / Manage
// groups. After membership/stream changes that affect the current
// user, we refresh `me` so the science-filter pickers stay current.

import { useEffect, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Chip,
  Divider,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  Stack,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Tooltip,
  Typography,
} from "@mui/material";
import DeleteIcon from "@mui/icons-material/Delete";
import AddIcon from "@mui/icons-material/Add";
import { useParams } from "react-router-dom";
import {
  addGroupMember,
  addGroupStream,
  clearError,
  fetchGroup,
  removeGroupMember,
  removeGroupStream,
} from "../ducks/groups";
import { fetchStreams } from "../ducks/streams";
import { loadMe } from "../ducks/auth";
import { useAppDispatch, useAppSelector } from "../store";
import { useHasAcl, useIsGroupAdmin } from "../hooks/access";
import { ACL_MANAGE_GROUPS } from "../types/access";
import { UserPicker } from "./UserPicker";

export function GroupDetailPage() {
  const { id = "" } = useParams();
  const dispatch = useAppDispatch();
  const group = useAppSelector((s) => s.groups.current);
  const error = useAppSelector((s) => s.groups.error);
  const allStreams = useAppSelector((s) => s.streams.items);
  const canManage = useIsGroupAdmin(id) || useHasAcl(ACL_MANAGE_GROUPS);

  const [newMember, setNewMember] = useState("");
  const [newMemberAdmin, setNewMemberAdmin] = useState(false);
  const [streamToAdd, setStreamToAdd] = useState("");

  useEffect(() => {
    dispatch(fetchGroup(id));
    dispatch(fetchStreams());
  }, [dispatch, id]);

  // Keep `me` (and thus the filter pickers) fresh after a change.
  const refresh = () => dispatch(loadMe());

  if (!group || group.id !== id) {
    return (
      <Typography variant="body2" color="text.secondary">
        Loading group…
      </Typography>
    );
  }

  const members = group.members ?? [];
  const streams = group.streams ?? [];
  const grantedIds = new Set(streams.map((s) => s.id));
  const available = allStreams.filter((s) => !grantedIds.has(s._id));

  async function onAddMember() {
    if (!newMember.trim()) return;
    await dispatch(
      addGroupMember({ groupId: id, sub: newMember.trim(), admin: newMemberAdmin }),
    );
    setNewMember("");
    setNewMemberAdmin(false);
    refresh();
  }

  return (
    <Stack spacing={2}>
      <Typography variant="h5">{group.name}</Typography>
      {group.description && (
        <Typography variant="body2" color="text.secondary">
          {group.description}
        </Typography>
      )}

      {error && (
        <Alert severity="error" onClose={() => dispatch(clearError())}>
          {error}
        </Alert>
      )}

      <Paper sx={{ p: 2 }}>
        <Typography variant="subtitle2" gutterBottom>
          Members
        </Typography>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>User</TableCell>
              <TableCell>Admin</TableCell>
              <TableCell align="right" />
            </TableRow>
          </TableHead>
          <TableBody>
            {members.map((m) => (
              <TableRow key={m.sub}>
                <TableCell>
                  <Typography variant="body2">
                    {m.display_name || m.sub}
                  </Typography>
                  {m.display_name && (
                    <Typography variant="caption" color="text.secondary">
                      {m.sub}
                    </Typography>
                  )}
                </TableCell>
                <TableCell>
                  <Switch
                    size="small"
                    checked={m.admin}
                    disabled={!canManage}
                    onChange={async (e) => {
                      await dispatch(
                        addGroupMember({
                          groupId: id,
                          sub: m.sub,
                          admin: e.target.checked,
                        }),
                      );
                      refresh();
                    }}
                  />
                </TableCell>
                <TableCell align="right">
                  {canManage && (
                    <Tooltip title="Remove member">
                      <IconButton
                        size="small"
                        aria-label="Remove member"
                        onClick={async () => {
                          await dispatch(
                            removeGroupMember({ groupId: id, sub: m.sub }),
                          );
                          refresh();
                        }}
                      >
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        {canManage && (
          <Stack direction="row" spacing={1.5} alignItems="center" sx={{ mt: 2 }}>
            <UserPicker value={newMember} onPick={setNewMember} />
            <Tooltip title="Make this member a group admin">
              <Box>
                <Switch
                  size="small"
                  checked={newMemberAdmin}
                  onChange={(e) => setNewMemberAdmin(e.target.checked)}
                />
              </Box>
            </Tooltip>
            <Button
              variant="outlined"
              size="small"
              startIcon={<AddIcon />}
              onClick={onAddMember}
              disabled={!newMember.trim()}
            >
              Add member
            </Button>
          </Stack>
        )}
      </Paper>

      <Paper sx={{ p: 2 }}>
        <Typography variant="subtitle2" gutterBottom>
          Streams
        </Typography>
        <Stack direction="row" spacing={0.5} flexWrap="wrap" useFlexGap>
          {streams.map((s) => (
            <Chip
              key={s.id}
              label={s.name}
              size="small"
              onDelete={
                canManage
                  ? async () => {
                      await dispatch(
                        removeGroupStream({ groupId: id, streamId: s.id }),
                      );
                      refresh();
                    }
                  : undefined
              }
            />
          ))}
          {streams.length === 0 && (
            <Typography variant="caption" color="text.secondary">
              No streams granted — filters in this group can't draw from any
              messenger channel yet.
            </Typography>
          )}
        </Stack>
        {canManage && available.length > 0 && (
          <Stack direction="row" spacing={1.5} alignItems="center" sx={{ mt: 2 }}>
            <Divider sx={{ my: 1 }} />
            <FormControl size="small" sx={{ minWidth: 220 }}>
              <InputLabel id="add-stream-label">Add stream</InputLabel>
              <Select
                labelId="add-stream-label"
                label="Add stream"
                value={streamToAdd}
                onChange={(e) => setStreamToAdd(e.target.value)}
              >
                {available.map((s) => (
                  <MenuItem key={s._id} value={s._id}>
                    {s.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <Button
              variant="outlined"
              size="small"
              startIcon={<AddIcon />}
              disabled={!streamToAdd}
              onClick={async () => {
                await dispatch(
                  addGroupStream({ groupId: id, streamId: streamToAdd }),
                );
                setStreamToAdd("");
                refresh();
              }}
            >
              Grant
            </Button>
          </Stack>
        )}
      </Paper>
    </Stack>
  );
}
