// Admin: stream catalog + direct access grants. Gated by Manage
// streams (route wrapped in RequireAcl).

import { useEffect, useState } from "react";
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import {
  clearError,
  createStream,
  fetchStreams,
  grantStreamAccess,
} from "../ducks/streams";
import { useAppDispatch, useAppSelector } from "../store";
import { UserPicker } from "./UserPicker";

export function AdminStreamsPage() {
  const dispatch = useAppDispatch();
  const streams = useAppSelector((s) => s.streams.items);
  const loading = useAppSelector((s) => s.streams.loading);
  const error = useAppSelector((s) => s.streams.error);

  const [open, setOpen] = useState(false);
  const [id, setId] = useState("");
  const [name, setName] = useState("");
  const [grantFor, setGrantFor] = useState<string | null>(null);
  const [grantSub, setGrantSub] = useState("");

  useEffect(() => {
    dispatch(fetchStreams());
  }, [dispatch]);

  async function onCreate() {
    if (!id.trim() || !name.trim()) return;
    const res = await dispatch(
      createStream({ id: id.trim(), name: name.trim() }),
    );
    if (createStream.fulfilled.match(res)) {
      setOpen(false);
      setId("");
      setName("");
    }
  }

  return (
    <Stack spacing={2}>
      <Stack direction="row" alignItems="center" spacing={2}>
        <Typography variant="h5">Streams</Typography>
        {loading && <CircularProgress size={18} />}
        <Box sx={{ flexGrow: 1 }} />
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => setOpen(true)}
        >
          New stream
        </Button>
      </Stack>
      <Typography variant="body2" color="text.secondary">
        Streams are the messenger ingest channels. Grant a group access (on the
        group page) or a user direct access here.
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
              <TableCell>Stream</TableCell>
              <TableCell>ID</TableCell>
              <TableCell align="right">Grant access</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {streams.map((s) => (
              <TableRow key={s._id}>
                <TableCell>
                  <Typography variant="body2" sx={{ fontWeight: 600 }}>
                    {s.name}
                  </Typography>
                </TableCell>
                <TableCell>
                  <code>{s._id}</code>
                </TableCell>
                <TableCell align="right">
                  {grantFor === s._id ? (
                    <Stack
                      direction="row"
                      spacing={1}
                      alignItems="center"
                      justifyContent="flex-end"
                    >
                      <UserPicker value={grantSub} onPick={setGrantSub} />
                      <Button
                        size="small"
                        variant="outlined"
                        disabled={!grantSub.trim()}
                        onClick={async () => {
                          await dispatch(
                            grantStreamAccess({
                              streamId: s._id,
                              sub: grantSub.trim(),
                            }),
                          );
                          setGrantFor(null);
                          setGrantSub("");
                        }}
                      >
                        Grant
                      </Button>
                      <Button size="small" onClick={() => setGrantFor(null)}>
                        Cancel
                      </Button>
                    </Stack>
                  ) : (
                    <Button size="small" onClick={() => setGrantFor(s._id)}>
                      Grant to user…
                    </Button>
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Paper>

      <Dialog
        open={open}
        onClose={() => setOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>New stream</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2} sx={{ mt: 0.5 }}>
            <TextField
              label="ID (slug)"
              size="small"
              value={id}
              onChange={(e) => setId(e.target.value)}
              required
              fullWidth
            />
            <TextField
              label="Name"
              size="small"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              fullWidth
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            onClick={onCreate}
            disabled={!id.trim() || !name.trim()}
          >
            Create
          </Button>
        </DialogActions>
      </Dialog>
    </Stack>
  );
}
