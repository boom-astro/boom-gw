// Superevents list. Plain MUI Table — we dropped mui-datatables
// (peer-conflicts MUI v5) and don't yet need column reordering /
// CSV export, so a thin sortable table is enough. If we end up
// reaching for those features we can swap in @tanstack/table.
//
// Pagination is server-side: each page change refetches `skip=` /
// `limit=` and the total comes from a separate `/api/superevents/
// count`. Sorting is intentionally limited to the columns the
// backend can sort on cheaply (`t_0` is the natural index); the
// "ID" and "SNR" sort buttons are gone for now — operators usually
// scan by time, and a UI affordance for sort orders we can't
// actually push down would mislead the user.

import { useEffect, useState } from "react";
import {
  Alert,
  Box,
  Chip,
  CircularProgress,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TablePagination,
  TableRow,
  Typography,
} from "@mui/material";
import { useNavigate } from "react-router-dom";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import { fetchSuperevents, fetchSupereventsCount } from "../ducks/superevents";
import { useAppDispatch, useAppSelector } from "../store";
import type { SupereventDoc } from "../types/api";

dayjs.extend(utc);

function fmtGps(t: number | undefined): string {
  if (t === undefined || t === null) return "—";
  // SupereventDoc.t_0 is GPS seconds. Convert to UTC for humans.
  // GPS epoch = 1980-01-06 00:00:00 UTC; 18 leap-second offset as of 2017.
  const unix = t + 315964800 - 18;
  return dayjs.unix(unix).utc().format("YYYY-MM-DD HH:mm:ss[Z]");
}

export function SuperEventsPage() {
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const { items, total, loading, error } = useAppSelector((s) => s.superevents);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);

  // Fetch this page + the current total on every page/size change.
  // Refresh the count alongside the items so a freshly-arrived
  // superevent in the background shifts "of N" up without a manual
  // reload.
  useEffect(() => {
    dispatch(
      fetchSuperevents({ limit: rowsPerPage, skip: page * rowsPerPage }),
    );
    dispatch(fetchSupereventsCount());
  }, [dispatch, page, rowsPerPage]);

  return (
    <Stack spacing={2}>
      <Box>
        <Typography variant="h5">Superevents</Typography>
        <Typography variant="body2" color="text.secondary">
          {total != null ? `${total} total` : `${items.length} loaded`}
          {loading && <CircularProgress size={14} sx={{ ml: 1 }} />}
        </Typography>
      </Box>
      {error && <Alert severity="error">{error}</Alert>}
      <Paper>
        <Table size="small" stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell>ID</TableCell>
              <TableCell>t₀ (UTC)</TableCell>
              <TableCell>Preferred GraceID</TableCell>
              <TableCell align="right">SNR</TableCell>
              <TableCell align="right">G-events</TableCell>
              <TableCell>Skymap</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {items.map((s: SupereventDoc) => (
              <TableRow
                key={s._id}
                hover
                sx={{ cursor: "pointer" }}
                onClick={() => navigate(`/superevents/${s._id}`)}
              >
                <TableCell>
                  <code>{s._id}</code>
                </TableCell>
                <TableCell>{fmtGps(s.t_0)}</TableCell>
                <TableCell>
                  <code>{s.preferred_graceid}</code>
                </TableCell>
                <TableCell align="right">
                  {s.preferred_snr.toFixed(2)}
                </TableCell>
                <TableCell align="right">{s.g_event_graceids.length}</TableCell>
                <TableCell>
                  {s.skymap_summary ? (
                    <Chip
                      size="small"
                      color="success"
                      label={`${(s.skymap_summary.bytes_size / 1024).toFixed(0)} KB`}
                    />
                  ) : (
                    <Chip size="small" label="—" />
                  )}
                </TableCell>
              </TableRow>
            ))}
            {items.length === 0 && !loading && (
              <TableRow>
                <TableCell colSpan={6} align="center">
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ py: 4 }}
                  >
                    No superevents yet.
                  </Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
        <TablePagination
          component="div"
          // Pass -1 when we don't know the total yet so MUI renders
          // "X-Y of more than Z" instead of a misleading "of N".
          count={total ?? -1}
          page={page}
          onPageChange={(_, p) => setPage(p)}
          rowsPerPage={rowsPerPage}
          onRowsPerPageChange={(e) => {
            setRowsPerPage(parseInt(e.target.value, 10));
            setPage(0);
          }}
          rowsPerPageOptions={[10, 25, 50, 100, 250]}
        />
      </Paper>
    </Stack>
  );
}
