// Superevents list. Plain MUI Table — we dropped mui-datatables
// (peer-conflicts MUI v5) and don't yet need column reordering /
// CSV export, so a thin sortable table is enough. If we end up
// reaching for those features we can swap in @tanstack/table.

import { useEffect, useMemo, useState } from "react";
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
  TableSortLabel,
  Typography,
} from "@mui/material";
import { useNavigate } from "react-router-dom";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import { fetchSuperevents } from "../ducks/superevents";
import { useAppDispatch, useAppSelector } from "../store";
import type { SupereventDoc } from "../types/api";

dayjs.extend(utc);

type SortKey = "t_0" | "preferred_snr" | "_id";

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
  const { items, loading, error } = useAppSelector((s) => s.superevents);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);
  const [sortKey, setSortKey] = useState<SortKey>("t_0");
  const [sortDesc, setSortDesc] = useState(true);

  useEffect(() => {
    dispatch(fetchSuperevents({ limit: 500 }));
  }, [dispatch]);

  const sorted = useMemo(() => {
    const copy = [...items];
    copy.sort((a, b) => {
      const av = a[sortKey] as number | string | undefined;
      const bv = b[sortKey] as number | string | undefined;
      if (av === bv) return 0;
      if (av === undefined || av === null) return 1;
      if (bv === undefined || bv === null) return -1;
      return (av > bv ? 1 : -1) * (sortDesc ? -1 : 1);
    });
    return copy;
  }, [items, sortKey, sortDesc]);

  const visible = sorted.slice(
    page * rowsPerPage,
    page * rowsPerPage + rowsPerPage,
  );

  function toggleSort(key: SortKey) {
    if (key === sortKey) {
      setSortDesc(!sortDesc);
    } else {
      setSortKey(key);
      setSortDesc(true);
    }
  }

  return (
    <Stack spacing={2}>
      <Box>
        <Typography variant="h5">Superevents</Typography>
        <Typography variant="body2" color="text.secondary">
          {items.length} loaded
          {loading && <CircularProgress size={14} sx={{ ml: 1 }} />}
        </Typography>
      </Box>
      {error && <Alert severity="error">{error}</Alert>}
      <Paper>
        <Table size="small" stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell sortDirection={sortKey === "_id" ? (sortDesc ? "desc" : "asc") : false}>
                <TableSortLabel
                  active={sortKey === "_id"}
                  direction={sortDesc ? "desc" : "asc"}
                  onClick={() => toggleSort("_id")}
                >
                  ID
                </TableSortLabel>
              </TableCell>
              <TableCell sortDirection={sortKey === "t_0" ? (sortDesc ? "desc" : "asc") : false}>
                <TableSortLabel
                  active={sortKey === "t_0"}
                  direction={sortDesc ? "desc" : "asc"}
                  onClick={() => toggleSort("t_0")}
                >
                  t₀ (UTC)
                </TableSortLabel>
              </TableCell>
              <TableCell>Preferred GraceID</TableCell>
              <TableCell
                align="right"
                sortDirection={
                  sortKey === "preferred_snr" ? (sortDesc ? "desc" : "asc") : false
                }
              >
                <TableSortLabel
                  active={sortKey === "preferred_snr"}
                  direction={sortDesc ? "desc" : "asc"}
                  onClick={() => toggleSort("preferred_snr")}
                >
                  SNR
                </TableSortLabel>
              </TableCell>
              <TableCell align="right">G-events</TableCell>
              <TableCell>Skymap</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {visible.map((s: SupereventDoc) => (
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
                <TableCell align="right">
                  {s.g_event_graceids.length}
                </TableCell>
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
            {visible.length === 0 && !loading && (
              <TableRow>
                <TableCell colSpan={6} align="center">
                  <Typography variant="body2" color="text.secondary" sx={{ py: 4 }}>
                    No superevents yet.
                  </Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
        <TablePagination
          component="div"
          count={sorted.length}
          page={page}
          onPageChange={(_, p) => setPage(p)}
          rowsPerPage={rowsPerPage}
          onRowsPerPageChange={(e) => {
            setRowsPerPage(parseInt(e.target.value, 10));
            setPage(0);
          }}
          rowsPerPageOptions={[10, 25, 50, 100]}
        />
      </Paper>
    </Stack>
  );
}
