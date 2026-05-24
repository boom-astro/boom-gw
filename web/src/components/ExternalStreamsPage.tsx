// "External Streams" — top-level page that surfaces every non-GW
// signal we've ingested: GRB triggers from GCN and (once wired)
// BOOM cross-matched optical alerts. Lets the operator browse the
// raw event firehose independent of which superevents (if any)
// they've cross-matched against.

import { useEffect, useMemo, useState } from "react";
import {
  Alert,
  Box,
  CircularProgress,
  Paper,
  Stack,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TablePagination,
  TableRow,
  Tabs,
  Typography,
} from "@mui/material";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import { fetchBoomAlerts, fetchGrbTriggers } from "../ducks/externalAlerts";
import { useAppDispatch, useAppSelector } from "../store";
import type { BoomAlertDoc, GrbTriggerDoc } from "../types/api";

dayjs.extend(utc);

function fmtGps(t: number | null | undefined): string {
  if (t == null || !Number.isFinite(t) || t === 0) return "—";
  const unix = t + 315964800 - 18;
  return dayjs.unix(unix).utc().format("YYYY-MM-DD HH:mm:ss[Z]");
}

function fmtIngested(raw: GrbTriggerDoc["ingested_at"]): string {
  if (typeof raw === "string") return raw;
  const inner = raw?.$date;
  if (!inner) return "";
  if (typeof inner === "string") return inner;
  const ms = inner.$numberLong ? Number(inner.$numberLong) : 0;
  return new Date(ms).toISOString();
}

function GrbTriggersTable() {
  const items = useAppSelector((s) => s.externalAlerts.grbTriggers);
  const loading = useAppSelector((s) => s.externalAlerts.loading);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);

  // Newest first (by ingest time).
  const sorted = useMemo(() => {
    const copy = [...items];
    copy.sort((a, b) => (b.trigger_time ?? 0) - (a.trigger_time ?? 0));
    return copy;
  }, [items]);
  const visible = sorted.slice(
    page * rowsPerPage,
    page * rowsPerPage + rowsPerPage,
  );

  return (
    <Paper>
      <Box sx={{ p: 1.5, display: "flex", alignItems: "center", gap: 1 }}>
        <Typography variant="subtitle2">GRB triggers</Typography>
        {loading && <CircularProgress size={14} />}
        <Box sx={{ flexGrow: 1 }} />
        <Typography variant="caption" color="text.secondary">
          {items.length} loaded
        </Typography>
      </Box>
      <Table size="small" stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Instrument</TableCell>
            <TableCell>Trigger ID</TableCell>
            <TableCell>Trigger time (UTC)</TableCell>
            <TableCell align="right">RA (°)</TableCell>
            <TableCell align="right">Dec (°)</TableCell>
            <TableCell align="right">Err radius (°)</TableCell>
            <TableCell align="right">Significance</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {visible.map((t) => (
            <TableRow key={`${t.instrument}/${t.trigger_id}`}>
              <TableCell>{t.instrument}</TableCell>
              <TableCell>
                <code>{t.trigger_id}</code>
              </TableCell>
              <TableCell>{fmtGps(t.trigger_time)}</TableCell>
              <TableCell align="right">
                {t.position?.ra != null ? t.position.ra.toFixed(2) : "—"}
              </TableCell>
              <TableCell align="right">
                {t.position?.dec != null ? t.position.dec.toFixed(2) : "—"}
              </TableCell>
              <TableCell align="right">
                {t.error_radius_deg != null
                  ? t.error_radius_deg.toFixed(2)
                  : "—"}
              </TableCell>
              <TableCell align="right">
                {t.significance > 0 ? t.significance.toFixed(2) : "—"}
              </TableCell>
            </TableRow>
          ))}
          {visible.length === 0 && !loading && (
            <TableRow>
              <TableCell colSpan={7} align="center">
                <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ py: 4 }}
                >
                  No GRB triggers yet. Run <code>gw-gcn-consumer</code> to
                  start ingesting from <code>kafka.gcn.nasa.gov</code>.
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
  );
}

function BoomAlertsTable() {
  const items = useAppSelector((s) => s.externalAlerts.boomAlerts);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);

  const sorted = useMemo(() => {
    const copy = [...items];
    copy.sort((a, b) => (b.alert_time ?? 0) - (a.alert_time ?? 0));
    return copy;
  }, [items]);
  const visible = sorted.slice(
    page * rowsPerPage,
    page * rowsPerPage + rowsPerPage,
  );

  return (
    <Paper>
      <Box sx={{ p: 1.5, display: "flex", alignItems: "center", gap: 1 }}>
        <Typography variant="subtitle2">BOOM optical-transient alerts</Typography>
        <Box sx={{ flexGrow: 1 }} />
        <Typography variant="caption" color="text.secondary">
          {items.length} loaded
        </Typography>
      </Box>
      <Table size="small" stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Alert ID</TableCell>
            <TableCell>Alert time (UTC)</TableCell>
            <TableCell align="right">RA (°)</TableCell>
            <TableCell align="right">Dec (°)</TableCell>
            <TableCell>Classification</TableCell>
            <TableCell align="right">Score</TableCell>
            <TableCell>Cross-matches</TableCell>
            <TableCell>Ingested</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {visible.map((a: BoomAlertDoc) => (
            <TableRow key={a._id}>
              <TableCell>
                <code>{a.alert_id}</code>
              </TableCell>
              <TableCell>{fmtGps(a.alert_time)}</TableCell>
              <TableCell align="right">
                {a.ra != null ? a.ra.toFixed(3) : "—"}
              </TableCell>
              <TableCell align="right">
                {a.dec != null ? a.dec.toFixed(3) : "—"}
              </TableCell>
              <TableCell>{a.classification ?? "—"}</TableCell>
              <TableCell align="right">
                {a.classification_score != null
                  ? a.classification_score.toFixed(2)
                  : "—"}
              </TableCell>
              <TableCell>
                <Typography variant="caption" color="text.secondary">
                  {a.cross_match_summary ?? "—"}
                </Typography>
              </TableCell>
              <TableCell>
                <Typography variant="caption" color="text.secondary">
                  {fmtIngested(a.ingested_at)}
                </Typography>
              </TableCell>
            </TableRow>
          ))}
          {visible.length === 0 && (
            <TableRow>
              <TableCell colSpan={8} align="center">
                <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ py: 4 }}
                >
                  No BOOM alerts yet. The consumer subscribes to{" "}
                  <code>gcn.notices.boom.alert</code> by default — alerts
                  will appear here as the broker emits them.
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
  );
}

export function ExternalStreamsPage() {
  const dispatch = useAppDispatch();
  const error = useAppSelector((s) => s.externalAlerts.error);
  const [tab, setTab] = useState(0);

  useEffect(() => {
    dispatch(fetchGrbTriggers({ limit: 500 }));
    dispatch(fetchBoomAlerts({ limit: 500 }));
  }, [dispatch]);

  return (
    <Stack spacing={2}>
      <Box>
        <Typography variant="h5">External streams</Typography>
        <Typography variant="body2" color="text.secondary">
          Events ingested from upstream brokers (GCN GRB notices, BOOM
          optical-transient alerts). Cross-matches against GW superevents
          live on the per-superevent page.
        </Typography>
      </Box>
      {error && <Alert severity="error">{error}</Alert>}
      <Paper>
        <Tabs
          value={tab}
          onChange={(_, v) => setTab(v)}
          variant="scrollable"
          scrollButtons="auto"
        >
          <Tab label="GRB triggers" />
          <Tab label="BOOM alerts" />
        </Tabs>
      </Paper>
      {tab === 0 && <GrbTriggersTable />}
      {tab === 1 && <BoomAlertsTable />}
    </Stack>
  );
}
