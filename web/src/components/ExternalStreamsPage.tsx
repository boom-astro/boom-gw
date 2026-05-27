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
  Tooltip,
  Typography,
} from "@mui/material";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import { useNavigate } from "react-router-dom";
import {
  fetchBoomAlerts,
  fetchFrbAlerts,
  fetchNeutrinoAlerts,
} from "../ducks/externalAlerts";
import {
  fetchGrbTriggerSummaries,
  fetchGrbTriggerSummariesCount,
} from "../ducks/grbTriggerSummaries";
import { useAppDispatch, useAppSelector } from "../store";
import type {
  BoomAlertDoc,
  FrbAlertDoc,
  GrbTriggerDoc,
  NeutrinoAlertDoc,
} from "../types/api";

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
  // One row per `trigger_id` — Fermi-GBM emits FLT/GND/FIN/SUBTHRESH
  // updates for the same burst, and operators want them collapsed
  // into a single row with the most-refined localization. Per-stage
  // detail lives at /grb-triggers/:trigger_id.
  const navigate = useNavigate();
  const dispatch = useAppDispatch();
  const items = useAppSelector((s) => s.grbTriggerSummaries.items);
  const total = useAppSelector((s) => s.grbTriggerSummaries.total);
  const loading = useAppSelector((s) => s.grbTriggerSummaries.loading);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);

  useEffect(() => {
    dispatch(
      fetchGrbTriggerSummaries({
        limit: rowsPerPage,
        skip: page * rowsPerPage,
      }),
    );
    dispatch(fetchGrbTriggerSummariesCount());
  }, [dispatch, page, rowsPerPage]);

  return (
    <Paper>
      <Box sx={{ p: 1.5, display: "flex", alignItems: "center", gap: 1 }}>
        <Typography variant="subtitle2">GRB triggers</Typography>
        {loading && <CircularProgress size={14} />}
        <Box sx={{ flexGrow: 1 }} />
        <Typography variant="caption" color="text.secondary">
          {total != null ? `${total} total` : `${items.length} loaded`}
        </Typography>
      </Box>
      <Table size="small" stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Trigger ID</TableCell>
            <TableCell>Best stage</TableCell>
            <TableCell>Trigger time (UTC)</TableCell>
            <TableCell align="right">RA (°)</TableCell>
            <TableCell align="right">Dec (°)</TableCell>
            <TableCell align="right">Err radius (°)</TableCell>
            <TableCell align="right">Significance</TableCell>
            <TableCell align="right">Stages</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {items.map((t) => (
            <TableRow
              key={t._id}
              hover
              sx={{ cursor: "pointer" }}
              onClick={() =>
                navigate(`/grb-triggers/${encodeURIComponent(t._id)}`)
              }
            >
              <TableCell>
                <code>{t._id}</code>
              </TableCell>
              <TableCell>{t.best_instrument}</TableCell>
              <TableCell>{fmtGps(t.trigger_time)}</TableCell>
              <TableCell align="right">
                {t.ra != null ? t.ra.toFixed(2) : "—"}
              </TableCell>
              <TableCell align="right">
                {t.dec != null ? t.dec.toFixed(2) : "—"}
              </TableCell>
              <TableCell align="right">
                {t.error_radius_deg != null
                  ? t.error_radius_deg.toFixed(2)
                  : "—"}
              </TableCell>
              <TableCell align="right">
                {t.max_significance != null && t.max_significance > 0
                  ? t.max_significance.toFixed(2)
                  : "—"}
              </TableCell>
              <TableCell align="right">{t.stage_count}</TableCell>
            </TableRow>
          ))}
          {items.length === 0 && !loading && (
            <TableRow>
              <TableCell colSpan={8} align="center">
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
        count={total ?? -1}
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
            <TableCell>
              <Tooltip title="Last upper-limit before the first detection. A GW merger must lie between these two to match.">
                <span>Last non-det</span>
              </Tooltip>
            </TableCell>
            <TableCell>
              <Tooltip title="Earliest detection across this target's photometry. A GW merger must lie before this to match.">
                <span>First det</span>
              </Tooltip>
            </TableCell>
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
                  {fmtGps(a.last_non_detection_time)}
                </Typography>
              </TableCell>
              <TableCell>
                <Typography variant="caption" color="text.secondary">
                  {fmtGps(a.first_detection_time)}
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
              <TableCell colSpan={10} align="center">
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

function FrbAlertsTable() {
  const items = useAppSelector((s) => s.externalAlerts.frbAlerts);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);

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
        <Typography variant="subtitle2">FRB alerts</Typography>
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
            <TableCell align="right">SNR</TableCell>
            <TableCell align="right">DM</TableCell>
            <TableCell>Known source</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {visible.map((a: FrbAlertDoc) => (
            <TableRow key={`${a.instrument}/${a.trigger_id}`}>
              <TableCell>{a.instrument}</TableCell>
              <TableCell>
                <code>{a.trigger_id}</code>
              </TableCell>
              <TableCell>{fmtGps(a.trigger_time)}</TableCell>
              <TableCell align="right">
                {a.position?.ra != null ? a.position.ra.toFixed(2) : "—"}
              </TableCell>
              <TableCell align="right">
                {a.position?.dec != null ? a.position.dec.toFixed(2) : "—"}
              </TableCell>
              <TableCell align="right">
                {a.error_radius_deg != null
                  ? a.error_radius_deg.toFixed(3)
                  : "—"}
              </TableCell>
              <TableCell align="right">
                {a.snr != null ? a.snr.toFixed(1) : "—"}
              </TableCell>
              <TableCell align="right">
                {a.dm != null ? a.dm.toFixed(1) : "—"}
              </TableCell>
              <TableCell>
                <Typography variant="caption" color="text.secondary">
                  {a.known_source ?? "—"}
                </Typography>
              </TableCell>
            </TableRow>
          ))}
          {visible.length === 0 && (
            <TableRow>
              <TableCell colSpan={9} align="center">
                <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ py: 4 }}
                >
                  No FRB alerts yet. The consumer subscribes to{" "}
                  <code>gcn.notices.chime.frb</code> and{" "}
                  <code>gcn.notices.dsa110.frb</code> by default.
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

function NeutrinoAlertsTable() {
  const items = useAppSelector((s) => s.externalAlerts.neutrinoAlerts);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);

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
        <Typography variant="subtitle2">Neutrino alerts</Typography>
        <Box sx={{ flexGrow: 1 }} />
        <Typography variant="caption" color="text.secondary">
          {items.length} loaded
        </Typography>
      </Box>
      <Table size="small" stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Instrument</TableCell>
            <TableCell>Event</TableCell>
            <TableCell>Trigger time (UTC)</TableCell>
            <TableCell align="right">RA (°)</TableCell>
            <TableCell align="right">Dec (°)</TableCell>
            <TableCell align="right">Err radius (°)</TableCell>
            <TableCell>Pipeline</TableCell>
            <TableCell>Topology</TableCell>
            <TableCell align="right">
              <Tooltip title="IceCube p_astro (probability astrophysical) when present, otherwise KM3NeT p_value.">
                <span>p</span>
              </Tooltip>
            </TableCell>
            <TableCell align="right">ν energy (TeV)</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {visible.map((a: NeutrinoAlertDoc) => (
            <TableRow key={`${a.instrument}/${a.trigger_id}`}>
              <TableCell>{a.instrument}</TableCell>
              <TableCell>
                <code>{a.event_name ?? a.trigger_id}</code>
              </TableCell>
              <TableCell>{fmtGps(a.trigger_time)}</TableCell>
              <TableCell align="right">
                {a.position?.ra != null ? a.position.ra.toFixed(2) : "—"}
              </TableCell>
              <TableCell align="right">
                {a.position?.dec != null ? a.position.dec.toFixed(2) : "—"}
              </TableCell>
              <TableCell align="right">
                {a.error_radius_deg != null
                  ? a.error_radius_deg.toFixed(2)
                  : "—"}
              </TableCell>
              <TableCell>{a.pipeline ?? "—"}</TableCell>
              <TableCell>{a.alert_topology ?? "—"}</TableCell>
              <TableCell align="right">
                {a.p_astro != null
                  ? a.p_astro.toFixed(3)
                  : a.p_value != null
                    ? a.p_value.toFixed(3)
                    : "—"}
              </TableCell>
              <TableCell align="right">
                {a.nu_energy != null ? a.nu_energy.toFixed(1) : "—"}
              </TableCell>
            </TableRow>
          ))}
          {visible.length === 0 && (
            <TableRow>
              <TableCell colSpan={10} align="center">
                <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ py: 4 }}
                >
                  No neutrino alerts yet. The consumer subscribes to{" "}
                  <code>gcn.notices.icecube.single_neutrino_alerts</code> and{" "}
                  <code>gcn.notices.km3net.alert</code> by default.
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
    // GRB summaries fetch themselves inside GrbTriggersTable (so a
    // page change there doesn't refetch BOOM/FRB/neutrino).
    dispatch(fetchBoomAlerts({ limit: 500 }));
    dispatch(fetchFrbAlerts({ limit: 500 }));
    dispatch(fetchNeutrinoAlerts({ limit: 500 }));
  }, [dispatch]);

  return (
    <Stack spacing={2}>
      <Box>
        <Typography variant="h5">External streams</Typography>
        <Typography variant="body2" color="text.secondary">
          Events ingested from upstream brokers (GCN GRB notices, BOOM
          optical-transient alerts, CHIME / DSA110 FRBs, IceCube / KM3NeT
          neutrinos). Cross-matches against GW superevents live on the
          per-superevent page.
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
          <Tab label="FRB alerts" />
          <Tab label="Neutrino alerts" />
        </Tabs>
      </Paper>
      {tab === 0 && <GrbTriggersTable />}
      {tab === 1 && <BoomAlertsTable />}
      {tab === 2 && <FrbAlertsTable />}
      {tab === 3 && <NeutrinoAlertsTable />}
    </Stack>
  );
}
