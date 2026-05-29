// "System Health" — anonymous-readable dashboard surfacing ingest
// freshness, localize queue depth, and recent BAYESTAR failures.
//
// All numbers come from `/api/health/dashboard` which runs only
// mongo counts — when this page shows stale data the actual signal
// is "stuff has stopped landing in mongo", which is the question
// scientists watching the page actually have.

import { useEffect } from "react";
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
  TableRow,
  Tooltip,
  Typography,
} from "@mui/material";
import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";
import utc from "dayjs/plugin/utc";
import { fetchHealthDashboard } from "../ducks/health";
import { useAppDispatch, useAppSelector } from "../store";
import type { StreamHealth } from "../types/api";

dayjs.extend(utc);
dayjs.extend(relativeTime);

// Match `fmtIngested` in ExternalStreamsPage — the API returns the
// extended-JSON `{ $date: { $numberLong } }` shape from
// mongodb-bson serde.
function extractIso(
  raw: StreamHealth["last_ingested_at"] | undefined,
): string | null {
  if (raw == null) return null;
  if (typeof raw === "string") return raw;
  const inner = raw.$date;
  if (!inner) return null;
  if (typeof inner === "string") return inner;
  const ms = inner.$numberLong ? Number(inner.$numberLong) : NaN;
  return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
}

interface StreamRowProps {
  label: string;
  stream: StreamHealth;
  /** When the last-ingest age exceeds this many seconds the chip
   *  flips to warning. Set generously for sparse streams (e.g.
   *  high-energy neutrinos) so the page doesn't cry wolf. */
  staleAfterSec: number;
}

function StreamRow({ label, stream, staleAfterSec }: StreamRowProps) {
  const iso = extractIso(stream.last_ingested_at);
  let status: "ok" | "stale" | "no-data" = "no-data";
  let detail = "no ingest timestamp recorded";
  if (iso) {
    const ageSec = (Date.now() - new Date(iso).valueOf()) / 1000;
    status = ageSec > staleAfterSec ? "stale" : "ok";
    detail = `last: ${dayjs(iso).utc().format("YYYY-MM-DD HH:mm:ss[Z]")} (${dayjs(
      iso,
    ).fromNow()})`;
  } else if (stream.total > 0) {
    // Collection has data but no per-doc timestamp field — surfaced
    // explicitly so it doesn't look like the stream is broken.
    status = "ok";
    detail = "no per-doc timestamp; using total only";
  }

  const chipColor =
    status === "ok" ? "success" : status === "stale" ? "warning" : "default";
  const chipLabel =
    status === "ok"
      ? "ingesting"
      : status === "stale"
      ? `idle ${staleAfterSec >= 3600 ? `>${staleAfterSec / 3600}h` : `>${staleAfterSec / 60}m`}`
      : "no data";

  return (
    <TableRow>
      <TableCell sx={{ fontWeight: 500 }}>{label}</TableCell>
      <TableCell>
        <Chip label={chipLabel} color={chipColor} size="small" />
      </TableCell>
      <TableCell>{stream.total.toLocaleString()}</TableCell>
      <TableCell>
        {stream.count_1h != null ? stream.count_1h.toLocaleString() : "—"}
      </TableCell>
      <TableCell>
        <Tooltip title={detail}>
          <Typography
            variant="body2"
            sx={{ color: "text.secondary", cursor: "help" }}
          >
            {iso ? dayjs(iso).fromNow() : "—"}
          </Typography>
        </Tooltip>
      </TableCell>
    </TableRow>
  );
}

const POLL_INTERVAL_MS = 10_000;

export function SystemHealthPage() {
  const dispatch = useAppDispatch();
  const { data, loading, error, lastFetchedAt } = useAppSelector(
    (s) => s.health,
  );

  useEffect(() => {
    dispatch(fetchHealthDashboard());
    const id = window.setInterval(() => {
      dispatch(fetchHealthDashboard());
    }, POLL_INTERVAL_MS);
    return () => window.clearInterval(id);
  }, [dispatch]);

  if (loading && !data) {
    return (
      <Box sx={{ display: "flex", justifyContent: "center", py: 6 }}>
        <CircularProgress />
      </Box>
    );
  }
  if (error && !data) {
    return (
      <Alert severity="error" sx={{ mt: 2 }}>
        {error}
      </Alert>
    );
  }
  if (!data) return null;

  const errorRate =
    data.localize.total_results > 0
      ? (
          (100 * data.localize.total_errors) /
          data.localize.total_results
        ).toFixed(1) + "%"
      : "—";

  // Submitted = anything that got past the gate, whether already
  // completed or still queued for bayestar. Skipped = the gate
  // dropped it. Denominator is everything the clusterer saw
  // post-gating, so 100% submitted means the gate isn't filtering.
  const submitted = data.localize.total_results + data.localize.pending;
  const considered = submitted + data.localize.total_skipped;
  const submittedPct =
    considered > 0 ? ((100 * submitted) / considered).toFixed(1) + "%" : "—";

  return (
    <Stack spacing={3}>
      <Box>
        <Typography variant="h5">System Health</Typography>
        <Typography variant="body2" sx={{ color: "text.secondary" }}>
          Auto-refreshing every {POLL_INTERVAL_MS / 1000}s. Last updated{" "}
          {lastFetchedAt ? dayjs(lastFetchedAt).fromNow() : "—"}.
        </Typography>
      </Box>

      <Paper variant="outlined">
        <Box sx={{ p: 2 }}>
          <Typography variant="h6">Ingest streams</Typography>
        </Box>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Stream</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Total</TableCell>
              <TableCell>Last 1h</TableCell>
              <TableCell>Last seen</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            <StreamRow
              label="GraceDB (GW)"
              stream={data.streams.gracedb_gw}
              staleAfterSec={data.config.stream_stale_sec.gracedb_gw}
            />
            <StreamRow
              label="GCN — GRB"
              stream={data.streams.gcn_grb}
              staleAfterSec={data.config.stream_stale_sec.gcn_grb}
            />
            <StreamRow
              label="GCN — FRB"
              stream={data.streams.gcn_frb}
              staleAfterSec={data.config.stream_stale_sec.gcn_frb}
            />
            <StreamRow
              label="GCN — Neutrino"
              stream={data.streams.gcn_neutrino}
              staleAfterSec={data.config.stream_stale_sec.gcn_neutrino}
            />
            <StreamRow
              label="BOOM alerts"
              stream={data.streams.gcn_boom}
              staleAfterSec={data.config.stream_stale_sec.gcn_boom}
            />
          </TableBody>
        </Table>
      </Paper>

      <Paper variant="outlined" sx={{ p: 2 }}>
        <Typography variant="h6" sx={{ mb: 2 }}>
          Localize queue
        </Typography>
        <Stack
          direction={{ xs: "column", sm: "row" }}
          spacing={3}
          divider={<Box sx={{ display: { xs: "none", sm: "block" } }} />}
        >
          <Box sx={{ flex: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Pending
            </Typography>
            <Typography variant="h4">
              {data.localize.pending.toLocaleString()}
            </Typography>
          </Box>
          <Box sx={{ flex: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Total completed
            </Typography>
            <Typography variant="h4">
              {data.localize.total_results.toLocaleString()}
            </Typography>
          </Box>
          <Box sx={{ flex: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Total errors
            </Typography>
            <Typography variant="h4">
              {data.localize.total_errors.toLocaleString()}
            </Typography>
          </Box>
          <Box sx={{ flex: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Error rate
            </Typography>
            <Typography variant="h4">{errorRate}</Typography>
          </Box>
        </Stack>
        <Stack
          direction={{ xs: "column", sm: "row" }}
          spacing={3}
          sx={{ mt: 3, pt: 2, borderTop: 1, borderColor: "divider" }}
        >
          <Box sx={{ flex: 1 }}>
            <Typography variant="caption" color="text.secondary">
              Skipped by gate
            </Typography>
            <Typography variant="h4">
              {data.localize.total_skipped.toLocaleString()}
            </Typography>
          </Box>
          <Box sx={{ flex: 1 }}>
            <Tooltip title="g-events submitted to BAYESTAR / (submitted + gate-skipped). 100% means the SNR/FAR gate isn't filtering any events.">
              <Typography
                variant="caption"
                color="text.secondary"
                sx={{ cursor: "help" }}
              >
                Submitted vs. considered
              </Typography>
            </Tooltip>
            <Typography variant="h4">{submittedPct}</Typography>
          </Box>
          <Box sx={{ flex: 2 }} />
        </Stack>
      </Paper>

      <Paper variant="outlined">
        <Box sx={{ p: 2 }}>
          <Typography variant="h6">Recent BAYESTAR errors</Typography>
        </Box>
        {data.recent_errors.length === 0 ? (
          <Box sx={{ px: 2, pb: 2 }}>
            <Typography variant="body2" color="text.secondary">
              No recent localization errors.
            </Typography>
          </Box>
        ) : (
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Request</TableCell>
                <TableCell>Superevent</TableCell>
                <TableCell>Elapsed</TableCell>
                <TableCell>Error</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {data.recent_errors.map((e) => (
                <TableRow key={e.request_id}>
                  <TableCell sx={{ fontFamily: "monospace" }}>
                    {e.request_id}
                  </TableCell>
                  <TableCell>{e.superevent_id}</TableCell>
                  <TableCell>{(e.elapsed_ms / 1000).toFixed(1)}s</TableCell>
                  <TableCell
                    sx={{
                      maxWidth: 480,
                      whiteSpace: "nowrap",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                    }}
                  >
                    <Tooltip title={e.error_message ?? ""}>
                      <span>{e.error_message ?? "—"}</span>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </Paper>
    </Stack>
  );
}
