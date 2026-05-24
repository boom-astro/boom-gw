// Outgoing GCN alerts list. Phase 1 is view-only; "Assemble &
// publish" lives in Phase 2 once we wire the allowlist surface so
// non-publishers see a friendly disabled state instead of a 403.

import { useEffect } from "react";
import {
  Box,
  Chip,
  CircularProgress,
  Paper,
  Stack,
  Typography,
} from "@mui/material";
import { fetchAlerts } from "../ducks/alerts";
import { useAppDispatch, useAppSelector } from "../store";
import type { AlertDoc } from "../types/api";

function fmtCreatedAt(raw: AlertDoc["created_at"]): string {
  if (typeof raw === "string") return raw;
  const inner = raw?.$date;
  if (!inner) return "";
  if (typeof inner === "string") return inner;
  const ms = inner.$numberLong ? Number(inner.$numberLong) : 0;
  return new Date(ms).toISOString();
}

interface Props {
  supereventId: string;
}

export function AlertsPanel({ supereventId }: Props) {
  const dispatch = useAppDispatch();
  const items = useAppSelector(
    (s) => s.alerts.bySuperevent[supereventId] ?? [],
  );
  const loading = useAppSelector((s) => s.alerts.loading);

  useEffect(() => {
    dispatch(fetchAlerts(supereventId));
  }, [dispatch, supereventId]);

  if (loading && items.length === 0) {
    return <CircularProgress size={20} />;
  }
  if (items.length === 0) {
    return (
      <Typography variant="body2" color="text.secondary">
        No alerts assembled for this superevent yet.
      </Typography>
    );
  }
  return (
    <Stack spacing={1}>
      {items.map((a) => (
        <Paper key={a._id} sx={{ p: 2 }}>
          <Box
            sx={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "baseline",
              mb: 1,
            }}
          >
            <Typography variant="subtitle2">{a.alert_type}</Typography>
            <Stack direction="row" spacing={1} alignItems="center">
              <Chip
                size="small"
                color={a.published ? "success" : "default"}
                label={a.published ? "Published" : "Draft"}
              />
              <Typography variant="caption" color="text.secondary">
                {fmtCreatedAt(a.created_at)}
              </Typography>
            </Stack>
          </Box>
          <Box
            component="pre"
            sx={{
              m: 0,
              fontSize: 12,
              maxHeight: 200,
              overflow: "auto",
              bgcolor: "rgba(255,255,255,0.04)",
              p: 1,
              borderRadius: 1,
            }}
          >
            {JSON.stringify(a.body, null, 2)}
          </Box>
        </Paper>
      ))}
    </Stack>
  );
}
