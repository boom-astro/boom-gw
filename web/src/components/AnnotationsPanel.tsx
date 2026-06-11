// Annotations side panel. Read-only in Phase 1 — the "add
// annotation" form lands in Phase 2 along with the alert-assemble
// button so we can build the POST UX once and reuse it.

import { useEffect } from "react";
import { Box, CircularProgress, Paper, Stack, Typography } from "@mui/material";
import { fetchAnnotations } from "../ducks/annotations";
import { useAppDispatch, useAppSelector } from "../store";
import type { AnnotationDoc } from "../types/api";

function fmtCreatedAt(raw: AnnotationDoc["created_at"]): string {
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

export function AnnotationsPanel({ supereventId }: Props) {
  const dispatch = useAppDispatch();
  const items = useAppSelector(
    (s) => s.annotations.bySuperevent[supereventId] ?? [],
  );
  const loading = useAppSelector((s) => s.annotations.loading);

  useEffect(() => {
    dispatch(fetchAnnotations(supereventId));
  }, [dispatch, supereventId]);

  if (loading && items.length === 0) {
    return <CircularProgress size={20} />;
  }
  if (items.length === 0) {
    return (
      <Typography variant="body2" color="text.secondary">
        No annotations yet.
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
            <Typography variant="subtitle2">
              {a.kind}{" "}
              <Typography
                component="span"
                variant="caption"
                color="text.secondary"
              >
                · {a.author}
              </Typography>
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {fmtCreatedAt(a.created_at)}
            </Typography>
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
            {JSON.stringify(a.payload, null, 2)}
          </Box>
        </Paper>
      ))}
    </Stack>
  );
}
