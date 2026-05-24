// Shapes returned by the boom-gw API. Manually maintained for now;
// once the API is more stable we'll generate these from the Rust
// types via something like `typeshare` or schemars + openapi-gen.

export interface ApiEnvelope<T> {
  message: string;
  data: T;
}

export interface SkymapSummary {
  bytes_size: number;
  elapsed_ms: number;
}

export interface SupereventDoc {
  _id: string;
  t_0: number;
  t_start: number;
  t_end: number;
  preferred_graceid: string;
  preferred_snr: number;
  g_event_graceids: string[];
  skymap_summary?: SkymapSummary;
}

export interface EventDoc {
  _id: string;
  pipeline: string;
  producer_timestamp: number;
  message_type: string;
  submitter: string;
  end_time: number;
  ifos: string;
  snr: number;
  far: number;
  mchirp?: number | null;
  total_mass?: number | null;
}

export interface AnnotationDoc {
  _id: string;
  superevent_id: string;
  kind: string;
  author: string;
  payload: unknown;
  // bson datetime arrives as { $date: ... }; flatten on the wire side
  // before consuming in components.
  created_at: { $date?: { $numberLong?: string } | string } | string;
}

export interface AlertDoc {
  _id: string;
  superevent_id: string;
  alert_type: string;
  body: unknown;
  created_at: { $date?: { $numberLong?: string } | string } | string;
  published: boolean;
}

export interface LocalizeRequestDoc {
  _id: string;
  superevent_id: string;
  graceid: string;
  pipeline: string;
}

export interface LocalizeResultDoc {
  _id: string;
  superevent_id: string;
  graceid: string;
  status: "ok" | "error";
  elapsed_ms: number;
  error_message?: string | null;
  skymap_fits_bytes?: number | null;
}

export interface SkyPosition {
  ra: number;
  dec: number;
  uncertainty_arcsec: number;
}

export interface GrbTriggerDoc {
  _id: { instrument: string; trigger_id: string };
  instrument: string;
  trigger_id: string;
  trigger_time: number;
  position?: SkyPosition | null;
  significance: number;
  skymap_url?: string | null;
  error_radius_deg?: number | null;
  ingested_at:
    | string
    | { $date?: { $numberLong?: string } | string };
}

/// One ingested BOOM cross-matched optical-transient alert (GCN
/// topic `gcn.notices.boom.alert`). Holds the typed fields we
/// parse out plus the raw alert body for forward-compat with the
/// evolving GCN schema.
export interface BoomAlertDoc {
  _id: string;
  alert_id: string;
  alert_time?: number | null;
  ra?: number | null;
  dec?: number | null;
  error_radius_deg?: number | null;
  classification?: string | null;
  classification_score?: number | null;
  cross_match_summary?: string | null;
  /// The original alert body as received — kept opaque so future
  /// fields don't require code changes to surface.
  body?: unknown;
  ingested_at:
    | string
    | { $date?: { $numberLong?: string } | string };
}

export interface CrossMatchDoc {
  _id: { superevent_id: string; instrument: string; trigger_id: string };
  superevent_id: string;
  instrument: string;
  trigger_id: string;
  time_offset_sec: number;
  spatial_overlap: number;
  in_50cr: boolean;
  in_90cr: boolean;
  joint_far_per_year?: number | null;
  /** Empirical one-sided p-value from the rotation-based Monte Carlo,
   *  or null if the p-value path wasn't run for this match. */
  p_value?: number | null;
  /** Number of MC trials behind p_value. Lets the UI distinguish a
   *  tight 500-trial estimate from a coarse 20-trial one. */
  p_value_trials?: number | null;
  /** Bias-corrected joint FAR using the RAVEN remapping formula —
   *  the calibrated counterpart to joint_far_per_year. */
  joint_far_remapped_per_year?: number | null;
  /** Operator's commitment that this match is a real association.
   *  Default false; flipped via PATCH from the UI. */
  associated?: boolean;
  computed_at:
    | string
    | { $date?: { $numberLong?: string } | string };
}
