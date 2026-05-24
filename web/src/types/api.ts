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
  computed_at:
    | string
    | { $date?: { $numberLong?: string } | string };
}
