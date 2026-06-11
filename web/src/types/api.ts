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
  /** Representative position for the localization, degrees —
   *  computed server-side as the sphere-average of the 50%
   *  credible region's cell centers. Used as the Aladin viewer's
   *  initial center so the operator doesn't have to pan. */
  center_ra?: number | null;
  center_dec?: number | null;
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

/// One row per `trigger_id` from `/api/grb-trigger-summaries`. The
/// `best_*` fields come from the highest-priority stage Fermi-GBM
/// has emitted so far (FIN > GND > FLT > SUBTHRESH); `stages` is
/// the full refinement chain used by the drill-down page.
export interface GrbTriggerStage {
  instrument: string;
  trigger_time: number;
  position?: SkyPosition | null;
  error_radius_deg?: number | null;
  significance?: number | null;
  ingested_at?: string | { $date?: { $numberLong?: string } | string } | null;
}

export interface GrbTriggerSummary {
  /** `_id` from the aggregation is the trigger_id directly. */
  _id: string;
  best_instrument: string;
  ra?: number | null;
  dec?: number | null;
  error_radius_deg?: number | null;
  trigger_time: number;
  max_significance?: number | null;
  stage_count: number;
  stages: GrbTriggerStage[];
  latest_ingest?: string | { $date?: { $numberLong?: string } | string } | null;
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
  /** Per-trigger false-alarm rate in Hz (IceCube/KM3NeT `far`,
   *  Fermi-GBM subthreshold `far`, etc.). When present + the
   *  instrument is a RAVEN-supported targeted-search pipeline
   *  (Fermi-GBM, Swift-BAT), the cross-match populates
   *  `CrossMatchDoc.targeted_joint_far_per_year`. */
  far_hz?: number | null;
  ingested_at: string | { $date?: { $numberLong?: string } | string };
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
  /** GPS time of the earliest detection across this target's
   *  photometry rows (filter-agnostic). `null` if the alert
   *  carries only upper limits. */
  first_detection_time?: number | null;
  /** GPS time of the latest non-detection strictly before
   *  `first_detection_time`. Together they bracket the window in
   *  which the transient turned on — the right time-match
   *  criterion for kilonova searches. */
  last_non_detection_time?: number | null;
  /// The original alert body as received — kept opaque so future
  /// fields don't require code changes to surface.
  body?: unknown;
  ingested_at: string | { $date?: { $numberLong?: string } | string };
}

/// One CHIME or DSA110 FRB alert. The cross-match-relevant fields
/// (`trigger_id`, `instrument`, `trigger_time`, `position`,
/// `error_radius_deg`) are flattened from the backend `trigger`
/// struct, matching the `#[serde(flatten)]` projection used on
/// `FrbAlertDoc` in the Rust side.
export interface FrbAlertDoc {
  _id: { instrument: string; trigger_id: string };
  instrument: string;
  trigger_id: string;
  trigger_time: number;
  position?: SkyPosition | null;
  significance: number;
  error_radius_deg?: number | null;
  /** Dispersion measure in pc/cm^3. Distinguishes Galactic
   *  pulsars (low DM) from extragalactic FRBs (high DM). */
  dm?: number | null;
  dm_error?: number | null;
  importance?: number | null;
  snr?: number | null;
  known_source?: string | null;
  ingested_at: string | { $date?: { $numberLong?: string } | string };
}

/// One IceCube single-neutrino or KM3NeT neutrino alert. Same
/// flatten convention as `FrbAlertDoc`.
export interface NeutrinoAlertDoc {
  _id: { instrument: string; trigger_id: string };
  instrument: string;
  trigger_id: string;
  trigger_time: number;
  position?: SkyPosition | null;
  significance: number;
  error_radius_deg?: number | null;
  alert_topology?: string | null;
  pipeline?: string | null;
  nu_energy?: number | null;
  /** IceCube-only: probability the event is astrophysical. */
  p_astro?: number | null;
  /** KM3NeT-only: occurrence probability of the alert. */
  p_value?: number | null;
  far?: number | null;
  healpix_url?: string | null;
  event_name?: string | null;
  ingested_at: string | { $date?: { $numberLong?: string } | string };
}

/// One coincident IceCube track event reported inside a
/// `IceCubeLvkSearchDoc`. Mirrors the Rust `CoincidentTrackEvent`.
export interface CoincidentTrackEvent {
  id: string;
  event_dt: number;
  localization?: SkyPosition | null;
  event_pval_generic?: number | null;
  event_pval_bayesian?: number | null;
}

/// One IceCube LVK Nu Track Search result attached to a specific
/// superevent. The composite `_id` is `(superevent_id, alert_time)`.
export interface IceCubeLvkSearchDoc {
  _id: { superevent_id: string; alert_time_gps: string };
  superevent_id: string;
  alert_time: number;
  trigger_time: number;
  observation_start?: number | null;
  observation_stop?: number | null;
  observation_livetime?: number | null;
  pval_generic?: number | null;
  pval_bayesian?: number | null;
  n_events_coincident: number;
  coincident_events: CoincidentTrackEvent[];
  most_probable_direction?: SkyPosition | null;
  flux_sensitivity_range?: [number, number] | null;
  sensitive_energy_range?: [number, number] | null;
  ingested_at: string | { $date?: { $numberLong?: string } | string };
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
  /** RAVEN-targeted joint FAR (SubGRBTargeted regime). Populated
   *  when the trigger reports a per-trigger far_hz AND the
   *  instrument is Fermi-GBM* or Swift-BAT. Falls back to null
   *  otherwise; callers should display `joint_far_per_year` then. */
  targeted_joint_far_per_year?: number | null;
  /** Operator's commitment that this match is a real association.
   *  Default false; flipped via PATCH from the UI. */
  associated?: boolean;
  computed_at: string | { $date?: { $numberLong?: string } | string };
}

// --- Access control (users, roles, groups, streams) ------------------------

export interface StreamRef {
  id: string;
  name: string;
}

/// A group as seen from the current user's profile: its name, whether
/// the user administers it, and the streams it can access.
export interface GroupRef {
  id: string;
  name: string;
  admin: boolean;
  streams?: StreamRef[];
}

/// Enriched current-user profile from `GET /api/users/me` — the SPA's
/// source of truth for identity + authorization. Arrays may be omitted
/// by the API when empty; always guard with `?? []`.
export interface Me {
  sub: string;
  email?: string | null;
  display_name?: string | null;
  iss?: string;
  scopes?: string[];
  acls?: string[];
  roles?: string[];
  groups?: GroupRef[];
  streams?: StreamRef[];
}

export interface UserDoc {
  sub: string;
  display_name?: string | null;
  email?: string | null;
  roles?: string[];
}

export interface RoleDoc {
  _id: string;
  name: string;
  acls?: string[];
  description?: string;
  system?: boolean;
}

export interface GroupMember {
  sub: string;
  admin: boolean;
  display_name?: string | null;
  email?: string | null;
}

export interface GroupDoc {
  id: string;
  name: string;
  description?: string;
  /** Whether the *current user* administers this group (set on list). */
  admin?: boolean;
  members?: GroupMember[];
  streams?: StreamRef[];
  created_at?: string | { $date?: { $numberLong?: string } | string };
}

export interface StreamDoc {
  _id: string;
  name: string;
  description?: string;
  system?: boolean;
}

/// Threshold cuts a science filter applies to the objective
/// cross-match metrics. Every field is optional — an unset cut is
/// not applied. Mirrors the Rust `FilterCuts`.
export interface FilterCuts {
  /** Restrict to these instruments (matched against
   *  `CrossMatchDoc.instrument`). Empty → any instrument. */
  instruments?: string[];
  /** Keep matches with `|time_offset_sec| <= time_window_sec`. */
  time_window_sec?: number | null;
  spatial_overlap_min?: number | null;
  p_value_max?: number | null;
  joint_far_remapped_max_per_year?: number | null;
  require_in_90cr?: boolean | null;
}

/// A named confidence tier. A match is tagged with the tightest
/// tier (smallest threshold) whose bound it clears.
export interface ConfidenceTier {
  name: string;
  joint_far_remapped_max_per_year: number;
}

/// A saved, per-user science filter. The objective metrics live on
/// `CrossMatchDoc`; a filter re-thresholds them at query time, so two
/// users can surface different associations at different confidence.
export interface ScienceFilterDoc {
  _id: string;
  owner: string;
  /** Group this filter is shared with (FK to GroupDoc.id). `null`/
   *  absent → private to the owner. */
  group_id?: string | null;
  /** Messenger streams the filter draws from (FK to StreamDoc.id);
   *  subset of the group's streams. Omitted when empty. */
  stream_ids?: string[];
  name: string;
  /** Reserved for the future stream-time alerting path. */
  active?: boolean;
  cuts: FilterCuts;
  /** Stored most-significant first (ascending threshold). Omitted by
   *  the API when empty (`skip_serializing_if`), so treat as optional. */
  confidence_tiers?: ConfidenceTier[];
  created_at?: string | { $date?: { $numberLong?: string } | string };
  updated_at?: string | { $date?: { $numberLong?: string } | string };
}

/// One cross-match seen through a science filter: the objective
/// document plus the confidence tier it earned under that filter.
export type FilteredCrossMatch = CrossMatchDoc & {
  confidence_tier?: string | null;
};

export interface StreamHealth {
  total: number;
  last_ingested_at:
    | string
    | { $date?: { $numberLong?: string } | string }
    | null;
  count_1h: number | null;
}

export interface RecentLocalizeError {
  request_id: string;
  superevent_id: string;
  graceid: string;
  error_message: string | null;
  elapsed_ms: number;
}

export interface StreamStaleConfig {
  gracedb_gw: number;
  gcn_grb: number;
  gcn_frb: number;
  gcn_neutrino: number;
  gcn_boom: number;
}

export interface HealthConfig {
  _id: string;
  stream_stale_sec: StreamStaleConfig;
}

export interface HealthDashboard {
  generated_at: string | { $date?: { $numberLong?: string } | string };
  streams: {
    gracedb_gw: StreamHealth;
    gcn_grb: StreamHealth;
    gcn_frb: StreamHealth;
    gcn_neutrino: StreamHealth;
    gcn_boom: StreamHealth;
  };
  localize: {
    pending: number;
    total_results: number;
    total_errors: number;
    /** g-events the clusterer's SNR/FAR gate dropped before
     *  publishing to bayestar. Paired with `total_results + pending`
     *  to compute the gate's submitted-vs-skipped ratio. */
    total_skipped: number;
  };
  recent_errors: RecentLocalizeError[];
  config: HealthConfig;
}
