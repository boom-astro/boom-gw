//! Superevent clustering.
//!
//! Reproduces the semantics of `sgn-llai`'s `SupereventCreator` so the
//! BOOM-based path can be compared to the SGN backing stack on the same
//! input streams. The two policy choices that matter for parity are:
//!
//! * **Time window.** A superevent owns a window of `window_duration`
//!   seconds centred on the GPS time of the event that first opened it.
//!   Incoming events match the superevent whose centre `t_0` is closest to
//!   the incoming event's time, provided the absolute distance is no more
//!   than `window_duration / 2`. The default of 5.0 seconds matches sgn-llai.
//! * **Preferred event.** Within a superevent, the preferred G event is the
//!   one with the highest network SNR. Events that arrive into an existing
//!   superevent with a lower SNR than the current preferred are skipped
//!   (they are recorded in the superevent's `g_events` list but do not
//!   replace the preferred slot). This matches sgn-llai's
//!   `_process_event` logic.
//!
//! The clustering layer is pure: it takes a stream of `GwEvent`s and
//! returns a stream of `SupereventUpdate`s. State is held in memory.

use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::event::GwEvent;

/// Default time window in seconds. Matches `sgn-llai`'s
/// `--window-duration` default of 5.0 (a ±2.5 s window around `t_0`).
pub const DEFAULT_WINDOW_SECS: f64 = 5.0;

/// A logical superevent built from one or more G events.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Superevent {
    pub id: String,
    /// GPS time that opened the window (the t_0 of the first event in).
    pub t_0: f64,
    pub t_start: f64,
    pub t_end: f64,
    pub preferred_event: GwEvent,
    pub g_events: Vec<GwEvent>,
}

/// One emission from [`SupereventCreator::process`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum SupereventUpdate {
    /// A new superevent was opened by this event.
    Created {
        superevent: Superevent,
    },
    /// The arriving event had a higher SNR than the current preferred
    /// event of the superevent it landed in; the preferred slot was
    /// replaced.
    PreferredUpdated {
        superevent: Superevent,
        previous_preferred_graceid: String,
    },
    /// The arriving event matched an existing superevent but its SNR was
    /// below the current preferred; it was recorded but not promoted.
    Skipped {
        superevent_id: String,
        event: GwEvent,
        reason: SkipReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    LowerSnr,
}

/// Stateful clustering engine. One per pipeline-group; typically one per
/// process, since incoming events from all pipelines should cluster
/// together to form a single timeline of superevents.
pub struct SupereventCreator {
    window_duration: f64,
    /// All open superevents, keyed by `t_0`. The key is the same one used
    /// in the temporal index below.
    superevents: BTreeMap<OrderedTime, Superevent>,
    /// `t_0` keys held in sorted order so we can bisect to the nearest
    /// candidate window in `O(log n)`. Mirrors sgn-llai's bisect approach.
    times: BTreeMap<OrderedTime, String>,
    next_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
struct OrderedTime(f64);

impl Eq for OrderedTime {}

impl Ord for OrderedTime {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // NaN is treated as greater than everything; should not appear in
        // GPS times.
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl SupereventCreator {
    pub fn new(window_duration: f64) -> Self {
        Self {
            window_duration,
            superevents: BTreeMap::new(),
            times: BTreeMap::new(),
            next_seq: 0,
        }
    }

    pub fn with_default_window() -> Self {
        Self::new(DEFAULT_WINDOW_SECS)
    }

    /// Rebuild a creator from a previously persisted set of open
    /// superevents. The `next_seq` argument is the next id sequence
    /// number to allocate — recovered from the stored ids by the
    /// caller. Used by [`crate::state::load_from_redis`].
    pub fn from_persisted(
        window_duration: f64,
        superevents: Vec<Superevent>,
        next_seq: u64,
    ) -> Self {
        let mut creator = Self::new(window_duration);
        for s in superevents {
            let key = OrderedTime(s.t_0);
            creator.times.insert(key, s.id.clone());
            creator.superevents.insert(key, s);
        }
        creator.next_seq = next_seq;
        creator
    }

    /// Number of open superevents currently held.
    pub fn len(&self) -> usize {
        self.superevents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.superevents.is_empty()
    }

    /// Iterate over all open superevents.
    pub fn superevents(&self) -> impl Iterator<Item = &Superevent> {
        self.superevents.values()
    }

    /// Drop superevents whose window ends before `cutoff_time`. Returns the
    /// dropped superevents in arrival order.
    pub fn cleanup_before(&mut self, cutoff_time: f64) -> Vec<Superevent> {
        let to_remove: Vec<OrderedTime> = self
            .superevents
            .iter()
            .filter_map(|(k, v)| (v.t_end < cutoff_time).then_some(*k))
            .collect();
        let mut removed = Vec::new();
        for key in to_remove {
            self.times.remove(&key);
            if let Some(s) = self.superevents.remove(&key) {
                removed.push(s);
            }
        }
        removed
    }

    /// Ingest one event and return the resulting superevent update.
    pub fn process(&mut self, event: GwEvent) -> SupereventUpdate {
        let gpstime = event.end_time;
        match self.find_closest_t0(gpstime) {
            Some(existing_t0) => {
                let superevent = self
                    .superevents
                    .get_mut(&OrderedTime(existing_t0))
                    .expect("temporal index pointed at a missing superevent");
                if event.snr > superevent.preferred_event.snr {
                    let previous_preferred_graceid =
                        superevent.preferred_event.graceid.clone();
                    superevent.preferred_event = event.clone();
                    superevent.g_events.push(event);
                    SupereventUpdate::PreferredUpdated {
                        superevent: superevent.clone(),
                        previous_preferred_graceid,
                    }
                } else {
                    let superevent_id = superevent.id.clone();
                    superevent.g_events.push(event.clone());
                    SupereventUpdate::Skipped {
                        superevent_id,
                        event,
                        reason: SkipReason::LowerSnr,
                    }
                }
            }
            None => {
                let half = self.window_duration / 2.0;
                let id = self.allocate_id();
                let t_0 = gpstime;
                let superevent = Superevent {
                    id: id.clone(),
                    t_0,
                    t_start: t_0 - half,
                    t_end: t_0 + half,
                    preferred_event: event.clone(),
                    g_events: vec![event],
                };
                self.times.insert(OrderedTime(t_0), id.clone());
                self.superevents
                    .insert(OrderedTime(t_0), superevent.clone());
                SupereventUpdate::Created { superevent }
            }
        }
    }

    /// Mirror sgn-llai's `_find_superevent`: bisect to find the two
    /// candidate `t_0` values surrounding `gpstime`, keep the ones within
    /// `±window_duration/2`, return the closest. None if no candidate
    /// window contains `gpstime`.
    fn find_closest_t0(&self, gpstime: f64) -> Option<f64> {
        if self.times.is_empty() {
            return None;
        }
        let key = OrderedTime(gpstime);
        let before = self.times.range(..key).next_back().map(|(k, _)| k.0);
        let after = self.times.range(key..).next().map(|(k, _)| k.0);
        let half = self.window_duration / 2.0;
        let mut candidates: Vec<f64> = Vec::with_capacity(2);
        for t in [before, after].into_iter().flatten() {
            if (gpstime - t).abs() <= half {
                candidates.push(t);
            }
        }
        candidates
            .into_iter()
            .min_by(|a, b| {
                (gpstime - a)
                    .abs()
                    .partial_cmp(&(gpstime - b).abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }

    fn allocate_id(&mut self) -> String {
        let id = format!("S{:06}", self.next_seq);
        self.next_seq += 1;
        id
    }
}

impl Default for SupereventCreator {
    fn default() -> Self {
        Self::with_default_window()
    }
}

/// Summary record suitable for printing a side-by-side comparison against
/// any other clustering implementation. Keys are graceid; values are the
/// superevent assignment and whether the event was the preferred one.
pub fn summarize(creator: &SupereventCreator) -> HashMap<String, EventAssignment> {
    let mut out = HashMap::new();
    for s in creator.superevents() {
        for ev in &s.g_events {
            let assignment = EventAssignment {
                superevent_id: s.id.clone(),
                is_preferred: ev.graceid == s.preferred_event.graceid,
            };
            out.insert(ev.graceid.clone(), assignment);
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventAssignment {
    pub superevent_id: String,
    pub is_preferred: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ligo_lw::CoincInspiralEvent;

    fn make_event(graceid: &str, end_time: f64, snr: f64) -> GwEvent {
        let coinc = CoincInspiralEvent {
            coinc_event_id: graceid.to_string(),
            ifos: "H1,L1".to_string(),
            combined_far: 1e-9,
            snr,
            mass: None,
            mchirp: None,
            end_time,
            sngls: vec![],
        };
        GwEvent {
            pipeline: "test".to_string(),
            graceid: graceid.to_string(),
            producer_timestamp: 0.0,
            message_type: "new".to_string(),
            submitter: "ci".to_string(),
            end_time,
            ifos: "H1,L1".to_string(),
            snr,
            far: 1e-9,
            mchirp: None,
            total_mass: None,
            coinc,
        }
    }

    #[test]
    fn opens_new_superevent_when_no_match() {
        let mut sc = SupereventCreator::with_default_window();
        let update = sc.process(make_event("G1", 1000.0, 10.0));
        match update {
            SupereventUpdate::Created { superevent } => {
                assert_eq!(superevent.preferred_event.graceid, "G1");
                assert!((superevent.t_0 - 1000.0).abs() < 1e-9);
                assert!((superevent.t_start - 997.5).abs() < 1e-9);
                assert!((superevent.t_end - 1002.5).abs() < 1e-9);
            }
            other => panic!("expected Created, got {other:?}"),
        }
    }

    #[test]
    fn within_window_replaces_preferred_when_snr_higher() {
        let mut sc = SupereventCreator::with_default_window();
        sc.process(make_event("G1", 1000.0, 8.0));
        let update = sc.process(make_event("G2", 1000.5, 10.0));
        match update {
            SupereventUpdate::PreferredUpdated {
                superevent,
                previous_preferred_graceid,
            } => {
                assert_eq!(superevent.preferred_event.graceid, "G2");
                assert_eq!(previous_preferred_graceid, "G1");
                assert_eq!(superevent.g_events.len(), 2);
            }
            other => panic!("expected PreferredUpdated, got {other:?}"),
        }
    }

    #[test]
    fn within_window_lower_snr_is_skipped() {
        let mut sc = SupereventCreator::with_default_window();
        sc.process(make_event("G1", 1000.0, 10.0));
        let update = sc.process(make_event("G2", 1001.0, 8.0));
        match update {
            SupereventUpdate::Skipped { reason, .. } => {
                assert_eq!(reason, SkipReason::LowerSnr);
            }
            other => panic!("expected Skipped, got {other:?}"),
        }
        // The skipped event is still recorded in g_events for traceability.
        let s = sc.superevents().next().unwrap();
        assert_eq!(s.g_events.len(), 2);
        assert_eq!(s.preferred_event.graceid, "G1");
    }

    #[test]
    fn outside_window_opens_separate_superevent() {
        let mut sc = SupereventCreator::with_default_window();
        sc.process(make_event("G1", 1000.0, 10.0));
        let update = sc.process(make_event("G2", 1003.0, 12.0));
        match update {
            SupereventUpdate::Created { .. } => {}
            other => panic!("expected Created (outside window), got {other:?}"),
        }
        assert_eq!(sc.len(), 2);
    }

    #[test]
    fn chooses_closest_t0_when_two_candidates_overlap() {
        let mut sc = SupereventCreator::new(5.0); // half-window = 2.5
        sc.process(make_event("G1", 1000.0, 10.0));
        sc.process(make_event("G2", 1004.0, 10.0)); // Outside window of G1 → new
        assert_eq!(sc.len(), 2);
        // An event at 1002.0 is within ±2.5 of both 1000.0 and 1004.0;
        // closest is 1000.0.
        let update = sc.process(make_event("G3", 1002.0, 9.0));
        match update {
            SupereventUpdate::Skipped { superevent_id, .. } => {
                // Should land in S0 (the one centred at 1000.0), not S1.
                assert_eq!(superevent_id, "S000000");
            }
            other => panic!("expected Skipped onto S0, got {other:?}"),
        }
    }
}
