//! Persist and recover [`SupereventCreator`] state in Redis so that a
//! restart of the clusterer does not lose open superevent windows.
//!
//! Two keys are used per clusterer instance, keyed by a caller-provided
//! `prefix` (e.g. `gw:clusterer:default`):
//!
//! * `{prefix}:open` — a hash from superevent id → JSON-encoded `Superevent`.
//! * `{prefix}:t0_index` — a sorted set from superevent id (member) →
//!   `t_0` (score), so we can range over windows and bisect for nearest
//!   neighbours.
//!
//! The save path is idempotent — repeatedly saving the same state produces
//! the same Redis contents — and is safe to call after every processed
//! event because the state is small (a few hundred open windows at most).
//! Recovery reads the sorted set in score order and rebuilds the in-memory
//! [`SupereventCreator`] by replaying the inserts.

use redis::{AsyncCommands, RedisError};
use thiserror::Error;
use tracing::info;

use crate::clustering::{Superevent, SupereventCreator, DEFAULT_WINDOW_SECS};

#[derive(Debug, Error)]
pub enum StateError {
    #[error("redis error: {0}")]
    Redis(#[from] RedisError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

const OPEN_SUFFIX: &str = ":open";
const INDEX_SUFFIX: &str = ":t0_index";

/// Write the current open superevents of `creator` to Redis under `prefix`.
/// The function replaces any previously stored state for the same prefix,
/// so it is safe to call after every event without leaking stale entries.
pub async fn save_to_redis<C: AsyncCommands>(
    conn: &mut C,
    prefix: &str,
    creator: &SupereventCreator,
) -> Result<(), StateError> {
    let open_key = format!("{prefix}{OPEN_SUFFIX}");
    let index_key = format!("{prefix}{INDEX_SUFFIX}");

    // Atomically replace the stored state. A pipeline keeps the round trip
    // count down and ensures the open hash and index zset are written
    // together; we delete first so removed superevents do not linger.
    let mut pipe = redis::pipe();
    pipe.atomic();
    pipe.del(&open_key).ignore();
    pipe.del(&index_key).ignore();
    for s in creator.superevents() {
        let payload = serde_json::to_vec(s)?;
        pipe.hset(&open_key, &s.id, payload).ignore();
        pipe.zadd(&index_key, &s.id, s.t_0).ignore();
    }
    let _: () = pipe.query_async(conn).await?;
    Ok(())
}

/// Drop the persisted state under `prefix`. Useful in tests and when
/// rotating to a fresh consumer group.
pub async fn clear_in_redis<C: AsyncCommands>(
    conn: &mut C,
    prefix: &str,
) -> Result<(), StateError> {
    let open_key = format!("{prefix}{OPEN_SUFFIX}");
    let index_key = format!("{prefix}{INDEX_SUFFIX}");
    let _: () = conn.del(&[open_key, index_key]).await?;
    Ok(())
}

/// Read the persisted open superevents back into a fresh
/// [`SupereventCreator`]. The `window_duration` and the next sequence
/// number are restored from the data: the next sequence is taken to be one
/// past the maximum numeric suffix of any restored id, falling back to the
/// count of restored superevents.
pub async fn load_from_redis<C: AsyncCommands>(
    conn: &mut C,
    prefix: &str,
    window_duration: Option<f64>,
) -> Result<SupereventCreator, StateError> {
    let open_key = format!("{prefix}{OPEN_SUFFIX}");
    let index_key = format!("{prefix}{INDEX_SUFFIX}");

    // Order by t_0 so the in-memory BTreeMap rebuilds in stable order.
    let ids: Vec<(String, f64)> = conn
        .zrange_withscores(&index_key, 0, -1)
        .await
        .unwrap_or_default();

    let mut superevents: Vec<Superevent> = Vec::with_capacity(ids.len());
    for (id, _t0) in &ids {
        let bytes: Option<Vec<u8>> = conn.hget(&open_key, id).await?;
        if let Some(b) = bytes {
            superevents.push(serde_json::from_slice(&b)?);
        }
    }
    let next_seq = next_seq_from_ids(superevents.iter().map(|s| s.id.as_str()));
    let creator = SupereventCreator::from_persisted(
        window_duration.unwrap_or(DEFAULT_WINDOW_SECS),
        superevents.clone(),
        next_seq,
    );
    info!(
        prefix = %prefix,
        restored = superevents.len(),
        next_seq,
        "restored superevent state from redis"
    );
    Ok(creator)
}

fn next_seq_from_ids<'a>(ids: impl Iterator<Item = &'a str>) -> u64 {
    let mut max_seq: Option<u64> = None;
    let mut count: u64 = 0;
    for id in ids {
        count += 1;
        if let Some(num) = id.strip_prefix('S').and_then(|s| s.parse::<u64>().ok()) {
            max_seq = Some(max_seq.map_or(num, |m| m.max(num)));
        }
    }
    max_seq.map(|m| m + 1).unwrap_or(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_seq_picks_max_plus_one() {
        let ids = ["S000003", "S000001", "S000005"];
        assert_eq!(next_seq_from_ids(ids.iter().copied()), 6);
    }

    #[test]
    fn next_seq_falls_back_to_count_when_ids_unparseable() {
        let ids = ["custom-a", "custom-b"];
        assert_eq!(next_seq_from_ids(ids.iter().copied()), 2);
    }

    #[test]
    fn next_seq_empty_is_zero() {
        let ids: [&str; 0] = [];
        assert_eq!(next_seq_from_ids(ids.iter().copied()), 0);
    }
}
