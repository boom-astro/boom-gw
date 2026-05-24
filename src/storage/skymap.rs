//! Pluggable storage for the FITS sky-map blobs.
//!
//! Mirrors BOOM proper's `utils/cutouts.rs` (PR #313): one struct
//! per backend (`MongoSkymapStorage`, `S3SkymapStorage`), both
//! wrapped in a [`SkymapStorage`] enum that dispatches at runtime.
//! Call sites never branch on backend; they hand a [`SkymapBlob`]
//! to `upsert` and ask for one by `superevent_id` via `get`.
//!
//! ## Backends
//!
//! * **Mongo** — bytes stored as BSON Binary (subtype Generic) in a
//!   dedicated `skymaps` collection keyed by `superevent_id`.
//!   WiredTiger compresses the data on disk, so we do not bother
//!   compressing on the application side. Fast for both single and
//!   bulk gets.
//!
//! * **S3** — bytes serialized as base64 inside a small JSON
//!   envelope at `{key_prefix}/skymaps/{superevent_id}.json`.
//!   Optionally zstd-compressed before serialization (FITS files
//!   compress well, typically ~3-5x). A Valkey-backed
//!   [`SkymapCache`] sits in front of S3 so a clusterer that
//!   re-attaches the same sky map within the cache TTL doesn't
//!   re-fetch from S3.
//!
//! ## When to pick which
//!
//! Mongo backend is the default — simpler operationally, faster
//! reads, no external dep. The S3 backend is intended for NRP-scale
//! deployments where the archive will eventually outgrow a single
//! mongo node, or where operators want the skymaps in object
//! storage for sharing with other services.

use std::time::Duration;

use mongodb::bson::{self, doc};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use thiserror::Error;
use tracing::{debug, error, instrument, warn};

/// In-application representation of one FITS sky-map blob.
/// Both backends accept and return this shape; the wire
/// representation (BSON for mongo, JSON-with-base64 for S3) is an
/// internal concern of the backend.
#[derive(Debug, Clone)]
pub struct SkymapBlob {
    pub superevent_id: String,
    pub bytes: Vec<u8>,
    pub elapsed_ms: u64,
}

#[derive(Debug, Error)]
pub enum SkymapStorageError {
    #[error("skymap not found for superevent_id {0:?}")]
    NotFound(String),
    #[error("mongo error: {0}")]
    Mongo(#[from] mongodb::error::Error),
    #[error("bson serialize error: {0}")]
    Bson(#[from] bson::ser::Error),
    #[error("s3 put_object failed: {0}")]
    S3Put(String),
    #[error("s3 get_object failed: {0}")]
    S3Get(String),
    #[error("s3 delete_object failed: {0}")]
    S3Delete(String),
    #[error("s3 bucket setup failed: {0}")]
    S3Bucket(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("zstd error: {0}")]
    Zstd(String),
    #[error("redis cache error: {0}")]
    Redis(#[from] redis::RedisError),
}

/// Selected storage backend, used by CLI / env config to pick
/// between the variants without forcing every caller to import
/// the backend types directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkymapBackendKind {
    Mongo,
    S3,
}

impl std::str::FromStr for SkymapBackendKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "mongo" | "mongodb" => Ok(SkymapBackendKind::Mongo),
            "s3" => Ok(SkymapBackendKind::S3),
            other => Err(format!(
                "unknown skymap backend {other:?}; expected \"mongo\" or \"s3\""
            )),
        }
    }
}

/// S3 connection settings supplied by config / env. `endpoint_url`
/// is optional — leave it unset to talk to real AWS S3, set it to
/// point at MinIO / rustfs / a self-hosted S3-compatible store.
#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub key_prefix: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub endpoint_url: Option<String>,
    /// `true` → zstd-compress the FITS before writing. Saves
    /// ~3-5× on S3 storage for real BAYESTAR output.
    pub compress: bool,
    /// Optional Valkey-backed read cache. When `None`, every
    /// `get` hits S3.
    pub cache: Option<SkymapCacheConfig>,
}

#[derive(Debug, Clone)]
pub struct SkymapCacheConfig {
    pub redis_url: String,
    pub ttl: Duration,
    pub key_prefix: String,
}

/// Construct a [`SkymapStorage`] from runtime config. The mongo
/// variant just needs the existing `mongodb::Database` handle;
/// the S3 variant builds the AWS client, optionally wires the
/// cache, and runs `ensure_bucket` so the bucket exists.
pub async fn build_storage(
    kind: SkymapBackendKind,
    db: &mongodb::Database,
    s3: Option<S3Config>,
) -> Result<SkymapStorage, SkymapStorageError> {
    match kind {
        SkymapBackendKind::Mongo => Ok(SkymapStorage::Mongo(MongoSkymapStorage::new(db))),
        SkymapBackendKind::S3 => {
            let s3_cfg = s3.ok_or_else(|| {
                SkymapStorageError::S3Bucket(
                    "BOOM_GW_SKYMAP_STORAGE=s3 but no S3 connection settings supplied".into(),
                )
            })?;
            let credentials = aws_sdk_s3::config::Credentials::new(
                s3_cfg.access_key.clone(),
                s3_cfg.secret_key.clone(),
                None,
                None,
                "boom-gw-skymap",
            );
            let region = aws_sdk_s3::config::Region::new(s3_cfg.region.clone());
            let mut builder = aws_config::defaults(aws_sdk_s3::config::BehaviorVersion::latest())
                .credentials_provider(credentials)
                .region(region);
            if let Some(endpoint) = &s3_cfg.endpoint_url {
                builder = builder.endpoint_url(endpoint.clone());
            }
            let shared = builder.load().await;
            // For self-hosted S3 (rustfs/MinIO) the path-style
            // addressing is the safe default — virtual-hosted style
            // requires DNS magic the operator may not have set up.
            let s3_conf = aws_sdk_s3::config::Builder::from(&shared)
                .force_path_style(true)
                .build();
            let s3_client = aws_sdk_s3::Client::from_conf(s3_conf);
            let cache = if let Some(cache_cfg) = &s3_cfg.cache {
                let client = redis::Client::open(cache_cfg.redis_url.clone())
                    .map_err(SkymapStorageError::Redis)?;
                let conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(SkymapStorageError::Redis)?;
                Some(SkymapCache::new(
                    conn,
                    cache_cfg.ttl,
                    cache_cfg.key_prefix.clone(),
                ))
            } else {
                None
            };
            let s3_storage = S3SkymapStorage::new(
                s3_client,
                s3_cfg.bucket,
                s3_cfg.key_prefix,
                cache,
                s3_cfg.compress,
            )
            .await?;
            Ok(SkymapStorage::S3(s3_storage))
        }
    }
}

/// Top-level storage dispatcher.
pub enum SkymapStorage {
    Mongo(MongoSkymapStorage),
    S3(S3SkymapStorage),
}

impl SkymapStorage {
    pub async fn upsert(&self, blob: SkymapBlob) -> Result<(), SkymapStorageError> {
        match self {
            SkymapStorage::Mongo(m) => m.upsert(blob).await,
            SkymapStorage::S3(s) => s.upsert(blob).await,
        }
    }

    pub async fn get(&self, superevent_id: &str) -> Result<SkymapBlob, SkymapStorageError> {
        match self {
            SkymapStorage::Mongo(m) => m.get(superevent_id).await,
            SkymapStorage::S3(s) => s.get(superevent_id).await,
        }
    }

    pub async fn delete(&self, superevent_id: &str) -> Result<(), SkymapStorageError> {
        match self {
            SkymapStorage::Mongo(m) => m.delete(superevent_id).await,
            SkymapStorage::S3(s) => s.delete(superevent_id).await,
        }
    }

    /// Store a credible-region contour MOC for `superevent_id` at
    /// `level_pct` (an integer percent, e.g. `50` or `90`). Stored
    /// in a separate keyspace from the main skymap so the existing
    /// `upsert`/`get` paths are untouched.
    pub async fn upsert_contour(
        &self,
        superevent_id: &str,
        level_pct: u8,
        bytes: Vec<u8>,
    ) -> Result<(), SkymapStorageError> {
        match self {
            SkymapStorage::Mongo(m) => m.upsert_contour(superevent_id, level_pct, bytes).await,
            SkymapStorage::S3(s) => s.upsert_contour(superevent_id, level_pct, bytes).await,
        }
    }

    pub async fn get_contour(
        &self,
        superevent_id: &str,
        level_pct: u8,
    ) -> Result<Vec<u8>, SkymapStorageError> {
        match self {
            SkymapStorage::Mongo(m) => m.get_contour(superevent_id, level_pct).await,
            SkymapStorage::S3(s) => s.get_contour(superevent_id, level_pct).await,
        }
    }
}

// ===================== Mongo backend =====================

pub const SKYMAPS_COLLECTION: &str = "skymaps";
pub const SKYMAP_CONTOURS_COLLECTION: &str = "skymap_contours";

/// BSON wire shape for the mongo backend. `bytes` is stored as
/// native BSON Binary (subtype Generic) — denser than a base64
/// string, and `$bsonSize` works naturally for ops queries.
/// Mirrors BOOM's `AlertCutout::serialize_cutout` helper.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MongoSkymapDoc {
    #[serde(rename = "_id")]
    pub superevent_id: String,
    #[serde(
        serialize_with = "serialize_bson_binary",
        deserialize_with = "deserialize_bson_binary"
    )]
    pub bytes: Vec<u8>,
    pub elapsed_ms: u64,
}

fn serialize_bson_binary<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let binary = bson::Binary {
        subtype: bson::spec::BinarySubtype::Generic,
        bytes: bytes.clone(),
    };
    binary.serialize(serializer)
}

fn deserialize_bson_binary<'de, D>(d: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let binary = <bson::Binary as Deserialize>::deserialize(d)?;
    Ok(binary.bytes)
}

pub struct MongoSkymapStorage {
    collection: mongodb::Collection<MongoSkymapDoc>,
    contours: mongodb::Collection<MongoContourDoc>,
}

/// Mongo wire shape for a single contour MOC. Composite `_id` keeps
/// the document keyed by `(superevent_id, level_pct)`; the natural
/// `_id` index handles all lookups.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MongoContourDoc {
    #[serde(rename = "_id")]
    pub id: MongoContourId,
    #[serde(
        serialize_with = "serialize_bson_binary",
        deserialize_with = "deserialize_bson_binary"
    )]
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct MongoContourId {
    pub superevent_id: String,
    pub level_pct: u8,
}

impl MongoSkymapStorage {
    /// Bind to the `skymaps` collection on the given database. The
    /// collection's natural `_id` (superevent_id) index is enough;
    /// no further indexes are created.
    pub fn new(db: &mongodb::Database) -> Self {
        let collection = db.collection::<MongoSkymapDoc>(SKYMAPS_COLLECTION);
        let contours = db.collection::<MongoContourDoc>(SKYMAP_CONTOURS_COLLECTION);
        Self {
            collection,
            contours,
        }
    }

    #[instrument(skip_all, err, fields(superevent_id = %superevent_id, level_pct = level_pct))]
    pub async fn upsert_contour(
        &self,
        superevent_id: &str,
        level_pct: u8,
        bytes: Vec<u8>,
    ) -> Result<(), SkymapStorageError> {
        let id = MongoContourId {
            superevent_id: superevent_id.to_string(),
            level_pct,
        };
        let doc = MongoContourDoc {
            id: id.clone(),
            bytes,
        };
        let filter = doc! {"_id": bson::to_bson(&id)?};
        self.contours
            .replace_one(filter, &doc)
            .upsert(true)
            .await?;
        Ok(())
    }

    #[instrument(skip_all, err, fields(superevent_id = %superevent_id, level_pct = level_pct))]
    pub async fn get_contour(
        &self,
        superevent_id: &str,
        level_pct: u8,
    ) -> Result<Vec<u8>, SkymapStorageError> {
        let id = MongoContourId {
            superevent_id: superevent_id.to_string(),
            level_pct,
        };
        let filter = doc! {"_id": bson::to_bson(&id)?};
        match self.contours.find_one(filter).await? {
            Some(d) => Ok(d.bytes),
            None => Err(SkymapStorageError::NotFound(format!(
                "{superevent_id}@{level_pct}%"
            ))),
        }
    }

    #[instrument(skip_all, err, fields(superevent_id = %blob.superevent_id))]
    pub async fn upsert(&self, blob: SkymapBlob) -> Result<(), SkymapStorageError> {
        let doc = MongoSkymapDoc {
            superevent_id: blob.superevent_id.clone(),
            bytes: blob.bytes,
            elapsed_ms: blob.elapsed_ms,
        };
        let filter = doc! {"_id": &blob.superevent_id};
        self.collection
            .replace_one(filter, &doc)
            .upsert(true)
            .await?;
        Ok(())
    }

    #[instrument(skip_all, err, fields(superevent_id = %superevent_id))]
    pub async fn get(&self, superevent_id: &str) -> Result<SkymapBlob, SkymapStorageError> {
        let filter = doc! {"_id": superevent_id};
        match self.collection.find_one(filter).await? {
            Some(d) => Ok(SkymapBlob {
                superevent_id: d.superevent_id,
                bytes: d.bytes,
                elapsed_ms: d.elapsed_ms,
            }),
            None => Err(SkymapStorageError::NotFound(superevent_id.to_string())),
        }
    }

    #[instrument(skip_all, err, fields(superevent_id = %superevent_id))]
    pub async fn delete(&self, superevent_id: &str) -> Result<(), SkymapStorageError> {
        let filter = doc! {"_id": superevent_id};
        self.collection.delete_one(filter).await?;
        Ok(())
    }
}

// ===================== S3 backend =====================

/// JSON wire shape for the S3 backend. `bytes` is base64 inside
/// JSON because `serde_json` can't emit raw bytes — this is the
/// same trade-off BOOM made in `S3AlertCutout`. zstd compression
/// happens *before* base64 when enabled, so the on-the-wire payload
/// shrinks even with the base64 expansion.
#[serde_as]
#[derive(Serialize, Deserialize, Clone)]
struct S3SkymapEnvelope {
    pub superevent_id: String,
    #[serde_as(as = "Base64")]
    pub bytes: Vec<u8>,
    pub elapsed_ms: u64,
    /// `true` if `bytes` is zstd-compressed; the consumer
    /// transparently inflates on read.
    #[serde(default)]
    pub compressed: bool,
}

fn s3_key(key_prefix: &str, superevent_id: &str) -> String {
    format!("{key_prefix}/skymaps/{superevent_id}.json")
}

fn s3_contour_key(key_prefix: &str, superevent_id: &str, level_pct: u8) -> String {
    format!("{key_prefix}/contours/{superevent_id}/{level_pct}.fits")
}

fn zstd_encode(data: &[u8]) -> Result<Vec<u8>, SkymapStorageError> {
    // Level 0 = libzstd's "default" (currently 3). Good speed +
    // ratio trade-off; same level BOOM uses for LSST cutouts.
    zstd::encode_all(data, 0).map_err(|e| SkymapStorageError::Zstd(e.to_string()))
}

fn zstd_decode(data: &[u8]) -> Result<Vec<u8>, SkymapStorageError> {
    zstd::decode_all(data).map_err(|e| SkymapStorageError::Zstd(e.to_string()))
}

pub struct S3SkymapStorage {
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    key_prefix: String,
    cache: Option<SkymapCache>,
    /// When `true`, FITS bytes are zstd-compressed before being
    /// written to S3 and the cache. FITS files compress well
    /// (~3-5×), so this is a real saving on S3 storage costs.
    compress: bool,
}

impl S3SkymapStorage {
    /// Create a new S3 backend. Will issue a `head_bucket` and,
    /// if the bucket does not exist, `create_bucket` — same idempotent
    /// pattern as BOOM's `S3CutoutStorage::new`.
    pub async fn new(
        s3_client: aws_sdk_s3::Client,
        bucket: String,
        key_prefix: String,
        cache: Option<SkymapCache>,
        compress: bool,
    ) -> Result<Self, SkymapStorageError> {
        ensure_bucket(&s3_client, &bucket).await?;
        Ok(Self {
            s3_client,
            bucket,
            key_prefix,
            cache,
            compress,
        })
    }

    #[instrument(skip_all, err, fields(superevent_id = %blob.superevent_id))]
    pub async fn upsert(&self, blob: SkymapBlob) -> Result<(), SkymapStorageError> {
        let key = s3_key(&self.key_prefix, &blob.superevent_id);
        let (bytes, compressed) = if self.compress {
            let encoded = tokio::task::spawn_blocking({
                let bytes = blob.bytes.clone();
                move || zstd_encode(&bytes)
            })
            .await
            .map_err(|e| SkymapStorageError::Zstd(format!("spawn_blocking: {e}")))??;
            (encoded, true)
        } else {
            (blob.bytes.clone(), false)
        };
        let envelope = S3SkymapEnvelope {
            superevent_id: blob.superevent_id.clone(),
            bytes,
            elapsed_ms: blob.elapsed_ms,
            compressed,
        };
        let body = aws_sdk_s3::primitives::ByteStream::from(serde_json::to_vec(&envelope)?);
        self.s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .send()
            .await
            .map_err(|e| SkymapStorageError::S3Put(format!("{e}")))?;
        // Populate the cache with the *uncompressed* bytes — the
        // application always wants the FITS bytes, not the zstd
        // stream.
        if let Some(cache) = &self.cache {
            let cache_blob = SkymapBlob {
                superevent_id: blob.superevent_id,
                bytes: blob.bytes,
                elapsed_ms: blob.elapsed_ms,
            };
            cache.set(&cache_blob).await;
        }
        Ok(())
    }

    #[instrument(skip_all, err, fields(superevent_id = %superevent_id))]
    pub async fn get(&self, superevent_id: &str) -> Result<SkymapBlob, SkymapStorageError> {
        if let Some(cache) = &self.cache {
            if let Some(blob) = cache.get(superevent_id).await {
                debug!(superevent_id, "skymap cache hit");
                return Ok(blob);
            }
        }
        let key = s3_key(&self.key_prefix, superevent_id);
        let resp = self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                if e.as_service_error()
                    .map(|se| se.is_no_such_key())
                    .unwrap_or(false)
                {
                    SkymapStorageError::NotFound(superevent_id.to_string())
                } else {
                    SkymapStorageError::S3Get(format!("{e}"))
                }
            })?;
        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| SkymapStorageError::S3Get(format!("body collect: {e}")))?
            .into_bytes();
        let envelope: S3SkymapEnvelope = serde_json::from_slice(&bytes)?;
        let final_bytes = if envelope.compressed {
            tokio::task::spawn_blocking(move || zstd_decode(&envelope.bytes))
                .await
                .map_err(|e| SkymapStorageError::Zstd(format!("spawn_blocking: {e}")))??
        } else {
            envelope.bytes
        };
        let blob = SkymapBlob {
            superevent_id: envelope.superevent_id,
            bytes: final_bytes,
            elapsed_ms: envelope.elapsed_ms,
        };
        if let Some(cache) = &self.cache {
            cache.set(&blob).await;
        }
        Ok(blob)
    }

    #[instrument(skip_all, err, fields(superevent_id = %superevent_id))]
    pub async fn delete(&self, superevent_id: &str) -> Result<(), SkymapStorageError> {
        if let Some(cache) = &self.cache {
            cache.del(superevent_id).await;
        }
        let key = s3_key(&self.key_prefix, superevent_id);
        self.s3_client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| SkymapStorageError::S3Delete(format!("{e}")))?;
        Ok(())
    }

    /// Contour MOCs are stored raw (no JSON envelope, no zstd) — at
    /// ~12 KiB per FITS the savings don't justify the round-trip
    /// cost, and Aladin Lite can stream the bytes straight into
    /// `A.MOCFromURL`. Cache layer is intentionally skipped for the
    /// same reason.
    #[instrument(skip_all, err, fields(superevent_id = %superevent_id, level_pct = level_pct))]
    pub async fn upsert_contour(
        &self,
        superevent_id: &str,
        level_pct: u8,
        bytes: Vec<u8>,
    ) -> Result<(), SkymapStorageError> {
        let key = s3_contour_key(&self.key_prefix, superevent_id, level_pct);
        let body = aws_sdk_s3::primitives::ByteStream::from(bytes);
        self.s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .send()
            .await
            .map_err(|e| SkymapStorageError::S3Put(format!("{e}")))?;
        Ok(())
    }

    #[instrument(skip_all, err, fields(superevent_id = %superevent_id, level_pct = level_pct))]
    pub async fn get_contour(
        &self,
        superevent_id: &str,
        level_pct: u8,
    ) -> Result<Vec<u8>, SkymapStorageError> {
        let key = s3_contour_key(&self.key_prefix, superevent_id, level_pct);
        let resp = self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                if e.as_service_error()
                    .map(|se| se.is_no_such_key())
                    .unwrap_or(false)
                {
                    SkymapStorageError::NotFound(format!("{superevent_id}@{level_pct}%"))
                } else {
                    SkymapStorageError::S3Get(format!("{e}"))
                }
            })?;
        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| SkymapStorageError::S3Get(format!("body collect: {e}")))?
            .into_bytes();
        Ok(bytes.to_vec())
    }
}

/// Idempotent: `head_bucket` first, `create_bucket` on
/// `NoSuchBucket`, tolerate `BucketAlreadyExists`/
/// `BucketAlreadyOwnedByYou` from concurrent creators. Same
/// shape as BOOM's `create_bucket_if_not_exists`.
async fn ensure_bucket(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
) -> Result<(), SkymapStorageError> {
    match s3_client.head_bucket().bucket(bucket).send().await {
        Ok(_) => return Ok(()),
        Err(e) => {
            let is_not_found = e
                .as_service_error()
                .map(|se| se.is_not_found())
                .unwrap_or(false);
            if !is_not_found {
                return Err(SkymapStorageError::S3Bucket(format!(
                    "head_bucket failed: {e}"
                )));
            }
        }
    }
    match s3_client.create_bucket().bucket(bucket).send().await {
        Ok(_) => Ok(()),
        Err(e) => {
            let already = e
                .as_service_error()
                .map(|se| se.is_bucket_already_exists() || se.is_bucket_already_owned_by_you())
                .unwrap_or(false);
            if already {
                Ok(())
            } else {
                Err(SkymapStorageError::S3Bucket(format!(
                    "create_bucket failed: {e}"
                )))
            }
        }
    }
}

// ===================== Valkey-backed cache (for S3) =====================

/// Tiny cache that sits in front of [`S3SkymapStorage`]. Keys
/// are `{prefix}:skymap:{superevent_id}` and the value is a
/// length-prefixed binary blob containing `elapsed_ms` + the
/// FITS bytes. The same TTL applies to every key; the cache
/// evicts naturally when the TTL fires or when Valkey hits its
/// configured `maxmemory` cap.
#[derive(Clone)]
pub struct SkymapCache {
    connection: redis::aio::MultiplexedConnection,
    ttl: Duration,
    key_prefix: String,
}

impl SkymapCache {
    pub fn new(
        connection: redis::aio::MultiplexedConnection,
        ttl: Duration,
        key_prefix: impl Into<String>,
    ) -> Self {
        Self {
            connection,
            ttl,
            key_prefix: key_prefix.into(),
        }
    }

    fn cache_key(&self, superevent_id: &str) -> String {
        format!("{}:skymap:{}", self.key_prefix, superevent_id)
    }

    fn pack(blob: &SkymapBlob) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + blob.bytes.len());
        buf.extend_from_slice(&blob.elapsed_ms.to_le_bytes());
        buf.extend_from_slice(&blob.bytes);
        buf
    }

    fn unpack(superevent_id: &str, raw: &[u8]) -> Option<SkymapBlob> {
        if raw.len() < 8 {
            return None;
        }
        let elapsed_ms = u64::from_le_bytes(raw[0..8].try_into().ok()?);
        let bytes = raw[8..].to_vec();
        Some(SkymapBlob {
            superevent_id: superevent_id.to_string(),
            bytes,
            elapsed_ms,
        })
    }

    async fn set(&self, blob: &SkymapBlob) {
        let key = self.cache_key(&blob.superevent_id);
        let mut conn = self.connection.clone();
        let ttl = self.ttl.as_secs().max(1);
        if let Err(e) = conn.set_ex::<_, _, ()>(&key, Self::pack(blob), ttl).await {
            warn!("skymap cache set failed for {}: {e}", blob.superevent_id);
        }
    }

    async fn get(&self, superevent_id: &str) -> Option<SkymapBlob> {
        let key = self.cache_key(superevent_id);
        let mut conn = self.connection.clone();
        let raw: Option<Vec<u8>> = match conn.get(&key).await {
            Ok(v) => v,
            Err(e) => {
                warn!("skymap cache get failed for {superevent_id}: {e}");
                return None;
            }
        };
        raw.and_then(|b| Self::unpack(superevent_id, &b))
    }

    async fn del(&self, superevent_id: &str) {
        let key = self.cache_key(superevent_id);
        let mut conn = self.connection.clone();
        if let Err(e) = conn.del::<_, ()>(&key).await {
            warn!("skymap cache del failed for {superevent_id}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_unpack_round_trips() {
        let blob = SkymapBlob {
            superevent_id: "S000042".into(),
            bytes: b"some bytes".to_vec(),
            elapsed_ms: 137,
        };
        let packed = SkymapCache::pack(&blob);
        let unpacked = SkymapCache::unpack("S000042", &packed).expect("unpack");
        assert_eq!(unpacked.superevent_id, "S000042");
        assert_eq!(unpacked.bytes, b"some bytes");
        assert_eq!(unpacked.elapsed_ms, 137);
    }

    #[test]
    fn pack_unpack_handles_empty_payload() {
        let blob = SkymapBlob {
            superevent_id: "S0".into(),
            bytes: vec![],
            elapsed_ms: 0,
        };
        let packed = SkymapCache::pack(&blob);
        // 8 bytes for elapsed_ms only.
        assert_eq!(packed.len(), 8);
        let unpacked = SkymapCache::unpack("S0", &packed).unwrap();
        assert_eq!(unpacked.bytes.len(), 0);
        assert_eq!(unpacked.elapsed_ms, 0);
    }

    #[test]
    fn unpack_rejects_truncated_input() {
        // 7 bytes — header is only 4. Should fail rather than panic.
        let raw = vec![0u8; 7];
        assert!(SkymapCache::unpack("S0", &raw).is_none());
    }

    #[test]
    fn s3_key_layout() {
        assert_eq!(s3_key("boom-gw", "S000001"), "boom-gw/skymaps/S000001.json");
        assert_eq!(
            s3_key("nrp/prod", "S_unusual_id"),
            "nrp/prod/skymaps/S_unusual_id.json"
        );
    }

    #[test]
    fn s3_contour_key_layout() {
        assert_eq!(
            s3_contour_key("boom-gw", "S000001", 90),
            "boom-gw/contours/S000001/90.fits"
        );
        assert_eq!(
            s3_contour_key("nrp/prod", "S_unusual", 50),
            "nrp/prod/contours/S_unusual/50.fits"
        );
    }

    #[test]
    fn zstd_round_trip() {
        let original = b"FITS-like-content".repeat(100);
        let encoded = zstd_encode(&original).unwrap();
        // Compression should reduce a repetitive payload.
        assert!(encoded.len() < original.len());
        let decoded = zstd_decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn mongo_skymap_doc_serializes_id_as_underscore_id() {
        let doc = MongoSkymapDoc {
            superevent_id: "S000007".into(),
            bytes: b"abc".to_vec(),
            elapsed_ms: 42,
        };
        let bson_doc = bson::to_document(&doc).unwrap();
        assert_eq!(bson_doc.get_str("_id").unwrap(), "S000007");
        // bytes is a BSON Binary, not a string — exactly the
        // point of using serialize_bson_binary.
        match bson_doc.get("bytes").unwrap() {
            bson::Bson::Binary(b) => {
                assert_eq!(b.subtype, bson::spec::BinarySubtype::Generic);
                assert_eq!(b.bytes, b"abc");
            }
            other => panic!("expected Binary, got {other:?}"),
        }
    }
}
