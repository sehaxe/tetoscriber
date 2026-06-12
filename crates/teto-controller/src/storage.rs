use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, Distance, PointId, QueryPointsBuilder, UpsertPointsBuilder,
    VectorParamsBuilder,
};
use qdrant_client::{Payload, Qdrant};
use serde_json::json;
use teto_protocol::{
    TranscriptEmbedding, VoiceFingerprint, VoiceIdentityUpsert, VOICE_FINGERPRINT_DIM,
};
use tracing::{debug, warn};
use uuid::Uuid;

const DEFAULT_QDRANT_URL: &str = "http://localhost:6334";
const DEFAULT_VOICE_COLLECTION: &str = "teto_voices";
const DEFAULT_TRANSCRIPT_COLLECTION: &str = "teto_transcripts";
const DEFAULT_TRANSCRIPT_DIM: usize = 1024;

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub qdrant_url: String,
    pub voice_collection: String,
    pub transcript_collection: String,
    pub transcript_dim: usize,
    pub voice_match_threshold: f32,
}

impl StorageConfig {
    pub fn from_env() -> Self {
        Self {
            qdrant_url: env_or_default("QDRANT_URL", DEFAULT_QDRANT_URL),
            voice_collection: env_or_default("TETO_VOICE_COLLECTION", DEFAULT_VOICE_COLLECTION),
            transcript_collection: env_or_default(
                "TETO_TRANSCRIPT_COLLECTION",
                DEFAULT_TRANSCRIPT_COLLECTION,
            ),
            transcript_dim: env_usize("TETO_TRANSCRIPT_DIM", DEFAULT_TRANSCRIPT_DIM),
            voice_match_threshold: env_f32(
                "TETO_VOICE_MATCH_THRESHOLD",
                teto_protocol::DEFAULT_VOICE_MATCH_THRESHOLD,
            ),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct MatchedVoice {
    pub name: String,
    pub confidence: f32,
    pub point_id: Option<String>,
}

pub struct SovereignMemory {
    client: Qdrant,
    config: StorageConfig,
}

#[async_trait]
pub trait IdentityStorage: Send + Sync {
    async fn match_voice(&self, fingerprint: &VoiceFingerprint) -> Result<Option<MatchedVoice>>;
    async fn upsert_voice_identity(&self, upsert: &VoiceIdentityUpsert) -> Result<()>;
    async fn index_transcript(&self, embedding: &TranscriptEmbedding) -> Result<()>;
}

#[allow(dead_code)]
impl SovereignMemory {
    pub async fn connect(config: StorageConfig) -> Result<Self> {
        let client = Qdrant::from_url(&config.qdrant_url)
            .skip_compatibility_check()
            .build()
            .with_context(|| format!("failed to build Qdrant client for {}", config.qdrant_url))?;

        Ok(Self { client, config })
    }

    pub async fn ensure_collections(&self) -> Result<()> {
        self.ensure_voice_collection().await?;
        self.ensure_transcript_collection().await?;
        Ok(())
    }

    pub async fn match_voice(
        &self,
        fingerprint: &VoiceFingerprint,
    ) -> Result<Option<MatchedVoice>> {
        let response = self
            .client
            .query(
                QueryPointsBuilder::new(self.config.voice_collection.clone())
                    .query(fingerprint.clone().into_vec())
                    .limit(1)
                    .with_payload(true)
                    .score_threshold(self.config.voice_match_threshold),
            )
            .await
            .with_context(|| {
                format!(
                    "failed to query voice collection '{}'",
                    self.config.voice_collection
                )
            })?;

        let Some(point) = response.result.into_iter().next() else {
            return Ok(None);
        };

        let payload_json: serde_json::Value = Payload::from(point.payload).into();
        let Some(name) = payload_json.get("name").and_then(|value| value.as_str()) else {
            warn!(
                collection = %self.config.voice_collection,
                "Qdrant returned a voice match without a name payload"
            );
            return Ok(None);
        };

        Ok(Some(MatchedVoice {
            name: name.to_owned(),
            confidence: point.score,
            point_id: point.id.as_ref().map(point_id_to_string),
        }))
    }

    pub async fn upsert_voice_identity(&self, upsert: &VoiceIdentityUpsert) -> Result<()> {
        let payload: Payload = json!({
            "session_id": upsert.session_id,
            "speaker_tag": upsert.speaker_tag,
            "name": upsert.name,
            "confidence": upsert.confidence,
            "source": format!("{:?}", upsert.source),
        })
        .try_into()
        .context("failed to convert voice identity payload for Qdrant")?;

        let point_id = voice_point_id(&upsert.session_id, &upsert.speaker_tag);
        let point = qdrant_client::qdrant::PointStruct::new(
            point_id,
            upsert.fingerprint.clone().into_vec(),
            payload,
        );

        self.client
            .upsert_points(UpsertPointsBuilder::new(
                self.config.voice_collection.clone(),
                vec![point],
            ))
            .await
            .with_context(|| {
                format!(
                    "failed to upsert voice identity into collection '{}'",
                    self.config.voice_collection
                )
            })?;

        Ok(())
    }

    pub async fn index_transcript(&self, embedding: &TranscriptEmbedding) -> Result<()> {
        if embedding.vector.len() != self.config.transcript_dim {
            bail!(
                "invalid transcript embedding dimension for collection '{}': expected {}, got {}",
                self.config.transcript_collection,
                self.config.transcript_dim,
                embedding.vector.len()
            );
        }

        let payload: Payload = json!({
            "session_id": embedding.session_id,
            "text": embedding.text,
        })
        .try_into()
        .context("failed to convert transcript payload for Qdrant")?;

        let point = qdrant_client::qdrant::PointStruct::new(
            Uuid::new_v4().to_string(),
            embedding.vector.clone(),
            payload,
        );

        self.client
            .upsert_points(UpsertPointsBuilder::new(
                self.config.transcript_collection.clone(),
                vec![point],
            ))
            .await
            .with_context(|| {
                format!(
                    "failed to index transcript into collection '{}'",
                    self.config.transcript_collection
                )
            })?;

        Ok(())
    }

    async fn ensure_voice_collection(&self) -> Result<()> {
        self.ensure_collection(
            &self.config.voice_collection,
            VOICE_FINGERPRINT_DIM,
            "voice",
        )
        .await
    }

    async fn ensure_transcript_collection(&self) -> Result<()> {
        self.ensure_collection(
            &self.config.transcript_collection,
            self.config.transcript_dim,
            "transcript",
        )
        .await
    }

    async fn ensure_collection(
        &self,
        collection_name: &str,
        vector_dim: usize,
        collection_kind: &str,
    ) -> Result<()> {
        if self.client.collection_exists(collection_name).await? {
            debug!(
                collection = %collection_name,
                "{collection_kind} collection already exists"
            );
            return Ok(());
        }

        self.client
            .create_collection(
                CreateCollectionBuilder::new(collection_name).vectors_config(
                    VectorParamsBuilder::new(vector_dim as u64, Distance::Cosine),
                ),
            )
            .await
            .with_context(|| {
                format!("failed to create {collection_kind} collection '{collection_name}'")
            })?;

        debug!(
            collection = %collection_name,
            dim = vector_dim,
            "created {collection_kind} collection"
        );

        Ok(())
    }
}

#[async_trait]
impl IdentityStorage for SovereignMemory {
    async fn match_voice(&self, fingerprint: &VoiceFingerprint) -> Result<Option<MatchedVoice>> {
        SovereignMemory::match_voice(self, fingerprint).await
    }

    async fn upsert_voice_identity(&self, upsert: &VoiceIdentityUpsert) -> Result<()> {
        SovereignMemory::upsert_voice_identity(self, upsert).await
    }

    async fn index_transcript(&self, embedding: &TranscriptEmbedding) -> Result<()> {
        SovereignMemory::index_transcript(self, embedding).await
    }
}

fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_f32(key: &str, default: f32) -> f32 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<f32>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0 && *value <= 1.0)
        .unwrap_or(default)
}

fn point_id_to_string(point_id: &PointId) -> String {
    match point_id.point_id_options.as_ref() {
        Some(PointIdOptions::Uuid(uuid)) => uuid.clone(),
        Some(PointIdOptions::Num(num)) => num.to_string(),
        None => String::new(),
    }
}

fn voice_point_id(session_id: &str, speaker_tag: &str) -> String {
    format!("{session_id}:{speaker_tag}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_config_uses_safe_defaults_when_env_is_missing() {
        std::env::remove_var("QDRANT_URL");
        std::env::remove_var("TETO_VOICE_COLLECTION");
        std::env::remove_var("TETO_TRANSCRIPT_COLLECTION");
        std::env::remove_var("TETO_TRANSCRIPT_DIM");
        std::env::remove_var("TETO_VOICE_MATCH_THRESHOLD");

        let config = StorageConfig::from_env();

        assert_eq!(config.qdrant_url, DEFAULT_QDRANT_URL);
        assert_eq!(config.voice_collection, DEFAULT_VOICE_COLLECTION);
        assert_eq!(config.transcript_collection, DEFAULT_TRANSCRIPT_COLLECTION);
        assert_eq!(config.transcript_dim, DEFAULT_TRANSCRIPT_DIM);
        assert_eq!(
            config.voice_match_threshold,
            teto_protocol::DEFAULT_VOICE_MATCH_THRESHOLD
        );
    }

    #[test]
    fn voice_point_id_is_stable_for_same_session_and_speaker() {
        assert_eq!(
            voice_point_id("session-1", "Speaker_1"),
            voice_point_id("session-1", "Speaker_1")
        );
        assert_ne!(
            voice_point_id("session-1", "Speaker_1"),
            voice_point_id("session-2", "Speaker_1")
        );
    }
}
