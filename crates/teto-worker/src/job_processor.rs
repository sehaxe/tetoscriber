use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use teto_protocol::{TranscriptionSegment, VoiceFingerprint, VOICE_FINGERPRINT_DIM};
use tokio::fs::File;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;
use tracing::{info, warn};
use uuid::Uuid;

use crate::riva_client::{RivaClient, RivaClientConfig};

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1/";
const DEFAULT_ARCHIVE_QUEUE: &str = "teto_archive_jobs";
const DEFAULT_LIVE_CHANNEL: &str = "teto_audio_live";
const DEFAULT_TRANSCRIPTION_STREAM: &str = "teto_transcription_stream";
const DEFAULT_TRANSCRIPTION_FIELD: &str = "segment";
const DEFAULT_FILE_CHUNK_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub redis_url: String,
    pub archive_queue: String,
    pub live_channel: String,
    pub transcription_stream: String,
    pub transcription_field: String,
    pub file_chunk_bytes: usize,
    pub reconnect_delay: Duration,
}

impl WorkerConfig {
    pub fn from_env() -> Self {
        Self {
            redis_url: env_or_default("REDIS_URL", DEFAULT_REDIS_URL),
            archive_queue: env_or_default("TETO_ARCHIVE_JOBS", DEFAULT_ARCHIVE_QUEUE),
            live_channel: env_or_default("TETO_AUDIO_LIVE", DEFAULT_LIVE_CHANNEL),
            transcription_stream: env_or_default(
                "TETO_TRANSCRIPTION_STREAM",
                DEFAULT_TRANSCRIPTION_STREAM,
            ),
            transcription_field: env_or_default(
                "TETO_TRANSCRIPTION_FIELD",
                DEFAULT_TRANSCRIPTION_FIELD,
            ),
            file_chunk_bytes: env_usize("TETO_FILE_CHUNK_BYTES", DEFAULT_FILE_CHUNK_BYTES),
            reconnect_delay: Duration::from_secs(env_u64("TETO_RECONNECT_DELAY_SECS", 2)),
        }
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

#[derive(Debug, Clone)]
pub struct JobProcessor {
    config: WorkerConfig,
    riva: RivaClient,
}

impl JobProcessor {
    pub fn from_env() -> Self {
        Self::new(WorkerConfig::from_env(), RivaClientConfig::from_env())
    }

    pub fn new(config: WorkerConfig, riva_config: RivaClientConfig) -> Self {
        Self {
            config,
            riva: RivaClient::new(riva_config),
        }
    }

    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    pub async fn listen_archive_jobs(self) {
        loop {
            match self.run_archive_connected().await {
                Ok(()) => {}
                Err(error) => {
                    warn!(%error, "archive job consumer failed; reconnecting");
                    sleep(self.config.reconnect_delay).await;
                }
            }
        }
    }

    pub async fn listen_live_audio(self) {
        loop {
            match self.run_live_connected().await {
                Ok(()) => {}
                Err(error) => {
                    warn!(%error, "live audio consumer failed; reconnecting");
                    sleep(self.config.reconnect_delay).await;
                }
            }
        }
    }

    async fn run_archive_connected(&self) -> Result<()> {
        let mut connection = self.connect().await?;

        loop {
            let payload: Option<String> = connection
                .brpop(&[self.config.archive_queue.as_str()], 0.0)
                .await
                .with_context(|| {
                    format!(
                        "failed to read archive jobs from '{}'",
                        self.config.archive_queue
                    )
                })?;

            if let Some(path) = payload {
                self.process_archive_job(&mut connection, path).await?;
            }
        }
    }

    async fn process_archive_job(
        &self,
        connection: &mut ConnectionManager,
        path: String,
    ) -> Result<()> {
        let path = PathBuf::from(path.trim());
        let session_id = Uuid::new_v4().to_string();
        let file = File::open(&path)
            .await
            .with_context(|| format!("failed to open archive audio file '{}'", path.display()))?;

        info!(
            session_id = %session_id,
            path = %path.display(),
            chunk_bytes = self.config.file_chunk_bytes,
            "processing archive audio job"
        );

        let chunks =
            ReaderStream::with_capacity(file, self.config.file_chunk_bytes).filter_map(|chunk| {
                match chunk {
                    Ok(bytes) if !bytes.is_empty() => Some(bytes.to_vec()),
                    _ => None,
                }
            });

        let mut segments = self
            .riva
            .stream_chunks_to_riva(session_id.clone(), chunks)
            .await
            .with_context(|| format!("Riva transcription failed for '{}'", path.display()))?;

        bridge_placeholder_fingerprints(&mut segments);
        self.publish_segments(connection, &segments).await?;

        info!(
            session_id = %session_id,
            path = %path.display(),
            segments = segments.len(),
            "published archive transcription segments"
        );

        Ok(())
    }

    async fn run_live_connected(&self) -> Result<()> {
        let client = Client::open(self.config.redis_url.as_str())
            .with_context(|| format!("invalid Redis URL '{}'", self.config.redis_url))?;
        let mut pubsub = client
            .get_async_pubsub()
            .await
            .context("failed to open Redis Pub/Sub connection")?;

        pubsub
            .subscribe(&self.config.live_channel)
            .await
            .with_context(|| format!("failed to subscribe to '{}'", self.config.live_channel))?;

        let mut messages = pubsub.on_message();
        let session_id = Uuid::new_v4().to_string();
        let mut stream = self.riva.open_streaming_session(session_id.clone()).await?;

        info!(
            session_id = %session_id,
            channel = %self.config.live_channel,
            "live audio stream opened"
        );

        while let Some(message) = messages.next().await {
            let chunk: Vec<u8> = message
                .get_payload()
                .context("failed to decode live audio Pub/Sub payload as bytes")?;
            stream.send_chunk(chunk).await?;

            let mut published = 0usize;
            while let Some(segment) = stream.recv_next().await? {
                self.publish_segment(&segment).await?;
                published += 1;
            }

            if published > 0 {
                info!(
                    session_id = %session_id,
                    published,
                    "published live transcription segments"
                );
            }
        }

        let mut segments = stream.close().await?;
        bridge_placeholder_fingerprints(&mut segments);

        let mut connection = self.connect().await?;
        self.publish_segments(&mut connection, &segments).await?;

        info!(
            session_id = %session_id,
            segments = segments.len(),
            "live audio stream closed"
        );

        Ok(())
    }

    async fn publish_segments(
        &self,
        connection: &mut ConnectionManager,
        segments: &[TranscriptionSegment],
    ) -> Result<()> {
        for segment in segments {
            self.publish_segment_to_connection(connection, segment)
                .await?;
        }

        Ok(())
    }

    async fn publish_segment(&self, segment: &TranscriptionSegment) -> Result<()> {
        let mut connection = self.connect().await?;
        self.publish_segment_to_connection(&mut connection, segment)
            .await
    }

    async fn publish_segment_to_connection(
        &self,
        connection: &mut ConnectionManager,
        segment: &TranscriptionSegment,
    ) -> Result<()> {
        let payload = serde_json::to_string(segment).context("failed to encode segment as JSON")?;
        let _: String = connection
            .xadd(
                self.config.transcription_stream.as_str(),
                "*",
                &[(self.config.transcription_field.as_str(), payload.as_str())],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to publish segment to Redis stream '{}'",
                    self.config.transcription_stream
                )
            })?;

        Ok(())
    }

    async fn connect(&self) -> Result<ConnectionManager> {
        let client = Client::open(self.config.redis_url.as_str())
            .with_context(|| format!("invalid Redis URL '{}'", self.config.redis_url))?;

        tokio::time::timeout(Duration::from_secs(5), client.get_connection_manager())
            .await
            .context("Redis connection manager creation timed out")?
            .context("failed to create Redis connection manager")
    }
}

fn bridge_placeholder_fingerprints(segments: &mut [TranscriptionSegment]) {
    for segment in segments {
        if segment.voice_fingerprint.is_none() {
            segment.voice_fingerprint = Some(placeholder_voice_fingerprint(
                &segment.session_id,
                &segment.speaker_tag,
            ));
        }
    }
}

fn placeholder_voice_fingerprint(session_id: &str, speaker_tag: &str) -> VoiceFingerprint {
    let seed = fnv1a64(format!("{session_id}:{speaker_tag}").as_bytes());
    let mut values = Vec::with_capacity(VOICE_FINGERPRINT_DIM);

    for index in 0..VOICE_FINGERPRINT_DIM {
        let mixed = seed ^ (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let unit = ((mixed >> 11) as f32) / ((1u64 << 53) as f32);
        values.push(unit * 2.0 - 1.0);
    }

    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut values {
            *value /= norm;
        }
    }

    VoiceFingerprint::new(values).expect("placeholder fingerprint has 192 dimensions")
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
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

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_fingerprint_is_deterministic_and_dimensional() {
        let first = placeholder_voice_fingerprint("session", "Speaker_1");
        let second = placeholder_voice_fingerprint("session", "Speaker_1");
        let other = placeholder_voice_fingerprint("session", "Speaker_2");

        assert_eq!(first.as_slice().len(), VOICE_FINGERPRINT_DIM);
        assert_eq!(first, second);
        assert_ne!(first, other);
    }

    #[test]
    fn fnv1a64_changes_with_input() {
        assert_ne!(fnv1a64(b"Speaker_1"), fnv1a64(b"Speaker_2"));
    }
}
