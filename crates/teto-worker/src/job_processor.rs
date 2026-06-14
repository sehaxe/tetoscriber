use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use teto_protocol::{TranscriptionSegment, VoiceFingerprint, VOICE_FINGERPRINT_DIM};
use tokio_stream::StreamExt;
use tracing::{info, warn};
use uuid::Uuid;

use crate::audio::{decode_audio_bytes, read_audio_file, AudioInfo};
use crate::riva_client::{RivaClient, RivaClientConfig};

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1/";
const DEFAULT_ARCHIVE_QUEUE: &str = "teto_archive_jobs";
const DEFAULT_LIVE_CHANNEL: &str = "teto_audio_live";
const DEFAULT_TRANSCRIPTION_STREAM: &str = "teto_transcription_stream";
const DEFAULT_TRANSCRIPTION_FIELD: &str = "segment";
const DEFAULT_FILE_CHUNK_BYTES: usize = 32 * 1024;
const DEFAULT_RIVA_BITS_PER_SAMPLE: u16 = 16;
const DEFAULT_RIVA_CHANNELS: u16 = 1;

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
            let Some(path) = brpop_payload(
                &mut connection,
                &self.config.archive_queue,
                "archive jobs queue",
            )
            .await?
            else {
                continue;
            };

            self.process_archive_job(&mut connection, path).await?;
        }
    }

    async fn process_archive_job(
        &self,
        connection: &mut ConnectionManager,
        path: String,
    ) -> Result<()> {
        let path = PathBuf::from(path.trim());
        let session_id = Uuid::new_v4().to_string();
        let audio_info = self.riva_audio_info();
        let decoded_audio = read_audio_file(&path, audio_info)
            .await
            .with_context(|| format!("failed to decode archive audio '{}'", path.display()))?;

        info!(
            session_id = %session_id,
            path = %path.display(),
            decoded_bytes = decoded_audio.samples.len(),
            sample_rate_hz = decoded_audio.info.sample_rate_hz,
            channels = decoded_audio.info.channels,
            bits_per_sample = decoded_audio.info.bits_per_sample,
            chunk_bytes = self.config.file_chunk_bytes,
            "processing archive audio job"
        );

        let chunks = chunk_bytes(&decoded_audio.samples, self.config.file_chunk_bytes);
        let mut segments = self
            .riva
            .stream_chunks_to_riva(session_id.clone(), tokio_stream::iter(chunks))
            .await
            .with_context(|| format!("Riva transcription failed for '{}'", path.display()))?;

        bridge_audio_fingerprints(&mut segments, &decoded_audio.samples, decoded_audio.info);
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
        let audio_info = self.riva_audio_info();
        let mut audio_timeline = AudioTimeline::new();

        info!(
            session_id = %session_id,
            channel = %self.config.live_channel,
            "live audio stream opened"
        );

        while let Some(message) = messages.next().await {
            let chunk: Vec<u8> = message
                .get_payload()
                .context("failed to decode live audio Pub/Sub payload as bytes")?;

            audio_timeline.push(chunk.clone(), audio_info);
            stream.send_chunk(chunk).await?;

            let mut published = 0usize;
            while let Some(segment) = stream.recv_next().await? {
                let mut segment = segment;
                bridge_audio_fingerprint_from_timeline(&mut segment, &audio_timeline);
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
        bridge_audio_fingerprints_from_timeline(&mut segments, &audio_timeline);

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

    fn riva_audio_info(&self) -> AudioInfo {
        AudioInfo {
            sample_rate_hz: self.riva.config().sample_rate_hertz as u32,
            channels: DEFAULT_RIVA_CHANNELS,
            bits_per_sample: DEFAULT_RIVA_BITS_PER_SAMPLE,
        }
    }
}

async fn brpop_payload(
    connection: &mut ConnectionManager,
    queue: &str,
    queue_label: &str,
) -> Result<Option<String>> {
    let response: Option<Vec<String>> = redis::cmd("BRPOP")
        .arg(queue)
        .arg(0)
        .query_async(connection)
        .await
        .with_context(|| format!("failed to read {queue_label} '{queue}'"))?;

    Ok(response.and_then(|mut values| values.into_iter().nth(1)))
}

#[derive(Debug, Clone)]
struct AudioTimeline {
    entries: Vec<AudioTimelineEntry>,
    offset_ms: u64,
}

impl AudioTimeline {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            offset_ms: 0,
        }
    }

    fn push(&mut self, samples: Vec<u8>, info: AudioInfo) -> u64 {
        let start_ms = self.offset_ms;
        let duration_ms = chunk_duration_ms(samples.len(), info).max(1);
        self.entries.push(AudioTimelineEntry {
            start_ms,
            duration_ms,
            samples,
            info,
        });
        self.offset_ms = self.offset_ms.saturating_add(duration_ms);
        start_ms
    }

    fn audio_for_range(&self, start_ms: u64, end_ms: u64) -> Option<Vec<u8>> {
        if end_ms <= start_ms || self.entries.is_empty() {
            return None;
        }

        let mut out = Vec::new();
        for entry in &self.entries {
            let entry_end_ms = entry.start_ms.saturating_add(entry.duration_ms);
            if entry_end_ms <= start_ms || entry.start_ms >= end_ms {
                continue;
            }

            let slice_start =
                sample_index_for_ms(start_ms.saturating_sub(entry.start_ms), entry.info());
            let slice_end =
                sample_index_for_ms(end_ms.saturating_sub(entry.start_ms), entry.info());

            if slice_start < slice_end && slice_end <= entry.samples.len() {
                out.extend_from_slice(&entry.samples[slice_start..slice_end]);
            }
        }

        (!out.is_empty()).then_some(out)
    }
}

#[derive(Debug, Clone)]
struct AudioTimelineEntry {
    start_ms: u64,
    duration_ms: u64,
    samples: Vec<u8>,
    info: AudioInfo,
}

impl AudioTimelineEntry {
    fn info(&self) -> AudioInfo {
        self.info
    }
}

fn bridge_audio_fingerprints(segments: &mut [TranscriptionSegment], audio: &[u8], info: AudioInfo) {
    for segment in segments {
        if let Some(slice) =
            audio_slice_for_time_range(audio, segment.start_ms, segment.end_ms, info)
        {
            segment.voice_fingerprint = Some(acoustic_fingerprint(slice));
        } else {
            ensure_speaker_fallback_fingerprint(segment);
        }
    }
}

fn bridge_audio_fingerprints_from_timeline(
    segments: &mut [TranscriptionSegment],
    timeline: &AudioTimeline,
) {
    for segment in segments {
        if let Some(slice) = timeline.audio_for_range(segment.start_ms, segment.end_ms) {
            segment.voice_fingerprint = Some(acoustic_fingerprint(&slice));
        } else {
            ensure_speaker_fallback_fingerprint(segment);
        }
    }
}

fn bridge_audio_fingerprint_from_timeline(
    segment: &mut TranscriptionSegment,
    timeline: &AudioTimeline,
) {
    if segment.voice_fingerprint.is_some() {
        return;
    }

    if let Some(slice) = timeline.audio_for_range(segment.start_ms, segment.end_ms) {
        segment.voice_fingerprint = Some(acoustic_fingerprint(&slice));
    } else {
        segment.voice_fingerprint = Some(speaker_fallback_fingerprint(&segment.speaker_tag));
    }
}

fn ensure_speaker_fallback_fingerprint(segment: &mut TranscriptionSegment) {
    if segment.voice_fingerprint.is_none() {
        segment.voice_fingerprint = Some(speaker_fallback_fingerprint(&segment.speaker_tag));
    }
}

fn audio_slice_for_time_range(
    audio: &[u8],
    start_ms: u64,
    end_ms: u64,
    info: AudioInfo,
) -> Option<Vec<u8>> {
    if end_ms <= start_ms {
        return None;
    }

    let start = sample_index_for_ms(start_ms, info);
    let end = sample_index_for_ms(end_ms, info).min(audio.len());

    (start < end).then(|| audio[start..end].to_vec())
}

fn sample_index_for_ms(ms: u64, info: AudioInfo) -> usize {
    let bytes_per_sample = (info.bits_per_sample / 8) as u128;
    let channels = info.channels as u128;
    let bytes_per_frame = bytes_per_sample * channels;
    let sample_rate = info.sample_rate_hz as u128;

    ((ms as u128 * sample_rate * bytes_per_frame) / 1_000).min(usize::MAX as u128) as usize
}

fn chunk_bytes(audio: &[u8], chunk_size: usize) -> Vec<Vec<u8>> {
    audio
        .chunks(chunk_size.max(1))
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn chunk_duration_ms(chunk_len: usize, info: AudioInfo) -> u64 {
    let bytes_per_sample = (info.bits_per_sample / 8) as u64;
    let channels = info.channels as u64;
    let bytes_per_second = info.sample_rate_hz as u64 * channels * bytes_per_sample;

    if bytes_per_second == 0 {
        return 0;
    }

    ((chunk_len as u64 * 1_000).div_ceil(bytes_per_second)).max(1)
}

fn acoustic_fingerprint(pcm_i16_mono: &[u8]) -> VoiceFingerprint {
    let samples = pcm_i16_samples(pcm_i16_mono);
    if samples.is_empty() {
        return speaker_fallback_fingerprint("Speaker_unknown");
    }

    const FRAMES: usize = 16;
    const BINS: usize = 12;

    let mut values = Vec::with_capacity(VOICE_FINGERPRINT_DIM);
    for frame_index in 0..FRAMES {
        let frame_start = frame_index * samples.len() / FRAMES;
        let frame_end = if frame_index + 1 == FRAMES {
            samples.len()
        } else {
            (frame_index + 1) * samples.len() / FRAMES
        };
        let frame = &samples[frame_start..frame_end];
        let frame_energy = frame
            .iter()
            .map(|sample| (*sample as f32).abs())
            .sum::<f32>()
            .max(1e-6);

        for bin_index in 0..BINS {
            let bin = bin_index as f32;
            let mut real = 0.0f32;
            let mut imag = 0.0f32;

            for (sample_index, sample) in frame.iter().enumerate() {
                let phase = 2.0 * std::f32::consts::PI * bin * sample_index as f32
                    / frame.len().max(1) as f32;
                real += *sample as f32 * phase.cos();
                imag += *sample as f32 * phase.sin();
            }

            values.push((real.hypot(imag) / frame_energy).min(10.0));
        }
    }

    normalize_vector(&mut values);
    VoiceFingerprint::new(values).expect("acoustic fingerprint has 192 dimensions")
}

fn pcm_i16_samples(bytes: &[u8]) -> Vec<i16> {
    bytes
        .chunks_exact(2)
        .map(|chunk| i16::from_le_bytes([chunk[0], chunk[1]]))
        .collect()
}

fn speaker_fallback_fingerprint(speaker_tag: &str) -> VoiceFingerprint {
    let seed = fnv1a64(speaker_tag.as_bytes());
    let mut values = Vec::with_capacity(VOICE_FINGERPRINT_DIM);

    for index in 0..VOICE_FINGERPRINT_DIM {
        let mixed = seed ^ (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let unit = ((mixed >> 11) as f32) / ((1u64 << 53) as f32);
        values.push(unit * 2.0 - 1.0);
    }

    normalize_vector(&mut values);
    VoiceFingerprint::new(values).expect("speaker fallback fingerprint has 192 dimensions")
}

fn normalize_vector(values: &mut [f32]) {
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in values {
            *value /= norm;
        }
    }
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
    fn chunk_bytes_splits_by_configured_size() {
        let chunks = chunk_bytes(&[0; 100], 32);

        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].len(), 32);
        assert_eq!(chunks[1].len(), 32);
        assert_eq!(chunks[2].len(), 32);
        assert_eq!(chunks[3].len(), 4);
    }

    #[test]
    fn speaker_fallback_fingerprint_is_deterministic_and_dimensional() {
        let first = speaker_fallback_fingerprint("Speaker_1");
        let second = speaker_fallback_fingerprint("Speaker_1");
        let other = speaker_fallback_fingerprint("Speaker_2");

        assert_eq!(first.as_slice().len(), VOICE_FINGERPRINT_DIM);
        assert_eq!(first, second);
        assert_ne!(first, other);
    }

    #[test]
    fn acoustic_fingerprint_is_deterministic_dimensional_and_content_sensitive() {
        let first = acoustic_fingerprint(&[0; 64]);
        let second = acoustic_fingerprint(&[0; 64]);
        let other = acoustic_fingerprint(&[0x7f; 64]);

        assert_eq!(first.as_slice().len(), VOICE_FINGERPRINT_DIM);
        assert_eq!(first, second);
        assert_ne!(first, other);
    }

    #[test]
    fn bridge_audio_fingerprints_uses_audio_slice() {
        let audio = vec![0u8; 16_000 * 2];
        let mut segments = vec![
            TranscriptionSegment::new("session", "Speaker_1", "first", 0, 100),
            TranscriptionSegment::new("session", "Speaker_2", "second", 100, 200),
        ];
        let info = AudioInfo {
            sample_rate_hz: 16_000,
            channels: 1,
            bits_per_sample: 16,
        };

        bridge_audio_fingerprints(&mut segments, &audio, info);

        assert!(segments[0].voice_fingerprint.is_some());
        assert!(segments[1].voice_fingerprint.is_some());
        assert_ne!(
            segments[0].voice_fingerprint.as_ref().unwrap(),
            segments[1].voice_fingerprint.as_ref().unwrap()
        );
    }

    #[tokio::test]
    async fn audio_timeline_tracks_chunk_offsets() {
        let mut timeline = AudioTimeline::new();
        let info = AudioInfo {
            sample_rate_hz: 16_000,
            channels: 1,
            bits_per_sample: 16,
        };

        assert_eq!(timeline.push(vec![0; 3_200], info), 0);
        assert_eq!(timeline.push(vec![0; 3_200], info), 100);
        assert_eq!(
            timeline.audio_for_range(50, 150).map(|bytes| bytes.len()),
            Some(3_200)
        );
    }

    #[test]
    fn fnv1a64_changes_with_input() {
        assert_ne!(fnv1a64(b"Speaker_1"), fnv1a64(b"Speaker_2"));
    }
}
