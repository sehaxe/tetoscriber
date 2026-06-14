use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use teto_protocol::{TranscriptionSegment, VoiceFingerprint};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tonic::Request;
use tracing::warn;

use crate::riva::asr::riva_speech_recognition_client::RivaSpeechRecognitionClient;
use crate::riva::asr::streaming_recognize_request::StreamingRequest;
use crate::riva::asr::{
    StreamingRecognitionConfig, StreamingRecognitionResult, StreamingRecognizeRequest, WordInfo,
};
use crate::riva::{
    asr::{RecognitionConfig, SpeakerDiarizationConfig},
    AudioEncoding,
};

const DEFAULT_RIVA_ASR_ENDPOINT: &str = "http://127.0.0.1:50051";
const DEFAULT_RIVA_LANGUAGE_CODE: &str = "en-US";
const DEFAULT_RIVA_SAMPLE_RATE_HZ: i32 = 16_000;
const DEFAULT_RIVA_CHUNK_DURATION_MS: u64 = 100;
const DEFAULT_RIVA_MAX_SPEAKER_COUNT: i32 = 2;

#[derive(Debug, Clone)]
pub struct RivaClientConfig {
    pub asr_endpoint: String,
    pub language_code: String,
    pub sample_rate_hertz: i32,
    pub encoding: AudioEncoding,
    pub chunk_duration: Duration,
    pub enable_interim_results: bool,
    pub max_speaker_count: i32,
    pub model_name: String,
    pub custom_configuration: Vec<(String, String)>,
}

impl RivaClientConfig {
    pub fn from_env() -> Self {
        Self {
            asr_endpoint: env_or_default("RIVA_ASR_ENDPOINT", DEFAULT_RIVA_ASR_ENDPOINT),
            language_code: env_or_default("RIVA_LANGUAGE_CODE", DEFAULT_RIVA_LANGUAGE_CODE),
            sample_rate_hertz: env_i32("RIVA_SAMPLE_RATE_HZ", DEFAULT_RIVA_SAMPLE_RATE_HZ),
            encoding: AudioEncoding::LinearPcm,
            chunk_duration: Duration::from_millis(env_u64(
                "RIVA_CHUNK_DURATION_MS",
                DEFAULT_RIVA_CHUNK_DURATION_MS,
            )),
            enable_interim_results: env_bool("RIVA_INTERIM_RESULTS", true),
            max_speaker_count: env_i32("RIVA_MAX_SPEAKER_COUNT", DEFAULT_RIVA_MAX_SPEAKER_COUNT),
            model_name: env_or_default("RIVA_ASR_MODEL", ""),
            custom_configuration: parse_custom_configuration(
                &std::env::var("RIVA_CUSTOM_CONFIGURATION").unwrap_or_default(),
            ),
        }
    }
}

impl Default for RivaClientConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

#[derive(Debug)]
pub struct RivaStreamingSession {
    session_id: String,
    sender: mpsc::Sender<Vec<u8>>,
    response: tonic::Streaming<crate::riva::asr::StreamingRecognizeResponse>,
}

impl RivaStreamingSession {
    pub async fn send_chunk(&mut self, chunk: Vec<u8>) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }

        self.sender.send(chunk).await.with_context(|| {
            format!(
                "failed to send audio chunk for session '{}'",
                self.session_id
            )
        })
    }

    pub async fn send_stream<S>(&mut self, chunks: S) -> Result<()>
    where
        S: Stream<Item = Vec<u8>> + Send,
    {
        let mut chunks = Box::pin(chunks);
        while let Some(chunk) = chunks.next().await {
            self.send_chunk(chunk).await?;
        }

        Ok(())
    }

    pub async fn recv_next(&mut self) -> Result<Option<TranscriptionSegment>> {
        while let Some(message) = self.response.message().await.with_context(|| {
            format!(
                "Riva streaming ASR response failed for session '{}'",
                self.session_id
            )
        })? {
            for result in message.results {
                if let Some(segment) = result_to_segment(&self.session_id, result) {
                    return Ok(Some(segment));
                }
            }
        }

        Ok(None)
    }

    pub async fn close(mut self) -> Result<Vec<TranscriptionSegment>> {
        drop(self.sender);
        let mut segments = Vec::new();

        while let Some(message) = self.response.message().await.with_context(|| {
            format!(
                "Riva streaming ASR response failed while closing session '{}'",
                self.session_id
            )
        })? {
            for result in message.results {
                if let Some(segment) = result_to_segment(&self.session_id, result) {
                    segments.push(segment);
                }
            }
        }

        Ok(segments)
    }
}

#[derive(Debug, Clone)]
pub struct RivaClient {
    config: RivaClientConfig,
}

impl RivaClient {
    pub fn new(config: RivaClientConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &RivaClientConfig {
        &self.config
    }

    pub async fn open_streaming_session(
        &self,
        session_id: impl Into<String>,
    ) -> Result<RivaStreamingSession> {
        let session_id = session_id.into();
        let mut client = RivaSpeechRecognitionClient::connect(self.config.asr_endpoint.clone())
            .await
            .with_context(|| {
                format!(
                    "failed to connect to Riva ASR endpoint '{}'",
                    self.config.asr_endpoint
                )
            })?;

        let (sender, receiver) = mpsc::channel(128);
        let requests =
            self.streaming_requests(tokio_stream::wrappers::ReceiverStream::new(receiver));
        let response = client
            .streaming_recognize(Request::new(requests))
            .await
            .with_context(|| {
                format!(
                    "failed to open Riva streaming ASR request for session '{}'",
                    session_id
                )
            })?
            .into_inner();

        Ok(RivaStreamingSession {
            session_id,
            sender,
            response,
        })
    }

    pub async fn stream_chunks_to_riva<S>(
        &self,
        session_id: impl Into<String>,
        chunks: S,
    ) -> Result<Vec<TranscriptionSegment>>
    where
        S: Stream<Item = Vec<u8>> + Send + 'static,
    {
        let mut session = self.open_streaming_session(session_id).await?;
        session.send_stream(chunks).await?;
        session.close().await
    }

    pub async fn stream_audio_to_riva(
        &self,
        session_id: impl Into<String>,
        audio: Vec<u8>,
    ) -> Result<Vec<TranscriptionSegment>> {
        if audio.is_empty() {
            bail!("cannot stream empty audio payload to Riva");
        }

        let chunks = audio_chunks(
            &audio,
            self.config.chunk_duration,
            self.config.sample_rate_hertz,
        );
        self.stream_chunks_to_riva(session_id, tokio_stream::iter(chunks))
            .await
    }

    pub async fn extract_voice_fingerprint(
        &self,
        _audio: &[u8],
    ) -> Result<Option<VoiceFingerprint>> {
        warn!(
            "Riva Speaker Recognition proto is not configured; voice_fingerprint will remain None until RIVA_SPEAKER_PROTO_FILES or a speaker-recognition client is added"
        );
        Ok(None)
    }

    fn streaming_requests<S>(
        &self,
        chunks: S,
    ) -> impl Stream<Item = StreamingRecognizeRequest> + Send + 'static
    where
        S: Stream<Item = Vec<u8>> + Send + 'static,
    {
        let config_request = StreamingRecognizeRequest {
            runtime_config: Default::default(),
            id: None,
            streaming_request: Some(StreamingRequest::StreamingConfig(
                StreamingRecognitionConfig {
                    config: Some(self.recognition_config()),
                    interim_results: self.config.enable_interim_results,
                },
            )),
        };

        let audio_requests = chunks.map(|chunk| StreamingRecognizeRequest {
            runtime_config: Default::default(),
            id: None,
            streaming_request: Some(StreamingRequest::AudioContent(chunk)),
        });

        tokio_stream::once(config_request).chain(audio_requests)
    }

    fn recognition_config(&self) -> RecognitionConfig {
        RecognitionConfig {
            encoding: self.config.encoding as i32,
            sample_rate_hertz: self.config.sample_rate_hertz,
            language_code: self.config.language_code.clone(),
            max_alternatives: 1,
            profanity_filter: false,
            speech_contexts: Vec::new(),
            audio_channel_count: 1,
            enable_word_time_offsets: true,
            enable_automatic_punctuation: true,
            enable_separate_recognition_per_channel: false,
            model: self.config.model_name.clone(),
            verbatim_transcripts: false,
            diarization_config: Some(SpeakerDiarizationConfig {
                enable_speaker_diarization: true,
                max_speaker_count: self.config.max_speaker_count,
            }),
            custom_configuration: self
                .config
                .custom_configuration
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<HashMap<_, _>>(),
            endpointing_config: None,
        }
    }
}

fn result_to_segment(
    session_id: &str,
    result: StreamingRecognitionResult,
) -> Option<TranscriptionSegment> {
    if !result.is_final {
        return None;
    }

    let alternative = result.alternatives.into_iter().max_by(|left, right| {
        left.confidence
            .partial_cmp(&right.confidence)
            .unwrap_or(Ordering::Equal)
    })?;

    let transcript = alternative.transcript.trim();
    if transcript.is_empty() {
        return None;
    }

    let speaker_tag = speaker_tag_from_words(&alternative.words);
    let (start_ms, end_ms) = time_range_from_words(&alternative.words, result.audio_processed);

    Some(
        TranscriptionSegment::new(session_id, speaker_tag, transcript, start_ms, end_ms)
            .with_finality(true),
    )
}

fn speaker_tag_from_words(words: &[WordInfo]) -> String {
    let speaker_tag = words
        .iter()
        .filter_map(|word| (word.speaker_tag > 0).then_some(word.speaker_tag))
        .max()
        .unwrap_or(0);

    if speaker_tag > 0 {
        format!("Speaker_{speaker_tag}")
    } else {
        "Speaker_unknown".to_owned()
    }
}

fn time_range_from_words(words: &[WordInfo], audio_processed: f32) -> (u64, u64) {
    let start_ms = words
        .iter()
        .filter_map(|word| (word.start_time >= 0).then_some(word.start_time as u64))
        .min()
        .unwrap_or(0);

    let end_ms = words
        .iter()
        .filter_map(|word| (word.end_time >= 0).then_some(word.end_time as u64))
        .max()
        .unwrap_or_else(|| audio_processed_ms(audio_processed));

    (start_ms, end_ms.max(start_ms))
}

fn audio_processed_ms(audio_processed: f32) -> u64 {
    (audio_processed.max(0.0) * 1_000.0).round() as u64
}

fn audio_chunks(audio: &[u8], chunk_duration: Duration, sample_rate_hertz: i32) -> Vec<Vec<u8>> {
    if audio.is_empty() || chunk_duration.is_zero() || sample_rate_hertz <= 0 {
        return Vec::new();
    }

    let bytes_per_ms = ((sample_rate_hertz as u64 * 2).max(1))
        .div_ceil(1_000)
        .max(1);
    let chunk_size = (chunk_duration.as_millis() as usize * bytes_per_ms as usize).max(1);

    audio
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn parse_custom_configuration(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .filter_map(|entry| entry.split_once('='))
        .map(|(key, value)| (key.trim().to_owned(), value.trim().to_owned()))
        .collect()
}

fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn env_i32(key: &str, default: i32) -> i32 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
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

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audio_chunks_split_linear_pcm_by_configured_duration() {
        let audio = vec![0u8; 16_000 * 2];
        let chunks = audio_chunks(&audio, Duration::from_millis(100), 16_000);

        assert_eq!(chunks.len(), 10);
        assert!(chunks.iter().all(|chunk| chunk.len() == 3_200));
    }

    #[test]
    fn parse_custom_configuration_ignores_invalid_entries() {
        assert_eq!(
            parse_custom_configuration("task=transcribe, invalid, model=asr"),
            vec![
                ("task".to_owned(), "transcribe".to_owned()),
                ("model".to_owned(), "asr".to_owned()),
            ]
        );
    }
}
