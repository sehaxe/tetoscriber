use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

pub const VOICE_FINGERPRINT_DIM: usize = 192;
pub const DEFAULT_VOICE_MATCH_THRESHOLD: f32 = 0.85;
pub const DEFAULT_BRAIN_CONFIDENCE_THRESHOLD: f32 = 0.80;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TranscriptionSegment {
    pub session_id: String,
    pub speaker_tag: String,
    pub text: String,
    pub start_ms: u64,
    pub end_ms: u64,
    #[serde(default)]
    pub is_final: bool,
    pub voice_fingerprint: Option<VoiceFingerprint>,
    pub identified_name: Option<String>,
}

impl TranscriptionSegment {
    pub fn new(
        session_id: impl Into<String>,
        speaker_tag: impl Into<String>,
        text: impl Into<String>,
        start_ms: u64,
        end_ms: u64,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            speaker_tag: speaker_tag.into(),
            text: text.into(),
            start_ms,
            end_ms,
            is_final: false,
            voice_fingerprint: None,
            identified_name: None,
        }
    }

    pub fn with_finality(mut self, is_final: bool) -> Self {
        self.is_final = is_final;
        self
    }

    pub fn with_voice_fingerprint(mut self, fingerprint: VoiceFingerprint) -> Self {
        self.voice_fingerprint = Some(fingerprint);
        self
    }

    pub fn with_identified_name(mut self, name: impl Into<String>) -> Self {
        self.identified_name = Some(name.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VoiceFingerprint(Vec<f32>);

impl VoiceFingerprint {
    pub fn new(values: Vec<f32>) -> Result<Self, FingerprintError> {
        if values.len() != VOICE_FINGERPRINT_DIM {
            return Err(FingerprintError::InvalidDimension {
                expected: VOICE_FINGERPRINT_DIM,
                actual: values.len(),
            });
        }

        Ok(Self(values))
    }

    pub fn as_slice(&self) -> &[f32] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<f32> {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FingerprintError {
    InvalidDimension { expected: usize, actual: usize },
}

impl fmt::Display for FingerprintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDimension { expected, actual } => {
                write!(
                    f,
                    "invalid voice fingerprint dimension: expected {expected}, got {actual}"
                )
            }
        }
    }
}

impl std::error::Error for FingerprintError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TranscriptionBatch {
    pub session_id: String,
    pub segments: Vec<TranscriptionSegment>,
    pub speaker_metadata: BTreeMap<String, SpeakerMetadata>,
}

impl TranscriptionBatch {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            segments: Vec::new(),
            speaker_metadata: BTreeMap::new(),
        }
    }

    pub fn push_segment(&mut self, segment: TranscriptionSegment) {
        self.segments.push(segment);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpeakerMetadata {
    pub voice_vector: Option<VoiceFingerprint>,
    pub resolved_name: Option<String>,
}

impl SpeakerMetadata {
    pub fn unknown() -> Self {
        Self {
            voice_vector: None,
            resolved_name: None,
        }
    }

    pub fn with_voice_vector(mut self, voice_vector: VoiceFingerprint) -> Self {
        self.voice_vector = Some(voice_vector);
        self
    }

    pub fn with_resolved_name(mut self, resolved_name: impl Into<String>) -> Self {
        self.resolved_name = Some(resolved_name.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrainIdentityRequest {
    pub session_id: String,
    pub speaker_tags: Vec<String>,
    pub transcript: String,
}

impl BrainIdentityRequest {
    pub fn new(
        session_id: impl Into<String>,
        speaker_tags: Vec<String>,
        transcript: impl Into<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            speaker_tags,
            transcript: transcript.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrainIdentityResponse {
    pub session_id: String,
    pub identities: BTreeMap<String, String>,
}

impl BrainIdentityResponse {
    pub fn new(session_id: impl Into<String>, identities: BTreeMap<String, String>) -> Self {
        Self {
            session_id: session_id.into(),
            identities,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdentityResolution {
    pub speaker_tag: String,
    pub name: String,
    pub confidence: f32,
    pub source: IdentitySource,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdentityResolved {
    #[serde(rename = "type")]
    pub event_type: String,
    pub session_id: String,
    pub speaker_tag: String,
    pub name: String,
    pub confidence: f32,
    pub source: IdentitySource,
}

impl IdentityResolved {
    pub fn new(
        session_id: impl Into<String>,
        speaker_tag: impl Into<String>,
        name: impl Into<String>,
        confidence: f32,
        source: IdentitySource,
    ) -> Self {
        Self {
            event_type: "IdentityResolved".to_owned(),
            session_id: session_id.into(),
            speaker_tag: speaker_tag.into(),
            name: name.into(),
            confidence,
            source,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentitySource {
    VoiceMatch,
    BrainReasoning,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VoiceIdentityUpsert {
    pub session_id: String,
    pub speaker_tag: String,
    pub name: String,
    pub fingerprint: VoiceFingerprint,
    pub confidence: f32,
    pub source: IdentitySource,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TranscriptEmbedding {
    pub session_id: String,
    pub text: String,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RedisTask<T> {
    TranscribeOffline { payload: T },
    TranscribeLive { payload: T },
    ResolveIdentities { payload: T },
    IndexTranscript { payload: T },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryCollections {
    pub voices: String,
    pub transcripts: String,
}

impl Default for MemoryCollections {
    fn default() -> Self {
        Self {
            voices: "teto_voices".to_owned(),
            transcripts: "teto_history".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn voice_fingerprint_rejects_wrong_dimension() {
        let err = VoiceFingerprint::new(vec![0.0; 3]).unwrap_err();

        assert_eq!(
            err.to_string(),
            "invalid voice fingerprint dimension: expected 192, got 3"
        );
    }

    #[test]
    fn voice_fingerprint_serializes_as_plain_array() {
        let fingerprint = VoiceFingerprint::new(vec![0.25; VOICE_FINGERPRINT_DIM]).unwrap();
        let json = serde_json::to_string(&fingerprint).unwrap();

        assert_eq!(
            json,
            format!("[{}]", vec!["0.25"; VOICE_FINGERPRINT_DIM].join(","))
        );
    }

    #[test]
    fn transcription_segment_round_trips_through_json() {
        let segment = TranscriptionSegment::new("session", "Speaker_1", "Hello Nick.", 0, 2500)
            .with_finality(true)
            .with_identified_name("Nick");
        let json = serde_json::to_string(&segment).unwrap();
        let decoded: TranscriptionSegment = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, segment);
    }

    #[test]
    fn transcription_segment_defaults_to_non_final_for_backward_compatibility() {
        let json = r#"{"session_id":"session","speaker_tag":"Speaker_1","text":"Hello","start_ms":0,"end_ms":100}"#;
        let decoded: TranscriptionSegment = serde_json::from_str(json).unwrap();

        assert!(!decoded.is_final);
    }
}
