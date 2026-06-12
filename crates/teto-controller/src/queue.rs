use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use redis::aio::ConnectionManager;
use redis::streams::{StreamId, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client, ErrorKind, ServerErrorKind};
use teto_protocol::{
    BrainIdentityRequest, BrainIdentityResponse, IdentityResolved, IdentitySource,
    TranscriptionSegment, VoiceFingerprint, VoiceIdentityUpsert,
    DEFAULT_BRAIN_CONFIDENCE_THRESHOLD,
};
use tokio::sync::{broadcast, Mutex};
use tracing::{debug, info, warn};

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1/";
const DEFAULT_TRANSCRIPTION_STREAM: &str = "teto_transcription_stream";
const DEFAULT_CONTROLLER_GROUP: &str = "teto_controller";
const DEFAULT_CONTROLLER_CONSUMER: &str = "teto_controller";
const DEFAULT_BRAIN_QUEUE: &str = "teto_brain_queue";
const DEFAULT_BRAIN_RESOLUTIONS_QUEUE: &str = "brain_resolutions";
const DEFAULT_LIVE_CHANNEL: &str = "teto_live_transcripts";
const DEFAULT_STREAM_BLOCK_MS: usize = 5_000;
const DEFAULT_STREAM_BATCH_SIZE: usize = 10;
const DEFAULT_BRAIN_REQUEST_SEGMENTS: usize = 5;
const DEFAULT_CONTROLLER_ADDR: &str = "127.0.0.1:3030";
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub redis_url: String,
    pub transcription_stream: String,
    pub controller_group: String,
    pub controller_consumer: String,
    pub brain_queue: String,
    pub brain_resolutions_queue: String,
    pub live_channel: String,
    pub stream_block_ms: usize,
    pub stream_batch_size: usize,
    pub brain_request_segments: usize,
    pub controller_addr: SocketAddr,
}

impl QueueConfig {
    pub fn from_env() -> Self {
        Self {
            redis_url: env_or_default("REDIS_URL", DEFAULT_REDIS_URL),
            transcription_stream: env_or_default(
                "TETO_TRANSCRIPTION_STREAM",
                DEFAULT_TRANSCRIPTION_STREAM,
            ),
            controller_group: env_or_default("TETO_CONTROLLER_GROUP", DEFAULT_CONTROLLER_GROUP),
            controller_consumer: env_or_default(
                "TETO_CONTROLLER_CONSUMER",
                DEFAULT_CONTROLLER_CONSUMER,
            ),
            brain_queue: env_or_default("TETO_BRAIN_QUEUE", DEFAULT_BRAIN_QUEUE),
            brain_resolutions_queue: env_or_default(
                "TETO_BRAIN_RESOLUTIONS_QUEUE",
                DEFAULT_BRAIN_RESOLUTIONS_QUEUE,
            ),
            live_channel: env_or_default("TETO_LIVE_CHANNEL", DEFAULT_LIVE_CHANNEL),
            stream_block_ms: env_usize("TETO_STREAM_BLOCK_MS", DEFAULT_STREAM_BLOCK_MS),
            stream_batch_size: env_usize("TETO_STREAM_BATCH_SIZE", DEFAULT_STREAM_BATCH_SIZE),
            brain_request_segments: env_usize(
                "TETO_BRAIN_REQUEST_SEGMENTS",
                DEFAULT_BRAIN_REQUEST_SEGMENTS,
            ),
            controller_addr: env_socket_addr("TETO_CONTROLLER_ADDR", DEFAULT_CONTROLLER_ADDR),
        }
    }
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

#[derive(Debug, Clone)]
pub struct PendingSession {
    transcript: String,
    segments: Vec<TranscriptionSegment>,
    fingerprints: BTreeMap<String, VoiceFingerprint>,
    brain_requested: bool,
}

pub type PendingIdentityBuffer = Arc<Mutex<HashMap<String, PendingSession>>>;
pub type SharedIdentityStorage = Arc<dyn crate::storage::IdentityStorage>;

pub struct StreamListener {
    config: QueueConfig,
    live_tx: broadcast::Sender<TranscriptionSegment>,
    memory: Option<SharedIdentityStorage>,
    pending: PendingIdentityBuffer,
}

impl StreamListener {
    pub fn new_pending() -> PendingIdentityBuffer {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[allow(dead_code)]
    pub fn new(
        config: QueueConfig,
        live_tx: broadcast::Sender<TranscriptionSegment>,
        memory: Option<SharedIdentityStorage>,
    ) -> Self {
        Self::with_pending(config, live_tx, memory, Self::new_pending())
    }

    pub fn with_pending(
        config: QueueConfig,
        live_tx: broadcast::Sender<TranscriptionSegment>,
        memory: Option<SharedIdentityStorage>,
        pending: PendingIdentityBuffer,
    ) -> Self {
        Self {
            config,
            live_tx,
            memory,
            pending,
        }
    }

    pub async fn listen_transcription_stream(&self) {
        loop {
            match self.run_connected().await {
                Ok(()) => {}
                Err(error) => {
                    warn!(%error, "Redis transcription stream listener failed; reconnecting");
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
            }
        }
    }

    pub async fn dispatch_brain_request(&self, request: &BrainIdentityRequest) -> Result<()> {
        let mut connection = self.connect().await?;
        let payload = serde_json::to_string(request).context("failed to encode brain request")?;
        let _: usize = connection
            .rpush(&self.config.brain_queue, payload)
            .await
            .with_context(|| {
                format!(
                    "failed to push brain request to '{}'",
                    self.config.brain_queue
                )
            })?;

        info!(
            queue = %self.config.brain_queue,
            session_id = %request.session_id,
            speaker_count = request.speaker_tags.len(),
            "dispatched brain identity request"
        );

        Ok(())
    }

    pub async fn emit_final_transcript(&self, segment: &TranscriptionSegment) -> Result<()> {
        let mut connection = self.connect().await?;
        self.emit_final_transcript_to_connection(&mut connection, segment)
            .await
    }

    async fn run_connected(&self) -> Result<()> {
        let mut connection = self.connect().await?;
        self.ensure_stream_group(&mut connection).await?;

        loop {
            let start_id = pending_start_id(&mut connection, &self.config).await?;
            let reply = self.read_stream_from(&mut connection, start_id).await?;
            for stream in reply.keys {
                for id in stream.ids {
                    self.handle_stream_id(&mut connection, id).await?;
                }
            }
        }
    }

    async fn read_stream_from(
        &self,
        connection: &mut ConnectionManager,
        start_id: &str,
    ) -> Result<StreamReadReply> {
        let options = StreamReadOptions::default()
            .group(
                self.config.controller_group.as_str(),
                self.config.controller_consumer.as_str(),
            )
            .block(self.config.stream_block_ms)
            .count(self.config.stream_batch_size);

        connection
            .xread_options(
                &[self.config.transcription_stream.as_str()],
                &[start_id],
                &options,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to read Redis stream '{}' from '{start_id}'",
                    self.config.transcription_stream
                )
            })
    }

    async fn handle_stream_id(
        &self,
        connection: &mut ConnectionManager,
        stream_id: StreamId,
    ) -> Result<()> {
        let segment = decode_segment(&stream_id)
            .with_context(|| format!("failed to decode stream id '{}'", stream_id.id))?;

        match self.process_segment(segment).await {
            Ok(()) => {
                acknowledge(connection, &self.config, &stream_id.id).await?;
            }
            Err(error) => {
                warn!(
                    stream_id = %stream_id.id,
                    %error,
                    "segment processing failed; leaving message pending for retry"
                );
                return Err(error);
            }
        }

        Ok(())
    }

    async fn process_segment(&self, mut segment: TranscriptionSegment) -> Result<()> {
        let Some(fingerprint) = segment.voice_fingerprint.clone() else {
            self.emit_final_transcript(&segment).await?;
            return Ok(());
        };

        let Some(memory) = &self.memory else {
            self.buffer_unknown_segment(segment).await?;
            return Ok(());
        };

        match memory.match_voice(&fingerprint).await? {
            Some(matched) => {
                segment.identified_name = Some(matched.name);
                self.emit_final_transcript(&segment).await?;
            }
            None => {
                self.buffer_unknown_segment(segment).await?;
            }
        }

        Ok(())
    }

    async fn buffer_unknown_segment(&self, segment: TranscriptionSegment) -> Result<()> {
        let mut pending = self.pending.lock().await;
        let entry = pending
            .entry(segment.session_id.clone())
            .or_insert_with(|| PendingSession {
                transcript: String::new(),
                segments: Vec::new(),
                fingerprints: BTreeMap::new(),
                brain_requested: false,
            });

        if let Some(fingerprint) = segment.voice_fingerprint.clone() {
            entry
                .fingerprints
                .entry(segment.speaker_tag.clone())
                .or_insert(fingerprint);
        }

        if !entry.transcript.is_empty() {
            entry.transcript.push('\n');
        }
        entry
            .transcript
            .push_str(&format!("{}: {}", segment.speaker_tag, segment.text));
        entry.segments.push(segment);

        if !entry.brain_requested
            && entry.segments.len() >= self.config.brain_request_segments
            && !entry.fingerprints.is_empty()
        {
            let speaker_tags = entry.fingerprints.keys().cloned().collect::<Vec<_>>();
            let request = BrainIdentityRequest::new(
                entry.segments[0].session_id.clone(),
                speaker_tags,
                entry.transcript.clone(),
            );

            self.dispatch_brain_request(&request).await?;
            entry.brain_requested = true;
        }

        Ok(())
    }

    async fn ensure_stream_group(&self, connection: &mut ConnectionManager) -> Result<()> {
        let result: redis::RedisResult<()> = connection
            .xgroup_create_mkstream(
                self.config.transcription_stream.as_str(),
                self.config.controller_group.as_str(),
                "0",
            )
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(error) if is_busy_group(&error) => Ok(()),
            Err(error) => Err(error).context("failed to create Redis stream consumer group"),
        }
    }

    async fn emit_final_transcript_to_connection(
        &self,
        connection: &mut ConnectionManager,
        segment: &TranscriptionSegment,
    ) -> Result<()> {
        let payload =
            serde_json::to_string(segment).context("failed to encode transcript segment")?;
        let _: usize = connection
            .publish(&self.config.live_channel, payload)
            .await
            .with_context(|| {
                format!(
                    "failed to publish live transcript to '{}'",
                    self.config.live_channel
                )
            })?;

        let _ = self.live_tx.send(segment.clone());
        debug!(
            session_id = %segment.session_id,
            speaker_tag = %segment.speaker_tag,
            channel = %self.config.live_channel,
            "emitted final transcript segment"
        );

        Ok(())
    }

    async fn connect(&self) -> Result<ConnectionManager> {
        let client = Client::open(self.config.redis_url.as_str())
            .with_context(|| format!("invalid Redis URL '{}'", self.config.redis_url))?;

        let connection =
            tokio::time::timeout(Duration::from_secs(5), client.get_connection_manager())
                .await
                .context("Redis connection manager creation timed out")??;

        Ok(connection)
    }
}

pub struct BrainResolutionListener {
    config: QueueConfig,
    live_tx: broadcast::Sender<TranscriptionSegment>,
    memory: Option<SharedIdentityStorage>,
    pending: PendingIdentityBuffer,
    identity_tx: broadcast::Sender<IdentityResolved>,
}

impl BrainResolutionListener {
    pub fn new(
        config: QueueConfig,
        live_tx: broadcast::Sender<TranscriptionSegment>,
        memory: Option<SharedIdentityStorage>,
        pending: PendingIdentityBuffer,
        identity_tx: broadcast::Sender<IdentityResolved>,
    ) -> Self {
        Self {
            config,
            live_tx,
            memory,
            pending,
            identity_tx,
        }
    }

    pub async fn listen_brain_resolutions(&self) {
        loop {
            match self.run_connected().await {
                Ok(()) => {}
                Err(error) => {
                    warn!(%error, "Redis brain resolution listener failed; reconnecting");
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
            }
        }
    }

    async fn run_connected(&self) -> Result<()> {
        let mut connection = self.connect().await?;

        loop {
            let payload: Option<String> = connection
                .brpop(&[self.config.brain_resolutions_queue.as_str()], 0.0)
                .await
                .with_context(|| {
                    format!(
                        "failed to read brain resolutions from '{}'",
                        self.config.brain_resolutions_queue
                    )
                })?;

            if let Some(payload) = payload {
                self.handle_resolution_payload(&mut connection, &payload)
                    .await?;
            }
        }
    }

    async fn handle_resolution_payload(
        &self,
        connection: &mut ConnectionManager,
        payload: &str,
    ) -> Result<()> {
        let response: BrainIdentityResponse = serde_json::from_str(payload)
            .with_context(|| format!("failed to decode brain resolution payload: {payload}"))?;

        let Some(mut pending) = self.pending.lock().await.remove(&response.session_id) else {
            warn!(
                session_id = %response.session_id,
                "brain resolution arrived after pending identity buffer was dropped"
            );
            return Ok(());
        };

        let Some(memory) = &self.memory else {
            warn!(
                session_id = %response.session_id,
                "brain resolution arrived while Qdrant memory is unavailable"
            );
            return Ok(());
        };

        let mut upserted = 0usize;
        let mut replayed = 0usize;
        for (speaker_tag, name) in response.identities {
            let Some(fingerprint) = pending.fingerprints.get(&speaker_tag) else {
                warn!(
                    session_id = %response.session_id,
                    %speaker_tag,
                    "brain returned identity for speaker without a stored fingerprint"
                );
                continue;
            };

            let upsert = VoiceIdentityUpsert {
                session_id: response.session_id.clone(),
                speaker_tag: speaker_tag.clone(),
                name: name.clone(),
                fingerprint: fingerprint.clone(),
                confidence: DEFAULT_BRAIN_CONFIDENCE_THRESHOLD,
                source: IdentitySource::BrainReasoning,
            };

            memory
                .upsert_voice_identity(&upsert)
                .await
                .with_context(|| {
                    format!(
                    "failed to store brain identity for speaker '{speaker_tag}' in session '{}'",
                    response.session_id
                )
                })?;
            upserted += 1;

            let event = IdentityResolved::new(
                response.session_id.clone(),
                speaker_tag.clone(),
                name.clone(),
                DEFAULT_BRAIN_CONFIDENCE_THRESHOLD,
                IdentitySource::BrainReasoning,
            );
            self.emit_identity_resolved_to_connection(connection, &event)
                .await?;

            replayed += self
                .replay_buffered_segments(connection, &mut pending, &speaker_tag, &name)
                .await?;
        }

        info!(
            session_id = %response.session_id,
            upserted,
            replayed,
            "applied brain identity resolutions"
        );

        Ok(())
    }

    async fn emit_final_transcript_to_connection(
        &self,
        connection: &mut ConnectionManager,
        segment: &TranscriptionSegment,
    ) -> Result<()> {
        let payload =
            serde_json::to_string(segment).context("failed to encode transcript segment")?;
        let _: usize = connection
            .publish(&self.config.live_channel, payload)
            .await
            .with_context(|| {
                format!(
                    "failed to publish live transcript to '{}'",
                    self.config.live_channel
                )
            })?;

        let _ = self.live_tx.send(segment.clone());
        debug!(
            session_id = %segment.session_id,
            speaker_tag = %segment.speaker_tag,
            channel = %self.config.live_channel,
            "replayed resolved transcript segment"
        );

        Ok(())
    }

    async fn replay_buffered_segments(
        &self,
        connection: &mut ConnectionManager,
        pending: &mut PendingSession,
        speaker_tag: &str,
        name: &str,
    ) -> Result<usize> {
        let mut replayed = 0usize;
        for segment in &mut pending.segments {
            if segment.speaker_tag == speaker_tag && segment.identified_name.is_none() {
                segment.identified_name = Some(name.to_owned());
                self.emit_final_transcript_to_connection(connection, segment)
                    .await?;
                replayed += 1;
            }
        }

        Ok(replayed)
    }

    async fn emit_identity_resolved_to_connection(
        &self,
        connection: &mut ConnectionManager,
        event: &IdentityResolved,
    ) -> Result<()> {
        let payload = serde_json::to_string(event).context("failed to encode identity event")?;
        let _: usize = connection
            .publish(&self.config.live_channel, payload)
            .await
            .with_context(|| {
                format!(
                    "failed to publish identity resolution to '{}'",
                    self.config.live_channel
                )
            })?;

        let _ = self.identity_tx.send(event.clone());
        debug!(
            session_id = %event.session_id,
            speaker_tag = %event.speaker_tag,
            name = %event.name,
            channel = %self.config.live_channel,
            "emitted identity resolved event"
        );

        Ok(())
    }

    async fn connect(&self) -> Result<ConnectionManager> {
        let client = Client::open(self.config.redis_url.as_str())
            .with_context(|| format!("invalid Redis URL '{}'", self.config.redis_url))?;

        let connection =
            tokio::time::timeout(Duration::from_secs(5), client.get_connection_manager())
                .await
                .context("Redis connection manager creation timed out")??;

        Ok(connection)
    }
}

#[derive(Clone)]
pub struct LiveState {
    live_tx: broadcast::Sender<TranscriptionSegment>,
    identity_tx: broadcast::Sender<IdentityResolved>,
}

impl LiveState {
    pub fn new(
        live_tx: broadcast::Sender<TranscriptionSegment>,
        identity_tx: broadcast::Sender<IdentityResolved>,
    ) -> Self {
        Self {
            live_tx,
            identity_tx,
        }
    }
}

pub async fn live_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<LiveState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| {
        handle_live_socket(
            socket,
            state.live_tx.subscribe(),
            state.identity_tx.subscribe(),
        )
    })
}

async fn handle_live_socket(
    mut socket: WebSocket,
    mut receiver: broadcast::Receiver<TranscriptionSegment>,
    mut identity_receiver: broadcast::Receiver<IdentityResolved>,
) {
    loop {
        tokio::select! {
            message = receiver.recv() => {
                match message {
                    Ok(segment) => {
                        let payload = match serde_json::to_string(&segment) {
                            Ok(payload) => payload,
                            Err(error) => {
                                warn!(%error, "failed to encode WebSocket transcript segment");
                                continue;
                            }
                        };

                        if socket.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            identity = identity_receiver.recv() => {
                match identity {
                    Ok(event) => {
                        let payload = match serde_json::to_string(&event) {
                            Ok(payload) => payload,
                            Err(error) => {
                                warn!(%error, "failed to encode WebSocket identity event");
                                continue;
                            }
                        };

                        if socket.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Text(_))) => {}
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        warn!(%error, "WebSocket live client read failed");
                        break;
                    }
                }
            }
        }
    }

    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: 1000,
            reason: "live transcript stream closed".into(),
        })))
        .await;
}

async fn pending_start_id(
    connection: &mut ConnectionManager,
    config: &QueueConfig,
) -> Result<&'static str> {
    let pending: redis::streams::StreamPendingReply = connection
        .xpending(
            config.transcription_stream.as_str(),
            config.controller_group.as_str(),
        )
        .await
        .with_context(|| {
            format!(
                "failed to inspect pending messages for stream '{}' group '{}'",
                config.transcription_stream, config.controller_group
            )
        })?;

    Ok(if pending.count() > 0 { "0" } else { ">" })
}

async fn acknowledge(
    connection: &mut ConnectionManager,
    config: &QueueConfig,
    stream_id: &str,
) -> Result<()> {
    let _: usize = connection
        .xack(
            config.transcription_stream.as_str(),
            config.controller_group.as_str(),
            &[stream_id],
        )
        .await
        .with_context(|| {
            format!(
                "failed to acknowledge Redis stream message '{stream_id}' in group '{}'",
                config.controller_group
            )
        })?;

    Ok(())
}

fn decode_segment(stream_id: &StreamId) -> Result<TranscriptionSegment> {
    for key in ["segment", "payload", "json"] {
        if let Some(raw) = stream_id.get::<String>(key) {
            return serde_json::from_str(&raw).with_context(|| {
                format!("failed to decode Redis field '{key}' as TranscriptionSegment")
            });
        }
    }

    let mut object = serde_json::Map::new();
    for (key, value) in &stream_id.map {
        object.insert(key.clone(), redis_value_to_json(value));
    }

    serde_json::from_value(serde_json::Value::Object(object))
        .context("failed to decode Redis stream fields as TranscriptionSegment")
}

fn redis_value_to_json(value: &redis::Value) -> serde_json::Value {
    match value {
        redis::Value::Nil => serde_json::Value::Null,
        redis::Value::Int(value) => serde_json::json!(value),
        redis::Value::BulkString(value) => String::from_utf8_lossy(value).into_owned().into(),
        redis::Value::SimpleString(value) => value.clone().into(),
        redis::Value::Okay => "OK".into(),
        redis::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(redis_value_to_json).collect())
        }
        redis::Value::Map(values) => {
            let mut object = serde_json::Map::new();
            for (key, value) in values {
                object.insert(
                    redis_value_to_json(key).to_string(),
                    redis_value_to_json(value),
                );
            }
            serde_json::Value::Object(object)
        }
        redis::Value::Attribute { data, .. } => redis_value_to_json(data),
        redis::Value::Set(values) => {
            serde_json::Value::Array(values.iter().map(redis_value_to_json).collect())
        }
        redis::Value::Double(value) => serde_json::json!(value),
        redis::Value::Boolean(value) => serde_json::json!(value),
        redis::Value::VerbatimString { text, .. } => text.clone().into(),
        redis::Value::BigNumber(value) => serde_json::json!(value),
        redis::Value::Push { data, .. } => {
            serde_json::Value::Array(data.iter().map(redis_value_to_json).collect())
        }
        redis::Value::ServerError(error) => serde_json::json!({ "error": error.to_string() }),
        _ => serde_json::Value::Null,
    }
}

fn is_busy_group(error: &redis::RedisError) -> bool {
    matches!(
        error.kind(),
        ErrorKind::Server(ServerErrorKind::ResponseError)
    ) && error.to_string().contains("BUSYGROUP")
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

fn env_socket_addr(key: &str, default: &str) -> SocketAddr {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<SocketAddr>().ok())
        .unwrap_or_else(|| {
            default
                .parse()
                .expect("default controller address must be valid")
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::Value;

    #[test]
    fn queue_config_uses_safe_defaults_when_env_is_missing() {
        std::env::remove_var("REDIS_URL");
        std::env::remove_var("TETO_TRANSCRIPTION_STREAM");
        std::env::remove_var("TETO_CONTROLLER_GROUP");
        std::env::remove_var("TETO_CONTROLLER_CONSUMER");
        std::env::remove_var("TETO_BRAIN_QUEUE");
        std::env::remove_var("TETO_BRAIN_RESOLUTIONS_QUEUE");
        std::env::remove_var("TETO_LIVE_CHANNEL");
        std::env::remove_var("TETO_STREAM_BLOCK_MS");
        std::env::remove_var("TETO_STREAM_BATCH_SIZE");
        std::env::remove_var("TETO_BRAIN_REQUEST_SEGMENTS");
        std::env::remove_var("TETO_CONTROLLER_ADDR");

        let config = QueueConfig::from_env();

        assert_eq!(config.redis_url, DEFAULT_REDIS_URL);
        assert_eq!(config.transcription_stream, DEFAULT_TRANSCRIPTION_STREAM);
        assert_eq!(config.controller_group, DEFAULT_CONTROLLER_GROUP);
        assert_eq!(config.controller_consumer, DEFAULT_CONTROLLER_CONSUMER);
        assert_eq!(config.brain_queue, DEFAULT_BRAIN_QUEUE);
        assert_eq!(
            config.brain_resolutions_queue,
            DEFAULT_BRAIN_RESOLUTIONS_QUEUE
        );
        assert_eq!(config.live_channel, DEFAULT_LIVE_CHANNEL);
        assert_eq!(config.stream_block_ms, DEFAULT_STREAM_BLOCK_MS);
        assert_eq!(config.stream_batch_size, DEFAULT_STREAM_BATCH_SIZE);
        assert_eq!(
            config.brain_request_segments,
            DEFAULT_BRAIN_REQUEST_SEGMENTS
        );
        assert_eq!(
            config.controller_addr,
            DEFAULT_CONTROLLER_ADDR
                .parse::<SocketAddr>()
                .expect("default controller address must be valid")
        );
    }

    #[test]
    fn redis_value_to_json_handles_common_values() {
        assert_eq!(
            redis_value_to_json(&Value::BulkString(b"Mike".to_vec())),
            serde_json::json!("Mike")
        );
        assert_eq!(redis_value_to_json(&Value::Int(42)), serde_json::json!(42));
        assert_eq!(
            redis_value_to_json(&Value::Boolean(true)),
            serde_json::json!(true)
        );
    }

    #[test]
    fn decode_segment_accepts_segment_json_field() {
        let segment = TranscriptionSegment::new("session-1", "Speaker_1", "Hello Mike", 0, 1000);
        let json = serde_json::to_string(&segment).unwrap();
        let stream_id = StreamId {
            id: "0-1".to_owned(),
            map: HashMap::from([("segment".to_owned(), Value::BulkString(json.into_bytes()))]),
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let decoded = decode_segment(&stream_id).unwrap();

        assert_eq!(decoded, segment);
    }
}
