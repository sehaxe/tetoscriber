use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use redis::{AsyncCommands, Client};
use testcontainers::core::{ContainerAsync, IntoContainerPort, WaitFor};
use testcontainers::{runners::AsyncRunner, GenericImage};
use teto_controller::queue::{
    BrainResolutionListener, QueueConfig, SharedIdentityStorage, StreamListener,
};
use teto_controller::storage::{IdentityStorage, MatchedVoice};
use teto_protocol::{
    BrainIdentityRequest, BrainIdentityResponse, IdentitySource, TranscriptEmbedding,
    TranscriptionSegment, VoiceFingerprint, VoiceIdentityUpsert, VOICE_FINGERPRINT_DIM,
};
use tokio::sync::broadcast;
use tokio::time::timeout;

#[tokio::test]
async fn identity_loop_closes_without_riva() -> Result<()> {
    let Some(redis) = start_redis().await? else {
        eprintln!("skipping Redis-backed identity loop test: docker is unavailable");
        return Ok(());
    };

    let config = test_config(redis.url.clone());
    let client = Client::open(config.redis_url.as_str()).context("failed to open Redis client")?;
    let mut connection = client
        .get_connection_manager()
        .await
        .context("failed to connect to Redis")?;

    let storage = Arc::new(MockStorage::default());
    let memory: Option<SharedIdentityStorage> = Some(storage.clone() as SharedIdentityStorage);
    let pending = StreamListener::new_pending();
    let (live_tx, mut live_rx) = broadcast::channel(1024);
    let (identity_tx, mut identity_rx) = broadcast::channel(1024);

    let stream_listener = StreamListener::with_pending(
        config.clone(),
        live_tx.clone(),
        memory.clone(),
        pending.clone(),
    );
    let stream_handle = tokio::spawn(async move {
        stream_listener.listen_transcription_stream().await;
    });

    let brain_listener = BrainResolutionListener::new(
        config.clone(),
        live_tx,
        memory,
        pending.clone(),
        identity_tx,
    );
    let brain_handle = tokio::spawn(async move {
        brain_listener.listen_brain_resolutions().await;
    });

    let fingerprint = fingerprint();
    let segment =
        TranscriptionSegment::new("session-1", "Speaker_1", "Меня зовут Алиса.", 0, 1_000)
            .with_voice_fingerprint(fingerprint.clone());
    push_segment(&mut connection, &config, &segment).await?;

    let request_payload = timeout_brpop(&mut connection, &config.brain_queue)
        .await?
        .context("controller did not dispatch a brain identity request")?;
    let request: BrainIdentityRequest = serde_json::from_str(&request_payload)
        .context("brain request payload was not a BrainIdentityRequest")?;
    assert_eq!(request.session_id, "session-1");
    assert_eq!(request.speaker_tags, vec!["Speaker_1".to_owned()]);
    assert!(request.transcript.contains("Speaker_1: Меня зовут Алиса."));

    let mut identities = BTreeMap::new();
    identities.insert("Speaker_1".to_owned(), "Алиса".to_owned());
    let response = BrainIdentityResponse::new("session-1", identities);
    let response_payload = serde_json::to_string(&response).context("failed to encode response")?;
    let _: usize = connection
        .lpush(config.brain_resolutions_queue.as_str(), response_payload)
        .await
        .context("failed to push brain resolution")?;

    let identity = timeout(Duration::from_secs(5), identity_rx.recv())
        .await
        .context("timed out waiting for IdentityResolved event")?
        .context("identity broadcast channel closed")?;
    assert_eq!(identity.event_type, "IdentityResolved");
    assert_eq!(identity.session_id, "session-1");
    assert_eq!(identity.speaker_tag, "Speaker_1");
    assert_eq!(identity.name, "Алиса");
    assert_eq!(identity.source, IdentitySource::BrainReasoning);

    let replayed = timeout(Duration::from_secs(5), live_rx.recv())
        .await
        .context("timed out waiting for replayed transcript segment")?
        .context("live transcript channel closed")?;
    assert_eq!(replayed.session_id, "session-1");
    assert_eq!(replayed.speaker_tag, "Speaker_1");
    assert_eq!(replayed.text, "Меня зовут Алиса.");
    assert_eq!(replayed.identified_name.as_deref(), Some("Алиса"));

    let upserts = storage.upserts();
    assert_eq!(upserts.len(), 1);
    assert_eq!(upserts[0].session_id, "session-1");
    assert_eq!(upserts[0].speaker_tag, "Speaker_1");
    assert_eq!(upserts[0].name, "Алиса");
    assert_eq!(upserts[0].source, IdentitySource::BrainReasoning);

    let fast_path_segment = TranscriptionSegment::new(
        "session-1",
        "Speaker_1",
        "Алиса продолжает разговор.",
        1_001,
        2_000,
    )
    .with_voice_fingerprint(fingerprint);
    push_segment(&mut connection, &config, &fast_path_segment).await?;

    let fast_path = timeout(Duration::from_secs(5), live_rx.recv())
        .await
        .context("timed out waiting for fast-path transcript segment")?
        .context("live transcript channel closed")?;
    assert_eq!(fast_path.text, "Алиса продолжает разговор.");
    assert_eq!(fast_path.identified_name.as_deref(), Some("Алиса"));

    let maybe_new_request = try_lpop_payload(&mut connection, &config.brain_queue)
        .await
        .context("fast-path lpop failed")?;
    assert!(
        maybe_new_request.is_none(),
        "fast-path segment should match persisted voice identity without Brain dispatch"
    );

    stream_handle.abort();
    brain_handle.abort();

    Ok(())
}

struct RedisContainer {
    _container: ContainerAsync<GenericImage>,
    url: String,
}

async fn start_redis() -> Result<Option<RedisContainer>> {
    if !docker_available().await {
        return Ok(None);
    }

    let container = GenericImage::new("redis", "7.2.4")
        .with_exposed_port(6379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await
        .context("failed to start Redis testcontainer")?;

    let host = container
        .get_host()
        .await
        .context("failed to read Redis testcontainer host")?
        .to_string();
    let port = container
        .get_host_port_ipv4(6379.tcp())
        .await
        .context("failed to read Redis testcontainer port")?;

    Ok(Some(RedisContainer {
        _container: container,
        url: format!("redis://{host}:{port}/"),
    }))
}

async fn docker_available() -> bool {
    tokio::process::Command::new("docker")
        .arg("info")
        .output()
        .await
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn test_config(redis_url: String) -> QueueConfig {
    let suffix = uuid::Uuid::new_v4();
    QueueConfig {
        redis_url,
        transcription_stream: format!("teto_test_transcription_stream_{suffix}"),
        controller_group: format!("teto_test_controller_group_{suffix}"),
        controller_consumer: format!("teto_test_controller_consumer_{suffix}"),
        brain_queue: format!("teto_test_brain_queue_{suffix}"),
        brain_resolutions_queue: format!("teto_test_brain_resolutions_{suffix}"),
        live_channel: format!("teto_test_live_transcripts_{suffix}"),
        stream_block_ms: 100,
        stream_batch_size: 10,
        brain_request_segments: 1,
        controller_addr: "127.0.0.1:0".parse().expect("valid test address"),
    }
}

async fn push_segment(
    connection: &mut redis::aio::ConnectionManager,
    config: &QueueConfig,
    segment: &TranscriptionSegment,
) -> Result<()> {
    let payload = serde_json::to_string(segment).context("failed to encode segment")?;
    let _: String = connection
        .xadd(
            config.transcription_stream.as_str(),
            "*",
            &[("segment", payload.as_str())],
        )
        .await
        .context("failed to push segment to Redis stream")?;
    Ok(())
}

async fn timeout_brpop(
    connection: &mut redis::aio::ConnectionManager,
    queue: &str,
) -> Result<Option<String>> {
    match timeout(
        Duration::from_secs(5),
        redis::cmd("BRPOP")
            .arg(queue)
            .arg(0)
            .query_async(connection),
    )
    .await
    {
        Ok(Ok(response)) => {
            let response: Option<Vec<String>> = response;
            Ok(response.and_then(|values| values.into_iter().nth(1)))
        }
        Ok(Err(error)) => Err(error).context("Redis brpop failed"),
        Err(_) => Ok(None),
    }
}

async fn try_lpop_payload(
    connection: &mut redis::aio::ConnectionManager,
    queue: &str,
) -> Result<Option<String>> {
    let response: Option<Vec<String>> = redis::cmd("LPOP")
        .arg(queue)
        .query_async(connection)
        .await
        .context("Redis lpop failed")?;

    Ok(response.and_then(|values| values.into_iter().nth(1)))
}

fn fingerprint() -> VoiceFingerprint {
    VoiceFingerprint::new(vec![0.42; VOICE_FINGERPRINT_DIM])
        .expect("test fingerprint has the required dimension")
}

#[derive(Default)]
struct MockStorage {
    voices: Mutex<Vec<(Vec<f32>, String)>>,
    upserts: Mutex<Vec<VoiceIdentityUpsert>>,
}

impl MockStorage {
    fn upserts(&self) -> Vec<VoiceIdentityUpsert> {
        self.upserts
            .lock()
            .expect("mock storage lock poisoned")
            .clone()
    }
}

#[async_trait]
impl IdentityStorage for MockStorage {
    async fn match_voice(&self, fingerprint: &VoiceFingerprint) -> Result<Option<MatchedVoice>> {
        let voices = self.voices.lock().expect("mock storage lock poisoned");
        Ok(voices
            .iter()
            .find(|(stored, _)| stored.as_slice() == fingerprint.as_slice())
            .map(|(_, name)| MatchedVoice {
                name: name.clone(),
                confidence: 1.0,
                point_id: None,
            }))
    }

    async fn upsert_voice_identity(&self, upsert: &VoiceIdentityUpsert) -> Result<()> {
        self.voices
            .lock()
            .expect("mock storage lock poisoned")
            .push((
                upsert.fingerprint.as_slice().to_owned(),
                upsert.name.clone(),
            ));
        self.upserts
            .lock()
            .expect("mock storage lock poisoned")
            .push(upsert.clone());
        Ok(())
    }

    async fn index_transcript(&self, _embedding: &TranscriptEmbedding) -> Result<()> {
        Ok(())
    }
}
