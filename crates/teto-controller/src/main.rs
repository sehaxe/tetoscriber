use anyhow::Result;
use axum::{routing::get, Router};
use std::sync::Arc;
use std::time::Duration;
use teto_controller::queue::{
    live_ws_handler, BrainResolutionListener, LiveState, QueueConfig, StreamListener,
};
use teto_controller::storage::{SovereignMemory, StorageConfig};
use tokio::sync::broadcast;
use tracing::{info, warn};
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    info!("teto-controller starting");
    info!("Redis and Qdrant endpoints are configured by environment variables");

    let queue_config = QueueConfig::from_env();
    info!(
        redis_url = %queue_config.redis_url,
        transcription_stream = %queue_config.transcription_stream,
        brain_queue = %queue_config.brain_queue,
        live_channel = %queue_config.live_channel,
        controller_addr = %queue_config.controller_addr,
        "Redis queue configuration loaded"
    );

    let (live_tx, _) = broadcast::channel(1024);
    let (identity_tx, _) = broadcast::channel(1024);
    let memory = init_memory().await;
    let memory =
        memory.map(|memory| Arc::new(memory) as Arc<dyn teto_controller::storage::IdentityStorage>);

    if memory.is_some() {
        info!("starting Redis stream listener and brain resolution listener");
    } else {
        warn!("Qdrant memory is unavailable; Redis listeners will run in degraded mode without voice matching or identity upserts");
    }

    let pending = StreamListener::new_pending();
    let stream_pending = pending.clone();

    tokio::spawn({
        let memory = memory.clone();
        let config = queue_config.clone();
        let live_tx = live_tx.clone();
        async move {
            StreamListener::with_pending(config, live_tx, memory, stream_pending)
                .listen_transcription_stream()
                .await;
        }
    });

    let brain_live_tx = live_tx.clone();
    let brain_identity_tx = identity_tx.clone();
    tokio::spawn({
        let config = queue_config.clone();
        async move {
            BrainResolutionListener::new(config, brain_live_tx, memory, pending, brain_identity_tx)
                .listen_brain_resolutions()
                .await;
        }
    });

    let app = Router::new()
        .route("/ws/live", get(live_ws_handler))
        .with_state(LiveState::new(live_tx, identity_tx))
        .with_state(());

    let listener = tokio::net::TcpListener::bind(queue_config.controller_addr).await?;
    info!(addr = %queue_config.controller_addr, "WebSocket live endpoint ready");

    axum::serve(listener, app).await?;

    Ok(())
}

async fn init_memory() -> Option<SovereignMemory> {
    let storage_config = StorageConfig::from_env();
    info!(
        qdrant_url = %storage_config.qdrant_url,
        voice_collection = %storage_config.voice_collection,
        transcript_collection = %storage_config.transcript_collection,
        voice_match_threshold = storage_config.voice_match_threshold,
        "Qdrant memory configuration loaded"
    );

    match tokio::time::timeout(
        Duration::from_secs(5),
        SovereignMemory::connect(storage_config.clone()),
    )
    .await
    {
        Ok(Ok(memory)) => {
            info!("Qdrant client initialized");

            match tokio::time::timeout(Duration::from_secs(10), memory.ensure_collections()).await {
                Ok(Ok(())) => info!("Qdrant memory collections are ready"),
                Ok(Err(error)) => {
                    warn!(%error, "Qdrant collection bootstrap failed; controller will keep running without memory")
                }
                Err(_) => warn!(
                    "Qdrant collection bootstrap timed out; controller will keep running without memory"
                ),
            }

            Some(memory)
        }
        Ok(Err(error)) => {
            warn!(%error, "Qdrant client initialization failed; controller will keep running without memory");
            None
        }
        Err(_) => {
            warn!(
                "Qdrant client initialization timed out; controller will keep running without memory"
            );
            None
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}
