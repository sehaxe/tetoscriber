use anyhow::Result;
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    info!("{}", teto_worker::worker_ready());
    #[cfg(feature = "riva")]
    {
        let processor = teto_worker::job_processor::JobProcessor::from_env();
        info!(
            redis_url = %processor.config().redis_url,
            archive_queue = %processor.config().archive_queue,
            live_channel = %processor.config().live_channel,
            transcription_stream = %processor.config().transcription_stream,
            "Teto-Worker dispatcher configuration loaded"
        );

        tokio::spawn(processor.clone().listen_archive_jobs());
        tokio::spawn(processor.listen_live_audio());

        std::future::pending::<()>().await;
    }
    #[cfg(not(feature = "riva"))]
    {
        info!("Set RIVA_PROTO_DIR and run `cargo run -p teto-worker --features riva` to compile the Riva ASR gRPC client.");
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}
