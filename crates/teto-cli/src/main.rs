use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use redis::AsyncCommands;

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1/";
const DEFAULT_ARCHIVE_QUEUE: &str = "teto_archive_jobs";

#[derive(Debug, Parser)]
#[command(name = "tetoscribe")]
#[command(about = "Local sovereign speech pipeline CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Enqueue an audio file for archive transcription.
    Process {
        /// Audio file path. WAV/PCM and raw PCM are accepted by the worker.
        path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Process { path } => process(path).await,
    }
}

async fn process(path: PathBuf) -> Result<()> {
    if !path.exists() {
        anyhow::bail!("audio path does not exist: {}", path.display());
    }

    let redis_url = env_or_default("REDIS_URL", DEFAULT_REDIS_URL);
    let archive_queue = env_or_default("TETO_ARCHIVE_JOBS", DEFAULT_ARCHIVE_QUEUE);
    let client = redis::Client::open(redis_url.as_str())
        .with_context(|| format!("invalid Redis URL '{redis_url}'"))?;
    let mut connection = client
        .get_connection_manager()
        .await
        .context("failed to connect to Redis")?;

    let path = path.to_string_lossy().into_owned();
    let _: usize = connection
        .rpush(&archive_queue, &path)
        .await
        .with_context(|| format!("failed to enqueue archive job in '{archive_queue}'"))?;

    println!("enqueued {path} in {archive_queue}");
    Ok(())
}

fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_process_command() {
        let cli = Cli::parse_from(["tetoscribe", "process", "./mike_and_nick.wav"]);

        match cli.command {
            Command::Process { path } => assert_eq!(path, PathBuf::from("./mike_and_nick.wav")),
        }
    }
}
