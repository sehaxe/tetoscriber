use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use teto_protocol::{BrainIdentityRequest, BrainIdentityResponse};
use tokio::io::AsyncWriteExt;
use tokio::process::Command as TokioCommand;
use tracing::{info, warn};
use tracing_subscriber::{fmt, EnvFilter};

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1/";
const DEFAULT_BRAIN_QUEUE: &str = "teto_brain_queue";
const DEFAULT_BRAIN_RESOLUTIONS_QUEUE: &str = "brain_resolutions";
const DEFAULT_KNOWN_NAMES_JSON: &str = "";
const DEFAULT_RECONNECT_DELAY_SECS: u64 = 2;
const DEFAULT_BRAIN_BACKEND: &str = "regex";

#[derive(Debug, Clone)]
pub struct BrainConfig {
    pub redis_url: String,
    pub brain_queue: String,
    pub brain_resolutions_queue: String,
    pub reconnect_delay: Duration,
}

impl BrainConfig {
    pub fn from_env() -> Self {
        Self {
            redis_url: env_or_default("REDIS_URL", DEFAULT_REDIS_URL),
            brain_queue: env_or_default("TETO_BRAIN_QUEUE", DEFAULT_BRAIN_QUEUE),
            brain_resolutions_queue: env_or_default(
                "TETO_BRAIN_RESOLUTIONS_QUEUE",
                DEFAULT_BRAIN_RESOLUTIONS_QUEUE,
            ),
            reconnect_delay: Duration::from_secs(env_u64(
                "TETO_BRAIN_RECONNECT_DELAY_SECS",
                DEFAULT_RECONNECT_DELAY_SECS,
            )),
        }
    }
}

impl Default for BrainConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

#[derive(Clone)]
pub struct TetoBrain {
    config: BrainConfig,
    backend: Arc<dyn BrainBackend>,
}

impl TetoBrain {
    pub fn new(config: BrainConfig, backend: Arc<dyn BrainBackend>) -> Self {
        Self { config, backend }
    }

    pub async fn run(self) {
        loop {
            match self.run_connected().await {
                Ok(()) => {}
                Err(error) => {
                    warn!(%error, "Teto-Brain listener failed; reconnecting");
                    tokio::time::sleep(self.config.reconnect_delay).await;
                }
            }
        }
    }

    async fn run_connected(&self) -> Result<()> {
        let mut connection = self.connect().await?;

        loop {
            let Some(payload) =
                Self::brpop_payload(&mut connection, &self.config.brain_queue, "brain queue")
                    .await?
            else {
                continue;
            };

            let request: BrainIdentityRequest = serde_json::from_str(&payload)
                .with_context(|| format!("failed to decode brain identity request: {payload}"))?;

            let response = self.backend.infer(&request).await?;
            let response_payload = serde_json::to_string(&response)
                .context("failed to encode brain identity response")?;

            let _: usize = connection
                .lpush(&self.config.brain_resolutions_queue, response_payload)
                .await
                .with_context(|| {
                    format!(
                        "failed to push brain response to '{}'",
                        self.config.brain_resolutions_queue
                    )
                })?;

            info!(
                session_id = %request.session_id,
                identities = response.identities.len(),
                "Teto-Brain published identity response"
            );
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

        Ok(response.and_then(|values| values.into_iter().nth(1)))
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

#[async_trait]
pub trait BrainBackend: Send + Sync {
    async fn infer(&self, request: &BrainIdentityRequest) -> Result<BrainIdentityResponse>;
}

#[derive(Debug)]
struct RegexBrainBackend {
    engine: RegexIdentityEngine,
}

#[async_trait]
impl BrainBackend for RegexBrainBackend {
    async fn infer(&self, request: &BrainIdentityRequest) -> Result<BrainIdentityResponse> {
        self.engine.infer(request)
    }
}

#[derive(Debug)]
struct CommandBrainBackend {
    command: Vec<OsString>,
}

#[async_trait]
impl BrainBackend for CommandBrainBackend {
    async fn infer(&self, request: &BrainIdentityRequest) -> Result<BrainIdentityResponse> {
        let Some((program, args)) = self.command.split_first() else {
            bail!("TETO_BRAIN_COMMAND must contain at least one executable");
        };

        let mut child = TokioCommand::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| {
                format!(
                    "failed to spawn brain command '{}'",
                    program.to_string_lossy()
                )
            })?;

        let request_payload =
            serde_json::to_vec(request).context("failed to encode brain request")?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(&request_payload)
                .await
                .context("failed to write brain request to command stdin")?;
        }

        let output = child
            .wait_with_output()
            .await
            .context("failed to read brain command output")?;

        if !output.status.success() {
            bail!(
                "brain command exited with status {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
        }

        serde_json::from_slice(&output.stdout)
            .context("failed to decode brain command stdout as BrainIdentityResponse")
    }
}

#[derive(Debug, Clone)]
pub struct RegexIdentityEngine {
    known_names: KnownNames,
}

impl RegexIdentityEngine {
    pub fn from_env() -> Result<Self> {
        let raw = env_or_default("TETO_KNOWN_NAMES_JSON", DEFAULT_KNOWN_NAMES_JSON);
        let known_names =
            KnownNames::from_json(&raw).context("failed to parse TETO_KNOWN_NAMES_JSON")?;

        Ok(Self { known_names })
    }

    pub fn new(known_names: KnownNames) -> Self {
        Self { known_names }
    }

    pub fn infer(&self, request: &BrainIdentityRequest) -> Result<BrainIdentityResponse> {
        let mut assignments = BTreeMap::new();
        let lines = parse_speaker_lines(&request.transcript);

        for (speaker_tag, text) in &lines {
            for known_name in self.known_names.iter() {
                let lowered = text.to_lowercase();

                if mentions_self_as(&lowered, &known_name.canonical, &known_name.aliases)
                    || mentions_name(&lowered, &known_name.aliases)
                {
                    assignments.insert(speaker_tag.clone(), known_name.canonical.clone());
                }
            }
        }

        for speaker_tag in &request.speaker_tags {
            if assignments.contains_key(speaker_tag) {
                continue;
            }

            if let Some(name) = fallback_name_for_remaining_speaker(
                speaker_tag,
                &assignments,
                &request.speaker_tags,
                &self.known_names,
            ) {
                assignments.insert(speaker_tag.clone(), name);
            }
        }

        Ok(BrainIdentityResponse::new(
            request.session_id.clone(),
            assignments,
        ))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KnownNames {
    entries: Vec<KnownName>,
}

impl KnownNames {
    pub fn from_json(raw: &str) -> Result<Self> {
        if raw.trim().is_empty() {
            return Ok(Self::default());
        }

        let parsed: BTreeMap<String, Vec<String>> = serde_json::from_str(raw)
            .context("expected JSON object mapping canonical names to aliases")?;

        let entries = parsed
            .into_iter()
            .map(|(canonical, aliases)| {
                let canonical = canonical.trim().to_owned();
                if canonical.is_empty() {
                    bail!("known name canonical value cannot be empty");
                }

                let aliases = aliases
                    .into_iter()
                    .map(|alias| alias.trim().to_lowercase())
                    .filter(|alias| !alias.is_empty())
                    .collect::<Vec<_>>();

                Ok(KnownName { canonical, aliases })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { entries })
    }

    pub fn iter(&self) -> impl Iterator<Item = &KnownName> {
        self.entries.iter()
    }

    fn canonical_names(&self) -> impl Iterator<Item = &str> {
        self.entries
            .iter()
            .map(|known_name| known_name.canonical.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownName {
    canonical: String,
    aliases: Vec<String>,
}

fn parse_speaker_lines(transcript: &str) -> Vec<(String, String)> {
    transcript
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }

            let (speaker_tag, text) = line
                .split_once(':')
                .map(|(speaker_tag, text)| (speaker_tag.trim().to_owned(), text.trim()))
                .unwrap_or_else(|| ("unknown".to_owned(), line));

            Some((speaker_tag, text.to_owned()))
        })
        .collect()
}

fn mentions_self_as(lowered_text: &str, canonical_name: &str, aliases: &[String]) -> bool {
    let markers = [
        format!("i am {canonical_name}"),
        format!("i'm {canonical_name}"),
        format!("this is {canonical_name}"),
        format!("я {canonical_name}"),
        format!("это {canonical_name}"),
    ];

    markers.iter().any(|marker| lowered_text.contains(marker))
        || aliases.iter().any(|alias| {
            let alias = alias.to_lowercase();
            lowered_text.contains(&format!("i am {alias}"))
                || lowered_text.contains(&format!("i'm {alias}"))
                || lowered_text.contains(&format!("я {alias}"))
                || lowered_text.contains(&format!("это {alias}"))
        })
}

fn mentions_name(lowered_text: &str, aliases: &[String]) -> bool {
    aliases
        .iter()
        .any(|alias| lowered_text.contains(&alias.to_lowercase()))
}

fn fallback_name_for_remaining_speaker(
    speaker_tag: &str,
    assignments: &BTreeMap<String, String>,
    speaker_tags: &[String],
    known_names: &KnownNames,
) -> Option<String> {
    let assigned_names = assignments.values().cloned().collect::<BTreeSet<_>>();
    let remaining_names = known_names
        .canonical_names()
        .filter(|name| !assigned_names.contains(*name))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    let unassigned_count = speaker_tags
        .iter()
        .filter(|tag| !assignments.contains_key(tag.as_str()))
        .count();

    if remaining_names.len() == unassigned_count {
        let index = speaker_tags
            .iter()
            .take_while(|tag| tag.as_str() != speaker_tag)
            .filter(|tag| !assignments.contains_key(tag.as_str()))
            .count();

        remaining_names.into_iter().nth(index)
    } else {
        None
    }
}

fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config = BrainConfig::from_env();
    info!(
        redis_url = %config.redis_url,
        brain_queue = %config.brain_queue,
        brain_resolutions_queue = %config.brain_resolutions_queue,
        "Teto-Brain configuration loaded"
    );

    let backend = brain_backend_from_env().context("failed to initialize identity backend")?;

    TetoBrain::new(config, backend).run().await;
    Ok(())
}

fn brain_backend_from_env() -> Result<Arc<dyn BrainBackend>> {
    let backend = env_or_default("TETO_BRAIN_BACKEND", DEFAULT_BRAIN_BACKEND).to_ascii_lowercase();

    match backend.as_str() {
        "regex" => {
            let engine = RegexIdentityEngine::from_env()
                .context("failed to initialize regex identity engine")?;
            Ok(Arc::new(RegexBrainBackend { engine }))
        }
        "command" => {
            let raw = std::env::var("TETO_BRAIN_COMMAND")
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
                .context("TETO_BRAIN_COMMAND is required when TETO_BRAIN_BACKEND=command")?;

            let command = raw
                .split_whitespace()
                .map(OsString::from)
                .collect::<Vec<_>>();

            Ok(Arc::new(CommandBrainBackend { command }))
        }
        other => bail!("unsupported TETO_BRAIN_BACKEND '{other}'; expected 'regex' or 'command'"),
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_speaker_tagged_transcript() {
        let lines = parse_speaker_lines("Speaker_1: Алло, Алиса.\nSpeaker_2: Привет, Борис.");

        assert_eq!(
            lines,
            vec![
                ("Speaker_1".to_owned(), "Алло, Алиса.".to_owned()),
                ("Speaker_2".to_owned(), "Привет, Борис.".to_owned()),
            ]
        );
    }

    #[test]
    fn regex_engine_maps_mentioned_names_from_config() {
        let request = BrainIdentityRequest::new(
            "session-1",
            vec!["Speaker_1".to_owned(), "Speaker_2".to_owned()],
            "Speaker_1: Алло, Алиса.\nSpeaker_2: Привет, Борис.",
        );
        let engine = RegexIdentityEngine::new(test_known_names());

        let response = engine.infer(&request).unwrap();

        assert_eq!(
            response.identities.get("Speaker_1"),
            Some(&"Алиса".to_owned())
        );
        assert_eq!(
            response.identities.get("Speaker_2"),
            Some(&"Борис".to_owned())
        );
    }

    #[test]
    fn regex_engine_maps_self_introduction_from_config() {
        let request = BrainIdentityRequest::new(
            "session-1",
            vec!["Speaker_1".to_owned(), "Speaker_2".to_owned()],
            "Speaker_1: Привет, это Алиса.\nSpeaker_2: Рад знакомству.",
        );
        let engine = RegexIdentityEngine::new(test_known_names());

        let response = engine.infer(&request).unwrap();

        assert_eq!(
            response.identities.get("Speaker_1"),
            Some(&"Алиса".to_owned())
        );
    }

    #[test]
    fn known_names_rejects_invalid_json() {
        let err = KnownNames::from_json(r#"{"Алиса": "алиса"}"#).unwrap_err();

        assert!(err.to_string().contains("expected JSON object"));
    }

    fn test_known_names() -> KnownNames {
        KnownNames::from_json(
            r#"{
                "Алиса": ["alice", "алиса"],
                "Борис": ["bob", "борис"]
            }"#,
        )
        .unwrap()
    }
}
