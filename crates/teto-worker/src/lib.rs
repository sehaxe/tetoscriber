#[cfg(feature = "riva")]
pub mod riva {
    pub mod asr {
        tonic::include_proto!("nvidia.riva.asr");
    }

    tonic::include_proto!("nvidia.riva");
}

#[cfg(feature = "riva")]
pub mod riva_client;

#[cfg(feature = "riva")]
pub mod job_processor;

pub fn worker_ready() -> &'static str {
    "teto-worker ready"
}

#[cfg(test)]
mod tests {
    use super::worker_ready;

    #[test]
    fn worker_ready_reports_status() {
        assert_eq!(worker_ready(), "teto-worker ready");
    }
}
