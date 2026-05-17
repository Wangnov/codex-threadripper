use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::service::ServiceStatus as BackgroundServiceStatus;

pub type ProviderDistribution = Vec<(String, u64)>;

#[derive(Debug, Deserialize)]
pub struct ConfigToml {
    pub model_provider: Option<String>,
    pub sqlite_home: Option<String>,
}

#[derive(Debug)]
pub struct ReconcileSummary {
    pub provider: String,
    pub changed_rows: u64,
    pub total_rows: u64,
    pub changed_rollouts: u64,
    pub checked_rollouts: u64,
    pub prepared_rollouts: u64,
    pub skipped_rollouts: u64,
    pub elapsed: Duration,
    pub backup_path: Option<PathBuf>,
    pub rollout_journal_path: Option<PathBuf>,
}

#[derive(Debug)]
pub struct StatusSummary {
    pub codex_home: PathBuf,
    pub sqlite_path: PathBuf,
    pub config_path: PathBuf,
    pub provider: String,
    pub total_rows: u64,
    pub mismatched_rows: u64,
    pub distribution: ProviderDistribution,
    pub service_status: BackgroundServiceStatus,
    pub exceeds_desktop_cap: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RolloutScope {
    None,
    MismatchedRows,
    AllRows,
}

#[derive(Debug)]
pub struct RolloutTarget {
    pub thread_id: String,
    pub path: PathBuf,
}

#[derive(Debug, Default)]
pub struct RolloutReconcileSummary {
    pub checked_files: u64,
    pub changed_files: u64,
    pub prepared_files: u64,
    pub skipped_files: u64,
    pub journal_path: Option<PathBuf>,
}

#[derive(Debug)]
pub struct BucketPrepareSummary {
    pub checked_rollouts: u64,
    pub prepared_rollouts: u64,
    pub skipped_rollouts: u64,
    pub elapsed: Duration,
    pub journal_path: Option<PathBuf>,
}

#[derive(Clone, Copy)]
pub enum RolloutChangeMode {
    InPlace,
    RewriteWithPadding,
}

#[derive(Debug)]
pub struct FirstLine {
    pub content: Vec<u8>,
    pub newline: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct RolloutPatchOutcome {
    pub changed: bool,
    pub prepared: bool,
    pub skipped: bool,
}

pub struct RolloutChangeJournal {
    pub path: PathBuf,
    pub writer: Option<std::io::BufWriter<std::fs::File>>,
}

#[derive(Debug)]
pub struct BucketPrepareSummary_ {
    pub checked_rollouts: u64,
    pub prepared_rollouts: u64,
    pub skipped_rollouts: u64,
    pub elapsed: Duration,
    pub journal_path: Option<PathBuf>,
}

#[derive(Debug)]
pub struct RestoreSummary {
    pub backup_path: PathBuf,
}

#[derive(Debug)]
pub struct PruneSummary {
    pub removed: usize,
    pub kept: usize,
}
