use anyhow::Result;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use crate::cli::DEFAULT_BUCKET_PADDING_BYTES;
use crate::codex_config::read_provider_from_config;
use crate::codex_config::resolve_sqlite_path;
use crate::fs_sync::with_threadripper_lock;
use crate::rollout::RolloutProgressConfig;
use crate::rollout::RolloutReconcileSummary;
use crate::rollout::RolloutScope;
use crate::rollout::reconcile_rollout_metadata_from_sqlite_with_progress;
use crate::service;
use crate::service::ServiceStatus as BackgroundServiceStatus;
use crate::state_db::ProviderDistribution;
use crate::state_db::create_sqlite_backup_file;
use crate::state_db::inspect_sqlite_distribution;
use crate::state_db::reconcile_sqlite_in_place;

#[derive(Debug)]
pub(crate) struct ReconcileSummary {
    pub(crate) provider: String,
    pub(crate) changed_rows: u64,
    pub(crate) total_rows: u64,
    pub(crate) changed_rollouts: u64,
    pub(crate) checked_rollouts: u64,
    pub(crate) prepared_rollouts: u64,
    pub(crate) skipped_rollouts: u64,
    pub(crate) elapsed: Duration,
    pub(crate) backup_path: Option<PathBuf>,
    pub(crate) rollout_journal_path: Option<PathBuf>,
}

#[derive(Debug)]
pub(crate) struct StatusSummary {
    pub(crate) codex_home: PathBuf,
    pub(crate) sqlite_path: PathBuf,
    pub(crate) config_path: PathBuf,
    pub(crate) provider: String,
    pub(crate) total_rows: u64,
    pub(crate) mismatched_rows: u64,
    pub(crate) distribution: ProviderDistribution,
    pub(crate) service_status: BackgroundServiceStatus,
}

pub(crate) fn collect_status(
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
) -> Result<StatusSummary> {
    let config_path = codex_home.join("config.toml");
    let sqlite_path = resolve_sqlite_path(codex_home, profile_override)?;
    let provider = match provider_override {
        Some(provider) => provider.to_string(),
        None => read_provider_from_config(codex_home, profile_override)?,
    };
    let (total_rows, mismatched_rows, distribution) =
        inspect_sqlite_distribution(&sqlite_path, provider.as_str())?;
    let service_status = service::current_service_status()?;

    Ok(StatusSummary {
        codex_home: codex_home.to_path_buf(),
        sqlite_path,
        config_path,
        provider,
        total_rows,
        mismatched_rows,
        distribution,
        service_status,
    })
}

pub(crate) fn reconcile_once(
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    rollout_scope: RolloutScope,
) -> Result<ReconcileSummary> {
    reconcile_once_with_progress(
        codex_home,
        provider_override,
        profile_override,
        rollout_scope,
        None,
    )
}

fn reconcile_once_with_progress(
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    rollout_scope: RolloutScope,
    progress: Option<RolloutProgressConfig>,
) -> Result<ReconcileSummary> {
    with_threadripper_lock(codex_home, || {
        reconcile_once_with_progress_unlocked(
            codex_home,
            provider_override,
            profile_override,
            rollout_scope,
            progress,
        )
    })
}

fn reconcile_once_with_progress_unlocked(
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    rollout_scope: RolloutScope,
    progress: Option<RolloutProgressConfig>,
) -> Result<ReconcileSummary> {
    let provider = match provider_override {
        Some(provider) => provider.to_string(),
        None => read_provider_from_config(codex_home, profile_override)?,
    };
    let sqlite_path = resolve_sqlite_path(codex_home, profile_override)?;
    let started = Instant::now();
    let mut rollout_summary = reconcile_rollout_metadata_from_sqlite_with_progress(
        &sqlite_path,
        codex_home,
        provider.as_str(),
        rollout_scope,
        None,
        DEFAULT_BUCKET_PADDING_BYTES,
        progress,
    )?;
    let (changed_rows, total_rows) = reconcile_sqlite_in_place(&sqlite_path, provider.as_str())?;
    if rollout_scope == RolloutScope::MismatchedRows && changed_rows > 0 {
        let followup_summary = reconcile_rollout_metadata_from_sqlite_with_progress(
            &sqlite_path,
            codex_home,
            provider.as_str(),
            RolloutScope::AllRows,
            None,
            DEFAULT_BUCKET_PADDING_BYTES,
            None,
        )?;
        add_rollout_summary(&mut rollout_summary, followup_summary);
    }

    Ok(ReconcileSummary {
        provider,
        changed_rows,
        total_rows,
        changed_rollouts: rollout_summary.changed_files,
        checked_rollouts: rollout_summary.checked_files,
        prepared_rollouts: rollout_summary.prepared_files,
        skipped_rollouts: rollout_summary.skipped_files,
        elapsed: started.elapsed(),
        backup_path: None,
        rollout_journal_path: rollout_summary.journal_path,
    })
}

pub(crate) fn reconcile_once_with_backup_progress(
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    rollout_scope: RolloutScope,
    progress: Option<RolloutProgressConfig>,
) -> Result<ReconcileSummary> {
    reconcile_once_with_backup_and_padding(
        codex_home,
        provider_override,
        profile_override,
        rollout_scope,
        DEFAULT_BUCKET_PADDING_BYTES,
        progress,
    )
}

pub(crate) fn reconcile_once_with_backup_and_padding(
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    rollout_scope: RolloutScope,
    padding_bytes: usize,
    progress: Option<RolloutProgressConfig>,
) -> Result<ReconcileSummary> {
    with_threadripper_lock(codex_home, || {
        reconcile_once_with_backup_and_padding_unlocked(
            codex_home,
            provider_override,
            profile_override,
            rollout_scope,
            padding_bytes,
            progress,
        )
    })
}

fn reconcile_once_with_backup_and_padding_unlocked(
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    rollout_scope: RolloutScope,
    padding_bytes: usize,
    progress: Option<RolloutProgressConfig>,
) -> Result<ReconcileSummary> {
    let provider = match provider_override {
        Some(provider) => provider.to_string(),
        None => read_provider_from_config(codex_home, profile_override)?,
    };
    let sqlite_path = resolve_sqlite_path(codex_home, profile_override)?;
    let started = Instant::now();
    let backup_path = create_sqlite_backup_file(&sqlite_path)?;
    let rollout_journal_path =
        backup_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!(
                "rollouts.{}.jsonl",
                backup_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("state-db.bak")
            ));
    let rollout_summary = reconcile_rollout_metadata_from_sqlite_with_progress(
        &sqlite_path,
        codex_home,
        provider.as_str(),
        rollout_scope,
        Some(rollout_journal_path.as_path()),
        padding_bytes,
        progress,
    )?;
    let (changed_rows, total_rows) = reconcile_sqlite_in_place(&sqlite_path, provider.as_str())?;

    Ok(ReconcileSummary {
        provider,
        changed_rows,
        total_rows,
        changed_rollouts: rollout_summary.changed_files,
        checked_rollouts: rollout_summary.checked_files,
        prepared_rollouts: rollout_summary.prepared_files,
        skipped_rollouts: rollout_summary.skipped_files,
        elapsed: started.elapsed(),
        backup_path: Some(backup_path),
        rollout_journal_path: rollout_summary.journal_path,
    })
}

fn add_rollout_summary(summary: &mut RolloutReconcileSummary, extra: RolloutReconcileSummary) {
    summary.checked_files += extra.checked_files;
    summary.changed_files += extra.changed_files;
    summary.prepared_files += extra.prepared_files;
    summary.skipped_files += extra.skipped_files;
}
