use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, UNIX_EPOCH};

use anyhow::{Context, Result};
use rusqlite::Connection;
use rusqlite::backup::{Backup, StepResult};

use crate::config::{read_provider_from_config, resolve_sqlite_path, DEFAULT_BUCKET_PADDING_BYTES, STATE_DB_FILENAME};
use crate::locale;
use crate::rollout::{self, RolloutProgressConfig};
use crate::sqlite;
use crate::types::*;

pub fn reconcile_once(
    codex_home: &Path,
    provider_override: Option<&str>,
    rollout_scope: RolloutScope,
) -> Result<ReconcileSummary> {
    reconcile_once_with_progress(codex_home, provider_override, rollout_scope, None)
}

pub fn reconcile_once_with_progress(
    codex_home: &Path,
    provider_override: Option<&str>,
    rollout_scope: RolloutScope,
    progress: Option<RolloutProgressConfig>,
) -> Result<ReconcileSummary> {
    let provider = match provider_override {
        Some(provider) => provider.to_string(),
        None => read_provider_from_config(codex_home)?,
    };
    let sqlite_path = resolve_sqlite_path(codex_home)?;
    let started = Instant::now();
    let rollout_summary = rollout::reconcile_rollout_metadata_from_sqlite_with_progress(
        &sqlite_path,
        codex_home,
        provider.as_str(),
        rollout_scope,
        None,
        DEFAULT_BUCKET_PADDING_BYTES,
        progress,
    )?;
    let (changed_rows, total_rows) = sqlite::reconcile_sqlite_in_place(&sqlite_path, provider.as_str())?;

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

pub fn reconcile_once_with_backup_progress(
    codex_home: &Path,
    provider_override: Option<&str>,
    rollout_scope: RolloutScope,
    progress: Option<RolloutProgressConfig>,
) -> Result<ReconcileSummary> {
    reconcile_once_with_backup_and_padding(
        codex_home,
        provider_override,
        rollout_scope,
        DEFAULT_BUCKET_PADDING_BYTES,
        progress,
    )
}

pub fn reconcile_once_with_backup_and_padding(
    codex_home: &Path,
    provider_override: Option<&str>,
    rollout_scope: RolloutScope,
    padding_bytes: usize,
    progress: Option<RolloutProgressConfig>,
) -> Result<ReconcileSummary> {
    let provider = match provider_override {
        Some(provider) => provider.to_string(),
        None => read_provider_from_config(codex_home)?,
    };
    let sqlite_path = resolve_sqlite_path(codex_home)?;
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
    let rollout_summary = rollout::reconcile_rollout_metadata_from_sqlite_with_progress(
        &sqlite_path,
        codex_home,
        provider.as_str(),
        rollout_scope,
        Some(rollout_journal_path.as_path()),
        padding_bytes,
        progress,
    )?;
    let (changed_rows, total_rows) = sqlite::reconcile_sqlite_in_place(&sqlite_path, provider.as_str())?;

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

fn create_sqlite_backup(sqlite_path: &Path, backup_path: &Path) -> Result<()> {
    sqlite::ensure_sqlite_exists(sqlite_path)?;
    let source = Connection::open(sqlite_path)
        .with_context(|| format!("failed to open {}", sqlite_path.display()))?;
    source.busy_timeout(Duration::from_millis(5_000))?;
    let mut destination = Connection::open(backup_path)
        .with_context(|| format!("failed to open {}", backup_path.display()))?;
    let backup = Backup::new(&source, &mut destination)?;
    let started = Instant::now();
    let timeout = Duration::from_secs(30);

    loop {
        if started.elapsed() >= timeout {
            anyhow::bail!(
                "sqlite backup timed out after {} seconds for {}",
                timeout.as_secs(),
                sqlite_path.display()
            );
        }

        match backup.step(100)? {
            StepResult::Done => break,
            StepResult::More => {}
            StepResult::Busy | StepResult::Locked => {
                std::thread::sleep(Duration::from_millis(50));
            }
            _ => {}
        }
    }

    drop(backup);
    drop(destination);
    sqlite::sync_file(backup_path)?;

    Ok(())
}

pub(crate) fn create_sqlite_backup_file(sqlite_path: &Path) -> Result<PathBuf> {
    let backups_dir = sqlite_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("backups");
    fs::create_dir_all(&backups_dir)
        .with_context(|| format!("failed to create {}", backups_dir.display()))?;

    let timestamp = sqlite::unix_timestamp_millis()?;
    let sqlite_name = sqlite_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(STATE_DB_FILENAME);
    let backup_name = format!("{sqlite_name}.{timestamp}.bak");
    let backup_path = backups_dir.join(&backup_name);
    let backup_temp_path = backups_dir.join(format!("{backup_name}.tmp"));

    if backup_temp_path.exists() {
        fs::remove_file(&backup_temp_path)
            .with_context(|| format!("failed to remove {}", backup_temp_path.display()))?;
    }

    create_sqlite_backup(sqlite_path, &backup_temp_path)?;
    fs::rename(&backup_temp_path, &backup_path)
        .with_context(|| format!("failed to finalize {}", backup_path.display()))?;
    sqlite::sync_dir(&backups_dir)?;

    Ok(backup_path)
}

pub fn list_backup_files(codex_home: &Path) -> Result<Vec<PathBuf>> {
    let sqlite_path = resolve_sqlite_path(codex_home)?;
    let backups_dir = sqlite_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("backups");

    if !backups_dir.exists() {
        return Ok(Vec::new());
    }

    let mut backups: Vec<PathBuf> = fs::read_dir(&backups_dir)
        .with_context(|| format!("failed to read {}", backups_dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("bak"))
        .collect();

    backups.sort_by(|a, b| {
        b.metadata().and_then(|m| m.created()).ok()
            .cmp(&a.metadata().and_then(|m| m.created()).ok())
    });

    Ok(backups)
}

pub fn run_restore(codex_home: &Path, backup_arg: Option<&Path>, dry_run: bool) -> Result<RestoreSummary> {
    let sqlite_path = resolve_sqlite_path(codex_home)?;

    let backup_path = match backup_arg {
        Some(path) => {
            if !path.exists() {
                anyhow::bail!("Backup file not found: {}", path.display());
            }
            path.to_path_buf()
        }
        None => {
            let backups = list_backup_files(codex_home)?;
            if backups.is_empty() {
                anyhow::bail!("No backups found in the backups directory.");
            }
            println!("Available backups:");
            for (i, b) in backups.iter().enumerate() {
                let size = fs::metadata(b).map(|m| m.len()).unwrap_or(0);
                let modified = b.metadata()
                    .and_then(|m| m.modified())
                    .ok()
                    .map(|t| {
                        let duration = t.duration_since(UNIX_EPOCH).unwrap_or_default();
                        duration.as_secs()
                    })
                    .unwrap_or(0);
                println!("  {}. {} ({} bytes, modified {})", i + 1, b.display(), size, modified);
            }
            if let Some(backup) = backups.first() {
                println!("\nUse: codex-threadripper restore <BACKUP_PATH>");
                backup.clone()
            } else {
                anyhow::bail!("No .bak backup files found.");
            }
        }
    };

    if dry_run {
        println!("{} Would restore SQLite from: {}", locale::dry_run_label(locale::Locale::En), backup_path.display());
        return Ok(RestoreSummary { backup_path });
    }

    fs::copy(&backup_path, &sqlite_path)
        .with_context(|| format!("failed to restore {} from {}", sqlite_path.display(), backup_path.display()))?;

    println!("{}", locale::restore_backup_path_label(locale::detect_locale()));
    println!("  {}", backup_path.display());

    Ok(RestoreSummary { backup_path })
}

pub fn run_prune_backups(codex_home: &Path, keep: usize, dry_run: bool) -> Result<PruneSummary> {
    let backups = list_backup_files(codex_home)?;
    let kept = backups.len().min(keep);
    let to_remove: Vec<&PathBuf> = backups.iter().skip(kept).collect();
    let removed = to_remove.len();

    if removed == 0 {
        println!("No old backups to prune ({} total, keeping {})", backups.len(), keep);
        return Ok(PruneSummary { removed: 0, kept: backups.len() });
    }

    if dry_run {
        println!("{} Would remove {} old backup(s), keep {}:", locale::dry_run_label(locale::Locale::En), removed, keep);
        for b in &to_remove {
            println!("  would remove: {}", b.display());
        }
    } else {
        for b in &to_remove {
            fs::remove_file(b)
                .with_context(|| format!("failed to remove {}", b.display()))?;
        }
    }

    println!("  {}: {}", locale::prune_backups_removed_label(locale::detect_locale()), removed);
    println!("  {}: {}", locale::prune_backups_kept_label(locale::detect_locale()), kept);

    Ok(PruneSummary { removed, kept })
}

#[cfg(test)]
pub(crate) fn reconcile_sqlite_with_backup(sqlite_path: &Path, provider: &str) -> Result<(u64, u64, PathBuf)> {
    let backup_path = create_sqlite_backup_file(sqlite_path)?;
    let (changed_rows, total_rows) = sqlite::reconcile_sqlite_in_place(sqlite_path, provider)?;
    Ok((changed_rows, total_rows, backup_path))
}
