use anyhow::Result;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use rusqlite::Error as RusqliteError;
use rusqlite::ErrorCode;

use crate::cli::DEFAULT_BUCKET_PADDING_BYTES;
use crate::codex_config::read_provider_from_config;
use crate::codex_config::resolve_sqlite_path;
use crate::fs_sync::with_threadripper_lock;
use crate::locale::detect_locale;
use crate::rollout::RolloutProgressConfig;
use crate::rollout::RolloutReconcileSummary;
use crate::rollout::RolloutScope;
use crate::rollout::reconcile_rollout_metadata_from_sqlite_with_progress;
use crate::rollout::reconcile_rollouts_for_stores;
use crate::service;
use crate::service::ServiceStatus as BackgroundServiceStatus;
use crate::state_db::ProviderDistribution;
use crate::state_db::create_sqlite_backup_file_in;
use crate::state_db::inspect_sqlite_distribution;
use crate::state_db::read_backfill_status;
use crate::state_db::reconcile_sqlite_in_place;
use crate::state_db::unix_timestamp_millis;
use crate::stores::StoreKind;
use crate::stores::StoreTarget;
use crate::stores::discover_stores;
use crate::stores::no_store_found_message;

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

/// Per-store status for a single discovered `state_5.sqlite` surface.
#[derive(Debug)]
pub(crate) struct StoreStatus {
    pub(crate) kind: StoreKind,
    pub(crate) db_path: PathBuf,
    pub(crate) total_rows: u64,
    pub(crate) mismatched_rows: u64,
    pub(crate) distribution: ProviderDistribution,
    pub(crate) backfill_status: Option<String>,
    pub(crate) error: Option<String>,
}

#[derive(Debug)]
pub(crate) struct StatusSummary {
    pub(crate) codex_home: PathBuf,
    pub(crate) config_path: PathBuf,
    pub(crate) provider: String,
    pub(crate) stores: Vec<StoreStatus>,
    pub(crate) service_status: BackgroundServiceStatus,
}

pub(crate) fn collect_status(
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
) -> Result<StatusSummary> {
    let config_path = codex_home.join("config.toml");
    let provider = match provider_override {
        Some(provider) => provider.to_string(),
        None => read_provider_from_config(codex_home, profile_override)?,
    };

    let targets = discover_stores(codex_home, profile_override)?;
    if targets.is_empty() {
        anyhow::bail!(no_store_found_message(detect_locale(), codex_home));
    }
    let mut stores = Vec::with_capacity(targets.len());
    for target in targets {
        // Best-effort: backfill status is auxiliary display info, so a read
        // failure (e.g. a read-only or transiently locked DB during an
        // in-progress rebuild) must not abort the whole status command.
        let backfill_status = read_backfill_status(&target.db_path).ok().flatten();
        match inspect_sqlite_distribution(&target.db_path, provider.as_str()) {
            Ok((total_rows, mismatched_rows, distribution)) => {
                stores.push(StoreStatus {
                    kind: target.kind,
                    db_path: target.db_path,
                    total_rows,
                    mismatched_rows,
                    distribution,
                    backfill_status,
                    error: None,
                });
            }
            Err(error) => {
                stores.push(StoreStatus {
                    kind: target.kind,
                    db_path: target.db_path,
                    total_rows: 0,
                    mismatched_rows: 0,
                    distribution: ProviderDistribution::default(),
                    backfill_status,
                    error: Some(error.to_string()),
                });
            }
        }
    }
    if stores.iter().all(|store| store.error.is_some()) {
        let details = stores
            .iter()
            .filter_map(|store| {
                store
                    .error
                    .as_deref()
                    .map(|error| format!("{}: {error}", store.db_path.display()))
            })
            .collect::<Vec<_>>()
            .join("; ");
        anyhow::bail!("failed to inspect any Codex state database: {details}");
    }

    let service_status = service::current_service_status()?;

    Ok(StatusSummary {
        codex_home: codex_home.to_path_buf(),
        config_path,
        provider,
        stores,
        service_status,
    })
}

/// Status of a multi-store reconcile run, mapped to a process exit code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconcileStatus {
    /// Every selected store was updated. Exit code 0.
    Full,
    /// Some stores were updated and at least one was skipped or failed. Exit code 2.
    Partial,
    /// No store could be updated. Exit code 1.
    Failed,
}

#[derive(Debug)]
pub(crate) enum StoreOutcome {
    Updated {
        changed_rows: u64,
        total_rows: u64,
        backup_path: Option<PathBuf>,
    },
    /// Left untouched because Codex's startup backfill was still running after
    /// the bounded wait; the user should re-run once the rebuild finishes.
    Skipped,
    Failed {
        error: String,
    },
}

#[derive(Debug)]
pub(crate) struct StoreReconcileResult {
    pub(crate) kind: StoreKind,
    pub(crate) db_path: PathBuf,
    pub(crate) outcome: StoreOutcome,
}

#[derive(Debug)]
pub(crate) struct MultiReconcileSummary {
    pub(crate) provider: String,
    pub(crate) stores: Vec<StoreReconcileResult>,
    pub(crate) changed_rollouts: u64,
    pub(crate) checked_rollouts: u64,
    pub(crate) prepared_rollouts: u64,
    pub(crate) skipped_rollouts: u64,
    pub(crate) rollout_journal_path: Option<PathBuf>,
    pub(crate) elapsed: Duration,
}

impl MultiReconcileSummary {
    pub(crate) fn status(&self) -> ReconcileStatus {
        let updated = self
            .stores
            .iter()
            .filter(|store| matches!(store.outcome, StoreOutcome::Updated { .. }))
            .count();
        if updated == self.stores.len() {
            ReconcileStatus::Full
        } else if updated == 0 {
            ReconcileStatus::Failed
        } else {
            ReconcileStatus::Partial
        }
    }

    /// True when the Codex App store was actually updated — used to warn that
    /// `--sqlite-only` edits there may be reverted by Codex's rollout backfill.
    /// A skipped/failed App store did not change, so no warning is needed.
    pub(crate) fn app_store_updated(&self, codex_home: &Path) -> bool {
        let app_db_path = codex_home
            .join(crate::stores::APP_SQLITE_SUBDIR)
            .join(crate::codex_config::STATE_DB_FILENAME);
        let app_db_path = app_db_path.canonicalize().unwrap_or(app_db_path);
        self.stores.iter().any(|store| {
            matches!(store.outcome, StoreOutcome::Updated { .. })
                && (store.kind == StoreKind::App || store.db_path == app_db_path)
        })
    }
}

/// Default bounded wait for an in-progress Codex backfill before a store is
/// skipped. A one-shot `sync` can afford to pause briefly; if the rebuild is not
/// done by then the store is skipped and the user re-runs later.
pub(crate) const DEFAULT_BACKFILL_WAIT: Duration = Duration::from_secs(10);

const BACKFILL_POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackfillReadiness {
    Ready,
    Busy,
}

/// Wait up to `budget` for a store's Codex startup-backfill to finish before we
/// write to it, so threadripper never races Codex's rebuild. A store with no
/// `backfill_state` table (older Codex) or a `complete` status is ready
/// immediately; a status read error is treated as ready and the write phase
/// surfaces any real problem.
fn wait_for_store_backfill(db_path: &Path, budget: Duration) -> BackfillReadiness {
    let started = Instant::now();
    loop {
        match read_backfill_status(db_path) {
            Ok(None) => return BackfillReadiness::Ready,
            Ok(Some(status)) if status == "complete" => return BackfillReadiness::Ready,
            Ok(Some(_)) => {}
            Err(error) if is_sqlite_lock_error(&error) => {}
            Err(_) => return BackfillReadiness::Ready,
        }
        if started.elapsed() >= budget {
            return BackfillReadiness::Busy;
        }
        std::thread::sleep(BACKFILL_POLL_INTERVAL.min(budget));
    }
}

fn is_sqlite_lock_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<RusqliteError>(),
            Some(RusqliteError::SqliteFailure(error, _))
                if matches!(error.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
        )
    })
}

/// Reconcile the provider across **all** discovered stores plus the shared
/// rollout JSONL, backing up each store first. This is the multi-store write
/// path for the one-shot `sync` / `bucket switch` commands. A store whose Codex
/// backfill is still running after `backfill_wait` is skipped, not written.
pub(crate) fn reconcile_all_stores_with_backup(
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    rollout_scope: RolloutScope,
    padding_bytes: usize,
    backfill_wait: Duration,
    progress: Option<RolloutProgressConfig>,
) -> Result<MultiReconcileSummary> {
    with_threadripper_lock(codex_home, || {
        reconcile_all_stores_with_backup_unlocked(
            codex_home,
            provider_override,
            profile_override,
            rollout_scope,
            padding_bytes,
            backfill_wait,
            progress,
        )
    })
}

fn reconcile_all_stores_with_backup_unlocked(
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    rollout_scope: RolloutScope,
    padding_bytes: usize,
    backfill_wait: Duration,
    progress: Option<RolloutProgressConfig>,
) -> Result<MultiReconcileSummary> {
    let provider = match provider_override {
        Some(provider) => provider.to_string(),
        None => read_provider_from_config(codex_home, profile_override)?,
    };
    let targets = discover_stores(codex_home, profile_override)?;
    if targets.is_empty() {
        anyhow::bail!(no_store_found_message(detect_locale(), codex_home));
    }
    let started = Instant::now();

    // The multi-store path only supports whole-store scopes. MismatchedRows
    // relies on the single-store followup pass (see reconcile_once_with_progress)
    // that this path does not run; `sync` / `bucket switch` only pass AllRows or
    // None. Guard against a future caller wiring MismatchedRows through here.
    debug_assert!(
        rollout_scope != RolloutScope::MismatchedRows,
        "multi-store reconcile expects AllRows or None"
    );

    // 0) Backfill guard: a store whose Codex startup-backfill is still running
    //    after the bounded wait is skipped entirely — we neither collect its
    //    (possibly partial) rollout targets nor write its DB, so we never race
    //    the rebuild.
    let busy: HashSet<PathBuf> = targets
        .iter()
        .filter(|target| {
            wait_for_store_backfill(&target.db_path, backfill_wait) == BackfillReadiness::Busy
        })
        .map(|target| target.db_path.clone())
        .collect();

    // Rewriting any shared rollout JSONL (scope != None) while *any* store's
    // backfill is running races Codex's rebuild on its own source of truth — the
    // rollout files it is actively reading. Even a "ready" store's rollouts may
    // be referenced by the busy store's session. So if we would touch rollouts
    // and a backfill is in progress, skip the whole round and let the user re-run
    // once it completes. `--sqlite-only` (RolloutScope::None) touches no rollout,
    // so its ready stores can still be written below.
    if rollout_scope != RolloutScope::None && !busy.is_empty() {
        let stores = targets
            .iter()
            .map(|target| StoreReconcileResult {
                kind: target.kind,
                db_path: target.db_path.clone(),
                outcome: StoreOutcome::Skipped,
            })
            .collect();
        return Ok(MultiReconcileSummary {
            provider,
            stores,
            changed_rollouts: 0,
            checked_rollouts: 0,
            prepared_rollouts: 0,
            skipped_rollouts: 0,
            rollout_journal_path: None,
            elapsed: started.elapsed(),
        });
    }

    // 1) Take every ready store's backup before touching shared rollout JSONL.
    //    If a rollout-writing command cannot back up one selected store, skip
    //    the whole round so no store is left with rewritten rollouts but an old DB.
    let mut backup_paths: HashMap<PathBuf, PathBuf> = HashMap::new();
    let mut backup_failed: HashMap<PathBuf, String> = HashMap::new();
    for target in targets
        .iter()
        .filter(|target| !busy.contains(&target.db_path))
    {
        match create_store_backup(target) {
            Ok(backup_path) => {
                backup_paths.insert(target.db_path.clone(), backup_path);
            }
            Err(error) => {
                backup_failed.insert(target.db_path.clone(), error.to_string());
            }
        }
    }
    if rollout_scope != RolloutScope::None && !backup_failed.is_empty() {
        let stores = targets
            .iter()
            .map(|target| {
                if busy.contains(&target.db_path) {
                    return StoreReconcileResult {
                        kind: target.kind,
                        db_path: target.db_path.clone(),
                        outcome: StoreOutcome::Skipped,
                    };
                }
                let error = backup_failed
                    .get(&target.db_path)
                    .cloned()
                    .unwrap_or_else(|| {
                        "skipped because another store could not be backed up before rollout rewrite"
                            .to_string()
                    });
                StoreReconcileResult {
                    kind: target.kind,
                    db_path: target.db_path.clone(),
                    outcome: StoreOutcome::Failed { error },
                }
            })
            .collect();
        return Ok(MultiReconcileSummary {
            provider,
            stores,
            changed_rollouts: 0,
            checked_rollouts: 0,
            prepared_rollouts: 0,
            skipped_rollouts: 0,
            rollout_journal_path: None,
            elapsed: started.elapsed(),
        });
    }

    // 2) Rollout JSONL is the shared, durable source of truth. Collect targets
    //    across the ready, backed-up stores (deduped by canonical path) and
    //    rewrite once, before any SQLite row is flipped.
    let ready_db_paths: Vec<PathBuf> = targets
        .iter()
        .filter(|target| !busy.contains(&target.db_path))
        .filter(|target| !backup_failed.contains_key(&target.db_path))
        .map(|target| target.db_path.clone())
        .collect();
    let rollout_journal_path = codex_home
        .join("backups")
        .join(format!("rollouts.{}.jsonl", unix_timestamp_millis()?));
    let rollout_outcome = reconcile_rollouts_for_stores(
        ready_db_paths.as_slice(),
        provider.as_str(),
        rollout_scope,
        Some(rollout_journal_path.as_path()),
        padding_bytes,
        progress,
    )?;
    let rollout_summary = rollout_outcome.summary;
    let rollout_failed: HashMap<PathBuf, String> =
        rollout_outcome.failed_stores.into_iter().collect();

    // 3) Reconcile each ready, backed-up store's SQLite. A store mid-backfill is
    //    Skipped; one whose rollouts could not be read is Failed and left
    //    untouched (so we never flip a DB while its rollouts stay stale); other
    //    per-store failures are likewise reported without aborting healthy stores.
    let stores: Vec<StoreReconcileResult> = targets
        .iter()
        .map(|target| {
            if busy.contains(&target.db_path) {
                StoreReconcileResult {
                    kind: target.kind,
                    db_path: target.db_path.clone(),
                    outcome: StoreOutcome::Skipped,
                }
            } else if let Some(error) = backup_failed.get(&target.db_path) {
                StoreReconcileResult {
                    kind: target.kind,
                    db_path: target.db_path.clone(),
                    outcome: StoreOutcome::Failed {
                        error: error.clone(),
                    },
                }
            } else if let Some(error) = rollout_failed.get(&target.db_path) {
                StoreReconcileResult {
                    kind: target.kind,
                    db_path: target.db_path.clone(),
                    outcome: StoreOutcome::Failed {
                        error: error.clone(),
                    },
                }
            } else {
                let backup_path = backup_paths
                    .remove(&target.db_path)
                    .expect("backup was prepared for ready store");
                reconcile_single_store(target, provider.as_str(), backup_path)
            }
        })
        .collect();

    Ok(MultiReconcileSummary {
        provider,
        stores,
        changed_rollouts: rollout_summary.changed_files,
        checked_rollouts: rollout_summary.checked_files,
        prepared_rollouts: rollout_summary.prepared_files,
        skipped_rollouts: rollout_summary.skipped_files,
        rollout_journal_path: rollout_summary.journal_path,
        elapsed: started.elapsed(),
    })
}

fn create_store_backup(target: &StoreTarget) -> Result<PathBuf> {
    let backups_dir = target
        .db_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("backups")
        .join(target.kind.slug());
    create_sqlite_backup_file_in(&target.db_path, &backups_dir)
}

fn reconcile_single_store(
    target: &StoreTarget,
    provider: &str,
    backup_path: PathBuf,
) -> StoreReconcileResult {
    let outcome = match reconcile_single_store_inner(target, provider, backup_path) {
        Ok((changed_rows, total_rows, backup_path)) => StoreOutcome::Updated {
            changed_rows,
            total_rows,
            backup_path: Some(backup_path),
        },
        Err(error) => StoreOutcome::Failed {
            error: error.to_string(),
        },
    };
    StoreReconcileResult {
        kind: target.kind,
        db_path: target.db_path.clone(),
        outcome,
    }
}

fn reconcile_single_store_inner(
    target: &StoreTarget,
    provider: &str,
    backup_path: PathBuf,
) -> Result<(u64, u64, PathBuf)> {
    let (changed_rows, total_rows) = reconcile_sqlite_in_place(&target.db_path, provider)?;
    Ok((changed_rows, total_rows, backup_path))
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

fn add_rollout_summary(summary: &mut RolloutReconcileSummary, extra: RolloutReconcileSummary) {
    summary.checked_files += extra.checked_files;
    summary.changed_files += extra.changed_files;
    summary.prepared_files += extra.prepared_files;
    summary.skipped_files += extra.skipped_files;
}
