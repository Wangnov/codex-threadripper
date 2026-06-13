use anyhow::Context;
use anyhow::Result;
use rusqlite::Connection;
use rusqlite::OpenFlags;
use rusqlite::OptionalExtension;
use rusqlite::TransactionBehavior;
use rusqlite::backup::Backup;
use rusqlite::backup::StepResult;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::codex_config::STATE_DB_FILENAME;
use crate::fs_sync::sync_dir;
use crate::fs_sync::sync_file;
use crate::locale::Locale;
use crate::locale::detect_locale;

pub(crate) type ProviderDistribution = Vec<(String, u64)>;

pub(crate) fn inspect_sqlite_distribution(
    sqlite_path: &Path,
    provider: &str,
) -> Result<(u64, u64, ProviderDistribution)> {
    ensure_sqlite_exists(sqlite_path)?;
    let connection = Connection::open(sqlite_path)
        .with_context(|| format!("failed to open {}", sqlite_path.display()))?;
    let total_rows = connection.query_row("SELECT COUNT(*) FROM threads", [], |row| row.get(0))?;
    let mismatched_rows = connection.query_row(
        "SELECT COUNT(*) FROM threads WHERE model_provider <> ?1",
        [provider],
        |row| row.get(0),
    )?;
    let mut statement = connection.prepare(
        "SELECT model_provider, COUNT(*) AS row_count FROM threads GROUP BY model_provider ORDER BY row_count DESC, model_provider ASC",
    )?;
    let distribution = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<Vec<(String, u64)>>>()?;
    Ok((total_rows, mismatched_rows, distribution))
}

/// Read the Codex startup-backfill status for a store, if present.
///
/// Codex 26.609+ rebuilds the `state_5.sqlite` index from rollout files on
/// startup and records progress in a `backfill_state` table. Returns `Ok(None)`
/// when the table is absent (older Codex, or a DB that has never backfilled) so
/// callers can treat "no backfill machinery" and "complete" distinctly. Opens
/// read-only to avoid contending with an in-progress rebuild.
pub(crate) fn read_backfill_status(sqlite_path: &Path) -> Result<Option<String>> {
    read_backfill_status_with_timeout(sqlite_path, Duration::from_millis(2_000))
}

pub(crate) fn read_backfill_status_with_timeout(
    sqlite_path: &Path,
    busy_timeout: Duration,
) -> Result<Option<String>> {
    if !sqlite_path.exists() {
        return Ok(None);
    }
    let connection = Connection::open_with_flags(
        sqlite_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("failed to open {}", sqlite_path.display()))?;
    connection.busy_timeout(busy_timeout)?;
    let has_table: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'backfill_state'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if has_table.is_none() {
        return Ok(None);
    }
    let status: Option<String> = connection
        .query_row(
            "SELECT status FROM backfill_state WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(status)
}

pub(crate) fn reconcile_sqlite_in_place(sqlite_path: &Path, provider: &str) -> Result<(u64, u64)> {
    ensure_sqlite_exists(sqlite_path)?;
    let mut connection = Connection::open(sqlite_path)
        .with_context(|| format!("failed to open {}", sqlite_path.display()))?;
    connection.busy_timeout(Duration::from_millis(5_000))?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let total_rows: u64 =
        transaction.query_row("SELECT COUNT(*) FROM threads", [], |row| row.get(0))?;
    let changed_rows = transaction.execute(
        "UPDATE threads SET model_provider = ?1 WHERE model_provider <> ?1",
        [provider],
    )? as u64;
    transaction.commit()?;

    Ok((changed_rows, total_rows))
}

#[cfg(test)]
pub(crate) fn reconcile_sqlite_with_backup(
    sqlite_path: &Path,
    provider: &str,
) -> Result<(u64, u64, PathBuf)> {
    let backups_dir = sqlite_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("backups");
    let backup_path = create_sqlite_backup_file_in(sqlite_path, &backups_dir)?;
    let (changed_rows, total_rows) = reconcile_sqlite_in_place(sqlite_path, provider)?;
    Ok((changed_rows, total_rows, backup_path))
}

/// Back up `sqlite_path` into an explicit `backups_dir`. Multi-store sync uses
/// this with a per-store namespaced directory (`<db_parent>/backups/<store>/`)
/// so concurrent surfaces never clobber each other's backups.
pub(crate) fn create_sqlite_backup_file_in(
    sqlite_path: &Path,
    backups_dir: &Path,
) -> Result<PathBuf> {
    fs::create_dir_all(backups_dir)
        .with_context(|| format!("failed to create {}", backups_dir.display()))?;

    let timestamp = unix_timestamp_millis()?;
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
    sync_dir(backups_dir)?;

    Ok(backup_path)
}

pub(crate) fn unix_timestamp_millis() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is earlier than UNIX_EPOCH")?
        .as_millis())
}

fn create_sqlite_backup(sqlite_path: &Path, backup_path: &Path) -> Result<()> {
    ensure_sqlite_exists(sqlite_path)?;
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
    sync_file(backup_path)?;

    Ok(())
}

pub(crate) fn ensure_sqlite_exists(sqlite_path: &Path) -> Result<()> {
    if sqlite_path.exists() {
        return Ok(());
    }

    anyhow::bail!(sqlite_missing_error(detect_locale(), sqlite_path));
}

fn sqlite_missing_error(locale: Locale, path: &Path) -> String {
    match locale {
        Locale::En => format!(
            "database not found at {} — run Codex at least once to create it",
            path.display()
        ),
        Locale::ZhHans => format!(
            "未找到数据库 {} — 请先运行一次 Codex 以生成它",
            path.display()
        ),
    }
}
