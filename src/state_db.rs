use anyhow::Context;
use anyhow::Result;
use rusqlite::Connection;
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
    let backup_path = create_sqlite_backup_file(sqlite_path)?;
    let (changed_rows, total_rows) = reconcile_sqlite_in_place(sqlite_path, provider)?;
    Ok((changed_rows, total_rows, backup_path))
}

pub(crate) fn create_sqlite_backup_file(sqlite_path: &Path) -> Result<PathBuf> {
    let backups_dir = sqlite_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("backups");
    fs::create_dir_all(&backups_dir)
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
    sync_dir(&backups_dir)?;

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

#[derive(Debug)]
pub(crate) struct BackupEntry {
    pub(crate) path: PathBuf,
    pub(crate) timestamp_ms: Option<u128>,
}

pub(crate) fn list_backups(sqlite_path: &Path) -> Result<Vec<BackupEntry>> {
    let backups_dir = sqlite_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("backups");
    if !backups_dir.exists() {
        return Ok(Vec::new());
    }
    let mut entries: Vec<BackupEntry> = Vec::new();
    for entry in fs::read_dir(&backups_dir)
        .with_context(|| format!("failed to read {}", backups_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("bak") {
            continue;
        }
        let timestamp_ms = parse_backup_timestamp(&path);
        entries.push(BackupEntry { path, timestamp_ms });
    }
    entries.sort_by_key(|b| std::cmp::Reverse(b.timestamp_ms));
    Ok(entries)
}

fn parse_backup_timestamp(path: &Path) -> Option<u128> {
    let name = path.file_stem()?.to_str()?;
    let last_dot = name.rfind('.')?;
    let ts_str = &name[last_dot + 1..];
    ts_str.parse::<u128>().ok()
}

pub(crate) fn restore_sqlite_from_backup(sqlite_path: &Path, backup_path: &Path) -> Result<()> {
    if !backup_path.exists() {
        anyhow::bail!("backup file not found: {}", backup_path.display());
    }
    let source = Connection::open(backup_path)
        .with_context(|| format!("failed to open backup {}", backup_path.display()))?;
    source.query_row("SELECT COUNT(*) FROM threads", [], |_| Ok(()))?;

    if sqlite_path.exists() {
        fs::remove_file(sqlite_path)
            .with_context(|| format!("failed to remove {}", sqlite_path.display()))?;
    }
    fs::copy(backup_path, sqlite_path).with_context(|| {
        format!(
            "failed to copy {} to {}",
            backup_path.display(),
            sqlite_path.display()
        )
    })?;
    sync_file(sqlite_path)?;
    if let Some(parent) = sqlite_path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

pub(crate) fn prune_backups(entries: &[BackupEntry], keep: usize) -> Result<(usize, usize)> {
    let parseable: Vec<&BackupEntry> = entries
        .iter()
        .filter(|e| e.timestamp_ms.is_some())
        .collect();
    if parseable.len() <= keep {
        return Ok((0, parseable.len()));
    }
    let to_delete = &parseable[keep..];
    let deleted = to_delete.len();
    for entry in to_delete {
        fs::remove_file(&entry.path)
            .with_context(|| format!("failed to delete {}", entry.path.display()))?;
    }
    Ok((deleted, keep))
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
