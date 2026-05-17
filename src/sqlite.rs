use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use rusqlite::Connection;
use rusqlite::TransactionBehavior;

use crate::locale::sqlite_missing_error;
use crate::types::ProviderDistribution;

pub fn ensure_sqlite_exists(sqlite_path: &Path) -> Result<()> {
    if sqlite_path.exists() {
        return Ok(());
    }
    anyhow::bail!(sqlite_missing_error(
        crate::locale::detect_locale(),
        sqlite_path
    ));
}

pub fn inspect_sqlite_distribution(
    sqlite_path: &Path,
    target_provider: &str,
) -> Result<(u64, u64, ProviderDistribution)> {
    ensure_sqlite_exists(sqlite_path)?;
    let connection = Connection::open(sqlite_path)
        .with_context(|| format!("failed to open {}", sqlite_path.display()))?;
    let total_rows: u64 =
        connection.query_row("SELECT COUNT(*) FROM threads", [], |row| row.get(0))?;
    let mismatched_rows: u64 = connection.query_row(
        "SELECT COUNT(*) FROM threads WHERE model_provider <> ?1",
        [target_provider],
        |row| row.get(0),
    )?;
    let mut statement = connection.prepare(
        "SELECT model_provider, COUNT(*) AS row_count FROM threads GROUP BY model_provider ORDER BY row_count DESC, model_provider ASC",
    )?;
    let distribution = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok((total_rows, mismatched_rows, distribution))
}

pub fn reconcile_sqlite_in_place(sqlite_path: &Path, provider: &str) -> Result<(u64, u64)> {
    ensure_sqlite_exists(sqlite_path)?;
    let mut connection = Connection::open(sqlite_path)
        .with_context(|| format!("failed to open {}", sqlite_path.display()))?;
    connection.busy_timeout(Duration::from_secs(5))?;

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

pub fn sync_file(path: &Path) -> Result<()> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("open for sync: {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("fsync: {}", path.display()))?;
    Ok(())
}

#[cfg(unix)]
pub fn sync_dir(path: &Path) -> Result<()> {
    let dir = std::fs::File::open(path)
        .with_context(|| format!("open for sync: {}", path.display()))?;
    dir.sync_all()
        .with_context(|| format!("fsync: {}", path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
pub fn sync_dir(_path: &Path) -> Result<()> { Ok(()) }

pub fn unix_timestamp_millis() -> Result<u128> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is earlier than UNIX_EPOCH")?
        .as_millis())
}
