use anyhow::Context;
use anyhow::Result;
use fs2::FileExt;
#[cfg(unix)]
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;

pub(crate) fn with_threadripper_lock<T>(
    codex_home: &Path,
    action: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let lock_path = codex_home.join("threadripper.lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("failed to open {}", lock_path.display()))?;
    lock.lock_exclusive()
        .with_context(|| format!("failed to lock {}", lock_path.display()))?;
    let result = action();
    lock.unlock()
        .with_context(|| format!("failed to unlock {}", lock_path.display()))?;
    result
}

pub(crate) fn sync_file(path: &Path) -> Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("open for sync: {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("fsync: {}", path.display()))?;
    Ok(())
}

#[cfg(unix)]
pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    let dir = File::open(path).with_context(|| format!("open for sync: {}", path.display()))?;
    dir.sync_all()
        .with_context(|| format!("fsync: {}", path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn sync_dir(_path: &Path) -> Result<()> {
    Ok(())
}
