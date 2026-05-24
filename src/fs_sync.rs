use anyhow::Context;
use anyhow::Result;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;

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
