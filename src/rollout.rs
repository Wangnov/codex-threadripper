use anyhow::Context;
use anyhow::Result;
use filetime::FileTime;
use filetime::set_file_times;
use rusqlite::Connection;
use serde_json::Value;
use serde_json::json;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::IsTerminal;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use crate::codex_config::DEFAULT_PROVIDER;
use crate::fs_sync::sync_dir;
use crate::fs_sync::with_threadripper_lock;
use crate::locale::Locale;
use crate::output::RolloutProgressSnapshot;
use crate::output::rollout_progress_message;
use crate::state_db::ensure_sqlite_exists;
use crate::state_db::unix_timestamp_millis;
use crate::stores::StoreFilter;
use crate::stores::discover_stores;

const ROLLOUT_PROGRESS_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RolloutScope {
    None,
    MismatchedRows,
    AllRows,
}

#[derive(Debug)]
struct RolloutTarget {
    thread_id: String,
    path: PathBuf,
}

#[derive(Debug, Default)]
pub(crate) struct RolloutReconcileSummary {
    pub(crate) checked_files: u64,
    pub(crate) changed_files: u64,
    pub(crate) prepared_files: u64,
    pub(crate) skipped_files: u64,
    pub(crate) journal_path: Option<PathBuf>,
}

#[derive(Debug)]
pub(crate) struct BucketPrepareSummary {
    pub(crate) checked_rollouts: u64,
    pub(crate) prepared_rollouts: u64,
    pub(crate) skipped_rollouts: u64,
    pub(crate) elapsed: Duration,
    pub(crate) journal_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RolloutProgressConfig {
    pub(crate) locale: Locale,
}

struct RolloutProgress {
    config: RolloutProgressConfig,
    total_files: u64,
    visited_files: u64,
    started_at: Instant,
    last_print_at: Instant,
    is_terminal: bool,
    printed: bool,
}

#[derive(Debug)]
struct FirstLine {
    content: Vec<u8>,
    newline: Vec<u8>,
}

impl RolloutProgress {
    fn new(config: RolloutProgressConfig, total_files: u64) -> Self {
        let now = Instant::now();
        Self {
            config,
            total_files,
            visited_files: 0,
            started_at: now,
            last_print_at: now,
            is_terminal: std::io::stderr().is_terminal(),
            printed: false,
        }
    }

    fn tick(&mut self, summary: &RolloutReconcileSummary) {
        if self.total_files == 0 {
            return;
        }
        self.visited_files += 1;
        let now = Instant::now();
        let should_print = self.visited_files == 1
            || self.visited_files == self.total_files
            || now.duration_since(self.last_print_at) >= ROLLOUT_PROGRESS_INTERVAL;
        if should_print {
            self.print(summary, false);
            self.last_print_at = now;
        }
    }

    fn finish(&mut self, summary: &RolloutReconcileSummary) {
        if self.total_files == 0 {
            return;
        }
        self.print(summary, true);
    }

    fn print(&mut self, summary: &RolloutReconcileSummary, final_line: bool) {
        let snapshot = RolloutProgressSnapshot {
            visited_files: self.visited_files,
            total_files: self.total_files,
            checked_files: summary.checked_files,
            changed_files: summary.changed_files,
            prepared_files: summary.prepared_files,
            skipped_files: summary.skipped_files,
            elapsed: self.started_at.elapsed(),
        };
        let message = rollout_progress_message(self.config.locale, &snapshot);
        let mut stderr = std::io::stderr();
        if self.is_terminal {
            let _ = write!(stderr, "\r{message}\x1b[K");
            if final_line {
                let _ = writeln!(stderr);
            }
        } else if final_line || !self.printed {
            let _ = writeln!(stderr, "{message}");
        }
        let _ = stderr.flush();
        self.printed = true;
    }
}

#[derive(Debug, Default)]
struct RolloutPatchOutcome {
    changed: bool,
    prepared: bool,
    skipped: bool,
}

struct RolloutChangeJournal {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
}

#[derive(Clone, Copy)]
enum RolloutChangeMode {
    InPlace,
}

#[cfg(test)]
pub(crate) fn reconcile_rollout_metadata_from_sqlite_with_progress(
    sqlite_path: &Path,
    _codex_home: &Path,
    provider: &str,
    scope: RolloutScope,
    journal_path: Option<&Path>,
    padding_bytes: usize,
    progress: Option<RolloutProgressConfig>,
) -> Result<RolloutReconcileSummary> {
    if scope == RolloutScope::None {
        return Ok(RolloutReconcileSummary::default());
    }
    let targets = rollout_targets_for_scope(sqlite_path, provider, scope)?;
    reconcile_rollout_metadata_files(
        targets.as_slice(),
        provider,
        journal_path,
        padding_bytes,
        progress,
    )
}

/// Result of a multi-store rollout reconcile: the aggregate rewrite summary plus
/// the stores whose rollout targets could not be read.
#[derive(Debug, Default)]
pub(crate) struct MultiStoreRolloutOutcome {
    pub(crate) summary: RolloutReconcileSummary,
    /// `(db_path, error)` for each store whose rollout targets failed to load.
    /// Callers must treat these stores as failed and skip writing their SQLite,
    /// otherwise the DB could be flipped while its rollouts stayed stale.
    pub(crate) failed_stores: Vec<(PathBuf, String)>,
}

/// Reconcile rollout metadata across multiple store DBs.
///
/// CLI and App surfaces share `CODEX_HOME/sessions`, so the same rollout JSONL
/// can be referenced by more than one `state_5.sqlite`. Collect targets from
/// every store, de-duplicate by canonical rollout path, and rewrite each file's
/// provider exactly once (the rewrite is idempotent, but de-duping avoids
/// re-reading and double-counting shared files).
///
/// A store whose targets can't be read is recorded in `failed_stores` rather
/// than aborting the whole rewrite: healthy stores are still reconciled, and the
/// caller must skip the failed store's SQLite write so we never flip a DB whose
/// rollouts were left untouched.
pub(crate) fn reconcile_rollouts_for_stores(
    store_db_paths: &[PathBuf],
    provider: &str,
    scope: RolloutScope,
    journal_path: Option<&Path>,
    padding_bytes: usize,
    progress: Option<RolloutProgressConfig>,
) -> Result<MultiStoreRolloutOutcome> {
    if scope == RolloutScope::None {
        return Ok(MultiStoreRolloutOutcome::default());
    }
    let (targets, failed_stores) = rollout_targets_for_store_paths(store_db_paths, provider, scope);
    let summary = reconcile_rollout_metadata_files(
        targets.as_slice(),
        provider,
        journal_path,
        padding_bytes,
        progress,
    )?;
    Ok(MultiStoreRolloutOutcome {
        summary,
        failed_stores,
    })
}

fn rollout_targets_for_store_paths(
    store_db_paths: &[PathBuf],
    provider: &str,
    scope: RolloutScope,
) -> (Vec<RolloutTarget>, Vec<(PathBuf, String)>) {
    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut targets: Vec<RolloutTarget> = Vec::new();
    let mut failed_stores: Vec<(PathBuf, String)> = Vec::new();
    for db_path in store_db_paths {
        match rollout_targets_for_scope(db_path, provider, scope) {
            Ok(store_targets) => {
                for target in store_targets {
                    let key = target
                        .path
                        .canonicalize()
                        .unwrap_or_else(|_| target.path.clone());
                    if seen.insert(key) {
                        targets.push(target);
                    }
                }
            }
            Err(error) => failed_stores.push((db_path.clone(), error.to_string())),
        }
    }
    (targets, failed_stores)
}

fn rollout_targets_for_scope(
    sqlite_path: &Path,
    provider: &str,
    scope: RolloutScope,
) -> Result<Vec<RolloutTarget>> {
    ensure_sqlite_exists(sqlite_path)?;
    let connection = Connection::open(sqlite_path)
        .with_context(|| format!("failed to open {}", sqlite_path.display()))?;
    let sql = match scope {
        RolloutScope::None => return Ok(Vec::new()),
        RolloutScope::MismatchedRows => {
            "SELECT id, rollout_path FROM threads WHERE model_provider <> ?1 AND rollout_path <> '' ORDER BY updated_at DESC"
        }
        RolloutScope::AllRows => {
            "SELECT id, rollout_path FROM threads WHERE rollout_path <> '' ORDER BY updated_at DESC"
        }
    };
    let mut statement = connection.prepare(sql)?;
    if scope == RolloutScope::MismatchedRows {
        let rows = statement.query_map([provider], |row| {
            Ok(RolloutTarget {
                thread_id: row.get(0)?,
                path: PathBuf::from(row.get::<_, String>(1)?),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    } else {
        let rows = statement.query_map([], |row| {
            Ok(RolloutTarget {
                thread_id: row.get(0)?,
                path: PathBuf::from(row.get::<_, String>(1)?),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }
}

fn reconcile_rollout_metadata_files(
    targets: &[RolloutTarget],
    provider: &str,
    journal_path: Option<&Path>,
    padding_bytes: usize,
    progress: Option<RolloutProgressConfig>,
) -> Result<RolloutReconcileSummary> {
    let mut summary = RolloutReconcileSummary::default();
    let mut journal = journal_path.map(|path| RolloutChangeJournal::new(path.to_path_buf()));
    let mut progress = progress.map(|config| RolloutProgress::new(config, targets.len() as u64));
    for target in targets {
        if !target.path.exists() {
            if let Some(progress) = progress.as_mut() {
                progress.tick(&summary);
            }
            continue;
        }
        summary.checked_files += 1;
        let outcome =
            rewrite_rollout_provider_first_line(target, provider, journal.as_mut(), padding_bytes)?;
        if outcome.changed {
            summary.changed_files += 1;
        }
        if outcome.prepared {
            summary.prepared_files += 1;
        }
        if outcome.skipped {
            summary.skipped_files += 1;
        }
        if let Some(progress) = progress.as_mut() {
            progress.tick(&summary);
        }
    }
    if let Some(progress) = progress.as_mut() {
        progress.finish(&summary);
    }
    if let Some(journal) = journal {
        summary.journal_path = journal.finish()?;
    }
    Ok(summary)
}

fn rewrite_rollout_provider_first_line(
    target: &RolloutTarget,
    provider: &str,
    journal: Option<&mut RolloutChangeJournal>,
    padding_bytes: usize,
) -> Result<RolloutPatchOutcome> {
    let original_metadata = fs::metadata(&target.path)
        .with_context(|| format!("failed to stat {}", target.path.display()))?;
    let original_times = file_times_from_metadata(&original_metadata);
    let first_line = read_first_line(&target.path)?;
    let Some(mut value) =
        parse_matching_session_meta(first_line.content.as_slice(), target.thread_id.as_str())?
    else {
        return Ok(RolloutPatchOutcome {
            skipped: true,
            ..RolloutPatchOutcome::default()
        });
    };
    let old_provider = session_meta_provider(&value).unwrap_or("").to_string();

    if !set_session_meta_provider(&mut value, provider) {
        return Ok(RolloutPatchOutcome::default());
    }

    let rendered = serde_json::to_vec(&value)
        .with_context(|| format!("failed to render {}", target.path.display()))?;
    if rendered.len() <= first_line.content.len() {
        let mut replacement = rendered;
        replacement.resize(first_line.content.len(), b' ');
        record_rollout_change(
            journal,
            target,
            old_provider.as_str(),
            provider,
            first_line.content.len(),
            replacement.len(),
            RolloutChangeMode::InPlace,
        )?;
        patch_first_line_in_place(
            &target.path,
            replacement.as_slice(),
            first_line.newline.as_slice(),
            original_times,
        )?;
        return Ok(RolloutPatchOutcome {
            changed: true,
            ..RolloutPatchOutcome::default()
        });
    }

    let _ = padding_bytes;
    Ok(RolloutPatchOutcome {
        skipped: true,
        ..RolloutPatchOutcome::default()
    })
}

pub(crate) fn prepare_bucket_padding(
    codex_home: &Path,
    profile_override: Option<&str>,
    padding_bytes: usize,
) -> Result<BucketPrepareSummary> {
    with_threadripper_lock(codex_home, || {
        prepare_bucket_padding_unlocked(codex_home, profile_override, padding_bytes)
    })
}

fn prepare_bucket_padding_unlocked(
    codex_home: &Path,
    profile_override: Option<&str>,
    padding_bytes: usize,
) -> Result<BucketPrepareSummary> {
    let started = Instant::now();
    let store_db_paths = discover_stores(codex_home, profile_override, StoreFilter::All)?
        .into_iter()
        .map(|store| store.db_path)
        .collect::<Vec<_>>();
    if store_db_paths.is_empty() {
        anyhow::bail!(
            "no Codex state database found for bucket prepare under {}",
            codex_home.display()
        );
    }
    let (targets, failed_stores) =
        rollout_targets_for_store_paths(&store_db_paths, DEFAULT_PROVIDER, RolloutScope::AllRows);
    if !failed_stores.is_empty() {
        let details = failed_stores
            .into_iter()
            .map(|(path, error)| format!("{}: {error}", path.display()))
            .collect::<Vec<_>>()
            .join("; ");
        anyhow::bail!("failed to read rollout targets for bucket prepare: {details}");
    }
    let journal_path = codex_home
        .join("backups")
        .join(format!("bucket-prepare.{}.jsonl", unix_timestamp_millis()?));
    let mut journal = RolloutChangeJournal::new(journal_path);
    let mut checked_rollouts = 0;
    let mut prepared_rollouts = 0;
    let mut skipped_rollouts = 0;

    for target in &targets {
        if !target.path.exists() {
            continue;
        }
        checked_rollouts += 1;
        let outcome = prepare_rollout_first_line_padding(target, &mut journal, padding_bytes)?;
        if outcome.prepared {
            prepared_rollouts += 1;
        }
        if outcome.skipped {
            skipped_rollouts += 1;
        }
    }
    let journal_path = journal.finish()?;

    Ok(BucketPrepareSummary {
        checked_rollouts,
        prepared_rollouts,
        skipped_rollouts,
        elapsed: started.elapsed(),
        journal_path,
    })
}

fn prepare_rollout_first_line_padding(
    target: &RolloutTarget,
    _journal: &mut RolloutChangeJournal,
    padding_bytes: usize,
) -> Result<RolloutPatchOutcome> {
    let first_line = read_first_line(&target.path)?;
    let Some(value) =
        parse_matching_session_meta(first_line.content.as_slice(), target.thread_id.as_str())?
    else {
        return Ok(RolloutPatchOutcome {
            skipped: true,
            ..RolloutPatchOutcome::default()
        });
    };

    let rendered = serde_json::to_vec(&value)
        .with_context(|| format!("failed to render {}", target.path.display()))?;
    let desired_len = rendered.len() + padding_bytes;
    if first_line.content.len() >= desired_len {
        return Ok(RolloutPatchOutcome::default());
    }
    Ok(RolloutPatchOutcome {
        skipped: true,
        ..RolloutPatchOutcome::default()
    })
}

fn read_first_line(path: &Path) -> Result<FirstLine> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::new();
    reader
        .read_until(b'\n', &mut buffer)
        .with_context(|| format!("failed to read {}", path.display()))?;
    if buffer.is_empty() {
        anyhow::bail!("empty rollout file: {}", path.display());
    }

    let mut newline = Vec::new();
    if buffer.ends_with(b"\n") {
        buffer.pop();
        if buffer.ends_with(b"\r") {
            buffer.pop();
            newline.extend_from_slice(b"\r\n");
        } else {
            newline.push(b'\n');
        }
    }

    Ok(FirstLine {
        content: buffer,
        newline,
    })
}

fn parse_matching_session_meta(line: &[u8], thread_id: &str) -> Result<Option<Value>> {
    let value = serde_json::from_slice::<Value>(line)?;
    if session_meta_belongs_to_thread(&value, thread_id) {
        Ok(Some(value))
    } else {
        Ok(None)
    }
}

fn patch_first_line_in_place(
    path: &Path,
    replacement: &[u8],
    newline: &[u8],
    original_times: (FileTime, FileTime),
) -> Result<()> {
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        file.seek(SeekFrom::Start(0))
            .with_context(|| format!("failed to seek {}", path.display()))?;
        file.write_all(replacement)
            .with_context(|| format!("failed to write {}", path.display()))?;
        file.write_all(newline)
            .with_context(|| format!("failed to write {}", path.display()))?;
        file.sync_all()
            .with_context(|| format!("fsync: {}", path.display()))?;
    }
    restore_file_times(path, original_times)?;
    Ok(())
}

fn file_times_from_metadata(metadata: &fs::Metadata) -> (FileTime, FileTime) {
    (
        FileTime::from_last_access_time(metadata),
        FileTime::from_last_modification_time(metadata),
    )
}

fn restore_file_times(path: &Path, times: (FileTime, FileTime)) -> Result<()> {
    set_file_times(path, times.0, times.1)
        .with_context(|| format!("failed to restore file times for {}", path.display()))
}

fn record_rollout_change(
    journal: Option<&mut RolloutChangeJournal>,
    target: &RolloutTarget,
    old_provider: &str,
    new_provider: &str,
    old_len: usize,
    new_len: usize,
    mode: RolloutChangeMode,
) -> Result<()> {
    if let Some(journal) = journal {
        journal.record(target, old_provider, new_provider, old_len, new_len, mode)?;
    }
    Ok(())
}

impl RolloutChangeJournal {
    fn new(path: PathBuf) -> Self {
        Self { path, writer: None }
    }

    fn record(
        &mut self,
        target: &RolloutTarget,
        old_provider: &str,
        new_provider: &str,
        old_len: usize,
        new_len: usize,
        mode: RolloutChangeMode,
    ) -> Result<()> {
        if self.writer.is_none() {
            if let Some(parent) = self.path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            let file = File::create(&self.path)
                .with_context(|| format!("failed to create {}", self.path.display()))?;
            self.writer = Some(BufWriter::new(file));
        }

        let writer = self.writer.as_mut().expect("journal writer initialized");
        serde_json::to_writer(
            &mut *writer,
            &json!({
                "path": target.path.display().to_string(),
                "thread_id": target.thread_id.as_str(),
                "mode": mode.as_str(),
                "old_provider": old_provider,
                "new_provider": new_provider,
                "old_first_line_len": old_len,
                "new_first_line_len": new_len,
            }),
        )
        .with_context(|| format!("failed to write {}", self.path.display()))?;
        writer
            .write_all(b"\n")
            .with_context(|| format!("failed to write {}", self.path.display()))?;
        Ok(())
    }

    fn finish(mut self) -> Result<Option<PathBuf>> {
        let Some(mut writer) = self.writer.take() else {
            return Ok(None);
        };
        writer
            .flush()
            .with_context(|| format!("failed to flush {}", self.path.display()))?;
        writer
            .get_ref()
            .sync_all()
            .with_context(|| format!("fsync: {}", self.path.display()))?;
        if let Some(parent) = self.path.parent() {
            sync_dir(parent)?;
        }
        Ok(Some(self.path))
    }
}

impl RolloutChangeMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::InPlace => "in_place",
        }
    }
}

fn session_meta_belongs_to_thread(value: &Value, thread_id: &str) -> bool {
    value.get("type").and_then(Value::as_str) == Some("session_meta")
        && value
            .get("payload")
            .and_then(|payload| payload.get("id"))
            .and_then(Value::as_str)
            == Some(thread_id)
}

fn session_meta_provider(value: &Value) -> Option<&str> {
    value
        .get("payload")
        .and_then(|payload| payload.get("model_provider"))
        .and_then(Value::as_str)
}

fn set_session_meta_provider(value: &mut Value, provider: &str) -> bool {
    let Some(payload) = value.get_mut("payload").and_then(Value::as_object_mut) else {
        return false;
    };
    if payload
        .get("model_provider")
        .and_then(Value::as_str)
        .is_some_and(|current| current == provider)
    {
        return false;
    }
    payload.insert(
        "model_provider".to_string(),
        Value::String(provider.to_string()),
    );
    true
}
