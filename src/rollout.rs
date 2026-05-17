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

use anyhow::Context;
use anyhow::Result;
use filetime::FileTime;
use filetime::set_file_times;
use rusqlite::Connection;
use serde_json::Value;
use serde_json::json;

use crate::config::DEFAULT_PROVIDER;
use crate::sqlite;
use crate::types::*;

pub fn rollout_targets_for_scope(
    sqlite_path: &Path,
    provider: &str,
    scope: RolloutScope,
) -> Result<Vec<RolloutTarget>> {
    sqlite::ensure_sqlite_exists(sqlite_path)?;
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

pub fn reconcile_rollout_metadata_from_sqlite_with_progress(
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

pub fn reconcile_rollout_metadata_files(
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

pub fn read_first_line(path: &Path) -> Result<FirstLine> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
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

pub fn parse_matching_session_meta(line: &[u8], thread_id: &str) -> Result<Option<Value>> {
    let value = serde_json::from_slice::<Value>(line)?;
    if session_meta_belongs_to_thread(&value, thread_id) {
        Ok(Some(value))
    } else {
        Ok(None)
    }
}

pub fn session_meta_belongs_to_thread(value: &Value, thread_id: &str) -> bool {
    value.get("type").and_then(Value::as_str) == Some("session_meta")
        && value
            .get("payload")
            .and_then(|payload| payload.get("id"))
            .and_then(Value::as_str)
            == Some(thread_id)
}

pub fn session_meta_provider(value: &Value) -> Option<&str> {
    value
        .get("payload")
        .and_then(|payload| payload.get("model_provider"))
        .and_then(Value::as_str)
}

pub fn set_session_meta_provider(value: &mut Value, provider: &str) -> bool {
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

fn rewrite_rollout_provider_first_line(
    target: &RolloutTarget,
    provider: &str,
    journal: Option<&mut RolloutChangeJournal>,
    padding_bytes: usize,
) -> Result<RolloutPatchOutcome> {
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
        record_change(
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
        )?;
        return Ok(RolloutPatchOutcome {
            changed: true,
            ..RolloutPatchOutcome::default()
        });
    }

    let mut replacement = rendered;
    replacement.resize(replacement.len() + padding_bytes, b' ');
    record_change(
        journal,
        target,
        old_provider.as_str(),
        provider,
        first_line.content.len(),
        replacement.len(),
        RolloutChangeMode::RewriteWithPadding,
    )?;
    rewrite_first_line_atomically(
        &target.path,
        replacement.as_slice(),
        first_line.newline.as_slice(),
    )?;
    Ok(RolloutPatchOutcome {
        changed: true,
        prepared: true,
        ..RolloutPatchOutcome::default()
    })
}

pub fn prepare_bucket_padding(codex_home: &Path, padding_bytes: usize) -> Result<BucketPrepareSummary> {
    let sqlite_path = crate::config::resolve_sqlite_path(codex_home)?;
    let started = Instant::now();
    let targets = rollout_targets_for_scope(&sqlite_path, DEFAULT_PROVIDER, RolloutScope::AllRows)?;
    let journal_path = codex_home
        .join("backups")
        .join(format!("bucket-prepare.{}.jsonl", sqlite::unix_timestamp_millis()?));
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
    journal: &mut RolloutChangeJournal,
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
    let old_provider = session_meta_provider(&value).unwrap_or("").to_string();

    let rendered = serde_json::to_vec(&value)
        .with_context(|| format!("failed to render {}", target.path.display()))?;
    let desired_len = rendered.len() + padding_bytes;
    if first_line.content.len() >= desired_len {
        return Ok(RolloutPatchOutcome::default());
    }

    let mut replacement = rendered;
    replacement.resize(desired_len, b' ');
    record_change(
        Some(journal),
        target,
        old_provider.as_str(),
        old_provider.as_str(),
        first_line.content.len(),
        replacement.len(),
        RolloutChangeMode::RewriteWithPadding,
    )?;
    rewrite_first_line_atomically(
        &target.path,
        replacement.as_slice(),
        first_line.newline.as_slice(),
    )?;
    Ok(RolloutPatchOutcome {
        changed: true,
        prepared: true,
        ..RolloutPatchOutcome::default()
    })
}

fn patch_first_line_in_place(path: &Path, replacement: &[u8], newline: &[u8]) -> Result<()> {
    let original_times = capture_file_times(path)?;
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        file.seek(SeekFrom::Start(0))
            .with_context(|| format!("failed to seek {}", path.display()))?;
        file.write_all(replacement)
            .with_context(|| format!("failed to write {}", path.display()))?;
        file.write_all(newline)
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    restore_file_times(path, original_times)?;
    Ok(())
}

fn rewrite_first_line_atomically(path: &Path, replacement: &[u8], newline: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("rollout.jsonl");
    let temp_path = parent.join(format!(
        ".{file_name}.{}.threadripper.tmp",
        std::process::id()
    ));
    let metadata =
        std::fs::metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    let original_times = file_times_from_metadata(&metadata);

    {
        let input = std::fs::File::open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let mut reader = BufReader::new(input);
        let mut ignored_first_line = Vec::new();
        reader
            .read_until(b'\n', &mut ignored_first_line)
            .with_context(|| format!("failed to read {}", path.display()))?;

        let output = std::fs::File::create(&temp_path)
            .with_context(|| format!("failed to create {}", temp_path.display()))?;
        std::fs::set_permissions(&temp_path, metadata.permissions())
            .with_context(|| format!("failed to set permissions on {}", temp_path.display()))?;
        let mut writer = BufWriter::new(output);
        writer
            .write_all(replacement)
            .with_context(|| format!("failed to write {}", temp_path.display()))?;
        writer
            .write_all(newline)
            .with_context(|| format!("failed to write {}", temp_path.display()))?;
        std::io::copy(&mut reader, &mut writer)
            .with_context(|| format!("failed to copy {}", path.display()))?;
        writer
            .flush()
            .with_context(|| format!("failed to flush {}", temp_path.display()))?;
        writer
            .get_ref()
            .sync_all()
            .with_context(|| format!("fsync: {}", temp_path.display()))?;
    }

    std::fs::rename(&temp_path, path).with_context(|| {
        format!(
            "failed to replace {} with {}",
            path.display(),
            temp_path.display()
        )
    })?;
    restore_file_times(path, original_times)?;
    sqlite::sync_dir(parent)?;
    Ok(())
}

fn capture_file_times(path: &Path) -> Result<(FileTime, FileTime)> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("failed to stat {}", path.display()))?;
    Ok(file_times_from_metadata(&metadata))
}

fn file_times_from_metadata(metadata: &std::fs::Metadata) -> (FileTime, FileTime) {
    (
        FileTime::from_last_access_time(metadata),
        FileTime::from_last_modification_time(metadata),
    )
}

fn restore_file_times(path: &Path, times: (FileTime, FileTime)) -> Result<()> {
    set_file_times(path, times.0, times.1)
        .with_context(|| format!("failed to restore file times for {}", path.display()))
}

fn record_change(
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
    pub fn new(path: PathBuf) -> Self {
        Self { path, writer: None }
    }

    pub fn record(
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
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            let file = std::fs::File::create(&self.path)
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

    pub fn finish(mut self) -> Result<Option<PathBuf>> {
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
            sqlite::sync_dir(parent)?;
        }
        Ok(Some(self.path))
    }
}

impl RolloutChangeMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InPlace => "in_place",
            Self::RewriteWithPadding => "rewrite_with_padding",
        }
    }
}

// ── RolloutProgressConfig ──

#[derive(Clone, Copy)]
pub struct RolloutProgressConfig {
    pub locale: crate::locale::Locale,
}

// ── RolloutProgress ──

const ROLLOUT_PROGRESS_INTERVAL: Duration = Duration::from_millis(500);

pub struct RolloutProgress {
    config: RolloutProgressConfig,
    total_files: u64,
    visited_files: u64,
    started_at: Instant,
    last_print_at: Instant,
    is_terminal: bool,
    printed: bool,
}

impl RolloutProgress {
    pub fn new(config: RolloutProgressConfig, total_files: u64) -> Self {
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

    pub fn tick(&mut self, summary: &RolloutReconcileSummary) {
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

    pub fn finish(&mut self, summary: &RolloutReconcileSummary) {
        if self.total_files == 0 {
            return;
        }
        self.print(summary, true);
    }

    fn print(&mut self, summary: &RolloutReconcileSummary, final_line: bool) {
        use std::io::Write;
        let message = crate::locale::rollout_progress_message(
            self.config.locale,
            self.visited_files,
            self.total_files,
            summary.checked_files,
            summary.changed_files,
            summary.prepared_files,
            summary.skipped_files,
            self.started_at.elapsed(),
        );
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
