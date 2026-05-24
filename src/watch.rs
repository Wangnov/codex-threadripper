use anyhow::Context;
use anyhow::Result;
use fs2::FileExt;
use notify::Config as NotifyConfig;
use notify::EventKind;
use notify::RecommendedWatcher;
use notify::RecursiveMode;
use notify::Watcher;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use crate::codex_config::selected_profile_config_path;
use crate::locale::Locale;
use crate::output::background_reconcile_title;
use crate::output::config_change_title;
use crate::output::print_sync_summary;
use crate::output::watch_already_running_error;
use crate::output::watch_initial_reconcile_error_message;
use crate::output::watch_reconcile_skipped_message;
use crate::output::watch_running_message;
use crate::output::watch_started_title;
use crate::output::watch_stopped_message;
use crate::output::watcher_disconnected_error;
use crate::output::watcher_error_message;
use crate::rollout::RolloutScope;
use crate::sync::reconcile_once;

pub(crate) const WATCH_FULL_ROLLOUT_POLL_INTERVALS: u64 = 120;

pub(crate) fn run_watch(
    locale: Locale,
    codex_home: &Path,
    provider_override: Option<String>,
    profile_override: Option<String>,
    rollout_scope: RolloutScope,
    poll_interval: Duration,
) -> Result<()> {
    let lock_path = codex_home.join("watch.lock");
    let watch_lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("failed to open {}", lock_path.display()))?;
    if let Err(err) = watch_lock.try_lock_exclusive() {
        if err.kind() == std::io::ErrorKind::WouldBlock {
            anyhow::bail!(watch_already_running_error(locale));
        }
        return Err(err).with_context(|| format!("failed to lock {}", lock_path.display()));
    }

    let config_path = codex_home.join("config.toml");
    let mut watched_paths = watched_config_paths(codex_home, profile_override.as_deref());
    let full_rollout_scope = full_watch_rollout_scope(rollout_scope);
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_for_handler = Arc::clone(&shutdown);
    ctrlc::set_handler(move || {
        shutdown_for_handler.store(true, Ordering::Relaxed);
    })?;

    let (tx, rx) = mpsc::channel();
    let mut watcher = RecommendedWatcher::new(
        move |event| {
            let _ = tx.send(event);
        },
        NotifyConfig::default(),
    )?;

    if let Some(parent) = config_path.parent()
        && parent.exists()
    {
        watcher.watch(parent, RecursiveMode::NonRecursive)?;
    }

    let mut last_provider = None;
    match reconcile_once(
        codex_home,
        provider_override.as_deref(),
        profile_override.as_deref(),
        full_rollout_scope,
    ) {
        Ok(summary) => {
            print_sync_summary(locale, watch_started_title(locale), &summary);
            last_provider = Some(summary.provider.clone());
        }
        Err(err) => {
            eprintln!("{}", watch_initial_reconcile_error_message(locale, &err));
        }
    }
    println!(
        "{}",
        watch_running_message(locale, codex_home, poll_interval)
    );

    let mut next_poll_deadline = Instant::now() + poll_interval;
    let mut poll_count = 0_u64;

    while !shutdown.load(Ordering::Relaxed) {
        let timeout = next_poll_deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(timeout) {
            Ok(Ok(event)) => {
                if touches_config_file(&event, watched_paths.as_slice()) {
                    match reconcile_once(
                        codex_home,
                        provider_override.as_deref(),
                        profile_override.as_deref(),
                        full_rollout_scope,
                    ) {
                        Ok(summary) => {
                            if last_provider.as_deref() != Some(summary.provider.as_str())
                                || summary.changed_rows > 0
                                || summary.changed_rollouts > 0
                            {
                                print_sync_summary(locale, config_change_title(locale), &summary);
                            }
                            last_provider = Some(summary.provider.clone());
                            watched_paths =
                                watched_config_paths(codex_home, profile_override.as_deref());
                        }
                        Err(err) => {
                            eprintln!("{}", watch_reconcile_skipped_message(locale, &err));
                        }
                    }
                    next_poll_deadline = Instant::now() + poll_interval;
                }
            }
            Ok(Err(err)) => {
                eprintln!("{}", watcher_error_message(locale, err));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                poll_count = poll_count.wrapping_add(1);
                match reconcile_once(
                    codex_home,
                    provider_override.as_deref(),
                    profile_override.as_deref(),
                    periodic_watch_rollout_scope(rollout_scope, poll_count),
                ) {
                    Ok(summary) => {
                        if last_provider.as_deref() != Some(summary.provider.as_str())
                            || summary.changed_rows > 0
                            || summary.changed_rollouts > 0
                        {
                            print_sync_summary(
                                locale,
                                background_reconcile_title(locale),
                                &summary,
                            );
                        }
                        last_provider = Some(summary.provider.clone());
                    }
                    Err(err) => {
                        eprintln!("{}", watch_reconcile_skipped_message(locale, &err));
                    }
                }
                next_poll_deadline = Instant::now() + poll_interval;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                anyhow::bail!(watcher_disconnected_error(locale));
            }
        }
    }
    println!("{}", watch_stopped_message(locale));
    Ok(())
}

pub(crate) fn periodic_watch_rollout_scope(
    rollout_scope: RolloutScope,
    poll_count: u64,
) -> RolloutScope {
    if poll_count.is_multiple_of(WATCH_FULL_ROLLOUT_POLL_INTERVALS) {
        return full_watch_rollout_scope(rollout_scope);
    }
    rollout_scope
}

pub(crate) fn full_watch_rollout_scope(rollout_scope: RolloutScope) -> RolloutScope {
    match rollout_scope {
        RolloutScope::None => RolloutScope::None,
        RolloutScope::MismatchedRows | RolloutScope::AllRows => RolloutScope::AllRows,
    }
}

pub(crate) fn watched_config_paths(
    codex_home: &Path,
    profile_override: Option<&str>,
) -> Vec<PathBuf> {
    let mut paths = vec![codex_home.join("config.toml")];
    if let Some(profile_path) = selected_profile_config_path(codex_home, profile_override) {
        paths.push(profile_path);
    }
    paths
}

fn touches_config_file(event: &notify::Event, config_paths: &[PathBuf]) -> bool {
    matches!(
        event.kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    ) && event
        .paths
        .iter()
        .any(|path| config_paths.iter().any(|config_path| path == config_path))
}
