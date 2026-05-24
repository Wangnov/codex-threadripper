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
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

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
        rollout_scope,
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

    while !shutdown.load(Ordering::Relaxed) {
        let timeout = next_poll_deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(timeout) {
            Ok(Ok(event)) => {
                if touches_config_file(&event, &config_path) {
                    match reconcile_once(
                        codex_home,
                        provider_override.as_deref(),
                        profile_override.as_deref(),
                        rollout_scope,
                    ) {
                        Ok(summary) => {
                            if last_provider.as_deref() != Some(summary.provider.as_str())
                                || summary.changed_rows > 0
                                || summary.changed_rollouts > 0
                            {
                                print_sync_summary(locale, config_change_title(locale), &summary);
                            }
                            last_provider = Some(summary.provider.clone());
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
                match reconcile_once(
                    codex_home,
                    provider_override.as_deref(),
                    profile_override.as_deref(),
                    rollout_scope,
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

fn touches_config_file(event: &notify::Event, config_path: &Path) -> bool {
    matches!(
        event.kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    ) && event.paths.iter().any(|path| path == config_path)
}
