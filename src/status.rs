use std::path::Path;
use std::time::Duration;

use anyhow::Result;

use crate::config::{read_provider_from_config, resolve_sqlite_path};
use crate::locale::{self, Locale};
use crate::service::{self, ServiceInstallSummary, ServiceManager};
use crate::sqlite::inspect_sqlite_distribution;
use crate::types::*;

pub fn collect_status(codex_home: &Path, provider_override: Option<&str>) -> Result<StatusSummary> {
    let config_path = codex_home.join("config.toml");
    let sqlite_path = resolve_sqlite_path(codex_home)?;
    let provider = match provider_override {
        Some(provider) => provider.to_string(),
        None => read_provider_from_config(codex_home)?,
    };
    let (total_rows, mismatched_rows, distribution) =
        inspect_sqlite_distribution(&sqlite_path, provider.as_str())?;
    let service_status = service::current_service_status()?;

    Ok(StatusSummary {
        codex_home: codex_home.to_path_buf(),
        sqlite_path,
        config_path,
        provider,
        total_rows,
        mismatched_rows,
        distribution,
        service_status,
        exceeds_desktop_cap: total_rows > 50,
    })
}

pub fn print_status(locale: Locale, summary: &StatusSummary) {
    println!("{}", locale::status_title(locale));
    println!();
    println!(
        "{}: {}",
        locale::status_codex_home_label(locale),
        summary.codex_home.display()
    );
    println!(
        "{}: {}",
        locale::status_config_file_label(locale),
        summary.config_path.display()
    );
    println!(
        "{}: {}",
        locale::status_sqlite_file_label(locale),
        summary.sqlite_path.display()
    );
    println!(
        "{}: {}",
        locale::status_target_provider_label(locale),
        summary.provider
    );
    println!(
        "{}: {}",
        locale::status_total_threads_label(locale),
        summary.total_rows
    );
    println!(
        "{}: {}",
        locale::status_rows_needing_reconcile_label(locale),
        summary.mismatched_rows
    );
    println!();
    println!("{}", locale::status_distribution_heading(locale));
    for (provider, count) in &summary.distribution {
        println!("  {provider}: {count}");
    }
    if summary.exceeds_desktop_cap {
        println!();
        println!("{}", locale::status_desktop_cap_heading(locale));
        println!("  {}", locale::status_desktop_cap_warning(locale, summary.total_rows));
    }
    println!();
    println!("{}", locale::status_background_service_heading(locale));
    println!(
        "  {}: {}",
        locale::status_service_manager_label(locale),
        service::manager_name(summary.service_status.manager)
    );
    println!(
        "  {}: {}",
        locale::status_plist_path_label(locale),
        summary.service_status.config_path.display()
    );
    println!(
        "  {}: {}",
        locale::status_installed_label(locale),
        locale::yes_no(locale, summary.service_status.installed)
    );
    println!(
        "  {}: {}",
        locale::status_loaded_label(locale),
        locale::yes_no(locale, summary.service_status.running)
    );
}

pub fn print_sync_summary(locale: Locale, title: &str, summary: &ReconcileSummary) {
    println!("{title}");
    println!(
        "{}: {}",
        locale::status_target_provider_label(locale),
        summary.provider
    );
    println!(
        "{}: {}",
        locale::sync_rows_updated_label(locale),
        summary.changed_rows
    );
    if summary.checked_rollouts > 0 || summary.changed_rollouts > 0 {
        println!(
            "{}: {}",
            locale::sync_rollouts_checked_label(locale),
            summary.checked_rollouts
        );
        println!(
            "{}: {}",
            locale::sync_rollouts_updated_label(locale),
            summary.changed_rollouts
        );
        if summary.prepared_rollouts > 0 {
            println!(
                "{}: {}",
                locale::sync_rollouts_prepared_label(locale),
                summary.prepared_rollouts
            );
        }
        if summary.skipped_rollouts > 0 {
            println!(
                "{}: {}",
                locale::sync_rollouts_skipped_label(locale),
                summary.skipped_rollouts
            );
        }
    }
    println!(
        "{}: {}",
        locale::status_total_threads_label(locale),
        summary.total_rows
    );
    println!(
        "{}: {} ms",
        locale::sync_elapsed_label(locale),
        summary.elapsed.as_millis()
    );
    if let Some(backup_path) = &summary.backup_path {
        println!("{}: {}", locale::sync_backup_label(locale), backup_path.display());
    }
    if let Some(journal_path) = &summary.rollout_journal_path {
        println!(
            "{}: {}",
            locale::sync_rollout_journal_label(locale),
            journal_path.display()
        );
    }
}

pub fn print_bucket_prepare_summary(locale: Locale, summary: &BucketPrepareSummary) {
    println!("{}", locale::bucket_prepare_complete_title(locale));
    println!(
        "{}: {}",
        locale::sync_rollouts_checked_label(locale),
        summary.checked_rollouts
    );
    println!(
        "{}: {}",
        locale::sync_rollouts_prepared_label(locale),
        summary.prepared_rollouts
    );
    if summary.skipped_rollouts > 0 {
        println!(
            "{}: {}",
            locale::sync_rollouts_skipped_label(locale),
            summary.skipped_rollouts
        );
    }
    println!(
        "{}: {} ms",
        locale::sync_elapsed_label(locale),
        summary.elapsed.as_millis()
    );
    if let Some(journal_path) = &summary.journal_path {
        println!(
            "{}: {}",
            locale::sync_rollout_journal_label(locale),
            journal_path.display()
        );
    }
}

pub fn print_restore_summary(locale: Locale, summary: &RestoreSummary) {
    if locale == Locale::ZhHans {
        println!("{}", locale::restore_complete_title(locale));
        println!("  SQLite 已从 {} 恢复", summary.backup_path.display());
    } else {
        println!("{}", locale::restore_complete_title(locale));
        println!("  SQLite restored from {}", summary.backup_path.display());
    }
}

pub fn print_prune_summary(locale: Locale, summary: &PruneSummary) {
    if locale == Locale::ZhHans {
        println!("{}", locale::prune_backups_kept_label(locale));
        println!("  已保留: {}", summary.kept);
        println!("  已删除: {}", summary.removed);
    } else {
        println!("{}", locale::prune_backups_kept_label(locale));
        println!("  Kept: {}", summary.kept);
        println!("  Removed: {}", summary.removed);
    }
}

pub fn print_install_service_summary(
    locale: Locale,
    codex_home: &Path,
    poll_interval: Duration,
    summary: &ServiceInstallSummary,
) {
    println!("{}", locale::install_launchd_done(locale));
    println!(
        "{}: {}",
        locale::status_service_manager_label(locale),
        service::manager_name(summary.manager)
    );
    println!("{}", locale::launchd_label_message(locale, service::SERVICE_LABEL));
    println!("{}", locale::launchd_plist_message(locale, &summary.config_path));
    println!("{}", locale::launchd_codex_home_message(locale, codex_home));
    println!("{}", locale::launchd_polling_message(locale, poll_interval));
    println!("{}", locale::service_log_message(locale, &summary.log_path));
}

pub fn install_next_steps(
    locale: Locale,
    exe_path: &Path,
    codex_home: &Path,
    manager: ServiceManager,
) -> Result<Vec<String>> {
    let status_command = cli_status_command(exe_path, codex_home);
    let log_path = service::log_path()?;
    let mut steps = vec![run_status_next_step(locale, &status_command)];
    if let Some(command) = service::current_service_inspect_command()? {
        steps.push(inspect_service_next_step(locale, manager, &command));
    }
    steps.push(tail_log_next_step(
        locale,
        &tail_log_command(manager, &log_path),
    ));
    Ok(steps)
}

pub fn cli_status_command(exe_path: impl AsRef<Path>, codex_home: &Path) -> String {
    format!(
        "{} --codex-home {} status",
        shell_quote(exe_path.as_ref().display().to_string()),
        shell_quote(codex_home.display().to_string())
    )
}

fn shell_quote(input: String) -> String {
    if input
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-'))
    {
        return input;
    }
    format!("'{}'", input.replace('\'', r"'\''"))
}

pub fn run_status_next_step(locale: Locale, command: &str) -> String {
    match locale {
        Locale::En => format!("Run this to verify status: {command}"),
        Locale::ZhHans => format!("运行这条命令查看状态：{command}"),
    }
}

pub fn inspect_service_next_step(locale: Locale, manager: ServiceManager, command: &str) -> String {
    let manager_name = service::manager_name(manager);
    match locale {
        Locale::En => format!("Run this to inspect the {manager_name} service: {command}"),
        Locale::ZhHans => format!("运行这条命令查看 {manager_name} 服务：{command}"),
    }
}

pub fn tail_log_command(manager: ServiceManager, log_path: &Path) -> String {
    match manager {
        ServiceManager::WindowsStartup => format!(
            "powershell -NoProfile -Command \"Get-Content -Path '{}' -Wait\"",
            log_path.display()
        ),
        _ => format!("tail -f {}", log_path.display()),
    }
}

pub fn tail_log_next_step(locale: Locale, command: &str) -> String {
    match locale {
        Locale::En => format!("Run this to watch the live log: {command}"),
        Locale::ZhHans => format!("运行这条命令查看实时日志：{command}"),
    }
}
