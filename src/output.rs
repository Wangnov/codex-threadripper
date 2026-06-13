use anyhow::Result;
use std::path::Path;
use std::time::Duration;

use crate::locale::Locale;
use crate::rollout::BucketPrepareSummary;
use crate::service;
use crate::service::ServiceInstallSummary;
use crate::service::ServiceManager;
use crate::sync::MultiReconcileSummary;
use crate::sync::ReconcileStatus;
use crate::sync::StatusSummary;
use crate::sync::StoreOutcome;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RolloutProgressSnapshot {
    pub(crate) visited_files: u64,
    pub(crate) total_files: u64,
    pub(crate) checked_files: u64,
    pub(crate) changed_files: u64,
    pub(crate) prepared_files: u64,
    pub(crate) skipped_files: u64,
    pub(crate) elapsed: Duration,
}

pub(crate) fn print_status(locale: Locale, summary: &StatusSummary) {
    println!("{}", status_title(locale));
    println!();
    println!(
        "{}: {}",
        status_codex_home_label(locale),
        summary.codex_home.display()
    );
    println!(
        "{}: {}",
        status_config_file_label(locale),
        summary.config_path.display()
    );
    println!(
        "{}: {}",
        status_target_provider_label(locale),
        summary.provider
    );
    println!();
    println!("{}", status_stores_heading(locale));
    for store in &summary.stores {
        println!();
        println!("  [{}] {}", store.kind.slug(), store.kind.label(locale));
        println!(
            "    {}: {}",
            status_sqlite_file_label(locale),
            store.db_path.display()
        );
        if let Some(error) = &store.error {
            println!("    {}: {}", status_store_error_label(locale), error);
            if let Some(backfill) = &store.backfill_status {
                println!("    {}: {}", status_backfill_label(locale), backfill);
            }
            continue;
        }
        println!(
            "    {}: {}",
            status_total_threads_label(locale),
            store.total_rows
        );
        println!(
            "    {}: {}",
            status_rows_needing_reconcile_label(locale),
            store.mismatched_rows
        );
        if let Some(backfill) = &store.backfill_status {
            println!("    {}: {}", status_backfill_label(locale), backfill);
        }
        println!("    {}", status_distribution_heading(locale));
        for (provider, count) in &store.distribution {
            println!("      {provider}: {count}");
        }
    }
    println!();
    println!("{}", status_split_note(locale));
    println!();
    println!("{}", status_background_service_heading(locale));
    println!(
        "  {}: {}",
        status_service_manager_label(locale),
        service::manager_name(summary.service_status.manager)
    );
    println!(
        "  {}: {}",
        status_plist_path_label(locale),
        summary.service_status.config_path.display()
    );
    println!(
        "  {}: {}",
        status_installed_label(locale),
        yes_no(locale, summary.service_status.installed)
    );
    println!(
        "  {}: {}",
        status_loaded_label(locale),
        yes_no(locale, summary.service_status.running)
    );
}

pub(crate) fn rollout_progress_message(
    locale: Locale,
    snapshot: &RolloutProgressSnapshot,
) -> String {
    let RolloutProgressSnapshot {
        visited_files,
        total_files,
        checked_files,
        changed_files,
        prepared_files,
        skipped_files,
        elapsed,
    } = *snapshot;
    match locale {
        Locale::En => format!(
            "Rollouts: scanned {visited_files}/{total_files}, checked {checked_files}, updated {changed_files}, prepared {prepared_files}, skipped {skipped_files}, elapsed {} ms",
            elapsed.as_millis()
        ),
        Locale::ZhHans => format!(
            "rollout 进度: 已扫描 {visited_files}/{total_files}，已检查 {checked_files}，已更新 {changed_files}，已准备 {prepared_files}，已跳过 {skipped_files}，耗时 {} ms",
            elapsed.as_millis()
        ),
    }
}

pub(crate) fn print_multi_sync_summary(
    locale: Locale,
    title: &str,
    summary: &MultiReconcileSummary,
) {
    println!("{title}");
    println!(
        "{}: {}",
        status_target_provider_label(locale),
        summary.provider
    );
    if summary.checked_rollouts > 0 || summary.changed_rollouts > 0 {
        println!(
            "{}: {}",
            sync_rollouts_checked_label(locale),
            summary.checked_rollouts
        );
        println!(
            "{}: {}",
            sync_rollouts_updated_label(locale),
            summary.changed_rollouts
        );
        if summary.prepared_rollouts > 0 {
            println!(
                "{}: {}",
                sync_rollouts_prepared_label(locale),
                summary.prepared_rollouts
            );
        }
        if summary.skipped_rollouts > 0 {
            println!(
                "{}: {}",
                sync_rollouts_skipped_label(locale),
                summary.skipped_rollouts
            );
        }
    }

    println!();
    println!("{}", sync_stores_heading(locale));
    for store in &summary.stores {
        println!();
        println!("  [{}] {}", store.kind.slug(), store.kind.label(locale));
        println!(
            "    {}: {}",
            status_sqlite_file_label(locale),
            store.db_path.display()
        );
        match &store.outcome {
            StoreOutcome::Updated {
                changed_rows,
                total_rows,
                backup_path,
            } => {
                println!("    {}: {}", sync_rows_updated_label(locale), changed_rows);
                println!("    {}: {}", status_total_threads_label(locale), total_rows);
                if let Some(backup_path) = backup_path {
                    println!(
                        "    {}: {}",
                        sync_backup_label(locale),
                        backup_path.display()
                    );
                }
            }
            StoreOutcome::Skipped => {
                println!("    {}", sync_store_skipped_label(locale));
            }
            StoreOutcome::Failed { error } => {
                println!("    {}: {}", sync_store_failed_label(locale), error);
            }
        }
    }

    println!();
    if let Some(journal_path) = &summary.rollout_journal_path {
        println!(
            "{}: {}",
            sync_rollout_journal_label(locale),
            journal_path.display()
        );
    }
    println!(
        "{}: {} ms",
        sync_elapsed_label(locale),
        summary.elapsed.as_millis()
    );
    println!("{}", reconcile_status_line(locale, summary.status()));
}

pub(crate) fn print_bucket_prepare_summary(locale: Locale, summary: &BucketPrepareSummary) {
    println!("{}", bucket_prepare_complete_title(locale));
    println!(
        "{}: {}",
        sync_rollouts_checked_label(locale),
        summary.checked_rollouts
    );
    println!(
        "{}: {}",
        sync_rollouts_prepared_label(locale),
        summary.prepared_rollouts
    );
    if summary.skipped_rollouts > 0 {
        println!(
            "{}: {}",
            sync_rollouts_skipped_label(locale),
            summary.skipped_rollouts
        );
    }
    println!(
        "{}: {} ms",
        sync_elapsed_label(locale),
        summary.elapsed.as_millis()
    );
    if let Some(journal_path) = &summary.journal_path {
        println!(
            "{}: {}",
            sync_rollout_journal_label(locale),
            journal_path.display()
        );
    }
}

pub(crate) fn yes_no(locale: Locale, value: bool) -> &'static str {
    match (locale, value) {
        (Locale::En, true) => "yes",
        (Locale::En, false) => "no",
        (Locale::ZhHans, true) => "是",
        (Locale::ZhHans, false) => "否",
    }
}

pub(crate) fn root_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Keep Codex's SQLite state DB aligned with the current model_provider.",
        Locale::ZhHans => "让 Codex 的 SQLite 状态库持续对齐当前 model_provider。",
    }
}

pub(crate) fn root_long_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => {
            "codex-threadripper is a human-first maintenance tool for Codex thread history.\n\nIt reads the effective Codex provider from CODEX_HOME/config.toml, including the selected profile config when present, and rewrites Codex's SQLite state DB so every thread stays in the same provider bucket. The DB defaults to CODEX_HOME/state_5.sqlite, but sqlite_home and CODEX_SQLITE_HOME are respected. That makes thread lists and resume flows stop fragmenting across providers.\n\nExamples:\n  codex-threadripper status\n  codex-threadripper sync\n  codex-threadripper watch\n  codex-threadripper install-service"
        }
        Locale::ZhHans => {
            "codex-threadripper 是一个面向人的 Codex 线程历史维护工具。\n\n它会读取 CODEX_HOME/config.toml 和已选 profile 配置合成后的有效 provider，并改写 Codex 的 SQLite 状态库，让所有线程始终落在同一个 provider 桶里。状态库默认是 CODEX_HOME/state_5.sqlite，同时会尊重 sqlite_home 和 CODEX_SQLITE_HOME。这样线程列表和 resume 流程就不会再被 provider 切碎。\n\n示例：\n  codex-threadripper status\n  codex-threadripper sync\n  codex-threadripper watch\n  codex-threadripper install-service"
        }
    }
}

pub(crate) fn help_template(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "{before-help}{about-section}Usage: {usage}\n\n{all-args}{after-help}",
        Locale::ZhHans => "{before-help}{about-section}用法：{usage}\n\n{all-args}{after-help}",
    }
}

pub(crate) fn commands_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Commands",
        Locale::ZhHans => "命令",
    }
}

pub(crate) fn options_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Options",
        Locale::ZhHans => "选项",
    }
}

pub(crate) fn dir_value_name(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "DIR",
        Locale::ZhHans => "目录",
    }
}

pub(crate) fn provider_value_name(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "PROVIDER",
        Locale::ZhHans => "PROVIDER",
    }
}

pub(crate) fn profile_value_name(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "PROFILE",
        Locale::ZhHans => "PROFILE",
    }
}

pub(crate) fn milliseconds_value_name(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "MILLISECONDS",
        Locale::ZhHans => "毫秒",
    }
}

pub(crate) fn bytes_value_name(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "BYTES",
        Locale::ZhHans => "字节",
    }
}

pub(crate) fn codex_home_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Codex home directory. Defaults to CODEX_HOME, then $HOME/.codex.",
        Locale::ZhHans => "Codex home 目录。默认值依次使用 CODEX_HOME、$HOME/.codex。",
    }
}

pub(crate) fn provider_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Force a provider instead of reading model_provider from config.toml.",
        Locale::ZhHans => "强制指定 provider，跳过从 config.toml 读取 model_provider。",
    }
}

pub(crate) fn profile_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => {
            "Resolve model_provider and sqlite_home from a Codex profile config first. Profile names may contain ASCII letters, digits, '_' or '-'."
        }
        Locale::ZhHans => {
            "优先从 Codex profile 配置解析 model_provider 和 sqlite_home。profile 名称可包含 ASCII 字母、数字、'_' 或 '-'。"
        }
    }
}

pub(crate) fn provider_empty_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "provider must contain at least one non-whitespace character",
        Locale::ZhHans => "provider 需要包含至少一个非空白字符",
    }
}

pub(crate) fn profile_empty_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "profile must contain at least one non-whitespace character",
        Locale::ZhHans => "profile 需要包含至少一个非空白字符",
    }
}

pub(crate) fn profile_path_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => {
            "profile must be a plain name containing only ASCII letters, digits, '_' or '-'"
        }
        Locale::ZhHans => "profile 需要是只包含 ASCII 字母、数字、'_' 或 '-' 的普通名称",
    }
}

pub(crate) fn help_option_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Print help",
        Locale::ZhHans => "显示帮助信息",
    }
}

pub(crate) fn version_option_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Print version",
        Locale::ZhHans => "显示版本号",
    }
}

pub(crate) fn status_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => {
            "Show the current config provider, SQLite distribution, and background service state."
        }
        Locale::ZhHans => "显示当前 config provider、SQLite 分布和后台服务状态。",
    }
}

pub(crate) fn sync_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Reconcile Codex's SQLite state DB and rollout metadata once right now.",
        Locale::ZhHans => "立刻收敛 Codex SQLite 状态库和 rollout 元数据。",
    }
}

pub(crate) fn bucket_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Manage the provider bucket label stored in rollout first lines.",
        Locale::ZhHans => "管理 rollout 首行里的 provider 可见桶标签。",
    }
}

pub(crate) fn bucket_prepare_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Inspect first-line padding without growing live rollout files.",
        Locale::ZhHans => "检查首行 padding，但不扩容仍可能在写入的 rollout 文件。",
    }
}

pub(crate) fn bucket_switch_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Switch every thread into one provider bucket.",
        Locale::ZhHans => "把所有线程切到同一个 provider 可见桶。",
    }
}

pub(crate) fn bucket_switch_provider_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Target provider bucket. Defaults to --provider or config.toml.",
        Locale::ZhHans => "目标 provider 桶。默认读取 --provider 或 config.toml。",
    }
}

pub(crate) fn padding_bytes_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Extra JSON whitespace to reserve at the end of the rollout first line.",
        Locale::ZhHans => "在 rollout 首行末尾预留的额外 JSON 空白字节数。",
    }
}

pub(crate) fn watch_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => {
            "Keep watching config.toml and keep reconciling new rows in SQLite and rollout metadata."
        }
        Locale::ZhHans => "持续监听 config.toml，并持续收敛 SQLite 与 rollout 新增元数据。",
    }
}

pub(crate) fn poll_interval_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "How often to reconcile SQLite while watching.",
        Locale::ZhHans => "watch 模式下，定时收敛 SQLite 的频率。",
    }
}

pub(crate) fn sqlite_only_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => {
            "Only update Codex's SQLite state DB; leave rollout JSONL metadata untouched."
        }
        Locale::ZhHans => "只更新 Codex SQLite 状态库，不改 rollout JSONL 元数据。",
    }
}

pub(crate) fn print_plist_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Print the platform-specific background service config for this tool.",
        Locale::ZhHans => "打印这个工具在当前平台上的后台服务配置。",
    }
}

pub(crate) fn install_launchd_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Install and start the platform-specific background service.",
        Locale::ZhHans => "安装并启动当前平台上的后台服务。",
    }
}

pub(crate) fn uninstall_launchd_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Stop and remove the platform-specific background service.",
        Locale::ZhHans => "停止并移除当前平台上的后台服务。",
    }
}

pub(crate) fn launchd_poll_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Polling interval for the background watcher.",
        Locale::ZhHans => "后台 watcher 的轮询间隔。",
    }
}

pub(crate) fn current_exe_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "failed to resolve current executable",
        Locale::ZhHans => "无法解析当前可执行文件路径",
    }
}

pub(crate) fn sync_complete_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Sync complete",
        Locale::ZhHans => "同步完成",
    }
}

pub(crate) fn bucket_switch_complete_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Bucket switch complete",
        Locale::ZhHans => "可见桶切换完成",
    }
}

pub(crate) fn bucket_prepare_complete_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Bucket prepare complete",
        Locale::ZhHans => "可见桶准备完成",
    }
}

pub(crate) fn watch_started_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Watch started",
        Locale::ZhHans => "已开始监听",
    }
}

pub(crate) fn config_change_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Config change detected",
        Locale::ZhHans => "检测到配置变更",
    }
}

pub(crate) fn background_reconcile_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Background reconcile",
        Locale::ZhHans => "后台收敛",
    }
}

pub(crate) fn watch_running_message(
    locale: Locale,
    codex_home: &Path,
    poll_interval: Duration,
) -> String {
    match locale {
        Locale::En => format!(
            "Watching {} with a {} ms poll interval. Press Ctrl-C to stop.",
            codex_home.display(),
            poll_interval.as_millis()
        ),
        Locale::ZhHans => format!(
            "正在监听 {}，轮询间隔 {} ms。按 Ctrl-C 停止。",
            codex_home.display(),
            poll_interval.as_millis()
        ),
    }
}

pub(crate) fn watch_initial_reconcile_error_message(locale: Locale, err: &anyhow::Error) -> String {
    match locale {
        Locale::En => format!(
            "Initial reconcile hit an error. Watch will keep running and retry on config changes or the next poll: {err:#}"
        ),
        Locale::ZhHans => {
            format!("首轮收敛遇到错误。watch 会继续运行，并在配置变更或下一次轮询时重试：{err:#}")
        }
    }
}

pub(crate) fn watch_reconcile_skipped_message(locale: Locale, err: &anyhow::Error) -> String {
    match locale {
        Locale::En => format!("Reconcile is waiting for the next retry: {err:#}"),
        Locale::ZhHans => format!("本轮收敛等待下一次重试：{err:#}"),
    }
}

pub(crate) fn watcher_error_message(locale: Locale, err: notify::Error) -> String {
    match locale {
        Locale::En => format!("Watcher error: {err}"),
        Locale::ZhHans => format!("监听器错误：{err}"),
    }
}

pub(crate) fn watcher_disconnected_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "watcher disconnected",
        Locale::ZhHans => "监听器已断开",
    }
}

pub(crate) fn watch_already_running_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "another watch is already running for this codex home",
        Locale::ZhHans => "这个 codex home 已经有另一个 watch 在运行",
    }
}

pub(crate) fn watch_stopped_message(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Watch stopped.",
        Locale::ZhHans => "监听已停止。",
    }
}

pub(crate) fn install_launchd_done(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Installed background service.",
        Locale::ZhHans => "已安装后台服务。",
    }
}

pub(crate) fn uninstall_launchd_done(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Removed background service.",
        Locale::ZhHans => "已移除后台服务。",
    }
}

pub(crate) fn launchd_label_message(locale: Locale, label: &str) -> String {
    match locale {
        Locale::En => format!("Service label: {label}"),
        Locale::ZhHans => format!("服务标签：{label}"),
    }
}

pub(crate) fn launchd_plist_message(locale: Locale, path: &Path) -> String {
    match locale {
        Locale::En => format!("Config path: {}", path.display()),
        Locale::ZhHans => format!("配置路径：{}", path.display()),
    }
}

pub(crate) fn launchd_codex_home_message(locale: Locale, path: &Path) -> String {
    match locale {
        Locale::En => format!("Codex home: {}", path.display()),
        Locale::ZhHans => format!("Codex home：{}", path.display()),
    }
}

pub(crate) fn launchd_polling_message(locale: Locale, poll_interval: Duration) -> String {
    match locale {
        Locale::En => format!("Polling every {} ms.", poll_interval.as_millis()),
        Locale::ZhHans => format!("轮询间隔：{} ms。", poll_interval.as_millis()),
    }
}

pub(crate) fn next_steps_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Next steps",
        Locale::ZhHans => "下一步",
    }
}

pub(crate) fn print_install_service_summary(
    locale: Locale,
    codex_home: &Path,
    poll_interval: Duration,
    summary: &ServiceInstallSummary,
) {
    println!("{}", install_launchd_done(locale));
    println!(
        "{}: {}",
        status_service_manager_label(locale),
        service::manager_name(summary.manager)
    );
    println!("{}", launchd_label_message(locale, service::SERVICE_LABEL));
    println!("{}", launchd_plist_message(locale, &summary.config_path));
    println!("{}", launchd_codex_home_message(locale, codex_home));
    println!("{}", launchd_polling_message(locale, poll_interval));
    println!("{}", service_log_message(locale, &summary.log_path));
}

pub(crate) fn install_next_steps(
    locale: Locale,
    exe_path: &Path,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    manager: ServiceManager,
) -> Result<Vec<String>> {
    let status_command =
        cli_status_command(exe_path, codex_home, provider_override, profile_override);
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

pub(crate) fn cli_status_command(
    exe_path: impl AsRef<Path>,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
) -> String {
    let mut parts = vec![
        shell_quote(exe_path.as_ref().display().to_string()),
        "--codex-home".to_string(),
        shell_quote(codex_home.display().to_string()),
    ];
    if let Some(provider) = provider_override {
        parts.push("--provider".to_string());
        parts.push(shell_quote(provider.to_string()));
    }
    if let Some(profile) = profile_override {
        parts.push("--profile".to_string());
        parts.push(shell_quote(profile.to_string()));
    }
    parts.push("status".to_string());
    parts.join(" ")
}

pub(crate) fn shell_quote(input: String) -> String {
    if input
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-'))
    {
        return input;
    }
    format!("'{}'", input.replace('\'', r"'\''"))
}

pub(crate) fn run_status_next_step(locale: Locale, command: &str) -> String {
    match locale {
        Locale::En => format!("Run this to verify status: {command}"),
        Locale::ZhHans => format!("运行这条命令查看状态：{command}"),
    }
}

pub(crate) fn inspect_service_next_step(
    locale: Locale,
    manager: ServiceManager,
    command: &str,
) -> String {
    let manager_name = service::manager_name(manager);
    match locale {
        Locale::En => format!("Run this to inspect the {manager_name} service: {command}"),
        Locale::ZhHans => format!("运行这条命令查看 {manager_name} 服务：{command}"),
    }
}

pub(crate) fn tail_log_command(manager: ServiceManager, log_path: &Path) -> String {
    match manager {
        ServiceManager::WindowsStartup => format!(
            "powershell -NoProfile -Command \"Get-Content -Path '{}' -Wait\"",
            log_path.display()
        ),
        _ => format!("tail -f {}", log_path.display()),
    }
}

pub(crate) fn tail_log_next_step(locale: Locale, command: &str) -> String {
    match locale {
        Locale::En => format!("Run this to watch the live log: {command}"),
        Locale::ZhHans => format!("运行这条命令查看实时日志：{command}"),
    }
}

pub(crate) fn no_launchd_plist_message(locale: Locale, path: &Path) -> String {
    match locale {
        Locale::En => format!(
            "No background service config is installed at {}.",
            path.display()
        ),
        Locale::ZhHans => format!("{} 这里还没有安装后台服务配置。", path.display()),
    }
}

pub(crate) fn status_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Codex Threadripper",
        Locale::ZhHans => "Codex Threadripper",
    }
}

pub(crate) fn status_codex_home_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Codex home",
        Locale::ZhHans => "Codex home",
    }
}

pub(crate) fn status_config_file_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Config file",
        Locale::ZhHans => "配置文件",
    }
}

pub(crate) fn status_sqlite_file_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "SQLite file",
        Locale::ZhHans => "SQLite 文件",
    }
}

pub(crate) fn status_target_provider_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Target provider",
        Locale::ZhHans => "目标 provider",
    }
}

pub(crate) fn status_total_threads_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Total threads",
        Locale::ZhHans => "线程总数",
    }
}

pub(crate) fn status_rows_needing_reconcile_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rows needing reconcile",
        Locale::ZhHans => "待收敛行数",
    }
}

pub(crate) fn status_distribution_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Provider distribution:",
        Locale::ZhHans => "Provider 分布：",
    }
}

pub(crate) fn status_stores_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Storage surfaces:",
        Locale::ZhHans => "存储面：",
    }
}

pub(crate) fn status_backfill_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rebuild (backfill) status",
        Locale::ZhHans => "重建（backfill）状态",
    }
}

pub(crate) fn status_store_error_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Inspection failed",
        Locale::ZhHans => "检查失败",
    }
}

pub(crate) fn status_split_note(locale: Locale) -> &'static str {
    match locale {
        Locale::En => {
            "Note: threadripper normalizes the provider within each store; it does not merge CLI and App histories across stores."
        }
        Locale::ZhHans => {
            "说明：threadripper 只在每个库内部归一 provider，不会跨库合并 CLI 与 App 的历史。"
        }
    }
}

pub(crate) fn sync_stores_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Stores:",
        Locale::ZhHans => "存储面：",
    }
}

pub(crate) fn sync_store_failed_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Failed",
        Locale::ZhHans => "失败",
    }
}

pub(crate) fn sync_store_skipped_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => {
            "Skipped — a Codex backfill has not completed; threadripper avoids racing the rebuild. Re-run once Codex finishes (if it keeps skipping, check whether the backfill is stuck)."
        }
        Locale::ZhHans => {
            "已跳过 —— Codex backfill 尚未完成；threadripper 不与重建竞态。待 Codex 完成后重跑（若持续跳过，请检查 backfill 是否卡住）。"
        }
    }
}

pub(crate) fn reconcile_status_line(locale: Locale, status: ReconcileStatus) -> String {
    match (status, locale) {
        (ReconcileStatus::Full, Locale::En) => "Result: all stores updated.".to_string(),
        (ReconcileStatus::Full, Locale::ZhHans) => "结果：所有库均已更新。".to_string(),
        (ReconcileStatus::Partial, Locale::En) => {
            "Result: PARTIAL — some stores updated, at least one was skipped or failed (see above). Re-run after it is resolved.".to_string()
        }
        (ReconcileStatus::Partial, Locale::ZhHans) => {
            "结果：部分成功 —— 部分库已更新，至少一个被跳过或失败（见上）。解决后请重跑。".to_string()
        }
        (ReconcileStatus::Failed, Locale::En) => {
            "Result: FAILED — no store could be updated.".to_string()
        }
        (ReconcileStatus::Failed, Locale::ZhHans) => {
            "结果：失败 —— 没有任何库被更新。".to_string()
        }
    }
}

pub(crate) fn sqlite_only_app_warning(locale: Locale) -> &'static str {
    match locale {
        Locale::En => {
            "Warning: --sqlite-only edits to the Codex App store take effect immediately but may be reverted by Codex's startup backfill, because the rollout JSONL is the source of truth. Run a full sync (without --sqlite-only) to persist the change."
        }
        Locale::ZhHans => {
            "警告：--sqlite-only 对 Codex App 库的改动会立即生效，但可能被 Codex 启动时的 backfill 从 rollout 还原（rollout 才是事实源）。要持久化请运行完整 sync（不加 --sqlite-only）。"
        }
    }
}

pub(crate) fn status_background_service_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Background service:",
        Locale::ZhHans => "后台服务：",
    }
}

pub(crate) fn status_service_manager_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Manager",
        Locale::ZhHans => "管理器",
    }
}

pub(crate) fn status_plist_path_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Config path",
        Locale::ZhHans => "配置路径",
    }
}

pub(crate) fn status_installed_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Installed",
        Locale::ZhHans => "已安装",
    }
}

pub(crate) fn status_loaded_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Running",
        Locale::ZhHans => "运行中",
    }
}

pub(crate) fn sync_rows_updated_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rows updated",
        Locale::ZhHans => "已更新行数",
    }
}

pub(crate) fn sync_rollouts_checked_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollouts checked",
        Locale::ZhHans => "已检查 rollout",
    }
}

pub(crate) fn sync_rollouts_updated_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollouts updated",
        Locale::ZhHans => "已更新 rollout",
    }
}

pub(crate) fn sync_rollouts_prepared_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollouts prepared",
        Locale::ZhHans => "已准备 rollout",
    }
}

pub(crate) fn sync_rollouts_skipped_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollouts skipped",
        Locale::ZhHans => "已跳过 rollout",
    }
}

pub(crate) fn sync_elapsed_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Elapsed",
        Locale::ZhHans => "耗时",
    }
}

pub(crate) fn sync_backup_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Backup",
        Locale::ZhHans => "备份文件",
    }
}

pub(crate) fn sync_rollout_journal_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollout first-line journal",
        Locale::ZhHans => "rollout 首行记录",
    }
}

pub(crate) fn service_log_message(locale: Locale, path: &Path) -> String {
    match locale {
        Locale::En => format!("Log path: {}", path.display()),
        Locale::ZhHans => format!("日志路径：{}", path.display()),
    }
}
