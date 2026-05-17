use std::path::Path;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Locale {
    En,
    ZhHans,
}

pub fn detect_locale() -> Locale {
    let mut candidates = Vec::new();
    if let Ok(value) = std::env::var("CODEX_THREADRIPPER_LANG") {
        candidates.push(value);
    }
    for key in ["LC_ALL", "LC_MESSAGES", "LANG"] {
        if let Ok(value) = std::env::var(key) {
            candidates.push(value);
        }
    }

    detect_locale_from_sources(
        candidates.iter().map(String::as_str),
        apple_languages_output().as_deref(),
    )
}

pub fn detect_locale_from_sources<'a, I>(candidates: I, apple_languages: Option<&str>) -> Locale
where
    I: IntoIterator<Item = &'a str>,
{
    for candidate in candidates {
        if let Some(locale) = parse_locale_tag(candidate) {
            return locale;
        }
    }
    if let Some(output) = apple_languages
        && let Some(locale) = parse_apple_languages(output)
    {
        return locale;
    }
    Locale::En
}

pub fn parse_locale_tag(input: &str) -> Option<Locale> {
    let normalized = input.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    if normalized.starts_with("zh") {
        return Some(Locale::ZhHans);
    }
    if normalized.starts_with("en") {
        return Some(Locale::En);
    }
    None
}

pub(crate) fn parse_apple_languages(output: &str) -> Option<Locale> {
    output
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'))
        .find_map(parse_locale_tag)
}

fn apple_languages_output() -> Option<String> {
    if !cfg!(target_os = "macos") {
        return None;
    }
    let output = std::process::Command::new("defaults")
        .args(["read", "-g", "AppleLanguages"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

// ── Help / about ──

pub fn root_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Keep Codex's SQLite state DB aligned with the current model_provider.",
        Locale::ZhHans => "让 Codex 的 SQLite 状态库持续对齐当前 model_provider。",
    }
}

pub fn root_long_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => {
            "codex-threadripper is a human-first maintenance tool for Codex thread history.\n\nIt reads the active provider from CODEX_HOME/config.toml and rewrites Codex's SQLite state DB so every thread stays in the same provider bucket. The DB defaults to CODEX_HOME/state_5.sqlite, but sqlite_home and CODEX_SQLITE_HOME are respected. That makes thread lists and resume flows stop fragmenting across providers.\n\nExamples:\n  codex-threadripper status\n  codex-threadripper sync\n  codex-threadripper watch\n  codex-threadripper install-service"
        }
        Locale::ZhHans => {
            "codex-threadripper 是一个面向人的 Codex 线程历史维护工具。\n\n它会读取 CODEX_HOME/config.toml 里的当前 provider，并改写 Codex 的 SQLite 状态库，让所有线程始终落在同一个 provider 桶里。状态库默认是 CODEX_HOME/state_5.sqlite，同时会尊重 sqlite_home 和 CODEX_SQLITE_HOME。这样线程列表和 resume 流程就不会再被 provider 切碎。\n\n示例：\n  codex-threadripper status\n  codex-threadripper sync\n  codex-threadripper watch\n  codex-threadripper install-service"
        }
    }
}

pub fn help_template(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "{before-help}{about-section}Usage: {usage}\n\n{all-args}{after-help}",
        Locale::ZhHans => "{before-help}{about-section}用法：{usage}\n\n{all-args}{after-help}",
    }
}

pub fn commands_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Commands",
        Locale::ZhHans => "命令",
    }
}

pub fn options_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Options",
        Locale::ZhHans => "选项",
    }
}

pub fn dir_value_name(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "DIR",
        Locale::ZhHans => "目录",
    }
}

pub fn provider_value_name(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "PROVIDER",
        Locale::ZhHans => "PROVIDER",
    }
}

pub fn milliseconds_value_name(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "MILLISECONDS",
        Locale::ZhHans => "毫秒",
    }
}

pub fn bytes_value_name(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "BYTES",
        Locale::ZhHans => "字节",
    }
}

pub fn codex_home_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Codex home directory. Defaults to $HOME/.codex.",
        Locale::ZhHans => "Codex home 目录。默认值是 $HOME/.codex。",
    }
}

pub fn provider_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Force a provider instead of reading model_provider from config.toml.",
        Locale::ZhHans => "强制指定 provider，跳过从 config.toml 读取 model_provider。",
    }
}

pub fn provider_empty_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "provider must contain at least one non-whitespace character",
        Locale::ZhHans => "provider 需要包含至少一个非空白字符",
    }
}

pub fn help_option_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Print help",
        Locale::ZhHans => "显示帮助信息",
    }
}

pub fn version_option_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Print version",
        Locale::ZhHans => "显示版本号",
    }
}

pub fn status_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Show the current config provider, SQLite distribution, and background service state.",
        Locale::ZhHans => "显示当前 config provider、SQLite 分布和后台服务状态。",
    }
}

pub fn sync_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Reconcile Codex's SQLite state DB and rollout metadata once right now.",
        Locale::ZhHans => "立刻收敛 Codex SQLite 状态库和 rollout 元数据。",
    }
}

pub fn bucket_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Manage the provider bucket label stored in rollout first lines.",
        Locale::ZhHans => "管理 rollout 首行里的 provider 可见桶标签。",
    }
}

pub fn bucket_prepare_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Reserve first-line padding so future provider bucket switches stay fast.",
        Locale::ZhHans => "给首行预留 padding，让后续 provider 桶切换保持快速。",
    }
}

pub fn bucket_switch_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Switch every thread into one provider bucket.",
        Locale::ZhHans => "把所有线程切到同一个 provider 可见桶。",
    }
}

pub fn bucket_switch_provider_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Target provider bucket. Defaults to --provider or config.toml.",
        Locale::ZhHans => "目标 provider 桶。默认读取 --provider 或 config.toml。",
    }
}

pub fn padding_bytes_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Extra JSON whitespace to reserve at the end of the rollout first line.",
        Locale::ZhHans => "在 rollout 首行末尾预留的额外 JSON 空白字节数。",
    }
}

pub fn watch_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Keep watching config.toml and keep reconciling new rows in SQLite and rollout metadata.",
        Locale::ZhHans => "持续监听 config.toml，并持续收敛 SQLite 与 rollout 新增元数据。",
    }
}

pub fn poll_interval_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "How often to reconcile SQLite while watching.",
        Locale::ZhHans => "watch 模式下，定时收敛 SQLite 的频率。",
    }
}

pub fn sqlite_only_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Only update Codex's SQLite state DB; leave rollout JSONL metadata untouched.",
        Locale::ZhHans => "只更新 Codex SQLite 状态库，不改 rollout JSONL 元数据。",
    }
}

pub fn print_plist_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Print the platform-specific background service config for this tool.",
        Locale::ZhHans => "打印这个工具在当前平台上的后台服务配置。",
    }
}

pub fn install_launchd_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Install and start the platform-specific background service.",
        Locale::ZhHans => "安装并启动当前平台上的后台服务。",
    }
}

pub fn uninstall_launchd_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Stop and remove the platform-specific background service.",
        Locale::ZhHans => "停止并移除当前平台上的后台服务。",
    }
}

pub fn launchd_poll_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Polling interval for the background watcher.",
        Locale::ZhHans => "后台 watcher 的轮询间隔。",
    }
}

pub fn current_exe_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "failed to resolve current executable",
        Locale::ZhHans => "无法解析当前可执行文件路径",
    }
}

// ── restore / prune / dry-run ──

pub fn restore_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Restore SQLite database from a previous backup.",
        Locale::ZhHans => "从之前的备份恢复 SQLite 数据库。",
    }
}

pub fn restore_backup_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Backup file to restore. List available backups if omitted.",
        Locale::ZhHans => "要恢复的备份文件。省略则列出可用备份。",
    }
}

pub fn prune_backups_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Remove old backups, keeping the N most recent.",
        Locale::ZhHans => "删除旧备份，保留最近 N 份。",
    }
}

pub fn prune_backups_keep_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Number of most recent backups to keep.",
        Locale::ZhHans => "保留最近备份的份数。",
    }
}

pub fn dry_run_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Preview changes without writing anything.",
        Locale::ZhHans => "预览模式，只显示将发生的更改而不实际写入。",
    }
}

pub fn status_desktop_cap_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Desktop 50-session cap",
        Locale::ZhHans => "Desktop 50 条上限",
    }
}

pub fn status_desktop_cap_warning(locale: Locale, total: u64) -> String {
    match locale {
        Locale::En => format!("Total threads ({total}) exceed the 50-session cap. Some threads may not appear in the Desktop sidebar."),
        Locale::ZhHans => format!("总线程数（{total}）超过 50 条上限，部分线程可能不会显示在 Desktop 侧栏中。"),
    }
}

pub fn restore_complete_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Restore complete",
        Locale::ZhHans => "恢复完成",
    }
}

pub fn prune_backups_removed_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Removed",
        Locale::ZhHans => "已删除",
    }
}

pub fn prune_backups_kept_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Kept",
        Locale::ZhHans => "已保留",
    }
}

pub fn restore_backup_path_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Restored from",
        Locale::ZhHans => "已从备份恢复",
    }
}

pub fn dry_run_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "[DRY RUN]",
        Locale::ZhHans => "[DRY RUN]",
    }
}

// ── Status labels ──

pub fn status_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Codex Threadripper",
        Locale::ZhHans => "Codex Threadripper",
    }
}

pub fn status_codex_home_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Codex home",
        Locale::ZhHans => "Codex home",
    }
}

pub fn status_config_file_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Config file",
        Locale::ZhHans => "配置文件",
    }
}

pub fn status_sqlite_file_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "SQLite file",
        Locale::ZhHans => "SQLite 文件",
    }
}

pub fn status_target_provider_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Target provider",
        Locale::ZhHans => "目标 provider",
    }
}

pub fn status_total_threads_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Total threads",
        Locale::ZhHans => "线程总数",
    }
}

pub fn status_rows_needing_reconcile_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rows needing reconcile",
        Locale::ZhHans => "待收敛行数",
    }
}

pub fn status_distribution_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Provider distribution:",
        Locale::ZhHans => "Provider 分布：",
    }
}

pub fn status_background_service_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Background service:",
        Locale::ZhHans => "后台服务：",
    }
}

pub fn status_service_manager_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Manager",
        Locale::ZhHans => "管理器",
    }
}

pub fn status_plist_path_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Config path",
        Locale::ZhHans => "配置路径",
    }
}

pub fn status_installed_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Installed",
        Locale::ZhHans => "已安装",
    }
}

pub fn status_loaded_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Running",
        Locale::ZhHans => "运行中",
    }
}

// ── Sync labels ──

pub fn sync_complete_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Sync complete",
        Locale::ZhHans => "同步完成",
    }
}

pub fn bucket_switch_complete_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Bucket switch complete",
        Locale::ZhHans => "可见桶切换完成",
    }
}

pub fn bucket_prepare_complete_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Bucket prepare complete",
        Locale::ZhHans => "可见桶准备完成",
    }
}

pub fn sync_rows_updated_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rows updated",
        Locale::ZhHans => "已更新行数",
    }
}

pub fn sync_rollouts_checked_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollouts checked",
        Locale::ZhHans => "已检查 rollout",
    }
}

pub fn sync_rollouts_updated_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollouts updated",
        Locale::ZhHans => "已更新 rollout",
    }
}

pub fn sync_rollouts_prepared_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollouts prepared",
        Locale::ZhHans => "已准备 rollout",
    }
}

pub fn sync_rollouts_skipped_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollouts skipped",
        Locale::ZhHans => "已跳过 rollout",
    }
}

pub fn sync_elapsed_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Elapsed",
        Locale::ZhHans => "耗时",
    }
}

pub fn sync_backup_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Backup",
        Locale::ZhHans => "备份文件",
    }
}

pub fn sync_rollout_journal_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollout first-line journal",
        Locale::ZhHans => "rollout 首行记录",
    }
}

// ── Watch labels ──

pub fn watch_started_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Watch started",
        Locale::ZhHans => "已开始监听",
    }
}

pub fn config_change_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Config change detected",
        Locale::ZhHans => "检测到配置变更",
    }
}

pub fn background_reconcile_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Background reconcile",
        Locale::ZhHans => "后台收敛",
    }
}

pub fn watch_running_message(locale: Locale, codex_home: &Path, poll_interval: Duration) -> String {
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

pub fn watch_initial_reconcile_error_message(locale: Locale, err: &anyhow::Error) -> String {
    match locale {
        Locale::En => format!(
            "Initial reconcile hit an error. Watch will keep running and retry on config changes or the next poll: {err:#}"
        ),
        Locale::ZhHans => {
            format!("首轮收敛遇到错误。watch 会继续运行，并在配置变更或下一次轮询时重试：{err:#}")
        }
    }
}

pub fn watch_reconcile_skipped_message(locale: Locale, err: &anyhow::Error) -> String {
    match locale {
        Locale::En => format!("Reconcile is waiting for the next retry: {err:#}"),
        Locale::ZhHans => format!("本轮收敛等待下一次重试：{err:#}"),
    }
}

pub fn watcher_error_message(locale: Locale, err: notify::Error) -> String {
    match locale {
        Locale::En => format!("Watcher error: {err}"),
        Locale::ZhHans => format!("监听器错误：{err}"),
    }
}

pub fn watcher_disconnected_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "watcher disconnected",
        Locale::ZhHans => "监听器已断开",
    }
}

pub fn watch_already_running_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "another watch is already running for this codex home",
        Locale::ZhHans => "这个 codex home 已经有另一个 watch 在运行",
    }
}

pub fn watch_stopped_message(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Watch stopped.",
        Locale::ZhHans => "监听已停止。",
    }
}

// ── Service labels ──

pub fn install_launchd_done(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Installed background service.",
        Locale::ZhHans => "已安装后台服务。",
    }
}

pub fn uninstall_launchd_done(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Removed background service.",
        Locale::ZhHans => "已移除后台服务。",
    }
}

pub fn launchd_label_message(locale: Locale, label: &str) -> String {
    match locale {
        Locale::En => format!("Service label: {label}"),
        Locale::ZhHans => format!("服务标签：{label}"),
    }
}

pub fn launchd_plist_message(locale: Locale, path: &Path) -> String {
    match locale {
        Locale::En => format!("Config path: {}", path.display()),
        Locale::ZhHans => format!("配置路径：{}", path.display()),
    }
}

pub fn launchd_codex_home_message(locale: Locale, path: &Path) -> String {
    match locale {
        Locale::En => format!("Codex home: {}", path.display()),
        Locale::ZhHans => format!("Codex home：{}", path.display()),
    }
}

pub fn launchd_polling_message(locale: Locale, poll_interval: Duration) -> String {
    match locale {
        Locale::En => format!("Polling every {} ms.", poll_interval.as_millis()),
        Locale::ZhHans => format!("轮询间隔：{} ms。", poll_interval.as_millis()),
    }
}

pub fn next_steps_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Next steps",
        Locale::ZhHans => "下一步",
    }
}

pub fn service_log_message(locale: Locale, path: &Path) -> String {
    match locale {
        Locale::En => format!("Log path: {}", path.display()),
        Locale::ZhHans => format!("日志路径：{}", path.display()),
    }
}

pub fn sqlite_missing_error(locale: Locale, path: &Path) -> String {
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

pub fn no_launchd_plist_message(locale: Locale, path: &Path) -> String {
    match locale {
        Locale::En => format!(
            "No background service config is installed at {}.",
            path.display()
        ),
        Locale::ZhHans => format!("{} 这里还没有安装后台服务配置。", path.display()),
    }
}

pub fn rollout_progress_message(
    locale: Locale,
    visited_files: u64,
    total_files: u64,
    checked_files: u64,
    changed_files: u64,
    prepared_files: u64,
    skipped_files: u64,
    elapsed: Duration,
) -> String {
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

pub fn no_service_label() -> &'static str {
    "(none)"
}

// ── Yes/No ──

pub fn yes_no(locale: Locale, value: bool) -> &'static str {
    match (locale, value) {
        (Locale::En, true) => "yes",
        (Locale::En, false) => "no",
        (Locale::ZhHans, true) => "是",
        (Locale::ZhHans, false) => "否",
    }
}

