use crate::cli::APP_VERSION;
use crate::cli::Cli;
use crate::cli::Command;
use crate::cli::DEFAULT_BUCKET_PADDING_BYTES;
use crate::cli::DEFAULT_POLL_INTERVAL_MS;
use crate::cli::localized_command;
use crate::cli::validate_provider_override;
use crate::cli::validate_provider_override_args;
use crate::cli::validate_store_filter_rollout_scope;
use crate::cli::validate_store_filter_supported;
use crate::codex_config::read_provider_from_config;
use crate::codex_config::resolve_sqlite_home_from_config;
use crate::codex_config::resolve_sqlite_path;
use crate::locale::Locale;
use crate::locale::detect_locale_from_sources;
use crate::locale::parse_apple_languages;
use crate::locale::parse_locale_tag;
use crate::output::install_next_steps;
use crate::rollout::RolloutScope;
use crate::rollout::prepare_bucket_padding;
use crate::rollout::reconcile_rollout_metadata_from_sqlite_with_progress;
use crate::service::ServiceManager;
use crate::state_db::inspect_sqlite_distribution;
use crate::state_db::reconcile_sqlite_in_place;
use crate::state_db::reconcile_sqlite_with_backup;
use crate::stores::StoreFilter;
use crate::stores::StoreKind;
use crate::stores::discover_stores;
use crate::stores::discover_stores_with;
use crate::sync::MultiReconcileSummary;
use crate::sync::ReconcileStatus;
use crate::sync::StoreOutcome;
use crate::sync::StoreReconcileResult;
use crate::sync::collect_status;
use crate::sync::reconcile_all_stores;
use crate::sync::reconcile_all_stores_with_backup;
use crate::watch::WATCH_FULL_ROLLOUT_POLL_INTERVALS;
use crate::watch::full_watch_rollout_scope;
use crate::watch::periodic_watch_rollout_scope;
use crate::watch::watch_should_print_summary;
use crate::watch::watched_config_paths;
use anyhow::Result;
use clap::FromArgMatches;
use clap::error::ErrorKind;
use filetime::FileTime;
use filetime::set_file_times;
use rusqlite::Connection;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

#[test]
fn parses_supported_locale_tags() {
    assert_eq!(parse_locale_tag("zh_CN.UTF-8"), Some(Locale::ZhHans));
    assert_eq!(parse_locale_tag("zh-Hans-CN"), Some(Locale::ZhHans));
    assert_eq!(parse_locale_tag("en_US.UTF-8"), Some(Locale::En));
    assert_eq!(parse_locale_tag("en-GB"), Some(Locale::En));
    assert_eq!(parse_locale_tag("ja_JP.UTF-8"), None);
}

#[test]
fn parses_apple_languages_output() {
    let output = "(\n    \"zh-Hans-CN\",\n    \"en-US\"\n)\n";
    assert_eq!(parse_apple_languages(output), Some(Locale::ZhHans));
}

#[test]
fn detects_locale_from_sources() {
    let locale = detect_locale_from_sources(["", "zh_CN.UTF-8"], None);
    assert_eq!(locale, Locale::ZhHans);

    let locale = detect_locale_from_sources([""], Some("(\n    en-US\n)\n"));
    assert_eq!(locale, Locale::En);
}

#[test]
fn localizes_help_surface() {
    let mut command = localized_command(Locale::ZhHans);
    let mut help = Vec::new();
    command.write_long_help(&mut help).unwrap();
    let rendered = String::from_utf8(help).unwrap();
    assert!(rendered.contains("面向人的 Codex 线程历史维护工具"));
    assert!(rendered.contains("用法："));
    assert!(rendered.contains("命令:"));
    assert!(rendered.contains("选项:"));
    assert!(rendered.contains("安装并启动当前平台上的后台服务"));
    assert!(rendered.contains("强制指定 provider，跳过从 config.toml 读取 model_provider"));
    assert!(rendered.contains("显示帮助信息"));
    assert!(rendered.contains("显示版本号"));

    let mut command = localized_command(Locale::En);
    let mut help = Vec::new();
    command.write_long_help(&mut help).unwrap();
    let rendered = String::from_utf8(help).unwrap();
    assert!(rendered.contains("human-first maintenance tool for Codex thread history"));
    assert!(rendered.contains("Usage:"));
    assert!(rendered.contains("Commands:"));
    assert!(rendered.contains("Options:"));
    assert!(rendered.contains("Install and start the platform-specific background service"));
    assert!(
        rendered.contains("Force a provider instead of reading model_provider from config.toml")
    );
    assert!(rendered.contains("Print help"));
    assert!(rendered.contains("Print version"));
}

#[test]
fn subcommand_help_and_version_are_stable() {
    let command = localized_command(Locale::En);
    let help_error = command
        .clone()
        .try_get_matches_from(["codex-threadripper", "install-service", "--help"])
        .unwrap_err();
    assert_eq!(help_error.kind(), ErrorKind::DisplayHelp);

    let version_error = command
        .try_get_matches_from(["codex-threadripper", "install-service", "--version"])
        .unwrap_err();
    assert_eq!(version_error.kind(), ErrorKind::DisplayVersion);
    assert!(version_error.to_string().contains(APP_VERSION));
}

#[test]
fn reads_provider_from_config() -> Result<()> {
    let dir = tempfile::tempdir()?;
    fs::write(dir.path().join("config.toml"), "model_provider = \"vm\"\n")?;
    let provider = read_provider_from_config(dir.path(), None)?;
    assert_eq!(provider, "vm");
    Ok(())
}

#[test]
fn reads_provider_from_profile_v2_config() -> Result<()> {
    let dir = tempfile::tempdir()?;
    fs::write(
        dir.path().join("config.toml"),
        "model_provider = \"openai\"\n",
    )?;
    fs::write(
        dir.path().join("work.config.toml"),
        "model_provider = \"bedrock\"\nsqlite_home = \"work-state\"\n",
    )?;

    let provider = read_provider_from_config(dir.path(), Some("work"))?;
    let sqlite_path = resolve_sqlite_path(dir.path(), Some("work"))?;

    assert_eq!(provider, "bedrock");
    assert_eq!(sqlite_path, dir.path().join("work-state/state_5.sqlite"));
    Ok(())
}

#[test]
fn reads_provider_from_selected_legacy_profile() -> Result<()> {
    let dir = tempfile::tempdir()?;
    fs::write(
        dir.path().join("config.toml"),
        r#"
model_provider = "openai"
profile = "work"

[profiles.work]
model_provider = "local"
"#,
    )?;

    let provider = read_provider_from_config(dir.path(), None)?;

    assert_eq!(provider, "local");
    Ok(())
}

#[test]
fn errors_when_profile_config_is_missing() -> Result<()> {
    let dir = tempfile::tempdir()?;
    fs::write(
        dir.path().join("config.toml"),
        "model_provider = \"openai\"\n",
    )?;

    let err = read_provider_from_config(dir.path(), Some("missing")).unwrap_err();

    assert!(err.to_string().contains("profile `missing` was not found"));
    Ok(())
}

#[test]
fn falls_back_to_root_config_when_selected_profile_is_missing() -> Result<()> {
    let dir = tempfile::tempdir()?;
    fs::write(
        dir.path().join("config.toml"),
        "model_provider = \"openai\"\nsqlite_home = \"root-state\"\nprofile = \"missing\"\n",
    )?;

    let provider = read_provider_from_config(dir.path(), None)?;
    let sqlite_path = resolve_sqlite_path(dir.path(), None)?;

    assert_eq!(provider, "openai");
    assert_eq!(sqlite_path, dir.path().join("root-state/state_5.sqlite"));
    Ok(())
}

#[test]
fn defaults_to_openai_without_config() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let provider = read_provider_from_config(dir.path(), None)?;
    assert_eq!(provider, "openai");
    Ok(())
}

#[test]
fn defaults_to_openai_with_blank_provider_in_config() -> Result<()> {
    let dir = tempfile::tempdir()?;
    fs::write(dir.path().join("config.toml"), "model_provider = \"   \"\n")?;
    let provider = read_provider_from_config(dir.path(), None)?;
    assert_eq!(provider, "openai");
    Ok(())
}

#[test]
fn resolves_codex_home_from_cli_then_env_then_home() -> Result<()> {
    let env_dir = tempfile::tempdir()?;
    assert_eq!(
        crate::resolve_codex_home_from_env(
            Some(PathBuf::from("/cli-codex")),
            Some(env_dir.path().to_path_buf()),
            Some(PathBuf::from("/home")),
        )?,
        PathBuf::from("/cli-codex")
    );
    assert_eq!(
        crate::resolve_codex_home_from_env(
            None,
            Some(env_dir.path().to_path_buf()),
            Some(PathBuf::from("/home")),
        )?,
        env_dir.path().canonicalize()?
    );
    assert_eq!(
        crate::resolve_codex_home_from_env(
            None,
            Some(PathBuf::new()),
            Some(PathBuf::from("/home")),
        )?,
        PathBuf::from("/home/.codex")
    );
    assert_eq!(
        crate::resolve_codex_home_from_env(None, None, Some(PathBuf::from("/home")))?,
        PathBuf::from("/home/.codex")
    );
    let invalid_env_path = env_dir.path().join("not-a-directory");
    fs::write(&invalid_env_path, "not a directory")?;
    let err = crate::resolve_codex_home_from_env(
        None,
        Some(invalid_env_path.clone()),
        Some(PathBuf::from("/home")),
    )
    .unwrap_err();
    assert!(err.to_string().contains("CODEX_HOME must point"));
    assert!(
        err.to_string()
            .contains(&invalid_env_path.display().to_string())
    );
    Ok(())
}

#[test]
fn resolves_sqlite_path_from_config_sqlite_home() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let sqlite_home = dir.path().join("custom-state");
    fs::write(
        dir.path().join("config.toml"),
        sqlite_home_config(&sqlite_home),
    )?;

    let sqlite_path = resolve_sqlite_path(dir.path(), None)?;

    assert_eq!(sqlite_path, sqlite_home.join("state_5.sqlite"));
    Ok(())
}

#[test]
fn resolves_sqlite_home_env_after_config() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let current_dir = dir.path().join("project");

    let config_wins = resolve_sqlite_home_from_config(
        dir.path(),
        Some("configured-state"),
        Some("env-state"),
        current_dir.as_path(),
    );
    let env_fallback = resolve_sqlite_home_from_config(
        dir.path(),
        Some("   "),
        Some("env-state"),
        current_dir.as_path(),
    );
    let default_home =
        resolve_sqlite_home_from_config(dir.path(), None, None, current_dir.as_path());

    assert_eq!(config_wins, dir.path().join("configured-state"));
    assert_eq!(env_fallback, current_dir.join("env-state"));
    assert_eq!(default_home, dir.path());
    Ok(())
}

#[test]
fn discover_returns_cli_default_when_only_top_level_present() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let home = dir.path();
    fs::write(home.join("state_5.sqlite"), b"")?;

    let stores = discover_stores_with(home, None);

    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0].kind, StoreKind::Cli);
    Ok(())
}

#[test]
fn discover_returns_app_when_only_subdir_present() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let home = dir.path();
    fs::create_dir_all(home.join("sqlite"))?;
    fs::write(home.join("sqlite").join("state_5.sqlite"), b"")?;

    let stores = discover_stores_with(home, None);

    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0].kind, StoreKind::App);
    Ok(())
}

#[test]
fn discover_returns_app_and_cli_when_split() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let home = dir.path();
    fs::write(home.join("state_5.sqlite"), b"")?;
    fs::create_dir_all(home.join("sqlite"))?;
    fs::write(home.join("sqlite").join("state_5.sqlite"), b"")?;

    let stores = discover_stores_with(home, None);

    let kinds: Vec<StoreKind> = stores.iter().map(|store| store.kind).collect();
    assert_eq!(kinds, vec![StoreKind::App, StoreKind::Cli]);
    Ok(())
}

#[test]
fn discover_dedupes_configured_pointing_at_cli_default() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let home = dir.path();
    fs::write(home.join("state_5.sqlite"), b"")?;

    // Configured sqlite_home == codex_home resolves to the same file as the CLI
    // default; the canonical de-dupe must keep a single entry (Configured wins).
    let stores = discover_stores_with(home, Some(home));

    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0].kind, StoreKind::Configured);
    Ok(())
}

#[test]
fn discover_returns_empty_when_no_db_present() {
    let dir = tempfile::tempdir().expect("tempdir");
    let stores = discover_stores_with(dir.path(), None);
    assert!(stores.is_empty());
}

#[test]
fn discover_dedupes_configured_pointing_at_app_default() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let home = dir.path();
    fs::create_dir_all(home.join("sqlite"))?;
    fs::write(home.join("sqlite").join("state_5.sqlite"), b"")?;

    // Configured sqlite_home == <home>/sqlite resolves to the App default file;
    // Configured must win the label over App after the canonical de-dupe.
    let stores = discover_stores_with(home, Some(home.join("sqlite").as_path()));

    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0].kind, StoreKind::Configured);
    Ok(())
}

#[test]
fn store_filter_keeps_cli_when_configured_aliases_cli_default() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let home = dir.path();
    fs::write(home.join("config.toml"), "sqlite_home = \".\"\n")?;
    fs::write(home.join("state_5.sqlite"), b"")?;

    let stores = discover_stores(home, None, StoreFilter::Cli)?;

    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0].kind, StoreKind::Cli);
    let all = discover_stores(home, None, StoreFilter::All)?;
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].kind, StoreKind::Configured);
    Ok(())
}

#[test]
fn store_filter_keeps_app_when_configured_aliases_app_default() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let home = dir.path();
    fs::write(home.join("config.toml"), "sqlite_home = \"sqlite\"\n")?;
    fs::create_dir_all(home.join("sqlite"))?;
    fs::write(home.join("sqlite").join("state_5.sqlite"), b"")?;

    let stores = discover_stores(home, None, StoreFilter::App)?;

    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0].kind, StoreKind::App);
    let all = discover_stores(home, None, StoreFilter::All)?;
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].kind, StoreKind::Configured);
    Ok(())
}

#[test]
fn discover_orders_configured_app_cli_when_all_distinct() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let home = dir.path();
    fs::write(home.join("state_5.sqlite"), b"")?;
    fs::create_dir_all(home.join("sqlite"))?;
    fs::write(home.join("sqlite").join("state_5.sqlite"), b"")?;
    let configured = home.join("custom");
    fs::create_dir_all(&configured)?;
    fs::write(configured.join("state_5.sqlite"), b"")?;

    let stores = discover_stores_with(home, Some(configured.as_path()));

    let kinds: Vec<StoreKind> = stores.iter().map(|store| store.kind).collect();
    assert_eq!(
        kinds,
        vec![StoreKind::Configured, StoreKind::App, StoreKind::Cli]
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn discover_dedupes_symlinked_app_to_cli_default() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let home = dir.path();
    fs::write(home.join("state_5.sqlite"), b"")?;
    fs::create_dir_all(home.join("sqlite"))?;
    // The App path is a symlink to the CLI default file: the same physical DB.
    std::os::unix::fs::symlink(
        home.join("state_5.sqlite"),
        home.join("sqlite").join("state_5.sqlite"),
    )?;

    let stores = discover_stores_with(home, None);

    // Canonical de-dupe must collapse to a single store (the App candidate is
    // checked before Cli and wins), so PR2's write path never touches the same
    // physical file twice.
    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0].kind, StoreKind::App);
    Ok(())
}

#[test]
fn status_reports_broken_store_without_hiding_healthy_store() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let home = dir.path();
    isolate_process_sqlite_home(home)?;
    let cli_db = home.join("state_5.sqlite");
    seed_sqlite(&cli_db)?;
    let app_db = home.join("sqlite").join("state_5.sqlite");
    fs::create_dir_all(app_db.parent().unwrap())?;
    fs::write(&app_db, b"not a sqlite database")?;

    let summary = collect_status(home, Some("openai"), None, StoreFilter::All)?;

    let cli = summary
        .stores
        .iter()
        .find(|store| store.kind == StoreKind::Cli)
        .expect("cli store present");
    assert_eq!(cli.total_rows, 3);
    assert_eq!(cli.error, None);
    let app = summary
        .stores
        .iter()
        .find(|store| store.kind == StoreKind::App)
        .expect("app store present");
    assert!(app.error.is_some());
    Ok(())
}

#[test]
fn status_reports_missing_configured_store_alongside_default_store() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let home = dir.path();
    fs::write(
        home.join("config.toml"),
        "model_provider = \"openai\"\nsqlite_home = \"missing-state\"\n",
    )?;
    let cli_db = home.join("state_5.sqlite");
    seed_sqlite(&cli_db)?;

    let summary = collect_status(home, Some("openai"), None, StoreFilter::All)?;

    let configured = summary
        .stores
        .iter()
        .find(|store| store.kind == StoreKind::Configured)
        .expect("configured store present");
    assert!(configured.error.is_some());
    let cli = summary
        .stores
        .iter()
        .find(|store| store.kind == StoreKind::Cli)
        .expect("cli store present");
    assert_eq!(cli.error, None);
    Ok(())
}

#[test]
fn status_errors_when_every_store_is_broken() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let home = dir.path();
    fs::write(home.join("config.toml"), sqlite_home_config(home))?;
    fs::write(home.join("state_5.sqlite"), b"not a sqlite database")?;

    let err = collect_status(home, Some("openai"), None, StoreFilter::All).unwrap_err();

    assert!(err.to_string().contains("failed to inspect any"));
    Ok(())
}

#[test]
fn reconcile_all_stores_updates_every_surface_and_dedupes_rollout() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    isolate_process_sqlite_home(codex_home)?;

    // A single rollout JSONL shared by both stores' thread "1".
    let rollout_path = codex_home.join("sessions/2026/05/07/rollout-1.jsonl");
    fs::create_dir_all(rollout_path.parent().unwrap())?;
    fs::write(
        &rollout_path,
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"1\",\"model_provider\":\"cong\"}}        \n",
    )?;

    let cli_db = codex_home.join("state_5.sqlite");
    let app_db = codex_home.join("sqlite").join("state_5.sqlite");
    fs::create_dir_all(app_db.parent().unwrap())?;
    for db in [&cli_db, &app_db] {
        seed_sqlite(db)?;
        let connection = Connection::open(db)?;
        connection.execute(
            "UPDATE threads SET rollout_path = ?1, model_provider = 'cong' WHERE id = '1'",
            [rollout_path.display().to_string()],
        )?;
    }

    let summary = reconcile_all_stores_with_backup(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::AllRows,
        DEFAULT_BUCKET_PADDING_BYTES,
        Duration::from_millis(0),
        StoreFilter::All,
        None,
    )?;

    assert_eq!(summary.status(), ReconcileStatus::Full);
    let kinds: Vec<StoreKind> = summary.stores.iter().map(|store| store.kind).collect();
    assert!(kinds.contains(&StoreKind::App));
    assert!(kinds.contains(&StoreKind::Cli));
    for store in &summary.stores {
        assert!(matches!(store.outcome, StoreOutcome::Updated { .. }));
    }

    // Both DBs now report the target provider for thread "1".
    for db in [&cli_db, &app_db] {
        let connection = Connection::open(db)?;
        let provider: String = connection.query_row(
            "SELECT model_provider FROM threads WHERE id = '1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(provider, "openai");
    }

    // The shared rollout was rewritten exactly once (deduped across stores).
    assert_eq!(summary.changed_rollouts, 1);
    let rewritten = fs::read_to_string(&rollout_path)?;
    assert!(rewritten.contains("\"model_provider\":\"openai\""));

    // Backups are namespaced per store.
    assert!(codex_home.join("backups/cli").exists());
    assert!(codex_home.join("sqlite/backups/app").exists());
    Ok(())
}

#[test]
fn reconcile_all_stores_reports_partial_when_a_store_is_unreadable() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    isolate_process_sqlite_home(codex_home)?;

    // Healthy CLI store with a mismatched provider.
    let cli_db = codex_home.join("state_5.sqlite");
    seed_sqlite(&cli_db)?;
    {
        let connection = Connection::open(&cli_db)?;
        connection.execute(
            "UPDATE threads SET model_provider = 'cong' WHERE id = '1'",
            [],
        )?;
    }
    // App store exists but is not a valid SQLite database.
    let app_db = codex_home.join("sqlite").join("state_5.sqlite");
    fs::create_dir_all(app_db.parent().unwrap())?;
    fs::write(&app_db, b"not a sqlite database")?;

    let summary = reconcile_all_stores_with_backup(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::None,
        DEFAULT_BUCKET_PADDING_BYTES,
        Duration::from_millis(0),
        StoreFilter::All,
        None,
    )?;

    assert_eq!(summary.status(), ReconcileStatus::Partial);
    let cli = summary
        .stores
        .iter()
        .find(|store| store.kind == StoreKind::Cli)
        .expect("cli store present");
    assert!(matches!(cli.outcome, StoreOutcome::Updated { .. }));
    let app = summary
        .stores
        .iter()
        .find(|store| store.kind == StoreKind::App)
        .expect("app store present");
    assert!(matches!(app.outcome, StoreOutcome::Failed { .. }));

    // The healthy store was still updated despite the broken one.
    let connection = Connection::open(&cli_db)?;
    let provider: String = connection.query_row(
        "SELECT model_provider FROM threads WHERE id = '1'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(provider, "openai");
    Ok(())
}

#[test]
fn sqlite_only_warning_detects_configured_app_alias() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    let app_home = codex_home.join("sqlite");
    fs::create_dir_all(&app_home)?;
    fs::write(
        codex_home.join("config.toml"),
        sqlite_home_config(&app_home),
    )?;
    seed_sqlite(&app_home.join("state_5.sqlite"))?;

    let summary = reconcile_all_stores_with_backup(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::None,
        DEFAULT_BUCKET_PADDING_BYTES,
        Duration::from_millis(50),
        StoreFilter::All,
        None,
    )?;

    assert_eq!(summary.status(), ReconcileStatus::Full);
    assert_eq!(summary.stores.len(), 1);
    assert_eq!(summary.stores[0].kind, StoreKind::Configured);
    assert!(summary.app_store_updated(codex_home));
    Ok(())
}

#[test]
fn bucket_prepare_checks_rollouts_from_all_stores() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    fs::write(
        codex_home.join("config.toml"),
        sqlite_home_config(codex_home),
    )?;

    let cli_rollout = codex_home.join("sessions/2026/05/07/rollout-cli.jsonl");
    let app_rollout = codex_home.join("sessions/2026/05/07/rollout-app.jsonl");
    fs::create_dir_all(cli_rollout.parent().unwrap())?;
    fs::write(
        &cli_rollout,
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"1\",\"model_provider\":\"cong\"}}\n",
    )?;
    fs::write(
        &app_rollout,
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"1\",\"model_provider\":\"cong\"}}\n",
    )?;

    let cli_db = codex_home.join("state_5.sqlite");
    let app_db = codex_home.join("sqlite").join("state_5.sqlite");
    fs::create_dir_all(app_db.parent().unwrap())?;
    for (db, rollout) in [(&cli_db, &cli_rollout), (&app_db, &app_rollout)] {
        seed_sqlite(db)?;
        let connection = Connection::open(db)?;
        connection.execute(
            "UPDATE threads SET rollout_path = ?1 WHERE id = '1'",
            [rollout.display().to_string()],
        )?;
    }

    let summary = prepare_bucket_padding(codex_home, None, DEFAULT_BUCKET_PADDING_BYTES)?;

    assert_eq!(summary.checked_rollouts, 2);
    Ok(())
}

#[cfg(unix)]
#[test]
fn reconcile_all_stores_skips_rollout_when_store_backup_fails() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    isolate_process_sqlite_home(codex_home)?;

    let rollout_path = codex_home.join("sessions/2026/05/07/rollout-1.jsonl");
    fs::create_dir_all(rollout_path.parent().unwrap())?;
    fs::write(
        &rollout_path,
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"1\",\"model_provider\":\"cong\"}}        \n",
    )?;

    let cli_db = codex_home.join("state_5.sqlite");
    let app_db = codex_home.join("sqlite").join("state_5.sqlite");
    fs::create_dir_all(app_db.parent().unwrap())?;
    for db in [&cli_db, &app_db] {
        seed_sqlite(db)?;
        let connection = Connection::open(db)?;
        connection.execute(
            "UPDATE threads SET rollout_path = ?1, model_provider = 'cong' WHERE id = '1'",
            [rollout_path.display().to_string()],
        )?;
    }

    let app_dir = app_db.parent().unwrap();
    let original_mode = fs::metadata(app_dir)?.permissions().mode();
    fs::set_permissions(app_dir, fs::Permissions::from_mode(0o500))?;
    let summary = reconcile_all_stores_with_backup(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::AllRows,
        DEFAULT_BUCKET_PADDING_BYTES,
        Duration::from_millis(0),
        StoreFilter::All,
        None,
    );
    fs::set_permissions(app_dir, fs::Permissions::from_mode(original_mode))?;
    let summary = summary?;

    assert_eq!(summary.status(), ReconcileStatus::Failed);
    assert_eq!(summary.changed_rollouts, 0);
    assert!(fs::read_to_string(&rollout_path)?.contains("\"model_provider\":\"cong\""));
    for db in [&cli_db, &app_db] {
        let connection = Connection::open(db)?;
        let provider: String = connection.query_row(
            "SELECT model_provider FROM threads WHERE id = '1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(provider, "cong");
    }
    Ok(())
}

#[test]
fn reconcile_all_stores_fails_store_when_rollout_targets_unreadable() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    fs::write(
        codex_home.join("config.toml"),
        sqlite_home_config(codex_home),
    )?;

    // A valid SQLite DB whose `threads` table has `model_provider` but no
    // `rollout_path` column: `UPDATE model_provider` would succeed, yet rollout
    // target collection fails. The store must be reported Failed (not silently
    // Updated), and its DB must be left untouched.
    let cli_db = codex_home.join("state_5.sqlite");
    {
        let connection = Connection::open(&cli_db)?;
        connection.execute(
            "CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT NOT NULL)",
            [],
        )?;
        connection.execute(
            "INSERT INTO threads (id, model_provider) VALUES ('1', 'cong')",
            [],
        )?;
    }

    let summary = reconcile_all_stores_with_backup(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::AllRows,
        DEFAULT_BUCKET_PADDING_BYTES,
        Duration::from_millis(0),
        StoreFilter::All,
        None,
    )?;

    assert_eq!(summary.status(), ReconcileStatus::Failed);
    assert!(matches!(
        summary.stores[0].outcome,
        StoreOutcome::Failed { .. }
    ));
    let connection = Connection::open(&cli_db)?;
    let provider: String = connection.query_row(
        "SELECT model_provider FROM threads WHERE id = '1'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(provider, "cong");
    Ok(())
}

fn seed_store_with_provider(db: &Path, provider: &str) -> Result<()> {
    seed_sqlite(db)?;
    let connection = Connection::open(db)?;
    connection.execute(
        "UPDATE threads SET model_provider = ?1 WHERE id = '1'",
        [provider],
    )?;
    Ok(())
}

fn set_backfill_status(db: &Path, status: &str) -> Result<()> {
    let connection = Connection::open(db)?;
    connection.execute(
        "CREATE TABLE IF NOT EXISTS backfill_state (id INTEGER PRIMARY KEY, status TEXT NOT NULL)",
        [],
    )?;
    connection.execute(
        "INSERT OR REPLACE INTO backfill_state (id, status) VALUES (1, ?1)",
        [status],
    )?;
    Ok(())
}

fn provider_of(db: &Path) -> Result<String> {
    let connection = Connection::open(db)?;
    Ok(connection.query_row(
        "SELECT model_provider FROM threads WHERE id = '1'",
        [],
        |row| row.get(0),
    )?)
}

#[test]
fn reconcile_all_stores_skips_busy_store_in_sqlite_only() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    isolate_process_sqlite_home(codex_home)?;

    let cli_db = codex_home.join("state_5.sqlite");
    seed_store_with_provider(&cli_db, "cong")?;
    let app_db = codex_home.join("sqlite").join("state_5.sqlite");
    fs::create_dir_all(app_db.parent().unwrap())?;
    seed_store_with_provider(&app_db, "cong")?;
    set_backfill_status(&app_db, "running")?;

    // --sqlite-only (RolloutScope::None) touches no rollout, so the ready CLI
    // store is still written while the busy App store is skipped.
    let summary = reconcile_all_stores_with_backup(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::None,
        DEFAULT_BUCKET_PADDING_BYTES,
        Duration::from_millis(50),
        StoreFilter::All,
        None,
    )?;

    assert_eq!(summary.status(), ReconcileStatus::Partial);
    let cli = summary
        .stores
        .iter()
        .find(|store| store.kind == StoreKind::Cli)
        .expect("cli store present");
    assert!(matches!(cli.outcome, StoreOutcome::Updated { .. }));
    let app = summary
        .stores
        .iter()
        .find(|store| store.kind == StoreKind::App)
        .expect("app store present");
    assert!(matches!(app.outcome, StoreOutcome::Skipped));
    assert_eq!(provider_of(&cli_db)?, "openai");
    assert_eq!(provider_of(&app_db)?, "cong");
    Ok(())
}

#[test]
fn reconcile_all_stores_treats_locked_backfill_status_as_busy() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    fs::write(
        codex_home.join("config.toml"),
        sqlite_home_config(codex_home),
    )?;

    let cli_db = codex_home.join("state_5.sqlite");
    seed_store_with_provider(&cli_db, "cong")?;
    set_backfill_status(&cli_db, "running")?;
    let lock = Connection::open(&cli_db)?;
    lock.execute_batch("BEGIN EXCLUSIVE;")?;

    let started = Instant::now();
    let summary = reconcile_all_stores_with_backup(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::None,
        DEFAULT_BUCKET_PADDING_BYTES,
        Duration::from_millis(0),
        StoreFilter::All,
        None,
    )?;
    let elapsed = started.elapsed();
    lock.execute_batch("ROLLBACK;")?;
    drop(lock);

    assert!(elapsed < Duration::from_millis(500));
    assert_eq!(summary.status(), ReconcileStatus::Failed);
    assert!(matches!(summary.stores[0].outcome, StoreOutcome::Skipped));
    assert_eq!(provider_of(&cli_db)?, "cong");
    Ok(())
}

#[test]
fn reconcile_all_stores_skips_whole_round_when_backfill_running() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    isolate_process_sqlite_home(codex_home)?;

    // A rollout shared by both stores' thread "1".
    let rollout_path = codex_home.join("sessions/2026/05/07/rollout-1.jsonl");
    fs::create_dir_all(rollout_path.parent().unwrap())?;
    fs::write(
        &rollout_path,
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"1\",\"model_provider\":\"cong\"}}        \n",
    )?;

    let cli_db = codex_home.join("state_5.sqlite");
    let app_db = codex_home.join("sqlite").join("state_5.sqlite");
    fs::create_dir_all(app_db.parent().unwrap())?;
    for db in [&cli_db, &app_db] {
        seed_store_with_provider(db, "cong")?;
        let connection = Connection::open(db)?;
        connection.execute(
            "UPDATE threads SET rollout_path = ?1 WHERE id = '1'",
            [rollout_path.display().to_string()],
        )?;
    }
    set_backfill_status(&app_db, "running")?;

    // A rollout-rewriting scope (AllRows) while App's backfill runs must skip the
    // whole round: rewriting the shared rollout would race Codex's rebuild.
    let summary = reconcile_all_stores_with_backup(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::AllRows,
        DEFAULT_BUCKET_PADDING_BYTES,
        Duration::from_millis(50),
        StoreFilter::All,
        None,
    )?;

    assert_eq!(summary.status(), ReconcileStatus::Failed);
    for store in &summary.stores {
        assert!(matches!(store.outcome, StoreOutcome::Skipped));
    }
    // Nothing was touched: both DBs and the shared rollout are unchanged.
    assert_eq!(provider_of(&cli_db)?, "cong");
    assert_eq!(provider_of(&app_db)?, "cong");
    let rollout = fs::read_to_string(&rollout_path)?;
    assert!(rollout.contains("\"model_provider\":\"cong\""));
    assert!(!rollout.contains("openai"));
    Ok(())
}

#[test]
fn reconcile_all_stores_treats_complete_backfill_as_ready() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    isolate_process_sqlite_home(codex_home)?;

    let cli_db = codex_home.join("state_5.sqlite");
    seed_store_with_provider(&cli_db, "cong")?;
    set_backfill_status(&cli_db, "complete")?;

    let summary = reconcile_all_stores_with_backup(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::AllRows,
        DEFAULT_BUCKET_PADDING_BYTES,
        Duration::from_millis(50),
        StoreFilter::All,
        None,
    )?;

    assert_eq!(summary.status(), ReconcileStatus::Full);
    assert!(matches!(
        summary.stores[0].outcome,
        StoreOutcome::Updated { .. }
    ));
    assert_eq!(provider_of(&cli_db)?, "openai");
    Ok(())
}

#[test]
fn reconcile_all_stores_writes_no_journal_without_backup() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    isolate_process_sqlite_home(codex_home)?;

    let rollout_path = codex_home.join("sessions/2026/05/07/rollout-1.jsonl");
    fs::create_dir_all(rollout_path.parent().unwrap())?;
    fs::write(
        &rollout_path,
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"1\",\"model_provider\":\"cong\"}}        \n",
    )?;
    let cli_db = codex_home.join("state_5.sqlite");
    seed_store_with_provider(&cli_db, "cong")?;
    {
        let connection = Connection::open(&cli_db)?;
        connection.execute(
            "UPDATE threads SET rollout_path = ?1 WHERE id = '1'",
            [rollout_path.display().to_string()],
        )?;
    }

    // The no-backup path (watch) rewrites the rollout but must not write a
    // backups/rollouts.*.jsonl change journal, matching the old watch behaviour.
    let summary = reconcile_all_stores(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::AllRows,
        Duration::from_millis(0),
        StoreFilter::All,
        None,
    )?;

    assert_eq!(summary.changed_rollouts, 1);
    assert!(fs::read_to_string(&rollout_path)?.contains("\"model_provider\":\"openai\""));
    assert!(summary.rollout_journal_path.is_none());

    let backups_dir = codex_home.join("backups");
    let journal_count = if backups_dir.exists() {
        fs::read_dir(&backups_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("rollouts."))
            .count()
    } else {
        0
    };
    assert_eq!(journal_count, 0);
    Ok(())
}

#[test]
fn store_filter_limits_reconcile_to_selected_surface() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    isolate_process_sqlite_home(codex_home)?;
    let cli_db = codex_home.join("state_5.sqlite");
    seed_store_with_provider(&cli_db, "cong")?;
    let app_db = codex_home.join("sqlite").join("state_5.sqlite");
    fs::create_dir_all(app_db.parent().unwrap())?;
    seed_store_with_provider(&app_db, "cong")?;

    // `--store cli` reconciles only the CLI surface; the App store is untouched.
    let summary = reconcile_all_stores_with_backup(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::None,
        DEFAULT_BUCKET_PADDING_BYTES,
        Duration::from_millis(0),
        StoreFilter::Cli,
        None,
    )?;

    assert_eq!(summary.stores.len(), 1);
    assert_eq!(summary.stores[0].kind, StoreKind::Cli);
    assert_eq!(provider_of(&cli_db)?, "openai");
    assert_eq!(provider_of(&app_db)?, "cong");
    Ok(())
}

#[test]
fn store_filter_reconciles_cli_when_configured_aliases_cli_default() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    fs::write(codex_home.join("config.toml"), "sqlite_home = \".\"\n")?;
    let cli_db = codex_home.join("state_5.sqlite");
    seed_store_with_provider(&cli_db, "cong")?;

    let summary = reconcile_all_stores_with_backup(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::None,
        DEFAULT_BUCKET_PADDING_BYTES,
        Duration::from_millis(0),
        StoreFilter::Cli,
        None,
    )?;

    assert_eq!(summary.stores.len(), 1);
    assert_eq!(summary.stores[0].kind, StoreKind::Cli);
    assert_eq!(provider_of(&cli_db)?, "openai");
    Ok(())
}

#[test]
fn store_filter_reconciles_app_when_configured_aliases_app_default() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    fs::write(codex_home.join("config.toml"), "sqlite_home = \"sqlite\"\n")?;
    let app_db = codex_home.join("sqlite").join("state_5.sqlite");
    fs::create_dir_all(app_db.parent().unwrap())?;
    seed_store_with_provider(&app_db, "cong")?;

    let summary = reconcile_all_stores(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::None,
        Duration::from_millis(0),
        StoreFilter::App,
        None,
    )?;

    assert_eq!(summary.stores.len(), 1);
    assert_eq!(summary.stores[0].kind, StoreKind::App);
    assert_eq!(provider_of(&app_db)?, "openai");
    Ok(())
}

#[test]
fn store_filter_rejects_rollout_writing_scope() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    isolate_process_sqlite_home(codex_home)?;

    let rollout_path = codex_home.join("sessions/2026/05/07/rollout-1.jsonl");
    fs::create_dir_all(rollout_path.parent().unwrap())?;
    fs::write(
        &rollout_path,
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"1\",\"model_provider\":\"cong\"}}        \n",
    )?;

    let cli_db = codex_home.join("state_5.sqlite");
    seed_store_with_provider(&cli_db, "cong")?;
    {
        let connection = Connection::open(&cli_db)?;
        connection.execute(
            "UPDATE threads SET rollout_path = ?1 WHERE id = '1'",
            [rollout_path.display().to_string()],
        )?;
    }

    let app_db = codex_home.join("sqlite").join("state_5.sqlite");
    fs::create_dir_all(app_db.parent().unwrap())?;
    seed_store_with_provider(&app_db, "cong")?;

    let err = reconcile_all_stores_with_backup(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::AllRows,
        DEFAULT_BUCKET_PADDING_BYTES,
        Duration::from_millis(50),
        StoreFilter::Cli,
        None,
    )
    .unwrap_err();

    assert!(err.to_string().contains("--sqlite-only"));
    assert_eq!(provider_of(&cli_db)?, "cong");
    assert_eq!(provider_of(&app_db)?, "cong");
    let rollout = fs::read_to_string(&rollout_path)?;
    assert!(rollout.contains("\"model_provider\":\"cong\""));
    assert!(!rollout.contains("openai"));
    Ok(())
}

#[test]
fn rejects_store_filter_for_commands_that_do_not_select_stores() {
    let err = validate_store_filter_supported(Locale::En, StoreFilter::App, "install-service")
        .unwrap_err();
    assert!(err.to_string().contains("--store is not supported"));
    assert!(err.to_string().contains("install-service"));
    assert!(!err.to_string().contains("bucket switch"));

    validate_store_filter_supported(Locale::En, StoreFilter::All, "install-service").unwrap();
}

#[test]
fn rejects_store_filter_for_rollout_writing_commands() {
    let err = validate_store_filter_rollout_scope(Locale::En, StoreFilter::Cli, false, "sync")
        .unwrap_err();
    assert!(err.to_string().contains("--sqlite-only"));
    assert!(err.to_string().contains("sync"));

    validate_store_filter_rollout_scope(Locale::En, StoreFilter::Cli, true, "sync").unwrap();
    validate_store_filter_rollout_scope(Locale::En, StoreFilter::All, false, "sync").unwrap();
}

#[test]
fn status_reports_when_store_filter_matches_no_detected_store() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    isolate_process_sqlite_home(codex_home)?;
    let cli_db = codex_home.join("state_5.sqlite");
    seed_store_with_provider(&cli_db, "openai")?;

    let err = collect_status(codex_home, Some("openai"), None, StoreFilter::App).unwrap_err();

    assert!(err.to_string().contains("--store app"));
    assert!(!err.to_string().contains("run Codex at least once"));
    Ok(())
}

#[test]
fn rejects_blank_provider_override() {
    let err = validate_provider_override(Locale::En, Some("   ")).unwrap_err();
    assert!(err.to_string().contains("provider must contain"));
}

#[test]
fn rejects_blank_provider_override_in_cli_args_before_help_short_circuit() {
    let err = validate_provider_override_args(
        Locale::En,
        ["codex-threadripper", "--provider", "", "status", "--help"],
    )
    .unwrap_err();
    assert!(err.to_string().contains("provider must contain"));
}

#[test]
fn rejects_blank_profile_override_in_cli_args_before_help_short_circuit() {
    let err = validate_provider_override_args(
        Locale::En,
        ["codex-threadripper", "--profile", "", "status", "--help"],
    )
    .unwrap_err();
    assert!(err.to_string().contains("profile must contain"));
}

#[test]
fn rejects_path_profile_override_in_cli_args_before_help_short_circuit() {
    let err = validate_provider_override_args(
        Locale::En,
        [
            "codex-threadripper",
            "--profile",
            "nested/work",
            "status",
            "--help",
        ],
    )
    .unwrap_err();
    assert!(err.to_string().contains("profile must be a plain name"));
}

#[test]
fn reconcile_all_stores_reports_missing_configured_store_as_failed() -> Result<()> {
    let dir = tempfile::tempdir()?;
    fs::write(
        dir.path().join("config.toml"),
        "model_provider = \"openai\"\nsqlite_home = \".threadripper-test-missing\"\n",
    )?;

    let summary = reconcile_all_stores(
        dir.path(),
        Some("openai"),
        None,
        RolloutScope::None,
        Duration::from_millis(0),
        StoreFilter::All,
        None,
    )?;

    assert_eq!(summary.status(), ReconcileStatus::Failed);
    assert_eq!(summary.stores.len(), 1);
    assert_eq!(summary.stores[0].kind, StoreKind::Configured);
    assert!(matches!(
        summary.stores[0].outcome,
        StoreOutcome::Failed { .. }
    ));
    assert!(
        !dir.path()
            .join(".threadripper-test-missing")
            .join("state_5.sqlite")
            .exists()
    );
    Ok(())
}

#[test]
fn inspect_sqlite_distribution_returns_error_when_db_missing() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let sqlite_path = dir.path().join("state_5.sqlite");
    let backups_path = dir.path().join("backups");

    let err = inspect_sqlite_distribution(&sqlite_path, "openai").unwrap_err();

    assert!(err.to_string().contains(&sqlite_path.display().to_string()));
    assert!(!sqlite_path.exists());
    assert!(!backups_path.exists());
    Ok(())
}

#[test]
fn reconciles_sqlite_rows_in_place() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let sqlite_path = dir.path().join("state_5.sqlite");
    seed_sqlite(&sqlite_path)?;

    let (changed_rows, total_rows) = reconcile_sqlite_in_place(&sqlite_path, "openai")?;
    let connection = Connection::open(&sqlite_path)?;
    let other_rows: u64 = connection.query_row(
        "SELECT COUNT(*) FROM threads WHERE model_provider <> 'openai'",
        [],
        |row| row.get(0),
    )?;

    assert_eq!(changed_rows, 2);
    assert_eq!(total_rows, 3);
    assert_eq!(other_rows, 0);
    Ok(())
}

#[test]
fn sync_backup_preserves_pre_reconcile_state() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let sqlite_path = dir.path().join("state_5.sqlite");
    seed_sqlite(&sqlite_path)?;

    let (changed_rows, total_rows, backup_path) =
        reconcile_sqlite_with_backup(&sqlite_path, "openai")?;
    let live_connection = Connection::open(&sqlite_path)?;
    let backup_connection = Connection::open(&backup_path)?;
    let live_other_rows: u64 = live_connection.query_row(
        "SELECT COUNT(*) FROM threads WHERE model_provider <> 'openai'",
        [],
        |row| row.get(0),
    )?;
    let backup_other_rows: u64 = backup_connection.query_row(
        "SELECT COUNT(*) FROM threads WHERE model_provider <> 'openai'",
        [],
        |row| row.get(0),
    )?;

    assert_eq!(changed_rows, 2);
    assert_eq!(total_rows, 3);
    assert_eq!(live_other_rows, 0);
    assert_eq!(backup_other_rows, 2);
    Ok(())
}

#[test]
fn seed_sqlite_fixture_includes_current_codex_thread_columns() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let sqlite_path = dir.path().join("state_5.sqlite");
    seed_sqlite(&sqlite_path)?;
    let connection = Connection::open(&sqlite_path)?;

    let mut statement = connection.prepare("PRAGMA table_info(threads)")?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for column in ["created_at_ms", "updated_at_ms", "thread_source", "preview"] {
        assert!(columns.iter().any(|candidate| candidate == column));
    }

    let mut statement = connection.prepare("PRAGMA index_list(threads)")?;
    let indexes = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for index in [
        "idx_threads_created_at_ms",
        "idx_threads_updated_at_ms",
        "idx_threads_archived_cwd_created_at_ms",
        "idx_threads_archived_cwd_updated_at_ms",
        "idx_threads_visible_created_at_ms",
        "idx_threads_visible_updated_at_ms",
    ] {
        assert!(indexes.iter().any(|candidate| candidate == index));
    }

    let mut statement = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")?;
    let triggers = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for trigger in [
        "threads_created_at_ms_after_insert",
        "threads_updated_at_ms_after_insert",
        "threads_created_at_ms_after_update",
        "threads_updated_at_ms_after_update",
    ] {
        assert!(triggers.iter().any(|candidate| candidate == trigger));
    }

    connection.execute(
        "INSERT INTO threads (
            id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
            sandbox_policy, approval_mode
        ) VALUES ('trigger-test', '', 2, 3, 'cli', 'openai', '/tmp', 'trigger test',
            'workspace-write', 'auto')",
        [],
    )?;
    let triggered_ms: (i64, i64) = connection.query_row(
        "SELECT created_at_ms, updated_at_ms FROM threads WHERE id = 'trigger-test'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(triggered_ms, (2000, 3000));

    let (created_at_ms, updated_at_ms, thread_source, preview): (
            i64,
            i64,
            String,
            String,
        ) = connection.query_row(
            "SELECT created_at_ms, updated_at_ms, thread_source, preview FROM threads WHERE id = '1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    assert_eq!((created_at_ms, updated_at_ms), (1000, 1000));
    assert_eq!(thread_source, "user");
    assert_eq!(preview, "a");
    Ok(())
}

#[test]
fn durable_sync_updates_matching_rollout_session_meta() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    let sqlite_path = codex_home.join("state_5.sqlite");
    let rollout_path = codex_home.join("sessions/2026/05/07/rollout-2026-05-07T14-06-18-1.jsonl");
    fs::create_dir_all(rollout_path.parent().unwrap())?;
    fs::write(
        &rollout_path,
        concat!(
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"1\",\"model_provider\":\"cong\"}}       \n",
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"other\",\"model_provider\":\"cong\"}}\n",
            "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"hi\"}}\n",
        ),
    )?;
    let original_mtime = FileTime::from_unix_time(1_700_000_000, 0);
    set_file_times(&rollout_path, original_mtime, original_mtime)?;
    seed_sqlite(&sqlite_path)?;
    let connection = Connection::open(&sqlite_path)?;
    connection.execute(
        "UPDATE threads SET rollout_path = ?1, model_provider = 'openai' WHERE id = '1'",
        [rollout_path.display().to_string()],
    )?;
    drop(connection);

    let journal_path = codex_home.join("backups/rollouts.test.jsonl");
    let summary = reconcile_rollout_metadata_from_sqlite_with_progress(
        &sqlite_path,
        codex_home,
        "openai",
        RolloutScope::AllRows,
        Some(journal_path.as_path()),
        DEFAULT_BUCKET_PADDING_BYTES,
        None,
    )?;

    assert_rollout_times(&rollout_path, original_mtime)?;
    let rewritten = fs::read_to_string(&rollout_path)?;
    let journal = fs::read_to_string(&journal_path)?;
    assert_eq!(summary.checked_files, 1);
    assert_eq!(summary.changed_files, 1);
    assert!(rewritten.contains("\"id\":\"1\""));
    assert!(rewritten.contains("\"model_provider\":\"openai\""));
    assert!(rewritten.contains("\"id\":\"other\""));
    assert!(rewritten.contains("\"model_provider\":\"cong\""));
    assert!(journal.contains("\"mode\":\"in_place\""));
    assert!(journal.contains("cong"));
    Ok(())
}

#[test]
fn durable_sync_patches_shorter_provider_in_place() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    let sqlite_path = codex_home.join("state_5.sqlite");
    let rollout_path = codex_home.join("sessions/2026/05/07/rollout-2026-05-07T14-06-18-1.jsonl");
    fs::create_dir_all(rollout_path.parent().unwrap())?;
    fs::write(
        &rollout_path,
        concat!(
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"1\",\"model_provider\":\"openai\"}}      \n",
            "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"hi\"}}\n",
        ),
    )?;
    let original_mtime = FileTime::from_unix_time(1_700_000_100, 0);
    set_file_times(&rollout_path, original_mtime, original_mtime)?;
    seed_sqlite(&sqlite_path)?;
    let connection = Connection::open(&sqlite_path)?;
    connection.execute(
        "UPDATE threads SET rollout_path = ?1, model_provider = 'openai' WHERE id = '1'",
        [rollout_path.display().to_string()],
    )?;
    drop(connection);
    let before_len = fs::metadata(&rollout_path)?.len();

    let journal_path = codex_home.join("backups/rollouts.in-place.jsonl");
    let summary = reconcile_rollout_metadata_from_sqlite_with_progress(
        &sqlite_path,
        codex_home,
        "cong",
        RolloutScope::AllRows,
        Some(journal_path.as_path()),
        DEFAULT_BUCKET_PADDING_BYTES,
        None,
    )?;

    assert_rollout_times(&rollout_path, original_mtime)?;
    let after_len = fs::metadata(&rollout_path)?.len();
    let rewritten = fs::read_to_string(&rollout_path)?;
    let journal = fs::read_to_string(&journal_path)?;
    assert_eq!(summary.checked_files, 1);
    assert_eq!(summary.changed_files, 1);
    assert_eq!(summary.prepared_files, 0);
    assert_eq!(before_len, after_len);
    assert!(
        rewritten
            .lines()
            .next()
            .unwrap()
            .contains("\"model_provider\":\"cong\"")
    );
    assert!(journal.contains("\"mode\":\"in_place\""));
    Ok(())
}

#[test]
fn durable_sync_skips_longer_provider_without_padding() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    let sqlite_path = codex_home.join("state_5.sqlite");
    let rollout_path = codex_home.join("sessions/2026/05/07/rollout-no-padding.jsonl");
    fs::create_dir_all(rollout_path.parent().unwrap())?;
    fs::write(
        &rollout_path,
        concat!(
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"1\",\"model_provider\":\"vm\"}}\n",
            "{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"hi\"}}\n",
        ),
    )?;
    seed_sqlite(&sqlite_path)?;
    let connection = Connection::open(&sqlite_path)?;
    connection.execute(
        "UPDATE threads SET rollout_path = ?1, model_provider = 'vm' WHERE id = '1'",
        [rollout_path.display().to_string()],
    )?;
    drop(connection);

    let summary = reconcile_rollout_metadata_from_sqlite_with_progress(
        &sqlite_path,
        codex_home,
        "openai",
        RolloutScope::AllRows,
        None,
        DEFAULT_BUCKET_PADDING_BYTES,
        None,
    )?;

    assert_eq!(summary.checked_files, 1);
    assert_eq!(summary.changed_files, 0);
    assert_eq!(summary.skipped_files, 1);
    assert!(fs::read_to_string(&rollout_path)?.contains("\"model_provider\":\"vm\""));
    Ok(())
}

#[test]
fn mismatched_scope_followup_repairs_matching_sqlite_rollout_after_sqlite_changes() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    isolate_process_sqlite_home(codex_home)?;
    let sqlite_path = codex_home.join("state_5.sqlite");
    let rollout_a = codex_home.join("sessions/2026/05/07/rollout-a.jsonl");
    let rollout_b = codex_home.join("sessions/2026/05/07/rollout-b.jsonl");
    fs::create_dir_all(rollout_a.parent().unwrap())?;
    fs::write(
        &rollout_a,
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"1\",\"model_provider\":\"vm\"}}       \n",
    )?;
    fs::write(
        &rollout_b,
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"2\",\"model_provider\":\"cp\"}}       \n",
    )?;
    let original_time = FileTime::from_unix_time(1_700_000_200, 0);
    set_file_times(&rollout_a, original_time, original_time)?;
    set_file_times(&rollout_b, original_time, original_time)?;
    seed_sqlite(&sqlite_path)?;
    let connection = Connection::open(&sqlite_path)?;
    connection.execute(
        "UPDATE threads SET rollout_path = ?1, model_provider = 'vm' WHERE id = '1'",
        [rollout_a.display().to_string()],
    )?;
    connection.execute(
        "UPDATE threads SET rollout_path = ?1, model_provider = 'openai' WHERE id = '2'",
        [rollout_b.display().to_string()],
    )?;
    connection.execute("UPDATE threads SET rollout_path = '' WHERE id = '3'", [])?;
    drop(connection);

    let summary = reconcile_all_stores(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::MismatchedRows,
        Duration::from_millis(0),
        StoreFilter::All,
        None,
    )?;

    assert_eq!(summary.total_changed_rows(), 1);
    assert_eq!(summary.changed_rollouts, 2);
    assert!(fs::read_to_string(&rollout_a)?.contains("\"model_provider\":\"openai\""));
    assert!(fs::read_to_string(&rollout_b)?.contains("\"model_provider\":\"openai\""));
    Ok(())
}

#[test]
fn mismatched_scope_followup_excludes_failed_stores() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let codex_home = dir.path();
    isolate_process_sqlite_home(codex_home)?;

    let rollout_cli = codex_home.join("sessions/2026/05/07/rollout-cli.jsonl");
    let rollout_app = codex_home.join("sessions/2026/05/07/rollout-app.jsonl");
    fs::create_dir_all(rollout_cli.parent().unwrap())?;
    fs::write(
        &rollout_cli,
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"1\",\"model_provider\":\"vm\"}}       \n",
    )?;
    fs::write(
        &rollout_app,
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"app\",\"model_provider\":\"cong\"}}       \n",
    )?;

    let cli_db = codex_home.join("state_5.sqlite");
    seed_store_with_provider(&cli_db, "vm")?;
    {
        let connection = Connection::open(&cli_db)?;
        connection.execute(
            "UPDATE threads SET rollout_path = ?1 WHERE id = '1'",
            [rollout_cli.display().to_string()],
        )?;
    }

    let app_db = codex_home.join("sqlite").join("state_5.sqlite");
    fs::create_dir_all(app_db.parent().unwrap())?;
    {
        let connection = Connection::open(&app_db)?;
        connection.execute(
            "CREATE TABLE threads (id TEXT PRIMARY KEY, rollout_path TEXT NOT NULL)",
            [],
        )?;
        connection.execute(
            "INSERT INTO threads (id, rollout_path) VALUES ('app', ?1)",
            [rollout_app.display().to_string()],
        )?;
    }

    let summary = reconcile_all_stores(
        codex_home,
        Some("openai"),
        None,
        RolloutScope::MismatchedRows,
        Duration::from_millis(0),
        StoreFilter::All,
        None,
    )?;

    assert_eq!(summary.status(), ReconcileStatus::Partial);
    assert_eq!(summary.total_changed_rows(), 2);
    assert!(fs::read_to_string(&rollout_cli)?.contains("\"model_provider\":\"openai\""));
    assert!(fs::read_to_string(&rollout_app)?.contains("\"model_provider\":\"cong\""));
    assert!(!fs::read_to_string(&rollout_app)?.contains("openai"));
    Ok(())
}

#[test]
fn watch_uses_five_hundred_ms_by_default() -> Result<()> {
    let matches =
        localized_command(Locale::En).try_get_matches_from(["codex-threadripper", "watch"])?;
    let cli = Cli::from_arg_matches(&matches)?;

    match cli.command {
        Command::Watch {
            poll_interval_ms, ..
        } => {
            assert_eq!(poll_interval_ms, DEFAULT_POLL_INTERVAL_MS);
        }
        other => panic!("expected watch command, got {other:?}"),
    }

    Ok(())
}

#[test]
fn watch_promotes_initial_rollout_scan_to_all_rows() {
    assert_eq!(
        full_watch_rollout_scope(RolloutScope::MismatchedRows),
        RolloutScope::AllRows
    );
    assert_eq!(
        full_watch_rollout_scope(RolloutScope::None),
        RolloutScope::None
    );
}

#[test]
fn watch_periodically_runs_full_rollout_scan() {
    assert_eq!(
        periodic_watch_rollout_scope(RolloutScope::MismatchedRows, 1),
        RolloutScope::MismatchedRows
    );
    assert_eq!(
        periodic_watch_rollout_scope(
            RolloutScope::MismatchedRows,
            WATCH_FULL_ROLLOUT_POLL_INTERVALS
        ),
        RolloutScope::AllRows
    );
    assert_eq!(
        periodic_watch_rollout_scope(RolloutScope::None, WATCH_FULL_ROLLOUT_POLL_INTERVALS),
        RolloutScope::None
    );
}

#[test]
fn watch_prints_non_full_summary_even_when_counts_are_unchanged() {
    let summary = MultiReconcileSummary {
        provider: "openai".to_string(),
        stores: vec![StoreReconcileResult {
            kind: StoreKind::Cli,
            db_path: PathBuf::from("/tmp/state_5.sqlite"),
            outcome: StoreOutcome::Failed {
                error: "missing store".to_string(),
            },
        }],
        changed_rollouts: 0,
        checked_rollouts: 0,
        prepared_rollouts: 0,
        skipped_rollouts: 0,
        rollout_journal_path: None,
        elapsed: Duration::ZERO,
    };

    assert!(watch_should_print_summary(Some("openai"), &summary));
}

#[test]
fn watch_tracks_selected_profile_config_file() -> Result<()> {
    let dir = tempfile::tempdir()?;
    fs::write(
        dir.path().join("config.toml"),
        "model_provider = \"openai\"\nprofile = \"work\"\n",
    )?;

    let implicit = watched_config_paths(dir.path(), None);
    let explicit = watched_config_paths(dir.path(), Some("other"));

    assert!(implicit.contains(&dir.path().join("config.toml")));
    assert!(implicit.contains(&dir.path().join("work.config.toml")));
    assert!(explicit.contains(&dir.path().join("other.config.toml")));
    Ok(())
}

#[test]
fn generates_launchd_plist() {
    let plist = crate::service::build_launchd_plist(
        PathBuf::from("/tmp/codex-threadripper").as_path(),
        PathBuf::from("/tmp/codex-home").as_path(),
        Some("openai"),
        Some("work"),
        Duration::from_millis(DEFAULT_POLL_INTERVAL_MS),
    );
    assert!(plist.contains("dev.wangnov.codex-threadripper"));
    assert!(plist.contains("--codex-home"));
    assert!(plist.contains("--provider"));
    assert!(plist.contains("--profile"));
    assert!(plist.contains("watch"));
}

#[test]
fn builds_install_next_steps() -> Result<()> {
    let manager = crate::service::current_manager();
    let steps = install_next_steps(
        Locale::ZhHans,
        PathBuf::from("/tmp/codex threadripper").as_path(),
        PathBuf::from("/tmp/codex home").as_path(),
        Some("openai"),
        Some("work"),
        manager,
    )?;
    assert_eq!(steps.len(), 3);
    assert!(steps[0].contains("运行这条命令查看状态"));
    assert!(
        steps[0].contains(
            "'/tmp/codex threadripper' --codex-home '/tmp/codex home' --provider openai --profile work status"
        )
    );
    assert!(steps[1].contains(crate::service::manager_name(manager)));
    match manager {
        ServiceManager::WindowsStartup => assert!(steps[2].contains("Get-Content")),
        _ => assert!(steps[2].contains("tail -f")),
    }
    Ok(())
}

fn seed_sqlite(path: &Path) -> Result<()> {
    let connection = Connection::open(path)?;
    connection.execute_batch(
        "
            CREATE TABLE threads (
                id TEXT PRIMARY KEY,
                rollout_path TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                created_at_ms INTEGER,
                updated_at_ms INTEGER,
                source TEXT NOT NULL,
                thread_source TEXT,
                model_provider TEXT NOT NULL,
                cwd TEXT NOT NULL,
                title TEXT NOT NULL,
                sandbox_policy TEXT NOT NULL,
                approval_mode TEXT NOT NULL,
                tokens_used INTEGER NOT NULL DEFAULT 0,
                has_user_event INTEGER NOT NULL DEFAULT 0,
                archived INTEGER NOT NULL DEFAULT 0,
                archived_at INTEGER,
                git_sha TEXT,
                git_branch TEXT,
                git_origin_url TEXT,
                cli_version TEXT NOT NULL DEFAULT '',
                first_user_message TEXT NOT NULL DEFAULT '',
                agent_nickname TEXT,
                agent_role TEXT,
                memory_mode TEXT NOT NULL DEFAULT 'enabled',
                model TEXT,
                reasoning_effort TEXT,
                agent_path TEXT,
                preview TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX idx_threads_created_at ON threads(created_at DESC, id DESC);
            CREATE INDEX idx_threads_updated_at ON threads(updated_at DESC, id DESC);
            CREATE INDEX idx_threads_archived ON threads(archived);
            CREATE INDEX idx_threads_source ON threads(source);
            CREATE INDEX idx_threads_provider ON threads(model_provider);
            CREATE INDEX idx_threads_created_at_ms ON threads(created_at_ms DESC, id DESC);
            CREATE INDEX idx_threads_updated_at_ms ON threads(updated_at_ms DESC, id DESC);
            CREATE INDEX idx_threads_archived_cwd_created_at_ms
                ON threads(archived, cwd, created_at_ms DESC, id DESC);
            CREATE INDEX idx_threads_archived_cwd_updated_at_ms
                ON threads(archived, cwd, updated_at_ms DESC, id DESC);
            CREATE INDEX idx_threads_visible_created_at_ms
                ON threads(archived, created_at_ms DESC)
                WHERE preview <> '';
            CREATE INDEX idx_threads_visible_updated_at_ms
                ON threads(archived, updated_at_ms DESC)
                WHERE preview <> '';
            CREATE TRIGGER threads_created_at_ms_after_insert
            AFTER INSERT ON threads
            WHEN NEW.created_at_ms IS NULL
            BEGIN
                UPDATE threads
                SET created_at_ms = NEW.created_at * 1000
                WHERE id = NEW.id;
            END;
            CREATE TRIGGER threads_updated_at_ms_after_insert
            AFTER INSERT ON threads
            WHEN NEW.updated_at_ms IS NULL
            BEGIN
                UPDATE threads
                SET updated_at_ms = NEW.updated_at * 1000
                WHERE id = NEW.id;
            END;
            CREATE TRIGGER threads_created_at_ms_after_update
            AFTER UPDATE OF created_at ON threads
            WHEN NEW.created_at != OLD.created_at
             AND NEW.created_at_ms IS OLD.created_at_ms
            BEGIN
                UPDATE threads
                SET created_at_ms = NEW.created_at * 1000
                WHERE id = NEW.id;
            END;
            CREATE TRIGGER threads_updated_at_ms_after_update
            AFTER UPDATE OF updated_at ON threads
            WHEN NEW.updated_at != OLD.updated_at
             AND NEW.updated_at_ms IS OLD.updated_at_ms
            BEGIN
                UPDATE threads
                SET updated_at_ms = NEW.updated_at * 1000
                WHERE id = NEW.id;
            END;
            INSERT INTO threads (
                id, rollout_path, created_at, updated_at, created_at_ms, updated_at_ms,
                source, thread_source, model_provider, cwd, title, sandbox_policy,
                approval_mode, cli_version, model, reasoning_effort, first_user_message, preview
            ) VALUES
                ('1', '/tmp/a', 1, 1, 1000, 1000, 'cli', 'user', 'vm', '/tmp', 'a',
                    'workspace-write', 'auto', '0.0.0-test', 'gpt-5-codex', 'medium', 'a', 'a'),
                ('2', '/tmp/b', 1, 1, 1001, 1001, 'cli', 'user', 'cp', '/tmp', 'b',
                    'workspace-write', 'auto', '0.0.0-test', 'gpt-5-codex', 'medium', 'b', 'b'),
                ('3', '/tmp/c', 1, 1, 1002, 1002, 'cli', 'user', 'openai', '/tmp', 'c',
                    'workspace-write', 'auto', '0.0.0-test', 'gpt-5-codex', 'medium', 'c', 'c');
            ",
    )?;
    Ok(())
}

fn sqlite_home_config(path: &Path) -> String {
    let path = path
        .display()
        .to_string()
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    format!("sqlite_home = \"{path}\"\n")
}

fn isolate_process_sqlite_home(codex_home: &Path) -> Result<()> {
    let configured_home = codex_home.join(".threadripper-test-configured");
    fs::create_dir_all(&configured_home)?;
    let configured_db = configured_home.join("state_5.sqlite");
    seed_sqlite(&configured_db)?;
    let connection = Connection::open(&configured_db)?;
    connection.execute(
        "UPDATE threads SET model_provider = 'openai', rollout_path = ''",
        [],
    )?;
    fs::write(
        codex_home.join("config.toml"),
        "model_provider = \"openai\"\nsqlite_home = \".threadripper-test-configured\"\n",
    )?;
    Ok(())
}

fn assert_rollout_times(path: &Path, expected: FileTime) -> Result<()> {
    let metadata = fs::metadata(path)?;
    let actual_atime = FileTime::from_last_access_time(&metadata);
    let actual_mtime = FileTime::from_last_modification_time(&metadata);
    assert_eq!(actual_atime.unix_seconds(), expected.unix_seconds());
    assert_eq!(actual_mtime.unix_seconds(), expected.unix_seconds());
    Ok(())
}
