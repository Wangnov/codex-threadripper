use crate::cli::APP_VERSION;
use crate::cli::Cli;
use crate::cli::Command;
use crate::cli::DEFAULT_BUCKET_PADDING_BYTES;
use crate::cli::DEFAULT_POLL_INTERVAL_MS;
use crate::cli::localized_command;
use crate::cli::validate_provider_override;
use crate::cli::validate_provider_override_args;
use crate::codex_config::read_provider_from_config;
use crate::codex_config::resolve_sqlite_home_from_config;
use crate::codex_config::resolve_sqlite_path;
use crate::locale::Locale;
use crate::locale::detect_locale_from_sources;
use crate::locale::parse_apple_languages;
use crate::locale::parse_locale_tag;
use crate::output::install_next_steps;
use crate::rollout::RolloutScope;
use crate::rollout::reconcile_rollout_metadata_from_sqlite_with_progress;
use crate::service::ServiceManager;
use crate::state_db::inspect_sqlite_distribution;
use crate::state_db::reconcile_sqlite_in_place;
use crate::state_db::reconcile_sqlite_with_backup;
use crate::sync::reconcile_once;
use crate::watch::WATCH_FULL_ROLLOUT_POLL_INTERVALS;
use crate::watch::full_watch_rollout_scope;
use crate::watch::periodic_watch_rollout_scope;
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
fn reconcile_once_returns_error_when_db_missing() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let sqlite_path = dir.path().join("state_5.sqlite");
    fs::write(
        dir.path().join("config.toml"),
        sqlite_home_config(dir.path()),
    )?;

    let err = reconcile_once(dir.path(), Some("openai"), None, RolloutScope::None).unwrap_err();

    assert!(err.to_string().contains(&sqlite_path.display().to_string()));
    assert!(!sqlite_path.exists());
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
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"1\",\"model_provider\":\"cong\"}}\n",
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

    let rewritten = fs::read_to_string(&rollout_path)?;
    let journal = fs::read_to_string(&journal_path)?;
    assert_eq!(summary.checked_files, 1);
    assert_eq!(summary.changed_files, 1);
    assert!(rewritten.contains("\"id\":\"1\""));
    assert!(rewritten.contains("\"model_provider\":\"openai\""));
    assert!(rewritten.contains("\"id\":\"other\""));
    assert!(rewritten.contains("\"model_provider\":\"cong\""));
    assert!(journal.contains("\"mode\":\"rewrite_with_padding\""));
    assert!(journal.contains("cong"));
    assert_rollout_mtime(&rollout_path, original_mtime)?;
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
    assert_rollout_mtime(&rollout_path, original_mtime)?;
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

fn assert_rollout_mtime(path: &Path, expected: FileTime) -> Result<()> {
    let actual = FileTime::from_last_modification_time(&fs::metadata(path)?);
    assert_eq!(actual.unix_seconds(), expected.unix_seconds());
    Ok(())
}
