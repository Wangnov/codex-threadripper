use anyhow::Context;
use anyhow::Result;
use clap::Arg;
use clap::ArgAction;
use clap::CommandFactory;
use clap::FromArgMatches;
use clap::Parser;
use clap::Subcommand;
use std::ffi::OsStr;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

mod backup;
mod config;
mod locale;
mod rollout;
mod service;
mod sqlite;
mod status;
mod types;
mod watch;

use config::default_codex_home;
use config::DEFAULT_BUCKET_PADDING_BYTES;
use locale::*;
use types::*;
use rollout::RolloutProgressConfig;
use rollout::prepare_bucket_padding;
use status::*;
use watch::run_watch;

use backup::reconcile_once_with_backup_and_padding;
use backup::reconcile_once_with_backup_progress;
use backup::run_restore;
use backup::run_prune_backups;


const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_POLL_INTERVAL_MS: u64 = 500;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "placeholder",
    long_about = "placeholder",
    disable_help_flag = true,
    disable_version_flag = true,
    disable_help_subcommand = true
)]
struct Cli {
    #[arg(long, global = true, value_name = "DIR", help = "placeholder")]
    codex_home: Option<PathBuf>,

    #[arg(long, global = true, value_name = "PROVIDER", help = "placeholder")]
    provider: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// placeholder
    Status,
    /// placeholder
    Sync {
        #[arg(long, help = "placeholder")]
        sqlite_only: bool,
        #[arg(long, help = "placeholder")]
        dry_run: bool,
    },
    /// placeholder
    Bucket {
        #[command(subcommand)]
        command: BucketCommand,
    },
    /// placeholder
    #[command(alias = "daemon")]
    Watch {
        #[arg(
            long,
            default_value_t = DEFAULT_POLL_INTERVAL_MS,
            value_name = "MILLISECONDS",
            help = "placeholder"
        )]
        poll_interval_ms: u64,
        #[arg(long, help = "placeholder")]
        sqlite_only: bool,
    },
    /// placeholder
    #[command(name = "print-service-config", alias = "print-config")]
    PrintServiceConfig {
        #[arg(
            long,
            default_value_t = DEFAULT_POLL_INTERVAL_MS,
            value_name = "MILLISECONDS",
            help = "placeholder"
        )]
        poll_interval_ms: u64,
    },
    /// placeholder
    #[command(name = "install-service")]
    InstallService {
        #[arg(
            long,
            default_value_t = DEFAULT_POLL_INTERVAL_MS,
            value_name = "MILLISECONDS",
            help = "placeholder"
        )]
        poll_interval_ms: u64,
    },
    /// placeholder
    #[command(name = "uninstall-service")]
    UninstallService,
    /// placeholder
    Restore {
        #[arg(value_name = "BACKUP")]
        backup: Option<PathBuf>,
        #[arg(long, help = "placeholder")]
        dry_run: bool,
    },
    /// placeholder
    #[command(name = "prune-backups")]
    PruneBackups {
        #[arg(long, default_value_t = 5, value_name = "N", help = "placeholder")]
        keep: usize,
        #[arg(long, help = "placeholder")]
        dry_run: bool,
    },
}

#[derive(Subcommand, Debug)]
enum BucketCommand {
    /// placeholder
    Prepare {
        #[arg(
            long,
            default_value_t = DEFAULT_BUCKET_PADDING_BYTES,
            value_name = "BYTES",
            help = "placeholder"
        )]
        padding_bytes: usize,
    },
    /// placeholder
    Switch {
        #[arg(value_name = "PROVIDER")]
        target_provider: Option<String>,
        #[arg(
            long,
            default_value_t = DEFAULT_BUCKET_PADDING_BYTES,
            value_name = "BYTES",
            help = "placeholder"
        )]
        padding_bytes: usize,
    },
}

fn main() -> Result<()> {
    let locale = detect_locale();
    let cli = parse_cli(locale)?;
    validate_provider_override(locale, cli.provider.as_deref())?;
    let codex_home = cli.codex_home.unwrap_or_else(default_codex_home);

    match cli.command {
        Command::Status => {
            let summary = collect_status(&codex_home, cli.provider.as_deref())?;
            print_status(locale, &summary);
        }
        Command::Sync { sqlite_only, dry_run } => {
            let rollout_scope = if sqlite_only {
                RolloutScope::None
            } else {
                RolloutScope::AllRows
            };
            let progress = if sqlite_only {
                None
            } else {
                Some(RolloutProgressConfig { locale })
            };
            if dry_run {
                let sqlite_path = config::resolve_sqlite_path(&codex_home)?;
                let provider = match cli.provider.as_deref() {
                    Some(provider) => provider.to_string(),
                    None => config::read_provider_from_config(&codex_home)?,
                };
                let (total_rows, mismatched_rows, _distribution) =
                    sqlite::inspect_sqlite_distribution(&sqlite_path, provider.as_str())?;
                println!("{}", dry_run_label(locale));
                println!("  Provider: {}", provider);
                println!("  Total threads: {}", total_rows);
                println!("  Threads needing update: {}", mismatched_rows);
                println!("  Backup: skipped (dry run)");
            } else {
                let summary = reconcile_once_with_backup_progress(
                    &codex_home,
                    cli.provider.as_deref(),
                    rollout_scope,
                    progress,
                )?;
                print_sync_summary(locale, sync_complete_title(locale), &summary);
            }
        }
        Command::Bucket { command } => match command {
            BucketCommand::Prepare { padding_bytes } => {
                let summary = prepare_bucket_padding(&codex_home, padding_bytes)?;
                print_bucket_prepare_summary(locale, &summary);
            }
            BucketCommand::Switch {
                target_provider,
                padding_bytes,
            } => {
                validate_provider_override(locale, target_provider.as_deref())?;
                let provider = match target_provider {
                    Some(provider) => Some(provider),
                    None => cli.provider.clone(),
                };
                let summary = reconcile_once_with_backup_and_padding(
                    &codex_home,
                    provider.as_deref(),
                    RolloutScope::AllRows,
                    padding_bytes,
                    Some(RolloutProgressConfig { locale }),
                )?;
                print_sync_summary(locale, bucket_switch_complete_title(locale), &summary);
            }
        },
        Command::Watch {
            poll_interval_ms,
            sqlite_only,
        } => {
            run_watch(
                locale,
                &codex_home,
                cli.provider.clone(),
                if sqlite_only {
                    RolloutScope::None
                } else {
                    RolloutScope::MismatchedRows
                },
                Duration::from_millis(poll_interval_ms),
            )?;
        }
        Command::PrintServiceConfig { poll_interval_ms } => {
            let exe_path = std::env::current_exe().context(current_exe_error(locale))?;
            let config = service::render_service_config(
                exe_path.as_path(),
                &codex_home,
                cli.provider.as_deref(),
                Duration::from_millis(poll_interval_ms),
            )?;
            println!("{config}");
        }
        Command::InstallService { poll_interval_ms } => {
            install_service(
                locale,
                &codex_home,
                cli.provider.as_deref(),
                Duration::from_millis(poll_interval_ms),
            )?;
        }
        Command::Restore { backup, dry_run } => {
            let summary = run_restore(&codex_home, backup.as_deref(), dry_run)?;
            print_restore_summary(locale, &summary);
        }
        Command::PruneBackups { keep, dry_run } => {
            let summary = run_prune_backups(&codex_home, keep, dry_run)?;
            print_prune_summary(locale, &summary);
        }
        Command::UninstallService => {
            uninstall_service(locale, &codex_home)?;
        }
    }

    Ok(())
}

fn parse_cli(locale: Locale) -> Result<Cli> {
    validate_provider_override_args(locale, std::env::args_os())?;
    let command = localized_command(locale);
    let matches = command.clone().get_matches();
    Cli::from_arg_matches(&matches).map_err(|err| anyhow::anyhow!(err.to_string()))
}

fn validate_provider_override(locale: Locale, provider: Option<&str>) -> Result<()> {
    if provider.is_some_and(|value| value.trim().is_empty()) {
        anyhow::bail!(provider_empty_error(locale));
    }
    Ok(())
}

fn validate_provider_override_args<I, T>(locale: Locale, args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: AsRef<OsStr>,
{
    let mut args = args.into_iter().skip(1);
    while let Some(arg) = args.next() {
        let arg = arg.as_ref();
        if arg == OsStr::new("--") {
            break;
        }
        if arg == OsStr::new("--provider") {
            if let Some(value) = args.next() {
                validate_provider_override(locale, value.as_ref().to_str())?;
            }
            continue;
        }

        let rendered = arg.to_string_lossy();
        if let Some(value) = rendered.strip_prefix("--provider=") {
            validate_provider_override(locale, Some(value))?;
        }
    }
    Ok(())
}

fn localized_command(locale: Locale) -> clap::Command {
    let mut command = Cli::command();
    command = command
        .version(APP_VERSION)
        .disable_help_flag(true)
        .disable_version_flag(true)
        .disable_help_subcommand(true)
        .about(root_about(locale))
        .long_about(root_long_about(locale))
        .help_template(help_template(locale))
        .subcommand_help_heading(commands_heading(locale))
        .arg(
            Arg::new("help")
                .short('h')
                .long("help")
                .global(true)
                .action(ArgAction::Help)
                .help(help_option_help(locale))
                .help_heading(options_heading(locale)),
        )
        .arg(
            Arg::new("version")
                .short('V')
                .long("version")
                .global(true)
                .action(ArgAction::Version)
                .help(version_option_help(locale))
                .help_heading(options_heading(locale)),
        );
    command = command.mut_arg("codex_home", |arg| {
        arg.help(codex_home_help(locale))
            .help_heading(options_heading(locale))
            .value_name(dir_value_name(locale))
    });
    command = command.mut_arg("provider", |arg| {
        arg.help(provider_help(locale))
            .help_heading(options_heading(locale))
            .value_name(provider_value_name(locale))
    });

    command = command.mut_subcommand("status", |sub| {
        sub.about(status_about(locale))
            .version(APP_VERSION)
            .disable_help_flag(true)
            .disable_version_flag(true)
            .disable_help_subcommand(true)
            .help_template(help_template(locale))
    });
    command = command.mut_subcommand("sync", |sub| {
        sub.about(sync_about(locale))
            .version(APP_VERSION)
            .disable_help_flag(true)
            .disable_version_flag(true)
            .disable_help_subcommand(true)
            .help_template(help_template(locale))
            .mut_arg("sqlite_only", |arg| {
                arg.help(sqlite_only_help(locale))
                    .help_heading(options_heading(locale))
            })
    });
    command = command.mut_subcommand("bucket", |sub| {
        sub.about(bucket_about(locale))
            .version(APP_VERSION)
            .disable_help_flag(true)
            .disable_version_flag(true)
            .disable_help_subcommand(true)
            .help_template(help_template(locale))
            .mut_subcommand("prepare", |sub| {
                sub.about(bucket_prepare_about(locale))
                    .version(APP_VERSION)
                    .disable_help_flag(true)
                    .disable_version_flag(true)
                    .disable_help_subcommand(true)
                    .help_template(help_template(locale))
                    .mut_arg("padding_bytes", |arg| {
                        arg.help(padding_bytes_help(locale))
                            .help_heading(options_heading(locale))
                            .value_name(bytes_value_name(locale))
                    })
            })
            .mut_subcommand("switch", |sub| {
                sub.about(bucket_switch_about(locale))
                    .version(APP_VERSION)
                    .disable_help_flag(true)
                    .disable_version_flag(true)
                    .disable_help_subcommand(true)
                    .help_template(help_template(locale))
                    .mut_arg("target_provider", |arg| {
                        arg.help(bucket_switch_provider_help(locale))
                            .value_name(provider_value_name(locale))
                    })
                    .mut_arg("padding_bytes", |arg| {
                        arg.help(padding_bytes_help(locale))
                            .help_heading(options_heading(locale))
                            .value_name(bytes_value_name(locale))
                    })
            })
    });
    command = command.mut_subcommand("watch", |sub| {
        sub.about(watch_about(locale))
            .version(APP_VERSION)
            .disable_help_flag(true)
            .disable_version_flag(true)
            .disable_help_subcommand(true)
            .help_template(help_template(locale))
            .mut_arg("sqlite_only", |arg| {
                arg.help(sqlite_only_help(locale))
                    .help_heading(options_heading(locale))
            })
            .mut_arg("poll_interval_ms", |arg| {
                arg.help(poll_interval_help(locale))
                    .help_heading(options_heading(locale))
                    .value_name(milliseconds_value_name(locale))
            })
    });
    command = command.mut_subcommand("print-service-config", |sub| {
        sub.about(print_service_config_about(locale))
            .version(APP_VERSION)
            .disable_help_flag(true)
            .disable_version_flag(true)
            .disable_help_subcommand(true)
            .help_template(help_template(locale))
            .mut_arg("poll_interval_ms", |arg| {
                arg.help(service_poll_help(locale))
                    .help_heading(options_heading(locale))
                    .value_name(milliseconds_value_name(locale))
            })
    });
    command = command.mut_subcommand("install-service", |sub| {
        sub.about(install_service_about(locale))
            .version(APP_VERSION)
            .disable_help_flag(true)
            .disable_version_flag(true)
            .disable_help_subcommand(true)
            .help_template(help_template(locale))
            .mut_arg("poll_interval_ms", |arg| {
                arg.help(service_poll_help(locale))
                    .help_heading(options_heading(locale))
                    .value_name(milliseconds_value_name(locale))
            })
    });
    command = command.mut_subcommand("uninstall-service", |sub| {
        sub.about(uninstall_service_about(locale))
            .version(APP_VERSION)
            .disable_help_flag(true)
            .disable_version_flag(true)
            .disable_help_subcommand(true)
            .help_template(help_template(locale))
    });
    command = command.mut_subcommand("restore", |sub| {
        sub.about(restore_about(locale))
            .version(APP_VERSION)
            .disable_help_flag(true)
            .disable_version_flag(true)
            .disable_help_subcommand(true)
            .help_template(help_template(locale))
            .mut_arg("backup", |arg| {
                arg.help(restore_backup_help(locale))
                    .value_name("BACKUP")
            })
            .mut_arg("dry_run", |arg| {
                arg.help(dry_run_help(locale))
                    .help_heading(options_heading(locale))
            })
    });
    command = command.mut_subcommand("prune-backups", |sub| {
        sub.about(prune_backups_about(locale))
            .version(APP_VERSION)
            .disable_help_flag(true)
            .disable_version_flag(true)
            .disable_help_subcommand(true)
            .help_template(help_template(locale))
            .mut_arg("keep", |arg| {
                arg.help(prune_backups_keep_help(locale))
                    .help_heading(options_heading(locale))
                    .value_name("N")
            })
            .mut_arg("dry_run", |arg| {
                arg.help(dry_run_help(locale))
                    .help_heading(options_heading(locale))
            })
    });

    command
}

fn install_service(
    locale: Locale,
    codex_home: &Path,
    provider_override: Option<&str>,
    poll_interval: Duration,
) -> Result<()> {
    let exe_path = std::env::current_exe().context(current_exe_error(locale))?;
    let summary = service::install_service(
        exe_path.as_path(),
        codex_home,
        provider_override,
        poll_interval,
    )?;

    print_install_service_summary(locale, codex_home, poll_interval, &summary);
    println!();
    println!("{}", next_steps_heading(locale));
    for line in install_next_steps(locale, exe_path.as_path(), codex_home, summary.manager)? {
        println!("{line}");
    }
    Ok(())
}

fn uninstall_service(locale: Locale, codex_home: &Path) -> Result<()> {
    let service_status = service::current_service_status()?;
    if service_status.installed {
        let config_path = service::uninstall_service()?;
        println!("{}", service_uninstalled_message(locale));
        println!("{}", service_config_path_message(locale, &config_path));
        println!();
        println!("{}", next_steps_heading(locale));
        println!(
            "{}",
            run_status_next_step(
                locale,
                &cli_status_command(std::env::current_exe()?, codex_home)
            )
        );
    } else {
        println!(
            "{}",
            no_service_config_message(locale, &service_status.config_path)
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::APP_VERSION;
    use super::Cli;
    use super::Command;
    use super::DEFAULT_POLL_INTERVAL_MS;
    use super::localized_command;
    use super::validate_provider_override;
    use super::validate_provider_override_args;
    use crate::backup::reconcile_once;
    use crate::backup::reconcile_sqlite_with_backup;
    use crate::config::DEFAULT_BUCKET_PADDING_BYTES;
    use crate::config::read_provider_from_config;
    use crate::config::resolve_sqlite_home_from_config;
    use crate::config::resolve_sqlite_path;
    use crate::locale::detect_locale_from_sources;
    use crate::locale::parse_apple_languages;
    use crate::locale::parse_locale_tag;
    use crate::locale::Locale;
    use crate::rollout::reconcile_rollout_metadata_from_sqlite_with_progress;
    use crate::sqlite::inspect_sqlite_distribution;
    use crate::sqlite::reconcile_sqlite_in_place;
    use crate::status::install_next_steps;
    use crate::types::RolloutScope;
    use crate::service::ServiceManager;
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
            rendered
                .contains("Force a provider instead of reading model_provider from config.toml")
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
        let provider = read_provider_from_config(dir.path())?;
        assert_eq!(provider, "vm");
        Ok(())
    }

    #[test]
    fn defaults_to_openai_without_config() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let provider = read_provider_from_config(dir.path())?;
        assert_eq!(provider, "openai");
        Ok(())
    }

    #[test]
    fn defaults_to_openai_with_blank_provider_in_config() -> Result<()> {
        let dir = tempfile::tempdir()?;
        fs::write(dir.path().join("config.toml"), "model_provider = \"   \"\n")?;
        let provider = read_provider_from_config(dir.path())?;
        assert_eq!(provider, "openai");
        Ok(())
    }

    #[test]
    fn resolves_sqlite_path_from_config_sqlite_home() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let sqlite_home = dir.path().join("custom-state");
        // Use forward slashes so TOML doesn't interpret Windows backslashes as escapes
        let path_str = sqlite_home.display().to_string().replace('\\', "/");
        fs::write(
            dir.path().join("config.toml"),
            format!("sqlite_home = \"{path_str}\"\n"),
        )?;

        let sqlite_path = resolve_sqlite_path(dir.path())?;

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
    fn reconcile_once_returns_error_when_db_missing() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let sqlite_path = dir.path().join("state_5.sqlite");
        let path_str = dir.path().display().to_string().replace('\\', "/");
        fs::write(
            dir.path().join("config.toml"),
            format!("sqlite_home = \"{path_str}\"\n"),
        )?;

        let err = reconcile_once(dir.path(), Some("openai"), RolloutScope::None).unwrap_err();

        let err_msg = err.to_string().replace('\\', "/");
        let path_str = sqlite_path.display().to_string().replace('\\', "/");
        assert!(
            err_msg.contains(&path_str),
            "error should contain path, got: {err_msg}"
        );
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
    fn durable_sync_updates_matching_rollout_session_meta() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let codex_home = dir.path();
        let sqlite_path = codex_home.join("state_5.sqlite");
        let rollout_path =
            codex_home.join("sessions/2026/05/07/rollout-2026-05-07T14-06-18-1.jsonl");
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
        let rollout_path =
            codex_home.join("sessions/2026/05/07/rollout-2026-05-07T14-06-18-1.jsonl");
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
    fn generates_launchd_plist() {
        let plist = crate::service::build_launchd_plist(
            PathBuf::from("/tmp/codex-threadripper").as_path(),
            PathBuf::from("/tmp/codex-home").as_path(),
            Some("openai"),
            Duration::from_millis(DEFAULT_POLL_INTERVAL_MS),
        );
        assert!(plist.contains("dev.wangnov.codex-threadripper"));
        assert!(plist.contains("--codex-home"));
        assert!(plist.contains("--provider"));
        assert!(plist.contains("watch"));
    }

    #[test]
    fn builds_install_next_steps() -> Result<()> {
        let steps = install_next_steps(
            Locale::ZhHans,
            PathBuf::from("/tmp/codex threadripper").as_path(),
            PathBuf::from("/tmp/codex home").as_path(),
            ServiceManager::Launchd,
        )?;
        assert!(steps.len() >= 2);
        assert!(steps[0].contains("运行这条命令查看状态"));
        assert!(
            steps[0].contains("'/tmp/codex threadripper' --codex-home '/tmp/codex home' status")
        );
        assert!(steps.last().unwrap().contains("tail -f"));
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
                source TEXT NOT NULL,
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
                agent_path TEXT
            );
            INSERT INTO threads (
                id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
                sandbox_policy, approval_mode
            ) VALUES
                ('1', '/tmp/a', 1, 1, 'cli', 'vm', '/tmp', 'a', 'workspace-write', 'auto'),
                ('2', '/tmp/b', 1, 1, 'cli', 'cp', '/tmp', 'b', 'workspace-write', 'auto'),
                ('3', '/tmp/c', 1, 1, 'cli', 'openai', '/tmp', 'c', 'workspace-write', 'auto');
            ",
        )?;
        Ok(())
    }

    fn assert_rollout_mtime(path: &Path, expected: FileTime) -> Result<()> {
        let actual = FileTime::from_last_modification_time(&fs::metadata(path)?);
        assert_eq!(actual.unix_seconds(), expected.unix_seconds());
        Ok(())
    }
}
