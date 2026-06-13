use anyhow::Context;
use anyhow::Result;
use std::path::Path;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

mod cli;
mod codex_config;
mod fs_sync;
mod locale;
mod output;
mod rollout;
mod service;
mod state_db;
mod stores;
mod sync;
#[cfg(test)]
mod tests;
mod watch;

use cli::BucketCommand;
use cli::Command;
use cli::DEFAULT_BUCKET_PADDING_BYTES;
use cli::parse_cli;
use cli::validate_profile_override;
use cli::validate_provider_override;
use locale::Locale;
use locale::detect_locale;
use output::bucket_switch_complete_title;
use output::cli_status_command;
use output::current_exe_error;
use output::install_next_steps;
use output::launchd_plist_message;
use output::next_steps_heading;
use output::no_launchd_plist_message;
use output::print_bucket_prepare_summary;
use output::print_install_service_summary;
use output::print_multi_sync_summary;
use output::print_status;
use output::run_status_next_step;
use output::sqlite_only_app_warning;
use output::sync_complete_title;
use output::uninstall_launchd_done;
use rollout::RolloutProgressConfig;
use rollout::RolloutScope;
use rollout::prepare_bucket_padding;
use sync::ReconcileStatus;
use sync::collect_status;
use sync::reconcile_all_stores_with_backup;
use watch::run_watch;

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err:?}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<ExitCode> {
    let locale = detect_locale();
    let cli = parse_cli(locale)?;
    validate_provider_override(locale, cli.provider.as_deref())?;
    validate_profile_override(locale, cli.profile.as_deref())?;
    let codex_home = resolve_codex_home(cli.codex_home)?;

    match cli.command {
        Command::Status => {
            let summary =
                collect_status(&codex_home, cli.provider.as_deref(), cli.profile.as_deref())?;
            print_status(locale, &summary);
            Ok(ExitCode::SUCCESS)
        }
        Command::Sync { sqlite_only } => {
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
            let summary = reconcile_all_stores_with_backup(
                &codex_home,
                cli.provider.as_deref(),
                cli.profile.as_deref(),
                rollout_scope,
                DEFAULT_BUCKET_PADDING_BYTES,
                progress,
            )?;
            print_multi_sync_summary(locale, sync_complete_title(locale), &summary);
            if sqlite_only && summary.touches_app_store(&codex_home) {
                eprintln!("{}", sqlite_only_app_warning(locale));
            }
            Ok(exit_code_for(summary.status()))
        }
        Command::Bucket { command } => match command {
            BucketCommand::Prepare { padding_bytes } => {
                let summary =
                    prepare_bucket_padding(&codex_home, cli.profile.as_deref(), padding_bytes)?;
                print_bucket_prepare_summary(locale, &summary);
                Ok(ExitCode::SUCCESS)
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
                let summary = reconcile_all_stores_with_backup(
                    &codex_home,
                    provider.as_deref(),
                    cli.profile.as_deref(),
                    RolloutScope::AllRows,
                    padding_bytes,
                    Some(RolloutProgressConfig { locale }),
                )?;
                print_multi_sync_summary(locale, bucket_switch_complete_title(locale), &summary);
                Ok(exit_code_for(summary.status()))
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
                cli.profile.clone(),
                if sqlite_only {
                    RolloutScope::None
                } else {
                    RolloutScope::MismatchedRows
                },
                Duration::from_millis(poll_interval_ms),
            )?;
            Ok(ExitCode::SUCCESS)
        }
        Command::PrintServiceConfig { poll_interval_ms } => {
            let exe_path = std::env::current_exe().context(current_exe_error(locale))?;
            let config = service::render_service_config(
                exe_path.as_path(),
                &codex_home,
                cli.provider.as_deref(),
                cli.profile.as_deref(),
                Duration::from_millis(poll_interval_ms),
            )?;
            println!("{config}");
            Ok(ExitCode::SUCCESS)
        }
        Command::InstallService { poll_interval_ms } => {
            install_service(
                locale,
                &codex_home,
                cli.provider.as_deref(),
                cli.profile.as_deref(),
                Duration::from_millis(poll_interval_ms),
            )?;
            Ok(ExitCode::SUCCESS)
        }
        Command::UninstallService => {
            uninstall_service(locale, &codex_home)?;
            Ok(ExitCode::SUCCESS)
        }
    }
}

fn exit_code_for(status: ReconcileStatus) -> ExitCode {
    match status {
        ReconcileStatus::Full => ExitCode::SUCCESS,
        ReconcileStatus::Partial => ExitCode::from(2),
        ReconcileStatus::Failed => ExitCode::FAILURE,
    }
}

fn resolve_codex_home(cli_codex_home: Option<PathBuf>) -> Result<PathBuf> {
    resolve_codex_home_from_env(
        cli_codex_home,
        std::env::var_os("CODEX_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    )
}

fn resolve_codex_home_from_env(
    cli_codex_home: Option<PathBuf>,
    env_codex_home: Option<PathBuf>,
    env_home: Option<PathBuf>,
) -> Result<PathBuf> {
    if let Some(path) = cli_codex_home {
        return Ok(path);
    }
    if let Some(path) = normalize_codex_home_env(env_codex_home)? {
        return Ok(path);
    }
    Ok(default_codex_home_from_env(env_home))
}

fn normalize_codex_home_env(env_codex_home: Option<PathBuf>) -> Result<Option<PathBuf>> {
    let Some(path) = env_codex_home else {
        return Ok(None);
    };
    if path.as_os_str().is_empty() {
        return Ok(None);
    }
    if !path.is_dir() {
        anyhow::bail!(
            "CODEX_HOME must point to an existing directory: {}",
            path.display()
        );
    }
    Ok(Some(path.canonicalize().unwrap_or(path)))
}

fn default_codex_home_from_env(env_home: Option<PathBuf>) -> PathBuf {
    env_home
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".codex")
}

fn install_service(
    locale: Locale,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    poll_interval: Duration,
) -> Result<()> {
    let exe_path = std::env::current_exe().context(current_exe_error(locale))?;
    let summary = service::install_service(
        exe_path.as_path(),
        codex_home,
        provider_override,
        profile_override,
        poll_interval,
    )?;

    print_install_service_summary(locale, codex_home, poll_interval, &summary);
    println!();
    println!("{}", next_steps_heading(locale));
    for line in install_next_steps(
        locale,
        exe_path.as_path(),
        codex_home,
        provider_override,
        profile_override,
        summary.manager,
    )? {
        println!("{line}");
    }
    Ok(())
}

fn uninstall_service(locale: Locale, codex_home: &Path) -> Result<()> {
    let service_status = service::current_service_status()?;
    if service_status.installed {
        let config_path = service::uninstall_service()?;
        println!("{}", uninstall_launchd_done(locale));
        println!("{}", launchd_plist_message(locale, &config_path));
        println!();
        println!("{}", next_steps_heading(locale));
        println!(
            "{}",
            run_status_next_step(
                locale,
                &cli_status_command(std::env::current_exe()?, codex_home, None, None)
            )
        );
    } else {
        println!(
            "{}",
            no_launchd_plist_message(locale, &service_status.config_path)
        );
    }
    Ok(())
}
