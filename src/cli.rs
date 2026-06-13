use anyhow::Result;
use clap::Arg;
use clap::ArgAction;
use clap::CommandFactory;
use clap::FromArgMatches;
use clap::Parser;
use clap::Subcommand;
use std::ffi::OsStr;
use std::path::PathBuf;

use crate::codex_config::is_valid_profile_name;
use crate::locale::Locale;
use crate::output::*;

pub(crate) const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
pub(crate) const DEFAULT_POLL_INTERVAL_MS: u64 = 500;
pub(crate) const DEFAULT_BUCKET_PADDING_BYTES: usize = 256;

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
pub(crate) struct Cli {
    #[arg(long, global = true, value_name = "DIR", help = "placeholder")]
    pub(crate) codex_home: Option<PathBuf>,

    #[arg(long, global = true, value_name = "PROVIDER", help = "placeholder")]
    pub(crate) provider: Option<String>,

    #[arg(long, global = true, value_name = "PROFILE", help = "placeholder")]
    pub(crate) profile: Option<String>,

    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
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
    #[command(name = "print-service-config", alias = "print-plist")]
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
    #[command(name = "install-service", alias = "install-launchd")]
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
    #[command(name = "uninstall-service", alias = "uninstall-launchd")]
    UninstallService,
    /// placeholder
    Restore {
        #[arg(
            value_name = "BACKUP_PATH",
            help = "placeholder",
            conflicts_with = "latest"
        )]
        backup_path: Option<PathBuf>,
        #[arg(long, help = "placeholder")]
        latest: bool,
        #[arg(long, help = "placeholder")]
        force: bool,
    },
    /// placeholder
    #[command(name = "prune-backups")]
    PruneBackups {
        #[arg(long, default_value_t = 5, value_name = "N", help = "placeholder")]
        keep: usize,
        #[arg(long, help = "placeholder")]
        force: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum BucketCommand {
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

pub(crate) fn localized_command(locale: Locale) -> clap::Command {
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
    command = command.mut_arg("profile", |arg| {
        arg.help(profile_help(locale))
            .help_heading(options_heading(locale))
            .value_name(profile_value_name(locale))
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
            .mut_arg("dry_run", |arg| {
                arg.help(dry_run_help(locale))
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
        sub.about(print_plist_about(locale))
            .version(APP_VERSION)
            .disable_help_flag(true)
            .disable_version_flag(true)
            .disable_help_subcommand(true)
            .help_template(help_template(locale))
            .mut_arg("poll_interval_ms", |arg| {
                arg.help(launchd_poll_help(locale))
                    .help_heading(options_heading(locale))
                    .value_name(milliseconds_value_name(locale))
            })
    });
    command = command.mut_subcommand("install-service", |sub| {
        sub.about(install_launchd_about(locale))
            .version(APP_VERSION)
            .disable_help_flag(true)
            .disable_version_flag(true)
            .disable_help_subcommand(true)
            .help_template(help_template(locale))
            .mut_arg("poll_interval_ms", |arg| {
                arg.help(launchd_poll_help(locale))
                    .help_heading(options_heading(locale))
                    .value_name(milliseconds_value_name(locale))
            })
    });
    command = command.mut_subcommand("uninstall-service", |sub| {
        sub.about(uninstall_launchd_about(locale))
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
            .mut_arg("backup_path", |arg| {
                arg.help(restore_path_help(locale))
                    .value_name(restore_path_value_name(locale))
            })
            .mut_arg("latest", |arg| {
                arg.help(restore_latest_help(locale))
                    .help_heading(options_heading(locale))
            })
            .mut_arg("force", |arg| {
                arg.help(restore_force_help(locale))
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
                arg.help(prune_keep_help(locale))
                    .help_heading(options_heading(locale))
            })
            .mut_arg("force", |arg| {
                arg.help(prune_force_help(locale))
                    .help_heading(options_heading(locale))
            })
    });

    command
}

pub(crate) fn parse_cli(locale: Locale) -> Result<Cli> {
    validate_provider_override_args(locale, std::env::args_os())?;
    let command = localized_command(locale);
    let matches = command.clone().get_matches();
    Cli::from_arg_matches(&matches).map_err(|err| anyhow::anyhow!(err.to_string()))
}

pub(crate) fn validate_provider_override(locale: Locale, provider: Option<&str>) -> Result<()> {
    if provider.is_some_and(|value| value.trim().is_empty()) {
        anyhow::bail!(provider_empty_error(locale));
    }
    Ok(())
}

pub(crate) fn validate_profile_override(locale: Locale, profile: Option<&str>) -> Result<()> {
    let Some(profile) = profile else {
        return Ok(());
    };
    let trimmed = profile.trim();
    if trimmed.is_empty() {
        anyhow::bail!(profile_empty_error(locale));
    }
    if !is_valid_profile_name(trimmed) {
        anyhow::bail!(profile_path_error(locale));
    }
    Ok(())
}

pub(crate) fn validate_provider_override_args<I, T>(locale: Locale, args: I) -> Result<()>
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
        if arg == OsStr::new("--profile") {
            if let Some(value) = args.next() {
                validate_profile_override(locale, value.as_ref().to_str())?;
            }
            continue;
        }

        let rendered = arg.to_string_lossy();
        if let Some(value) = rendered.strip_prefix("--provider=") {
            validate_provider_override(locale, Some(value))?;
        }
        if let Some(value) = rendered.strip_prefix("--profile=") {
            validate_profile_override(locale, Some(value))?;
        }
    }
    Ok(())
}
