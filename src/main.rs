use anyhow::Context;
use anyhow::Result;
use clap::Arg;
use clap::ArgAction;
use clap::CommandFactory;
use clap::FromArgMatches;
use clap::Parser;
use clap::Subcommand;
use filetime::FileTime;
use filetime::set_file_times;
use fs2::FileExt;
use notify::Config as NotifyConfig;
use notify::EventKind;
use notify::RecommendedWatcher;
use notify::RecursiveMode;
use notify::Watcher;
use rusqlite::Connection;
use rusqlite::TransactionBehavior;
use rusqlite::backup::Backup;
use rusqlite::backup::StepResult;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::IsTerminal;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command as ProcessCommand;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_PROVIDER: &str = "openai";
const DEFAULT_POLL_INTERVAL_MS: u64 = 500;
const DEFAULT_BUCKET_PADDING_BYTES: usize = 256;
const ROLLOUT_PROGRESS_INTERVAL: Duration = Duration::from_millis(500);

type ProviderDistribution = Vec<(String, u64)>;

mod service;

use service::ServiceInstallSummary;
use service::ServiceManager;
use service::ServiceStatus as BackgroundServiceStatus;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Locale {
    En,
    ZhHans,
}

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

#[derive(Debug, Deserialize)]
struct ConfigToml {
    model_provider: Option<String>,
}

#[derive(Debug)]
struct ReconcileSummary {
    provider: String,
    changed_rows: u64,
    total_rows: u64,
    changed_rollouts: u64,
    checked_rollouts: u64,
    prepared_rollouts: u64,
    skipped_rollouts: u64,
    elapsed: Duration,
    backup_path: Option<PathBuf>,
    rollout_journal_path: Option<PathBuf>,
}

#[derive(Debug)]
struct StatusSummary {
    codex_home: PathBuf,
    sqlite_path: PathBuf,
    config_path: PathBuf,
    provider: String,
    total_rows: u64,
    mismatched_rows: u64,
    distribution: ProviderDistribution,
    service_status: BackgroundServiceStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RolloutScope {
    None,
    MismatchedRows,
    AllRows,
}

#[derive(Debug)]
struct RolloutTarget {
    thread_id: String,
    path: PathBuf,
}

#[derive(Debug, Default)]
struct RolloutReconcileSummary {
    checked_files: u64,
    changed_files: u64,
    prepared_files: u64,
    skipped_files: u64,
    journal_path: Option<PathBuf>,
}

#[derive(Debug)]
struct BucketPrepareSummary {
    checked_rollouts: u64,
    prepared_rollouts: u64,
    skipped_rollouts: u64,
    elapsed: Duration,
    journal_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug)]
struct RolloutProgressConfig {
    locale: Locale,
}

struct RolloutProgress {
    config: RolloutProgressConfig,
    total_files: u64,
    visited_files: u64,
    started_at: Instant,
    last_print_at: Instant,
    is_terminal: bool,
    printed: bool,
}

#[derive(Debug)]
struct FirstLine {
    content: Vec<u8>,
    newline: Vec<u8>,
}

impl RolloutProgress {
    fn new(config: RolloutProgressConfig, total_files: u64) -> Self {
        let now = Instant::now();
        Self {
            config,
            total_files,
            visited_files: 0,
            started_at: now,
            last_print_at: now,
            is_terminal: std::io::stderr().is_terminal(),
            printed: false,
        }
    }

    fn tick(&mut self, summary: &RolloutReconcileSummary) {
        if self.total_files == 0 {
            return;
        }
        self.visited_files += 1;
        let now = Instant::now();
        let should_print = self.visited_files == 1
            || self.visited_files == self.total_files
            || now.duration_since(self.last_print_at) >= ROLLOUT_PROGRESS_INTERVAL;
        if should_print {
            self.print(summary, false);
            self.last_print_at = now;
        }
    }

    fn finish(&mut self, summary: &RolloutReconcileSummary) {
        if self.total_files == 0 {
            return;
        }
        self.print(summary, true);
    }

    fn print(&mut self, summary: &RolloutReconcileSummary, final_line: bool) {
        let message = rollout_progress_message(
            self.config.locale,
            self.visited_files,
            self.total_files,
            summary.checked_files,
            summary.changed_files,
            summary.prepared_files,
            summary.skipped_files,
            self.started_at.elapsed(),
        );
        let mut stderr = std::io::stderr();
        if self.is_terminal {
            let _ = write!(stderr, "\r{message}\x1b[K");
            if final_line {
                let _ = writeln!(stderr);
            }
        } else if final_line || !self.printed {
            let _ = writeln!(stderr, "{message}");
        }
        let _ = stderr.flush();
        self.printed = true;
    }
}

#[derive(Debug, Default)]
struct RolloutPatchOutcome {
    changed: bool,
    prepared: bool,
    skipped: bool,
}

struct RolloutChangeJournal {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
}

#[derive(Clone, Copy)]
enum RolloutChangeMode {
    InPlace,
    RewriteWithPadding,
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
            let summary = reconcile_once_with_backup_progress(
                &codex_home,
                cli.provider.as_deref(),
                rollout_scope,
                progress,
            )?;
            print_sync_summary(locale, sync_complete_title(locale), &summary);
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

    command
}

fn detect_locale() -> Locale {
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

fn detect_locale_from_sources<'a, I>(candidates: I, apple_languages: Option<&str>) -> Locale
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

fn parse_locale_tag(input: &str) -> Option<Locale> {
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

fn parse_apple_languages(output: &str) -> Option<Locale> {
    output
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'))
        .find_map(parse_locale_tag)
}

fn apple_languages_output() -> Option<String> {
    if !cfg!(target_os = "macos") {
        return None;
    }
    let output = ProcessCommand::new("defaults")
        .args(["read", "-g", "AppleLanguages"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

fn default_codex_home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".codex")
}

fn run_watch(
    locale: Locale,
    codex_home: &Path,
    provider_override: Option<String>,
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
    match reconcile_once(codex_home, provider_override.as_deref(), rollout_scope) {
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
                    match reconcile_once(codex_home, provider_override.as_deref(), rollout_scope) {
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
                match reconcile_once(codex_home, provider_override.as_deref(), rollout_scope) {
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

fn collect_status(codex_home: &Path, provider_override: Option<&str>) -> Result<StatusSummary> {
    let config_path = codex_home.join("config.toml");
    let sqlite_path = codex_home.join("state_5.sqlite");
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
    })
}

fn reconcile_once(
    codex_home: &Path,
    provider_override: Option<&str>,
    rollout_scope: RolloutScope,
) -> Result<ReconcileSummary> {
    reconcile_once_with_progress(codex_home, provider_override, rollout_scope, None)
}

fn reconcile_once_with_progress(
    codex_home: &Path,
    provider_override: Option<&str>,
    rollout_scope: RolloutScope,
    progress: Option<RolloutProgressConfig>,
) -> Result<ReconcileSummary> {
    let provider = match provider_override {
        Some(provider) => provider.to_string(),
        None => read_provider_from_config(codex_home)?,
    };
    let sqlite_path = codex_home.join("state_5.sqlite");
    let started = Instant::now();
    let rollout_summary = reconcile_rollout_metadata_from_sqlite_with_progress(
        &sqlite_path,
        codex_home,
        provider.as_str(),
        rollout_scope,
        None,
        DEFAULT_BUCKET_PADDING_BYTES,
        progress,
    )?;
    let (changed_rows, total_rows) = reconcile_sqlite_in_place(&sqlite_path, provider.as_str())?;

    Ok(ReconcileSummary {
        provider,
        changed_rows,
        total_rows,
        changed_rollouts: rollout_summary.changed_files,
        checked_rollouts: rollout_summary.checked_files,
        prepared_rollouts: rollout_summary.prepared_files,
        skipped_rollouts: rollout_summary.skipped_files,
        elapsed: started.elapsed(),
        backup_path: None,
        rollout_journal_path: rollout_summary.journal_path,
    })
}

fn reconcile_once_with_backup_progress(
    codex_home: &Path,
    provider_override: Option<&str>,
    rollout_scope: RolloutScope,
    progress: Option<RolloutProgressConfig>,
) -> Result<ReconcileSummary> {
    reconcile_once_with_backup_and_padding(
        codex_home,
        provider_override,
        rollout_scope,
        DEFAULT_BUCKET_PADDING_BYTES,
        progress,
    )
}

fn reconcile_once_with_backup_and_padding(
    codex_home: &Path,
    provider_override: Option<&str>,
    rollout_scope: RolloutScope,
    padding_bytes: usize,
    progress: Option<RolloutProgressConfig>,
) -> Result<ReconcileSummary> {
    let provider = match provider_override {
        Some(provider) => provider.to_string(),
        None => read_provider_from_config(codex_home)?,
    };
    let sqlite_path = codex_home.join("state_5.sqlite");
    let started = Instant::now();
    let backup_path = create_sqlite_backup_file(&sqlite_path)?;
    let rollout_journal_path =
        backup_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!(
                "rollouts.{}.jsonl",
                backup_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("state_5.sqlite.bak")
            ));
    let rollout_summary = reconcile_rollout_metadata_from_sqlite_with_progress(
        &sqlite_path,
        codex_home,
        provider.as_str(),
        rollout_scope,
        Some(rollout_journal_path.as_path()),
        padding_bytes,
        progress,
    )?;
    let (changed_rows, total_rows) = reconcile_sqlite_in_place(&sqlite_path, provider.as_str())?;

    Ok(ReconcileSummary {
        provider,
        changed_rows,
        total_rows,
        changed_rollouts: rollout_summary.changed_files,
        checked_rollouts: rollout_summary.checked_files,
        prepared_rollouts: rollout_summary.prepared_files,
        skipped_rollouts: rollout_summary.skipped_files,
        elapsed: started.elapsed(),
        backup_path: Some(backup_path),
        rollout_journal_path: rollout_summary.journal_path,
    })
}

fn read_provider_from_config(codex_home: &Path) -> Result<String> {
    let config_path = codex_home.join("config.toml");
    if !config_path.exists() {
        return Ok(DEFAULT_PROVIDER.to_string());
    }

    let raw = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    let parsed: ConfigToml = toml::from_str(&raw)
        .with_context(|| format!("failed to parse {}", config_path.display()))?;
    Ok(parsed
        .model_provider
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_PROVIDER.to_string()))
}

fn reconcile_rollout_metadata_from_sqlite_with_progress(
    sqlite_path: &Path,
    _codex_home: &Path,
    provider: &str,
    scope: RolloutScope,
    journal_path: Option<&Path>,
    padding_bytes: usize,
    progress: Option<RolloutProgressConfig>,
) -> Result<RolloutReconcileSummary> {
    if scope == RolloutScope::None {
        return Ok(RolloutReconcileSummary::default());
    }
    let targets = rollout_targets_for_scope(sqlite_path, provider, scope)?;
    reconcile_rollout_metadata_files(
        targets.as_slice(),
        provider,
        journal_path,
        padding_bytes,
        progress,
    )
}

fn rollout_targets_for_scope(
    sqlite_path: &Path,
    provider: &str,
    scope: RolloutScope,
) -> Result<Vec<RolloutTarget>> {
    ensure_sqlite_exists(sqlite_path)?;
    let connection = Connection::open(sqlite_path)
        .with_context(|| format!("failed to open {}", sqlite_path.display()))?;
    let sql = match scope {
        RolloutScope::None => return Ok(Vec::new()),
        RolloutScope::MismatchedRows => {
            "SELECT id, rollout_path FROM threads WHERE model_provider <> ?1 AND rollout_path <> '' ORDER BY updated_at DESC"
        }
        RolloutScope::AllRows => {
            "SELECT id, rollout_path FROM threads WHERE rollout_path <> '' ORDER BY updated_at DESC"
        }
    };
    let mut statement = connection.prepare(sql)?;
    if scope == RolloutScope::MismatchedRows {
        let rows = statement.query_map([provider], |row| {
            Ok(RolloutTarget {
                thread_id: row.get(0)?,
                path: PathBuf::from(row.get::<_, String>(1)?),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    } else {
        let rows = statement.query_map([], |row| {
            Ok(RolloutTarget {
                thread_id: row.get(0)?,
                path: PathBuf::from(row.get::<_, String>(1)?),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }
}

fn reconcile_rollout_metadata_files(
    targets: &[RolloutTarget],
    provider: &str,
    journal_path: Option<&Path>,
    padding_bytes: usize,
    progress: Option<RolloutProgressConfig>,
) -> Result<RolloutReconcileSummary> {
    let mut summary = RolloutReconcileSummary::default();
    let mut journal = journal_path.map(|path| RolloutChangeJournal::new(path.to_path_buf()));
    let mut progress = progress.map(|config| RolloutProgress::new(config, targets.len() as u64));
    for target in targets {
        if !target.path.exists() {
            if let Some(progress) = progress.as_mut() {
                progress.tick(&summary);
            }
            continue;
        }
        summary.checked_files += 1;
        let outcome =
            rewrite_rollout_provider_first_line(target, provider, journal.as_mut(), padding_bytes)?;
        if outcome.changed {
            summary.changed_files += 1;
        }
        if outcome.prepared {
            summary.prepared_files += 1;
        }
        if outcome.skipped {
            summary.skipped_files += 1;
        }
        if let Some(progress) = progress.as_mut() {
            progress.tick(&summary);
        }
    }
    if let Some(progress) = progress.as_mut() {
        progress.finish(&summary);
    }
    if let Some(journal) = journal {
        summary.journal_path = journal.finish()?;
    }
    Ok(summary)
}

fn rewrite_rollout_provider_first_line(
    target: &RolloutTarget,
    provider: &str,
    journal: Option<&mut RolloutChangeJournal>,
    padding_bytes: usize,
) -> Result<RolloutPatchOutcome> {
    let first_line = read_first_line(&target.path)?;
    let Some(mut value) =
        parse_matching_session_meta(first_line.content.as_slice(), target.thread_id.as_str())?
    else {
        return Ok(RolloutPatchOutcome {
            skipped: true,
            ..RolloutPatchOutcome::default()
        });
    };
    let old_provider = session_meta_provider(&value).unwrap_or("").to_string();

    if !set_session_meta_provider(&mut value, provider) {
        return Ok(RolloutPatchOutcome::default());
    }

    let rendered = serde_json::to_vec(&value)
        .with_context(|| format!("failed to render {}", target.path.display()))?;
    if rendered.len() <= first_line.content.len() {
        let mut replacement = rendered;
        replacement.resize(first_line.content.len(), b' ');
        record_rollout_change(
            journal,
            target,
            old_provider.as_str(),
            provider,
            first_line.content.len(),
            replacement.len(),
            RolloutChangeMode::InPlace,
        )?;
        patch_first_line_in_place(
            &target.path,
            replacement.as_slice(),
            first_line.newline.as_slice(),
        )?;
        return Ok(RolloutPatchOutcome {
            changed: true,
            ..RolloutPatchOutcome::default()
        });
    }

    let mut replacement = rendered;
    replacement.resize(replacement.len() + padding_bytes, b' ');
    record_rollout_change(
        journal,
        target,
        old_provider.as_str(),
        provider,
        first_line.content.len(),
        replacement.len(),
        RolloutChangeMode::RewriteWithPadding,
    )?;
    rewrite_first_line_atomically(
        &target.path,
        replacement.as_slice(),
        first_line.newline.as_slice(),
    )?;
    Ok(RolloutPatchOutcome {
        changed: true,
        prepared: true,
        ..RolloutPatchOutcome::default()
    })
}

fn prepare_bucket_padding(codex_home: &Path, padding_bytes: usize) -> Result<BucketPrepareSummary> {
    let sqlite_path = codex_home.join("state_5.sqlite");
    let started = Instant::now();
    let targets = rollout_targets_for_scope(&sqlite_path, DEFAULT_PROVIDER, RolloutScope::AllRows)?;
    let journal_path = codex_home
        .join("backups")
        .join(format!("bucket-prepare.{}.jsonl", unix_timestamp_millis()?));
    let mut journal = RolloutChangeJournal::new(journal_path);
    let mut checked_rollouts = 0;
    let mut prepared_rollouts = 0;
    let mut skipped_rollouts = 0;

    for target in &targets {
        if !target.path.exists() {
            continue;
        }
        checked_rollouts += 1;
        let outcome = prepare_rollout_first_line_padding(target, &mut journal, padding_bytes)?;
        if outcome.prepared {
            prepared_rollouts += 1;
        }
        if outcome.skipped {
            skipped_rollouts += 1;
        }
    }
    let journal_path = journal.finish()?;

    Ok(BucketPrepareSummary {
        checked_rollouts,
        prepared_rollouts,
        skipped_rollouts,
        elapsed: started.elapsed(),
        journal_path,
    })
}

fn prepare_rollout_first_line_padding(
    target: &RolloutTarget,
    journal: &mut RolloutChangeJournal,
    padding_bytes: usize,
) -> Result<RolloutPatchOutcome> {
    let first_line = read_first_line(&target.path)?;
    let Some(value) =
        parse_matching_session_meta(first_line.content.as_slice(), target.thread_id.as_str())?
    else {
        return Ok(RolloutPatchOutcome {
            skipped: true,
            ..RolloutPatchOutcome::default()
        });
    };
    let old_provider = session_meta_provider(&value).unwrap_or("").to_string();

    let rendered = serde_json::to_vec(&value)
        .with_context(|| format!("failed to render {}", target.path.display()))?;
    let desired_len = rendered.len() + padding_bytes;
    if first_line.content.len() >= desired_len {
        return Ok(RolloutPatchOutcome::default());
    }

    let mut replacement = rendered;
    replacement.resize(desired_len, b' ');
    record_rollout_change(
        Some(journal),
        target,
        old_provider.as_str(),
        old_provider.as_str(),
        first_line.content.len(),
        replacement.len(),
        RolloutChangeMode::RewriteWithPadding,
    )?;
    rewrite_first_line_atomically(
        &target.path,
        replacement.as_slice(),
        first_line.newline.as_slice(),
    )?;
    Ok(RolloutPatchOutcome {
        changed: true,
        prepared: true,
        ..RolloutPatchOutcome::default()
    })
}

fn read_first_line(path: &Path) -> Result<FirstLine> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::new();
    reader
        .read_until(b'\n', &mut buffer)
        .with_context(|| format!("failed to read {}", path.display()))?;
    if buffer.is_empty() {
        anyhow::bail!("empty rollout file: {}", path.display());
    }

    let mut newline = Vec::new();
    if buffer.ends_with(b"\n") {
        buffer.pop();
        if buffer.ends_with(b"\r") {
            buffer.pop();
            newline.extend_from_slice(b"\r\n");
        } else {
            newline.push(b'\n');
        }
    }

    Ok(FirstLine {
        content: buffer,
        newline,
    })
}

fn parse_matching_session_meta(line: &[u8], thread_id: &str) -> Result<Option<Value>> {
    let value = serde_json::from_slice::<Value>(line)?;
    if session_meta_belongs_to_thread(&value, thread_id) {
        Ok(Some(value))
    } else {
        Ok(None)
    }
}

fn patch_first_line_in_place(path: &Path, replacement: &[u8], newline: &[u8]) -> Result<()> {
    let original_times = capture_file_times(path)?;
    {
        let mut file = OpenOptions::new()
            .write(true)
            .open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        file.seek(SeekFrom::Start(0))
            .with_context(|| format!("failed to seek {}", path.display()))?;
        file.write_all(replacement)
            .with_context(|| format!("failed to write {}", path.display()))?;
        file.write_all(newline)
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    restore_file_times(path, original_times)?;
    Ok(())
}

fn rewrite_first_line_atomically(path: &Path, replacement: &[u8], newline: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("rollout.jsonl");
    let temp_path = parent.join(format!(
        ".{file_name}.{}.threadripper.tmp",
        std::process::id()
    ));
    let metadata =
        fs::metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    let original_times = file_times_from_metadata(&metadata);

    {
        let input =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let mut reader = BufReader::new(input);
        let mut ignored_first_line = Vec::new();
        reader
            .read_until(b'\n', &mut ignored_first_line)
            .with_context(|| format!("failed to read {}", path.display()))?;

        let output = File::create(&temp_path)
            .with_context(|| format!("failed to create {}", temp_path.display()))?;
        fs::set_permissions(&temp_path, metadata.permissions())
            .with_context(|| format!("failed to set permissions on {}", temp_path.display()))?;
        let mut writer = BufWriter::new(output);
        writer
            .write_all(replacement)
            .with_context(|| format!("failed to write {}", temp_path.display()))?;
        writer
            .write_all(newline)
            .with_context(|| format!("failed to write {}", temp_path.display()))?;
        std::io::copy(&mut reader, &mut writer)
            .with_context(|| format!("failed to copy {}", path.display()))?;
        writer
            .flush()
            .with_context(|| format!("failed to flush {}", temp_path.display()))?;
        writer
            .get_ref()
            .sync_all()
            .with_context(|| format!("fsync: {}", temp_path.display()))?;
    }

    fs::rename(&temp_path, path).with_context(|| {
        format!(
            "failed to replace {} with {}",
            path.display(),
            temp_path.display()
        )
    })?;
    restore_file_times(path, original_times)?;
    sync_dir(parent)?;
    Ok(())
}

fn capture_file_times(path: &Path) -> Result<(FileTime, FileTime)> {
    let metadata =
        fs::metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    Ok(file_times_from_metadata(&metadata))
}

fn file_times_from_metadata(metadata: &fs::Metadata) -> (FileTime, FileTime) {
    (
        FileTime::from_last_access_time(metadata),
        FileTime::from_last_modification_time(metadata),
    )
}

fn restore_file_times(path: &Path, times: (FileTime, FileTime)) -> Result<()> {
    set_file_times(path, times.0, times.1)
        .with_context(|| format!("failed to restore file times for {}", path.display()))
}

fn record_rollout_change(
    journal: Option<&mut RolloutChangeJournal>,
    target: &RolloutTarget,
    old_provider: &str,
    new_provider: &str,
    old_len: usize,
    new_len: usize,
    mode: RolloutChangeMode,
) -> Result<()> {
    if let Some(journal) = journal {
        journal.record(target, old_provider, new_provider, old_len, new_len, mode)?;
    }
    Ok(())
}

impl RolloutChangeJournal {
    fn new(path: PathBuf) -> Self {
        Self { path, writer: None }
    }

    fn record(
        &mut self,
        target: &RolloutTarget,
        old_provider: &str,
        new_provider: &str,
        old_len: usize,
        new_len: usize,
        mode: RolloutChangeMode,
    ) -> Result<()> {
        if self.writer.is_none() {
            if let Some(parent) = self.path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            let file = File::create(&self.path)
                .with_context(|| format!("failed to create {}", self.path.display()))?;
            self.writer = Some(BufWriter::new(file));
        }

        let writer = self.writer.as_mut().expect("journal writer initialized");
        serde_json::to_writer(
            &mut *writer,
            &json!({
                "path": target.path.display().to_string(),
                "thread_id": target.thread_id.as_str(),
                "mode": mode.as_str(),
                "old_provider": old_provider,
                "new_provider": new_provider,
                "old_first_line_len": old_len,
                "new_first_line_len": new_len,
            }),
        )
        .with_context(|| format!("failed to write {}", self.path.display()))?;
        writer
            .write_all(b"\n")
            .with_context(|| format!("failed to write {}", self.path.display()))?;
        Ok(())
    }

    fn finish(mut self) -> Result<Option<PathBuf>> {
        let Some(mut writer) = self.writer.take() else {
            return Ok(None);
        };
        writer
            .flush()
            .with_context(|| format!("failed to flush {}", self.path.display()))?;
        writer
            .get_ref()
            .sync_all()
            .with_context(|| format!("fsync: {}", self.path.display()))?;
        if let Some(parent) = self.path.parent() {
            sync_dir(parent)?;
        }
        Ok(Some(self.path))
    }
}

impl RolloutChangeMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::InPlace => "in_place",
            Self::RewriteWithPadding => "rewrite_with_padding",
        }
    }
}

fn session_meta_belongs_to_thread(value: &Value, thread_id: &str) -> bool {
    value.get("type").and_then(Value::as_str) == Some("session_meta")
        && value
            .get("payload")
            .and_then(|payload| payload.get("id"))
            .and_then(Value::as_str)
            == Some(thread_id)
}

fn session_meta_provider(value: &Value) -> Option<&str> {
    value
        .get("payload")
        .and_then(|payload| payload.get("model_provider"))
        .and_then(Value::as_str)
}

fn set_session_meta_provider(value: &mut Value, provider: &str) -> bool {
    let Some(payload) = value.get_mut("payload").and_then(Value::as_object_mut) else {
        return false;
    };
    if payload
        .get("model_provider")
        .and_then(Value::as_str)
        .is_some_and(|current| current == provider)
    {
        return false;
    }
    payload.insert(
        "model_provider".to_string(),
        Value::String(provider.to_string()),
    );
    true
}

fn inspect_sqlite_distribution(
    sqlite_path: &Path,
    target_provider: &str,
) -> Result<(u64, u64, ProviderDistribution)> {
    ensure_sqlite_exists(sqlite_path)?;
    let connection = Connection::open(sqlite_path)
        .with_context(|| format!("failed to open {}", sqlite_path.display()))?;
    let total_rows: u64 =
        connection.query_row("SELECT COUNT(*) FROM threads", [], |row| row.get(0))?;
    let mismatched_rows: u64 = connection.query_row(
        "SELECT COUNT(*) FROM threads WHERE model_provider <> ?1",
        [target_provider],
        |row| row.get(0),
    )?;
    let mut statement = connection.prepare(
        "SELECT model_provider, COUNT(*) AS row_count FROM threads GROUP BY model_provider ORDER BY row_count DESC, model_provider ASC",
    )?;
    let distribution = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok((total_rows, mismatched_rows, distribution))
}

fn reconcile_sqlite_in_place(sqlite_path: &Path, provider: &str) -> Result<(u64, u64)> {
    ensure_sqlite_exists(sqlite_path)?;
    let mut connection = Connection::open(sqlite_path)
        .with_context(|| format!("failed to open {}", sqlite_path.display()))?;
    connection.busy_timeout(Duration::from_secs(5))?;

    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let total_rows: u64 =
        transaction.query_row("SELECT COUNT(*) FROM threads", [], |row| row.get(0))?;
    let changed_rows = transaction.execute(
        "UPDATE threads SET model_provider = ?1 WHERE model_provider <> ?1",
        [provider],
    )? as u64;
    transaction.commit()?;

    Ok((changed_rows, total_rows))
}

#[cfg(test)]
fn reconcile_sqlite_with_backup(sqlite_path: &Path, provider: &str) -> Result<(u64, u64, PathBuf)> {
    let backup_path = create_sqlite_backup_file(sqlite_path)?;
    let (changed_rows, total_rows) = reconcile_sqlite_in_place(sqlite_path, provider)?;
    Ok((changed_rows, total_rows, backup_path))
}

fn create_sqlite_backup_file(sqlite_path: &Path) -> Result<PathBuf> {
    let backups_dir = sqlite_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("backups");
    fs::create_dir_all(&backups_dir)
        .with_context(|| format!("failed to create {}", backups_dir.display()))?;

    let timestamp = unix_timestamp_millis()?;
    let backup_name = format!("state_5.sqlite.{timestamp}.bak");
    let backup_path = backups_dir.join(&backup_name);
    let backup_temp_path = backups_dir.join(format!("{backup_name}.tmp"));

    if backup_temp_path.exists() {
        fs::remove_file(&backup_temp_path)
            .with_context(|| format!("failed to remove {}", backup_temp_path.display()))?;
    }

    create_sqlite_backup(sqlite_path, &backup_temp_path)?;
    fs::rename(&backup_temp_path, &backup_path)
        .with_context(|| format!("failed to finalize {}", backup_path.display()))?;
    sync_dir(&backups_dir)?;

    Ok(backup_path)
}

fn unix_timestamp_millis() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is earlier than UNIX_EPOCH")?
        .as_millis())
}

fn create_sqlite_backup(sqlite_path: &Path, backup_path: &Path) -> Result<()> {
    ensure_sqlite_exists(sqlite_path)?;
    let source = Connection::open(sqlite_path)
        .with_context(|| format!("failed to open {}", sqlite_path.display()))?;
    source.busy_timeout(Duration::from_millis(5_000))?;
    let mut destination = Connection::open(backup_path)
        .with_context(|| format!("failed to open {}", backup_path.display()))?;
    let backup = Backup::new(&source, &mut destination)?;
    let started = Instant::now();
    let timeout = Duration::from_secs(30);

    loop {
        if started.elapsed() >= timeout {
            anyhow::bail!(
                "sqlite backup timed out after {} seconds for {}",
                timeout.as_secs(),
                sqlite_path.display()
            );
        }

        match backup.step(100)? {
            StepResult::Done => break,
            StepResult::More => {}
            StepResult::Busy | StepResult::Locked => {
                std::thread::sleep(Duration::from_millis(50));
            }
            _ => {}
        }
    }

    drop(backup);
    drop(destination);
    sync_file(backup_path)?;

    Ok(())
}

fn ensure_sqlite_exists(sqlite_path: &Path) -> Result<()> {
    if sqlite_path.exists() {
        return Ok(());
    }

    anyhow::bail!(sqlite_missing_error(detect_locale(), sqlite_path));
}

fn sync_file(path: &Path) -> Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("open for sync: {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("fsync: {}", path.display()))?;
    Ok(())
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<()> {
    let dir = File::open(path).with_context(|| format!("open for sync: {}", path.display()))?;
    dir.sync_all()
        .with_context(|| format!("fsync: {}", path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> Result<()> {
    Ok(())
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
        println!("{}", uninstall_launchd_done(locale));
        println!("{}", launchd_plist_message(locale, &config_path));
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
            no_launchd_plist_message(locale, &service_status.config_path)
        );
    }
    Ok(())
}

fn print_status(locale: Locale, summary: &StatusSummary) {
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
        status_sqlite_file_label(locale),
        summary.sqlite_path.display()
    );
    println!(
        "{}: {}",
        status_target_provider_label(locale),
        summary.provider
    );
    println!(
        "{}: {}",
        status_total_threads_label(locale),
        summary.total_rows
    );
    println!(
        "{}: {}",
        status_rows_needing_reconcile_label(locale),
        summary.mismatched_rows
    );
    println!();
    println!("{}", status_distribution_heading(locale));
    for (provider, count) in &summary.distribution {
        println!("  {provider}: {count}");
    }
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

fn rollout_progress_message(
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

fn print_sync_summary(locale: Locale, title: &str, summary: &ReconcileSummary) {
    println!("{title}");
    println!(
        "{}: {}",
        status_target_provider_label(locale),
        summary.provider
    );
    println!(
        "{}: {}",
        sync_rows_updated_label(locale),
        summary.changed_rows
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
    println!(
        "{}: {}",
        status_total_threads_label(locale),
        summary.total_rows
    );
    println!(
        "{}: {} ms",
        sync_elapsed_label(locale),
        summary.elapsed.as_millis()
    );
    if let Some(backup_path) = &summary.backup_path {
        println!("{}: {}", sync_backup_label(locale), backup_path.display());
    }
    if let Some(journal_path) = &summary.rollout_journal_path {
        println!(
            "{}: {}",
            sync_rollout_journal_label(locale),
            journal_path.display()
        );
    }
}

fn print_bucket_prepare_summary(locale: Locale, summary: &BucketPrepareSummary) {
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

fn yes_no(locale: Locale, value: bool) -> &'static str {
    match (locale, value) {
        (Locale::En, true) => "yes",
        (Locale::En, false) => "no",
        (Locale::ZhHans, true) => "是",
        (Locale::ZhHans, false) => "否",
    }
}

fn root_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Keep CODEX_HOME/state_5.sqlite aligned with the current model_provider.",
        Locale::ZhHans => "让 CODEX_HOME/state_5.sqlite 持续对齐当前 model_provider。",
    }
}

fn root_long_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => {
            "codex-threadripper is a human-first maintenance tool for Codex thread history.\n\nIt reads the active provider from CODEX_HOME/config.toml and rewrites CODEX_HOME/state_5.sqlite so every thread stays in the same provider bucket. That makes thread lists and resume flows stop fragmenting across providers.\n\nExamples:\n  codex-threadripper status\n  codex-threadripper sync\n  codex-threadripper watch\n  codex-threadripper install-service"
        }
        Locale::ZhHans => {
            "codex-threadripper 是一个面向人的 Codex 线程历史维护工具。\n\n它会读取 CODEX_HOME/config.toml 里的当前 provider，并改写 CODEX_HOME/state_5.sqlite，让所有线程始终落在同一个 provider 桶里。这样线程列表和 resume 流程就不会再被 provider 切碎。\n\n示例：\n  codex-threadripper status\n  codex-threadripper sync\n  codex-threadripper watch\n  codex-threadripper install-service"
        }
    }
}

fn help_template(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "{before-help}{about-section}Usage: {usage}\n\n{all-args}{after-help}",
        Locale::ZhHans => "{before-help}{about-section}用法：{usage}\n\n{all-args}{after-help}",
    }
}

fn commands_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Commands",
        Locale::ZhHans => "命令",
    }
}

fn options_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Options",
        Locale::ZhHans => "选项",
    }
}

fn dir_value_name(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "DIR",
        Locale::ZhHans => "目录",
    }
}

fn provider_value_name(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "PROVIDER",
        Locale::ZhHans => "PROVIDER",
    }
}

fn milliseconds_value_name(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "MILLISECONDS",
        Locale::ZhHans => "毫秒",
    }
}

fn bytes_value_name(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "BYTES",
        Locale::ZhHans => "字节",
    }
}

fn codex_home_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Codex home directory. Defaults to $HOME/.codex.",
        Locale::ZhHans => "Codex home 目录。默认值是 $HOME/.codex。",
    }
}

fn provider_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Force a provider instead of reading model_provider from config.toml.",
        Locale::ZhHans => "强制指定 provider，跳过从 config.toml 读取 model_provider。",
    }
}

fn provider_empty_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "provider must contain at least one non-whitespace character",
        Locale::ZhHans => "provider 需要包含至少一个非空白字符",
    }
}

fn help_option_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Print help",
        Locale::ZhHans => "显示帮助信息",
    }
}

fn version_option_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Print version",
        Locale::ZhHans => "显示版本号",
    }
}

fn status_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => {
            "Show the current config provider, SQLite distribution, and background service state."
        }
        Locale::ZhHans => "显示当前 config provider、SQLite 分布和后台服务状态。",
    }
}

fn sync_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Reconcile state_5.sqlite and rollout metadata once right now.",
        Locale::ZhHans => "立刻收敛 state_5.sqlite 和 rollout 元数据。",
    }
}

fn bucket_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Manage the provider bucket label stored in rollout first lines.",
        Locale::ZhHans => "管理 rollout 首行里的 provider 可见桶标签。",
    }
}

fn bucket_prepare_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Reserve first-line padding so future provider bucket switches stay fast.",
        Locale::ZhHans => "给首行预留 padding，让后续 provider 桶切换保持快速。",
    }
}

fn bucket_switch_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Switch every thread into one provider bucket.",
        Locale::ZhHans => "把所有线程切到同一个 provider 可见桶。",
    }
}

fn bucket_switch_provider_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Target provider bucket. Defaults to --provider or config.toml.",
        Locale::ZhHans => "目标 provider 桶。默认读取 --provider 或 config.toml。",
    }
}

fn padding_bytes_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Extra JSON whitespace to reserve at the end of the rollout first line.",
        Locale::ZhHans => "在 rollout 首行末尾预留的额外 JSON 空白字节数。",
    }
}

fn watch_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => {
            "Keep watching config.toml and keep reconciling new rows in SQLite and rollout metadata."
        }
        Locale::ZhHans => "持续监听 config.toml，并持续收敛 SQLite 与 rollout 新增元数据。",
    }
}

fn poll_interval_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "How often to reconcile SQLite while watching.",
        Locale::ZhHans => "watch 模式下，定时收敛 SQLite 的频率。",
    }
}

fn sqlite_only_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Only update state_5.sqlite; leave rollout JSONL metadata untouched.",
        Locale::ZhHans => "只更新 state_5.sqlite，不改 rollout JSONL 元数据。",
    }
}

fn print_plist_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Print the platform-specific background service config for this tool.",
        Locale::ZhHans => "打印这个工具在当前平台上的后台服务配置。",
    }
}

fn install_launchd_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Install and start the platform-specific background service.",
        Locale::ZhHans => "安装并启动当前平台上的后台服务。",
    }
}

fn uninstall_launchd_about(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Stop and remove the platform-specific background service.",
        Locale::ZhHans => "停止并移除当前平台上的后台服务。",
    }
}

fn launchd_poll_help(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Polling interval for the background watcher.",
        Locale::ZhHans => "后台 watcher 的轮询间隔。",
    }
}

fn current_exe_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "failed to resolve current executable",
        Locale::ZhHans => "无法解析当前可执行文件路径",
    }
}

fn sync_complete_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Sync complete",
        Locale::ZhHans => "同步完成",
    }
}

fn bucket_switch_complete_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Bucket switch complete",
        Locale::ZhHans => "可见桶切换完成",
    }
}

fn bucket_prepare_complete_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Bucket prepare complete",
        Locale::ZhHans => "可见桶准备完成",
    }
}

fn watch_started_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Watch started",
        Locale::ZhHans => "已开始监听",
    }
}

fn config_change_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Config change detected",
        Locale::ZhHans => "检测到配置变更",
    }
}

fn background_reconcile_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Background reconcile",
        Locale::ZhHans => "后台收敛",
    }
}

fn watch_running_message(locale: Locale, codex_home: &Path, poll_interval: Duration) -> String {
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

fn watch_initial_reconcile_error_message(locale: Locale, err: &anyhow::Error) -> String {
    match locale {
        Locale::En => format!(
            "Initial reconcile hit an error. Watch will keep running and retry on config changes or the next poll: {err:#}"
        ),
        Locale::ZhHans => {
            format!("首轮收敛遇到错误。watch 会继续运行，并在配置变更或下一次轮询时重试：{err:#}")
        }
    }
}

fn watch_reconcile_skipped_message(locale: Locale, err: &anyhow::Error) -> String {
    match locale {
        Locale::En => format!("Reconcile is waiting for the next retry: {err:#}"),
        Locale::ZhHans => format!("本轮收敛等待下一次重试：{err:#}"),
    }
}

fn watcher_error_message(locale: Locale, err: notify::Error) -> String {
    match locale {
        Locale::En => format!("Watcher error: {err}"),
        Locale::ZhHans => format!("监听器错误：{err}"),
    }
}

fn watcher_disconnected_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "watcher disconnected",
        Locale::ZhHans => "监听器已断开",
    }
}

fn watch_already_running_error(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "another watch is already running for this codex home",
        Locale::ZhHans => "这个 codex home 已经有另一个 watch 在运行",
    }
}

fn watch_stopped_message(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Watch stopped.",
        Locale::ZhHans => "监听已停止。",
    }
}

fn install_launchd_done(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Installed background service.",
        Locale::ZhHans => "已安装后台服务。",
    }
}

fn uninstall_launchd_done(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Removed background service.",
        Locale::ZhHans => "已移除后台服务。",
    }
}

fn launchd_label_message(locale: Locale, label: &str) -> String {
    match locale {
        Locale::En => format!("Service label: {label}"),
        Locale::ZhHans => format!("服务标签：{label}"),
    }
}

fn launchd_plist_message(locale: Locale, path: &Path) -> String {
    match locale {
        Locale::En => format!("Config path: {}", path.display()),
        Locale::ZhHans => format!("配置路径：{}", path.display()),
    }
}

fn launchd_codex_home_message(locale: Locale, path: &Path) -> String {
    match locale {
        Locale::En => format!("Codex home: {}", path.display()),
        Locale::ZhHans => format!("Codex home：{}", path.display()),
    }
}

fn launchd_polling_message(locale: Locale, poll_interval: Duration) -> String {
    match locale {
        Locale::En => format!("Polling every {} ms.", poll_interval.as_millis()),
        Locale::ZhHans => format!("轮询间隔：{} ms。", poll_interval.as_millis()),
    }
}

fn next_steps_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Next steps",
        Locale::ZhHans => "下一步",
    }
}

fn print_install_service_summary(
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

fn install_next_steps(
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

fn cli_status_command(exe_path: impl AsRef<Path>, codex_home: &Path) -> String {
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

fn run_status_next_step(locale: Locale, command: &str) -> String {
    match locale {
        Locale::En => format!("Run this to verify status: {command}"),
        Locale::ZhHans => format!("运行这条命令查看状态：{command}"),
    }
}

fn inspect_service_next_step(locale: Locale, manager: ServiceManager, command: &str) -> String {
    let manager_name = service::manager_name(manager);
    match locale {
        Locale::En => format!("Run this to inspect the {manager_name} service: {command}"),
        Locale::ZhHans => format!("运行这条命令查看 {manager_name} 服务：{command}"),
    }
}

fn tail_log_command(manager: ServiceManager, log_path: &Path) -> String {
    match manager {
        ServiceManager::WindowsStartup => format!(
            "powershell -NoProfile -Command \"Get-Content -Path '{}' -Wait\"",
            log_path.display()
        ),
        _ => format!("tail -f {}", log_path.display()),
    }
}

fn tail_log_next_step(locale: Locale, command: &str) -> String {
    match locale {
        Locale::En => format!("Run this to watch the live log: {command}"),
        Locale::ZhHans => format!("运行这条命令查看实时日志：{command}"),
    }
}

fn no_launchd_plist_message(locale: Locale, path: &Path) -> String {
    match locale {
        Locale::En => format!(
            "No background service config is installed at {}.",
            path.display()
        ),
        Locale::ZhHans => format!("{} 这里还没有安装后台服务配置。", path.display()),
    }
}

fn sqlite_missing_error(locale: Locale, path: &Path) -> String {
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

fn status_title(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Codex Threadripper",
        Locale::ZhHans => "Codex Threadripper",
    }
}

fn status_codex_home_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Codex home",
        Locale::ZhHans => "Codex home",
    }
}

fn status_config_file_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Config file",
        Locale::ZhHans => "配置文件",
    }
}

fn status_sqlite_file_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "SQLite file",
        Locale::ZhHans => "SQLite 文件",
    }
}

fn status_target_provider_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Target provider",
        Locale::ZhHans => "目标 provider",
    }
}

fn status_total_threads_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Total threads",
        Locale::ZhHans => "线程总数",
    }
}

fn status_rows_needing_reconcile_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rows needing reconcile",
        Locale::ZhHans => "待收敛行数",
    }
}

fn status_distribution_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Provider distribution:",
        Locale::ZhHans => "Provider 分布：",
    }
}

fn status_background_service_heading(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Background service:",
        Locale::ZhHans => "后台服务：",
    }
}

fn status_service_manager_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Manager",
        Locale::ZhHans => "管理器",
    }
}

fn status_plist_path_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Config path",
        Locale::ZhHans => "配置路径",
    }
}

fn status_installed_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Installed",
        Locale::ZhHans => "已安装",
    }
}

fn status_loaded_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Running",
        Locale::ZhHans => "运行中",
    }
}

fn sync_rows_updated_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rows updated",
        Locale::ZhHans => "已更新行数",
    }
}

fn sync_rollouts_checked_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollouts checked",
        Locale::ZhHans => "已检查 rollout",
    }
}

fn sync_rollouts_updated_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollouts updated",
        Locale::ZhHans => "已更新 rollout",
    }
}

fn sync_rollouts_prepared_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollouts prepared",
        Locale::ZhHans => "已准备 rollout",
    }
}

fn sync_rollouts_skipped_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollouts skipped",
        Locale::ZhHans => "已跳过 rollout",
    }
}

fn sync_elapsed_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Elapsed",
        Locale::ZhHans => "耗时",
    }
}

fn sync_backup_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Backup",
        Locale::ZhHans => "备份文件",
    }
}

fn sync_rollout_journal_label(locale: Locale) -> &'static str {
    match locale {
        Locale::En => "Rollout first-line journal",
        Locale::ZhHans => "rollout 首行记录",
    }
}

fn service_log_message(locale: Locale, path: &Path) -> String {
    match locale {
        Locale::En => format!("Log path: {}", path.display()),
        Locale::ZhHans => format!("日志路径：{}", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use super::APP_VERSION;
    use super::Cli;
    use super::Command;
    use super::DEFAULT_BUCKET_PADDING_BYTES;
    use super::DEFAULT_POLL_INTERVAL_MS;
    use super::Locale;
    use super::RolloutScope;
    use super::detect_locale_from_sources;
    use super::inspect_sqlite_distribution;
    use super::install_next_steps;
    use super::localized_command;
    use super::parse_apple_languages;
    use super::parse_locale_tag;
    use super::read_provider_from_config;
    use super::reconcile_once;
    use super::reconcile_rollout_metadata_from_sqlite_with_progress;
    use super::reconcile_sqlite_in_place;
    use super::reconcile_sqlite_with_backup;
    use super::validate_provider_override;
    use super::validate_provider_override_args;
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

        let err = reconcile_once(dir.path(), Some("openai"), RolloutScope::None).unwrap_err();

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
        assert_eq!(steps.len(), 3);
        assert!(steps[0].contains("运行这条命令查看状态"));
        assert!(
            steps[0].contains("'/tmp/codex threadripper' --codex-home '/tmp/codex home' status")
        );
        assert!(steps[1].contains("launchctl print"));
        assert!(steps[2].contains("tail -f"));
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
