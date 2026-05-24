use anyhow::Context;
use anyhow::Result;
use std::fmt::Write as _;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command as ProcessCommand;
use std::process::Stdio;
use std::time::Duration;

#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;

pub const SERVICE_LABEL: &str = "dev.wangnov.codex-threadripper";
#[cfg(target_os = "windows")]
const WINDOWS_CREATE_NO_WINDOW: u32 = 0x0800_0000;
#[cfg(target_os = "windows")]
const WINDOWS_DETACHED_PROCESS: u32 = 0x0000_0008;
#[cfg(target_os = "windows")]
const WINDOWS_CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceManager {
    Launchd,
    SystemdUser,
    WindowsStartup,
}

#[derive(Debug)]
pub struct ServiceStatus {
    pub manager: ServiceManager,
    pub config_path: PathBuf,
    pub installed: bool,
    pub running: bool,
}

#[derive(Debug)]
pub struct ServiceInstallSummary {
    pub manager: ServiceManager,
    pub config_path: PathBuf,
    pub log_path: PathBuf,
}

pub fn current_manager() -> ServiceManager {
    #[cfg(target_os = "macos")]
    {
        ServiceManager::Launchd
    }
    #[cfg(target_os = "linux")]
    {
        ServiceManager::SystemdUser
    }
    #[cfg(target_os = "windows")]
    {
        ServiceManager::WindowsStartup
    }
}

pub fn current_service_status() -> Result<ServiceStatus> {
    let manager = current_manager();
    let codex_home = current_codex_home()?;
    let config_path = service_status_config_path(manager)?;
    let installed = config_path.exists();
    let running = service_running(manager, &codex_home)?;
    Ok(ServiceStatus {
        manager,
        config_path,
        installed,
        running,
    })
}

pub fn install_service(
    exe_path: &Path,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    poll_interval: Duration,
) -> Result<ServiceInstallSummary> {
    let manager = current_manager();
    stop_detached_watch_if_present(codex_home)?;

    match manager {
        ServiceManager::Launchd => install_launchd(
            exe_path,
            codex_home,
            provider_override,
            profile_override,
            poll_interval,
        ),
        ServiceManager::SystemdUser => install_systemd_user(
            exe_path,
            codex_home,
            provider_override,
            profile_override,
            poll_interval,
        ),
        ServiceManager::WindowsStartup => install_windows_startup(
            exe_path,
            codex_home,
            provider_override,
            profile_override,
            poll_interval,
        ),
    }
}

pub fn uninstall_service() -> Result<PathBuf> {
    let manager = current_manager();
    let codex_home = current_codex_home()?;
    stop_detached_watch_if_present(&codex_home)?;

    let config_path = service_config_path(manager)?;
    match manager {
        ServiceManager::Launchd => uninstall_launchd(&config_path)?,
        ServiceManager::SystemdUser => uninstall_systemd_user(&config_path)?,
        ServiceManager::WindowsStartup => uninstall_windows_startup(&config_path)?,
    }
    Ok(config_path)
}

pub fn render_service_config(
    exe_path: &Path,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    poll_interval: Duration,
) -> Result<String> {
    let manager = current_manager();
    match manager {
        ServiceManager::Launchd => Ok(build_launchd_plist(
            exe_path,
            codex_home,
            provider_override,
            profile_override,
            poll_interval,
        )),
        ServiceManager::SystemdUser => build_systemd_bundle(
            exe_path,
            codex_home,
            provider_override,
            profile_override,
            poll_interval,
        ),
        ServiceManager::WindowsStartup => Ok(build_windows_startup_vbs(
            exe_path,
            codex_home,
            provider_override,
            profile_override,
            poll_interval,
        )),
    }
}

pub fn manager_name(manager: ServiceManager) -> &'static str {
    match manager {
        ServiceManager::Launchd => "launchd",
        ServiceManager::SystemdUser => "systemd user",
        ServiceManager::WindowsStartup => "Windows Startup",
    }
}

pub fn log_path() -> Result<PathBuf> {
    let codex_home = current_codex_home()?;
    log_path_for(&codex_home)
}

fn log_path_for(codex_home: &Path) -> Result<PathBuf> {
    Ok(logs_dir(codex_home)?.join("codex-threadripper.log"))
}

pub fn current_service_inspect_command() -> Result<Option<String>> {
    let manager = current_manager();
    match manager {
        ServiceManager::Launchd => Ok(Some(format!(
            "launchctl print {}",
            launchctl_service_target()?
        ))),
        ServiceManager::SystemdUser => Ok(Some(format!(
            "systemctl --user status {}.service",
            SERVICE_LABEL
        ))),
        ServiceManager::WindowsStartup => Ok(Some(
            "powershell -NoProfile -Command \"Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*codex-threadripper*watch*' } | Select-Object ProcessId, CommandLine\""
                .to_string(),
        )),
    }
}

fn install_launchd(
    exe_path: &Path,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    poll_interval: Duration,
) -> Result<ServiceInstallSummary> {
    let plist_path = service_config_path(ServiceManager::Launchd)?;
    let launch_agents_dir = plist_path
        .parent()
        .with_context(|| format!("launchd plist path has no parent: {}", plist_path.display()))?;
    std::fs::create_dir_all(launch_agents_dir)?;

    let plist = build_launchd_plist(
        exe_path,
        codex_home,
        provider_override,
        profile_override,
        poll_interval,
    );
    std::fs::write(&plist_path, plist)
        .with_context(|| format!("failed to write {}", plist_path.display()))?;

    let domain = launchctl_domain()?;
    let plist_path_str = plist_path.to_string_lossy().to_string();
    let _ = run_command_capture(
        "launchctl",
        ["bootout", domain.as_str(), plist_path_str.as_str()],
    );
    run_command_ok(
        "launchctl",
        ["bootstrap", domain.as_str(), plist_path_str.as_str()],
    )?;
    let service_target = launchctl_service_target()?;
    run_command_ok("launchctl", ["kickstart", "-k", service_target.as_str()])?;

    Ok(ServiceInstallSummary {
        manager: ServiceManager::Launchd,
        config_path: plist_path,
        log_path: log_path_for(codex_home)?,
    })
}

fn uninstall_launchd(config_path: &Path) -> Result<()> {
    if config_path.exists() {
        let domain = launchctl_domain()?;
        let config_path_str = config_path.to_string_lossy().to_string();
        let _ = run_command_capture(
            "launchctl",
            ["bootout", domain.as_str(), config_path_str.as_str()],
        );
        std::fs::remove_file(config_path)
            .with_context(|| format!("failed to remove {}", config_path.display()))?;
    }
    Ok(())
}

fn install_systemd_user(
    exe_path: &Path,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    poll_interval: Duration,
) -> Result<ServiceInstallSummary> {
    let config_path = service_config_path(ServiceManager::SystemdUser)?;
    let config_dir = config_path
        .parent()
        .with_context(|| format!("systemd unit path has no parent: {}", config_path.display()))?;
    std::fs::create_dir_all(config_dir)?;

    let runner_script_path = linux_runner_script_path()?;
    let runner_script_dir = runner_script_path.parent().with_context(|| {
        format!(
            "runner script path has no parent: {}",
            runner_script_path.display()
        )
    })?;
    std::fs::create_dir_all(runner_script_dir)?;

    let script = build_linux_runner_script(
        exe_path,
        codex_home,
        provider_override,
        profile_override,
        poll_interval,
    );
    std::fs::write(&runner_script_path, script)
        .with_context(|| format!("failed to write {}", runner_script_path.display()))?;
    make_executable(&runner_script_path)?;

    let unit = build_systemd_unit(&runner_script_path);
    std::fs::write(&config_path, unit)
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let systemd_started = try_run_systemd_user_unit()?;
    if !systemd_started {
        start_detached_watch(
            exe_path,
            codex_home,
            provider_override,
            profile_override,
            poll_interval,
        )?;
    }

    Ok(ServiceInstallSummary {
        manager: ServiceManager::SystemdUser,
        config_path,
        log_path: log_path_for(codex_home)?,
    })
}

fn uninstall_systemd_user(config_path: &Path) -> Result<()> {
    let _ = run_command_capture(
        "systemctl",
        [
            "--user",
            "disable",
            "--now",
            &format!("{SERVICE_LABEL}.service"),
        ],
    );
    let _ = run_command_capture("systemctl", ["--user", "daemon-reload"]);
    if config_path.exists() {
        std::fs::remove_file(config_path)
            .with_context(|| format!("failed to remove {}", config_path.display()))?;
    }
    let runner_script = linux_runner_script_path()?;
    if runner_script.exists() {
        std::fs::remove_file(&runner_script)
            .with_context(|| format!("failed to remove {}", runner_script.display()))?;
    }
    Ok(())
}

fn install_windows_startup(
    exe_path: &Path,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    poll_interval: Duration,
) -> Result<ServiceInstallSummary> {
    let config_path = service_config_path(ServiceManager::WindowsStartup)?;
    let config_dir = config_path.parent().with_context(|| {
        format!(
            "startup script path has no parent: {}",
            config_path.display()
        )
    })?;
    std::fs::create_dir_all(config_dir)?;

    let script = build_windows_startup_vbs(
        exe_path,
        codex_home,
        provider_override,
        profile_override,
        poll_interval,
    );
    std::fs::write(&config_path, script)
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let legacy_cmd_path = legacy_windows_startup_cmd_path()?;
    if legacy_cmd_path.exists() {
        std::fs::remove_file(&legacy_cmd_path)
            .with_context(|| format!("failed to remove {}", legacy_cmd_path.display()))?;
    }

    start_detached_watch(
        exe_path,
        codex_home,
        provider_override,
        profile_override,
        poll_interval,
    )?;

    Ok(ServiceInstallSummary {
        manager: ServiceManager::WindowsStartup,
        config_path,
        log_path: log_path_for(codex_home)?,
    })
}

fn uninstall_windows_startup(config_path: &Path) -> Result<()> {
    if config_path.exists() {
        std::fs::remove_file(config_path)
            .with_context(|| format!("failed to remove {}", config_path.display()))?;
    }

    let legacy_cmd_path = legacy_windows_startup_cmd_path()?;
    if legacy_cmd_path.exists() {
        std::fs::remove_file(&legacy_cmd_path)
            .with_context(|| format!("failed to remove {}", legacy_cmd_path.display()))?;
    }

    Ok(())
}

fn service_running(manager: ServiceManager, codex_home: &Path) -> Result<bool> {
    match manager {
        ServiceManager::Launchd => launchd_service_loaded(),
        ServiceManager::SystemdUser => {
            if systemd_user_unit_active()? {
                return Ok(true);
            }
            detached_watch_running(codex_home)
        }
        ServiceManager::WindowsStartup => detached_watch_running(codex_home),
    }
}

fn service_config_path(manager: ServiceManager) -> Result<PathBuf> {
    match manager {
        ServiceManager::Launchd => Ok(home_dir()?
            .join("Library/LaunchAgents")
            .join(format!("{SERVICE_LABEL}.plist"))),
        ServiceManager::SystemdUser => Ok(home_dir()?
            .join(".config/systemd/user")
            .join(format!("{SERVICE_LABEL}.service"))),
        ServiceManager::WindowsStartup => {
            Ok(windows_startup_dir()?.join(format!("{SERVICE_LABEL}.vbs")))
        }
    }
}

fn service_status_config_path(manager: ServiceManager) -> Result<PathBuf> {
    let config_path = service_config_path(manager)?;
    if manager != ServiceManager::WindowsStartup {
        return Ok(config_path);
    }

    let legacy_cmd_path = legacy_windows_startup_cmd_path()?;
    Ok(installed_windows_startup_config_path(
        &config_path,
        &legacy_cmd_path,
    ))
}

fn installed_windows_startup_config_path(config_path: &Path, legacy_cmd_path: &Path) -> PathBuf {
    if config_path.exists() {
        return config_path.to_path_buf();
    }
    if legacy_cmd_path.exists() {
        return legacy_cmd_path.to_path_buf();
    }
    config_path.to_path_buf()
}

fn legacy_windows_startup_cmd_path() -> Result<PathBuf> {
    Ok(windows_startup_dir()?.join(format!("{SERVICE_LABEL}.cmd")))
}

fn linux_runner_script_path() -> Result<PathBuf> {
    Ok(home_dir()?.join(".local/share/codex-threadripper/run-watch.sh"))
}

fn build_systemd_bundle(
    exe_path: &Path,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    poll_interval: Duration,
) -> Result<String> {
    let runner_script_path = linux_runner_script_path()?;
    let script = build_linux_runner_script(
        exe_path,
        codex_home,
        provider_override,
        profile_override,
        poll_interval,
    );
    let unit = build_systemd_unit(&runner_script_path);
    Ok(format!(
        "# {}\n{}\n# {}\n{}",
        runner_script_path.display(),
        script,
        service_config_path(ServiceManager::SystemdUser)?.display(),
        unit
    ))
}

fn build_systemd_unit(runner_script_path: &Path) -> String {
    format!(
        "[Unit]\nDescription=codex-threadripper background watcher\n\n[Service]\nType=simple\nExecStart={}\nRestart=always\nRestartSec=1\n\n[Install]\nWantedBy=default.target\n",
        shell_quote(runner_script_path.display().to_string())
    )
}

fn build_linux_runner_script(
    exe_path: &Path,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    poll_interval: Duration,
) -> String {
    format!(
        "#!/bin/sh\nexec {}\n",
        watch_command_line(
            exe_path,
            codex_home,
            provider_override,
            profile_override,
            poll_interval,
            ShellFlavor::Sh
        )
    )
}

fn build_windows_startup_vbs(
    exe_path: &Path,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    poll_interval: Duration,
) -> String {
    let command = watch_command_line(
        exe_path,
        codex_home,
        provider_override,
        profile_override,
        poll_interval,
        ShellFlavor::WindowsProcess,
    );

    format!(
        "Set shell = CreateObject(\"WScript.Shell\")\r\nshell.Run {}, 0, False\r\n",
        vbs_quote(&command)
    )
}

pub fn build_launchd_plist(
    exe_path: &Path,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    poll_interval: Duration,
) -> String {
    let stdout_path =
        log_path_for(codex_home).unwrap_or_else(|_| PathBuf::from("/tmp/codex-threadripper.log"));
    let stderr_path = stderr_log_path_for(codex_home)
        .unwrap_or_else(|_| PathBuf::from("/tmp/codex-threadripper.error.log"));

    let mut arguments = vec![
        xml_escape(exe_path.to_string_lossy().as_ref()),
        "--codex-home".to_string(),
        xml_escape(codex_home.to_string_lossy().as_ref()),
    ];
    if let Some(provider) = provider_override {
        arguments.push("--provider".to_string());
        arguments.push(xml_escape(provider));
    }
    if let Some(profile) = profile_override {
        arguments.push("--profile".to_string());
        arguments.push(xml_escape(profile));
    }
    arguments.push("watch".to_string());
    arguments.push("--poll-interval-ms".to_string());
    arguments.push(poll_interval.as_millis().to_string());

    let mut plist = String::new();
    let _ = write!(
        plist,
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{SERVICE_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
"#
    );
    for argument in arguments {
        let _ = writeln!(plist, "    <string>{argument}</string>");
    }
    let _ = write!(
        plist,
        r#"  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>{}</string>
  <key>StandardErrorPath</key>
  <string>{}</string>
</dict>
</plist>
"#,
        xml_escape(stdout_path.to_string_lossy().as_ref()),
        xml_escape(stderr_path.to_string_lossy().as_ref()),
    );
    plist
}

fn watch_command_line(
    exe_path: &Path,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    poll_interval: Duration,
    flavor: ShellFlavor,
) -> String {
    let quote = |value: String| match flavor {
        ShellFlavor::Sh => shell_quote(value),
        ShellFlavor::WindowsProcess => windows_process_quote(&value),
    };

    let mut parts = vec![
        quote(exe_path.display().to_string()),
        "--codex-home".to_string(),
        quote(codex_home.display().to_string()),
    ];
    if let Some(provider) = provider_override {
        parts.push("--provider".to_string());
        parts.push(quote(provider.to_string()));
    }
    if let Some(profile) = profile_override {
        parts.push("--profile".to_string());
        parts.push(quote(profile.to_string()));
    }
    parts.push("watch".to_string());
    parts.push("--poll-interval-ms".to_string());
    parts.push(poll_interval.as_millis().to_string());
    parts.join(" ")
}

#[derive(Clone, Copy)]
enum ShellFlavor {
    Sh,
    WindowsProcess,
}

fn start_detached_watch(
    exe_path: &Path,
    codex_home: &Path,
    provider_override: Option<&str>,
    profile_override: Option<&str>,
    poll_interval: Duration,
) -> Result<()> {
    let stdout_path = log_path_for(codex_home)?;
    let stderr_path = stderr_log_path_for(codex_home)?;
    if let Some(parent) = stdout_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stdout_path)
        .with_context(|| format!("failed to open {}", stdout_path.display()))?;
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stderr_path)
        .with_context(|| format!("failed to open {}", stderr_path.display()))?;

    let mut command = ProcessCommand::new(exe_path);
    command.arg("--codex-home").arg(codex_home);

    if let Some(provider) = provider_override {
        command.arg("--provider").arg(provider);
    }
    if let Some(profile) = profile_override {
        command.arg("--profile").arg(profile);
    }

    command
        .arg("watch")
        .arg("--poll-interval-ms")
        .arg(poll_interval.as_millis().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));

    #[cfg(target_os = "windows")]
    {
        command.creation_flags(
            WINDOWS_CREATE_NO_WINDOW | WINDOWS_DETACHED_PROCESS | WINDOWS_CREATE_NEW_PROCESS_GROUP,
        );
    }

    let child = command
        .spawn()
        .context("failed to start detached watch process")?;
    write_pid_file(child.id(), codex_home)?;
    Ok(())
}

fn stop_detached_watch_if_present(codex_home: &Path) -> Result<()> {
    let Some(pid_file) = read_pid_file(codex_home)? else {
        return Ok(());
    };
    match process_is_running(pid_file.pid, pid_file.exe_path.as_deref())? {
        ProcessStatus::Running => {
            stop_process(pid_file.pid)?;
            remove_pid_file_if_exists(codex_home)?;
        }
        ProcessStatus::NotRunning => {
            remove_pid_file_if_exists(codex_home)?;
        }
        ProcessStatus::RunningMismatched => {}
    }
    Ok(())
}

fn detached_watch_running(codex_home: &Path) -> Result<bool> {
    let Some(pid_file) = read_pid_file(codex_home)? else {
        return Ok(false);
    };
    match process_is_running(pid_file.pid, pid_file.exe_path.as_deref())? {
        ProcessStatus::Running => Ok(true),
        ProcessStatus::NotRunning => {
            remove_pid_file_if_exists(codex_home)?;
            Ok(false)
        }
        ProcessStatus::RunningMismatched => Ok(false),
    }
}

fn pid_file_path(codex_home: &Path) -> Result<PathBuf> {
    Ok(runtime_dir(codex_home)?.join("watch.pid"))
}

fn write_pid_file(pid: u32, codex_home: &Path) -> Result<()> {
    let pid_path = pid_file_path(codex_home)?;
    if let Some(parent) = pid_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let exe_path = std::env::current_exe()
        .context("failed to read current executable for pid file")?
        .to_string_lossy()
        .into_owned();
    std::fs::write(&pid_path, format!("{pid}\n{exe_path}\n"))
        .with_context(|| format!("failed to write {}", pid_path.display()))?;
    Ok(())
}

fn read_pid_file(codex_home: &Path) -> Result<Option<PidFile>> {
    let pid_path = pid_file_path(codex_home)?;
    if !pid_path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&pid_path)
        .with_context(|| format!("failed to read {}", pid_path.display()))?;
    let mut lines = raw.lines();
    let pid = lines
        .next()
        .context("pid file is empty")?
        .trim()
        .parse::<u32>()
        .context("failed to parse pid file")?;
    let exe_path = lines
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(PathBuf::from);
    Ok(Some(PidFile { pid, exe_path }))
}

fn remove_pid_file_if_exists(codex_home: &Path) -> Result<()> {
    let pid_path = pid_file_path(codex_home)?;
    if pid_path.exists() {
        std::fs::remove_file(&pid_path)
            .with_context(|| format!("failed to remove {}", pid_path.display()))?;
    }
    Ok(())
}

fn process_is_running(pid: u32, expected_exe: Option<&Path>) -> Result<ProcessStatus> {
    #[cfg(target_os = "windows")]
    {
        let filter = format!("PID eq {pid}");
        let output = ProcessCommand::new("tasklist")
            .args(["/FI", filter.as_str(), "/FO", "CSV", "/NH"])
            .output()?;
        if !output.status.success() {
            return Ok(ProcessStatus::NotRunning);
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let trimmed = stdout.trim();
        if trimmed.is_empty() || trimmed.starts_with("INFO:") {
            return Ok(ProcessStatus::NotRunning);
        }
        if trimmed.to_ascii_lowercase().contains("codex-threadripper") {
            return Ok(ProcessStatus::Running);
        }
        let _ = expected_exe;
        Ok(ProcessStatus::RunningMismatched)
    }
    #[cfg(target_os = "macos")]
    {
        let status = ProcessCommand::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .status()?;
        if !status.success() {
            return Ok(ProcessStatus::NotRunning);
        }
        let output = ProcessCommand::new("ps")
            .args(["-p", &pid.to_string(), "-o", "comm="])
            .output()?;
        if !output.status.success() {
            return Ok(ProcessStatus::NotRunning);
        }
        if String::from_utf8_lossy(&output.stdout)
            .to_ascii_lowercase()
            .contains("codex-threadripper")
        {
            return Ok(ProcessStatus::Running);
        }
        let _ = expected_exe;
        Ok(ProcessStatus::RunningMismatched)
    }
    #[cfg(target_os = "linux")]
    {
        let status = ProcessCommand::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .status()?;
        if !status.success() {
            return Ok(ProcessStatus::NotRunning);
        }
        let Some(expected_exe) = expected_exe else {
            return Ok(ProcessStatus::Running);
        };
        let actual_exe = match std::fs::read_link(format!("/proc/{pid}/exe")) {
            Ok(path) => path,
            Err(_) => return Ok(ProcessStatus::NotRunning),
        };
        if executable_paths_match(&actual_exe, expected_exe) {
            return Ok(ProcessStatus::Running);
        }
        Ok(ProcessStatus::RunningMismatched)
    }
}

fn stop_process(pid: u32) -> Result<()> {
    #[cfg(target_os = "windows")]
    {
        let output = ProcessCommand::new("taskkill")
            .args(["/PID", &pid.to_string(), "/F", "/T"])
            .output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            if !stderr.contains("not found") && !stdout.contains("not found") {
                anyhow::bail!(
                    "failed to stop pid {pid}\nstdout: {}\nstderr: {}",
                    stdout.trim(),
                    stderr.trim()
                );
            }
        }
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = ProcessCommand::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()?;
    }
    Ok(())
}

fn try_run_systemd_user_unit() -> Result<bool> {
    let daemon_reload = run_command_capture("systemctl", ["--user", "daemon-reload"])?;
    if !daemon_reload.status.success() {
        return Ok(false);
    }
    let enable_now = run_command_capture(
        "systemctl",
        [
            "--user",
            "enable",
            "--now",
            &format!("{SERVICE_LABEL}.service"),
        ],
    )?;
    Ok(enable_now.status.success())
}

fn systemd_user_unit_active() -> Result<bool> {
    let output = run_command_capture(
        "systemctl",
        [
            "--user",
            "is-active",
            "--quiet",
            &format!("{SERVICE_LABEL}.service"),
        ],
    )?;
    Ok(output.status.success())
}

fn stderr_log_path_for(codex_home: &Path) -> Result<PathBuf> {
    Ok(log_path_for(codex_home)?
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("codex-threadripper.error.log"))
}

fn runtime_dir(codex_home: &Path) -> Result<PathBuf> {
    let tag = codex_home_tag(codex_home);
    #[cfg(target_os = "macos")]
    {
        Ok(home_dir()?
            .join("Library/Application Support/codex-threadripper")
            .join(tag))
    }
    #[cfg(target_os = "linux")]
    {
        Ok(home_dir()?
            .join(".local/state/codex-threadripper")
            .join(tag))
    }
    #[cfg(target_os = "windows")]
    {
        Ok(windows_local_app_data_dir()?
            .join("codex-threadripper")
            .join(tag))
    }
}

fn logs_dir(codex_home: &Path) -> Result<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let tag = codex_home_tag(codex_home);
        Ok(home_dir()?
            .join("Library/Logs/codex-threadripper")
            .join(tag))
    }
    #[cfg(target_os = "linux")]
    {
        Ok(runtime_dir(codex_home)?.join("logs"))
    }
    #[cfg(target_os = "windows")]
    {
        Ok(runtime_dir(codex_home)?.join("logs"))
    }
}

fn current_codex_home() -> Result<PathBuf> {
    let args = std::env::args_os().collect::<Vec<_>>();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == std::ffi::OsStr::new("--codex-home") {
            if let Some(path) = iter.next() {
                return Ok(PathBuf::from(path));
            }
            anyhow::bail!("--codex-home requires a path");
        }
        let arg_str = arg.to_string_lossy();
        if let Some(path) = arg_str.strip_prefix("--codex-home=") {
            return Ok(PathBuf::from(path));
        }
    }
    if let Some(path) = std::env::var_os("CODEX_HOME") {
        return Ok(PathBuf::from(path));
    }
    Ok(default_codex_home())
}

fn codex_home_tag(codex_home: &Path) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    codex_home.display().to_string().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn default_codex_home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".codex")
}

#[cfg(target_os = "linux")]
fn executable_paths_match(actual: &Path, expected: &Path) -> bool {
    if actual == expected {
        return true;
    }
    match std::fs::canonicalize(expected) {
        Ok(expected_canonical) => actual == expected_canonical,
        Err(_) => false,
    }
}

fn home_dir() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = std::env::var_os("USERPROFILE") {
        return Ok(PathBuf::from(path));
    }
    let home_drive = std::env::var_os("HOMEDRIVE");
    let home_path = std::env::var_os("HOMEPATH");
    match (home_drive, home_path) {
        (Some(drive), Some(path)) => {
            let mut joined = PathBuf::from(drive);
            joined.push(path);
            Ok(joined)
        }
        _ => anyhow::bail!("HOME is not set"),
    }
}

fn windows_appdata_dir() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("APPDATA") {
        return Ok(PathBuf::from(path));
    }
    Ok(home_dir()?.join("AppData/Roaming"))
}

#[cfg(target_os = "windows")]
fn windows_local_app_data_dir() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("LOCALAPPDATA") {
        return Ok(PathBuf::from(path));
    }
    Ok(home_dir()?.join("AppData/Local"))
}

fn windows_startup_dir() -> Result<PathBuf> {
    Ok(windows_appdata_dir()?.join("Microsoft/Windows/Start Menu/Programs/Startup"))
}

fn launchctl_domain() -> Result<String> {
    let uid = current_uid()?;
    Ok(format!("gui/{uid}"))
}

fn launchctl_service_target() -> Result<String> {
    Ok(format!("{}/{}", launchctl_domain()?, SERVICE_LABEL))
}

#[cfg(unix)]
fn current_uid() -> Result<u32> {
    // SAFETY: geteuid reads the effective uid for the current process.
    Ok(unsafe { libc::geteuid() })
}

#[cfg(not(unix))]
fn current_uid() -> Result<u32> {
    anyhow::bail!("current uid is unavailable on this platform")
}

fn launchd_service_loaded() -> Result<bool> {
    let service_target = launchctl_service_target()?;
    let output = ProcessCommand::new("launchctl")
        .arg("print")
        .arg(service_target)
        .output()?;
    Ok(output.status.success())
}

fn run_command_ok<I, S>(program: &str, args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let output = run_command_capture(program, args)?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    anyhow::bail!(
        "{} failed\nstdout: {}\nstderr: {}",
        program,
        stdout.trim(),
        stderr.trim()
    );
}

fn run_command_capture<I, S>(program: &str, args: I) -> Result<std::process::Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args_vec = args
        .into_iter()
        .map(|value| value.as_ref().to_string())
        .collect::<Vec<_>>();
    let output = ProcessCommand::new(program).args(&args_vec).output()?;
    Ok(output)
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

fn windows_process_quote(input: &str) -> String {
    if !input.is_empty() && input.chars().all(|ch| !ch.is_whitespace() && ch != '"') {
        return input.to_string();
    }

    let mut quoted = String::from("\"");
    let mut pending_backslashes = 0;
    for ch in input.chars() {
        match ch {
            '\\' => pending_backslashes += 1,
            '"' => {
                for _ in 0..(pending_backslashes * 2 + 1) {
                    quoted.push('\\');
                }
                quoted.push('"');
                pending_backslashes = 0;
            }
            _ => {
                for _ in 0..pending_backslashes {
                    quoted.push('\\');
                }
                quoted.push(ch);
                pending_backslashes = 0;
            }
        }
    }
    for _ in 0..(pending_backslashes * 2) {
        quoted.push('\\');
    }
    quoted.push('"');
    quoted
}

fn vbs_quote(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
}

fn xml_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = std::fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) -> Result<()> {
    Ok(())
}

#[derive(Debug)]
struct PidFile {
    pid: u32,
    exe_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProcessStatus {
    Running,
    RunningMismatched,
    NotRunning,
}

#[cfg(test)]
mod tests {
    use super::build_windows_startup_vbs;
    use super::installed_windows_startup_config_path;
    use anyhow::Result;
    use std::fs;
    use std::path::Path;
    use std::time::Duration;

    #[test]
    fn windows_status_config_path_uses_legacy_cmd_when_vbs_is_missing() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let vbs_path = dir.path().join("dev.wangnov.codex-threadripper.vbs");
        let cmd_path = dir.path().join("dev.wangnov.codex-threadripper.cmd");
        fs::write(&cmd_path, "@echo off\r\n")?;

        assert_eq!(
            installed_windows_startup_config_path(&vbs_path, &cmd_path),
            cmd_path
        );

        Ok(())
    }

    #[test]
    fn windows_startup_vbs_quotes_space_paths_without_cmd_wrapper() {
        let script = build_windows_startup_vbs(
            Path::new(r"C:\Program Files\codex threadripper\codex-threadripper.exe"),
            Path::new(r"C:\Users\Admin User\.codex"),
            None,
            None,
            Duration::from_millis(1500),
        );
        let command = shell_run_command(&script);

        assert!(script.contains(r#"CreateObject("WScript.Shell")"#));
        assert!(script.contains("shell.Run"));
        assert!(script.contains(", 0, False"));
        assert!(!script.contains("cmd.exe /c"));
        assert_eq!(
            command,
            concat!(
                r#""C:\Program Files\codex threadripper\codex-threadripper.exe" "#,
                r#"--codex-home "C:\Users\Admin User\.codex" "#,
                "watch --poll-interval-ms 1500"
            )
        );
    }

    #[test]
    fn windows_startup_vbs_escapes_provider_quotes_and_spaces() {
        let script = build_windows_startup_vbs(
            Path::new(r"C:\Tools\codex-threadripper.exe"),
            Path::new(r"C:\Codex"),
            Some(r#"open ai "beta""#),
            None,
            Duration::from_millis(500),
        );
        let command = shell_run_command(&script);

        assert!(!script.contains("cmd.exe /c"));
        assert_eq!(
            command,
            concat!(
                r"C:\Tools\codex-threadripper.exe --codex-home C:\Codex ",
                r#"--provider "open ai \"beta\"" watch --poll-interval-ms 500"#
            )
        );
    }

    fn shell_run_command(script: &str) -> String {
        let line = script
            .lines()
            .find(|line| line.starts_with("shell.Run "))
            .expect("script should contain shell.Run");
        let literal = line
            .strip_prefix("shell.Run ")
            .and_then(|value| value.strip_suffix(", 0, False"))
            .expect("shell.Run should pass a quoted command literal");
        assert!(literal.starts_with('"') && literal.ends_with('"'));
        literal[1..literal.len() - 1].replace("\"\"", "\"")
    }
}
