use anyhow::Context;
use anyhow::Result;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command as ProcessCommand;
use std::process::Stdio;
use std::time::Duration;

#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;

#[cfg(target_os = "linux")]
use super::executable_paths_match;
use super::log_path_for;
use super::runtime_dir;
use super::stderr_log_path_for;

#[cfg(target_os = "windows")]
const WINDOWS_CREATE_NO_WINDOW: u32 = 0x0800_0000;
#[cfg(target_os = "windows")]
const WINDOWS_DETACHED_PROCESS: u32 = 0x0000_0008;
#[cfg(target_os = "windows")]
const WINDOWS_CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

pub(super) fn start_detached_watch(
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

pub(super) fn stop_detached_watch_if_present(codex_home: &Path) -> Result<()> {
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

pub(super) fn detached_watch_running(codex_home: &Path) -> Result<bool> {
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
