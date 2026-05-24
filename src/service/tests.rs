use super::build_linux_runner_script;
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

#[test]
fn linux_runner_script_preserves_profile_override() {
    let script = build_linux_runner_script(
        Path::new("/opt/codex threadripper/bin/codex-threadripper"),
        Path::new("/home/user/.codex"),
        None,
        Some("work"),
        Duration::from_millis(500),
    );

    assert!(script.contains("'/opt/codex threadripper/bin/codex-threadripper'"));
    assert!(script.contains("--profile work"));
    assert!(script.contains("watch --poll-interval-ms 500"));
}

#[test]
fn windows_startup_vbs_preserves_profile_override() {
    let script = build_windows_startup_vbs(
        Path::new(r"C:\Tools\codex-threadripper.exe"),
        Path::new(r"C:\Codex"),
        None,
        Some("work"),
        Duration::from_millis(500),
    );
    let command = shell_run_command(&script);

    assert!(command.contains("--profile work"));
    assert!(command.ends_with("watch --poll-interval-ms 500"));
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
