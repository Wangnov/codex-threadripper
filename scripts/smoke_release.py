#!/usr/bin/env python3

import argparse
import os
import pathlib
import shutil
import signal
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
import zipfile


SCHEMA_SQL = """
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
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive")
    parser.add_argument("--binary")
    parser.add_argument("--platform", required=True, choices=["linux", "macos", "windows"])
    args = parser.parse_args()

    if bool(args.archive) == bool(args.binary):
        raise SystemExit("pass exactly one of --archive or --binary")

    workspace = pathlib.Path(tempfile.mkdtemp(prefix="codex-threadripper-smoke-"))
    extract_dir = workspace / "extract"
    codex_home = workspace / ".codex"

    try:
        if args.archive:
            binary_path = extract_binary(
                pathlib.Path(args.archive).resolve(), extract_dir, args.platform
            )
        else:
            binary_path = pathlib.Path(args.binary).resolve()
        prepare_codex_home(codex_home)
        assert_status(binary_path, codex_home, expected_total=3, expected_mismatched=2)
        backup_path = run_sync(binary_path, codex_home)
        assert_backup_contains_dirty_rows(backup_path)
        assert_status(binary_path, codex_home, expected_total=3, expected_mismatched=0)
        run_watch(binary_path, codex_home, args.platform)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)

    print(f"smoke test passed for {args.platform}")
    return 0


def extract_binary(archive_path: pathlib.Path, extract_dir: pathlib.Path, platform: str) -> pathlib.Path:
    extract_dir.mkdir(parents=True, exist_ok=True)
    if archive_path.suffix == ".zip":
        with zipfile.ZipFile(archive_path) as archive:
            archive.extractall(extract_dir)
    else:
        with tarfile.open(archive_path) as archive:
            archive.extractall(extract_dir)

    binary_name = "codex-threadripper.exe" if platform == "windows" else "codex-threadripper"
    matches = list(extract_dir.rglob(binary_name))
    if len(matches) != 1:
        raise RuntimeError(f"expected one {binary_name} in {archive_path}, found {len(matches)}")
    return matches[0]


def prepare_codex_home(codex_home: pathlib.Path) -> None:
    codex_home.mkdir(parents=True, exist_ok=True)
    (codex_home / "config.toml").write_text('model_provider = "openai"\n', encoding="utf-8")

    sessions_dir = codex_home / "sessions" / "2026" / "04" / "15"
    sessions_dir.mkdir(parents=True, exist_ok=True)
    (sessions_dir / "rollout-a.jsonl").write_text(
        '{"type":"session_meta","payload":{"model_provider":"vm"}}\n',
        encoding="utf-8",
    )
    (sessions_dir / "rollout-b.jsonl").write_text(
        '{"type":"session_meta","payload":{"model_provider":"cp"}}\n',
        encoding="utf-8",
    )

    sqlite_path = codex_home / "state_5.sqlite"
    connection = sqlite3.connect(sqlite_path)
    connection.executescript(SCHEMA_SQL)
    connection.executemany(
        """
        INSERT INTO threads (
            id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
            sandbox_policy, approval_mode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("1", "/tmp/a", 1, 1, "cli", "vm", "/tmp", "a", "workspace-write", "auto"),
            ("2", "/tmp/b", 1, 1, "cli", "cp", "/tmp", "b", "workspace-write", "auto"),
            ("3", "/tmp/c", 1, 1, "cli", "openai", "/tmp", "c", "workspace-write", "auto"),
        ],
    )
    connection.commit()
    connection.close()


def command_env() -> dict[str, str]:
    env = os.environ.copy()
    env["CODEX_THREADRIPPER_LANG"] = "en"
    env["LANG"] = "en_US.UTF-8"
    env["LC_ALL"] = "en_US.UTF-8"
    return env


def run_command(binary_path: pathlib.Path, codex_home: pathlib.Path, *args: str) -> str:
    completed = subprocess.run(
        [str(binary_path), "--codex-home", str(codex_home), *args],
        check=True,
        capture_output=True,
        text=True,
        env=command_env(),
    )
    return completed.stdout


def assert_status(
    binary_path: pathlib.Path,
    codex_home: pathlib.Path,
    *,
    expected_total: int,
    expected_mismatched: int,
) -> None:
    output = run_command(binary_path, codex_home, "status")
    expected_lines = [
        "Target provider: openai",
        f"Total threads: {expected_total}",
        f"Rows needing reconcile: {expected_mismatched}",
    ]
    for line in expected_lines:
        if line not in output:
            raise RuntimeError(f"missing line in status output: {line}\n\n{output}")


def run_sync(binary_path: pathlib.Path, codex_home: pathlib.Path) -> pathlib.Path:
    output = run_command(binary_path, codex_home, "sync")
    if "Rows updated: 2" not in output:
        raise RuntimeError(f"unexpected sync output\n\n{output}")

    backup_line = next(
        (line for line in output.splitlines() if line.startswith("Backup: ")),
        None,
    )
    if backup_line is None:
        raise RuntimeError(f"missing backup line in sync output\n\n{output}")
    backup_path = pathlib.Path(backup_line.removeprefix("Backup: ").strip())
    if not backup_path.exists():
        raise RuntimeError(f"backup file is missing: {backup_path}")
    return backup_path


def assert_backup_contains_dirty_rows(backup_path: pathlib.Path) -> None:
    connection = sqlite3.connect(backup_path)
    mismatched = connection.execute(
        "SELECT COUNT(*) FROM threads WHERE model_provider <> 'openai'"
    ).fetchone()[0]
    connection.close()
    if mismatched != 2:
        raise RuntimeError(f"backup expected 2 dirty rows, got {mismatched}")


def run_watch(binary_path: pathlib.Path, codex_home: pathlib.Path, platform: str) -> None:
    env = command_env()
    creationflags = 0
    if platform == "windows":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)

    process = subprocess.Popen(
        [str(binary_path), "--codex-home", str(codex_home), "watch"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        creationflags=creationflags,
    )

    try:
        wait_for_watch_banner(process, platform)
        insert_dirty_row(codex_home / "state_5.sqlite")
        write_new_session(codex_home)
        wait_for_reconcile(binary_path, codex_home)
    finally:
        stop_watch_process(process, platform)


def wait_for_watch_banner(process: subprocess.Popen[str], platform: str) -> None:
    deadline = time.time() + 10
    collected: list[str] = []
    while time.time() < deadline:
        line = process.stdout.readline()
        if line:
            collected.append(line)
            joined = "".join(collected)
            if "Watch started" in joined and "500 ms poll interval" in joined:
                return
        if process.poll() is not None:
            stderr = process.stderr.read() if process.stderr else ""
            raise RuntimeError(
                f"watch process exited early\nstdout:\n{''.join(collected)}\nstderr:\n{stderr}"
            )
    raise RuntimeError(f"watch banner timeout\nstdout:\n{''.join(collected)}")


def insert_dirty_row(sqlite_path: pathlib.Path) -> None:
    connection = sqlite3.connect(sqlite_path)
    connection.execute(
        """
        INSERT INTO threads (
            id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
            sandbox_policy, approval_mode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("4", "/tmp/d", 1, 1, "cli", "vm", "/tmp", "d", "workspace-write", "auto"),
    )
    connection.commit()
    connection.close()


def write_new_session(codex_home: pathlib.Path) -> None:
    sessions_dir = codex_home / "sessions" / "2026" / "04" / "15"
    sessions_dir.mkdir(parents=True, exist_ok=True)
    (sessions_dir / "rollout-c.jsonl").write_text(
        '{"type":"session_meta","payload":{"model_provider":"vm"}}\n',
        encoding="utf-8",
    )


def wait_for_reconcile(binary_path: pathlib.Path, codex_home: pathlib.Path) -> None:
    deadline = time.time() + 10
    while time.time() < deadline:
        output = run_command(binary_path, codex_home, "status")
        if "Total threads: 4" in output and "Rows needing reconcile: 0" in output:
            return
        time.sleep(0.25)
    raise RuntimeError("watch did not reconcile the new dirty row in time")


def stop_watch_process(process: subprocess.Popen[str], platform: str) -> None:
    if process.poll() is not None:
        return

    try:
        if platform == "windows":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            process.send_signal(signal.SIGINT)
        process.wait(timeout=5)
    except Exception:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


if __name__ == "__main__":
    sys.exit(main())
