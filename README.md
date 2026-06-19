<p align="center">
  <img src="./logo.png" width="220" alt="codex-threadripper logo">
</p>

<h1 align="center">codex-threadripper</h1>

<p align="center">
  Keep Codex thread history in one provider bucket.
</p>

<p align="center">
  <a href="https://github.com/Wangnov/codex-threadripper/releases/latest"><img src="https://img.shields.io/github/v/release/Wangnov/codex-threadripper?logo=github" alt="Latest release"></a>
  <a href="https://crates.io/crates/codex-threadripper"><img src="https://img.shields.io/crates/v/codex-threadripper?logo=rust" alt="Crates.io"></a>
  <a href="https://www.npmjs.com/package/codex-threadripper"><img src="https://img.shields.io/npm/v/codex-threadripper?logo=npm" alt="npm"></a>
  <a href="https://github.com/Wangnov/codex-threadripper/actions/workflows/release-candidate.yml"><img src="https://img.shields.io/badge/release%20candidate-manual%20gate-0969da" alt="Release candidate gate"></a>
  <a href="https://github.com/Wangnov/codex-threadripper/actions/workflows/release.yml"><img src="https://img.shields.io/badge/release-automated-2ea44f" alt="Release automation"></a>
  <a href="https://github.com/Wangnov/codex-threadripper/actions/workflows/npm-publish.yml"><img src="https://img.shields.io/badge/npm%20publish-automated-2ea44f" alt="npm publish automation"></a>
  <a href="https://github.com/Wangnov/homebrew-tap"><img src="https://img.shields.io/badge/Homebrew-wangnov%2Ftap-FBB040?logo=homebrew" alt="Homebrew tap"></a>
  <a href="https://github.com/Wangnov/codex-threadripper/blob/main/Cargo.toml"><img src="https://img.shields.io/badge/license-MIT-2ea44f" alt="MIT license"></a>
</p>

<p align="center">
  <a href="#readme-cn">中文</a> · <a href="#readme-en">English</a>
</p>

<p align="center">
  Homebrew · npm · crates.io · cargo-binstall · GitHub Release
</p>

---

<a id="readme-cn"></a>

# 中文

你有没有遇到过这种情况：在 Codex 里切到另一个 model_provider，然后发现之前的会话全不见了？打开 Codex.app，列表空空如也；执行 `codex resume`，什么都恢复不了。

会话其实还在，只是 Codex 的 SQLite 状态库里每条线程记录了创建它的 provider，切换 provider 之后 Codex 只会显示当前 provider 的线程，其他的就像蒸发了一样。

`codex-threadripper` 就是为了解决这件事而生的。它会把 Codex 的 SQLite 状态库和 rollout JSONL 首行里的 provider 字段统一收敛到你当前用的那个，这样不管你来回怎么切，`codex resume`、Codex.app 和 app-server 客户端都能看到完整的历史记录。

## 适合谁用

- 你会在同一个 Codex 里频繁切换不同的 model_provider
- 你希望所有历史会话在任何 provider 下都能打开和续接
- 你不想每次切完 provider 之后手动处理这个问题

## 社区

本项目链接并认可 [LINUX DO](https://linux.do/) 社区。欢迎在社区讨论帖中交流使用体验、问题和改进建议。

## 安装

### Homebrew

```bash
brew tap wangnov/tap
brew install codex-threadripper
```

### npm

```bash
npm i -g codex-threadripper
```

### cargo-binstall

```bash
cargo binstall codex-threadripper
```

### cargo install

```bash
cargo install codex-threadripper
```

### 直接下载二进制

从 [最新 GitHub Release](https://github.com/Wangnov/codex-threadripper/releases/latest) 下载对应平台的压缩包、安装脚本或 MSI 安装器。

## 快速上手

```bash
# 先看看现在的情况——有多少线程散落在不同的 provider 桶里
codex-threadripper status

# 立刻做一次收敛（会先备份，放心）
codex-threadripper sync

# 明确把所有历史切到某个 provider 可见桶
codex-threadripper bucket switch cong

# 在前台持续监听，实时处理新线程
codex-threadripper watch

# 装成系统后台服务，以后开机自动跑
codex-threadripper install-service
```

## 命令说明

- `status`：查看当前 provider、SQLite 里各 provider 的线程分布，以及后台服务的运行状态
- `sync`：立即执行一次收敛。执行前会在 SQLite 状态库旁边的 `backups/` 目录里写一份带时间戳的备份；如果 rollout 首行需要改写，也会写 compact journal
- `bucket switch <provider>`：把全部历史切到指定 provider 可见桶；首行有 padding 时原地 patch，旧的冷 rollout 需要扩容时会安全重写并补 padding
- `bucket prepare`：检查 rollout 首行 padding；需要扩容首行的文件会被跳过，避免替换仍可能被 Codex 写入的日志文件
- `watch`：持续监听 `config.toml` 的变化，同时定时收敛新增的线程记录
- `print-service-config`：打印当前平台的后台服务配置内容（launchd plist / systemd unit / Windows 隐藏 VBS 启动器）
- `install-service`：把后台服务装到系统里并立即启动
- `uninstall-service`：停止并卸载后台服务

以下旧命令名仍然有效，会自动映射到新命令：

- `print-plist` → `print-service-config`
- `install-launchd` → `install-service`
- `uninstall-launchd` → `uninstall-service`

`watch` 的默认轮询间隔是 500ms。`watch`、`print-service-config` 和 `install-service` 都支持 `--poll-interval-ms` 来自定义间隔。

所有命令都支持 `--provider <provider>` 强制指定目标桶，或 `--profile <name>` 按 Codex v2 profile 读取 `$CODEX_HOME/<name>.config.toml` 里的 `model_provider` / `sqlite_home`。Profile 名称只能包含 ASCII 字母、数字、`_` 或 `-`。如果没有同名 profile 文件，也会兼容旧的 `[profiles.<name>]` provider 配置。

## 它会改什么

- `sync` 会在 SQLite 状态库旁边的 `backups/` 目录里创建备份，然后更新 `state_5.sqlite` 里的 `model_provider` 字段
- `sync` / `bucket switch` 也会改写 rollout JSONL 首行里的 provider 可见桶，并写入 compact journal；有足够 padding 时会原地 patch，旧的冷 rollout 需要扩容时会通过临时文件安全重写并补 padding；刚写过、仍可能被 Codex 写入的 rollout 会被跳过，待稍后重跑。改写后会恢复原本的文件访问时间和修改时间，避免 Codex.app 的最近会话排序被工具污染
- `watch` 在运行期间会持续处理新写入的线程记录，保持数据库与 rollout 首行里的 provider 可见桶和当前 provider 对齐
- `--sqlite-only` 只更新 SQLite，适合只关心数据库索引的场景；当前 Codex.app 也会读取 rollout JSONL 元数据，所以日常可见性修复不建议使用它
- 默认状态库路径是 `CODEX_HOME/state_5.sqlite`；如果 Codex 配置了 `sqlite_home` 或环境变量 `CODEX_SQLITE_HOME`，`codex-threadripper` 会跟随 Codex 使用对应目录下的 `state_5.sqlite`

## 平台支持

- `status`、`sync`、`watch` 在任何能访问 Codex SQLite 状态库和 rollout JSONL 的环境下都能用
- macOS 后台服务使用 `launchd`
- Linux 后台服务使用 `systemd --user`（没有 systemd 时会退回到独立的后台进程）
- Windows 后台服务使用启动文件夹里的隐藏 VBS 启动器，登录后不会弹出控制台窗口
- 发布的二进制覆盖 Apple Silicon macOS、Intel macOS、Linux x64、Linux ARM64、Windows x64
- CLI 界面同时支持简体中文和英文，根据系统语言自动切换

## 从源码运行

```bash
cargo run -- --help
```

---

<a id="readme-en"></a>

# English

Here's a situation you might recognize: you switch `model_provider` in Codex, and suddenly your session list is empty. `codex resume` finds nothing. The Codex app shows a blank slate.

Your sessions aren't gone. They're just filed under a different provider in Codex's SQLite state DB. Codex only shows threads that match the active provider, so anything created under a different one becomes effectively invisible.

`codex-threadripper` fixes this by rewriting the `model_provider` field on every thread row in Codex's SQLite state DB and in the first line of each rollout JSONL to match whichever provider you have active right now. After that, `codex resume`, Codex.app, and app-server clients all see the full history — no matter how many times you've switched.

## Who this is for

- You switch between multiple `model_provider` values in the same Codex home
- You want thread history to follow you across provider switches, not stay locked to the provider that created each session
- You'd rather have this handled quietly in the background than think about it

## Community

This project links back to and recognizes the [LINUX DO](https://linux.do/) community. Feedback, usage notes, and improvement ideas are welcome in the community discussion thread.

## Install

### Homebrew

```bash
brew tap wangnov/tap
brew install codex-threadripper
```

### npm

```bash
npm i -g codex-threadripper
```

### cargo-binstall

```bash
cargo binstall codex-threadripper
```

### cargo install

```bash
cargo install codex-threadripper
```

### Direct binary download

Grab the right archive, installer script, or MSI from the
[latest GitHub Release](https://github.com/Wangnov/codex-threadripper/releases/latest).

## Quick start

```bash
# See how your threads are currently distributed across providers
codex-threadripper status

# Reconcile everything right now (a backup is written first)
codex-threadripper sync

# Explicitly move all history into one provider visibility bucket
codex-threadripper bucket switch cong

# Keep watching in the foreground, handling new threads as they arrive
codex-threadripper watch

# Or install it as a background service so it just runs from now on
codex-threadripper install-service
```

## Commands

- `status`: show the active provider, the per-provider thread counts from SQLite, and whether the background service is running
- `sync`: reconcile once right now, writing a timestamped backup to the `backups/` directory beside the SQLite state DB; rollout first-line changes also get a compact journal
- `bucket switch <provider>`: move all history into the requested provider visibility bucket; prepared first lines are patched in place, while older cold rollouts that need first-line growth are safely rewritten with padding
- `bucket prepare`: inspect rollout first-line padding; files that would require first-line growth are skipped so the tool does not replace logs Codex may still be writing
- `watch`: keep watching `config.toml` for provider changes and periodically reconcile any newly added thread rows
- `print-service-config`: print the platform-specific service config (launchd plist, systemd unit, or Windows hidden VBS launcher) without installing it
- `install-service`: write the service config and start the service
- `uninstall-service`: stop and remove the service

These legacy command names still work:

- `print-plist` → `print-service-config`
- `install-launchd` → `install-service`
- `uninstall-launchd` → `uninstall-service`

`watch` polls every 500ms by default. You can change that with `--poll-interval-ms` on `watch`, `print-service-config`, and `install-service`.

Every command accepts `--provider <provider>` to force the target bucket, or `--profile <name>` to resolve `model_provider` / `sqlite_home` from Codex's v2 `$CODEX_HOME/<name>.config.toml` profile. Profile names must contain only ASCII letters, digits, `_`, or `-`. If that profile file is missing, legacy `[profiles.<name>]` provider config is still supported.

## What it changes

- `sync` writes a backup next to the SQLite state DB and then updates the `model_provider` column in `state_5.sqlite`
- `sync` / `bucket switch` also rewrite the provider visibility bucket in rollout JSONL first lines and write a compact journal. Prepared first lines are patched in place; older cold rollouts that need first-line growth are safely rewritten through a temporary file and padded for future switches; recently written rollouts that Codex may still be writing are skipped and can be retried later. After each rewrite, the original file access and modification times are restored so Codex.app's recent-thread ordering is not polluted by the tool
- `watch` keeps both SQLite and rollout first-line provider buckets aligned with the active provider as new threads are written
- `--sqlite-only` updates SQLite only. Use it only when you deliberately do not need rollout JSONL metadata; current Codex.app builds also read rollout metadata for visibility
- The default state DB is `CODEX_HOME/state_5.sqlite`; if Codex uses `sqlite_home` or `CODEX_SQLITE_HOME`, `codex-threadripper` follows that directory and uses its `state_5.sqlite`

## Platforms

- `status`, `sync`, and `watch` work anywhere you can access Codex's SQLite state DB and rollout JSONL files
- macOS uses `launchd`
- Linux uses `systemd --user`, falling back to a detached process when systemd isn't available
- Windows uses a hidden VBS launcher in the Startup folder, so no console window appears on login
- Prebuilt binaries are available for Apple Silicon macOS, Intel macOS, Linux x64, Linux ARM64, and Windows x64
- The CLI detects your system language and switches between Simplified Chinese and English automatically

## Run from source

```bash
cargo run -- --help
```
