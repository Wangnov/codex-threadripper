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
  <a href="https://github.com/Wangnov/codex-threadripper/actions/workflows/release-candidate.yml"><img src="https://github.com/Wangnov/codex-threadripper/actions/workflows/release-candidate.yml/badge.svg" alt="Release candidate"></a>
  <a href="https://github.com/Wangnov/codex-threadripper/actions/workflows/release.yml"><img src="https://github.com/Wangnov/codex-threadripper/actions/workflows/release.yml/badge.svg" alt="Release"></a>
  <a href="https://github.com/Wangnov/codex-threadripper/actions/workflows/npm-publish.yml"><img src="https://github.com/Wangnov/codex-threadripper/actions/workflows/npm-publish.yml/badge.svg" alt="npm publish"></a>
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

`codex-threadripper` 是 CLI 工具，专门解决在 `CODEX_HOME` 里的 `config.toml` 中，切换不同的 model_provider 之后，会话的线程历史被分散到不同 provider 桶里，导致无法使用 `codex resume` 或在 Codex APP 中访问不通 Provider 创建的会话的问题。

它的做法是，把 `CODEX_HOME/state_5.sqlite` 持续收敛到当前 `model_provider`，这样 `codex resume`、Codex.app 和基于 app-server 的客户端都能看到所有完整的线程历史。

## 它适合谁

- 你会在同一个 Codex 里切换多个 model_provider
- 你希望 `resume` 和 APP 的会话列表里能看到不同的 Provider 的任何历史会话线程
- 你可能需要频繁地切换 model_provider，想把无法打开不同 Provider 会话这件事无感地解决掉

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

### cargo binstall

```bash
cargo binstall codex-threadripper
```

### 直接下载二进制

从 [最新 GitHub Release](https://github.com/Wangnov/codex-threadripper/releases/latest) 下载平台对应的压缩包、安装脚本或 MSI。

## 快速开始

```bash
# 检查你的 CODEX_HOME 下当前有多少个不同 model_provider 的会话线程
codex-threadripper status
# 同步所有不同的 model_provider 会话线程
codex-threadripper sync
# 在后台持续监控
codex-threadripper watch
# 安装并作为系统后台服务持续监控
codex-threadripper install-service
```

## 命令说明

- `status`：查看当前 provider、SQLite 分布和后台服务状态
- `sync`：立即执行一次收敛，并先在 `CODEX_HOME/backups/` 里写入一个带时间戳的备份
- `watch`：持续监听配置变化和新增线程，自动收敛到当前 provider
- `print-service-config`：打印当前平台的后台服务配置
- `install-service`：安装并启动后台服务
- `uninstall-service`：停止并移除后台服务

兼容旧命令别名：

- `print-plist`
- `install-launchd`
- `uninstall-launchd`

`watch` 默认轮询间隔是 `500ms`。`watch`、`print-service-config` 和 `install-service` 都支持 `--poll-interval-ms`。

## 它会改什么

- `sync` 会备份并更新 `CODEX_HOME/state_5.sqlite`
- `watch` 会持续处理新增的脏线程记录
- 日常使用不会改写 rollout 文件，如果想查询某个thread的具体model_provider，仍可回源去查看

## 平台与语言

- `status`、`sync`、`watch` 适合任何能访问 `CODEX_HOME/state_5.sqlite` 的环境
- macOS 使用 `launchd`
- Linux 使用 `systemd --user`
- Windows 使用启动文件夹中的后台启动脚本
- 已发布产物覆盖 Apple Silicon macOS、Intel macOS、Linux x64、Linux ARM64、Windows x64
- 当前支持简体中文和英文

## 从源码运行

```bash
cargo run -- --help
```

---

<a id="readme-en"></a>

# English

`codex-threadripper` is a CLI tool for one specific problem: after switching `model_provider` in `CODEX_HOME/config.toml`, thread history can get split across different provider buckets, which makes some sessions unreachable from `codex resume` or from Codex.app when they were created under another provider.

It works by continuously reconciling `CODEX_HOME/state_5.sqlite` toward the current `model_provider`, so `codex resume`, Codex.app, and app-server based clients can all see the full thread history in one place.

## Who this is for

- You switch between multiple `model_provider` values in one Codex home
- You want `resume` and the app session list to show thread history created under any provider
- You switch providers often and want this fixed quietly in the background

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

Download the right archive, installer script, or MSI from the
[latest GitHub Release](https://github.com/Wangnov/codex-threadripper/releases/latest).

## Quick start

```bash
# Check how many thread rows currently live under different model providers
codex-threadripper status
# Reconcile all thread rows into the current model provider
codex-threadripper sync
# Keep watching in the background
codex-threadripper watch
# Install and run it as a background service
codex-threadripper install-service
```

## Commands

- `status`: show the current provider, SQLite distribution, and background service state
- `sync`: reconcile once right now and write a timestamped backup into `CODEX_HOME/backups/`
- `watch`: keep listening for config changes and new thread rows
- `print-service-config`: print the background service config for the current platform
- `install-service`: install and start the background service
- `uninstall-service`: stop and remove the background service

Legacy aliases still work:

- `print-plist`
- `install-launchd`
- `uninstall-launchd`

`watch` uses `500ms` by default. `watch`, `print-service-config`, and `install-service` accept `--poll-interval-ms`.

## What it changes

- `sync` backs up and updates `CODEX_HOME/state_5.sqlite`
- `watch` keeps reconciling newly added dirty thread rows
- everyday usage leaves rollout files untouched, so if you want to inspect the original provider of a thread, you can still trace it back to the source data

## Platforms and languages

- `status`, `sync`, and `watch` fit any environment that can access `CODEX_HOME/state_5.sqlite`
- macOS uses `launchd`
- Linux uses `systemd --user`
- Windows uses a Startup folder background launcher
- published assets cover Apple Silicon macOS, Intel macOS, Linux x64, Linux ARM64, and Windows x64
- the CLI currently supports Simplified Chinese and English

## Run from source

```bash
cargo run -- --help
```
