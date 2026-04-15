# codex-threadripper

`codex-threadripper` 是一个面向人的小型 CLI，用来让 `CODEX_HOME/state_5.sqlite` 持续对齐当前 `model_provider`。

它的目标很直接：让 Codex 线程列表和 `resume` 流程始终落在同一个 provider 桶里，这样线程历史会更完整、更连续。

## 适合谁

- 经常在同一个 `CODEX_HOME` 下切换 provider 的人
- 想让 `codex resume` 和基于 `app-server` 的线程列表看到同一批历史线程的人
- 想把这件事做成后台常驻服务的人

## 功能

- `status`
  - 查看当前 config provider、SQLite 分布和后台服务状态
- `sync`
  - 立刻执行一次 SQLite 收敛
- `watch`
  - 持续监听 `config.toml`，并定时收敛新增线程
- `print-plist`
  - 打印 `launchd` plist
- `install-launchd`
  - 安装并加载后台服务
- `uninstall-launchd`
  - 卸载后台服务

## 安装

```bash
cargo install codex-threadripper
```

也可以从源码目录直接运行：

```bash
cargo run -- --help
```

## 示例

```bash
codex-threadripper status
codex-threadripper sync
codex-threadripper watch
codex-threadripper install-launchd
```

## 本地化

当前支持两种语言：

- 简体中文
- 英文

语言选择顺序：

1. `CODEX_THREADRIPPER_LANG`
2. `LC_ALL`
3. `LC_MESSAGES`
4. `LANG`
5. macOS 系统语言

## 平台

`launchd` 相关命令面向 macOS。  
`status`、`sync` 和 `watch` 这类核心命令适合任何能访问 `CODEX_HOME/state_5.sqlite` 的环境。

## English

`codex-threadripper` is a human-first CLI that keeps `CODEX_HOME/state_5.sqlite` aligned with the current `model_provider`.

It helps Codex thread lists and resume flows stay in one provider bucket, so thread history remains complete and continuous.

### Install

```bash
cargo install codex-threadripper
```

### Commands

- `status` shows the current provider, SQLite distribution, and background service state
- `sync` reconciles SQLite once right now
- `watch` keeps listening for provider changes and new thread rows
- `print-plist` prints the generated `launchd` plist
- `install-launchd` installs the background service
- `uninstall-launchd` removes the background service

## Release flow

This project uses a tag-driven `cargo-release` + `cargo-dist` flow.

Preview the next release:

```bash
cargo release patch --dry-run
```

Publish a real release:

```bash
cargo release patch --execute
```

The release tag format is:

```text
vX.Y.Z
```

After the tag is pushed, GitHub Actions runs `cargo-dist` and builds release artifacts for:

- macOS Intel
- macOS Apple Silicon
- Linux x64
- Linux ARM64
- Windows x64

Generated installers and package outputs include:

- shell installer
- PowerShell installer
- Homebrew formula artifact
- Windows MSI

GitHub Release keeps the native archives and installers. npm uses a matrix package layout:

- `codex-threadripper`
- `@wangnov/codex-threadripper-macos-arm64`
- `@wangnov/codex-threadripper-macos-x64`
- `@wangnov/codex-threadripper-linux-arm64`
- `@wangnov/codex-threadripper-linux-x64`
- `@wangnov/codex-threadripper-windows-x64`

The root package stays human-friendly. The platform packages carry the native binaries.

`.github/workflows/npm-publish.yml` downloads the release artifacts, assembles the npm matrix locally in CI, and publishes both the platform packages and the root package with npm trusted publishing.

Whenever you change `dist-workspace.toml`, rerun:

```bash
cargo dist init --yes
```
