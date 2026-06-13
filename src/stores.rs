use anyhow::Result;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use crate::codex_config::STATE_DB_FILENAME;
use crate::codex_config::configured_sqlite_home;
use crate::locale::Locale;

/// Subdirectory under `CODEX_HOME` that Codex App points its app-server at via
/// `CODEX_SQLITE_HOME=<codex_home>/sqlite`.
pub(crate) const APP_SQLITE_SUBDIR: &str = "sqlite";

/// A distinct on-disk Codex state-database surface.
///
/// Codex App (桌面应用) injects `CODEX_SQLITE_HOME=<codex_home>/sqlite` into its
/// embedded app-server, so its `state_5.sqlite` lives under the `sqlite/`
/// subdirectory. The open-source CLI default keeps the database at the top level
/// of `CODEX_HOME`. A machine that uses both ends up with two concurrent DBs,
/// and a shell that runs threadripper inherits neither the App's env var nor any
/// `sqlite_home` config — so we must discover every surface from the filesystem.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoreKind {
    /// Explicit `sqlite_home` config key or `CODEX_SQLITE_HOME` env var.
    Configured,
    /// Codex App desktop default: `<codex_home>/sqlite/state_5.sqlite`.
    App,
    /// Codex CLI default: `<codex_home>/state_5.sqlite`.
    Cli,
}

impl StoreKind {
    /// Stable machine-readable identifier, also used for `--store` values and
    /// per-store backup namespacing.
    pub(crate) fn slug(self) -> &'static str {
        match self {
            StoreKind::Configured => "configured",
            StoreKind::App => "app",
            StoreKind::Cli => "cli",
        }
    }

    /// Human-facing label for status output.
    pub(crate) fn label(self, locale: Locale) -> &'static str {
        match (self, locale) {
            (StoreKind::Configured, Locale::En) => "configured (sqlite_home / CODEX_SQLITE_HOME)",
            (StoreKind::Configured, Locale::ZhHans) => "自定义(sqlite_home / CODEX_SQLITE_HOME)",
            (StoreKind::App, Locale::En) => "Codex App (desktop)",
            (StoreKind::App, Locale::ZhHans) => "Codex App(桌面应用)",
            (StoreKind::Cli, Locale::En) => "Codex CLI",
            (StoreKind::Cli, Locale::ZhHans) => "Codex CLI(命令行)",
        }
    }
}

/// A discovered state-database surface: its kind and the canonical path to the
/// `state_5.sqlite` file that backs it.
#[derive(Clone, Debug)]
pub(crate) struct StoreTarget {
    pub(crate) kind: StoreKind,
    pub(crate) db_path: PathBuf,
}

/// Discover every existing `state_5.sqlite` store under `codex_home`,
/// canonicalized and de-duplicated. Reads the configured `sqlite_home` /
/// `CODEX_SQLITE_HOME` (if any) and then layers the App and CLI defaults.
pub(crate) fn discover_stores(
    codex_home: &Path,
    profile_override: Option<&str>,
) -> Result<Vec<StoreTarget>> {
    let configured = configured_sqlite_home(codex_home, profile_override)?;
    Ok(discover_stores_with(codex_home, configured.as_deref()))
}

/// Pure core of [`discover_stores`]: builds candidates from an already-resolved
/// configured `sqlite_home`, keeps existing default files plus an explicit
/// configured path even when it is missing, then canonicalizes and de-duplicates
/// so the same file is never processed twice (e.g. when `sqlite_home` resolves
/// to the CLI or App default path). Priority on a path collision is Configured >
/// App > Cli (first candidate wins the label).
pub(crate) fn discover_stores_with(
    codex_home: &Path,
    configured_sqlite_home: Option<&Path>,
) -> Vec<StoreTarget> {
    let mut candidates: Vec<(StoreKind, PathBuf)> = Vec::new();
    if let Some(dir) = configured_sqlite_home {
        candidates.push((StoreKind::Configured, dir.join(STATE_DB_FILENAME)));
    }
    candidates.push((
        StoreKind::App,
        codex_home.join(APP_SQLITE_SUBDIR).join(STATE_DB_FILENAME),
    ));
    candidates.push((StoreKind::Cli, codex_home.join(STATE_DB_FILENAME)));

    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut stores: Vec<StoreTarget> = Vec::new();
    for (kind, path) in candidates {
        if !path.exists() {
            if kind == StoreKind::Configured {
                stores.push(StoreTarget {
                    kind,
                    db_path: path,
                });
            }
            continue;
        }
        let canonical = path.canonicalize().unwrap_or(path);
        if seen.insert(canonical.clone()) {
            stores.push(StoreTarget {
                kind,
                db_path: canonical,
            });
        }
    }
    stores
}

/// Error message shown when no `state_5.sqlite` exists on any surface.
pub(crate) fn no_store_found_message(locale: Locale, codex_home: &Path) -> String {
    match locale {
        Locale::En => format!(
            "no Codex state database found under {} (looked at state_5.sqlite and sqlite/state_5.sqlite) — run Codex at least once to create it",
            codex_home.display()
        ),
        Locale::ZhHans => format!(
            "在 {} 下未找到 Codex 状态库(已查 state_5.sqlite 与 sqlite/state_5.sqlite)— 请先运行一次 Codex 以生成它",
            codex_home.display()
        ),
    }
}
