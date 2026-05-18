use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;

use crate::types::ConfigToml;

pub const DEFAULT_PROVIDER: &str = "openai";
pub const CODEX_SQLITE_HOME_ENV: &str = "CODEX_SQLITE_HOME";
pub const STATE_DB_FILENAME: &str = "state_5.sqlite";
pub const DEFAULT_BUCKET_PADDING_BYTES: usize = 256;

pub fn default_codex_home() -> PathBuf {
    // Cross-platform home detection: HOME (Unix), USERPROFILE (Windows cmd),
    // fall back to HOMEDRIVE+HOMEPATH (legacy Windows)
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .or_else(|| {
            let drive = std::env::var_os("HOMEDRIVE")?;
            let path = std::env::var_os("HOMEPATH")?;
            Some(PathBuf::from(drive).join(path))
        })
        .unwrap_or_else(|| PathBuf::from("."));
    home.join(".codex")
}

pub fn read_provider_from_config(codex_home: &Path) -> Result<String> {
    let parsed = read_codex_config(codex_home)?;
    Ok(parsed
        .model_provider
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_PROVIDER.to_string()))
}

pub fn read_codex_config(codex_home: &Path) -> Result<ConfigToml> {
    let config_path = codex_home.join("config.toml");
    if !config_path.exists() {
        return Ok(ConfigToml {
            model_provider: None,
            sqlite_home: None,
        });
    }

    let raw = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    toml::from_str(&raw).with_context(|| format!("failed to parse {}", config_path.display()))
}

pub fn resolve_sqlite_path(codex_home: &Path) -> Result<PathBuf> {
    let parsed = read_codex_config(codex_home)?;
    let current_dir =
        std::env::current_dir().context("failed to resolve current directory for sqlite_home")?;
    Ok(resolve_sqlite_home_from_config(
        codex_home,
        parsed.sqlite_home.as_deref(),
        std::env::var(CODEX_SQLITE_HOME_ENV).ok().as_deref(),
        current_dir.as_path(),
    )
    .join("state_5.sqlite"))
}

pub fn resolve_sqlite_home_from_config(
    codex_home: &Path,
    config_sqlite_home: Option<&str>,
    env_sqlite_home: Option<&str>,
    current_dir: &Path,
) -> PathBuf {
    if let Some(path) = config_sqlite_home.and_then(trimmed_path) {
        return resolve_path_relative_to(path, codex_home);
    }
    if let Some(path) = env_sqlite_home.and_then(trimmed_path) {
        return resolve_path_relative_to(path, current_dir);
    }
    codex_home.to_path_buf()
}

fn trimmed_path(value: &str) -> Option<&Path> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(Path::new(trimmed))
    }
}

fn resolve_path_relative_to(path: &Path, base: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}
