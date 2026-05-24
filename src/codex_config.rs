use anyhow::Context;
use anyhow::Result;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;

pub(crate) const DEFAULT_PROVIDER: &str = "openai";
pub(crate) const STATE_DB_FILENAME: &str = "state_5.sqlite";

const CODEX_SQLITE_HOME_ENV: &str = "CODEX_SQLITE_HOME";
const PROFILE_CONFIG_SUFFIX: &str = ".config.toml";

#[derive(Debug, Default, Deserialize)]
struct ConfigToml {
    model_provider: Option<String>,
    sqlite_home: Option<String>,
    profile: Option<String>,
    profiles: Option<BTreeMap<String, ConfigProfileToml>>,
}

#[derive(Debug, Deserialize)]
struct ConfigProfileToml {
    model_provider: Option<String>,
}

impl ConfigToml {
    fn overlay(&mut self, profile_config: ConfigToml) {
        if profile_config.model_provider.is_some() {
            self.model_provider = profile_config.model_provider;
        }
        if profile_config.sqlite_home.is_some() {
            self.sqlite_home = profile_config.sqlite_home;
        }
    }
}

pub(crate) fn is_valid_profile_name(profile: &str) -> bool {
    profile
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

pub(crate) fn read_provider_from_config(
    codex_home: &Path,
    profile_override: Option<&str>,
) -> Result<String> {
    let parsed = read_effective_codex_config(codex_home, profile_override)?;
    Ok(parsed
        .model_provider
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_PROVIDER.to_string()))
}

pub(crate) fn resolve_sqlite_path(
    codex_home: &Path,
    profile_override: Option<&str>,
) -> Result<PathBuf> {
    let parsed = read_effective_codex_config(codex_home, profile_override)?;
    let current_dir =
        std::env::current_dir().context("failed to resolve current directory for sqlite_home")?;
    Ok(resolve_sqlite_home_from_config(
        codex_home,
        parsed.sqlite_home.as_deref(),
        std::env::var(CODEX_SQLITE_HOME_ENV).ok().as_deref(),
        current_dir.as_path(),
    )
    .join(STATE_DB_FILENAME))
}

fn read_effective_codex_config(
    codex_home: &Path,
    profile_override: Option<&str>,
) -> Result<ConfigToml> {
    let mut config = read_codex_config(codex_home)?;
    let explicit_profile = profile_override.and_then(trimmed_string);
    let config_profile = config.profile.as_deref().and_then(trimmed_string);

    if let Some(profile) = explicit_profile {
        apply_profile_config(codex_home, &mut config, profile.as_str(), true)?;
    } else if let Some(profile) = config_profile {
        apply_profile_config(codex_home, &mut config, profile.as_str(), false)?;
    }

    Ok(config)
}

fn apply_profile_config(
    codex_home: &Path,
    config: &mut ConfigToml,
    profile: &str,
    required: bool,
) -> Result<()> {
    if !is_valid_profile_name(profile) {
        anyhow::bail!("profile `{profile}` is not a valid Codex profile name");
    }
    if let Some(profile_config) = read_profile_v2_config(codex_home, profile)? {
        config.overlay(profile_config);
        return Ok(());
    }
    if let Some(profile_config) = config
        .profiles
        .as_ref()
        .and_then(|profiles| profiles.get(profile))
    {
        if let Some(provider) = profile_config.model_provider.as_ref() {
            config.model_provider = Some(provider.clone());
        }
        return Ok(());
    }
    if required {
        anyhow::bail!(
            "profile `{}` was not found; expected {} or [profiles.{}] in {}",
            profile,
            profile_config_path(codex_home, profile).display(),
            profile,
            codex_home.join("config.toml").display()
        );
    }
    Ok(())
}

fn read_codex_config(codex_home: &Path) -> Result<ConfigToml> {
    read_optional_config_file(codex_home.join("config.toml").as_path())
        .map(|config| config.unwrap_or_default())
}

fn read_profile_v2_config(codex_home: &Path, profile: &str) -> Result<Option<ConfigToml>> {
    let path = profile_config_path(codex_home, profile);
    read_optional_config_file(path.as_path())
}

fn read_optional_config_file(path: &Path) -> Result<Option<ConfigToml>> {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("failed to read {}", path.display())),
    };
    toml::from_str(&raw)
        .map(Some)
        .with_context(|| format!("failed to parse {}", path.display()))
}

pub(crate) fn selected_profile_config_path(
    codex_home: &Path,
    profile_override: Option<&str>,
) -> Option<PathBuf> {
    let profile = profile_override.and_then(trimmed_string).or_else(|| {
        read_codex_config(codex_home)
            .ok()
            .and_then(|config| config.profile.as_deref().and_then(trimmed_string))
    })?;
    if is_valid_profile_name(profile.as_str()) {
        Some(profile_config_path(codex_home, profile.as_str()))
    } else {
        None
    }
}

fn profile_config_path(codex_home: &Path, profile: &str) -> PathBuf {
    codex_home.join(format!("{profile}{PROFILE_CONFIG_SUFFIX}"))
}

fn trimmed_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub(crate) fn resolve_sqlite_home_from_config(
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
