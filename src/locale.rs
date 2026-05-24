use std::process::Command as ProcessCommand;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Locale {
    En,
    ZhHans,
}

pub(crate) fn detect_locale() -> Locale {
    let mut candidates = Vec::new();
    if let Ok(value) = std::env::var("CODEX_THREADRIPPER_LANG") {
        candidates.push(value);
    }
    for key in ["LC_ALL", "LC_MESSAGES", "LANG"] {
        if let Ok(value) = std::env::var(key) {
            candidates.push(value);
        }
    }

    detect_locale_from_sources(
        candidates.iter().map(String::as_str),
        apple_languages_output().as_deref(),
    )
}

pub(crate) fn detect_locale_from_sources<'a, I>(
    candidates: I,
    apple_languages: Option<&str>,
) -> Locale
where
    I: IntoIterator<Item = &'a str>,
{
    for candidate in candidates {
        if let Some(locale) = parse_locale_tag(candidate) {
            return locale;
        }
    }
    if let Some(output) = apple_languages
        && let Some(locale) = parse_apple_languages(output)
    {
        return locale;
    }
    Locale::En
}

pub(crate) fn parse_locale_tag(input: &str) -> Option<Locale> {
    let normalized = input.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    if normalized.starts_with("zh") {
        return Some(Locale::ZhHans);
    }
    if normalized.starts_with("en") {
        return Some(Locale::En);
    }
    None
}

pub(crate) fn parse_apple_languages(output: &str) -> Option<Locale> {
    output
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'))
        .find_map(parse_locale_tag)
}

fn apple_languages_output() -> Option<String> {
    if !cfg!(target_os = "macos") {
        return None;
    }
    let output = ProcessCommand::new("defaults")
        .args(["read", "-g", "AppleLanguages"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}
