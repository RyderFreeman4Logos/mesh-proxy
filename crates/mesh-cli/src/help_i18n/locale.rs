use i18n_embed::{DesktopLanguageRequester, unic_langid::LanguageIdentifier};

#[cfg(test)]
use anyhow::{Context, Result};
#[cfg(test)]
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SupportedLocale {
    En,
    ZhCn,
}

impl SupportedLocale {
    pub(crate) fn bundle_order(self) -> [&'static str; 2] {
        match self {
            Self::En => ["en", "en"],
            Self::ZhCn => ["zh-CN", "en"],
        }
    }
}

pub(crate) fn select_locale_from_system() -> SupportedLocale {
    let requested = DesktopLanguageRequester::requested_languages();
    select_locale_from_requested(&requested)
}

pub(crate) fn select_locale_from_requested(requested: &[LanguageIdentifier]) -> SupportedLocale {
    for language in requested {
        match language.language.as_str() {
            "zh" => return SupportedLocale::ZhCn,
            "en" => return SupportedLocale::En,
            _ => continue,
        }
    }

    SupportedLocale::En
}

#[cfg(test)]
pub(crate) fn parse_requested_languages(tags: &[&str]) -> Result<Vec<LanguageIdentifier>> {
    tags.iter()
        .map(|tag| {
            LanguageIdentifier::from_str(tag)
                .with_context(|| format!("failed to parse language tag {tag}"))
        })
        .collect()
}
