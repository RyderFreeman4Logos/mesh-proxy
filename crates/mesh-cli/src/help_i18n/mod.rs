mod locale;
mod render;

use std::ffi::{OsStr, OsString};

use anyhow::Result;
use clap::{Command, CommandFactory};

use self::locale::select_locale_from_system;
#[cfg(test)]
use self::locale::{parse_requested_languages, select_locale_from_requested};
use self::render::render_help_for_path;

pub(crate) fn print_localized_help_if_requested<T, I>(args: I) -> Result<bool>
where
    T: CommandFactory,
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let Some(path) = help_target_path::<T>(&args) else {
        return Ok(false);
    };

    let help = render_help_for_path::<T>(&path, select_locale_from_system())?;
    print!("{help}");
    Ok(true)
}

#[cfg(test)]
pub(crate) fn render_localized_help_for_args<T, I>(
    args: I,
    requested_languages: &[&str],
) -> Result<Option<String>>
where
    T: CommandFactory,
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let Some(path) = help_target_path::<T>(&args) else {
        return Ok(None);
    };

    let requested = parse_requested_languages(requested_languages)?;
    let locale = select_locale_from_requested(&requested);
    render_help_for_path::<T>(&path, locale).map(Some)
}

fn help_target_path<T: CommandFactory>(args: &[OsString]) -> Option<Vec<String>> {
    let mut root = T::command();
    root.build();
    detect_help_target(args, &root)
}

fn detect_help_target(args: &[OsString], root: &Command) -> Option<Vec<String>> {
    if args.is_empty() {
        return None;
    }

    let mut current = root;
    let mut path = vec![root.get_name().to_string()];
    let mut positional_index = 0usize;
    let mut end_of_options = false;
    let mut index = 1usize;

    while index < args.len() {
        let token = &args[index];

        if !end_of_options {
            if token == OsStr::new("--") {
                end_of_options = true;
                index += 1;
                continue;
            }

            if token == OsStr::new("-h") || token == OsStr::new("--help") {
                return Some(path);
            }

            if token == OsStr::new("-V") || token == OsStr::new("--version") {
                return None;
            }

            if let Some((name, has_inline_value)) = parse_long_option(token) {
                let takes_value = option_takes_value(current, name)?;
                index += if takes_value && !has_inline_value {
                    2
                } else {
                    1
                };
                continue;
            }
        }

        let token_str = token.to_string_lossy();
        if let Some(subcommand) = current.find_subcommand(token_str.as_ref()) {
            current = subcommand;
            path.push(subcommand.get_name().to_string());
            positional_index = 0;
            end_of_options = false;
            index += 1;
            continue;
        }

        if has_positional_at(current, positional_index) {
            positional_index += 1;
            index += 1;
            continue;
        }

        return None;
    }

    None
}

fn parse_long_option(token: &OsStr) -> Option<(&str, bool)> {
    let token = token.to_str()?;
    if !token.starts_with("--") || token == "--" {
        return None;
    }

    let option = &token[2..];
    if option.is_empty() {
        return None;
    }

    if let Some((name, _value)) = option.split_once('=') {
        Some((name, true))
    } else {
        Some((option, false))
    }
}

fn option_takes_value(command: &Command, option_name: &str) -> Option<bool> {
    command
        .get_arguments()
        .find(|arg| arg.get_long() == Some(option_name))
        .map(|arg| {
            arg.get_num_args()
                .map(|range| range.takes_values())
                .unwrap_or(false)
        })
}

fn has_positional_at(command: &Command, position: usize) -> bool {
    command
        .get_positionals()
        .filter(|arg| !arg.is_hide_set())
        .nth(position)
        .is_some()
}

#[cfg(test)]
mod tests {
    use clap::{Parser, Subcommand};

    use super::*;

    #[derive(Parser)]
    #[command(name = "sample", about = "sample root", version)]
    struct SampleCli {
        #[arg(long, global = true)]
        config: Option<String>,

        #[command(subcommand)]
        command: SampleCommand,
    }

    #[derive(Subcommand)]
    enum SampleCommand {
        Start {
            #[arg(long)]
            role: String,
        },
        Status,
    }

    #[test]
    fn detects_root_help_after_global_option() {
        let path = help_target_path::<SampleCli>(&[
            OsString::from("sample"),
            OsString::from("--config"),
            OsString::from("/tmp/sample.toml"),
            OsString::from("--help"),
        ]);

        assert_eq!(path, Some(vec!["sample".to_string()]));
    }

    #[test]
    fn detects_subcommand_help() {
        let path = help_target_path::<SampleCli>(&[
            OsString::from("sample"),
            OsString::from("start"),
            OsString::from("--help"),
        ]);

        assert_eq!(path, Some(vec!["sample".to_string(), "start".to_string()]));
    }

    #[test]
    fn ignores_invalid_subcommand_before_help() {
        let path = help_target_path::<SampleCli>(&[
            OsString::from("sample"),
            OsString::from("unknown"),
            OsString::from("--help"),
        ]);

        assert_eq!(path, None);
    }

    #[test]
    fn keeps_version_out_of_help_path() {
        let path =
            help_target_path::<SampleCli>(&[OsString::from("sample"), OsString::from("--version")]);

        assert_eq!(path, None);
    }
}
