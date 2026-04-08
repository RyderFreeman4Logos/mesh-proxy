use std::fmt::Write as _;
use std::str::FromStr;

use anyhow::{Context, Result};
use clap::{Arg, Command, CommandFactory};
use i18n_embed::{LanguageLoader, fluent::FluentLanguageLoader, unic_langid::LanguageIdentifier};
use rust_embed::RustEmbed;

use super::locale::SupportedLocale;

const DOMAIN: &str = "mesh-proxy";

#[derive(RustEmbed)]
#[folder = "i18n/"]
struct Localizations;

pub(crate) fn render_help_for_path<T: CommandFactory>(
    path: &[String],
    locale: SupportedLocale,
) -> Result<String> {
    let command = command_for_path::<T>(path)?;
    let translator = Translator::load(locale)?;
    Ok(render_command_help(&command, path, &translator))
}

fn command_for_path<T: CommandFactory>(path: &[String]) -> Result<Command> {
    let mut current = T::command();
    current.build();

    for name in &path[1..] {
        current = current
            .find_subcommand(name)
            .cloned()
            .with_context(|| format!("unknown subcommand path component {name}"))?;
    }

    Ok(current)
}

fn render_command_help(command: &Command, path: &[String], translator: &Translator) -> String {
    let mut output = String::new();
    let about = translator.command_about(path, command);

    if !about.is_empty() {
        writeln!(output, "{about}").expect("writing help text");
        writeln!(output).expect("writing help text");
    }

    writeln!(
        output,
        "{}: {}",
        translator.text("heading-usage"),
        usage_line(command, path, translator)
    )
    .expect("writing help text");

    let commands = command_entries(command, path, translator);
    let arguments = positional_entries(command, path, translator);
    let options = option_entries(command, path, translator);

    if !commands.is_empty() {
        writeln!(output).expect("writing help text");
        writeln!(output, "{}:", translator.text("heading-commands")).expect("writing help text");
        render_entries(&mut output, &commands);
    }

    if !arguments.is_empty() {
        writeln!(output).expect("writing help text");
        writeln!(output, "{}:", translator.text("heading-arguments")).expect("writing help text");
        render_entries(&mut output, &arguments);
    }

    if !options.is_empty() {
        writeln!(output).expect("writing help text");
        writeln!(output, "{}:", translator.text("heading-options")).expect("writing help text");
        render_entries(&mut output, &options);
    }

    output
}

fn usage_line(command: &Command, path: &[String], translator: &Translator) -> String {
    let mut parts = vec![path.join(" ")];

    let visible_options: Vec<_> = command
        .get_opts()
        .filter(|arg| !arg.is_hide_set())
        .collect();
    let required_options: Vec<_> = visible_options
        .iter()
        .copied()
        .filter(|arg| arg.is_required_set())
        .collect();
    let has_optional_options = visible_options
        .iter()
        .any(|arg| !arg.is_required_set() && !matches!(arg.get_id().as_str(), "help" | "version"));

    for arg in required_options {
        parts.push(option_usage_piece(path, arg, translator));
    }

    if has_optional_options {
        parts.push(format!("[{}]", translator.text("label-options")));
    }

    for positional in command.get_positionals().filter(|arg| !arg.is_hide_set()) {
        parts.push(positional_usage_piece(path, positional, translator));
    }

    if command.has_subcommands() {
        let placeholder = translator.text("label-command");
        if command.is_subcommand_required_set() {
            parts.push(format!("<{placeholder}>"));
        } else {
            parts.push(format!("[{placeholder}]"));
        }
    }

    parts.join(" ")
}

fn option_usage_piece(path: &[String], arg: &Arg, translator: &Translator) -> String {
    let mut pieces = Vec::new();

    if let Some(long) = arg.get_long() {
        pieces.push(format!("--{long}"));
    } else if let Some(short) = arg.get_short() {
        pieces.push(format!("-{short}"));
    }

    if takes_value(arg) {
        pieces.push(format!("<{}>", translator.arg_value_name(path, arg)));
    }

    pieces.join(" ")
}

fn positional_usage_piece(path: &[String], arg: &Arg, translator: &Translator) -> String {
    let mut piece = format!("<{}>", translator.arg_value_name(path, arg));
    if !arg.is_required_set() {
        piece = format!("[{piece}]");
    }
    piece
}

fn command_entries(command: &Command, path: &[String], translator: &Translator) -> Vec<Entry> {
    let mut subcommands: Vec<_> = command
        .get_subcommands()
        .filter(|subcommand| !subcommand.is_hide_set())
        .collect();
    subcommands.sort_by_key(|subcommand| (subcommand.get_display_order(), subcommand.get_name()));

    subcommands
        .into_iter()
        .map(|subcommand| {
            let mut subcommand_path = path.to_vec();
            subcommand_path.push(subcommand.get_name().to_string());
            Entry {
                name: subcommand.get_name().to_string(),
                description: translator.command_about(&subcommand_path, subcommand),
                details: Vec::new(),
            }
        })
        .collect()
}

fn positional_entries(command: &Command, path: &[String], translator: &Translator) -> Vec<Entry> {
    let mut positionals: Vec<_> = command
        .get_positionals()
        .filter(|arg| !arg.is_hide_set())
        .collect();
    positionals.sort_by_key(|arg| (arg.get_display_order(), arg.get_id().as_str().to_string()));

    positionals
        .into_iter()
        .map(|arg| Entry {
            name: format!("<{}>", translator.arg_value_name(path, arg)),
            description: translator.arg_help(path, arg),
            details: arg_details(path, arg, translator),
        })
        .collect()
}

fn option_entries(command: &Command, path: &[String], translator: &Translator) -> Vec<Entry> {
    let mut options: Vec<_> = command
        .get_opts()
        .filter(|arg| !arg.is_hide_set())
        .collect();
    options.sort_by_key(|arg| (arg.get_display_order(), arg.get_id().as_str().to_string()));

    let mut entries: Vec<_> = options
        .into_iter()
        .map(|arg| Entry {
            name: option_display_name(path, arg, translator),
            description: translator.arg_help(path, arg),
            details: arg_details(path, arg, translator),
        })
        .collect();

    entries.push(Entry {
        name: "-h, --help".to_string(),
        description: translator.text("label-help"),
        details: Vec::new(),
    });

    if command.get_version().is_some() || command.get_long_version().is_some() {
        entries.push(Entry {
            name: "-V, --version".to_string(),
            description: translator.text("label-version"),
            details: Vec::new(),
        });
    }

    entries
}

fn option_display_name(path: &[String], arg: &Arg, translator: &Translator) -> String {
    let mut names = Vec::new();
    if let Some(short) = arg.get_short() {
        names.push(format!("-{short}"));
    }
    if let Some(long) = arg.get_long() {
        names.push(format!("--{long}"));
    }

    let mut display = names.join(", ");
    if takes_value(arg) {
        write!(display, " <{}>", translator.arg_value_name(path, arg)).expect("writing option");
    }
    display
}

fn arg_details(path: &[String], arg: &Arg, translator: &Translator) -> Vec<String> {
    let mut details = Vec::new();
    let default_values = arg.get_default_values();
    if !default_values.is_empty() {
        let value = default_values
            .iter()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join(", ");
        details.push(format!("[{}: {value}]", translator.text("label-default")));
    }

    let possible_values: Vec<_> = arg
        .get_possible_values()
        .into_iter()
        .filter(|value| !value.is_hide_set())
        .map(|value| value.get_name().to_string())
        .collect();
    if !possible_values.is_empty() {
        details.push(format!(
            "[{}: {}]",
            translator.text("label-possible-values"),
            possible_values.join(", ")
        ));
    }

    if matches!(arg.get_id().as_str(), "help" | "version") {
        return details;
    }

    if arg.is_global_set() && arg.get_id().as_str() == "config" && path.len() > 1 {
        return details;
    }

    details
}

fn takes_value(arg: &Arg) -> bool {
    arg.get_num_args()
        .map(|range| range.takes_values())
        .unwrap_or(false)
}

fn render_entries(output: &mut String, entries: &[Entry]) {
    for entry in entries {
        writeln!(output, "  {}", entry.name).expect("writing help text");
        if !entry.description.is_empty() {
            writeln!(output, "      {}", entry.description).expect("writing help text");
        }
        for detail in &entry.details {
            writeln!(output, "      {detail}").expect("writing help text");
        }
    }
}

struct Entry {
    name: String,
    description: String,
    details: Vec<String>,
}

struct Translator {
    loader: FluentLanguageLoader,
}

impl Translator {
    fn load(locale: SupportedLocale) -> Result<Self> {
        let fallback = LanguageIdentifier::from_str("en").context("parsing fallback locale")?;
        let loader = FluentLanguageLoader::new(DOMAIN, fallback);
        let languages = locale
            .bundle_order()
            .into_iter()
            .map(LanguageIdentifier::from_str)
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("parsing supported locales")?;
        loader
            .load_languages(&Localizations, &languages)
            .context("loading embedded help translations")?;
        Ok(Self { loader })
    }

    fn text(&self, key: &str) -> String {
        self.loader.get(key)
    }

    fn command_about(&self, path: &[String], command: &Command) -> String {
        let key = if path.len() == 1 {
            "command-root-about".to_string()
        } else if path.last().map(String::as_str) == Some("help") {
            "command-help-about".to_string()
        } else {
            format!("command-{}-about", path[1..].join("-"))
        };

        self.lookup(&key).unwrap_or_else(|| {
            fallback_styled(command.get_long_about().or_else(|| command.get_about()))
        })
    }

    fn arg_help(&self, path: &[String], arg: &Arg) -> String {
        match arg.get_id().as_str() {
            "help" => self.text("label-help"),
            "version" => self.text("label-version"),
            "config" => self
                .lookup("arg-config-help")
                .unwrap_or_else(|| fallback_styled(arg.get_long_help().or_else(|| arg.get_help()))),
            id => {
                let scoped = self.arg_key(path, id, "help");
                self.lookup(&scoped)
                    .or_else(|| self.lookup(&format!("arg-{id}-help")))
                    .unwrap_or_else(|| {
                        fallback_styled(arg.get_long_help().or_else(|| arg.get_help()))
                    })
            }
        }
    }

    fn arg_value_name(&self, path: &[String], arg: &Arg) -> String {
        let fallback = arg
            .get_value_names()
            .and_then(|names| names.first())
            .map(ToString::to_string)
            .unwrap_or_else(|| arg.get_id().as_str().to_uppercase());

        match arg.get_id().as_str() {
            "config" => self.lookup("arg-config-value").unwrap_or(fallback),
            id => {
                let scoped = self.arg_key(path, id, "value");
                self.lookup(&scoped)
                    .or_else(|| self.lookup(&format!("arg-{id}-value")))
                    .unwrap_or(fallback)
            }
        }
    }

    fn arg_key(&self, path: &[String], arg_id: &str, suffix: &str) -> String {
        if path.len() == 1 {
            format!("arg-{arg_id}-{suffix}")
        } else {
            format!("arg-{}-{arg_id}-{suffix}", path[1..].join("-"))
        }
    }

    fn lookup(&self, key: &str) -> Option<String> {
        let value = self.loader.get(key);
        let missing = format!("No localization for id: \"{key}\"");
        if value == missing { None } else { Some(value) }
    }
}

fn fallback_styled(value: Option<&clap::builder::StyledStr>) -> String {
    value.map(ToString::to_string).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::super::render_localized_help_for_args;
    use crate::Cli;

    #[test]
    fn falls_back_to_english_for_unsupported_locale() {
        let help = render_localized_help_for_args::<Cli, _>(["mesh-proxy", "--help"], &["fr-FR"])
            .unwrap()
            .unwrap();

        assert!(help.contains("Usage: mesh-proxy [OPTIONS] <COMMAND>"));
        assert!(help.contains("Commands:"));
        assert!(help.contains("Options:"));
        assert!(help.contains("-h, --help"));
        assert!(help.contains("-V, --version"));
        assert!(help.contains("Decentralized P2P port forwarding and service discovery"));
    }

    #[test]
    fn renders_root_help_in_zh_cn_for_zh_locales() {
        let help = render_localized_help_for_args::<Cli, _>(["mesh-proxy", "--help"], &["zh-HK"])
            .unwrap()
            .unwrap();

        assert!(help.contains("用法: mesh-proxy [选项] <命令>"));
        assert!(help.contains("命令:"));
        assert!(help.contains("选项:"));
        assert!(help.contains("-h, --help"));
        assert!(help.contains("-V, --version"));
        assert!(help.contains("去中心化 P2P 端口转发与服务发现工具"));
        assert!(help.contains("启动 mesh-proxy 守护进程。"));
    }

    #[test]
    fn renders_subcommand_help_in_zh_cn() {
        let help =
            render_localized_help_for_args::<Cli, _>(["mesh-proxy", "start", "--help"], &["zh-CN"])
                .unwrap()
                .unwrap();

        assert!(help.contains("用法: mesh-proxy start --role <角色> [选项]"));
        assert!(help.contains("选项:"));
        assert!(help.contains("--role <角色>"));
        assert!(help.contains("节点角色：control 或 edge。"));
        assert!(help.contains("控制节点端点地址序列化字符串"));
    }
}
