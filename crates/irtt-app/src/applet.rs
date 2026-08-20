use std::{ffi::OsString, path::Path};

use clap::Command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestedApplet {
    Client,
    Tui,
    Server,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppletDispatch {
    Run {
        applet: RequestedApplet,
        argv: Vec<OsString>,
    },
    Help(String),
}

pub fn detect_applet_from_argv0(argv0: &str) -> Option<RequestedApplet> {
    match applet_basename(argv0) {
        "irtt-client" => Some(RequestedApplet::Client),
        "irtt-tui" => Some(RequestedApplet::Tui),
        "irttd" | "irtt-server" => Some(RequestedApplet::Server),
        "irtt-rs" => None,
        _ => None,
    }
}

pub fn dispatch_from_argv(argv: Vec<OsString>) -> Result<AppletDispatch, String> {
    let argv0 = argv
        .first()
        .and_then(|arg| arg.to_str())
        .unwrap_or("irtt-rs");

    if let Some(applet) = detect_applet_from_argv0(argv0) {
        return Ok(AppletDispatch::Run { applet, argv });
    }

    let basename = applet_basename(argv0);
    if basename.starts_with("irtt-") && basename != "irtt-rs" {
        return Err(format!(
            "unknown applet name '{basename}'. Known applet names: {}",
            known_applet_binary_names()
        ));
    }

    let Some(command) = argv.get(1).and_then(|arg| arg.to_str()) else {
        return Err(format!(
            "choose an applet: {}\n\n{}",
            applet_command_names(),
            dispatcher_help()
        ));
    };

    match command {
        "-h" | "--help" => Ok(AppletDispatch::Help(dispatcher_help())),
        "client" => Ok(AppletDispatch::Run {
            applet: RequestedApplet::Client,
            argv: applet_argv("irtt-client", &argv[2..]),
        }),
        "tui" => Ok(AppletDispatch::Run {
            applet: RequestedApplet::Tui,
            argv: applet_argv("irtt-tui", &argv[2..]),
        }),
        "server" => Ok(AppletDispatch::Run {
            applet: RequestedApplet::Server,
            argv: applet_argv("irtt-server", &argv[2..]),
        }),
        _ => Err(format!(
            "unknown applet '{command}'. Choose one of: {}\n\n{}",
            applet_command_names(),
            dispatcher_help()
        )),
    }
}

pub fn dispatcher_help() -> String {
    let mut command = Command::new("irtt-rs")
        .about("IRTT-compatible multi-applet dispatcher")
        .subcommand(Command::new("client").about(client_applet_about()))
        .subcommand(Command::new("tui").about(tui_applet_about()))
        .subcommand(Command::new("server").about(server_applet_about()))
        .after_help(dispatcher_after_help());
    command.render_help().to_string()
}

fn client_applet_about() -> &'static str {
    if cfg!(feature = "client") {
        "Run the stream client applet"
    } else {
        "Stream client applet is not available in this build"
    }
}

fn tui_applet_about() -> &'static str {
    if cfg!(feature = "tui") {
        "Run the terminal UI applet"
    } else {
        "Terminal UI applet is not available in this build"
    }
}

fn server_applet_about() -> &'static str {
    if cfg!(feature = "server") {
        "Run the UDP server applet"
    } else {
        "UDP server applet is not available in this build"
    }
}

fn known_applet_binary_names() -> &'static str {
    "irtt-rs, irtt-client, irtt-tui, irtt-server"
}

fn applet_command_names() -> &'static str {
    "client, tui, or server"
}

fn dispatcher_after_help() -> String {
    let mut help = "Recognized applet binary names: irtt-client, irtt-tui, irtt-server".to_owned();

    let mut missing = Vec::new();
    if !cfg!(feature = "client") {
        missing.push("irtt-client requires the client feature");
    }
    if !cfg!(feature = "server") {
        missing.push("irtt-server requires the server feature");
    }
    if !cfg!(feature = "tui") {
        missing.push("irtt-tui requires the tui feature");
    }

    match missing.len() {
        0 => {}
        1 => help.push_str(&format!("\nOptional applet: {}", missing[0])),
        _ => help.push_str(&format!("\nOptional applets: {}", missing.join("; "))),
    }
    help
}

fn applet_basename(argv0: &str) -> &str {
    let name = Path::new(argv0)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(argv0);
    strip_suffix_ignoring_case(name, std::env::consts::EXE_SUFFIX)
}

/// Removes the platform executable suffix from a file name.
///
/// The suffix is part of the file name on Windows and empty everywhere else, so
/// this is a no-op on Unix. Without it, every applet name — including the
/// `irtt-rs` dispatcher itself — reaches argv0 detection as `<name>.exe` and is
/// rejected as an unknown applet. The applet names themselves are still matched
/// exactly; only this platform artifact is case-insensitive, because it is not
/// a name the user chose.
///
/// The suffix is a parameter so that Windows behavior is assertable from any
/// host, not only where `EXE_SUFFIX` happens to be non-empty.
fn strip_suffix_ignoring_case<'a>(name: &'a str, suffix: &str) -> &'a str {
    if suffix.is_empty() || name.len() <= suffix.len() {
        return name;
    }
    let split = name.len() - suffix.len();
    let (Some(stem), Some(tail)) = (name.get(..split), name.get(split..)) else {
        return name;
    };
    if tail.eq_ignore_ascii_case(suffix) {
        stem
    } else {
        name
    }
}

fn applet_argv(applet_name: &str, args: &[OsString]) -> Vec<OsString> {
    let mut argv = Vec::with_capacity(args.len() + 1);
    argv.push(OsString::from(applet_name));
    argv.extend(args.iter().cloned());
    argv
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applet_detection_maps_binary_names() {
        assert_eq!(
            detect_applet_from_argv0("/usr/bin/irtt-client"),
            Some(RequestedApplet::Client)
        );
        assert_eq!(
            detect_applet_from_argv0("irtt-tui"),
            Some(RequestedApplet::Tui)
        );
        assert_eq!(
            detect_applet_from_argv0("irtt-server"),
            Some(RequestedApplet::Server)
        );
        assert_eq!(detect_applet_from_argv0("irtt-rs"), None);
        assert_eq!(detect_applet_from_argv0("custom-name"), None);
    }

    #[test]
    fn old_irtt_cli_binary_name_is_no_longer_a_recognized_applet() {
        assert_eq!(detect_applet_from_argv0("irtt-cli"), None);
        assert_eq!(detect_applet_from_argv0("/usr/bin/irtt-cli"), None);
    }

    #[test]
    fn applet_detection_ignores_the_platform_executable_suffix() {
        let exe = std::env::consts::EXE_SUFFIX;

        assert_eq!(
            detect_applet_from_argv0(&format!("/opt/irtt/irtt-server{exe}")),
            Some(RequestedApplet::Server)
        );
        assert_eq!(
            detect_applet_from_argv0(&format!("irtt-client{exe}")),
            Some(RequestedApplet::Client)
        );

        // The dispatcher's own name must not be taken for an unknown applet.
        let dispatcher = format!("irtt-rs{exe}");
        assert_eq!(detect_applet_from_argv0(&dispatcher), None);
        assert!(dispatch_from_argv(vec![dispatcher.into(), "--help".into()]).is_ok());

        // A name that merely ends in the suffix letters keeps them.
        assert!(detect_applet_from_argv0("irtt-serverexe").is_none());
    }

    #[test]
    fn windows_executable_suffix_handling_is_assertable_from_any_host() {
        assert_eq!(
            strip_suffix_ignoring_case("irtt-server.exe", ".exe"),
            "irtt-server"
        );
        assert_eq!(strip_suffix_ignoring_case("IRTT-RS.EXE", ".exe"), "IRTT-RS");
        assert_eq!(
            strip_suffix_ignoring_case("irtt-server", ".exe"),
            "irtt-server"
        );
        assert_eq!(
            strip_suffix_ignoring_case("irtt-serverexe", ".exe"),
            "irtt-serverexe"
        );
        assert_eq!(strip_suffix_ignoring_case(".exe", ".exe"), ".exe");
        assert_eq!(
            strip_suffix_ignoring_case("irtt-server.exe", ""),
            "irtt-server.exe"
        );
        // A multi-byte tail must not split mid-character.
        assert_eq!(strip_suffix_ignoring_case("irtt-så", ".exe"), "irtt-så");
    }

    #[test]
    fn canonical_help_renders_dispatcher_help() {
        let dispatch = dispatch_from_argv(vec!["irtt-rs".into(), "--help".into()]).unwrap();
        let AppletDispatch::Help(help) = dispatch else {
            panic!("expected dispatcher help");
        };
        assert!(help.contains("IRTT-compatible multi-applet dispatcher"));
        assert!(help.contains("client"));
        if cfg!(feature = "tui") {
            assert!(help.contains("Run the terminal UI applet"));
        } else {
            assert!(help.contains("Terminal UI applet is not available in this build"));
        }
        assert!(help.contains("server"));
        if cfg!(feature = "server") {
            assert!(help.contains("Run the UDP server applet"));
        } else {
            assert!(help.contains("UDP server applet is not available in this build"));
        }
    }

    #[test]
    fn canonical_without_applet_is_an_error() {
        let err = dispatch_from_argv(vec!["irtt-rs".into()]).unwrap_err();
        assert!(err.contains("choose an applet"));
        assert!(err.contains("client"));
        if cfg!(feature = "tui") {
            assert!(err.contains("tui"));
        }
        assert!(err.contains("server"));
    }

    #[test]
    fn canonical_subcommands_dispatch_to_applet_argv() {
        assert_eq!(
            dispatch_from_argv(vec!["irtt-rs".into(), "client".into(), "host:2112".into()])
                .unwrap(),
            AppletDispatch::Run {
                applet: RequestedApplet::Client,
                argv: vec!["irtt-client".into(), "host:2112".into()],
            }
        );
        assert_eq!(
            dispatch_from_argv(vec!["irtt-rs".into(), "tui".into(), "host:2112".into()]).unwrap(),
            AppletDispatch::Run {
                applet: RequestedApplet::Tui,
                argv: vec!["irtt-tui".into(), "host:2112".into()],
            }
        );
        assert_eq!(
            dispatch_from_argv(vec!["irtt-rs".into(), "server".into()]).unwrap(),
            AppletDispatch::Run {
                applet: RequestedApplet::Server,
                argv: vec!["irtt-server".into()],
            }
        );
    }

    #[test]
    fn unknown_irtt_binary_name_is_an_error() {
        let err = dispatch_from_argv(vec!["/usr/bin/irtt-typo".into()]).unwrap_err();
        assert!(err.contains("unknown applet name 'irtt-typo'"));
        assert!(err.contains("irtt-client"));
        if cfg!(feature = "tui") {
            assert!(err.contains("irtt-tui"));
        }
        assert!(err.contains("irtt-server"));
    }
}
