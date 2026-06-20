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
        "irtt-cli" => Some(RequestedApplet::Client),
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
            "unknown applet name '{basename}'. Known applet names: irtt-rs, irtt-cli, irtt-tui, irtt-server"
        ));
    }

    let Some(command) = argv.get(1).and_then(|arg| arg.to_str()) else {
        return Err(format!(
            "choose an applet: client, tui, or server\n\n{}",
            dispatcher_help()
        ));
    };

    match command {
        "-h" | "--help" => Ok(AppletDispatch::Help(dispatcher_help())),
        "client" => Ok(AppletDispatch::Run {
            applet: RequestedApplet::Client,
            argv: applet_argv("irtt-cli", &argv[2..]),
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
            "unknown applet '{command}'. Choose one of: client, tui, server\n\n{}",
            dispatcher_help()
        )),
    }
}

pub fn dispatcher_help() -> String {
    let mut command = Command::new("irtt-rs")
        .about("IRTT-compatible multi-applet dispatcher")
        .subcommand(Command::new("client").about("Run the stream client applet"))
        .subcommand(Command::new("tui").about("Run the terminal UI applet"))
        .subcommand(Command::new("server").about("Run the server applet"))
        .after_help("Applet binary names: irtt-cli, irtt-tui, irtt-server");
    command.render_help().to_string()
}

fn applet_basename(argv0: &str) -> &str {
    Path::new(argv0)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(argv0)
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
            detect_applet_from_argv0("/usr/bin/irtt-cli"),
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
    fn default_applet_prefers_client_then_tui() {
        assert_eq!(
            default_applet_for_features(true, true, false),
            AppletAvailability::Client
        );
        assert_eq!(
            default_applet_for_features(false, true, false),
            AppletAvailability::Tui
        );
        assert_eq!(
            default_applet_for_features(false, false, false),
            AppletAvailability::None
        );
    }
}
