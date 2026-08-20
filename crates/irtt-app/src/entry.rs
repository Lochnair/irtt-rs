use std::{
    env,
    ffi::OsString,
    process::ExitCode,
    sync::{atomic::AtomicBool, Arc},
};

use crate::{
    applet::{dispatch_from_argv, AppletDispatch, RequestedApplet},
    signal::install_signal_handler,
};

/// Entry point for the `irtt-rs` multicall dispatcher binary.
///
/// Chooses an applet by subcommand or by recognized argv0 and may reference
/// every applet enabled by the active feature set.
pub fn dispatcher_main() -> ExitCode {
    run_with_exit_code("irtt-rs", run_dispatcher_from_env)
}

/// Entry point for the dedicated `irtt-client` binary.
///
/// Runs the stream client directly; it never consults argv0 or the applet
/// dispatcher, so a renamed or copied executable still runs the client.
#[cfg(feature = "client")]
pub fn client_main() -> ExitCode {
    run_with_exit_code("irtt-client", || {
        run_client_applet(env::args_os().collect())
    })
}

/// Entry point for the dedicated `irtt-tui` binary.
///
/// Runs the terminal UI directly; it never consults argv0 or the applet
/// dispatcher, so a renamed or copied executable still runs the TUI.
#[cfg(feature = "tui")]
pub fn tui_main() -> ExitCode {
    run_with_exit_code("irtt-tui", || run_tui_applet(env::args_os().collect()))
}

/// Entry point for the dedicated `irtt-server` binary.
///
/// Runs the server directly; it never consults argv0 or the applet
/// dispatcher, so a renamed or copied executable still runs the server.
#[cfg(feature = "server")]
pub fn server_main() -> ExitCode {
    run_with_exit_code("irtt-server", || {
        run_server_applet(env::args_os().collect())
    })
}

fn run_with_exit_code(
    program: &str,
    f: impl FnOnce() -> Result<(), Box<dyn std::error::Error>>,
) -> ExitCode {
    match f() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{program}: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run_dispatcher_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let argv: Vec<OsString> = env::args_os().collect();
    let (requested, argv) = match dispatch_from_argv(argv)? {
        AppletDispatch::Run { applet, argv } => (applet, argv),
        AppletDispatch::Help(help) => {
            print!("{help}");
            return Ok(());
        }
    };

    match requested {
        RequestedApplet::Client => run_client_applet(argv),
        RequestedApplet::Tui => run_tui_applet(argv),
        RequestedApplet::Server => run_server_applet(argv),
    }
}

/// Installs the shutdown signal handler shared by every applet and hands the
/// resulting flag to `f`.
fn with_shutdown_flag(
    f: impl FnOnce(&AtomicBool) -> Result<(), Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    install_signal_handler(Arc::clone(&shutdown_requested))
        .map_err(|err| format!("failed to install signal handler: {err}"))?;
    f(shutdown_requested.as_ref())
}

#[cfg(feature = "client")]
fn run_client_applet(argv: Vec<OsString>) -> Result<(), Box<dyn std::error::Error>> {
    use clap::Parser;

    with_shutdown_flag(|shutdown_requested| {
        let args = crate::cmd::client::ClientArgs::parse_from(argv);
        crate::cmd::client::run_stream(args, shutdown_requested)
    })
}

#[cfg(not(feature = "client"))]
fn run_client_applet(_argv: Vec<OsString>) -> Result<(), Box<dyn std::error::Error>> {
    Err("client applet is not available; rebuild with the client feature".into())
}

#[cfg(feature = "server")]
fn run_server_applet(argv: Vec<OsString>) -> Result<(), Box<dyn std::error::Error>> {
    use clap::Parser;

    with_shutdown_flag(|shutdown_requested| {
        let args = crate::cmd::server::ServerArgs::parse_from(argv);
        crate::cmd::server::run_server(args, shutdown_requested)
    })
}

#[cfg(not(feature = "server"))]
fn run_server_applet(_argv: Vec<OsString>) -> Result<(), Box<dyn std::error::Error>> {
    Err("server applet is not available; rebuild with the server feature".into())
}

#[cfg(feature = "tui")]
fn run_tui_applet(argv: Vec<OsString>) -> Result<(), Box<dyn std::error::Error>> {
    use clap::Parser;

    with_shutdown_flag(|shutdown_requested| {
        let args = crate::cmd::tui::TuiArgs::parse_from(argv);
        crate::cmd::tui::run_tui(args, shutdown_requested)
    })
}

#[cfg(not(feature = "tui"))]
fn run_tui_applet(_argv: Vec<OsString>) -> Result<(), Box<dyn std::error::Error>> {
    Err("TUI applet is not available; rebuild with the tui feature".into())
}
