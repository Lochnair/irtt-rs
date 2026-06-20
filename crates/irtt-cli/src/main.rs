use std::{
    env,
    ffi::OsString,
    process::ExitCode,
    sync::{atomic::AtomicBool, Arc},
};

use irtt_cli::{
    applet::{dispatch_from_argv, AppletDispatch, RequestedApplet},
    signal::install_signal_handler,
};

fn main() -> ExitCode {
    match run_from_env() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("irtt-rs: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let argv: Vec<OsString> = env::args_os().collect();
    let (requested, argv) = match dispatch_from_argv(argv)? {
        AppletDispatch::Run { applet, argv } => (applet, argv),
        AppletDispatch::Help(help) => {
            print!("{help}");
            return Ok(());
        }
    };

    let shutdown_requested = Arc::new(AtomicBool::new(false));
    install_signal_handler(Arc::clone(&shutdown_requested))
        .map_err(|err| format!("failed to install signal handler: {err}"))?;

    match requested {
        RequestedApplet::Client => run_client_applet(argv, shutdown_requested.as_ref()),
        RequestedApplet::Tui => run_tui_applet(argv, shutdown_requested.as_ref()),
        RequestedApplet::Server => Err("server applet is not available in this build".into()),
    }
}

#[cfg(feature = "client")]
fn run_client_applet(
    argv: Vec<OsString>,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    use clap::Parser;

    let args = irtt_cli::cmd::client::ClientArgs::parse_from(argv);
    irtt_cli::cmd::client::run_stream(args, shutdown_requested)
}

#[cfg(not(feature = "client"))]
fn run_client_applet(
    _argv: Vec<OsString>,
    _shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("client applet is not available; rebuild with the client feature".into())
}

#[cfg(feature = "tui")]
fn run_tui_applet(
    argv: Vec<OsString>,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    use clap::Parser;

    let args = irtt_cli::cmd::tui::TuiArgs::parse_from(argv);
    irtt_cli::cmd::tui::run_tui(args, shutdown_requested)
}

#[cfg(not(feature = "tui"))]
fn run_tui_applet(
    _argv: Vec<OsString>,
    _shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("TUI applet is not available; rebuild with the tui feature".into())
}
