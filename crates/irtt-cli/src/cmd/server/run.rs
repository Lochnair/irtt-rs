use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use irtt_server::Server;

use super::args::ServerArgs;

/// How often the shutdown future samples the process-wide signal flag.
///
/// The flag a signal handler sets has no async wake mechanism, so the future
/// polls it on a timer rather than spinning. `Server::run` owns everything after
/// that future becomes ready.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Runs one listener until the process is asked to shut down.
///
/// The applet owns exactly one current-thread Tokio runtime, one bound socket
/// and one reusable [`Server`]; the server's own loop is the event loop.
pub fn run_server(
    args: ServerArgs,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(serve(args, shutdown_requested))
}

async fn serve(
    args: ServerArgs,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut server = Server::bind(args.bind, args.server_config()).await?;
    eprintln!("irtt-server: listening on {}", server.local_addr()?);
    server.run(shutdown_signal(shutdown_requested)).await?;
    Ok(())
}

async fn shutdown_signal(shutdown_requested: &AtomicBool) {
    while !shutdown_requested.load(Ordering::Relaxed) {
        tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn an_already_requested_shutdown_binds_and_returns() {
        let args = ServerArgs::try_parse_from(["irtt-server", "--bind", "127.0.0.1:0"]).unwrap();
        let shutdown_requested = AtomicBool::new(true);

        run_server(args, &shutdown_requested).unwrap();
    }
}
