use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use irtt_server::ServerSet;

use super::args::ServerArgs;

/// How often the shutdown future samples the process-wide signal flag.
///
/// The flag a signal handler sets has no async wake mechanism, so the future
/// polls it on a timer rather than spinning. `ServerSet::run` owns everything
/// after that future becomes ready, including fanning it out to the listeners.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Runs the configured listeners until the process is asked to shut down.
///
/// The applet owns exactly one current-thread Tokio runtime and one
/// [`ServerSet`], whatever number of addresses was requested. A single `--bind`
/// is an ordinary set of one and takes the same path as a set of several: one
/// construction, one shutdown future, one supervision rule. Listener
/// concurrency is async I/O concurrency and needs no worker threads.
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
    let config = args.server_config();
    let set = ServerSet::bind(args.bind, config).await?;
    // Only once every listener is up. Announcing one address and then failing
    // to bind the next would report a service that never started.
    for addr in set.local_addrs() {
        eprintln!("irtt-server: listening on {addr}");
    }
    set.run(shutdown_signal(shutdown_requested)).await?;
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

    /// The ordinary invocation, and the one that keeps the single-listener case
    /// on the same orchestration path as every other.
    #[test]
    fn an_already_requested_shutdown_binds_one_listener_and_returns() {
        let args = ServerArgs::try_parse_from(["irtt-server", "--bind", "127.0.0.1:0"]).unwrap();
        let shutdown_requested = AtomicBool::new(true);

        run_server(args, &shutdown_requested).unwrap();
    }

    #[test]
    fn an_already_requested_shutdown_binds_every_listener_and_returns() {
        let args = ServerArgs::try_parse_from([
            "irtt-server",
            "--bind",
            "127.0.0.1:0",
            "--bind",
            "127.0.0.1:0",
        ])
        .unwrap();
        let shutdown_requested = AtomicBool::new(true);

        run_server(args, &shutdown_requested).unwrap();
    }
}
