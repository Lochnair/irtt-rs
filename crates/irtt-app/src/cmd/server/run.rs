use std::{
    net::SocketAddr,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use irtt_server::{address_family_available, ServerSet, ServerSetError};

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
    let binds = args.resolve_binds();
    let is_implicit_default = args.bind.is_empty();
    let set = match ServerSet::bind(binds.clone(), config.clone()).await {
        Ok(set) => set,
        Err(ServerSetError::ListenerSetup { addr, source }) => {
            let family_available = address_family_available(addr);
            match implicit_default_fallback(is_implicit_default, family_available, &binds, addr) {
                Some(remaining) => {
                    eprintln!(
                        "irtt-server: default listener {addr} unavailable ({source}); \
                         continuing with the remaining default listener"
                    );
                    // `remaining`'s lone survivor is bound exactly as it
                    // would be alone: the address family that just failed
                    // has no local support at all, so nothing on that family
                    // could be competing for the surviving listener's port.
                    ServerSet::bind(remaining, config).await?
                }
                None => return Err(ServerSetError::ListenerSetup { addr, source }.into()),
            }
        }
        Err(err) => return Err(err.into()),
    };
    // Only once every listener is up. Announcing one address and then failing
    // to bind the next would report a service that never started.
    for addr in set.local_addrs() {
        eprintln!("irtt-server: listening on {addr}");
    }
    set.run(shutdown_signal(shutdown_requested)).await?;
    Ok(())
}

/// Whether a listener-setup failure for `failed_addr` should be retried with
/// a reduced bind set instead of failing the whole invocation, and if so,
/// what to retry with.
///
/// Only the implicit zero-argument default gets this treatment, and only
/// when `failed_addr`'s own address family has no local support on this host
/// at all (`family_available` is `false`) — not merely because its specific
/// port was unavailable for some other reason: already in use, permission
/// denied, no safe wildcard reply-source path, or anything else
/// `ServerSet::bind` can fail with. Those are real configuration or
/// environment problems this invocation should fail on, not silently work
/// around by dropping a listener the operator asked for. An explicit
/// `--bind` list keeps the library's ordinary all-or-nothing behavior
/// regardless.
fn implicit_default_fallback(
    is_implicit_default: bool,
    family_available: bool,
    original_binds: &[SocketAddr],
    failed_addr: SocketAddr,
) -> Option<Vec<SocketAddr>> {
    if !is_implicit_default || family_available || !original_binds.contains(&failed_addr) {
        return None;
    }
    let remaining: Vec<SocketAddr> = original_binds
        .iter()
        .copied()
        .filter(|&addr| addr != failed_addr)
        .collect();
    (!remaining.is_empty()).then_some(remaining)
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
    use crate::cmd::server::env_lock::with_env_lock;

    /// The ordinary invocation, and the one that keeps the single-listener case
    /// on the same orchestration path as every other.
    #[test]
    fn an_already_requested_shutdown_binds_one_listener_and_returns() {
        let args = with_env_lock(|| {
            ServerArgs::try_parse_from(["irtt-server", "--bind", "127.0.0.1:0"]).unwrap()
        });
        let shutdown_requested = AtomicBool::new(true);

        run_server(args, &shutdown_requested).unwrap();
    }

    #[test]
    fn an_already_requested_shutdown_binds_every_listener_and_returns() {
        let args = with_env_lock(|| {
            ServerArgs::try_parse_from([
                "irtt-server",
                "--bind",
                "127.0.0.1:0",
                "--bind",
                "127.0.0.1:0",
            ])
            .unwrap()
        });
        let shutdown_requested = AtomicBool::new(true);

        run_server(args, &shutdown_requested).unwrap();
    }

    fn default_binds() -> Vec<SocketAddr> {
        with_env_lock(|| {
            ServerArgs::try_parse_from(["irtt-server"])
                .unwrap()
                .resolve_binds()
        })
    }

    #[test]
    fn implicit_default_losing_an_unsupported_family_falls_back_to_the_other() {
        let binds = default_binds();
        let failed = binds[0];

        let remaining = implicit_default_fallback(true, false, &binds, failed).unwrap();

        assert_eq!(remaining, [binds[1]]);
    }

    #[test]
    fn a_supported_familys_failure_never_falls_back() {
        // The family itself works fine here; whatever made this specific
        // bind fail (port in use, permission, ...) is a real problem this
        // invocation should surface, not paper over.
        let binds = default_binds();

        assert_eq!(
            implicit_default_fallback(true, true, &binds, binds[0]),
            None,
            "family_available: true means this is not a family-support problem"
        );
    }

    #[test]
    fn explicit_binds_never_fall_back() {
        let binds = default_binds();

        assert_eq!(
            implicit_default_fallback(false, false, &binds, binds[0]),
            None
        );
    }

    #[test]
    fn a_failure_outside_the_requested_set_does_not_fall_back() {
        let binds = default_binds();
        let unrelated = SocketAddr::from(([127, 0, 0, 1], 2113));

        assert_eq!(
            implicit_default_fallback(true, false, &binds, unrelated),
            None,
            "the failing address must actually be one this invocation asked for"
        );
    }

    #[test]
    fn losing_the_only_remaining_default_address_does_not_fall_back() {
        let single = [SocketAddr::from(([0, 0, 0, 0], 2112))];

        assert_eq!(
            implicit_default_fallback(true, false, &single, single[0]),
            None,
            "no address would be left to retry with"
        );
    }
}
