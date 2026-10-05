#[cfg(feature = "server")]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[cfg(feature = "server")]
pub fn install_signal_handler(shutdown_requested: Arc<AtomicBool>) -> Result<(), ctrlc::Error> {
    ctrlc::set_handler(move || {
        shutdown_requested.store(true, Ordering::Relaxed);
    })
}

/// Preserve ctrlc's SIGINT/SIGTERM policy while waking the async frontends.
#[cfg(any(feature = "client", feature = "tui"))]
pub fn install_async_signal_handler() -> Result<tokio::sync::watch::Receiver<bool>, ctrlc::Error> {
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    ctrlc::set_handler(move || {
        shutdown.send_replace(true);
    })?;
    Ok(receiver)
}
