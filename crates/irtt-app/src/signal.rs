use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub fn install_signal_handler(shutdown_requested: Arc<AtomicBool>) -> Result<(), ctrlc::Error> {
    ctrlc::set_handler(move || {
        shutdown_requested.store(true, Ordering::Relaxed);
    })
}
