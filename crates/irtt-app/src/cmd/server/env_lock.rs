//! Serializes every test in this module tree that touches process
//! environment variables.
//!
//! `ServerArgs` parsing reads `IRTT_SERVER_*` via `clap`'s `env` support, and
//! `std::env::set_var`/`remove_var` mutate genuinely process-wide state.
//! Every test that parses `ServerArgs` — whether or not it sets an env var
//! itself — must go through [`with_env_lock`], or a parse in one test thread
//! can observe a value another thread's test set only for the duration of
//! its own assertions. Reentrant per-thread, since a test that sets env vars
//! typically calls `parse` again from inside its own locked section.

use std::{cell::Cell, sync::Mutex};

static ENV_LOCK: Mutex<()> = Mutex::new(());
thread_local! {
    static ENV_LOCK_HELD: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn with_env_lock<T>(f: impl FnOnce() -> T) -> T {
    if ENV_LOCK_HELD.with(Cell::get) {
        return f();
    }
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    ENV_LOCK_HELD.with(|held| held.set(true));
    let result = f();
    ENV_LOCK_HELD.with(|held| held.set(false));
    result
}
