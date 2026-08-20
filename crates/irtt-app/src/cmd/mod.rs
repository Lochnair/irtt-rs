#[cfg(feature = "client")]
pub mod client;
#[cfg(any(feature = "client", feature = "tui"))]
mod format;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "tui")]
pub mod tui;

// Additional command applets belong here behind their own feature gate.
