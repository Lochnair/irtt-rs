#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "tui")]
pub mod tui;

// Future command applets, such as a server applet, belong here behind their
// own feature gate.
