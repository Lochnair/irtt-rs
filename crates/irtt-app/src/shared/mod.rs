#[cfg(any(feature = "client", feature = "tui"))]
pub mod client;

#[cfg(any(feature = "client", feature = "tui", feature = "server"))]
mod hmac;
#[cfg(any(feature = "client", feature = "tui", feature = "server"))]
pub(crate) use hmac::decode_hmac_key;
