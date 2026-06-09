#![forbid(unsafe_code)]

pub mod applet;
pub mod signal;

#[cfg(feature = "client")]
pub mod shared;

#[cfg(feature = "client")]
pub mod cmd;

#[cfg(feature = "stats")]
pub use cmd::client::summary;
#[cfg(feature = "client")]
pub use cmd::client::{
    format_event, format_human_event, format_human_event_with_options, ClientArgs, HumanEventStats,
    HumanIpdvPair, HumanOutputOptions, OutputMode,
};
#[cfg(feature = "tui")]
pub use cmd::tui::TuiArgs;
#[cfg(feature = "client")]
pub use shared::client::{
    parse_dscp, parse_duration, parse_length, parse_server_fill, parse_test_duration, parse_ttl,
    ClockArg, CommonClientArgs, ReceivedStatsArg, TimestampArg,
};
#[cfg(feature = "client")]
pub type CliArgs = ClientArgs;
