#[cfg(any(feature = "client", feature = "tui", feature = "server"))]
use std::ffi::OsString;

pub(crate) fn app() -> String {
    clap::Command::new("irtt-rs")
        .version(env!("CARGO_PKG_VERSION"))
        .render_version()
}

// Preserve Clap's version banner and append the compiled component versions.
#[cfg(any(feature = "client", feature = "tui", feature = "server"))]
pub(crate) fn parse<T: clap::Parser>(argv: Vec<OsString>, report: fn() -> String) -> T {
    match T::try_parse_from(argv) {
        Ok(args) => args,
        Err(err) if err.kind() == clap::error::ErrorKind::DisplayVersion => {
            print!("{}{}", err.render().ansi(), report());
            std::process::exit(0)
        }
        Err(err) => err.exit(),
    }
}

#[cfg(any(feature = "client", feature = "tui"))]
pub(crate) fn client() -> String {
    #[cfg(feature = "stats")]
    let stats = format!("  stats:  {}\n", irtt_stats::VERSION);
    #[cfg(not(feature = "stats"))]
    let stats = "  stats: disabled\n";
    format!(
        "  client: {}\n{}  proto:  {}\n",
        irtt_client::VERSION,
        stats,
        irtt_proto::VERSION,
    )
}

#[cfg(feature = "server")]
pub(crate) fn server() -> String {
    format!(
        "  server: {}\n  proto:  {}\n",
        irtt_server::VERSION,
        irtt_proto::VERSION,
    )
}
