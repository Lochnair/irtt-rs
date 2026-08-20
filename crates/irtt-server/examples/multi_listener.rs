//! Multi-listener example using [`ServerSet`].
//!
//! `ServerSet` binds several independent [`Server`] listeners as one service:
//! all-or-nothing bind, one shutdown fanned out to every listener, and the
//! whole set fails if any listener does. Each listener still gets its own
//! socket, session table, and tokens — a token issued by one is unknown at
//! the other. This is what the `irtt-rs` server CLI always runs through, even
//! for a single bind.
//!
//! Run with:
//!
//! ```text
//! cargo run -p irtt-server --example multi_listener
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use irtt_server::{ServerConfig, ServerSet};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let addrs = [
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
    ];
    let config = ServerConfig::default();

    let set = match ServerSet::bind(addrs, config).await {
        Ok(set) => set,
        Err(err) => {
            eprintln!("failed to bind listener set: {err}");
            return;
        }
    };

    println!("listening on {:?}", set.local_addrs());

    let result = set.run(tokio::time::sleep(Duration::from_secs(2))).await;
    if let Err(err) = result {
        eprintln!("listener set stopped with an error: {err}");
    }
}
