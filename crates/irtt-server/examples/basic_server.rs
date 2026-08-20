//! Basic single-listener server example.
//!
//! Binds one [`Server`] on an ephemeral loopback port, serves it for a short
//! fixed window, and shuts down. A real deployment would normally bind a
//! fixed port and run until an external shutdown signal.
//!
//! Run with:
//!
//! ```text
//! cargo run -p irtt-server --example basic_server
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use irtt_server::{Server, ServerConfig};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let config = ServerConfig::default();

    let mut server = match Server::bind(bind_addr, config).await {
        Ok(server) => server,
        Err(err) => {
            eprintln!("failed to bind server: {err}");
            return;
        }
    };

    println!(
        "listening on {}",
        server
            .local_addr()
            .expect("bound socket has a local address")
    );

    let result = server.run(tokio::time::sleep(Duration::from_secs(2))).await;
    if let Err(err) = result {
        eprintln!("server stopped with an error: {err}");
    }
}
