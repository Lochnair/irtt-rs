use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::mpsc,
    thread::{self, JoinHandle},
    time::Duration,
};

use irtt_server::{Server, ServerConfig};
use tokio::sync::oneshot;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(2);

pub struct InTreeServer {
    pub addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    done: Option<JoinHandle<Result<(), String>>>,
}

impl InTreeServer {
    pub fn start(config: ServerConfig) -> Self {
        let (startup_tx, startup_rx) = mpsc::sync_channel(1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let done = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("failed to build test server runtime: {error}"))?;
            runtime.block_on(async move {
                let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
                let mut server = match Server::bind(bind_addr, config).await {
                    Ok(server) => server,
                    Err(error) => {
                        let message = format!("failed to bind in-tree test server: {error}");
                        let _ = startup_tx.send(Err(message.clone()));
                        return Err(message);
                    }
                };
                let addr = match server.local_addr() {
                    Ok(addr) => addr,
                    Err(error) => {
                        let message =
                            format!("failed to read in-tree test server address: {error}");
                        let _ = startup_tx.send(Err(message.clone()));
                        return Err(message);
                    }
                };
                startup_tx
                    .send(Ok(addr))
                    .map_err(|_| "test stopped waiting for in-tree server startup".to_owned())?;
                server
                    .run(async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|error| format!("in-tree test server failed: {error}"))
            })
        });

        let addr = match startup_rx.recv_timeout(STARTUP_TIMEOUT) {
            Ok(Ok(addr)) => addr,
            Ok(Err(error)) => {
                done.join()
                    .expect("in-tree test server thread panicked")
                    .ok();
                panic!("{error}");
            }
            Err(error) => {
                let _ = shutdown_tx.send(());
                done.join()
                    .expect("in-tree test server thread panicked")
                    .ok();
                panic!("timed out waiting for in-tree test server startup: {error}");
            }
        };

        Self {
            addr,
            shutdown: Some(shutdown_tx),
            done: Some(done),
        }
    }

    fn stop_inner(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(done) = self.done.take() {
            done.join()
                .expect("in-tree test server thread panicked")
                .expect("in-tree test server stopped with an error");
        }
    }
}

impl Drop for InTreeServer {
    fn drop(&mut self) {
        self.stop_inner();
    }
}
