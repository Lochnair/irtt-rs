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
    done: Option<JoinHandle<Result<(), irtt_server::ServerRuntimeError>>>,
}

impl InTreeServer {
    pub fn start(config: ServerConfig) -> Self {
        let (startup_tx, startup_rx) = mpsc::sync_channel(1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let done = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build in-tree test server runtime");
            runtime.block_on(async move {
                let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
                let mut server = Server::bind(bind_addr, config).await?;
                startup_tx
                    .send(server.local_addr()?)
                    .expect("test stopped waiting for in-tree server startup");
                server
                    .run(async {
                        let _ = shutdown_rx.await;
                    })
                    .await
            })
        });

        let addr = match startup_rx.recv_timeout(STARTUP_TIMEOUT) {
            Ok(addr) => addr,
            Err(error) => {
                let _ = shutdown_tx.send(());
                let result = done.join().expect("in-tree test server thread panicked");
                panic!("in-tree test server failed before startup ({error}): {result:?}");
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
