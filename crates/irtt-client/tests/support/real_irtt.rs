#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, UdpSocket};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

pub struct RealIrtServer {
    addr: SocketAddr,
    child: Child,
}

impl RealIrtServer {
    pub fn start(hmac_key: Option<&[u8]>) -> Result<Self, String> {
        let port = Self::find_free_port()?;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let bind = format!("127.0.0.1:{}", port);

        let irtt_bin = std::env::var("IRTT_BIN").unwrap_or_else(|_| "irtt".to_string());

        let mut cmd = Command::new(&irtt_bin);
        cmd.arg("server")
            .arg("-b")
            .arg(&bind)
            .arg("--tstamp=dual")
            .stderr(Stdio::piped())
            .stdout(Stdio::piped());

        if let Some(key) = hmac_key {
            let hex_key: String = key.iter().map(|b| format!("{:02x}", b)).collect();
            cmd.arg(format!("--hmac=0x{}", hex_key));
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("failed to spawn irtt server: {}", e))?;

        let stdout = child.stdout.take().ok_or("failed to capture stdout")?;
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                match line {
                    Ok(l) => {
                        if tx.send(l).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        let timeout = Duration::from_secs(5);
        let start = Instant::now();
        let mut stdout_lines = Vec::new();
        loop {
            let remaining = timeout
                .checked_sub(start.elapsed())
                .unwrap_or(Duration::ZERO);
            match rx.recv_timeout(remaining) {
                Ok(line) => {
                    stdout_lines.push(line.clone());
                    if line.contains("[ListenerStart]") {
                        break;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    child.kill().ok();
                    child.wait().ok();
                    return Err(format!(
                        "timeout waiting for irtt server to start\nstdout:\n{}",
                        stdout_lines.join("\n")
                    ));
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    child.kill().ok();
                    child.wait().ok();
                    return Err(format!(
                        "irtt server exited unexpectedly\nstdout:\n{}",
                        stdout_lines.join("\n")
                    ));
                }
            }
        }

        drop(rx);

        Ok(Self { addr, child })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn find_free_port() -> Result<u16, String> {
        let socket = UdpSocket::bind("127.0.0.1:0")
            .map_err(|e| format!("failed to find free port: {}", e))?;
        let port = socket
            .local_addr()
            .map_err(|e| format!("failed to get local addr: {}", e))?
            .port();
        drop(socket);
        Ok(port)
    }
}

impl Drop for RealIrtServer {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}
