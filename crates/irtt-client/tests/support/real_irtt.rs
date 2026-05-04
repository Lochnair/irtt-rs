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
    keepalive_ms: Option<u64>,
}

impl RealIrtServer {
    pub fn start(hmac_key: Option<&[u8]>) -> Result<Self, String> {
        let port = Self::find_free_port()?;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let bind = format!("127.0.0.1:{}", port);

        let irtt_bin = std::env::var("IRTT_BIN").unwrap_or_else(|_| "irtt".to_string());

        let keepalive_ms = std::env::var("IRTT_TEST_KEEP_SERVER_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok());

        debug_eprintln!("[real_irtt] backend=real irtt_bin={irtt_bin} bind={bind}");

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

        let full_cmd = format!("{cmd:?}");
        debug_eprintln!("[real_irtt] spawning: {full_cmd}");

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("failed to spawn irtt server: {}", e))?;

        let pid = child.id();
        debug_eprintln!("[real_irtt] child pid={pid}");

        let stdout = child.stdout.take().ok_or("failed to capture stdout")?;
        let stderr = child.stderr.take().ok_or("failed to capture stderr")?;

        let (out_tx, out_rx) = mpsc::channel();
        let (err_tx, err_rx) = mpsc::channel();

        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                match line {
                    Ok(l) => {
                        if out_tx.send(("stdout", l)).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines() {
                match line {
                    Ok(l) => {
                        if err_tx.send(("stderr", l)).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        let timeout = Duration::from_secs(5);
        let start = Instant::now();
        let mut captured_lines = Vec::new();

        let ready = loop {
            let remaining = timeout
                .checked_sub(start.elapsed())
                .unwrap_or(Duration::ZERO);

            let line = select_line(&out_rx, &err_rx, remaining);
            match line {
                Some((stream, text)) => {
                    debug_eprintln!("[real_irtt] {stream}: {text}");
                    captured_lines.push(format!("{stream}: {text}"));
                    if text.contains("[ListenerStart]") {
                        break true;
                    }
                    if (text.contains("[ListenerStop]") || text.contains("[ServerStop]"))
                        && (text.contains("error") || text.contains("Error"))
                    {
                        break false;
                    }
                }
                None => {
                    break false;
                }
            }
        };

        if !ready {
            let exit_status = child.try_wait().ok().flatten();
            debug_eprintln!("[real_irtt] startup failed, exit_status={exit_status:?}");
            child.kill().ok();
            child.wait().ok();

            let output = captured_lines.join("\n");
            let extra = match exit_status {
                Some(status) => format!("\nexit status: {status}"),
                None => String::new(),
            };
            return Err(format!(
                "irtt server failed to start\noutput:\n{output}{extra}"
            ));
        }

        debug_eprintln!("[real_irtt] ready on {addr}");

        drop(out_rx);
        drop(err_rx);

        Ok(Self {
            addr,
            child,
            keepalive_ms,
        })
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
        if let Some(ms) = self.keepalive_ms {
            debug_eprintln!(
                "[real_irtt] keepalive: sleeping {ms}ms before kill (pid={})",
                self.child.id()
            );
            thread::sleep(Duration::from_millis(ms));
        }
        debug_eprintln!("[real_irtt] killing child pid={}", self.child.id());
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

fn select_line(
    out_rx: &mpsc::Receiver<(&str, String)>,
    err_rx: &mpsc::Receiver<(&str, String)>,
    timeout: Duration,
) -> Option<(&'static str, String)> {
    if let Ok((_, line)) = out_rx.recv_timeout(timeout) {
        return Some(("stdout", line));
    }
    if let Ok((_, line)) = err_rx.recv_timeout(Duration::ZERO) {
        return Some(("stderr", line));
    }
    if timeout > Duration::ZERO {
        if let Ok((_, line)) = out_rx.recv_timeout(Duration::from_millis(100)) {
            return Some(("stdout", line));
        }
        if let Ok((_, line)) = err_rx.recv_timeout(Duration::from_millis(100)) {
            return Some(("stderr", line));
        }
    }
    None
}

macro_rules! debug_eprintln {
    ($($arg:tt)*) => {
        if std::env::var("IRTT_TEST_BACKEND_DEBUG").as_deref() == Ok("1") {
            eprintln!($($arg)*);
        }
    };
}

use debug_eprintln;
