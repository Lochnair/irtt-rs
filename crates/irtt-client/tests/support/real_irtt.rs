#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read};
use std::net::{SocketAddr, UdpSocket};
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const STARTUP_ATTEMPTS: usize = 5;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

pub struct RealIrtServer {
    addr: SocketAddr,
    child: Child,
    keepalive_ms: Option<u64>,
    stdout_thread: Option<JoinHandle<()>>,
    stderr_thread: Option<JoinHandle<()>>,
}

impl RealIrtServer {
    pub fn start(hmac_key: Option<&[u8]>) -> Result<Self, String> {
        let irtt_bin = std::env::var("IRTT_BIN").unwrap_or_else(|_| "irtt".to_string());
        let keepalive_ms = std::env::var("IRTT_TEST_KEEP_SERVER_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok());

        let mut failures = Vec::new();

        for attempt in 1..=STARTUP_ATTEMPTS {
            let port = Self::find_free_port()?;
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            let bind = format!("127.0.0.1:{port}");

            debug_eprintln!(
                "[real_irtt] selected backend=real irtt_bin={irtt_bin} bind={bind} attempt={attempt}/{STARTUP_ATTEMPTS}"
            );

            match Self::start_attempt(&irtt_bin, keepalive_ms, hmac_key, addr, &bind) {
                Ok(server) => return Ok(server),
                Err(failure) => {
                    debug_eprintln!(
                        "[real_irtt] startup attempt {attempt}/{STARTUP_ATTEMPTS} failed for {bind}:\n{}",
                        failure.details()
                    );
                    failures.push(failure);
                }
            }
        }

        Err(format_startup_failures(&irtt_bin, &failures))
    }

    fn start_attempt(
        irtt_bin: &str,
        keepalive_ms: Option<u64>,
        hmac_key: Option<&[u8]>,
        addr: SocketAddr,
        bind: &str,
    ) -> Result<Self, StartupFailure> {
        let mut cmd = Command::new(irtt_bin);
        cmd.arg("server")
            .arg("-b")
            .arg(bind)
            .arg("--tstamp=dual")
            .stderr(Stdio::piped())
            .stdout(Stdio::piped());

        if let Some(key) = hmac_key {
            let hex_key: String = key.iter().map(|b| format!("{:02x}", b)).collect();
            cmd.arg(format!("--hmac=0x{hex_key}"));
        }

        let full_cmd = format!("{cmd:?}");
        debug_eprintln!("[real_irtt] spawning: {full_cmd}");

        let mut child = cmd.spawn().map_err(|e| StartupFailure {
            bind: bind.to_owned(),
            command: full_cmd.clone(),
            output: String::new(),
            exit_status: None,
            error: Some(format!("failed to spawn irtt server: {e}")),
        })?;

        let pid = child.id();
        debug_eprintln!("[real_irtt] child pid={pid}");

        let stdout = child.stdout.take().ok_or_else(|| StartupFailure {
            bind: bind.to_owned(),
            command: full_cmd.clone(),
            output: String::new(),
            exit_status: None,
            error: Some("failed to capture stdout".to_owned()),
        })?;
        let stderr = child.stderr.take().ok_or_else(|| StartupFailure {
            bind: bind.to_owned(),
            command: full_cmd.clone(),
            output: String::new(),
            exit_status: None,
            error: Some("failed to capture stderr".to_owned()),
        })?;

        let captured = Arc::new(Mutex::new(Vec::new()));
        let (line_tx, line_rx) = mpsc::channel();

        let stdout_thread = {
            let line_tx = line_tx.clone();
            let captured = Arc::clone(&captured);
            thread::spawn(move || drain_stream("stdout", stdout, line_tx, captured))
        };

        let stderr_thread = {
            let captured = Arc::clone(&captured);
            thread::spawn(move || drain_stream("stderr", stderr, line_tx, captured))
        };

        let ready = wait_for_ready(&line_rx, STARTUP_TIMEOUT);

        if !ready {
            let exit_status = child.try_wait().ok().flatten();
            debug_eprintln!("[real_irtt] readiness result=false exit_status={exit_status:?}");
            child.kill().ok();
            child.wait().ok();
            stdout_thread.join().ok();
            stderr_thread.join().ok();

            let output = captured.lock().unwrap().join("\n");
            return Err(StartupFailure {
                bind: bind.to_owned(),
                command: full_cmd,
                output,
                exit_status,
                error: Some("readiness timed out or server exited before ready".to_owned()),
            });
        }

        debug_eprintln!("[real_irtt] readiness result=true addr={addr}");

        Ok(Self {
            addr,
            child,
            keepalive_ms,
            stdout_thread: Some(stdout_thread),
            stderr_thread: Some(stderr_thread),
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn find_free_port() -> Result<u16, String> {
        let socket =
            UdpSocket::bind("127.0.0.1:0").map_err(|e| format!("failed to find free port: {e}"))?;
        let port = socket
            .local_addr()
            .map_err(|e| format!("failed to get local addr: {e}"))?
            .port();
        drop(socket);
        Ok(port)
    }
}

struct StartupFailure {
    bind: String,
    command: String,
    output: String,
    exit_status: Option<std::process::ExitStatus>,
    error: Option<String>,
}

impl StartupFailure {
    fn details(&self) -> String {
        let mut details = format!("bind: {}\ncommand: {}", self.bind, self.command);
        if let Some(error) = &self.error {
            details.push_str(&format!("\nerror: {error}"));
        }
        if let Some(status) = self.exit_status {
            details.push_str(&format!("\nexit status: {status}"));
        }
        if !self.output.is_empty() {
            details.push_str(&format!("\noutput:\n{}", self.output));
        }
        details
    }
}

fn format_startup_failures(irtt_bin: &str, failures: &[StartupFailure]) -> String {
    let mut message = format!(
        "irtt server failed to start after {} attempt(s) with binary '{irtt_bin}'",
        failures.len()
    );

    for (index, failure) in failures.iter().enumerate() {
        message.push_str(&format!(
            "\n\nattempt {}/{}:\n{}",
            index + 1,
            failures.len(),
            failure.details()
        ));
    }

    message
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
        if let Some(thread) = self.stdout_thread.take() {
            thread.join().ok();
        }
        if let Some(thread) = self.stderr_thread.take() {
            thread.join().ok();
        }
    }
}

fn wait_for_ready(line_rx: &mpsc::Receiver<(&'static str, String)>, timeout: Duration) -> bool {
    let start = Instant::now();
    loop {
        let remaining = timeout
            .checked_sub(start.elapsed())
            .unwrap_or(Duration::ZERO);

        match line_rx.recv_timeout(remaining) {
            Ok((stream, text)) => {
                debug_eprintln!("[real_irtt] {stream}: {text}");
                if text.contains("[ListenerStart]") {
                    return true;
                }
                if (text.contains("[ListenerStop]") || text.contains("[ServerStop]"))
                    && (text.contains("error") || text.contains("Error"))
                {
                    return false;
                }
            }
            Err(_) => return false,
        }
    }
}

fn drain_stream(
    stream: &'static str,
    pipe: impl Read,
    tx: mpsc::Sender<(&'static str, String)>,
    captured: Arc<Mutex<Vec<String>>>,
) {
    let reader = BufReader::new(pipe);
    for line in reader.lines() {
        match line {
            Ok(text) => {
                captured.lock().unwrap().push(format!("{stream}: {text}"));
                let _ = tx.send((stream, text));
            }
            Err(_) => break,
        }
    }
}

macro_rules! debug_eprintln {
    ($($arg:tt)*) => {
        if std::env::var("IRTT_TEST_BACKEND_DEBUG").as_deref() == Ok("1") {
            eprintln!($($arg)*);
        }
    };
}

use debug_eprintln;
