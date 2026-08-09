#![cfg(feature = "client")]

use std::{
    io::Read,
    net::{SocketAddr, UdpSocket},
    process::{Command, Output, Stdio},
    sync::mpsc,
    thread,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use irtt_proto::{
    echo_packet_len,
    flags::{FLAG_CLOSE, FLAG_OPEN, FLAG_REPLY},
    layout::PacketLayout,
    Clock, Params, ReceivedStats, StampAt, TimestampFields, MAGIC, PROTOCOL_VERSION,
};

const TOKEN: u64 = 0x1234_5678_90ab_cdef;

struct FakeServer {
    addr: SocketAddr,
    done: JoinHandle<()>,
}

impl FakeServer {
    fn join(self) {
        self.done.join().unwrap();
    }
}

struct InterruptibleFakeServer {
    addr: SocketAddr,
    opened: mpsc::Receiver<()>,
    done: JoinHandle<()>,
}

impl InterruptibleFakeServer {
    fn wait_until_opened(&self) {
        self.opened
            .recv_timeout(Duration::from_secs(2))
            .expect("timed out waiting for CLI to open target");
    }

    fn join(self) {
        self.done.join().unwrap();
    }
}

struct ObservedFakeServer {
    addr: SocketAddr,
    probes: mpsc::Receiver<Instant>,
    done: JoinHandle<()>,
}

impl ObservedFakeServer {
    fn join(self) -> Vec<Instant> {
        self.done.join().unwrap();
        self.probes.try_iter().collect()
    }
}

#[test]
fn list_columns_succeeds_without_target() {
    let output = Command::new(env!("CARGO_BIN_EXE_irtt-cli"))
        .arg("--list-columns")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Available event columns:"), "{stdout}");
    assert!(stdout.contains("target"), "{stdout}");
}

#[test]
fn single_target_default_table_omits_target_and_accepts_pacing() {
    let server = start_echo_server(test_params(
        Some(Duration::from_millis(30)),
        Duration::from_millis(10),
    ));

    let output = Command::new(env!("CARGO_BIN_EXE_irtt-cli"))
        .args([
            "--duration",
            "30ms",
            "--interval",
            "10ms",
            "--pacing",
            "burst",
            "--header",
            "always",
            &server.addr.to_string(),
        ])
        .output()
        .unwrap();

    server.join();

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let header = stdout.lines().next().unwrap_or_default();
    assert!(!header.split_whitespace().any(|column| column == "target"));
}

#[test]
fn single_target_custom_target_column_renders_positional_label() {
    let server = start_echo_server(test_params(
        Some(Duration::from_millis(30)),
        Duration::from_millis(10),
    ));
    let target = server.addr.to_string();

    let output = Command::new(env!("CARGO_BIN_EXE_irtt-cli"))
        .args([
            "--duration",
            "30ms",
            "--interval",
            "10ms",
            "--format",
            "csv",
            "--columns",
            "target,seq",
            "--header",
            "never",
            &target,
        ])
        .output()
        .unwrap();

    server.join();

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout
            .lines()
            .any(|line| line.starts_with(&format!("{target},"))),
        "{stdout}"
    );
}

#[test]
fn single_target_ten_millisecond_cadence_uses_managed_deadlines() {
    let duration = Duration::from_millis(90);
    let interval = Duration::from_millis(10);
    let server = start_observed_echo_server(test_params(Some(duration), interval));
    let mut command = Command::new(env!("CARGO_BIN_EXE_irtt-cli"));
    command.args([
        "--duration",
        "90ms",
        "--interval",
        "10ms",
        "--format",
        "csv",
        "--columns",
        "event,seq",
        "--header",
        "never",
        &server.addr.to_string(),
    ]);

    let output = run_with_timeout(command, Duration::from_secs(3));
    let probe_times = server.join();

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let sent_rows = stdout
        .lines()
        .filter(|line| line.starts_with("echo_sent,"))
        .count();
    assert!(
        (7..=11).contains(&sent_rows),
        "expected about 9 probes at 10 ms cadence, got {sent_rows}\n{stdout}"
    );
    assert_eq!(sent_rows, probe_times.len(), "{stdout}");
    assert!(
        !probe_times
            .windows(3)
            .any(|window| window[2].duration_since(window[0]) < Duration::from_millis(5)),
        "managed pacing emitted a catch-up burst: {probe_times:?}"
    );
    assert!(stdout
        .lines()
        .any(|line| line.starts_with("session_closed,")));
}

#[test]
fn finite_single_target_peer_close_exits_successfully() {
    let server = start_peer_close_server(test_params(
        Some(Duration::from_millis(40)),
        Duration::from_millis(10),
    ));
    let mut command = Command::new(env!("CARGO_BIN_EXE_irtt-cli"));
    command.args([
        "--duration",
        "40ms",
        "--interval",
        "10ms",
        "--format",
        "csv",
        "--columns",
        "seq",
        "--header",
        "never",
        &server.addr.to_string(),
    ]);

    let output = run_with_timeout(command, Duration::from_secs(3));
    server.join();

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stdout.trim().is_empty(), "{stdout}");
    assert!(!stderr.contains("peer closure"), "{stderr}");
}

#[test]
fn continuous_single_target_peer_close_exits_nonzero() {
    let server = start_peer_close_server(test_params(None, Duration::from_millis(10)));
    let mut command = Command::new(env!("CARGO_BIN_EXE_irtt-cli"));
    command.args([
        "--duration",
        "0s",
        "--interval",
        "10ms",
        "--format",
        "csv",
        "--columns",
        "seq",
        "--header",
        "never",
        &server.addr.to_string(),
    ]);

    let output = run_with_timeout(command, Duration::from_secs(3));
    server.join();

    assert!(
        !output.status.success(),
        "status={:?}\nstdout={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stdout.trim().is_empty(), "{stdout}");
    assert!(stderr.contains("continuous run"), "{stderr}");
    assert!(stderr.contains("peer closure"), "{stderr}");
}

#[cfg(unix)]
#[test]
fn continuous_single_target_interruption_drains_final_events_and_succeeds() {
    let params = test_params(None, Duration::from_millis(10));
    let server = start_interruptible_echo_server(params);
    let mut command = Command::new(env!("CARGO_BIN_EXE_irtt-cli"));
    command.args([
        "--duration",
        "0s",
        "--interval",
        "10ms",
        "--format",
        "csv",
        "--columns",
        "event,seq",
        "--header",
        "never",
        &server.addr.to_string(),
    ]);

    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    server.wait_until_opened();
    thread::sleep(Duration::from_millis(50));
    let signal_status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(signal_status.success());
    let output = wait_with_timeout(child, Duration::from_secs(2));
    server.join();

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stdout.lines().any(|line| line.starts_with("echo_reply,")));
    assert!(stdout
        .lines()
        .any(|line| line.starts_with("session_closed,")));
    assert!(stderr.contains("interrupted, closing managed run"), "{stderr}");
    assert!(!stderr.contains("peer closure"), "{stderr}");
}

#[test]
fn multi_target_csv_emits_rows_for_both_labels() {
    let params = test_params(Some(Duration::from_millis(40)), Duration::from_millis(10));
    let a = start_echo_server(params.clone());
    let b = start_echo_server(params);

    let output = Command::new(env!("CARGO_BIN_EXE_irtt-cli"))
        .args([
            "--duration",
            "40ms",
            "--interval",
            "10ms",
            "--format",
            "csv",
            "--columns",
            "target,seq,effective_rtt_us",
            "--header",
            "never",
            "--target",
            &format!("a={}", a.addr),
            "--target",
            &format!("b={}", b.addr),
        ])
        .output()
        .unwrap();

    a.join();
    b.join();

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.lines().any(|line| line.starts_with("a,")),
        "{stdout}"
    );
    assert!(
        stdout.lines().any(|line| line.starts_with("b,")),
        "{stdout}"
    );
}

#[test]
fn finite_multi_target_peer_close_is_accepted_as_completion() {
    let params = test_params(Some(Duration::from_millis(40)), Duration::from_millis(10));
    let a = start_peer_close_server(params.clone());
    let b = start_peer_close_server(params);
    let mut command = Command::new(env!("CARGO_BIN_EXE_irtt-cli"));
    command.args([
        "--duration",
        "40ms",
        "--interval",
        "10ms",
        "--format",
        "csv",
        "--columns",
        "target,seq",
        "--header",
        "never",
        "--target",
        &format!("a={}", a.addr),
        "--target",
        &format!("b={}", b.addr),
    ]);

    let output = run_with_timeout(command, Duration::from_secs(3));
    a.join();
    b.join();

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stdout.lines().any(|line| line.starts_with("a,")),
        "{stdout}"
    );
    assert!(
        stdout.lines().any(|line| line.starts_with("b,")),
        "{stdout}"
    );
    assert!(!stderr.contains("peer closure"), "{stderr}");
}

#[test]
fn continuous_all_peer_closed_targets_exit_nonzero() {
    let params = test_params(None, Duration::from_millis(10));
    let a = start_peer_close_server(params.clone());
    let b = start_peer_close_server(params);
    let mut command = Command::new(env!("CARGO_BIN_EXE_irtt-cli"));
    command.args([
        "--duration",
        "0s",
        "--interval",
        "10ms",
        "--format",
        "csv",
        "--columns",
        "target,seq",
        "--header",
        "never",
        "--target",
        &format!("a={}", a.addr),
        "--target",
        &format!("b={}", b.addr),
    ]);

    let output = run_with_timeout(command, Duration::from_secs(3));
    a.join();
    b.join();

    assert!(
        !output.status.success(),
        "status={:?}\nstdout={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stdout.lines().any(|line| line.starts_with("a,")),
        "{stdout}"
    );
    assert!(
        stdout.lines().any(|line| line.starts_with("b,")),
        "{stdout}"
    );
    assert!(stderr.contains("continuous run"), "{stderr}");
    assert!(
        stderr.contains("peer closure (2 target sessions)"),
        "{stderr}"
    );
}

#[test]
fn continuous_partial_peer_close_stops_promptly_and_preserves_queued_rows() {
    let params = test_params(None, Duration::from_millis(10));
    let (healthy_reply_tx, healthy_reply_rx) = mpsc::channel();
    let healthy = start_echo_server_with_first_reply(params.clone(), healthy_reply_tx);
    let peer_closed = start_gated_peer_close_server(params, vec![healthy_reply_rx]);
    let mut command = Command::new(env!("CARGO_BIN_EXE_irtt-cli"));
    command.args([
        "--duration",
        "0s",
        "--interval",
        "10ms",
        "--format",
        "csv",
        "--columns",
        "target,seq",
        "--header",
        "never",
        "--target",
        &format!("healthy={}", healthy.addr),
        "--target",
        &format!("peer={}", peer_closed.addr),
    ]);

    let started_at = Instant::now();
    let output = run_with_timeout(command, Duration::from_secs(2));
    let elapsed = started_at.elapsed();
    healthy.join();
    peer_closed.join();

    assert!(
        !output.status.success(),
        "status={:?}\nstdout={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "partial peer-close shutdown took {elapsed:?}"
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stdout.lines().any(|line| line.starts_with("healthy,")),
        "{stdout}"
    );
    assert!(
        stdout.lines().any(|line| line.starts_with("peer,")),
        "{stdout}"
    );
    assert!(
        stderr.contains("peer closure (1 target session)"),
        "{stderr}"
    );
    assert!(
        !stderr.contains("managed client group was cancelled"),
        "{stderr}"
    );
}

#[test]
fn continuous_mixed_peer_close_and_open_failure_exits_nonzero() {
    let params = test_params(None, Duration::from_millis(10));
    let (failure_tx, failure_rx) = mpsc::channel();
    let (healthy_reply_tx, healthy_reply_rx) = mpsc::channel();
    let failure = start_open_failure_server_with_signal(params.clone(), failure_tx);
    let healthy = start_echo_server_with_first_reply(params.clone(), healthy_reply_tx);
    let peer_closed = start_gated_peer_close_server(params, vec![failure_rx, healthy_reply_rx]);
    let mut command = Command::new(env!("CARGO_BIN_EXE_irtt-cli"));
    command.args([
        "--duration",
        "0s",
        "--interval",
        "10ms",
        "--format",
        "csv",
        "--columns",
        "target,seq",
        "--header",
        "never",
        "--target",
        &format!("failure={}", failure.addr),
        "--target",
        &format!("healthy={}", healthy.addr),
        "--target",
        &format!("peer={}", peer_closed.addr),
    ]);

    let output = run_with_timeout(command, Duration::from_secs(3));
    failure.join();
    healthy.join();
    peer_closed.join();

    assert!(
        !output.status.success(),
        "status={:?}\nstdout={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stdout.lines().any(|line| line.starts_with("peer,")),
        "{stdout}"
    );
    assert!(
        stdout.lines().any(|line| line.starts_with("healthy,")),
        "{stdout}"
    );
    assert!(stderr.contains("target failure failed"), "{stderr}");
    assert!(stderr.contains("zero token without close flag"), "{stderr}");
    assert!(
        stderr.contains("peer closure (1 target session)"),
        "{stderr}"
    );
}

#[cfg(unix)]
#[test]
fn explicit_group_interruption_succeeds_without_peer_close_error() {
    let params = test_params(None, Duration::from_millis(10));
    let a = start_interruptible_echo_server(params.clone());
    let b = start_interruptible_echo_server(params);
    let mut command = Command::new(env!("CARGO_BIN_EXE_irtt-cli"));
    command.args([
        "--duration",
        "0s",
        "--interval",
        "10ms",
        "--format",
        "csv",
        "--columns",
        "target,seq",
        "--header",
        "never",
        "--target",
        &format!("a={}", a.addr),
        "--target",
        &format!("b={}", b.addr),
    ]);

    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    a.wait_until_opened();
    b.wait_until_opened();
    thread::sleep(Duration::from_millis(50));
    let signal_status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(signal_status.success());
    let output = wait_with_timeout(child, Duration::from_secs(2));
    a.join();
    b.join();

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stdout.lines().any(|line| line.starts_with("a,")),
        "{stdout}"
    );
    assert!(
        stdout.lines().any(|line| line.starts_with("b,")),
        "{stdout}"
    );
    assert!(stderr.contains("interrupted, closing managed run"), "{stderr}");
    assert!(!stderr.contains("peer closure"), "{stderr}");
}

#[test]
fn all_open_failures_exit_nonzero_with_diagnostics() {
    let params = test_params(None, Duration::from_millis(10));
    let a = start_open_failure_server(params.clone());
    let b = start_open_failure_server(params);
    let mut command = Command::new(env!("CARGO_BIN_EXE_irtt-cli"));
    command.args([
        "--duration",
        "0s",
        "--interval",
        "10ms",
        "--target",
        &format!("a={}", a.addr),
        "--target",
        &format!("b={}", b.addr),
    ]);

    let output = run_with_timeout(command, Duration::from_secs(3));
    a.join();
    b.join();

    assert!(
        !output.status.success(),
        "status={:?}\nstdout={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("target a failed"), "{stderr}");
    assert!(stderr.contains("target b failed"), "{stderr}");
    assert!(
        stderr.contains("no managed target completed successfully"),
        "{stderr}"
    );
}

fn run_with_timeout(mut command: Command, timeout: Duration) -> Output {
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_with_timeout(child, timeout)
}

fn wait_with_timeout(mut child: std::process::Child, timeout: Duration) -> Output {
    let deadline = Instant::now() + timeout;
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            if let Some(mut pipe) = child.stdout.take() {
                let _ = pipe.read_to_end(&mut stdout);
            }
            if let Some(mut pipe) = child.stderr.take() {
                let _ = pipe.read_to_end(&mut stderr);
            }
            let _ = child.wait();
            panic!(
                "CLI timed out after {timeout:?}\nstdout={}\nstderr={}",
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            );
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn start_echo_server(params: Params) -> FakeServer {
    start_echo_server_inner(params, None)
}

fn start_echo_server_with_first_reply(
    params: Params,
    first_reply_tx: mpsc::Sender<()>,
) -> FakeServer {
    start_echo_server_inner(params, Some(first_reply_tx))
}

fn start_echo_server_inner(
    params: Params,
    mut first_reply_tx: Option<mpsc::Sender<()>>,
) -> FakeServer {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let addr = socket.local_addr().unwrap();
    let done = thread::spawn(move || {
        let (_, peer) = recv_request(&socket);
        socket
            .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
            .unwrap();

        loop {
            let (packet, peer) = recv_request(&socket);
            let flags = packet[3];
            if flags & FLAG_CLOSE != 0 {
                break;
            }
            let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
            socket
                .send_to(
                    &echo_reply_packet(
                        TOKEN,
                        seq,
                        &params,
                        &TimestampFields::default(),
                        FLAG_REPLY,
                    ),
                    peer,
                )
                .unwrap();
            if let Some(tx) = first_reply_tx.take() {
                tx.send(()).unwrap();
            }
        }
    });
    FakeServer { addr, done }
}

fn start_observed_echo_server(params: Params) -> ObservedFakeServer {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let addr = socket.local_addr().unwrap();
    let (probe_tx, probes) = mpsc::channel();
    let done = thread::spawn(move || {
        let (_, peer) = recv_request(&socket);
        socket
            .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
            .unwrap();

        loop {
            let (packet, peer) = recv_request(&socket);
            let flags = packet[3];
            if flags & FLAG_CLOSE != 0 {
                break;
            }
            probe_tx.send(Instant::now()).unwrap();
            let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
            socket
                .send_to(
                    &echo_reply_packet(
                        TOKEN,
                        seq,
                        &params,
                        &TimestampFields::default(),
                        FLAG_REPLY,
                    ),
                    peer,
                )
                .unwrap();
        }
    });
    ObservedFakeServer { addr, probes, done }
}

fn start_interruptible_echo_server(params: Params) -> InterruptibleFakeServer {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let addr = socket.local_addr().unwrap();
    let (opened_tx, opened) = mpsc::channel();
    let done = thread::spawn(move || {
        let (_, peer) = recv_request(&socket);
        opened_tx.send(()).unwrap();
        socket
            .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
            .unwrap();
        socket
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();

        while let Some((packet, peer)) = recv_request_timeout(&socket) {
            let flags = packet[3];
            if flags & FLAG_CLOSE != 0 {
                break;
            }
            let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
            socket
                .send_to(
                    &echo_reply_packet(
                        TOKEN,
                        seq,
                        &params,
                        &TimestampFields::default(),
                        FLAG_REPLY,
                    ),
                    peer,
                )
                .unwrap();
        }
    });
    InterruptibleFakeServer { addr, opened, done }
}

fn start_peer_close_server(params: Params) -> FakeServer {
    start_gated_peer_close_server(params, Vec::new())
}

fn start_gated_peer_close_server(
    params: Params,
    release_gates: Vec<mpsc::Receiver<()>>,
) -> FakeServer {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let addr = socket.local_addr().unwrap();
    let done = thread::spawn(move || {
        let (_, peer) = recv_request(&socket);
        socket
            .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
            .unwrap();
        let (packet, peer) = recv_request(&socket);
        for gate in release_gates {
            gate.recv_timeout(Duration::from_secs(2))
                .expect("timed out waiting to release peer-close reply");
        }
        let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
        socket
            .send_to(
                &echo_reply_packet(
                    TOKEN,
                    seq,
                    &params,
                    &TimestampFields::default(),
                    FLAG_REPLY | FLAG_CLOSE,
                ),
                peer,
            )
            .unwrap();
    });
    FakeServer { addr, done }
}

fn start_open_failure_server(params: Params) -> FakeServer {
    start_open_failure_server_inner(params, None)
}

fn start_open_failure_server_with_signal(
    params: Params,
    failure_tx: mpsc::Sender<()>,
) -> FakeServer {
    start_open_failure_server_inner(params, Some(failure_tx))
}

fn start_open_failure_server_inner(
    params: Params,
    failure_tx: Option<mpsc::Sender<()>>,
) -> FakeServer {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let addr = socket.local_addr().unwrap();
    let done = thread::spawn(move || {
        let (_, peer) = recv_request(&socket);
        socket
            .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, 0, &params), peer)
            .unwrap();
        if let Some(tx) = failure_tx {
            tx.send(()).unwrap();
        }
    });
    FakeServer { addr, done }
}

fn recv_request(socket: &UdpSocket) -> (Vec<u8>, SocketAddr) {
    let mut buf = [0_u8; 2048];
    let (size, peer) = socket.recv_from(&mut buf).unwrap();
    (buf[..size].to_vec(), peer)
}

fn recv_request_timeout(socket: &UdpSocket) -> Option<(Vec<u8>, SocketAddr)> {
    let mut buf = [0_u8; 2048];
    socket
        .recv_from(&mut buf)
        .ok()
        .map(|(size, peer)| (buf[..size].to_vec(), peer))
}

fn open_reply(flags: u8, token: u64, params: &Params) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.extend_from_slice(&MAGIC);
    packet.push(flags);
    packet.extend_from_slice(&token.to_le_bytes());
    packet.extend_from_slice(&params.encode());
    packet
}

fn echo_reply_packet(
    token: u64,
    seq: u32,
    params: &Params,
    timestamps: &TimestampFields,
    flags: u8,
) -> Vec<u8> {
    let layout = PacketLayout::echo(false, params);
    let packet_len = echo_packet_len(false, params).unwrap();
    let mut packet = Vec::with_capacity(packet_len);

    packet.extend_from_slice(&MAGIC);
    packet.push(flags);
    packet.extend_from_slice(&token.to_le_bytes());
    packet.extend_from_slice(&seq.to_le_bytes());

    if layout.recv_count {
        packet.extend_from_slice(&42_u32.to_le_bytes());
    }
    if layout.recv_window {
        packet.extend_from_slice(&0_u64.to_le_bytes());
    }
    if layout.recv_wall {
        packet.extend_from_slice(&timestamps.recv_wall.unwrap_or(0).to_le_bytes());
    }
    if layout.recv_mono {
        packet.extend_from_slice(&timestamps.recv_mono.unwrap_or(0).to_le_bytes());
    }
    if layout.midpoint_wall {
        packet.extend_from_slice(&timestamps.midpoint_wall.unwrap_or(0).to_le_bytes());
    }
    if layout.midpoint_mono {
        packet.extend_from_slice(&timestamps.midpoint_mono.unwrap_or(0).to_le_bytes());
    }
    if layout.send_wall {
        packet.extend_from_slice(&timestamps.send_wall.unwrap_or(0).to_le_bytes());
    }
    if layout.send_mono {
        packet.extend_from_slice(&timestamps.send_mono.unwrap_or(0).to_le_bytes());
    }

    packet.resize(packet_len, 0);
    packet
}

fn test_params(duration: Option<Duration>, interval: Duration) -> Params {
    Params {
        protocol_version: PROTOCOL_VERSION,
        duration_ns: duration.map_or(0, duration_ns_i64),
        interval_ns: duration_ns_i64(interval),
        length: 0,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        dscp: 0,
        server_fill: None,
    }
}

fn duration_ns_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap()
}
