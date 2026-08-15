#![cfg(feature = "client")]

#[path = "support/in_tree_server.rs"]
mod in_tree_server;

use std::{
    io::Read,
    net::{SocketAddr, UdpSocket},
    process::{Command, Output, Stdio},
    sync::mpsc,
    thread,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use in_tree_server::InTreeServer;
use irtt_proto::{
    echo_packet_len, encode_echo_reply, encode_open_reply,
    flags::{FLAG_CLOSE, FLAG_OPEN, FLAG_REPLY},
    layout::PacketLayout,
    Clock, EchoReply, OpenReply, Params, ReceivedStats, StampAt, TimestampFields, MAGIC,
    PROTOCOL_VERSION,
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
    first_reply_sent: mpsc::Receiver<()>,
    done: JoinHandle<()>,
}

impl InterruptibleFakeServer {
    fn wait_until_first_reply_sent(&self) {
        self.first_reply_sent
            .recv_timeout(Duration::from_secs(2))
            .expect("timed out waiting for fake server to send its first ECHO reply");
    }

    fn join(self) {
        self.done.join().unwrap();
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
fn single_target_default_table_includes_target_and_accepts_pacing() {
    let server = InTreeServer::start();
    let target = server.addr.to_string();

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
            &target,
        ])
        .output()
        .unwrap();

    drop(server);

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut lines = stdout.lines();
    let header = lines.next().unwrap_or_default();
    assert_eq!(header.split_whitespace().next(), Some("target"));
    assert!(
        lines.any(|line| line.split_whitespace().next() == Some(target.as_str())),
        "{stdout}"
    );
}

#[test]
fn single_labeled_target_default_table_includes_target() {
    let server = InTreeServer::start();

    let output = Command::new(env!("CARGO_BIN_EXE_irtt-cli"))
        .args([
            "--duration",
            "30ms",
            "--interval",
            "10ms",
            "--header",
            "always",
            "--target",
            &format!("eu={}", server.addr),
        ])
        .output()
        .unwrap();

    drop(server);

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut lines = stdout.lines();
    let header = lines.next().unwrap_or_default();
    assert_eq!(header.split_whitespace().next(), Some("target"));
    assert!(
        lines.any(|line| line.split_whitespace().next() == Some("eu")),
        "{stdout}"
    );
}

#[test]
fn single_labeled_target_default_csv_includes_target_and_event_wall_ns() {
    let server = InTreeServer::start();

    let output = Command::new(env!("CARGO_BIN_EXE_irtt-cli"))
        .args([
            "--duration",
            "30ms",
            "--interval",
            "10ms",
            "--format",
            "csv",
            "--header",
            "always",
            "--target",
            &format!("eu={}", server.addr),
        ])
        .output()
        .unwrap();

    drop(server);

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut lines = stdout.lines();
    let header = lines.next().unwrap_or_default();
    assert!(header.starts_with("target,event,"), "{header}");
    assert!(
        header.split(',').any(|column| column == "event_wall_ns"),
        "{header}"
    );
    assert!(lines.any(|line| line.starts_with("eu,")), "{stdout}");
}

#[test]
fn single_positional_target_default_csv_includes_target_as_positional_label() {
    let server = InTreeServer::start();
    let target = server.addr.to_string();

    let output = Command::new(env!("CARGO_BIN_EXE_irtt-cli"))
        .args([
            "--duration",
            "30ms",
            "--interval",
            "10ms",
            "--format",
            "csv",
            "--header",
            "always",
            &target,
        ])
        .output()
        .unwrap();

    drop(server);

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut lines = stdout.lines();
    let header = lines.next().unwrap_or_default();
    assert!(header.starts_with("target,event,"), "{header}");
    assert!(
        lines.any(|line| line.starts_with(&format!("{target},"))),
        "{stdout}"
    );
}

#[test]
fn multi_target_default_table_includes_both_labels() {
    let a = InTreeServer::start();
    let b = InTreeServer::start();

    let output = Command::new(env!("CARGO_BIN_EXE_irtt-cli"))
        .args([
            "--duration",
            "40ms",
            "--interval",
            "10ms",
            "--header",
            "always",
            "--target",
            &format!("a={}", a.addr),
            "--target",
            &format!("b={}", b.addr),
        ])
        .output()
        .unwrap();

    drop(a);
    drop(b);

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut lines = stdout.lines();
    let header = lines.next().unwrap_or_default();
    assert_eq!(header.split_whitespace().next(), Some("target"));
    let rows: Vec<&str> = lines.collect();
    assert!(
        rows.iter()
            .any(|line| line.split_whitespace().next() == Some("a")),
        "{stdout}"
    );
    assert!(
        rows.iter()
            .any(|line| line.split_whitespace().next() == Some("b")),
        "{stdout}"
    );
}

#[test]
fn single_target_custom_target_column_renders_positional_label() {
    let server = InTreeServer::start();
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

    drop(server);

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
fn custom_columns_remain_authoritative_and_omit_target() {
    let server = InTreeServer::start();

    let output = Command::new(env!("CARGO_BIN_EXE_irtt-cli"))
        .args([
            "--duration",
            "30ms",
            "--interval",
            "10ms",
            "--format",
            "csv",
            "--columns",
            "seq,rtt",
            "--header",
            "always",
            &server.addr.to_string(),
        ])
        .output()
        .unwrap();

    drop(server);

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let header = stdout.lines().next().unwrap_or_default();
    assert_eq!(header, "seq,rtt");
}

#[test]
fn columns_default_keyword_includes_target_for_single_target() {
    let server = InTreeServer::start();

    let output = Command::new(env!("CARGO_BIN_EXE_irtt-cli"))
        .args([
            "--duration",
            "30ms",
            "--interval",
            "10ms",
            "--format",
            "csv",
            "--columns",
            "default",
            "--header",
            "always",
            "--target",
            &format!("eu={}", server.addr),
        ])
        .output()
        .unwrap();

    drop(server);

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut lines = stdout.lines();
    let header = lines.next().unwrap_or_default();
    assert!(header.starts_with("target,event,"), "{header}");
    assert!(lines.any(|line| line.starts_with("eu,")), "{stdout}");
}

#[test]
fn single_labeled_target_default_jsonl_includes_target() {
    let server = InTreeServer::start();

    let output = Command::new(env!("CARGO_BIN_EXE_irtt-cli"))
        .args([
            "--duration",
            "30ms",
            "--interval",
            "10ms",
            "--format",
            "jsonl",
            "--target",
            &format!("eu={}", server.addr),
        ])
        .output()
        .unwrap();

    drop(server);

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
            .any(|line| line.contains("\"target\":\"eu\"")),
        "{stdout}"
    );
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
    // Only wait for the reply to be sent, not consumed: whether the client
    // has already read it or it is still in flight when SIGINT lands is the
    // scenario this test exercises. The echo_reply assertion below is what
    // proves shutdown drains a reply that was still queued at interrupt time
    // rather than losing it.
    server.wait_until_first_reply_sent();
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
    assert!(
        stderr.contains("interrupted, closing managed run"),
        "{stderr}"
    );
    assert!(!stderr.contains("peer closure"), "{stderr}");
}

#[test]
fn multi_target_csv_emits_rows_for_both_labels() {
    let a = InTreeServer::start();
    let b = InTreeServer::start();

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

    drop(a);
    drop(b);

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
    let (a_seen_tx, a_seen_rx) = mpsc::channel();
    let (a_release_tx, a_release_rx) = mpsc::channel();
    let (b_seen_tx, b_seen_rx) = mpsc::channel();
    let (b_release_tx, b_release_rx) = mpsc::channel();
    let a = start_synchronized_peer_close_server(params.clone(), a_seen_tx, a_release_rx);
    let b = start_synchronized_peer_close_server(params, b_seen_tx, b_release_rx);
    let coordinator = thread::spawn(move || {
        a_seen_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("timed out waiting for target A's first probe");
        b_seen_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("timed out waiting for target B's first probe");
        a_release_tx
            .send(())
            .expect("target A stopped waiting for peer-close release");
        b_release_tx
            .send(())
            .expect("target B stopped waiting for peer-close release");
    });
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
    coordinator.join().unwrap();
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
fn continuous_partial_peer_close_preserves_queued_rows_and_reports_peer_close() {
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

    let output = run_with_timeout(command, Duration::from_secs(2));
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
    a.wait_until_first_reply_sent();
    b.wait_until_first_reply_sent();
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
    assert!(
        stderr.contains("interrupted, closing managed run"),
        "{stderr}"
    );
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

fn start_echo_server_with_first_reply(
    params: Params,
    first_reply_tx: mpsc::Sender<()>,
) -> FakeServer {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let addr = socket.local_addr().unwrap();
    let done = thread::spawn(move || {
        let mut first_reply_tx = Some(first_reply_tx);
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
                    &echo_reply_fixture(
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

fn start_interruptible_echo_server(params: Params) -> InterruptibleFakeServer {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let addr = socket.local_addr().unwrap();
    let (first_reply_tx, first_reply_sent) = mpsc::channel();
    let done = thread::spawn(move || {
        let (_, peer) = recv_request(&socket);
        socket
            .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
            .unwrap();

        // Keep the longer read timeout until the first probe has actually
        // arrived: a loaded CI worker can leave the CLI descheduled well past
        // 500ms before it sends its first probe. Only shorten the timeout
        // afterward, to bound how long this thread waits for further probes
        // once the test moves on to shutdown.
        let (packet, peer) = recv_request(&socket);
        socket
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();

        let mut next = Some((packet, peer));
        let mut first_reply_tx = Some(first_reply_tx);
        while let Some((packet, peer)) = next.take().or_else(|| recv_request_timeout(&socket)) {
            let flags = packet[3];
            if flags & FLAG_CLOSE != 0 {
                break;
            }
            let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
            socket
                .send_to(
                    &echo_reply_fixture(
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
    InterruptibleFakeServer {
        addr,
        first_reply_sent,
        done,
    }
}

fn start_peer_close_server(params: Params) -> FakeServer {
    start_gated_peer_close_server(params, Vec::new())
}

fn start_synchronized_peer_close_server(
    params: Params,
    first_probe_seen_tx: mpsc::Sender<()>,
    release_peer_close_rx: mpsc::Receiver<()>,
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
        let flags = packet[3];
        assert_eq!(
            flags, 0,
            "target closed before synchronized first peer-close probe"
        );
        assert_eq!(
            packet.len(),
            echo_packet_len(false, &params).unwrap(),
            "expected a complete echo probe before synchronized peer-close reply"
        );
        let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
        first_probe_seen_tx
            .send(())
            .expect("peer-close coordinator stopped waiting for first probe");
        release_peer_close_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("timed out waiting to release synchronized peer-close reply");
        socket
            .send_to(
                &echo_reply_fixture(
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
        let flags = packet[3];
        if flags & FLAG_CLOSE != 0 {
            return;
        }
        for gate in release_gates {
            gate.recv_timeout(Duration::from_secs(2))
                .expect("timed out waiting to release peer-close reply");
        }
        assert_eq!(
            packet.len(),
            echo_packet_len(false, &params).unwrap(),
            "expected a complete echo probe before peer-close reply"
        );
        let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
        socket
            .send_to(
                &echo_reply_fixture(
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
            .send_to(&invalid_zero_token_open_reply(&params), peer)
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
    encode_open_reply(
        &OpenReply {
            flags,
            token,
            params: params.clone(),
        },
        None,
    )
    .unwrap()
}

/// Intentionally malformed: a zero-token OPEN|REPLY without FLAG_CLOSE, which
/// `irtt_proto::encode_open_reply` correctly rejects (`ProtoError::ZeroToken`).
/// Built by hand because this exercises non-compliant peer behavior.
fn invalid_zero_token_open_reply(params: &Params) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.extend_from_slice(&MAGIC);
    packet.push(FLAG_OPEN | FLAG_REPLY);
    packet.extend_from_slice(&0_u64.to_le_bytes());
    packet.extend_from_slice(&params.encode());
    packet
}

fn echo_reply_fixture(
    token: u64,
    seq: u32,
    params: &Params,
    timestamps: &TimestampFields,
    flags: u8,
) -> Vec<u8> {
    let layout = PacketLayout::echo(false, params);
    let reply = EchoReply {
        flags,
        token,
        sequence: seq,
        recv_count: layout.recv_count.then_some(42),
        recv_window: layout.recv_window.then_some(0),
        timestamps: TimestampFields {
            recv_wall: layout.recv_wall.then(|| timestamps.recv_wall.unwrap_or(0)),
            recv_mono: layout.recv_mono.then(|| timestamps.recv_mono.unwrap_or(0)),
            midpoint_wall: layout
                .midpoint_wall
                .then(|| timestamps.midpoint_wall.unwrap_or(0)),
            midpoint_mono: layout
                .midpoint_mono
                .then(|| timestamps.midpoint_mono.unwrap_or(0)),
            send_wall: layout.send_wall.then(|| timestamps.send_wall.unwrap_or(0)),
            send_mono: layout.send_mono.then(|| timestamps.send_mono.unwrap_or(0)),
        },
        payload: Vec::new(),
    };
    encode_echo_reply(&reply, params, None).unwrap()
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
