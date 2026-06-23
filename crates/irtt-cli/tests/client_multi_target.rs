#![cfg(feature = "client")]

use std::{
    net::{SocketAddr, UdpSocket},
    process::Command,
    thread,
    thread::JoinHandle,
    time::Duration,
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

fn start_echo_server(params: Params) -> FakeServer {
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
        }
    });
    FakeServer { addr, done }
}

fn recv_request(socket: &UdpSocket) -> (Vec<u8>, SocketAddr) {
    let mut buf = [0_u8; 2048];
    let (size, peer) = socket.recv_from(&mut buf).unwrap();
    (buf[..size].to_vec(), peer)
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
