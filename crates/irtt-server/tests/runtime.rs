use std::{net::Ipv4Addr, time::Duration};

use irtt_proto::{
    decode_echo_reply, decode_open_reply, encode_request, Clock, Params, ReceivedStats,
    RequestToEncode, StampAt,
};
use tokio::{net::UdpSocket, sync::oneshot, time::timeout};

use irtt_server::{Server, ServerConfig, ServerRuntimeError};
use std::{io, net::SocketAddr};

#[tokio::test(flavor = "current_thread")]
async fn bind_reports_local_address_and_shutdown_stops_run() {
    let requested = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let mut server = Server::bind(requested, ServerConfig::default())
        .await
        .unwrap();
    let bound = server.local_addr().unwrap();
    assert_eq!(bound.ip(), requested.ip());
    assert_ne!(bound.port(), 0);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    shutdown_tx.send(()).unwrap();
    server
        .run(async {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn ipv4_open_echo_close_crosses_a_real_udp_socket() {
    exercise_udp_path(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await;
}

#[tokio::test(flavor = "current_thread")]
async fn ipv6_open_echo_close_when_loopback_is_available() {
    let addr = "[::1]:0".parse().unwrap();
    let server = match Server::bind(addr, unthrottled()).await {
        Ok(server) => server,
        Err(ServerRuntimeError::Bind { source, .. })
            if matches!(
                source.kind(),
                io::ErrorKind::AddrNotAvailable | io::ErrorKind::Unsupported
            ) =>
        {
            return;
        }
        Err(error) => panic!("unexpected IPv6 loopback bind failure: {error}"),
    };
    exercise_bound_server(server).await;
}

async fn exercise_udp_path(addr: SocketAddr) {
    let server = Server::bind(addr, unthrottled()).await.unwrap();
    exercise_bound_server(server).await;
}

async fn exercise_bound_server(mut server: Server) {
    let server_addr = server.local_addr().unwrap();
    let client_bind = match server_addr {
        SocketAddr::V4(_) => "127.0.0.1:0",
        SocketAddr::V6(_) => "[::1]:0",
    };
    let client = UdpSocket::bind(client_bind).await.unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        server
            .run(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let requested = Params {
        protocol_version: 1,
        duration_ns: 1_000_000_000,
        interval_ns: 100_000_000,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    };
    let open = encode_request(
        RequestToEncode::Open {
            params: &requested,
            no_test: false,
        },
        None,
    )
    .unwrap();
    client.send_to(&open, server_addr).await.unwrap();

    let mut buffer = vec![0; 65_536];
    let (len, reply_source) = timeout(Duration::from_secs(1), client.recv_from(&mut buffer))
        .await
        .expect("OPEN reply timeout")
        .unwrap();
    assert_eq!(reply_source, server_addr);
    let open_reply = decode_open_reply(&buffer[..len], None).unwrap();
    assert_ne!(open_reply.token, 0);

    let echo = encode_request(
        RequestToEncode::Echo {
            token: open_reply.token,
            sequence: 0,
            params: &open_reply.params,
            payload: &[],
        },
        None,
    )
    .unwrap();
    client.send_to(&echo, server_addr).await.unwrap();
    let (len, reply_source) = timeout(Duration::from_secs(1), client.recv_from(&mut buffer))
        .await
        .expect("ECHO reply timeout")
        .unwrap();
    assert_eq!(reply_source, server_addr);
    let echo_reply = decode_echo_reply(&buffer[..len], &open_reply.params, None).unwrap();
    assert_eq!(echo_reply.token, open_reply.token);
    assert_eq!(echo_reply.sequence, 0);
    assert_eq!(echo_reply.recv_count, Some(1));
    assert_eq!(echo_reply.recv_window, Some(1));

    let close = encode_request(
        RequestToEncode::Close {
            token: open_reply.token,
        },
        None,
    )
    .unwrap();
    client.send_to(&close, server_addr).await.unwrap();
    client.send_to(&echo, server_addr).await.unwrap();
    assert!(
        timeout(Duration::from_millis(100), client.recv_from(&mut buffer))
            .await
            .is_err()
    );

    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();
}

fn unthrottled() -> ServerConfig {
    ServerConfig::default().with_min_send_interval(Duration::ZERO)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd")))]
#[tokio::test(flavor = "current_thread")]
async fn wildcard_construction_is_refused_without_reply_source_selection() {
    let addr = "0.0.0.0:0".parse().unwrap();
    assert!(matches!(
        Server::bind(addr, unthrottled()).await,
        Err(ServerRuntimeError::WildcardSourceSelectionUnsupported { .. })
    ));
}

// A current-thread runtime cannot receive while this test holds its thread.
// Queue real packets before delaying the server: no synthetic ancillary data or
// transport hooks. Observed queue timing determines whether kernel wall time
// is plausible under the one-second bound, including scheduler overshoot. Monotonic and
// midpoint timestamps must always remain at the later userspace observation.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn queued_echoes_select_kernel_receive_wall_only_when_plausible() {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ns = || {
        i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap()
    };
    for (placement, delay) in [
        (StampAt::Both, Duration::from_millis(100)),
        (StampAt::Both, Duration::from_millis(1100)),
        (StampAt::Midpoint, Duration::from_millis(100)),
    ] {
        let mut server = Server::bind("127.0.0.1:0".parse().unwrap(), unthrottled())
            .await
            .unwrap();
        let addr = server.local_addr().unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.connect(addr).await.unwrap();
        let (stop_tx, stop_rx) = oneshot::channel();
        let run = tokio::spawn(async move {
            server
                .run(async {
                    let _ = stop_rx.await;
                })
                .await
        });
        let params = Params {
            stamp_at: placement,
            clock: Clock::Both,
            received_stats: ReceivedStats::Both,
            ..Params::with_protocol_defaults()
        };
        let open = encode_request(
            RequestToEncode::Open {
                params: &params,
                no_test: false,
            },
            None,
        )
        .unwrap();
        client.send(&open).await.unwrap();
        let mut buffer = [0; 2048];
        let len = timeout(Duration::from_secs(2), client.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        let opened = decode_open_reply(&buffer[..len], None).unwrap();
        let echo = encode_request(
            RequestToEncode::Echo {
                token: opened.token,
                sequence: 0,
                params: &opened.params,
                payload: &[],
            },
            None,
        )
        .unwrap();
        client.writable().await.unwrap();
        let before_send = ns();
        client.try_send(&echo).unwrap(); // No await from submission through resume marker.
        let queued_by = ns();
        std::thread::sleep(delay);
        let resumed_at = ns();
        let len = timeout(Duration::from_secs(2), client.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        let reply = decode_echo_reply(&buffer[..len], &opened.params, None).unwrap();
        assert_eq!(reply.recv_count, Some(1));
        assert_eq!(reply.recv_window, Some(1));
        let timestamps = reply.timestamps;
        if placement == StampAt::Both {
            let receive = timestamps.recv_wall.unwrap();
            let send = timestamps.send_wall.unwrap();
            let kernel_endpoint = (before_send..=queued_by).contains(&receive);
            let userspace_endpoint = (resumed_at..=send).contains(&receive);
            // Arrival lies in [before_send, queued_by], and core observation
            // lies in [resumed_at, send]. Only demand one selection where these
            // observed bounds put the whole lag interval on one side of 1 s.
            // Scheduler overshoot can legitimately change the expected path.
            if send - before_send <= 1_000_000_000 {
                assert!(
                    kernel_endpoint,
                    "plausible arrival must select kernel wall time"
                );
            } else if resumed_at - queued_by > 1_000_000_000 {
                assert!(
                    userspace_endpoint,
                    "stale arrival must fall back to userspace"
                );
            } else {
                assert!(
                    kernel_endpoint || userspace_endpoint,
                    "boundary observation must identify a real endpoint"
                );
            }
            if kernel_endpoint {
                let wall_elapsed = send - receive;
                let mono_elapsed = timestamps.send_mono.unwrap() - timestamps.recv_mono.unwrap();
                assert!(
                    wall_elapsed - mono_elapsed >= 50_000_000,
                    "queue delay must not move the monotonic endpoint"
                );
            }
        } else {
            assert!(
                timestamps.midpoint_wall.unwrap() >= resumed_at,
                "midpoint must use userspace time"
            );
        }
        stop_tx.send(()).unwrap();
        run.await.unwrap().unwrap();
    }
}
