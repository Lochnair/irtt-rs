//! Observe the emitted IP byte, independently of protocol negotiation.
#![cfg(target_os = "linux")]

use std::{io::IoSliceMut, net::UdpSocket as StdSocket, os::fd::AsRawFd, time::Duration};

use irtt_proto::{
    decode_echo_reply, decode_open_reply, encode_request, OpenReply, Params, RequestToEncode,
    FLAG_CLOSE,
};
use irtt_server::{Server, ServerConfig};
use nix::sys::socket::{recvmsg, ControlMessageOwned, MsgFlags};
use tokio::{net::UdpSocket, sync::oneshot, time::timeout};

async fn exchange(client: &UdpSocket, request: &[u8], expected: u8) -> Vec<u8> {
    client.send(request).await.unwrap();
    loop {
        client.readable().await.unwrap();
        let packet = client.try_io(tokio::io::Interest::READABLE, || {
            let mut bytes = [0; 2048];
            let mut control = nix::cmsg_space!(u8);
            let mut iov = [IoSliceMut::new(&mut bytes)];
            let msg = recvmsg::<()>(
                client.as_raw_fd(),
                &mut iov,
                Some(&mut control),
                MsgFlags::MSG_DONTWAIT,
            )?;
            let size = msg.bytes;
            let class = msg.cmsgs().unwrap().find_map(|message| match message {
                ControlMessageOwned::Ipv4Tos(class) => Some(class),
                _ => None,
            });
            assert_eq!(class, Some(expected));
            Ok(bytes[..size].to_vec())
        });
        match packet {
            Ok(packet) => return packet,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("receive failed: {error}"),
        }
    }
}

fn echo(opened: &OpenReply, sequence: u32) -> Vec<u8> {
    encode_request(
        RequestToEncode::Echo {
            token: opened.token,
            sequence,
            params: &opened.params,
            payload: &[],
        },
        None,
    )
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn every_open_echo_and_server_close_applies_its_own_raw_traffic_class() {
    timeout(Duration::from_secs(8), async {
        let maximum = Duration::from_millis(1);
        let mut server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            ServerConfig::default()
                .with_min_send_interval(Duration::ZERO)
                .with_idle_timeout(Duration::MAX)
                .with_max_test_duration(maximum),
        )
        .await
        .unwrap();
        let addr = server.local_addr().unwrap();
        let (shutdown, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            server
                .run(async {
                    let _ = stopped.await;
                })
                .await
        });
        let socket = StdSocket::bind("127.0.0.1:0").unwrap();
        socket2::SockRef::from(&socket)
            .set_recv_tos_v4(true)
            .unwrap();
        socket.set_nonblocking(true).unwrap();
        let client = UdpSocket::from_std(socket).unwrap();
        client.connect(addr).await.unwrap();
        let mut sessions = Vec::new();
        // Each new OPEN follows a prior ECHO. The first raw byte also includes
        // ECN bits: neither a codepoint shift nor masking would preserve it.
        for (requested, class) in [(185, 185), (32, 32), (0, 0), (256, 0), (-1, 0)] {
            let params = Params {
                dscp: requested,
                ..Params::with_protocol_defaults()
            };
            let request = encode_request(
                RequestToEncode::Open {
                    params: &params,
                    no_test: false,
                },
                None,
            )
            .unwrap();
            let packet = exchange(&client, &request, 0).await;
            let opened = decode_open_reply(&packet, None).unwrap();
            assert_eq!(opened.params.dscp, requested);
            let packet = exchange(&client, &echo(&opened, 0), class).await;
            assert_eq!(
                decode_echo_reply(&packet, &opened.params, None)
                    .unwrap()
                    .flags
                    & FLAG_CLOSE,
                0
            );
            sessions.push((opened, class));
        }
        // Every session has now served its first echo. Waiting past all duration
        // deadlines is safe under oversleep; idle expiry cannot intervene.
        tokio::time::sleep(maximum + Duration::from_secs(2)).await;
        for (opened, class) in sessions {
            let packet = exchange(&client, &echo(&opened, 1), class).await;
            assert_ne!(
                decode_echo_reply(&packet, &opened.params, None)
                    .unwrap()
                    .flags
                    & FLAG_CLOSE,
                0
            );
        }
        shutdown.send(()).unwrap();
        task.await.unwrap().unwrap();
    })
    .await
    .expect("marked runtime exchanges did not complete");
}
