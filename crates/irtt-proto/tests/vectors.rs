use irtt_proto::{
    decode_echo_reply, decode_open_reply, encode_close_request, encode_echo_request,
    encode_open_request, verify_hmac, Clock, CloseRequest, EchoRequest, OpenRequest, Params,
    ReceivedStats, StampAt,
};

fn hex(input: &str) -> Vec<u8> {
    input
        .split_whitespace()
        .map(|byte| u8::from_str_radix(byte, 16).unwrap())
        .collect()
}

fn default_params_3s() -> Params {
    Params {
        protocol_version: 1,
        duration_ns: 3_000_000_000,
        interval_ns: 1_000_000_000,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    }
}

fn default_params_60s() -> Params {
    Params {
        duration_ns: 60_000_000_000,
        ..default_params_3s()
    }
}

fn hmac_params_2s() -> Params {
    Params {
        duration_ns: 2_000_000_000,
        ..default_params_3s()
    }
}

#[test]
fn vector_1_open_request_no_hmac() {
    let expected = hex("14 a7 5b 01 01 02 02 80 f8 82 ad 16 03 80 a8 d6
         b9 07 05 06 06 06 07 06");
    let packet = encode_open_request(
        &OpenRequest {
            params: default_params_3s(),
            close: false,
        },
        None,
    )
    .unwrap();
    assert_eq!(packet, expected);
}

#[test]
fn vector_2_open_reply_no_hmac() {
    let packet = hex("14 a7 5b 03 13 52 71 87 ab b6 96 78 01 02 02 80
         f8 82 ad 16 03 80 a8 d6 b9 07 05 06 06 06 07 06");
    let reply = decode_open_reply(&packet, None).unwrap();
    assert_eq!(reply.token, 0x7896_b6ab_8771_5213);
    assert_eq!(reply.params, default_params_3s());
}

#[test]
fn vector_3_echo_request_no_hmac() {
    let expected = hex("14 a7 5b 00 13 52 71 87 ab b6 96 78 00 00 00 00
         00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
         00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
         00 00 00 00 00 00 00 00 00 00 00 00");
    let packet = encode_echo_request(
        &EchoRequest {
            token: 0x7896_b6ab_8771_5213,
            sequence: 0,
            params: default_params_3s(),
            payload: Vec::new(),
        },
        None,
    )
    .unwrap();
    assert_eq!(packet, expected);
}

#[test]
fn vector_4_echo_reply_no_hmac() {
    let packet = hex("14 a7 5b 02 13 52 71 87 ab b6 96 78 02 00 00 00
         03 00 00 00 07 00 00 00 00 00 00 00 b8 1a 33 0c
         86 6d aa 18 de 26 35 95 00 00 00 00 80 4d 33 0c
         86 6d aa 18 b2 57 35 95 00 00 00 00");
    let reply = decode_echo_reply(&packet, &default_params_3s(), None).unwrap();
    assert_eq!(reply.sequence, 2);
    assert_eq!(reply.recv_count, Some(3));
    assert_eq!(reply.recv_window, Some(7));
}

#[test]
fn vector_5_close_request_no_hmac() {
    let expected = hex("14 a7 5b 04 13 52 71 87 ab b6 96 78");
    let packet = encode_close_request(
        &CloseRequest {
            token: 0x7896_b6ab_8771_5213,
        },
        None,
    )
    .unwrap();
    assert_eq!(packet, expected);
}

#[test]
fn vector_6_hmac_open_request() {
    let expected = hex("14 a7 5b 09 ff 90 16 a7 aa 53 78 16 9e c3 a2 d5
         54 dc 30 36 01 02 02 80 d0 ac f3 0e 03 80 a8 d6
         b9 07 05 06 06 06 07 06");
    let packet = encode_open_request(
        &OpenRequest {
            params: hmac_params_2s(),
            close: false,
        },
        Some(b"testkey"),
    )
    .unwrap();
    assert_eq!(packet, expected);
    verify_hmac(b"testkey", &packet, 4).unwrap();
}

#[test]
fn vector_7_hmac_echo_request() {
    let expected = hex("14 a7 5b 08 d9 87 4f 82 b3 13 10 31 59 ad 2c 8f
         6b da ef ff 59 ca 3f 5d eb e9 87 43 00 00 00 00
         00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
         00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
         00 00 00 00 00 00 00 00 00 00 00 00");
    let packet = encode_echo_request(
        &EchoRequest {
            token: 0x4387_e9eb_5d3f_ca59,
            sequence: 0,
            params: default_params_3s(),
            payload: Vec::new(),
        },
        Some(b"testkey"),
    )
    .unwrap();
    assert_eq!(packet, expected);
}

#[test]
fn vector_8_hmac_close_request() {
    let expected = hex("14 a7 5b 0c f5 cd 0f a9 de 9d 7d 66 8d 91 a5 32
         48 0e 42 e0 59 ca 3f 5d eb e9 87 43");
    let packet = encode_close_request(
        &CloseRequest {
            token: 0x4387_e9eb_5d3f_ca59,
        },
        Some(b"testkey"),
    )
    .unwrap();
    assert_eq!(packet, expected);
}

#[test]
fn vector_9_no_test_open_close_request() {
    let expected = hex("14 a7 5b 05 01 02 02 80 e0 ba 84 bf 03 03 80 a8
         d6 b9 07 05 06 06 06 07 06");
    let packet = encode_open_request(
        &OpenRequest {
            params: default_params_60s(),
            close: true,
        },
        None,
    )
    .unwrap();
    assert_eq!(packet, expected);
}

#[test]
fn vector_10_no_test_open_close_reply() {
    let packet = hex("14 a7 5b 07 00 00 00 00 00 00 00 00 01 02 02 80
         e0 ba 84 bf 03 03 80 a8 d6 b9 07 05 06 06 06 07
         06");
    let reply = decode_open_reply(&packet, None).unwrap();
    assert_eq!(reply.token, 0);
    assert_eq!(reply.params, default_params_60s());
}

#[test]
fn vector_11_minimal_echo_packet() {
    let expected = hex("14 a7 5b 00 4e 15 61 5c a2 6f 31 a0 00 00 00 00");
    let packet = encode_echo_request(
        &EchoRequest {
            token: 0xa031_6fa2_5c61_154e,
            sequence: 0,
            params: Params::default(),
            payload: Vec::new(),
        },
        None,
    )
    .unwrap();
    assert_eq!(packet, expected);
}

#[test]
fn vector_12_midpoint_reply() {
    let packet = hex("14 a7 5b 02 91 a4 1e 7a f0 14 f6 62 00 00 00 00
         01 00 00 00 01 00 00 00 00 00 00 00 f7 0e 2c 93
         a2 6d aa 18 5a 5f 14 47 03 00 00 00");
    let params = Params {
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Midpoint,
        clock: Clock::Both,
        ..Params::default()
    };
    let reply = decode_echo_reply(&packet, &params, None).unwrap();
    assert_eq!(reply.recv_count, Some(1));
    assert_eq!(reply.recv_window, Some(1));
    assert!(reply.timestamps.midpoint_wall.is_some());
    assert!(reply.timestamps.recv_wall.is_none());
    assert!(reply.timestamps.send_wall.is_none());
}

#[test]
fn spec_18_1_hmac_echo_request_fixture() {
    let expected = hex("14 a7 5b 08 e7 03 41 e7 d4 08 cf 69 41 f3 f4 78
         5a 56 0c 4c ea 3e b3 22 a7 c9 6b 88 bb a1 e2 6f
         00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
         00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
         00 00 00 00 00 00 00 00 00 00 00 00 ff fe fd fc
         ff fe fd fc ff fe fd fc ff fe fd fc");
    let params = Params {
        length: 92,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    };
    let packet = encode_echo_request(
        &EchoRequest {
            token: 0x886b_c9a7_22b3_3eea,
            sequence: 0x6fe2_a1bb,
            params,
            payload: [0xff, 0xfe, 0xfd, 0xfc].repeat(4),
        },
        Some(&[0x3c, 0x68, 0x1d, 0x39, 0x41, 0x1d, 0x72, 0x43]),
    )
    .unwrap();
    assert_eq!(packet, expected);
}

#[test]
fn spec_18_2_hmac_echo_reply_fixture() {
    let packet = hex("14 a7 5b 08 d2 98 a3 4a 6a 13 41 02 68 b2 67 a8
         d6 7e 28 25 c3 cb 6f 76 b6 ce 66 e6 06 07 3b 1d
         19 9f bc a3 9b 3b f8 86 e5 39 e9 d7 a1 75 2a ee
         3d f1 5a 52 9a c6 4a 3e 22 62 55 2d 69 3f 29 46
         d4 05 97 58 66 50 2c dd 2f f1 1d 46 ff fe fd fc
         ff fe fd fc ff fe fd fc ff fe fd fc");
    verify_hmac(
        &[0xda, 0xb3, 0xe9, 0x04, 0xa6, 0x87, 0x92, 0x49],
        &packet,
        4,
    )
    .unwrap();
}
