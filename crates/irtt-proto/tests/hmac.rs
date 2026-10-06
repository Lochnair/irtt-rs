use irtt_proto::{compute_hmac, compute_hmac_in_place, verify_hmac, ProtoError};

fn captured_open() -> Vec<u8> {
    // Captured vector 6; provenance and independently checked digest are in
    // docs/protocol/test-vectors/README.md. This is an oracle for the primitive.
    "14 a7 5b 09 ff 90 16 a7 aa 53 78 16 9e c3 a2 d5 54 dc 30 36
     01 02 02 80 d0 ac f3 0e 03 80 a8 d6 b9 07 05 06 06 06 07 06"
        .split_whitespace()
        .map(|byte| u8::from_str_radix(byte, 16).unwrap())
        .collect()
}

#[test]
fn computing_hmac_ignores_existing_digest_and_only_replaces_its_field() {
    let expected = captured_open();
    let mut packet = expected.clone();
    packet[4..20].fill(0xa5);
    let before = packet.clone();
    assert_eq!(
        compute_hmac(b"testkey", &packet, 4).unwrap().as_slice(),
        &expected[4..20]
    );
    assert_eq!(packet, before);
    compute_hmac_in_place(b"testkey", &mut packet, 4).unwrap();
    assert_eq!(packet, expected);
}

#[test]
fn verification_rejects_wrong_key_and_modified_authenticated_bytes() {
    let packet = captured_open();
    verify_hmac(b"testkey", &packet, 4).unwrap();
    assert_eq!(verify_hmac(b"wrong", &packet, 4), Err(ProtoError::BadHmac));
    for offset in [0, 4, 21, packet.len() - 1] {
        let mut modified = packet.clone();
        modified[offset] ^= 1;
        assert_eq!(
            verify_hmac(b"testkey", &modified, 4),
            Err(ProtoError::BadHmac)
        );
    }
}

#[test]
fn incomplete_or_out_of_bounds_digest_fields_are_rejected_without_mutation() {
    for offset in [5, 20, 21, usize::MAX] {
        let mut packet = vec![0xa5; 20];
        assert_eq!(
            compute_hmac(b"key", &packet, offset),
            Err(ProtoError::InvalidHmacOffset)
        );
        assert_eq!(
            verify_hmac(b"key", &packet, offset),
            Err(ProtoError::InvalidHmacOffset)
        );
        assert_eq!(
            compute_hmac_in_place(b"key", &mut packet, offset),
            Err(ProtoError::InvalidHmacOffset)
        );
        assert_eq!(packet, [0xa5; 20]);
    }
}
