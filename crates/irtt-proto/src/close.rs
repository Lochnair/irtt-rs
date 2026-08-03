use crate::{
    envelope::{self, FlagRule},
    flags::{FLAG_CLOSE, FLAG_OPEN, FLAG_REPLY},
    layout::PacketLayout,
    Result,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseRequest {
    pub token: u64,
}

pub fn encode_close_request(request: &CloseRequest, hmac_key: Option<&[u8]>) -> Result<Vec<u8>> {
    let mut out = envelope::begin(
        FLAG_CLOSE,
        hmac_key,
        &[
            FlagRule::Require(FLAG_CLOSE),
            FlagRule::Reject(FLAG_OPEN),
            FlagRule::Reject(FLAG_REPLY),
        ],
        PacketLayout::close_request(hmac_key.is_some()).header_len(),
    )?;
    out.extend_from_slice(&request.token.to_le_bytes());
    envelope::finish(out, hmac_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{hmac, FLAG_HMAC, HMAC_SIZE};

    #[test]
    fn hmac_close_request_places_token_after_hmac() {
        let packet = encode_close_request(
            &CloseRequest {
                token: 0x7896_b6ab_8771_5213,
            },
            Some(b"testkey"),
        )
        .unwrap();

        assert_eq!(packet[3], FLAG_CLOSE | FLAG_HMAC);
        assert_eq!(
            &packet[4 + HMAC_SIZE..],
            &0x7896_b6ab_8771_5213u64.to_le_bytes()
        );
        hmac::verify_hmac(b"testkey", &packet, hmac::hmac_offset()).unwrap();
    }
}
