use crate::{
    flags::{FLAG_CLOSE, FLAG_HMAC},
    hmac, write_header, Result, HMAC_SIZE,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseRequest {
    pub token: u64,
}

pub fn encode_close_request(request: &CloseRequest, hmac_key: Option<&[u8]>) -> Result<Vec<u8>> {
    let mut flags = FLAG_CLOSE;
    if hmac_key.is_some() {
        flags |= FLAG_HMAC;
    }

    let mut out = Vec::new();
    write_header(&mut out, flags);
    if hmac_key.is_some() {
        out.extend_from_slice(&[0; HMAC_SIZE]);
    }
    out.extend_from_slice(&request.token.to_le_bytes());
    if let Some(key) = hmac_key {
        hmac::compute_hmac_in_place(key, &mut out, hmac::hmac_offset())?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{hmac, FLAG_HMAC};

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
