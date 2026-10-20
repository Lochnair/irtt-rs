/// Decode CLI key syntax without ever including key material in diagnostics.
pub(crate) fn decode_hmac_key(input: &str) -> Result<Vec<u8>, String> {
    let key = match input.strip_prefix("0x") {
        Some(hex) => hex::decode(hex).map_err(|_| {
            "HMAC hexadecimal key must contain an even number of valid hexadecimal digits"
                .to_owned()
        })?,
        None => input.as_bytes().to_vec(),
    };
    if key.is_empty() {
        return Err("HMAC key must not be empty".to_owned());
    }
    Ok(key)
}
