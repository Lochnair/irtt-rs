use std::{fmt, sync::Arc};

/// Shared HMAC key bytes. Debug output never includes the bytes or a fingerprint.
///
/// An empty key is still an explicitly configured HMAC key.
#[derive(Clone, PartialEq, Eq)]
pub struct HmacKey(Arc<[u8]>);

impl HmacKey {
    /// Store key bytes without interpreting textual key syntax.
    pub fn new(bytes: impl Into<Arc<[u8]>>) -> Self {
        Self(bytes.into())
    }

    /// Explicitly borrow the key material for authentication.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for HmacKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HmacKey([REDACTED])")
    }
}

/// Concrete authentication for a low-level client session.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Authentication {
    /// Send and accept packets without HMAC authentication.
    #[default]
    Unauthenticated,
    /// Authenticate packets with this key, including when it is empty.
    Hmac(HmacKey),
}

impl Authentication {
    pub(crate) fn hmac_key(&self) -> Option<&[u8]> {
        match self {
            Self::Unauthenticated => None,
            Self::Hmac(key) => Some(key.as_bytes()),
        }
    }
}
