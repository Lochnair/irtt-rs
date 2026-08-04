use std::{fmt, sync::Arc};

/// Caller-owned stable target identity.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TargetId(Arc<str>);

impl TargetId {
    /// Construct a target identifier.
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    /// Borrow the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for TargetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TargetId").field(&self.0).finish()
    }
}

impl fmt::Display for TargetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for TargetId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for TargetId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl AsRef<str> for TargetId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
