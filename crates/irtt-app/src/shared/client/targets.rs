use std::{collections::HashSet, fmt};

use clap::ValueEnum;
use irtt_client::{
    managed::{ManagedPacing, ManagedTargetConfig, TargetId},
    ClientAuthConfig,
};

/// One raw positional target captured by Clap.
///
/// Parsing happens during run preparation so invalid target syntax never makes
/// Clap include the original argument (which may carry an HMAC key) in an
/// error diagnostic.
#[derive(Clone, PartialEq, Eq)]
pub struct TargetArg {
    input: String,
}

impl TargetArg {
    pub fn new(input: impl Into<String>) -> Self {
        Self {
            input: input.into(),
        }
    }
}

impl fmt::Debug for TargetArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TargetArg(<redacted>)")
    }
}

/// Capture one positional target without exposing it through Clap diagnostics.
pub fn parse_target(input: &str) -> Result<TargetArg, String> {
    Ok(TargetArg::new(input))
}

#[derive(Clone, PartialEq, Eq)]
pub enum TargetAuth {
    Inherit,
    Override(Vec<u8>),
    Disable,
}

impl fmt::Debug for TargetAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inherit => f.write_str("Inherit"),
            Self::Override(_) => f.write_str("Override(<redacted>)"),
            Self::Disable => f.write_str("Disable"),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TargetSpec {
    pub label: String,
    pub addr: String,
    pub auth: TargetAuth,
}

impl fmt::Debug for TargetSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TargetSpec")
            .field("label", &self.label)
            .field("addr", &self.addr)
            .field("auth", &self.auth)
            .finish()
    }
}

#[derive(Clone)]
pub struct PreparedTarget {
    pub label: String,
    pub managed: ManagedTargetConfig,
}

impl fmt::Debug for PreparedTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedTarget")
            .field("label", &self.label)
            .field("server_addr", &self.managed.server_addr)
            .field(
                "auth",
                &match &self.managed.auth {
                    None => TargetAuth::Inherit,
                    Some(ClientAuthConfig { hmac_key: Some(_) }) => {
                        TargetAuth::Override(Vec::new())
                    }
                    Some(ClientAuthConfig { hmac_key: None }) => TargetAuth::Disable,
                },
            )
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum GroupPacingArg {
    Staggered,
    Burst,
}

impl From<GroupPacingArg> for ManagedPacing {
    fn from(value: GroupPacingArg) -> Self {
        match value {
            GroupPacingArg::Staggered => Self::Staggered,
            GroupPacingArg::Burst => Self::Burst,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetParseError {
    EmptyLabel,
    EmptyAddress,
    InvalidEscape,
    EmptyTarget,
}

impl fmt::Display for TargetParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyLabel => "target label must not be empty",
            Self::EmptyAddress => "target address must not be empty",
            Self::InvalidEscape => "invalid target escape sequence",
            Self::EmptyTarget => "target must not be empty",
        };
        f.write_str(message)
    }
}

fn split_unescaped(input: &str, delimiter: char) -> Result<Vec<&str>, TargetParseError> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut escaped = false;
    for (index, ch) in input.char_indices() {
        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == delimiter {
            parts.push(&input[start..index]);
            start = index + ch.len_utf8();
        }
    }
    if escaped {
        return Err(TargetParseError::InvalidEscape);
    }
    parts.push(&input[start..]);
    Ok(parts)
}

fn first_unescaped(input: &str, delimiter: char) -> Result<Option<usize>, TargetParseError> {
    let mut escaped = false;
    for (index, ch) in input.char_indices() {
        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == delimiter {
            return Ok(Some(index));
        }
    }
    if escaped {
        return Err(TargetParseError::InvalidEscape);
    }
    Ok(None)
}

fn unescape(input: &str) -> Result<String, TargetParseError> {
    let mut output = String::with_capacity(input.len());
    let mut escaped = false;
    for ch in input.chars() {
        if escaped {
            if !matches!(ch, '\\' | '=' | ';' | ',' | '@') {
                return Err(TargetParseError::InvalidEscape);
            }
            output.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            output.push(ch);
        }
    }
    if escaped {
        return Err(TargetParseError::InvalidEscape);
    }
    Ok(output)
}

fn hmac_modifier(input: &str) -> Result<Option<usize>, TargetParseError> {
    let mut escaped = false;
    for (index, ch) in input.char_indices() {
        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if input[index..].starts_with("@hmac=") {
            return Ok(Some(index));
        }
    }
    if escaped {
        return Err(TargetParseError::InvalidEscape);
    }
    Ok(None)
}

fn parse_target_syntax(
    input: &str,
) -> Result<(Option<String>, String, TargetAuth), TargetParseError> {
    if input.is_empty() {
        return Err(TargetParseError::EmptyTarget);
    }
    let (main, auth) = match hmac_modifier(input)? {
        Some(index) => {
            let value = unescape(&input[index + "@hmac=".len()..])?;
            let auth = if value.is_empty() {
                TargetAuth::Disable
            } else {
                TargetAuth::Override(value.into_bytes())
            };
            (&input[..index], auth)
        }
        None => (input, TargetAuth::Inherit),
    };
    let (label, address) = match first_unescaped(main, '=')? {
        None => (None, unescape(main)?),
        Some(index) => {
            let label = unescape(&main[..index])?;
            let address = unescape(&main[index + '='.len_utf8()..])?;
            (Some(label), address)
        }
    };
    if label.as_deref().is_some_and(str::is_empty) {
        return Err(TargetParseError::EmptyLabel);
    }
    if address.is_empty() {
        return Err(TargetParseError::EmptyAddress);
    }
    Ok((label, address, auth))
}

pub(crate) fn target_specs_with_empty(
    targets: &[TargetArg],
    allow_empty: bool,
) -> Result<Vec<TargetSpec>, String> {
    let mut specs = Vec::with_capacity(targets.len());
    let mut unlabeled_counts = std::collections::HashMap::<String, usize>::new();
    for (index, target) in targets.iter().enumerate() {
        let (explicit_label, addr, auth) = parse_target_syntax(&target.input)
            .map_err(|error| format!("invalid target {}: {error}", index + 1))?;
        let label = match explicit_label {
            Some(label) => label,
            None => {
                let count = unlabeled_counts.entry(addr.clone()).or_default();
                *count += 1;
                if *count == 1 {
                    addr.clone()
                } else {
                    format!("{}#{}", addr, *count)
                }
            }
        };
        specs.push(TargetSpec { label, addr, auth });
    }

    if specs.is_empty() && !allow_empty {
        return Err("at least one target is required unless --list-columns is set".to_owned());
    }

    let mut labels = HashSet::new();
    for spec in &specs {
        if !labels.insert(spec.label.clone()) {
            return Err("duplicate target label".to_owned());
        }
    }

    Ok(specs)
}

pub fn target_specs(targets: &[TargetArg]) -> Result<Vec<TargetSpec>, String> {
    target_specs_with_empty(targets, false)
}

pub fn prepare_managed_targets(specs: Vec<TargetSpec>) -> Result<Vec<PreparedTarget>, String> {
    let mut targets = Vec::with_capacity(specs.len());
    for spec in specs {
        let mut managed = ManagedTargetConfig::new(TargetId::from(spec.label.clone()), spec.addr);
        managed.auth = match spec.auth {
            TargetAuth::Inherit => None,
            TargetAuth::Override(hmac_key) => Some(ClientAuthConfig {
                hmac_key: Some(hmac_key),
            }),
            TargetAuth::Disable => Some(ClientAuthConfig { hmac_key: None }),
        };
        targets.push(PreparedTarget {
            label: spec.label,
            managed,
        });
    }
    Ok(targets)
}

/// Parse one complete stdin target set after its line terminator was removed.
///
/// Commas frame stdin elements only; positional target arguments do not use
/// this framing and may therefore contain raw commas.
pub fn parse_stdin_target_set(
    record: &str,
    maximum_targets: usize,
) -> Result<Vec<PreparedTarget>, String> {
    if record == "[]" {
        return Ok(Vec::new());
    }
    let elements = split_unescaped(record, ',').map_err(|error| error.to_string())?;
    if elements.len() > maximum_targets {
        return Err(format!(
            "target set exceeds the {maximum_targets}-target limit"
        ));
    }
    let args = elements.into_iter().map(TargetArg::new).collect::<Vec<_>>();
    prepare_managed_targets(target_specs_with_empty(&args, true)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(input: &str) -> TargetArg {
        TargetArg::new(input)
    }

    fn specs(inputs: &[&str]) -> Result<Vec<TargetSpec>, String> {
        target_specs(&inputs.iter().map(|input| target(input)).collect::<Vec<_>>())
    }

    #[test]
    fn legacy_target_syntax_and_ipv6_remain_supported() {
        let parsed = specs(&[
            "host.example",
            "host.example:2112",
            "[::1]:2112",
            "eu=host.example",
        ])
        .unwrap();
        assert_eq!(parsed[0].label, "host.example");
        assert_eq!(parsed[1].label, "host.example:2112");
        assert_eq!(parsed[2].label, "[::1]:2112");
        assert_eq!(parsed[3].label, "eu");
        assert!(parsed
            .iter()
            .all(|target| matches!(target.auth, TargetAuth::Inherit)));
    }

    #[test]
    fn target_hmac_supports_inherit_override_disable_and_base64_padding() {
        let parsed = specs(&[
            "default=default.example",
            "private=private.example@hmac=YWJjZA==",
            "public=public.example@hmac=",
        ])
        .unwrap();
        assert!(matches!(parsed[0].auth, TargetAuth::Inherit));
        assert_eq!(parsed[1].auth, TargetAuth::Override(b"YWJjZA==".to_vec()));
        assert!(matches!(parsed[2].auth, TargetAuth::Disable));
    }

    #[test]
    fn target_delimiters_are_escaped_contextually() {
        let parsed = specs(&["eu\\=west=host\\@hmac=literal@hmac=a\\@b\\,c\\=d\\\\e"]).unwrap();
        assert_eq!(parsed[0].label, "eu=west");
        assert_eq!(parsed[0].addr, "host@hmac=literal");
        assert_eq!(parsed[0].auth, TargetAuth::Override(b"a@b,c=d\\e".to_vec()));
    }

    #[test]
    fn positional_commas_are_not_reserved() {
        let parsed = specs(&["edge=host,region@hmac=key,part"]).unwrap();
        assert_eq!(parsed[0].addr, "host,region");
        assert_eq!(parsed[0].auth, TargetAuth::Override(b"key,part".to_vec()));
    }

    #[test]
    fn parser_diagnostics_and_debug_do_not_reveal_hmac_values() {
        let secret = "very-secret-value";
        let error = specs(&[&format!("target=host@hmac={secret}\\q")]).unwrap_err();
        assert!(!error.contains(secret));
        assert!(!format!("{:?}", target(&format!("target=host@hmac={secret}"))).contains(secret));
        assert!(
            !format!("{:?}", TargetAuth::Override(secret.as_bytes().to_vec())).contains(secret)
        );
    }

    #[test]
    fn target_specs_suffix_repeated_unlabeled_and_reject_duplicate_labels() {
        let parsed = specs(&["host-a:2112", "host-a:2112"]).unwrap();
        assert_eq!(parsed[0].label, "host-a:2112");
        assert_eq!(parsed[1].label, "host-a:2112#2");
        assert_eq!(
            specs(&["host-a:2112", "host-a:2112=host-b:2112"]).unwrap_err(),
            "duplicate target label"
        );
    }

    #[test]
    fn target_specs_preserve_argument_order_and_unlabeled_counts() {
        let parsed = specs(&["local", "eu=eu.example", "foo", "us=us.example", "foo"]).unwrap();
        let labels: Vec<_> = parsed.iter().map(|target| target.label.as_str()).collect();
        assert_eq!(labels, ["local", "eu", "foo", "us", "foo#2"]);
        let addresses: Vec<_> = parsed.iter().map(|target| target.addr.as_str()).collect();
        assert_eq!(
            addresses,
            ["local", "eu.example", "foo", "us.example", "foo"]
        );
    }

    #[test]
    fn target_specs_reject_generated_suffix_collisions() {
        assert_eq!(
            specs(&[
                "host.example",
                "host.example",
                "host.example#2=other.example"
            ])
            .unwrap_err(),
            "duplicate target label"
        );
    }

    #[test]
    fn malformed_target_syntax_is_rejected_without_echoing_input() {
        for input in ["=host", "label=", "host@hmac=value\\q", "host\\x"] {
            assert!(specs(&[input]).is_err(), "{input}");
        }
    }

    #[test]
    fn target_preparation_maps_auth_to_managed_config() {
        let prepared =
            prepare_managed_targets(specs(&["a=one@hmac=key", "b=two@hmac="]).unwrap()).unwrap();
        assert_eq!(
            prepared[0]
                .managed
                .auth
                .as_ref()
                .unwrap()
                .hmac_key
                .as_deref(),
            Some(b"key".as_slice())
        );
        assert_eq!(prepared[1].managed.auth.as_ref().unwrap().hmac_key, None);
    }

    #[test]
    fn target_preparation_preserves_duplicate_endpoints() {
        let prepared =
            prepare_managed_targets(specs(&["a=example.test", "b=example.test"]).unwrap()).unwrap();
        assert_eq!(prepared[0].managed.server_addr, "example.test");
        assert_eq!(prepared[1].managed.server_addr, "example.test");
    }

    #[test]
    fn stdin_target_sets_frame_commas_without_trimming_payload() {
        let parsed = parse_stdin_target_set("a=one@hmac=x\\,y,b=two", 128).unwrap();
        assert_eq!(
            parsed[0].managed.auth.as_ref().unwrap().hmac_key.as_deref(),
            Some(b"x,y".as_slice())
        );
        assert_eq!(parsed[1].label, "b");
        assert_eq!(
            parse_stdin_target_set(" []", 128).unwrap()[0]
                .managed
                .server_addr,
            " []"
        );
        assert_eq!(
            parse_stdin_target_set("[] ", 128).unwrap()[0]
                .managed
                .server_addr,
            "[] "
        );
        assert!(parse_stdin_target_set("", 128).is_err());
        assert!(parse_stdin_target_set("a=one,b=two", 1).is_err());
    }

    #[test]
    fn stdin_empty_set_is_exactly_bracket_pair() {
        assert!(parse_stdin_target_set("[]", 128).unwrap().is_empty());
    }
}
