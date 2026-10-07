use std::{
    collections::{HashMap, HashSet},
    fmt, io,
    net::{IpAddr, SocketAddr},
};

use clap::ValueEnum;
use irtt_client::{
    managed::{ManagedPacing, ManagedTargetConfig, TargetAuth, TargetId},
    AddressFamily, Authentication, HmacKey,
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
            .field("address_family", &self.managed.address_family)
            .field("auth", &self.managed.auth)
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
                TargetAuth::Override(Authentication::Unauthenticated)
            } else {
                TargetAuth::Override(Authentication::Hmac(HmacKey::new(value.into_bytes())))
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

/// Family selections for the current desired declarations, never resolved IPs.
/// Unchanged stdin declarations retain their selections; removed declarations
/// are forgotten, so adding them again performs fresh discovery.
#[derive(Debug, Clone)]
pub struct TargetPreparation {
    family: AddressFamily,
    dual_stack: bool,
    discovered: HashMap<String, (String, [bool; 2])>,
}

impl TargetPreparation {
    pub fn new(family: AddressFamily, dual_stack: bool) -> Self {
        Self {
            family,
            dual_stack,
            discovered: HashMap::new(),
        }
    }

    pub async fn prepare(
        mut self,
        specs: Vec<TargetSpec>,
        maximum_targets: Option<usize>,
    ) -> Result<(Vec<PreparedTarget>, Self), String> {
        let mut targets = Vec::with_capacity(specs.len());
        let mut discovered = HashMap::new();
        let mut labels = HashSet::new();
        for (index, spec) in specs.into_iter().enumerate() {
            let families = if let Some(ip) = literal_ip(&spec.addr) {
                match (self.family, ip) {
                    (AddressFamily::Ipv4, IpAddr::V6(_)) => {
                        return Err(format!(
                            "target {}: IPv6 literal conflicts with --ipv4",
                            index + 1
                        ))
                    }
                    (AddressFamily::Ipv6, IpAddr::V4(_)) => {
                        return Err(format!(
                            "target {}: IPv4 literal conflicts with --ipv6",
                            index + 1
                        ))
                    }
                    _ => None,
                }
            } else if self.dual_stack {
                let families = match self.discovered.get(&spec.label) {
                    Some((addr, families)) if *addr == spec.addr => *families,
                    _ => {
                        let endpoint = discovery_endpoint(&spec.addr)
                            .map_err(|error| format!("target {}: {error}", index + 1))?;
                        discovery_families(tokio::net::lookup_host(endpoint).await)
                            .map_err(|error| format!("target {}: {error}", index + 1))?
                    }
                };
                discovered.insert(spec.label.clone(), (spec.addr.clone(), families));
                Some(families)
            } else {
                None
            };
            for target in expand_target(spec, families) {
                if !labels.insert(target.label.clone()) {
                    return Err("duplicate target label after address-family expansion".to_owned());
                }
                targets.push(target);
            }
            if let Some(maximum) = maximum_targets.filter(|maximum| targets.len() > *maximum) {
                return Err(format!(
                    "target set exceeds the {maximum}-target limit after address-family expansion"
                ));
            }
        }
        self.discovered = discovered;
        Ok((targets, self))
    }
}

fn literal_ip(endpoint: &str) -> Option<IpAddr> {
    endpoint
        .parse::<SocketAddr>()
        .map(|addr| addr.ip())
        .ok()
        .or_else(|| endpoint.parse().ok())
        .or_else(|| {
            if endpoint.starts_with('[') && endpoint.ends_with(']') {
                format!("{endpoint}:2112")
                    .parse::<SocketAddr>()
                    .ok()
                    .map(|addr| addr.ip())
            } else {
                None
            }
        })
}

fn discovery_endpoint(endpoint: &str) -> Result<(&str, u16), String> {
    match endpoint.rsplit_once(':') {
        Some((host, port)) => {
            let port = port.parse().map_err(|_| "invalid server port".to_owned())?;
            if host.is_empty() || host.contains(':') || host.starts_with('[') {
                return Err("invalid server endpoint".to_owned());
            }
            Ok((host, port))
        }
        None => Ok((endpoint, 2112)),
    }
}

fn discovery_families(
    result: io::Result<impl Iterator<Item = SocketAddr>>,
) -> Result<[bool; 2], String> {
    let mut families = [false; 2];
    for addr in result.map_err(|_| "DNS lookup failed".to_owned())? {
        families[usize::from(addr.is_ipv6())] = true;
    }
    if families == [false; 2] {
        return Err("DNS lookup returned no usable addresses".to_owned());
    }
    Ok(families)
}

fn expand_target(spec: TargetSpec, families: Option<[bool; 2]>) -> Vec<PreparedTarget> {
    let selections = match families {
        None => vec![(spec.label, None)],
        Some(families) => [("v4", AddressFamily::Ipv4), ("v6", AddressFamily::Ipv6)]
            .into_iter()
            .zip(families)
            .filter(|(_, exists)| *exists)
            .map(|((suffix, family), _)| (format!("{}/{suffix}", spec.label), Some(family)))
            .collect(),
    };
    selections
        .into_iter()
        .map(|(label, family)| {
            let mut managed =
                ManagedTargetConfig::new(TargetId::from(label.clone()), spec.addr.clone());
            managed.address_family = family;
            managed.auth = spec.auth.clone();
            PreparedTarget { label, managed }
        })
        .collect()
}

/// Parse one complete stdin target set after its line terminator was removed.
///
/// Commas frame stdin elements only; positional target arguments do not use
/// this framing and may therefore contain raw commas.
pub fn parse_stdin_target_set(
    record: &str,
    maximum_targets: usize,
) -> Result<Vec<TargetSpec>, String> {
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
    target_specs_with_empty(&args, true)
}
