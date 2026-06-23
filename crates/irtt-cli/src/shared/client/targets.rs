use std::{collections::HashSet, net::ToSocketAddrs};

use clap::ValueEnum;
use irtt_client::{ClientConfig, ManagedGroupPacing, ManagedTargetConfig, TargetId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelledTargetArg {
    pub label: String,
    pub addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetSpec {
    pub label: String,
    pub addr: String,
}

#[derive(Debug, Clone)]
pub struct ResolvedTarget {
    pub label: String,
    pub managed: ManagedTargetConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum GroupPacingArg {
    Staggered,
    Burst,
}

impl From<GroupPacingArg> for ManagedGroupPacing {
    fn from(value: GroupPacingArg) -> Self {
        match value {
            GroupPacingArg::Staggered => Self::Staggered,
            GroupPacingArg::Burst => Self::Burst,
        }
    }
}

pub fn parse_labelled_target(input: &str) -> Result<LabelledTargetArg, String> {
    let (label, addr) = input
        .split_once('=')
        .ok_or_else(|| "target must use LABEL=TARGET syntax".to_owned())?;
    if label.is_empty() {
        return Err("target label must not be empty".to_owned());
    }
    if addr.is_empty() {
        return Err("target address must not be empty".to_owned());
    }
    Ok(LabelledTargetArg {
        label: label.to_owned(),
        addr: addr.to_owned(),
    })
}

pub fn target_specs(
    positional_targets: &[String],
    labelled_targets: &[LabelledTargetArg],
) -> Result<Vec<TargetSpec>, String> {
    let mut specs = Vec::new();
    let mut positional_counts = std::collections::HashMap::<&str, usize>::new();
    for target in positional_targets {
        let count = positional_counts.entry(target.as_str()).or_default();
        *count += 1;
        let label = if *count == 1 {
            target.clone()
        } else {
            format!("{target}#{}", *count)
        };
        specs.push(TargetSpec {
            label,
            addr: target.clone(),
        });
    }

    for target in labelled_targets {
        specs.push(TargetSpec {
            label: target.label.clone(),
            addr: target.addr.clone(),
        });
    }

    if specs.is_empty() {
        return Err("at least one target is required unless --list-columns is set".to_owned());
    }

    let mut labels = HashSet::new();
    for spec in &specs {
        if !labels.insert(spec.label.clone()) {
            return Err(format!("duplicate target label {:?}", spec.label));
        }
    }

    Ok(specs)
}

pub fn resolved_managed_targets(
    specs: Vec<TargetSpec>,
    config: &ClientConfig,
) -> Result<Vec<ResolvedTarget>, String> {
    let mut remotes = HashSet::new();
    let mut targets = Vec::with_capacity(specs.len());
    for spec in specs {
        let remote = resolve_target(&spec.addr, config).map_err(|err| {
            format!(
                "failed to resolve target {} ({:?}): {err}",
                spec.label, spec.addr
            )
        })?;
        if !remotes.insert(remote) {
            return Err(format!(
                "duplicate resolved target address {remote} for label {}",
                spec.label
            ));
        }
        targets.push(ResolvedTarget {
            label: spec.label.clone(),
            managed: ManagedTargetConfig {
                id: TargetId::from(spec.label),
                remote,
                auth: None,
            },
        });
    }
    Ok(targets)
}

pub fn resolve_target(addr: &str, config: &ClientConfig) -> Result<std::net::SocketAddr, String> {
    let normalized = normalize_target_addr(addr);
    let mut addrs = normalized
        .to_socket_addrs()
        .map_err(|_| format!("failed to resolve address {normalized:?}"))?;
    addrs
        .find(|addr| {
            (!config.socket_config.ipv4_only || addr.is_ipv4())
                && (!config.socket_config.ipv6_only || addr.is_ipv6())
        })
        .ok_or_else(|| format!("failed to resolve address {normalized:?}"))
}

pub fn normalize_target_addr(addr: &str) -> String {
    const DEFAULT_PORT: u16 = 2112;
    if addr.parse::<std::net::SocketAddr>().is_ok() {
        return addr.to_owned();
    }
    if addr.starts_with('[') && addr.ends_with(']') {
        return format!("{addr}:{DEFAULT_PORT}");
    }
    if addr.starts_with('[') {
        return addr.to_owned();
    }
    if addr.parse::<std::net::Ipv6Addr>().is_ok() {
        return format!("[{addr}]:{DEFAULT_PORT}");
    }
    if addr
        .rsplit_once(':')
        .is_some_and(|(_, port)| port.parse::<u16>().is_ok())
    {
        return addr.to_owned();
    }
    format!("{addr}:{DEFAULT_PORT}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_normalization_matches_single_target_forms() {
        assert_eq!(normalize_target_addr("127.0.0.1"), "127.0.0.1:2112");
        assert_eq!(normalize_target_addr("127.0.0.1:1234"), "127.0.0.1:1234");
        assert_eq!(normalize_target_addr("example.com"), "example.com:2112");
        assert_eq!(
            normalize_target_addr("example.com:1234"),
            "example.com:1234"
        );
        assert_eq!(normalize_target_addr("[::1]"), "[::1]:2112");
        assert_eq!(normalize_target_addr("[::1]:1234"), "[::1]:1234");
        assert_eq!(normalize_target_addr("::1"), "[::1]:2112");
    }

    #[test]
    fn target_resolution_respects_family_filters() {
        let config = ClientConfig::default();
        assert_eq!(
            resolve_target("127.0.0.1", &config).unwrap(),
            "127.0.0.1:2112".parse().unwrap()
        );
        assert_eq!(
            resolve_target("[::1]", &config).unwrap(),
            "[::1]:2112".parse().unwrap()
        );
        assert_eq!(
            resolve_target("::1", &config).unwrap(),
            "[::1]:2112".parse().unwrap()
        );

        let mut ipv4_only = ClientConfig::default();
        ipv4_only.socket_config.ipv4_only = true;
        assert!(resolve_target("127.0.0.1", &ipv4_only).unwrap().is_ipv4());
        assert!(resolve_target("::1", &ipv4_only).is_err());

        let mut ipv6_only = ClientConfig::default();
        ipv6_only.socket_config.ipv6_only = true;
        assert!(resolve_target("::1", &ipv6_only).unwrap().is_ipv6());
        assert!(resolve_target("127.0.0.1", &ipv6_only).is_err());
    }

    #[test]
    fn target_specs_suffix_repeated_positionals_and_reject_duplicate_labels() {
        let positionals = vec!["host-a:2112".to_owned(), "host-a:2112".to_owned()];
        let specs = target_specs(&positionals, &[]).unwrap();

        assert_eq!(specs[0].label, "host-a:2112");
        assert_eq!(specs[1].label, "host-a:2112#2");

        let labels = vec![LabelledTargetArg {
            label: "host-a:2112".to_owned(),
            addr: "host-b:2112".to_owned(),
        }];
        let err = target_specs(&positionals[..1], &labels).unwrap_err();
        assert!(err.contains("duplicate target label"));
    }

    #[test]
    fn duplicate_resolved_target_addresses_are_rejected() {
        let specs = vec![
            TargetSpec {
                label: "a".to_owned(),
                addr: "127.0.0.1:2112".to_owned(),
            },
            TargetSpec {
                label: "b".to_owned(),
                addr: "127.0.0.1".to_owned(),
            },
        ];
        let err = resolved_managed_targets(specs, &ClientConfig::default()).unwrap_err();

        assert!(err.contains("duplicate resolved target address 127.0.0.1:2112"));
    }
}
