use std::collections::HashSet;

use clap::ValueEnum;
use irtt_client::managed::{ManagedPacing, ManagedTargetConfig, TargetId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetArg {
    pub label: Option<String>,
    pub addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetSpec {
    pub label: String,
    pub addr: String,
}

#[derive(Debug, Clone)]
pub struct PreparedTarget {
    pub label: String,
    pub managed: ManagedTargetConfig,
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

/// Parse one positional target argument, as `TARGET` or `LABEL=TARGET`.
pub fn parse_target(input: &str) -> Result<TargetArg, String> {
    match input.split_once('=') {
        None => Ok(TargetArg {
            label: None,
            addr: input.to_owned(),
        }),
        Some((label, addr)) => {
            if label.is_empty() {
                return Err("target label must not be empty".to_owned());
            }
            if addr.is_empty() {
                return Err("target address must not be empty".to_owned());
            }
            Ok(TargetArg {
                label: Some(label.to_owned()),
                addr: addr.to_owned(),
            })
        }
    }
}

pub fn target_specs(targets: &[TargetArg]) -> Result<Vec<TargetSpec>, String> {
    let mut specs = Vec::with_capacity(targets.len());
    let mut unlabeled_counts = std::collections::HashMap::<&str, usize>::new();
    for target in targets {
        let label = match &target.label {
            Some(label) => label.clone(),
            None => {
                let count = unlabeled_counts.entry(target.addr.as_str()).or_default();
                *count += 1;
                if *count == 1 {
                    target.addr.clone()
                } else {
                    format!("{}#{}", target.addr, *count)
                }
            }
        };
        specs.push(TargetSpec {
            label,
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

pub fn prepare_managed_targets(specs: Vec<TargetSpec>) -> Result<Vec<PreparedTarget>, String> {
    let mut targets = Vec::with_capacity(specs.len());
    for spec in specs {
        targets.push(PreparedTarget {
            label: spec.label.clone(),
            managed: ManagedTargetConfig::new(TargetId::from(spec.label), spec.addr),
        });
    }
    Ok(targets)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unlabeled(addr: &str) -> TargetArg {
        TargetArg {
            label: None,
            addr: addr.to_owned(),
        }
    }

    fn labeled(label: &str, addr: &str) -> TargetArg {
        TargetArg {
            label: Some(label.to_owned()),
            addr: addr.to_owned(),
        }
    }

    #[test]
    fn parse_target_without_equals_is_unlabeled() {
        let target = parse_target("host.example").unwrap();
        assert_eq!(target.label, None);
        assert_eq!(target.addr, "host.example");

        let target = parse_target("host.example:2112").unwrap();
        assert_eq!(target.label, None);
        assert_eq!(target.addr, "host.example:2112");

        let target = parse_target("[::1]:2112").unwrap();
        assert_eq!(target.label, None);
        assert_eq!(target.addr, "[::1]:2112");
    }

    #[test]
    fn parse_target_with_equals_splits_on_first() {
        let target = parse_target("eu=host.example").unwrap();
        assert_eq!(target.label, Some("eu".to_owned()));
        assert_eq!(target.addr, "host.example");

        let target = parse_target("eu=host.example:2112").unwrap();
        assert_eq!(target.label, Some("eu".to_owned()));
        assert_eq!(target.addr, "host.example:2112");
    }

    #[test]
    fn parse_target_rejects_empty_label_or_address() {
        assert!(parse_target("=host.example").is_err());
        assert!(parse_target("eu=").is_err());
    }

    #[test]
    fn target_specs_suffix_repeated_unlabeled_and_reject_duplicate_labels() {
        let targets = vec![unlabeled("host-a:2112"), unlabeled("host-a:2112")];
        let specs = target_specs(&targets).unwrap();

        assert_eq!(specs[0].label, "host-a:2112");
        assert_eq!(specs[1].label, "host-a:2112#2");

        let targets = vec![
            unlabeled("host-a:2112"),
            labeled("host-a:2112", "host-b:2112"),
        ];
        let err = target_specs(&targets).unwrap_err();
        assert!(err.contains("duplicate target label"));
    }

    #[test]
    fn target_specs_preserve_argument_order_with_mixed_labels() {
        let targets = vec![
            unlabeled("local"),
            labeled("eu", "eu.example"),
            unlabeled("backup"),
            labeled("us", "us.example"),
        ];
        let specs = target_specs(&targets).unwrap();

        let labels: Vec<_> = specs.iter().map(|spec| spec.label.as_str()).collect();
        assert_eq!(labels, ["local", "eu", "backup", "us"]);
        let addrs: Vec<_> = specs.iter().map(|spec| spec.addr.as_str()).collect();
        assert_eq!(addrs, ["local", "eu.example", "backup", "us.example"]);
    }

    #[test]
    fn target_specs_count_unlabeled_occurrences_independently_of_explicit_labels() {
        let targets = vec![unlabeled("foo"), labeled("eu", "foo"), unlabeled("foo")];
        let specs = target_specs(&targets).unwrap();

        let labels: Vec<_> = specs.iter().map(|spec| spec.label.as_str()).collect();
        assert_eq!(labels, ["foo", "eu", "foo#2"]);
    }

    #[test]
    fn target_specs_reject_generated_suffix_collisions() {
        let targets = vec![
            unlabeled("host.example"),
            unlabeled("host.example"),
            labeled("host.example#2", "other.example"),
        ];
        let err = target_specs(&targets).unwrap_err();
        assert!(err.contains("duplicate target label"));
    }

    #[test]
    fn target_preparation_preserves_hostname_and_allows_duplicate_endpoints() {
        let specs = vec![
            TargetSpec {
                label: "a".to_owned(),
                addr: "example.test".to_owned(),
            },
            TargetSpec {
                label: "b".to_owned(),
                addr: "example.test".to_owned(),
            },
        ];
        let targets = prepare_managed_targets(specs).unwrap();
        assert_eq!(targets[0].managed.server_addr, "example.test");
        assert_eq!(targets[1].managed.server_addr, "example.test");
    }
}
