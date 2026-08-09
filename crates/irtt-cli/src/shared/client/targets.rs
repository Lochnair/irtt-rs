use std::collections::HashSet;

use clap::ValueEnum;
use irtt_client::managed::{ManagedPacing, ManagedTargetConfig, TargetId};

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
