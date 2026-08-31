//! One preparation step from parsed arguments to a runnable managed setup.
//!
//! Both client applets accept the same target and pacing arguments and then
//! have to turn them into the same three things: a validated target set, one
//! shared [`ClientConfig`] template, and a [`ManagedPacing`]. Doing that
//! separately in each applet is how the CLI ended up deriving its client
//! configuration from an arbitrary "primary" target whose address the managed
//! driver immediately replaced per target.

use std::time::Duration;

use irtt_client::{
    managed::{ManagedClientConfig, ManagedCompletionPolicy, ManagedPacing},
    ClientConfig,
};

use super::{
    args::CommonClientArgs,
    targets::{prepare_managed_targets, target_specs, GroupPacingArg, TargetArg},
    PreparedTarget,
};

/// Capacity of the lossy managed presentation event channel.
pub const MANAGED_EVENT_CAPACITY: usize = 16_384;
pub const STDIN_MAX_DESIRED_TARGETS: usize = 128;
pub const STDIN_MAX_LIVE_TARGET_GENERATIONS: usize = 256;
pub const STDIN_OUTCOME_HISTORY_LIMIT: usize = 256;

/// Target and pacing arguments common to the client applets.
///
/// Duration is deliberately absent: the applets disagree about what a duration
/// means and what its default is, so each keeps its own.
#[derive(Debug, Clone, Copy)]
pub struct TargetSelection<'a> {
    pub targets: &'a [TargetArg],
    pub pacing: GroupPacingArg,
    pub stdin_controlled: bool,
}

/// Everything a managed run needs, prepared and validated together.
#[derive(Debug, Clone)]
pub struct ManagedRunSetup {
    /// Validated targets, in the order the user gave them.
    pub targets: Vec<PreparedTarget>,
    /// Shared client configuration template.
    ///
    /// The managed driver supplies each target's address and authentication
    /// from its [`ManagedTargetConfig`](irtt_client::managed::ManagedTargetConfig),
    /// so this template carries no target of its own.
    pub client: ClientConfig,
    /// Send coordination across active targets.
    pub pacing: ManagedPacing,
    /// Whether this stream may replace its desired set from stdin.
    pub stdin_controlled: bool,
}

impl ManagedRunSetup {
    /// Number of validated targets.
    pub fn target_count(&self) -> usize {
        self.targets.len()
    }

    /// Whether this run drives more than one target.
    pub fn is_multi_target(&self) -> bool {
        self.targets.len() > 1
    }

    /// Managed target configurations, in argument order.
    pub fn managed_targets(&self) -> Vec<irtt_client::managed::ManagedTargetConfig> {
        self.targets
            .iter()
            .map(|target| target.managed.clone())
            .collect()
    }

    /// Managed driver configuration for this run.
    pub fn managed_config(&self) -> ManagedClientConfig {
        let target_count = self.target_count();
        let mut config = ManagedClientConfig {
            client: self.client.clone(),
            pacing: self.pacing,
            completion: ManagedCompletionPolicy::FinishWhenQuiescent,
            event_capacity: MANAGED_EVENT_CAPACITY,
            outcome_history_limit: target_count,
            max_live_target_generations: target_count,
            ..ManagedClientConfig::default()
        };
        if self.stdin_controlled {
            config.completion = ManagedCompletionPolicy::ExplicitStop;
            config.outcome_history_limit = STDIN_OUTCOME_HISTORY_LIMIT;
            config.max_live_target_generations = STDIN_MAX_LIVE_TARGET_GENERATIONS;
        }
        config
    }
}

/// Validate the selected targets and build the shared run setup.
///
/// Targets are validated before any configuration is built, so a bad target set
/// fails before the caller can act on a half-prepared run.
pub fn prepare_managed_run(
    common: &CommonClientArgs,
    duration: Duration,
    selection: TargetSelection<'_>,
) -> Result<ManagedRunSetup, String> {
    let specs = if selection.stdin_controlled {
        if selection.targets.len() > STDIN_MAX_DESIRED_TARGETS {
            return Err(format!(
                "initial target set exceeds the {STDIN_MAX_DESIRED_TARGETS}-target limit"
            ));
        }
        super::targets::target_specs_with_empty(selection.targets, true)?
    } else {
        target_specs(selection.targets)?
    };
    let targets = prepare_managed_targets(specs)?;
    Ok(ManagedRunSetup {
        targets,
        client: common.to_client_config(duration),
        pacing: selection.pacing.into(),
        stdin_controlled: selection.stdin_controlled,
    })
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct CommonOnlyArgs {
        #[command(flatten)]
        common: CommonClientArgs,
    }

    fn common(args: &[&str]) -> CommonClientArgs {
        let mut argv = vec!["common-only"];
        argv.extend_from_slice(args);
        CommonOnlyArgs::try_parse_from(argv).unwrap().common
    }

    fn unlabeled(addr: &str) -> TargetArg {
        TargetArg::new(addr)
    }

    fn labeled(label: &str, addr: &str) -> TargetArg {
        TargetArg::new(format!("{label}={addr}"))
    }

    fn prepare(
        targets: &[TargetArg],
        pacing: GroupPacingArg,
        duration: Duration,
    ) -> Result<ManagedRunSetup, String> {
        prepare_managed_run(
            &common(&[]),
            duration,
            TargetSelection {
                targets,
                pacing,
                stdin_controlled: false,
            },
        )
    }

    #[test]
    fn a_single_target_prepares_one_managed_target_and_a_finite_duration() {
        let setup = prepare(
            &[unlabeled("127.0.0.1:2112")],
            GroupPacingArg::Staggered,
            Duration::from_secs(30),
        )
        .unwrap();

        assert_eq!(setup.target_count(), 1);
        assert!(!setup.is_multi_target());
        assert_eq!(setup.targets[0].label, "127.0.0.1:2112");
        assert_eq!(setup.targets[0].managed.server_addr, "127.0.0.1:2112");
        assert_eq!(setup.client.duration, Some(Duration::from_secs(30)));
        assert_eq!(setup.pacing, ManagedPacing::Staggered);
    }

    #[test]
    fn a_zero_duration_prepares_a_continuous_client_config() {
        let setup = prepare(
            &[unlabeled("127.0.0.1:2112")],
            GroupPacingArg::Staggered,
            Duration::ZERO,
        )
        .unwrap();

        assert_eq!(setup.client.duration, None);
    }

    #[test]
    fn multiple_targets_keep_argument_order_and_share_one_client_config() {
        let setup = prepare(
            &[
                unlabeled("host-a:2112"),
                unlabeled("host-b:2112"),
                labeled("sg", "sg.example.test:2112"),
            ],
            GroupPacingArg::Burst,
            Duration::from_secs(10),
        )
        .unwrap();

        assert!(setup.is_multi_target());
        let labels: Vec<_> = setup
            .targets
            .iter()
            .map(|target| target.label.as_str())
            .collect();
        assert_eq!(labels, ["host-a:2112", "host-b:2112", "sg"]);
        assert_eq!(setup.targets[2].managed.server_addr, "sg.example.test:2112");
        assert_eq!(setup.pacing, ManagedPacing::Burst);

        let config = setup.managed_config();
        assert_eq!(config.outcome_history_limit, 3);
        assert_eq!(config.max_live_target_generations, 3);
        assert_eq!(config.event_capacity, MANAGED_EVENT_CAPACITY);
        assert_eq!(config.pacing, ManagedPacing::Burst);
        assert_eq!(config.client, setup.client);
        assert_eq!(setup.managed_targets().len(), 3);
    }

    #[test]
    fn the_shared_client_config_carries_no_target_of_its_own() {
        // The managed driver replaces the address, and the CLI's key, per
        // target, so the template must not pin either to one target.
        let targets = vec![unlabeled("host-a:2112"), unlabeled("host-b:2112")];
        let setup = prepare_managed_run(
            &common(&["--hmac", "secret"]),
            Duration::from_secs(10),
            TargetSelection {
                targets: &targets,
                pacing: GroupPacingArg::Staggered,
                stdin_controlled: false,
            },
        )
        .unwrap();

        assert_eq!(
            setup.client.server_addr,
            ClientConfig::default().server_addr
        );
        assert_eq!(setup.client.hmac_key, Some(b"secret".to_vec()));
        // Per-target authentication is not exposed on the command line, so no
        // target carries an override of the shared key.
        assert!(setup
            .targets
            .iter()
            .all(|target| target.managed.auth.is_none()));
    }

    #[test]
    fn an_empty_target_set_is_rejected_with_the_list_columns_hint() {
        let err = prepare(&[], GroupPacingArg::Staggered, Duration::from_secs(10)).unwrap_err();

        assert!(err.contains("at least one target is required"));
        assert!(err.contains("--list-columns"));
    }

    #[test]
    fn a_duplicate_label_is_rejected_before_any_configuration_is_built() {
        let targets = vec![
            unlabeled("host-a:2112"),
            labeled("host-a:2112", "host-b:2112"),
        ];
        let err =
            prepare(&targets, GroupPacingArg::Staggered, Duration::from_secs(10)).unwrap_err();

        assert!(err.contains("duplicate target label"));
    }
}
