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
