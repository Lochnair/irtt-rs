use std::{collections::HashSet, net::ToSocketAddrs, time::Duration};

use clap::{Parser, ValueEnum};
use irtt_client::{ClientConfig, ManagedGroupPacing, ManagedTargetConfig, TargetId};

use crate::shared::client::{parse_test_duration, CommonClientArgs, TimestampArg};

pub const DEFAULT_CLIENT_DURATION: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Parser)]
#[command(name = "irtt-cli", about = "Minimal IRTT-compatible stream client")]
pub struct ClientArgs {
    /// Server address or host, with optional port. Repeat for multi-target mode.
    #[arg(
        value_name = "TARGET",
        num_args = 0..,
        long_help = "Server address or host, with optional port. Repeat the positional target for multi-target mode, for example: irtt-cli host-a:2112 host-b:2112."
    )]
    pub targets: Vec<String>,

    /// Explicit labelled target, as label=host:port. Repeat for multi-target mode.
    #[arg(
        long = "target",
        value_name = "LABEL=TARGET",
        value_parser = parse_labelled_target,
        long_help = "Explicit labelled target, as label=host:port. Repeat for multi-target mode. The label is used as the target column and must be unique."
    )]
    pub labelled_targets: Vec<LabelledTargetArg>,

    /// Managed group pacing for multi-target mode.
    #[arg(
        long,
        value_enum,
        default_value_t = GroupPacingArg::Staggered,
        long_help = "Managed group pacing for multi-target mode.\n\nstaggered spaces active targets across the probe interval. burst sends one probe to every active target back-to-back once per interval."
    )]
    pub pacing: GroupPacingArg,

    #[arg(
        long,
        default_value = "10s",
        value_parser = parse_test_duration,
        help = "Test duration; use 0 for continuous mode",
        long_help = "Test duration; use 0 for continuous mode.\n\nFinite runs retain exact statistics for final summaries. Continuous mode uses bounded-memory running statistics and prints a final summary only when interrupted."
    )]
    pub duration: Duration,

    #[command(flatten)]
    pub common: CommonClientArgs,

    /// Event row output format.
    #[arg(
        long,
        value_enum,
        default_value_t = OutputFormat::Table,
        long_help = "Event row output format.\n\nTable is the default interactive format. CSV, TSV, and JSON Lines default to all columns for structured export."
    )]
    pub format: OutputFormat,

    /// Comma-separated event row columns, or default/all.
    #[arg(
        short = 'c',
        long,
        value_name = "COLUMNS",
        long_help = "Comma-separated event row columns, or default/all.\n\nThe default table columns are compact and hide echo_sent rows. Custom table columns include all event rows. Run --list-columns to see valid names."
    )]
    pub columns: Option<String>,

    /// List available event row columns and aliases, then exit.
    #[arg(
        long,
        long_help = "List available event row columns and aliases, then exit.\n\nA server argument is not required when listing columns."
    )]
    pub list_columns: bool,

    /// Header policy for table, CSV, and TSV output.
    #[arg(
        long,
        value_enum,
        default_value_t = HeaderMode::Auto,
        long_help = "Header policy for table, CSV, and TSV output.\n\nJSON Lines never prints a header."
    )]
    pub header: HeaderMode,

    /// Include extra fields in table output and final summaries.
    #[arg(long)]
    pub verbose: bool,
}

impl ClientArgs {
    pub fn to_client_config(&self) -> ClientConfig {
        self.common.to_client_config(
            self.primary_target_addr()
                .expect("target is required unless --list-columns is set"),
            self.duration,
        )
    }

    pub fn is_continuous(&self) -> bool {
        self.duration == Duration::ZERO
    }

    pub fn timestamp_mode(&self) -> TimestampArg {
        self.common.tstamp
    }

    pub fn target_specs(&self) -> Result<Vec<CliTargetSpec>, String> {
        let mut specs = Vec::new();
        let mut positional_counts = std::collections::HashMap::<&str, usize>::new();
        for target in &self.targets {
            let count = positional_counts.entry(target.as_str()).or_default();
            *count += 1;
            let label = if *count == 1 {
                target.clone()
            } else {
                format!("{target}#{}", *count)
            };
            specs.push(CliTargetSpec {
                label,
                addr: target.clone(),
            });
        }

        for target in &self.labelled_targets {
            specs.push(CliTargetSpec {
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

    pub fn resolved_managed_targets(&self) -> Result<Vec<ResolvedCliTarget>, String> {
        let specs = self.target_specs()?;
        let config = self.to_client_config();
        let mut remotes = HashSet::new();
        let mut targets = Vec::with_capacity(specs.len());
        for spec in specs {
            let remote = resolve_cli_target(&spec.addr, &config).map_err(|err| {
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
            targets.push(ResolvedCliTarget {
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

    fn primary_target_addr(&self) -> Option<&str> {
        self.targets.first().map(String::as_str).or_else(|| {
            self.labelled_targets
                .first()
                .map(|target| target.addr.as_str())
        })
    }
}

impl std::ops::Deref for ClientArgs {
    type Target = CommonClientArgs;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Readable terminal table output.
    Table,
    /// Comma-separated event rows.
    Csv,
    /// Tab-separated event rows.
    Tsv,
    /// One JSON object per event row.
    Jsonl,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelledTargetArg {
    pub label: String,
    pub addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliTargetSpec {
    pub label: String,
    pub addr: String,
}

#[derive(Debug, Clone)]
pub struct ResolvedCliTarget {
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

impl OutputFormat {
    pub fn prints_summary(self) -> bool {
        matches!(self, Self::Table)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum HeaderMode {
    /// Print headers for table, CSV, and TSV output.
    Auto,
    /// Always print headers where the format supports them.
    Always,
    /// Never print headers.
    Never,
}

fn parse_labelled_target(input: &str) -> Result<LabelledTargetArg, String> {
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

fn resolve_cli_target(addr: &str, config: &ClientConfig) -> Result<std::net::SocketAddr, String> {
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

fn normalize_target_addr(addr: &str) -> String {
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
    use clap::{CommandFactory, Parser};
    use irtt_client::NegotiationPolicy;
    use irtt_proto::{Clock, ReceivedStats, StampAt};

    fn parse(args: &[&str]) -> Result<ClientArgs, clap::Error> {
        let mut argv = vec!["irtt-cli"];
        argv.extend_from_slice(args);
        ClientArgs::try_parse_from(argv)
    }

    #[test]
    fn client_parser_keeps_finite_default_duration() {
        let args = parse(&["127.0.0.1:2112"]).unwrap();

        assert_eq!(args.targets, ["127.0.0.1:2112"]);
        assert!(args.labelled_targets.is_empty());
        assert_eq!(args.pacing, GroupPacingArg::Staggered);
        assert_eq!(args.duration, DEFAULT_CLIENT_DURATION);
        assert_eq!(args.interval, Duration::from_secs(1));
        assert_eq!(args.format, OutputFormat::Table);
        assert_eq!(args.columns, None);
        assert_eq!(args.header, HeaderMode::Auto);
        assert!(!args.is_continuous());
        assert_eq!(args.timestamp_mode(), TimestampArg::Both);

        let config = args.to_client_config();
        assert_eq!(config.duration, Some(DEFAULT_CLIENT_DURATION));
        assert_eq!(config.received_stats, ReceivedStats::Both);
        assert_eq!(config.stamp_at, StampAt::Both);
        assert_eq!(config.clock, Clock::Both);
        assert_eq!(config.negotiation_policy, NegotiationPolicy::Strict);
    }

    #[test]
    fn client_parser_accepts_stream_formats_and_rejects_tui_output() {
        for format in ["table", "csv", "tsv", "jsonl"] {
            assert!(parse(&["--format", format, "127.0.0.1:2112"]).is_ok());
        }
        assert!(parse(&["--format", "tui", "127.0.0.1:2112"]).is_err());
    }

    #[test]
    fn list_columns_does_not_require_server() {
        let args = parse(&["--list-columns"]).unwrap();
        assert!(args.targets.is_empty());
        assert!(args.list_columns);
    }

    #[test]
    fn multiple_positional_targets_parse_as_targets() {
        let args = parse(&["host-a:2112", "host-b:2112"]).unwrap();
        let specs = args.target_specs().unwrap();

        assert_eq!(specs[0].label, "host-a:2112");
        assert_eq!(specs[0].addr, "host-a:2112");
        assert_eq!(specs[1].label, "host-b:2112");
        assert_eq!(specs[1].addr, "host-b:2112");
    }

    #[test]
    fn repeated_labelled_targets_parse() {
        let args = parse(&[
            "--target",
            "ams=ams.example.com:2112",
            "--target",
            "sg=sg.example.com:2112",
        ])
        .unwrap();
        let specs = args.target_specs().unwrap();

        assert_eq!(specs[0].label, "ams");
        assert_eq!(specs[0].addr, "ams.example.com:2112");
        assert_eq!(specs[1].label, "sg");
        assert_eq!(specs[1].addr, "sg.example.com:2112");
    }

    #[test]
    fn duplicate_labels_are_rejected() {
        let args = parse(&["host-a:2112", "--target", "host-a:2112=host-b:2112"]).unwrap();
        let err = args.target_specs().unwrap_err();

        assert!(err.contains("duplicate target label"));
    }

    #[test]
    fn duplicate_positional_target_strings_get_stable_suffixes() {
        let args = parse(&["host-a:2112", "host-a:2112"]).unwrap();
        let specs = args.target_specs().unwrap();

        assert_eq!(specs[0].label, "host-a:2112");
        assert_eq!(specs[1].label, "host-a:2112#2");
    }

    #[test]
    fn invalid_labelled_target_syntax_is_rejected() {
        assert!(parse(&["--target", "missing-equals"]).is_err());
        assert!(parse(&["--target", "=127.0.0.1:2112"]).is_err());
        assert!(parse(&["--target", "label="]).is_err());
    }

    #[test]
    fn pacing_option_accepts_supported_values() {
        assert_eq!(
            parse(&["--pacing", "staggered", "127.0.0.1:2112"])
                .unwrap()
                .pacing,
            GroupPacingArg::Staggered
        );
        assert_eq!(
            parse(&["--pacing", "burst", "127.0.0.1:2112"])
                .unwrap()
                .pacing,
            GroupPacingArg::Burst
        );
    }

    #[test]
    fn client_help_lists_shared_protocol_options() {
        let help = ClientArgs::command().render_help().to_string();
        assert!(help.contains("--format <FORMAT>"));
        assert!(help.contains("--target <LABEL=TARGET>"));
        assert!(help.contains("--pacing <PACING>"));
        assert!(help.contains("--columns <COLUMNS>"));
        assert!(help.contains("--list-columns"));
        assert!(help.contains("--header <HEADER>"));
        assert!(help.contains("--tstamp <MODE>"));
        assert!(help.contains("--stats <STATS>"));
        assert!(help.contains("--sfill <STRING>"));
        assert!(help.contains("--dscp <0..=63>"));
        assert!(help.contains("--ttl <1..=255>"));
        assert!(help.contains("--loose"));
    }

    #[test]
    fn output_format_summary_policy_is_table_only() {
        assert!(OutputFormat::Table.prints_summary());
        assert!(!OutputFormat::Csv.prints_summary());
        assert!(!OutputFormat::Tsv.prints_summary());
        assert!(!OutputFormat::Jsonl.prints_summary());
    }
}
